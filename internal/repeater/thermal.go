package repeater

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/wyw14/cry-103/internal/feed"
	"github.com/wyw14/cry-103/internal/platform"
	"github.com/wyw14/cry-103/internal/telemetry"
)

type ThermalState string

const (
	ThermalNormal        ThermalState = "normal"
	ThermalDerated       ThermalState = "derated"
	ThermalSuspended     ThermalState = "suspended"
	ThermalIndeterminate ThermalState = "indeterminate"
)

type SafetyAction string

const (
	SafetyNone    SafetyAction = "none"
	SafetyDerate  SafetyAction = "derate"
	SafetySuspend SafetyAction = "suspend"
	SafetyRecover SafetyAction = "recover"
)

type ThermalThresholds struct {
	DerateAtC       float64
	SuspendAtC      float64
	RecoverAtC      float64
	SampleMaxAge    time.Duration
	RecoverySamples int
}

func (t ThermalThresholds) Validate() error {
	if t.RecoverAtC >= t.DerateAtC || t.DerateAtC >= t.SuspendAtC {
		return errors.New("thermal thresholds must satisfy recover < derate < suspend")
	}
	if t.SampleMaxAge <= 0 {
		return errors.New("thermal sample max age must be positive")
	}
	if t.RecoverySamples < 2 {
		return errors.New("thermal recovery requires at least two consecutive samples")
	}
	return nil
}

type ThermalReading struct {
	TemperatureC float64   `json:"temperature_c"`
	CurrentA     float64   `json:"current_a"`
	VoltageV     float64   `json:"voltage_v"`
	Sequence     uint64    `json:"sequence"`
	Generation   uint64    `json:"generation"`
	ObservedAt   time.Time `json:"observed_at"`
}

type ThermalAssessment struct {
	RepeaterID       string         `json:"repeater_id"`
	State            ThermalState   `json:"state"`
	Action           SafetyAction   `json:"action"`
	Reading          ThermalReading `json:"reading"`
	HealthGeneration uint64         `json:"health_generation"`
	RecoveryStreak   int            `json:"recovery_streak"`
	Reason           string         `json:"reason,omitempty"`
	AssessedAt       time.Time      `json:"assessed_at"`
}

// ThermalSafety aggregates temperature, current and voltage telemetry against
// the repeater health generation, derives derate/suspend/recover thresholds and
// connects the resulting safety actions to power isolation and the incident
// lifecycle. A single sample never clears an existing fault: recovery requires
// RecoverySamples consecutive in-band readings, and stale or generation-lagging
// samples render the assessment indeterminate instead of changing state.
type ThermalSafety struct {
	mu         sync.Mutex
	thresholds ThermalThresholds
	incidents  IncidentSink
	isolation  *feed.IsolationService
	ids        platform.IDGenerator
	now        func() time.Time
	repeaters  map[string]*Repeater
	circuits   map[string]*feed.Circuit
	samples    map[string]map[telemetry.Kind]telemetry.Sample
	states     map[string]ThermalState
	streaks    map[string]int
}

func NewThermalSafety(thresholds ThermalThresholds, incidents IncidentSink, isolation *feed.IsolationService, ids platform.IDGenerator, now func() time.Time) (*ThermalSafety, error) {
	if err := thresholds.Validate(); err != nil {
		return nil, err
	}
	if now == nil {
		return nil, errors.New("clock is required")
	}
	return &ThermalSafety{
		thresholds: thresholds, incidents: incidents, isolation: isolation, ids: ids, now: now,
		repeaters: make(map[string]*Repeater), circuits: make(map[string]*feed.Circuit),
		samples: make(map[string]map[telemetry.Kind]telemetry.Sample),
		states:  make(map[string]ThermalState), streaks: make(map[string]int),
	}, nil
}

func (s *ThermalSafety) Register(repeater *Repeater) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.repeaters[repeater.Snapshot().ID] = repeater
}

func (s *ThermalSafety) RegisterCircuit(repeaterID string, circuit *feed.Circuit) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.circuits[repeaterID] = circuit
}

func (s *ThermalSafety) ApplySample(_ context.Context, sample telemetry.Sample) error {
	switch sample.Kind {
	case telemetry.KindTemperature, telemetry.KindCurrent, telemetry.KindVoltage:
	default:
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.repeaters[sample.AssetID] == nil {
		return nil
	}
	kinds := s.samples[sample.AssetID]
	if kinds == nil {
		kinds = make(map[telemetry.Kind]telemetry.Sample)
		s.samples[sample.AssetID] = kinds
	}
	if existing, ok := kinds[sample.Kind]; ok && sample.Sequence < existing.Sequence {
		return nil
	}
	kinds[sample.Kind] = sample
	s.evaluateLocked(sample.AssetID, sample.Kind == telemetry.KindTemperature)
	return nil
}

// Assess reports the current thermal assessment without mutating state. When
// the latest samples are stale or lag the repeater health generation the
// assessment is indeterminate.
func (s *ThermalSafety) Assess(repeaterID string) (ThermalAssessment, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.repeaters[repeaterID] == nil {
		return ThermalAssessment{}, errors.New("repeater not registered for thermal safety")
	}
	return s.assessLocked(repeaterID, false), nil
}

func (s *ThermalSafety) evaluateLocked(repeaterID string, advanceStreak bool) ThermalAssessment {
	assessment := s.assessLocked(repeaterID, advanceStreak)
	if assessment.State == ThermalIndeterminate {
		return assessment
	}
	s.streaks[repeaterID] = assessment.RecoveryStreak
	s.states[repeaterID] = assessment.State
	switch assessment.Action {
	case SafetySuspend:
		if circuit := s.circuits[repeaterID]; circuit != nil && s.isolation != nil && s.ids != nil {
			state := circuit.Snapshot().State
			if state == feed.StateEnergized || state == feed.StateReleased {
				_, _ = s.isolation.Begin(circuit, s.ids.New("thermal-isolation"), assessment.AssessedAt)
			}
		}
		if s.incidents != nil {
			_ = s.incidents.OpenRepeaterIncident(repeaterID, assessment.Reading.Generation)
		}
	case SafetyRecover:
		if s.incidents != nil {
			_ = s.incidents.ResolveRepeaterIncident(repeaterID, assessment.Reading.Generation)
		}
	}
	return assessment
}

func (s *ThermalSafety) assessLocked(repeaterID string, advanceStreak bool) ThermalAssessment {
	now := s.now().UTC()
	repeater := s.repeaters[repeaterID]
	snapshot := repeater.Snapshot()
	state := s.states[repeaterID]
	if state == "" {
		state = ThermalNormal
	}
	assessment := ThermalAssessment{
		RepeaterID: repeaterID, State: state, Action: SafetyNone,
		HealthGeneration: snapshot.TripGeneration, RecoveryStreak: s.streaks[repeaterID], AssessedAt: now,
	}
	kinds := s.samples[repeaterID]
	temperature, okTemp := kinds[telemetry.KindTemperature]
	current, okCurrent := kinds[telemetry.KindCurrent]
	voltage, okVoltage := kinds[telemetry.KindVoltage]
	if !okTemp || !okCurrent || !okVoltage {
		assessment.State = ThermalIndeterminate
		assessment.Reason = "temperature, current and voltage samples are incomplete"
		return assessment
	}
	reading := ThermalReading{
		TemperatureC: temperature.Value, CurrentA: current.Value, VoltageV: voltage.Value,
		Sequence: temperature.Sequence, Generation: temperature.Generation, ObservedAt: temperature.ObservedAt.UTC(),
	}
	for _, sample := range []telemetry.Sample{temperature, current, voltage} {
		if sample.Sequence > reading.Sequence {
			reading.Sequence = sample.Sequence
		}
		if sample.Generation < reading.Generation {
			reading.Generation = sample.Generation
		}
		if sample.ObservedAt.UTC().Before(reading.ObservedAt) {
			reading.ObservedAt = sample.ObservedAt.UTC()
		}
		if now.Sub(sample.ObservedAt.UTC()) > s.thresholds.SampleMaxAge {
			assessment.State = ThermalIndeterminate
			assessment.Reason = "telemetry samples are stale"
			return assessment
		}
		if sample.Generation < snapshot.TripGeneration {
			assessment.State = ThermalIndeterminate
			assessment.Reason = "sample generation lags health generation"
			return assessment
		}
	}
	assessment.Reading = reading
	switch {
	case reading.TemperatureC >= s.thresholds.SuspendAtC:
		assessment.State = ThermalSuspended
		assessment.Action = SafetySuspend
		assessment.RecoveryStreak = 0
	case reading.TemperatureC >= s.thresholds.DerateAtC:
		assessment.State = ThermalDerated
		assessment.Action = SafetyDerate
		assessment.RecoveryStreak = 0
	case reading.TemperatureC <= s.thresholds.RecoverAtC:
		streak := s.streaks[repeaterID]
		if advanceStreak {
			streak++
		}
		assessment.RecoveryStreak = streak
		if streak >= s.thresholds.RecoverySamples {
			assessment.State = ThermalNormal
			if s.states[repeaterID] == ThermalSuspended || s.states[repeaterID] == ThermalDerated {
				assessment.Action = SafetyRecover
			}
		} else {
			assessment.State = state
			assessment.Reason = "recovery pending consecutive normal samples"
		}
	default:
		// Hysteresis band between recover and derate: keep the previous state.
		assessment.State = state
	}
	return assessment
}
