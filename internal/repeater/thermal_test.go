package repeater

import (
	"context"
	"testing"
	"time"

	"github.com/wyw14/cry-103/internal/feed"
	"github.com/wyw14/cry-103/internal/incident"
	"github.com/wyw14/cry-103/internal/platform"
	"github.com/wyw14/cry-103/internal/telemetry"
)

type thermalFixture struct {
	safety    *ThermalSafety
	repeater  *Repeater
	circuit   *feed.Circuit
	incidents *incident.RepeaterLifecycle
	now       time.Time
}

func newThermalFixture(t *testing.T) *thermalFixture {
	t.Helper()
	start := time.Date(2026, 10, 5, 12, 0, 0, 0, time.UTC)
	fixture := &thermalFixture{now: start}
	asset, err := New("R07", start)
	if err != nil {
		t.Fatal(err)
	}
	circuit, err := feed.NewCircuit("A", "east", start)
	if err != nil {
		t.Fatal(err)
	}
	lifecycle := incident.NewRepeaterLifecycle()
	safety, err := NewThermalSafety(ThermalThresholds{
		DerateAtC: 70, SuspendAtC: 85, RecoverAtC: 60,
		SampleMaxAge: 10 * time.Second, RecoverySamples: 2,
	}, lifecycle, feed.NewIsolationService(), platform.UUIDGenerator{}, func() time.Time { return fixture.now })
	if err != nil {
		t.Fatal(err)
	}
	safety.Register(asset)
	safety.RegisterCircuit("R07", circuit)
	fixture.safety, fixture.repeater, fixture.circuit, fixture.incidents = safety, asset, circuit, lifecycle
	return fixture
}

func (f *thermalFixture) sample(kind telemetry.Kind, value float64, sequence uint64) telemetry.Sample {
	return telemetry.Sample{
		ID: string(kind) + "-sample", AssetID: "R07", Kind: kind, Value: value,
		Sequence: sequence, Generation: 1, ObservedAt: f.now,
	}
}

func (f *thermalFixture) feedAll(t *testing.T, temperature float64, sequence uint64) {
	t.Helper()
	for _, item := range []telemetry.Sample{
		f.sample(telemetry.KindTemperature, temperature, sequence),
		f.sample(telemetry.KindCurrent, 1.2, sequence),
		f.sample(telemetry.KindVoltage, 48, sequence),
	} {
		if err := f.safety.ApplySample(context.Background(), item); err != nil {
			t.Fatal(err)
		}
	}
}

func TestThermalThresholdsRejectMisordering(t *testing.T) {
	if _, err := NewThermalSafety(ThermalThresholds{
		DerateAtC: 85, SuspendAtC: 70, RecoverAtC: 60,
		SampleMaxAge: time.Second, RecoverySamples: 2,
	}, nil, nil, nil, time.Now); err == nil {
		t.Fatal("expected threshold ordering error")
	}
	if _, err := NewThermalSafety(ThermalThresholds{
		DerateAtC: 70, SuspendAtC: 85, RecoverAtC: 60,
		SampleMaxAge: time.Second, RecoverySamples: 1,
	}, nil, nil, nil, time.Now); err == nil {
		t.Fatal("expected recovery sample count error")
	}
}

func TestThermalIndeterminateUntilAllKindsArrive(t *testing.T) {
	fixture := newThermalFixture(t)
	if err := fixture.safety.ApplySample(context.Background(), fixture.sample(telemetry.KindTemperature, 50, 1)); err != nil {
		t.Fatal(err)
	}
	assessment, err := fixture.safety.Assess("R07")
	if err != nil {
		t.Fatal(err)
	}
	if assessment.State != ThermalIndeterminate {
		t.Fatalf("expected indeterminate, got %s", assessment.State)
	}
}

func TestThermalSuspendIsolatesCircuitAndOpensIncident(t *testing.T) {
	fixture := newThermalFixture(t)
	fixture.feedAll(t, 40, 1)
	fixture.feedAll(t, 72, 2)
	assessment, err := fixture.safety.Assess("R07")
	if err != nil {
		t.Fatal(err)
	}
	if assessment.State != ThermalDerated || assessment.Action != SafetyDerate {
		t.Fatalf("expected derated, got %+v", assessment)
	}
	if fixture.circuit.Snapshot().State != feed.StateEnergized {
		t.Fatalf("derate must not isolate the circuit, got %s", fixture.circuit.Snapshot().State)
	}

	fixture.feedAll(t, 90, 3)
	assessment, err = fixture.safety.Assess("R07")
	if err != nil {
		t.Fatal(err)
	}
	if assessment.State != ThermalSuspended || assessment.Action != SafetySuspend {
		t.Fatalf("expected suspended, got %+v", assessment)
	}
	if fixture.circuit.Snapshot().State != feed.StateIsolating {
		t.Fatalf("suspend must begin power isolation, got %s", fixture.circuit.Snapshot().State)
	}
	if _, active := fixture.incidents.Active("R07"); !active {
		t.Fatal("suspend must open a repeater incident")
	}
}

func TestThermalSingleCoolSampleDoesNotClearFault(t *testing.T) {
	fixture := newThermalFixture(t)
	fixture.feedAll(t, 90, 1)
	fixture.feedAll(t, 50, 2)
	assessment, err := fixture.safety.Assess("R07")
	if err != nil {
		t.Fatal(err)
	}
	if assessment.State != ThermalSuspended {
		t.Fatalf("single normal sample must not clear the fault, got %+v", assessment)
	}
	if _, active := fixture.incidents.Active("R07"); !active {
		t.Fatal("incident must remain open after a single normal sample")
	}

	fixture.feedAll(t, 50, 3)
	assessment, err = fixture.safety.Assess("R07")
	if err != nil {
		t.Fatal(err)
	}
	if assessment.State != ThermalNormal {
		t.Fatalf("expected recovery after consecutive normal samples, got %+v", assessment)
	}
	if _, active := fixture.incidents.Active("R07"); active {
		t.Fatal("incident must resolve after sustained recovery")
	}
}

func TestThermalStaleSamplesAreIndeterminate(t *testing.T) {
	fixture := newThermalFixture(t)
	fixture.feedAll(t, 40, 1)
	fixture.now = fixture.now.Add(30 * time.Second)
	assessment, err := fixture.safety.Assess("R07")
	if err != nil {
		t.Fatal(err)
	}
	if assessment.State != ThermalIndeterminate || assessment.Reason == "" {
		t.Fatalf("stale samples must be indeterminate, got %+v", assessment)
	}
}

func TestThermalLaggingGenerationIsIndeterminate(t *testing.T) {
	fixture := newThermalFixture(t)
	fixture.feedAll(t, 40, 1)
	fixture.repeater.update(func(current *Snapshot) {
		current.TripGeneration = 7
	})
	assessment, err := fixture.safety.Assess("R07")
	if err != nil {
		t.Fatal(err)
	}
	if assessment.State != ThermalIndeterminate {
		t.Fatalf("lagging generation must be indeterminate, got %+v", assessment)
	}
	if assessment.HealthGeneration != 7 {
		t.Fatalf("expected health generation 7, got %d", assessment.HealthGeneration)
	}
}
