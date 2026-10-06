package main

import (
	"fmt"
	"path/filepath"
	"time"

	"github.com/wyw14/cry-103/internal/api"
	"github.com/wyw14/cry-103/internal/feed"
	"github.com/wyw14/cry-103/internal/incident"
	"github.com/wyw14/cry-103/internal/landing"
	"github.com/wyw14/cry-103/internal/otdr"
	"github.com/wyw14/cry-103/internal/platform"
	"github.com/wyw14/cry-103/internal/repair"
	"github.com/wyw14/cry-103/internal/repeater"
	"github.com/wyw14/cry-103/internal/route"
	"github.com/wyw14/cry-103/internal/span"
	"github.com/wyw14/cry-103/internal/store"
	"github.com/wyw14/cry-103/internal/switchover"
	"github.com/wyw14/cry-103/internal/telemetry"
	"github.com/wyw14/cry-103/internal/topology"
)

func buildServices(cfg config) (*api.Services, error) {
	clock := platform.RealClock{}
	ids := platform.UUIDGenerator{}
	now := clock.Now
	snapshots := store.NewSnapshotStore(filepath.Join(cfg.Data, "snapshots"))
	checkpoints := store.NewCheckpoints(snapshots)
	journal := store.NewJournal(filepath.Join(cfg.Data, "events.jsonl"))

	topologies := topology.NewRegistry()
	if _, err := topologies.Register("S3", topology.PathPrimary, now()); err != nil {
		return nil, err
	}
	spanS3, err := span.New("S3", "South China Sea S3", 126.4, now())
	if err != nil {
		return nil, err
	}
	if _, err := spanS3.MarkCut(now()); err != nil {
		return nil, err
	}
	if _, err := spanS3.StartRepair(now()); err != nil {
		return nil, err
	}
	if _, err := spanS3.BeginProof(now()); err != nil {
		return nil, err
	}
	routeS3, err := route.New("R-S3", "S3", now())
	if err != nil {
		return nil, err
	}

	telemetryRepository := telemetry.NewRepository()
	mutes := telemetry.NewMuteManager(now)
	admission := incident.NewAdmissionService(mutes)
	repeaterIncidents := incident.NewRepeaterLifecycle()
	healthProjector := repeater.NewHealthProjector(repeaterIncidents, 85)
	repeaterR07, err := repeater.New("R07", now())
	if err != nil {
		return nil, err
	}
	healthProjector.Register(repeaterR07)

	// 馈电隔离、回路与中继站电源守卫：热停用直接驱动这些回路隔离/复电。
	receipts := landing.NewReceiptRegistry()
	isolation := feed.NewIsolationService()
	powerGuard := feed.NewRepeaterPowerGuard(isolation, ids)
	permits := repair.NewPermitService(isolation, receipts, ids, now)
	for _, circuitID := range []string{"A", "B"} {
		circuit, createErr := feed.NewCircuit(circuitID, "east", now())
		if createErr != nil {
			return nil, createErr
		}
		permits.RegisterCircuit(circuit)
		powerGuard.RegisterCircuit(circuit)
	}
	// R07 由 east 站 A 回路供电。
	if err := powerGuard.BindRepeater(feed.RepeaterCircuitBinding{
		RepeaterID: "R07", StationID: "east", CircuitID: "A",
	}); err != nil {
		return nil, err
	}

	// 热安全模块：汇总温/流/压/代次，联动电源隔离与事件生命周期。
	thermalEvents := api.NewThermalEventRecorder(journal)
	thermalSafety, err := repeater.NewThermalSafetyMonitor(
		repeater.DefaultThermalThresholds(), repeaterIncidents, powerGuard, thermalEvents, now)
	if err != nil {
		return nil, err
	}
	thermalSafety.Register(repeaterR07)
	ingestor := telemetry.NewIngestor(telemetryRepository, now, healthProjector, thermalSafety)

	scanner := landing.NewScanner(topologies, 2*time.Millisecond)
	otdrService := otdr.NewSessionService(topologies, scanner, ids, now)
	drainer := route.NewDrainer(4 * time.Millisecond)
	activator := route.NewActivator(topologies)
	switchovers := switchover.NewSessionService(drainer, activator, ids, now, 25*time.Millisecond)
	restorer := switchover.NewRestorer(drainer, activator, topologies, ids, now)

	acceptance := repair.NewAcceptanceService(restorer, now)
	acceptance.Register("S3", spanS3, routeS3)
	station, err := landing.NewStation("east", "East Landing", now())
	if err != nil {
		return nil, err
	}
	heartbeats := store.NewHeartbeatStore(checkpoints)
	ageEvaluator, err := telemetry.NewAgeEvaluator(now, 10*time.Second)
	if err != nil {
		return nil, err
	}
	health := landing.NewHealthService(ageEvaluator, heartbeats)
	if _, err := health.Record(station, 1, now()); err != nil {
		return nil, fmt.Errorf("seed heartbeat checkpoint: %w", err)
	}
	if _, err := health.Recover(station); err != nil {
		return nil, err
	}
	health.Evaluate(station, now())

	assets := repair.NewAssets()
	assets.AddSpan(spanS3)
	assets.AddRoute(routeS3)
	for _, circuitID := range []string{"A", "B"} {
		if circuit, ok := permitsCircuit(permits, "east", circuitID); ok {
			assets.AddCircuit(circuit)
		}
	}
	actuator := feed.NewActuator()
	ramps := repeater.NewRampController(actuator, telemetryRepository, telemetry.StabilityWindow{MinimumSamples: 2, MaximumSpread: 1.5, MinimumPeriod: time.Second}, checkpoints, now)
	wetPlantTests := repair.NewTestService(mutes, ids, now, 30*time.Second)

	return &api.Services{
		Clock: clock, IDs: ids, Journal: journal,
		Spans: map[string]*span.Span{"S3": spanS3}, Routes: map[string]*route.Route{"R-S3": routeS3},
		Repeaters: map[string]*repeater.Repeater{"R07": repeaterR07}, Topology: topologies,
		OTDR: otdrService, Switchovers: switchovers, Permits: permits, Receipts: receipts,
		Acceptance: acceptance, Telemetry: ingestor, TelemetryRepository: telemetryRepository, Admission: admission,
		Classifier: incident.NewClassifier(), Correlator: otdr.NewCorrelator(), Health: health, Station: station,
		WetPlantTests: wetPlantTests, Ramps: ramps, Assets: assets,
		RepeaterHealth: healthProjector, ThermalSafety: thermalSafety, PowerGuard: powerGuard, Actuator: actuator,
		Tests: make(map[string]*repair.TestSession), Incidents: make(map[string]*incident.Incident),
	}, nil
}

func permitsCircuit(service *repair.PermitService, stationID, circuitID string) (*feed.Circuit, bool) {
	return service.Circuit(stationID, circuitID)
}
