package api

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

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

func testServer(t *testing.T) *Server {
	t.Helper()
	clock := fixedClock{now: time.Date(2026, 8, 24, 3, 0, 0, 0, time.UTC)}
	ids := platform.UUIDGenerator{}
	registry := topology.NewRegistry()
	if _, err := registry.Register("S3", topology.PathPrimary, clock.Now()); err != nil {
		t.Fatal(err)
	}
	target, err := span.New("S3", "S3", 126, clock.Now())
	if err != nil {
		t.Fatal(err)
	}
	path, err := route.New("R-S3", "S3", clock.Now())
	if err != nil {
		t.Fatal(err)
	}
	drainer := route.NewDrainer(0)
	activator := route.NewActivator(registry)
	receipts := landing.NewReceiptRegistry()
	permitService := repair.NewPermitService(feed.NewIsolationService(), receipts, ids, clock.Now)
	circuit, err := feed.NewCircuit("B", "east", clock.Now())
	if err != nil {
		t.Fatal(err)
	}
	permitService.RegisterCircuit(circuit)
	repeaterAsset, err := repeater.New("R07", clock.Now())
	if err != nil {
		t.Fatal(err)
	}
	lifecycle := incident.NewRepeaterLifecycle()
	projector := repeater.NewHealthProjector(lifecycle, 85)
	projector.Register(repeaterAsset)
	repository := telemetry.NewRepository()

	// 热安全模块与电源守卫：R07 由 east 站 A 回路供电。
	thermalIsolation := feed.NewIsolationService()
	thermalCircuit, err := feed.NewCircuit("A", "east", clock.Now())
	if err != nil {
		t.Fatal(err)
	}
	powerGuard := feed.NewRepeaterPowerGuard(thermalIsolation, ids)
	powerGuard.RegisterCircuit(thermalCircuit)
	servicesJournal := store.NewJournal(filepath.Join(t.TempDir(), "events.jsonl"))
	thermalSafety, err := repeater.NewThermalSafetyMonitor(
		repeater.DefaultThermalThresholds(), lifecycle, powerGuard, NewThermalEventRecorder(servicesJournal), clock.Now)
	if err != nil {
		t.Fatal(err)
	}
	thermalSafety.Register(repeaterAsset)
	if err := powerGuard.BindRepeater(feed.RepeaterCircuitBinding{RepeaterID: "R07", StationID: "east", CircuitID: "A"}); err != nil {
		t.Fatal(err)
	}

	acceptance := repair.NewAcceptanceService(switchover.NewRestorer(drainer, activator, registry, ids, clock.Now), clock.Now)
	acceptance.Register("S3", target, path)
	services := &Services{
		Clock: clock, IDs: ids, Journal: servicesJournal,
		Spans: map[string]*span.Span{"S3": target}, Routes: map[string]*route.Route{"R-S3": path},
		Repeaters: map[string]*repeater.Repeater{"R07": repeaterAsset}, Topology: registry,
		OTDR:        otdr.NewSessionService(registry, landing.NewScanner(registry, 0), ids, clock.Now),
		Switchovers: switchover.NewSessionService(drainer, activator, ids, clock.Now, time.Second),
		Permits:     permitService, Receipts: receipts, Acceptance: acceptance,
		Telemetry: telemetry.NewIngestor(repository, clock.Now, projector, thermalSafety), TelemetryRepository: repository,
		Admission:      incident.NewAdmissionService(telemetry.NewMuteManager(clock.Now)),
		RepeaterHealth: projector, ThermalSafety: thermalSafety, PowerGuard: powerGuard,
	}
	return NewServer(services)
}

type fixedClock struct {
	now time.Time
}

func (c fixedClock) Now() time.Time {
	return c.now
}

func postTelemetry(t *testing.T, server *Server, kind string, value float64, sequence, generation uint64) {
	t.Helper()
	body, _ := json.Marshal(TelemetryRequest{Kind: kind, Value: value, Sequence: sequence, Generation: generation})
	response := httptest.NewRecorder()
	server.Handler().ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/api/repeaters/R07/telemetry", bytes.NewReader(body)))
	if response.Code != http.StatusAccepted {
		t.Fatalf("ingest %s status=%d body=%s", kind, response.Code, response.Body.String())
	}
}

func pushThermalBatch(t *testing.T, server *Server, temp, current, voltage float64, sequence, generation uint64) {
	postTelemetry(t, server, "temperature", temp, sequence, generation)
	postTelemetry(t, server, "current", current, sequence, generation)
	postTelemetry(t, server, "voltage", voltage, sequence, generation)
}

func getThermal(t *testing.T, server *Server) repeater.ThermalStatus {
	t.Helper()
	response := httptest.NewRecorder()
	server.Handler().ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/api/repeaters/R07/thermal", nil))
	if response.Code != http.StatusOK {
		t.Fatalf("thermal status=%d body=%s", response.Code, response.Body.String())
	}
	var status repeater.ThermalStatus
	if err := json.Unmarshal(response.Body.Bytes(), &status); err != nil {
		t.Fatal(err)
	}
	return status
}

func TestThermalSafetyHTTPFlow(t *testing.T) {
	server := testServer(t)

	// 通道未到齐时必须是不可判定，而非默认安全。
	if status := getThermal(t, server); status.Determinate {
		t.Fatalf("expected indeterminate before complete telemetry, got %+v", status)
	}

	// 85°C 达到停用阈值。
	pushThermalBatch(t, server, 85, 8, 450, 1, 1)
	status := getThermal(t, server)
	if status.Action != "shutdown" || !status.Latched || status.DerateFactor != 0 {
		t.Fatalf("expected shutdown, got %+v", status)
	}

	// 单个降温批次不能清除故障。
	pushThermalBatch(t, server, 40, 8, 450, 2, 1)
	if status := getThermal(t, server); !status.Latched {
		t.Fatalf("single cool batch must not clear latch, got %+v", status)
	}

	// 第二个连续恢复批次后解除停用。
	pushThermalBatch(t, server, 40, 8, 450, 3, 1)
	status = getThermal(t, server)
	if status.Latched || status.Action != "normal" {
		t.Fatalf("expected resume after two evidence cycles, got %+v", status)
	}

	// 事件生命周期中应同时存在停用与恢复记录。
	response := httptest.NewRecorder()
	server.Handler().ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/api/events?entity_id=R07", nil))
	if response.Code != http.StatusOK {
		t.Fatalf("events status=%d", response.Code)
	}
	var page EventPage
	if err := json.Unmarshal(response.Body.Bytes(), &page); err != nil {
		t.Fatal(err)
	}
	var sawShutdown, sawResume bool
	for _, record := range page.Items {
		if record.Kind == "thermal-shutdown" {
			sawShutdown = true
		}
		if record.Kind == "thermal-resume" {
			sawResume = true
		}
	}
	if !sawShutdown || !sawResume {
		t.Fatalf("expected thermal-shutdown and thermal-resume events, got %+v", page.Items)
	}
}

func TestHealthAndScanHTTPFlow(t *testing.T) {
	server := testServer(t)
	health := httptest.NewRecorder()
	server.Handler().ServeHTTP(health, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if health.Code != http.StatusOK {
		t.Fatalf("health status=%d body=%s", health.Code, health.Body.String())
	}

	body, _ := json.Marshal(ScanRequest{StationID: "east", LengthKM: 126, Chunks: 3})
	scan := httptest.NewRecorder()
	server.Handler().ServeHTTP(scan, httptest.NewRequest(http.MethodPost, "/api/spans/S3/otdr-scans", bytes.NewReader(body)))
	if scan.Code != http.StatusCreated {
		t.Fatalf("scan status=%d body=%s", scan.Code, scan.Body.String())
	}
	var trace otdr.Trace
	if err := json.Unmarshal(scan.Body.Bytes(), &trace); err != nil {
		t.Fatal(err)
	}
	if trace.SpanID != "S3" || len(trace.Chunks) != 3 {
		t.Fatalf("unexpected trace: %+v", trace)
	}

	status := httptest.NewRecorder()
	server.Handler().ServeHTTP(status, httptest.NewRequest(http.MethodGet, "/api/system/status", nil))
	if status.Code != http.StatusOK {
		t.Fatalf("status code=%d body=%s", status.Code, status.Body.String())
	}
}
