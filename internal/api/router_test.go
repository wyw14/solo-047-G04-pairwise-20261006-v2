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
	thermal, err := repeater.NewThermalSafety(repeater.ThermalThresholds{
		DerateAtC: 70, SuspendAtC: 85, RecoverAtC: 60,
		SampleMaxAge: 10 * time.Second, RecoverySamples: 2,
	}, lifecycle, feed.NewIsolationService(), ids, clock.Now)
	if err != nil {
		t.Fatal(err)
	}
	thermal.Register(repeaterAsset)
	repository := telemetry.NewRepository()
	acceptance := repair.NewAcceptanceService(switchover.NewRestorer(drainer, activator, registry, ids, clock.Now), clock.Now)
	acceptance.Register("S3", target, path)
	services := &Services{
		Clock: clock, IDs: ids, Journal: store.NewJournal(filepath.Join(t.TempDir(), "events.jsonl")),
		Spans: map[string]*span.Span{"S3": target}, Routes: map[string]*route.Route{"R-S3": path},
		Repeaters: map[string]*repeater.Repeater{"R07": repeaterAsset}, Topology: registry,
		OTDR:        otdr.NewSessionService(registry, landing.NewScanner(registry, 0), ids, clock.Now),
		Switchovers: switchover.NewSessionService(drainer, activator, ids, clock.Now, time.Second),
		Permits:     permitService, Receipts: receipts, Acceptance: acceptance,
		Telemetry: telemetry.NewIngestor(repository, clock.Now, projector, thermal), TelemetryRepository: repository,
		Admission: incident.NewAdmissionService(telemetry.NewMuteManager(clock.Now)), Thermal: thermal,
	}
	return NewServer(services)
}

type fixedClock struct {
	now time.Time
}

func (c fixedClock) Now() time.Time {
	return c.now
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

func TestRepeaterThermalHTTPFlow(t *testing.T) {
	server := testServer(t)
	ingest := func(kind string, value float64, sequence uint64) {
		t.Helper()
		body, _ := json.Marshal(TelemetryRequest{Kind: kind, Value: value, Sequence: sequence, Generation: 1})
		response := httptest.NewRecorder()
		server.Handler().ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/api/repeaters/R07/telemetry", bytes.NewReader(body)))
		if response.Code != http.StatusAccepted {
			t.Fatalf("ingest %s status=%d body=%s", kind, response.Code, response.Body.String())
		}
	}
	assess := func() repeater.ThermalAssessment {
		t.Helper()
		response := httptest.NewRecorder()
		server.Handler().ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/api/repeaters/R07/thermal", nil))
		if response.Code != http.StatusOK {
			t.Fatalf("thermal status=%d body=%s", response.Code, response.Body.String())
		}
		var assessment repeater.ThermalAssessment
		if err := json.Unmarshal(response.Body.Bytes(), &assessment); err != nil {
			t.Fatal(err)
		}
		return assessment
	}

	if assessment := assess(); assessment.State != repeater.ThermalIndeterminate {
		t.Fatalf("expected indeterminate before samples, got %+v", assessment)
	}
	ingest("temperature", 91, 1)
	ingest("current", 1.4, 1)
	ingest("voltage", 48, 1)
	if assessment := assess(); assessment.State != repeater.ThermalSuspended {
		t.Fatalf("expected suspended, got %+v", assessment)
	}
	ingest("temperature", 50, 2)
	ingest("current", 1.1, 2)
	ingest("voltage", 48, 2)
	if assessment := assess(); assessment.State != repeater.ThermalSuspended {
		t.Fatalf("single normal sample must not clear the fault, got %+v", assessment)
	}
	ingest("temperature", 50, 3)
	ingest("current", 1.1, 3)
	ingest("voltage", 48, 3)
	if assessment := assess(); assessment.State != repeater.ThermalNormal {
		t.Fatalf("expected recovery after consecutive normal samples, got %+v", assessment)
	}
}
