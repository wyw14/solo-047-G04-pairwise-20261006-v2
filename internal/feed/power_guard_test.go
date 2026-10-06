package feed

import (
	"testing"
	"time"

	"github.com/wyw14/cry-103/internal/platform"
)

func TestPowerGuardIsolateAndReenergize(t *testing.T) {
	base := time.Date(2026, 10, 5, 0, 0, 0, 0, time.UTC)
	isolation := NewIsolationService()
	guard := NewRepeaterPowerGuard(isolation, platform.UUIDGenerator{})
	circuit, err := NewCircuit("A", "east", base)
	if err != nil {
		t.Fatal(err)
	}
	guard.RegisterCircuit(circuit)
	if err := guard.BindRepeater(RepeaterCircuitBinding{RepeaterID: "R07", StationID: "east", CircuitID: "A"}); err != nil {
		t.Fatal(err)
	}

	if err := guard.IsolateForThermal("R07", 1, base); err != nil {
		t.Fatalf("isolate: %v", err)
	}
	if state := circuit.Snapshot().State; state != StateIsolated {
		t.Fatalf("expected isolated, got %s", state)
	}
	operationID, ok := guard.ThermalOperation("R07")
	if !ok || operationID == "" {
		t.Fatal("expected a tracked thermal isolation operation")
	}

	// 重复热停用必须幂等，不产生第二条操作。
	if err := guard.IsolateForThermal("R07", 1, base.Add(time.Second)); err != nil {
		t.Fatalf("idempotent isolate: %v", err)
	}

	if err := guard.ReenergizeForThermal("R07", 1, base.Add(2*time.Second)); err != nil {
		t.Fatalf("re-energize: %v", err)
	}
	if state := circuit.Snapshot().State; state != StateEnergized {
		t.Fatalf("expected energized, got %s", state)
	}
	if _, ok := guard.ThermalOperation("R07"); ok {
		t.Fatal("thermal isolation operation should be cleared after re-energize")
	}
}

func TestPowerGuardWillNotReenergizeGroundedCircuit(t *testing.T) {
	base := time.Date(2026, 10, 5, 0, 0, 0, 0, time.UTC)
	isolation := NewIsolationService()
	guard := NewRepeaterPowerGuard(isolation, platform.UUIDGenerator{})
	circuit, err := NewCircuit("A", "east", base)
	if err != nil {
		t.Fatal(err)
	}
	guard.RegisterCircuit(circuit)
	if err := guard.BindRepeater(RepeaterCircuitBinding{RepeaterID: "R07", StationID: "east", CircuitID: "A"}); err != nil {
		t.Fatal(err)
	}

	if err := guard.IsolateForThermal("R07", 1, base); err != nil {
		t.Fatal(err)
	}
	operationID, _ := guard.ThermalOperation("R07")
	// 人工在隔离回路上下挂接地：安全规程上禁止自动复电。
	if _, err := isolation.Ground(circuit, operationID, base.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	if err := guard.ReenergizeForThermal("R07", 1, base.Add(2*time.Second)); err == nil {
		t.Fatal("auto re-energize of a grounded circuit must fail")
	}
	if state := circuit.Snapshot().State; state != StateGrounded {
		t.Fatalf("grounded protection must be preserved, got %s", state)
	}
}

func TestPowerGuardRejectsUnknownBinding(t *testing.T) {
	guard := NewRepeaterPowerGuard(NewIsolationService(), platform.UUIDGenerator{})
	if err := guard.IsolateForThermal("ghost", 1, time.Now()); err == nil {
		t.Fatal("expected error for unbound repeater")
	}
}
