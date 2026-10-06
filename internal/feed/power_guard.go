package feed

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/wyw14/cry-103/internal/platform"
)

// RepeaterCircuitBinding 记录中继站由哪个登陆站的哪条馈电回路供电。
type RepeaterCircuitBinding struct {
	RepeaterID string
	StationID  string
	CircuitID  string
}

// RepeaterPowerGuard 把热安全动作翻译成馈电回路隔离/复电，
// 实现 repeater.PowerIsolator。
type RepeaterPowerGuard struct {
	mu         sync.RWMutex
	isolation  *IsolationService
	circuits   map[string]*Circuit // key: stationID\x00circuitID
	bindings   map[string]RepeaterCircuitBinding
	operations map[string]string // repeaterID -> 由热安全发起的隔离操作号
	ids        platform.IDGenerator
}

// NewRepeaterPowerGuard 创建电源守卫。
func NewRepeaterPowerGuard(isolation *IsolationService, ids platform.IDGenerator) *RepeaterPowerGuard {
	return &RepeaterPowerGuard{
		isolation:  isolation,
		circuits:   make(map[string]*Circuit),
		bindings:   make(map[string]RepeaterCircuitBinding),
		operations: make(map[string]string),
		ids:        ids,
	}
}

// RegisterCircuit 登记可供中继站使用的馈电回路。
func (g *RepeaterPowerGuard) RegisterCircuit(circuit *Circuit) {
	g.mu.Lock()
	defer g.mu.Unlock()
	snapshot := circuit.Snapshot()
	g.circuits[circuitGuardKey(snapshot.StationID, snapshot.ID)] = circuit
}

// BindRepeater 把中继站绑定到指定登陆站的馈电回路。
func (g *RepeaterPowerGuard) BindRepeater(binding RepeaterCircuitBinding) error {
	if binding.RepeaterID == "" || binding.StationID == "" || binding.CircuitID == "" {
		return errors.New("repeater binding requires repeater, station and circuit")
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.circuits[circuitGuardKey(binding.StationID, binding.CircuitID)] == nil {
		return fmt.Errorf("feed circuit %s/%s not registered", binding.StationID, binding.CircuitID)
	}
	g.bindings[binding.RepeaterID] = binding
	return nil
}

// IsolateForThermal 因热停用断开中继站馈电。对同一中继站重复调用是幂等的。
func (g *RepeaterPowerGuard) IsolateForThermal(repeaterID string, generation uint64, at time.Time) error {
	g.mu.Lock()
	circuit, binding, err := g.resolveLocked(repeaterID)
	g.mu.Unlock()
	if err != nil {
		return err
	}
	snapshot := circuit.Snapshot()
	if operationID := g.operations[repeaterID]; operationID != "" &&
		(snapshot.State == StateIsolating || snapshot.State == StateIsolated) {
		// 同一次热停用的重复触发：保持既有隔离操作。
		return nil
	}
	switch snapshot.State {
	case StateEnergized, StateReleased:
	default:
		return fmt.Errorf("circuit %s/%s in state %s cannot be isolated for repeater %s",
			binding.StationID, binding.CircuitID, snapshot.State, repeaterID)
	}
	operationID := g.ids.New("thermal-isolation")
	if _, err := g.isolation.Begin(circuit, operationID, at); err != nil {
		return fmt.Errorf("begin thermal isolation: %w", err)
	}
	if _, err := g.isolation.MarkIsolated(circuit, operationID, at); err != nil {
		return fmt.Errorf("confirm thermal isolation: %w", err)
	}
	g.mu.Lock()
	g.operations[repeaterID] = operationID
	g.mu.Unlock()
	return nil
}

// ReenergizeForThermal 在热条件解除后恢复中继站馈电。
// 已被人工接地的回路不会被自动复电，必须先走接地释放流程。
func (g *RepeaterPowerGuard) ReenergizeForThermal(repeaterID string, generation uint64, at time.Time) error {
	g.mu.Lock()
	circuit, _, err := g.resolveLocked(repeaterID)
	operationID := g.operations[repeaterID]
	g.mu.Unlock()
	if err != nil {
		return err
	}
	if operationID == "" {
		// 并非由热安全隔离（可能人工已处理）：仅在已送电时视为成功。
		if circuit.Snapshot().State == StateEnergized {
			return nil
		}
		return fmt.Errorf("no active thermal isolation for repeater %s", repeaterID)
	}
	if _, err := g.isolation.Reenergize(circuit, operationID, at); err != nil {
		return fmt.Errorf("re-energize after thermal resume: %w", err)
	}
	g.mu.Lock()
	delete(g.operations, repeaterID)
	g.mu.Unlock()
	return nil
}

// ThermalOperation 返回某中继站当前由热安全发起的隔离操作号（如有）。
func (g *RepeaterPowerGuard) ThermalOperation(repeaterID string) (string, bool) {
	g.mu.RLock()
	defer g.mu.RUnlock()
	operationID, ok := g.operations[repeaterID]
	return operationID, ok
}

func (g *RepeaterPowerGuard) resolveLocked(repeaterID string) (*Circuit, RepeaterCircuitBinding, error) {
	binding, ok := g.bindings[repeaterID]
	if !ok {
		return nil, RepeaterCircuitBinding{}, fmt.Errorf("repeater %s has no feed circuit binding", repeaterID)
	}
	circuit := g.circuits[circuitGuardKey(binding.StationID, binding.CircuitID)]
	if circuit == nil {
		return nil, binding, fmt.Errorf("feed circuit %s/%s not registered", binding.StationID, binding.CircuitID)
	}
	return circuit, binding, nil
}

func circuitGuardKey(stationID, circuitID string) string {
	return stationID + "\x00" + circuitID
}
