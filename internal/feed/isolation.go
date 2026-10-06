package feed

import (
	"errors"
	"time"
)

type IsolationCommand struct {
	StationID   string    `json:"station_id"`
	CircuitID   string    `json:"circuit_id"`
	OperationID string    `json:"operation_id"`
	IssuedAt    time.Time `json:"issued_at"`
}

type IsolationService struct{}

func NewIsolationService() *IsolationService {
	return &IsolationService{}
}

func (s *IsolationService) Begin(circuit *Circuit, operationID string, at time.Time) (IsolationCommand, error) {
	if operationID == "" {
		return IsolationCommand{}, errors.New("isolation operation ID is required")
	}
	snapshot := circuit.Snapshot()
	if snapshot.State != StateEnergized && snapshot.State != StateReleased {
		return IsolationCommand{}, errors.New("circuit is not ready for isolation")
	}
	circuit.update(StateIsolating, operationID, at)
	return IsolationCommand{
		StationID: snapshot.StationID, CircuitID: snapshot.ID, OperationID: operationID, IssuedAt: at.UTC(),
	}, nil
}

func (s *IsolationService) Confirm(circuit *Circuit, stationID, circuitID, operationID string, matched bool, at time.Time) (CircuitSnapshot, error) {
	snapshot := circuit.Snapshot()
	if snapshot.StationID != stationID || snapshot.ID != circuitID || snapshot.OperationID != operationID {
		return snapshot, errors.New("isolation receipt scope does not match active operation")
	}
	if !matched {
		return snapshot, errors.New("matching isolated receipt not available")
	}
	return circuit.update(StateIsolated, operationID, at), nil
}

func (s *IsolationService) Ground(circuit *Circuit, operationID string, at time.Time) (CircuitSnapshot, error) {
	snapshot := circuit.Snapshot()
	if snapshot.State != StateIsolated || snapshot.OperationID != operationID {
		return snapshot, errors.New("circuit must be isolated by the current operation before grounding")
	}
	return circuit.update(StateGrounded, operationID, at), nil
}

func (s *IsolationService) Release(circuit *Circuit, operationID string, at time.Time) (CircuitSnapshot, error) {
	snapshot := circuit.Snapshot()
	if snapshot.State != StateGrounded || snapshot.OperationID != operationID {
		return snapshot, errors.New("grounded operation does not match release")
	}
	return circuit.update(StateReleased, operationID, at), nil
}

// MarkIsolated 是机器保护（如热停用跳闸）使用的隔离确认：它不需要人工
// 隔离回执，直接把已进入 isolating 的回路推进到 isolated。
func (s *IsolationService) MarkIsolated(circuit *Circuit, operationID string, at time.Time) (CircuitSnapshot, error) {
	snapshot := circuit.Snapshot()
	if snapshot.OperationID != operationID {
		return snapshot, errors.New("isolation receipt scope does not match active operation")
	}
	if snapshot.State != StateIsolating {
		return snapshot, errors.New("circuit is not awaiting isolation confirmation")
	}
	return circuit.update(StateIsolated, operationID, at), nil
}

// Reenergize 在隔离（未接地）状态下恢复送电，用于热安全条件解除后的自动复电。
// 已接地回路必须走人工 Release 流程，本方法不会越过接地保护。
func (s *IsolationService) Reenergize(circuit *Circuit, operationID string, at time.Time) (CircuitSnapshot, error) {
	snapshot := circuit.Snapshot()
	if snapshot.OperationID != operationID {
		return snapshot, errors.New("re-energize operation does not match active isolation")
	}
	if snapshot.State != StateIsolated {
		return snapshot, errors.New("only an isolated (ungrounded) circuit can be re-energized")
	}
	return circuit.update(StateEnergized, "", at), nil
}
