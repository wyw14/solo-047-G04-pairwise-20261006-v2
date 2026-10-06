package api

import (
	"time"

	"github.com/wyw14/cry-103/internal/store"
)

// ThermalEventPayload 是写入事件日志（journal）的热安全生命周期事件负载。
type ThermalEventPayload struct {
	RepeaterID string    `json:"repeater_id"`
	Generation uint64    `json:"generation"`
	Kind       string    `json:"kind"`
	OccurredAt time.Time `json:"occurred_at"`
}

// ThermalEventRecorder 把热停用/恢复迁移写入事件生命周期日志，
// 实现 repeater.ThermalEventSink。
type ThermalEventRecorder struct {
	journal *store.Journal
}

// NewThermalEventRecorder 创建事件记录器。
func NewThermalEventRecorder(journal *store.Journal) *ThermalEventRecorder {
	return &ThermalEventRecorder{journal: journal}
}

func (r *ThermalEventRecorder) OnThermalShutdown(repeaterID string, generation uint64, at time.Time) error {
	return r.append("thermal-shutdown", repeaterID, generation, at)
}

func (r *ThermalEventRecorder) OnThermalResume(repeaterID string, generation uint64, at time.Time) error {
	return r.append("thermal-resume", repeaterID, generation, at)
}

func (r *ThermalEventRecorder) append(kind, repeaterID string, generation uint64, at time.Time) error {
	return r.journal.Append(kind, repeaterID, ThermalEventPayload{
		RepeaterID: repeaterID, Generation: generation, Kind: kind, OccurredAt: at.UTC(),
	}, at.UTC())
}
