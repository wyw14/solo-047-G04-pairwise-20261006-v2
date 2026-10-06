package repeater

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/wyw14/cry-103/internal/telemetry"
)

// ThermalAction 是热安全模块对单台中继站给出的电源处置建议。
type ThermalAction string

const (
	// ActionNormal 无需处置。
	ActionNormal ThermalAction = "normal"
	// ActionDerate 温度进入降额区，需要按比例限制馈电电流。
	ActionDerate ThermalAction = "derate"
	// ActionShutdown 达到停用阈值，需要断开该中继站的馈电。
	ActionShutdown ThermalAction = "shutdown"
	// ActionIndeterminate 采样过期或健康代次落后，无法判定安全状态。
	ActionIndeterminate ThermalAction = "indeterminate"
)

// ThermalThresholds 汇总热安全判定用到的全部阈值（摄氏度 / 安培 / 伏特）。
// 停用与恢复之间使用迟滞：温度回落到 ResumeAtC 以下才允许恢复。
type ThermalThresholds struct {
	DerateStartC float64       // 达到该温度开始线性降额
	ShutdownAtC  float64       // 达到该温度立即停用并锁存
	ResumeAtC    float64       // 低于该温度才允许解除停用（须严格小于 ShutdownAtC）
	MaxCurrentA  float64       // 电流额定上限，超过同样进入降额/停用判定
	CurrentTripA float64       // 电流硬上限，超过立即停用
	MinVoltageV  float64       // 电压低于该值视为欠压，不允许恢复送电
	MaxVoltageV  float64       // 电压高于该值视为过压，立即停用
	StaleAfter   time.Duration // 任一关键量的最新样本超过该时长即视为过期
	// ResumeConfirmations 是解除锁存前需要连续命中恢复包络的判定次数。
	// 单个（“单点”）恢复样本不足以清除既有故障。
	ResumeConfirmations int
}

// DefaultThermalThresholds 是与现有 85°C 跳闸设定一致的默认阈值。
func DefaultThermalThresholds() ThermalThresholds {
	return ThermalThresholds{
		DerateStartC:        70,
		ShutdownAtC:         85,
		ResumeAtC:           65,
		MaxCurrentA:         10,
		CurrentTripA:        12,
		MaxVoltageV:         600,
		MinVoltageV:         300,
		StaleAfter:          10 * time.Second,
		ResumeConfirmations: 2,
	}
}

func (t ThermalThresholds) Validate() error {
	if t.DerateStartC <= 0 || t.ShutdownAtC <= t.DerateStartC || t.ResumeAtC >= t.ShutdownAtC {
		return errors.New("thermal thresholds require 0 < derate < shutdown and resume < shutdown")
	}
	if t.MaxCurrentA <= 0 || t.CurrentTripA < t.MaxCurrentA {
		return errors.New("thermal thresholds require 0 < max-current <= current-trip")
	}
	if t.MinVoltageV <= 0 || t.MaxVoltageV <= t.MinVoltageV {
		return errors.New("thermal thresholds require 0 < min-voltage < max-voltage")
	}
	if t.StaleAfter <= 0 {
		return errors.New("thermal thresholds require a positive sample staleness window")
	}
	if t.ResumeConfirmations < 1 {
		return errors.New("thermal thresholds require at least one resume confirmation")
	}
	return nil
}

// ThermalReading 是参与判定的四个汇总量。
type ThermalReading struct {
	TemperatureC float64   `json:"temperature_c"`
	CurrentA     float64   `json:"current_a"`
	VoltageV     float64   `json:"voltage_v"`
	Generation   uint64    `json:"generation"`
	Sequence     uint64    `json:"sequence"`
	ObservedAt   time.Time `json:"observed_at"`
}

// ThermalStatus 是热安全模块对外发布的状态。
type ThermalStatus struct {
	RepeaterID   string            `json:"repeater_id"`
	Action       ThermalAction     `json:"action"`
	Determinate  bool              `json:"determinate"`
	Reason       string            `json:"reason,omitempty"`
	DerateFactor float64           `json:"derate_factor"` // 1 表示满功率，0 表示已停用
	Latched      bool              `json:"latched"`       // 已锁存停用，需人工/代次复位
	Reading      ThermalReading    `json:"reading"`
	Thresholds   ThermalThresholds `json:"thresholds"`
	UpdatedAt    time.Time         `json:"updated_at"`
}

// PowerIsolator 在热停用/恢复时驱动馈电回路隔离与重新送电。
type PowerIsolator interface {
	// IsolateForThermal 因热安全原因断开指定中继站的馈电。
	IsolateForThermal(repeaterID string, generation uint64, at time.Time) error
	// ReenergizeForThermal 热条件解除后恢复指定中继站的馈电。
	ReenergizeForThermal(repeaterID string, generation uint64, at time.Time) error
}

// ThermalEventSink 接收热安全状态迁移事件，用于事件生命周期与审计日志。
type ThermalEventSink interface {
	OnThermalShutdown(repeaterID string, generation uint64, at time.Time) error
	OnThermalResume(repeaterID string, generation uint64, at time.Time) error
}

// ThermalSafetyMonitor 按中继站汇总温/流/压与健康代次并给出安全动作。
type ThermalSafetyMonitor struct {
	mu sync.RWMutex

	thresholds ThermalThresholds
	incidents  IncidentSink
	power      PowerIsolator
	events     ThermalEventSink
	now        func() time.Time

	repeaters map[string]*Repeater
	statuses  map[string]ThermalStatus
	// 每个量分别记录最新样本，保证“汇总”而非单点判定。
	latest map[string]map[telemetry.Kind]telemetry.Sample
	// acknowledged 记录人工确认过的最高跳闸代次：该代次故障只能由复位解除。
	acknowledged map[string]uint64
	// resumeStreak 记录锁存后连续命中恢复包络的新鲜证据周期数。
	resumeStreak map[string]int
	// resumeBasis 记录上次计入恢复证据时各通道的序列号水位线；
	// 只有温/流/压三个通道都越过水位线，才算一个新的完整证据周期，
	// 避免同一批次内通道异步到达被重复计数。
	resumeBasis map[string]map[telemetry.Kind]uint64
}

// NewThermalSafetyMonitor 创建监控器。incidents/power/events 均可为空（仅计算不联动）。
func NewThermalSafetyMonitor(thresholds ThermalThresholds, incidents IncidentSink, power PowerIsolator, events ThermalEventSink, now func() time.Time) (*ThermalSafetyMonitor, error) {
	if err := thresholds.Validate(); err != nil {
		return nil, err
	}
	if now == nil {
		return nil, errors.New("clock is required")
	}
	return &ThermalSafetyMonitor{
		thresholds:   thresholds,
		incidents:    incidents,
		power:        power,
		events:       events,
		now:          now,
		repeaters:    make(map[string]*Repeater),
		statuses:     make(map[string]ThermalStatus),
		latest:       make(map[string]map[telemetry.Kind]telemetry.Sample),
		acknowledged: make(map[string]uint64),
		resumeStreak: make(map[string]int),
		resumeBasis:  make(map[string]map[telemetry.Kind]uint64),
	}, nil
}

// Register 注册受监控的中继站。
func (m *ThermalSafetyMonitor) Register(repeater *Repeater) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.repeaters[repeater.Snapshot().ID] = repeater
	if m.latest[repeater.Snapshot().ID] == nil {
		m.latest[repeater.Snapshot().ID] = make(map[telemetry.Kind]telemetry.Sample)
	}
}

// ApplySample 实现 telemetry.ProjectionSink，逐样本更新各量的最新值后汇总判定。
func (m *ThermalSafetyMonitor) ApplySample(ctx context.Context, sample telemetry.Sample) error {
	switch sample.Kind {
	case telemetry.KindTemperature, telemetry.KindCurrent, telemetry.KindVoltage:
	default:
		return nil
	}
	m.mu.Lock()
	if _, known := m.repeaters[sample.AssetID]; !known {
		m.mu.Unlock()
		return nil
	}
	// 代次落后的样本直接丢弃，不用旧代次数据做判定。
	if existing, ok := m.latest[sample.AssetID][sample.Kind]; ok &&
		(sample.Generation < existing.Generation ||
			(sample.Generation == existing.Generation && sample.Sequence <= existing.Sequence)) {
		m.mu.Unlock()
		return nil
	}
	m.latest[sample.AssetID][sample.Kind] = sample
	reading, ok := m.assembleLocked(sample.AssetID)
	m.mu.Unlock()
	if !ok {
		// 关键量尚未到齐，明确标记为不可判定而非默认可安全。
		return m.publishIndeterminate(sample.AssetID, reading, "telemetry channels not yet complete")
	}
	return m.evaluate(ctx, sample.AssetID, reading)
}

// assembleLocked 汇总各通道最新样本；任一类量缺失则不可判定。
func (m *ThermalSafetyMonitor) assembleLocked(assetID string) (ThermalReading, bool) {
	channels := m.latest[assetID]
	temp, tempOK := channels[telemetry.KindTemperature]
	current, currentOK := channels[telemetry.KindCurrent]
	voltage, voltageOK := channels[telemetry.KindVoltage]
	if !tempOK || !currentOK || !voltageOK {
		return ThermalReading{}, false
	}
	// 三个通道必须同属一个健康代次；跨代次拼样本没有安全意义。
	if temp.Generation != current.Generation || temp.Generation != voltage.Generation {
		return ThermalReading{Generation: maxGeneration(temp.Generation, current.Generation, voltage.Generation)}, false
	}
	return ThermalReading{
		TemperatureC: temp.Value,
		CurrentA:     current.Value,
		VoltageV:     voltage.Value,
		Generation:   temp.Generation,
		Sequence:     maxUint64(temp.Sequence, current.Sequence, voltage.Sequence),
		ObservedAt:   latestTime(temp.ObservedAt, current.ObservedAt, voltage.ObservedAt),
	}, true
}

// Refresh 使用当前时钟重新判定（用于过期检查的轮询）。
func (m *ThermalSafetyMonitor) Refresh(ctx context.Context, repeaterID string) error {
	m.mu.RLock()
	reading, ok := m.assembleLocked(repeaterID)
	m.mu.RUnlock()
	if !ok {
		return m.publishIndeterminate(repeaterID, reading, "telemetry channels incomplete or generation split")
	}
	return m.evaluate(ctx, repeaterID, reading)
}

// evaluate 执行阈值计算、迟滞锁存以及隔离/事件联动。调用时 m 不持锁。
func (m *ThermalSafetyMonitor) evaluate(ctx context.Context, repeaterID string, reading ThermalReading) error {
	_ = ctx
	at := m.now().UTC()

	m.mu.Lock()
	previous := m.statuses[repeaterID]
	repeaterObj := m.repeaters[repeaterID]

	// 1) 采样过期：最新汇总样本超出陈旧窗口，不允许据此做任何安全结论。
	age := at.Sub(reading.ObservedAt.UTC())
	stale := age > m.thresholds.StaleAfter || age < 0

	// 2) 健康代次落后：样本代次低于已锁存/已确认的跳闸代次时不可判定，
	//    防止旧代次（复位前）数据被当作恢复依据。
	lockedGeneration := m.acknowledged[repeaterID]
	if snap := repeaterObj.Snapshot(); snap.TripGeneration > lockedGeneration {
		lockedGeneration = snap.TripGeneration
	}
	generationStale := reading.Generation < lockedGeneration

	status := ThermalStatus{
		RepeaterID:   repeaterID,
		Determinate:  true,
		DerateFactor: 1,
		Reading:      reading,
		Thresholds:   m.thresholds,
		UpdatedAt:    at,
	}

	if stale || generationStale {
		status.Determinate = false
		status.Action = ActionIndeterminate
		status.Latched = previous.Latched
		// 不可判定时沿用上次的降额系数；若此前已停用则保持零输出，绝不自动恢复。
		status.DerateFactor = previous.DerateFactor
		switch {
		case generationStale:
			status.Reason = fmt.Sprintf("sample generation %d trails locked trip generation %d", reading.Generation, lockedGeneration)
		default:
			status.Reason = fmt.Sprintf("telemetry stale: newest sample age %s exceeds %s", age, m.thresholds.StaleAfter)
		}
		m.statuses[repeaterID] = status
		m.mu.Unlock()
		return nil
	}

	// 3) 阈值计算。先看硬性停用条件，再算线性降额。
	var resumed bool
	trip, tripReason := m.tripCheck(reading)
	if trip {
		status.Action = ActionShutdown
		status.DerateFactor = 0
		status.Latched = true
		status.Reason = tripReason
		m.resumeStreak[repeaterID] = 0
		// 以跳闸时各通道序列号作为恢复证据水位线：只有三个通道都越过该
		// 水位线，才算一个新的完整证据周期，防止通道异步到达重复计数。
		m.seedResumeBasisLocked(repeaterID)
	} else if previous.Latched {
		// 已锁存的故障不能被单个正常/降温样本清除：只有连续多个新鲜、代次
		// 不落后且全部落入恢复包络的“完整证据周期”，才允许解除。其余情况继续停用。
		if m.freshResumeCycleLocked(repeaterID) {
			if m.withinResumeEnvelope(reading) {
				m.resumeStreak[repeaterID]++
			} else {
				m.resumeStreak[repeaterID] = 0
			}
			m.advanceResumeBasisLocked(repeaterID)
		}
		if m.resumeStreak[repeaterID] >= m.thresholds.ResumeConfirmations {
			status.Action = ActionNormal
			status.DerateFactor = 1
			status.Latched = false
			status.Reason = fmt.Sprintf("thermal resume envelope confirmed %d times at generation %d",
				m.resumeStreak[repeaterID], reading.Generation)
			resumed = true
			m.resumeStreak[repeaterID] = 0
			delete(m.resumeBasis, repeaterID)
		} else {
			status.Action = ActionShutdown
			status.DerateFactor = 0
			status.Latched = true
			status.Reason = fmt.Sprintf("thermal shutdown latched; resume evidence %d/%d",
				m.resumeStreak[repeaterID], m.thresholds.ResumeConfirmations)
		}
	} else {
		factor, reason := m.derateFactor(reading)
		status.DerateFactor = factor
		status.Reason = reason
		if factor >= 1 {
			status.Action = ActionNormal
		} else {
			status.Action = ActionDerate
		}
	}

	// 4) 统一同步既有中继站健康投影：温度与序列号始终刷新，健康态跟随热动作；
	//    非跳闸态绝不清空别人已锁存的跳闸态（代次不足时跳过）。
	repeaterObj.update(func(current *Snapshot) {
		if reading.Generation < current.TripGeneration {
			return
		}
		current.TemperatureC = reading.TemperatureC
		current.LastSequence = maxUint64(current.LastSequence, reading.Sequence)
		current.UpdatedAt = at
		switch {
		case status.Latched:
			current.Health = HealthTripped
			current.TripGeneration = reading.Generation
		case resumed:
			current.Health = HealthNormal
		case reading.TemperatureC >= m.thresholds.DerateStartC:
			if current.Health != HealthTripped {
				current.Health = HealthHot
			}
		default:
			if current.Health == HealthHot {
				current.Health = HealthNormal
			}
		}
	})

	m.statuses[repeaterID] = status
	if status.Latched && reading.Generation > m.acknowledged[repeaterID] {
		m.acknowledged[repeaterID] = reading.Generation
	}
	m.mu.Unlock()

	// 5) 与中继站健康投影、馈电隔离、事件生命周期联动（锁外执行，避免回调重入）。
	if !previous.Latched && status.Latched && !resumed {
		return m.onShutdown(repeaterID, reading.Generation, at)
	}
	if resumed {
		return m.onResume(repeaterID, reading.Generation, at)
	}
	return nil
}

func (m *ThermalSafetyMonitor) onShutdown(repeaterID string, generation uint64, at time.Time) error {
	if m.incidents != nil {
		if err := m.incidents.OpenRepeaterIncident(repeaterID, generation); err != nil {
			return fmt.Errorf("open thermal incident: %w", err)
		}
	}
	if m.power != nil {
		if err := m.power.IsolateForThermal(repeaterID, generation, at); err != nil {
			return fmt.Errorf("isolate feeder for thermal trip: %w", err)
		}
	}
	if m.events != nil {
		if err := m.events.OnThermalShutdown(repeaterID, generation, at); err != nil {
			return fmt.Errorf("publish thermal shutdown event: %w", err)
		}
	}
	return nil
}

func (m *ThermalSafetyMonitor) onResume(repeaterID string, generation uint64, at time.Time) error {
	if m.incidents != nil {
		if err := m.incidents.ResolveRepeaterIncident(repeaterID, generation); err != nil {
			return fmt.Errorf("resolve thermal incident: %w", err)
		}
	}
	if m.power != nil {
		if err := m.power.ReenergizeForThermal(repeaterID, generation, at); err != nil {
			return fmt.Errorf("re-energize feeder after thermal resume: %w", err)
		}
	}
	if m.events != nil {
		if err := m.events.OnThermalResume(repeaterID, generation, at); err != nil {
			return fmt.Errorf("publish thermal resume event: %w", err)
		}
	}
	return nil
}

// tripCheck 返回是否命中任一硬停用条件。
func (m *ThermalSafetyMonitor) tripCheck(r ThermalReading) (bool, string) {
	switch {
	case r.TemperatureC >= m.thresholds.ShutdownAtC:
		return true, fmt.Sprintf("temperature %.2fC reaches shutdown threshold %.2fC", r.TemperatureC, m.thresholds.ShutdownAtC)
	case r.CurrentA >= m.thresholds.CurrentTripA:
		return true, fmt.Sprintf("current %.2fA reaches trip threshold %.2fA", r.CurrentA, m.thresholds.CurrentTripA)
	case r.VoltageV >= m.thresholds.MaxVoltageV:
		return true, fmt.Sprintf("voltage %.2fV exceeds max %.2fV", r.VoltageV, m.thresholds.MaxVoltageV)
	case r.VoltageV > 0 && r.VoltageV <= m.thresholds.MinVoltageV:
		return true, fmt.Sprintf("voltage %.2fV below minimum %.2fV", r.VoltageV, m.thresholds.MinVoltageV)
	}
	return false, ""
}

// withinResumeEnvelope 判断所有量是否进入恢复包络（迟滞门限比停用门限更保守）。
func (m *ThermalSafetyMonitor) withinResumeEnvelope(r ThermalReading) bool {
	return r.TemperatureC < m.thresholds.ResumeAtC &&
		r.CurrentA < m.thresholds.MaxCurrentA &&
		r.VoltageV > m.thresholds.MinVoltageV && r.VoltageV < m.thresholds.MaxVoltageV
}

// seedResumeBasisLocked 用各通道当前序列号建立恢复证据水位线。
func (m *ThermalSafetyMonitor) seedResumeBasisLocked(repeaterID string) {
	basis := make(map[telemetry.Kind]uint64, 3)
	for kind, sample := range m.latest[repeaterID] {
		basis[kind] = sample.Sequence
	}
	m.resumeBasis[repeaterID] = basis
}

// freshResumeCycleLocked 判断三通道是否都已越过上次证据水位线，
// 即是否构成一个新的完整遥测证据周期。
func (m *ThermalSafetyMonitor) freshResumeCycleLocked(repeaterID string) bool {
	basis := m.resumeBasis[repeaterID]
	if basis == nil {
		// 锁存在本监控器记账之外建立（如重启后读到既有跳闸代次）：
		// 先锚定当前水位线，本批次不计入恢复证据。
		m.seedResumeBasisLocked(repeaterID)
		return false
	}
	for _, kind := range []telemetry.Kind{telemetry.KindTemperature, telemetry.KindCurrent, telemetry.KindVoltage} {
		sample, ok := m.latest[repeaterID][kind]
		if !ok || sample.Sequence <= basis[kind] {
			return false
		}
	}
	return true
}

// advanceResumeBasisLocked 把水位线推进到各通道当前序列号。
func (m *ThermalSafetyMonitor) advanceResumeBasisLocked(repeaterID string) {
	basis := make(map[telemetry.Kind]uint64, 3)
	for kind, sample := range m.latest[repeaterID] {
		basis[kind] = sample.Sequence
	}
	m.resumeBasis[repeaterID] = basis
}

// derateFactor 在 [derate 起点, shutdown] 之间做线性降额；电流超额定但未跳闸同样降额。
func (m *ThermalSafetyMonitor) derateFactor(r ThermalReading) (float64, string) {
	factor := 1.0
	reason := ""
	if r.TemperatureC >= m.thresholds.DerateStartC {
		span := m.thresholds.ShutdownAtC - m.thresholds.DerateStartC
		f := (m.thresholds.ShutdownAtC - r.TemperatureC) / span
		factor = math.Min(factor, f)
		reason = fmt.Sprintf("temperature %.2fC in derate band", r.TemperatureC)
	}
	if r.CurrentA > m.thresholds.MaxCurrentA && r.CurrentA < m.thresholds.CurrentTripA {
		span := m.thresholds.CurrentTripA - m.thresholds.MaxCurrentA
		f := (m.thresholds.CurrentTripA - r.CurrentA) / span
		factor = math.Min(factor, f)
		if reason == "" {
			reason = fmt.Sprintf("current %.2fA above rating %.2fA", r.CurrentA, m.thresholds.MaxCurrentA)
		}
	}
	if factor < 0 {
		factor = 0
	}
	return factor, reason
}

// publishIndeterminate 在样本不全/过期/代次不符时发布不可判定状态。
func (m *ThermalSafetyMonitor) publishIndeterminate(repeaterID string, reading ThermalReading, reason string) error {
	m.mu.Lock()
	previous := m.statuses[repeaterID]
	status := ThermalStatus{
		RepeaterID:  repeaterID,
		Action:      ActionIndeterminate,
		Determinate: false,
		Reason:      reason,
		Reading:     reading,
		Thresholds:  m.thresholds,
		UpdatedAt:   m.now().UTC(),
	}
	// 已锁存停用时进入不可判定：保持停用输出，不得借“不可判定”之名自动恢复。
	status.Latched = previous.Latched
	status.DerateFactor = previous.DerateFactor
	if status.Latched {
		status.DerateFactor = 0
	}
	m.statuses[repeaterID] = status
	m.mu.Unlock()
	return nil
}

// Status 返回中继站当前热安全状态；从未收到过数据时为不可判定。
func (m *ThermalSafetyMonitor) Status(repeaterID string) ThermalStatus {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if status, ok := m.statuses[repeaterID]; ok {
		return status
	}
	return ThermalStatus{
		RepeaterID:   repeaterID,
		Action:       ActionIndeterminate,
		Determinate:  false,
		Reason:       "no thermal telemetry received",
		DerateFactor: 1,
		Thresholds:   m.thresholds,
		UpdatedAt:    m.now().UTC(),
	}
}

// Reset 在人工复位（代次推进）后清除锁存。只有新代次才能确认旧故障。
// 若此前处于锁存停用，复位会联动恢复送电并解决热事件。
func (m *ThermalSafetyMonitor) Reset(repeaterID string, generation uint64) error {
	m.mu.Lock()
	if generation < m.acknowledged[repeaterID] {
		m.mu.Unlock()
		return nil
	}
	m.acknowledged[repeaterID] = generation
	previous := m.statuses[repeaterID]
	if previous.Latched {
		repeaterObj := m.repeaters[repeaterID]
		status := previous
		status.Latched = false
		status.Action = ActionIndeterminate
		status.Determinate = false
		status.DerateFactor = 1
		status.Reason = "thermal latch reset; awaiting fresh telemetry"
		status.UpdatedAt = m.now().UTC()
		m.statuses[repeaterID] = status
		m.resumeStreak[repeaterID] = 0
		delete(m.resumeBasis, repeaterID)
		at := status.UpdatedAt
		if repeaterObj != nil {
			repeaterObj.update(func(current *Snapshot) {
				if generation >= current.TripGeneration {
					current.Health = HealthNormal
					current.TripGeneration = generation
					current.UpdatedAt = at
				}
			})
		}
		m.mu.Unlock()
		// 人工复位视为恢复确认：解决事件并重新送电。
		return m.onResume(repeaterID, generation, at)
	}
	m.mu.Unlock()
	return nil
}

func maxUint64(values ...uint64) uint64 {
	var best uint64
	for _, value := range values {
		if value > best {
			best = value
		}
	}
	return best
}

func maxGeneration(values ...uint64) uint64 {
	return maxUint64(values...)
}

func latestTime(times ...time.Time) time.Time {
	var best time.Time
	for _, value := range times {
		if value.After(best) {
			best = value
		}
	}
	return best
}
