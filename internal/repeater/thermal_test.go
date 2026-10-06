package repeater

import (
	"context"
	"testing"
	"time"

	"github.com/wyw14/cry-103/internal/telemetry"
)

type fakeClock struct{ t time.Time }

func (c *fakeClock) nowFn() time.Time { return c.t }

type recordingSink struct {
	opened   []uint64
	resolved []uint64
	active   map[string]uint64
}

func newRecordingSink() *recordingSink {
	return &recordingSink{active: make(map[string]uint64)}
}

func (s *recordingSink) OpenRepeaterIncident(id string, generation uint64) error {
	s.opened = append(s.opened, generation)
	s.active[id] = generation
	return nil
}

func (s *recordingSink) ResolveRepeaterIncident(id string, generation uint64) error {
	s.resolved = append(s.resolved, generation)
	delete(s.active, id)
	return nil
}

func (s *recordingSink) Active(id string) (uint64, bool) {
	g, ok := s.active[id]
	return g, ok
}

func (s *recordingSink) LastResolution(id string) uint64 {
	if len(s.resolved) == 0 {
		return 0
	}
	return s.resolved[len(s.resolved)-1]
}

type fakePower struct {
	isolated    []uint64
	reenergized []uint64
	failIsolate bool
}

func (p *fakePower) IsolateForThermal(_ string, generation uint64, _ time.Time) error {
	p.isolated = append(p.isolated, generation)
	return nil
}

func (p *fakePower) ReenergizeForThermal(_ string, generation uint64, _ time.Time) error {
	p.reenergized = append(p.reenergized, generation)
	return nil
}

type fakeEvents struct {
	shutdown []uint64
	resume   []uint64
}

func (e *fakeEvents) OnThermalShutdown(_ string, generation uint64, _ time.Time) error {
	e.shutdown = append(e.shutdown, generation)
	return nil
}

func (e *fakeEvents) OnThermalResume(_ string, generation uint64, _ time.Time) error {
	e.resume = append(e.resume, generation)
	return nil
}

func newTestMonitor(t *testing.T, thresholds ThermalThresholds, clock *fakeClock) (*ThermalSafetyMonitor, *Repeater, *recordingSink, *fakePower, *fakeEvents) {
	t.Helper()
	incidents := newRecordingSink()
	power := &fakePower{}
	events := &fakeEvents{}
	monitor, err := NewThermalSafetyMonitor(thresholds, incidents, power, events, clock.nowFn)
	if err != nil {
		t.Fatal(err)
	}
	r, err := New("R07", clock.t)
	if err != nil {
		t.Fatal(err)
	}
	monitor.Register(r)
	return monitor, r, incidents, power, events
}

func pushBatch(t *testing.T, m *ThermalSafetyMonitor, gen uint64, seq uint64, tempC, currentA, voltageV float64, at time.Time) {
	t.Helper()
	ctx := context.Background()
	samples := []telemetry.Sample{
		{ID: "t", AssetID: "R07", Kind: telemetry.KindTemperature, Value: tempC, Sequence: seq, Generation: gen, ObservedAt: at},
		{ID: "c", AssetID: "R07", Kind: telemetry.KindCurrent, Value: currentA, Sequence: seq, Generation: gen, ObservedAt: at},
		{ID: "v", AssetID: "R07", Kind: telemetry.KindVoltage, Value: voltageV, Sequence: seq, Generation: gen, ObservedAt: at},
	}
	for _, sample := range samples {
		if err := m.ApplySample(ctx, sample); err != nil {
			t.Fatalf("apply sample: %v", err)
		}
	}
}

func TestThermalIndeterminateWithoutCompleteChannels(t *testing.T) {
	clock := &fakeClock{t: time.Date(2026, 10, 5, 0, 0, 0, 0, time.UTC)}
	monitor, _, _, _, _ := newTestMonitor(t, DefaultThermalThresholds(), clock)

	if status := monitor.Status("R07"); status.Determinate || status.Action != ActionIndeterminate {
		t.Fatalf("expected indeterminate before data, got %+v", status)
	}

	ctx := context.Background()
	_ = monitor.ApplySample(ctx, telemetry.Sample{
		ID: "t", AssetID: "R07", Kind: telemetry.KindTemperature, Value: 40,
		Sequence: 1, Generation: 1, ObservedAt: clock.t,
	})
	status := monitor.Status("R07")
	if status.Determinate || status.Action != ActionIndeterminate {
		t.Fatalf("expected indeterminate with only temperature, got %+v", status)
	}
}

func TestThermalNormalAndDerate(t *testing.T) {
	clock := &fakeClock{t: time.Date(2026, 10, 5, 0, 0, 0, 0, time.UTC)}
	thresholds := DefaultThermalThresholds()
	monitor, _, _, _, _ := newTestMonitor(t, thresholds, clock)

	// 50°C：正常，满功率。
	pushBatch(t, monitor, 1, 1, 50, 8, 450, clock.t)
	status := monitor.Status("R07")
	if !status.Determinate || status.Action != ActionNormal || status.DerateFactor != 1 {
		t.Fatalf("expected normal full power, got %+v", status)
	}

	// 77.5°C 位于 [70,85] 中点：降额系数 0.5。
	pushBatch(t, monitor, 1, 2, 77.5, 8, 450, clock.t)
	status = monitor.Status("R07")
	if status.Action != ActionDerate || status.Determinate != true {
		t.Fatalf("expected derate, got %+v", status)
	}
	if diff := status.DerateFactor - 0.5; diff > 0.001 || diff < -0.001 {
		t.Fatalf("expected derate factor ~0.5, got %v", status.DerateFactor)
	}
}

func TestThermalShutdownLatchesAndDrivesSinks(t *testing.T) {
	clock := &fakeClock{t: time.Date(2026, 10, 5, 0, 0, 0, 0, time.UTC)}
	thresholds := DefaultThermalThresholds()
	monitor, r, incidents, power, events := newTestMonitor(t, thresholds, clock)

	pushBatch(t, monitor, 1, 1, 85, 8, 450, clock.t) // 85 == shutdown
	status := monitor.Status("R07")
	if status.Action != ActionShutdown || !status.Latched || status.DerateFactor != 0 {
		t.Fatalf("expected latched shutdown, got %+v", status)
	}
	if len(incidents.opened) != 1 || incidents.opened[0] != 1 {
		t.Fatalf("expected incident opened at gen 1, got %+v", incidents.opened)
	}
	if len(power.isolated) != 1 || power.isolated[0] != 1 {
		t.Fatalf("expected power isolated at gen 1, got %+v", power.isolated)
	}
	if len(events.shutdown) != 1 {
		t.Fatalf("expected one shutdown event, got %+v", events.shutdown)
	}
	if r.Snapshot().Health != HealthTripped {
		t.Fatalf("expected repeater health tripped, got %s", r.Snapshot().Health)
	}

	// 单点降温（只需一次判定）不能清除故障：默认需连续 2 次恢复证据。
	pushBatch(t, monitor, 1, 2, 40, 8, 450, clock.t)
	status = monitor.Status("R07")
	if !status.Latched || status.Action != ActionShutdown {
		t.Fatalf("single cool sample must not clear latch, got %+v", status)
	}
	if len(power.reenergized) != 0 {
		t.Fatalf("must not re-energize on single sample, got %+v", power.reenergized)
	}

	// 第二次连续恢复证据后才解除。
	pushBatch(t, monitor, 1, 3, 40, 8, 450, clock.t)
	status = monitor.Status("R07")
	if status.Latched || status.Action != ActionNormal || status.DerateFactor != 1 {
		t.Fatalf("expected resume after two confirmations, got %+v", status)
	}
	if len(power.reenergized) != 1 || len(incidents.resolved) != 1 {
		t.Fatalf("expected re-energize and resolve, got power=%v incidents=%v", power.reenergized, incidents.resolved)
	}
	if len(events.resume) != 1 {
		t.Fatalf("expected one resume event, got %+v", events.resume)
	}
	if r.Snapshot().Health != HealthNormal {
		t.Fatalf("expected repeater health normal, got %s", r.Snapshot().Health)
	}
}

func TestThermalResumeBreaksOnInterveningHotSample(t *testing.T) {
	clock := &fakeClock{t: time.Date(2026, 10, 5, 0, 0, 0, 0, time.UTC)}
	monitor, _, _, power, _ := newTestMonitor(t, DefaultThermalThresholds(), clock)

	pushBatch(t, monitor, 1, 1, 90, 8, 450, clock.t) // trip
	pushBatch(t, monitor, 1, 2, 40, 8, 450, clock.t) // resume evidence 1/2
	pushBatch(t, monitor, 1, 3, 80, 8, 450, clock.t) // back above resume → streak resets
	if monitor.Status("R07").Action != ActionShutdown {
		t.Fatalf("expected still shutdown after streak break")
	}
	pushBatch(t, monitor, 1, 4, 40, 8, 450, clock.t) // evidence 1/2 again
	if monitor.Status("R07").Action != ActionShutdown {
		t.Fatalf("expected still shutdown with only one fresh confirmation")
	}
	pushBatch(t, monitor, 1, 5, 40, 8, 450, clock.t) // evidence 2/2
	if monitor.Status("R07").Action != ActionNormal {
		t.Fatalf("expected normal after two consecutive confirmations")
	}
	if len(power.reenergized) != 1 {
		t.Fatalf("expected exactly one re-energize, got %v", power.reenergized)
	}
}

func TestThermalStaleSamplesAreIndeterminateAndHoldLatch(t *testing.T) {
	base := time.Date(2026, 10, 5, 0, 0, 0, 0, time.UTC)
	clock := &fakeClock{t: base}
	monitor, _, _, power, _ := newTestMonitor(t, DefaultThermalThresholds(), clock)

	pushBatch(t, monitor, 1, 1, 90, 8, 450, base)
	// 时间前进超过陈旧窗口，用 Refresh 重新评估。
	clock.t = base.Add(30 * time.Second)
	if err := monitor.Refresh(context.Background(), "R07"); err != nil {
		t.Fatal(err)
	}
	status := monitor.Status("R07")
	if status.Determinate || status.Action != ActionIndeterminate {
		t.Fatalf("expected indeterminate when stale, got %+v", status)
	}
	if !status.Latched || status.DerateFactor != 0 {
		t.Fatalf("stale data must hold the shutdown latch at zero output, got %+v", status)
	}
	if len(power.reenergized) != 0 {
		t.Fatalf("stale data must not re-energize, got %v", power.reenergized)
	}
}

func TestThermalGenerationLagIsIndeterminate(t *testing.T) {
	base := time.Date(2026, 10, 5, 0, 0, 0, 0, time.UTC)
	clock := &fakeClock{t: base}
	monitor, _, _, power, _ := newTestMonitor(t, DefaultThermalThresholds(), clock)

	// 代次 3 跳闸并锁存。
	pushBatch(t, monitor, 3, 1, 90, 8, 450, base)
	// 外部把健康代次推进到 5（人工/上游复位），但最新遥测仍停留在代次 3。
	if err := monitor.Reset("R07", 5); err != nil {
		t.Fatal(err)
	}
	reenergizedAfterReset := len(power.reenergized)
	if err := monitor.Refresh(context.Background(), "R07"); err != nil {
		t.Fatal(err)
	}
	status := monitor.Status("R07")
	if status.Determinate || status.Action != ActionIndeterminate {
		t.Fatalf("telemetry generation trailing health generation must be indeterminate, got %+v", status)
	}
	if !contains(status.Reason, "generation") {
		t.Fatalf("expected generation-lag reason, got %q", status.Reason)
	}
	// 不可判定期间不得再次触发隔离，也不得擅自改变馈电输出。
	if len(power.isolated) != 1 || len(power.reenergized) != reenergizedAfterReset {
		t.Fatalf("indeterminate state must not drive power actions, isolated=%v reenergized=%v",
			power.isolated, power.reenergized)
	}

	// 落后代次的旧样本到达时应被忽略，不能把状态改写成“正常”。
	pushBatch(t, monitor, 1, 9, 40, 8, 450, base)
	if status := monitor.Status("R07"); status.Determinate || status.Action != ActionIndeterminate {
		t.Fatalf("stale-generation samples must not downgrade to normal, got %+v", status)
	}
}

func contains(haystack, needle string) bool {
	return len(haystack) >= len(needle) && indexOf(haystack, needle) >= 0
}

func indexOf(haystack, needle string) int {
	for i := 0; i+len(needle) <= len(haystack); i++ {
		if haystack[i:i+len(needle)] == needle {
			return i
		}
	}
	return -1
}

func TestThermalMismatchedChannelGenerationsAreIndeterminate(t *testing.T) {
	base := time.Date(2026, 10, 5, 0, 0, 0, 0, time.UTC)
	clock := &fakeClock{t: base}
	monitor, _, _, _, _ := newTestMonitor(t, DefaultThermalThresholds(), clock)

	ctx := context.Background()
	mixed := []telemetry.Sample{
		{ID: "t", AssetID: "R07", Kind: telemetry.KindTemperature, Value: 40, Sequence: 1, Generation: 2, ObservedAt: base},
		{ID: "c", AssetID: "R07", Kind: telemetry.KindCurrent, Value: 8, Sequence: 1, Generation: 2, ObservedAt: base},
		{ID: "v", AssetID: "R07", Kind: telemetry.KindVoltage, Value: 450, Sequence: 1, Generation: 1, ObservedAt: base},
	}
	for _, sample := range mixed {
		if err := monitor.ApplySample(ctx, sample); err != nil {
			t.Fatal(err)
		}
	}
	if status := monitor.Status("R07"); status.Determinate {
		t.Fatalf("split generation channels must be indeterminate, got %+v", status)
	}
}

func TestThermalElectricalTripConditions(t *testing.T) {
	thresholds := DefaultThermalThresholds()
	cases := []struct {
		name             string
		temp, curr, volt float64
	}{
		{"overcurrent", 40, 12, 450},
		{"overvoltage", 40, 8, 600},
		{"undervoltage", 40, 8, 300},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			clock := &fakeClock{t: time.Date(2026, 10, 5, 0, 0, 0, 0, time.UTC)}
			monitor, _, incidents, _, _ := newTestMonitor(t, thresholds, clock)
			pushBatch(t, monitor, 1, 1, tc.temp, tc.curr, tc.volt, clock.t)
			if monitor.Status("R07").Action != ActionShutdown {
				t.Fatalf("expected shutdown for %s", tc.name)
			}
			if len(incidents.opened) != 1 {
				t.Fatalf("expected incident for %s", tc.name)
			}
		})
	}
}

func TestThermalResetReenergizes(t *testing.T) {
	base := time.Date(2026, 10, 5, 0, 0, 0, 0, time.UTC)
	clock := &fakeClock{t: base}
	monitor, _, incidents, power, events := newTestMonitor(t, DefaultThermalThresholds(), clock)

	pushBatch(t, monitor, 1, 1, 90, 8, 450, base)
	if err := monitor.Reset("R07", 2); err != nil {
		t.Fatal(err)
	}
	status := monitor.Status("R07")
	if status.Latched || status.Determinate {
		t.Fatalf("reset should clear latch and await fresh data, got %+v", status)
	}
	if len(power.reenergized) != 1 || len(incidents.resolved) != 1 || len(events.resume) != 1 {
		t.Fatalf("reset must re-energize, resolve incident and emit event")
	}

	// 复位后新代次正常数据应恢复正常判定。
	pushBatch(t, monitor, 2, 2, 40, 8, 450, base)
	if status := monitor.Status("R07"); !status.Determinate || status.Action != ActionNormal {
		t.Fatalf("expected normal after reset + fresh data, got %+v", status)
	}
}

func TestThermalThresholdsValidation(t *testing.T) {
	bad := DefaultThermalThresholds()
	bad.ResumeAtC = bad.ShutdownAtC
	if _, err := NewThermalSafetyMonitor(bad, nil, nil, nil, time.Now); err == nil {
		t.Fatal("expected validation error for resume >= shutdown")
	}
	good := DefaultThermalThresholds()
	if _, err := NewThermalSafetyMonitor(good, nil, nil, nil, time.Now); err != nil {
		t.Fatalf("default thresholds should be valid: %v", err)
	}
}
