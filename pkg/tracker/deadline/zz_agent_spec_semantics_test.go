package deadline

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

func agentSpecClock() *ManualClock {
	return NewManualClock(time.Date(2026, 11, 25, 0, 0, 0, 0, time.UTC))
}

func requireAgentSpecKind(t *testing.T, err error, want Kind) *Error {
	t.Helper()
	if err == nil {
		t.Fatalf("expected %s deadline error, got nil", want)
	}
	var deadlineErr *Error
	if !errors.As(err, &deadlineErr) {
		t.Fatalf("expected errors.As to *deadline.Error, got %T: %v", err, err)
	}
	switch want {
	case KindGlobal:
		var typed *GlobalDeadlineError
		if !errors.As(err, &typed) {
			t.Fatalf("expected errors.As to *GlobalDeadlineError: %v", err)
		}
	case KindPerResource:
		var typed *PerResourceDeadlineError
		if !errors.As(err, &typed) {
			t.Fatalf("expected errors.As to *PerResourceDeadlineError: %v", err)
		}
	case KindProgress:
		var typed *ProgressDeadlineError
		if !errors.As(err, &typed) {
			t.Fatalf("expected errors.As to *ProgressDeadlineError: %v", err)
		}
	}
	if deadlineErr.Kind != want {
		t.Fatalf("expected kind %q, got %q (err=%v)", want, deadlineErr.Kind, err)
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("deadline error must remain compatible with context.DeadlineExceeded: %v", err)
	}
	return deadlineErr
}

func TestAgentSpecMonitor_OnlyGlobalDeadline(t *testing.T) {
	clock := agentSpecClock()
	m, err := NewMonitorAt("deploy/app", Config{
		GlobalTimeout:          10 * time.Minute,
		DisableProgressTimeout: true,
		Clock:                  clock,
	}, clock.Now())
	if err != nil {
		t.Fatal(err)
	}

	clock.Advance(9 * time.Minute)
	if err := m.Check(); err != nil {
		t.Fatalf("deadline fired early: %v", err)
	}
	clock.Advance(time.Minute)
	err = m.Check()
	deadlineErr := requireAgentSpecKind(t, err, KindGlobal)
	if deadlineErr.Timeout != 10*time.Minute {
		t.Fatalf("unexpected configured timeout: %s", deadlineErr.Timeout)
	}
	if !errors.Is(err, ErrGlobalDeadline) {
		t.Fatal("errors.Is must match ErrGlobalDeadline")
	}
}

func TestAgentSpecMonitor_PerResourceDeadlineShorterThanGlobal(t *testing.T) {
	clock := agentSpecClock()
	m, err := NewMonitorAt("job/migrate", Config{
		GlobalTimeout:          time.Hour,
		PerResourceTimeout:     5 * time.Minute,
		DisableProgressTimeout: true,
		Clock:                  clock,
	}, clock.Now())
	if err != nil {
		t.Fatal(err)
	}

	clock.Advance(5 * time.Minute)
	err = m.Check()
	deadlineErr := requireAgentSpecKind(t, err, KindPerResource)
	if !errors.Is(err, ErrPerResourceDeadline) {
		t.Fatal("errors.Is must match ErrPerResourceDeadline")
	}
	if deadlineErr.ResourceID != "job/migrate" {
		t.Fatalf("unexpected resource id: %q", deadlineErr.ResourceID)
	}
}

func TestAgentSpecConfig_OverrideLongerThanGlobalIsClampedAndWarns(t *testing.T) {
	var warnings []string
	cfg := Config{
		GlobalTimeout:          10 * time.Minute,
		PerResourceTimeout:     3 * time.Minute,
		DisableProgressTimeout: true,
		ResourceTimeouts:       map[string]time.Duration{"job/long": time.Hour},
		Warnf:                  func(format string, args ...any) { warnings = append(warnings, format) },
	}

	resolved, err := cfg.Resolve("job/long")
	if err != nil {
		t.Fatal(err)
	}
	if !resolved.ClampedToGlobal {
		t.Fatal("expected override to be clamped")
	}
	if resolved.PerResourceTimeout != 10*time.Minute {
		t.Fatalf("expected global clamp, got %s", resolved.PerResourceTimeout)
	}
	if len(resolved.Warnings) != 1 || len(warnings) != 1 {
		t.Fatalf("expected one warning, got resolved=%v callback=%v", resolved.Warnings, warnings)
	}

	clock := agentSpecClock()
	resolved.ResolvedConfig.Clock = clock
	m := newMonitorAt("job/long", resolved, clock.Now())
	clock.Advance(10 * time.Minute)
	requireAgentSpecKind(t, m.Check(), KindGlobal)
}

func TestAgentSpecMonitor_ProgressSilenceFires(t *testing.T) {
	clock := agentSpecClock()
	m, err := NewMonitorAt("deploy/app", Config{
		ProgressTimeout: 4 * time.Minute,
		Clock:           clock,
	}, clock.Now())
	if err != nil {
		t.Fatal(err)
	}
	m.SetSummary(StateSummary{
		Replicas:   &IndicatorCount{Name: IndicatorReplicas, Value: 3, Target: 10},
		WaitingFor: []string{"available 3->10"},
	})

	clock.Advance(4 * time.Minute)
	err = m.Check()
	deadlineErr := requireAgentSpecKind(t, err, KindProgress)
	if !errors.Is(err, ErrProgressDeadline) {
		t.Fatal("errors.Is must match ErrProgressDeadline")
	}
	if deadlineErr.SinceLastProgress != 4*time.Minute {
		t.Fatalf("unexpected since-progress: %s", deadlineErr.SinceLastProgress)
	}
	if !strings.Contains(deadlineErr.StateSummary, "replicas=3/10") ||
		!strings.Contains(deadlineErr.StateSummary, "available 3->10") {
		t.Fatalf("state summary missing replica/wait details: %s", deadlineErr.StateSummary)
	}
}

func TestAgentSpecMonitor_NoiseEventsDoNotExtendProgressDeadline(t *testing.T) {
	clock := agentSpecClock()
	m, err := NewMonitorAt("deploy/app", Config{ProgressTimeout: 4 * time.Minute, Clock: clock}, clock.Now())
	if err != nil {
		t.Fatal(err)
	}

	clock.Advance(3 * time.Minute)
	if m.ObserveEvent() {
		t.Fatal("ordinary event must not count as progress in default mode")
	}
	clock.Advance(time.Minute)
	requireAgentSpecKind(t, m.Check(), KindProgress)
}

func TestAgentSpecMonitor_ReplicaTargetChangeResetsProgressOnly(t *testing.T) {
	clock := agentSpecClock()
	m, err := NewMonitorAt("deploy/app", Config{
		GlobalTimeout:      20 * time.Minute,
		PerResourceTimeout: 20 * time.Minute,
		ProgressTimeout:    5 * time.Minute,
		Clock:              clock,
	}, clock.Now())
	if err != nil {
		t.Fatal(err)
	}
	m.UpdateTarget(Target{Replicas: 3, Generation: 7})

	clock.Advance(3 * time.Minute)
	if !m.UpdateTarget(Target{Replicas: 10, Generation: 7}) {
		t.Fatal("replica target change must reset progress")
	}
	if m.ResetCount() != 1 {
		t.Fatalf("expected one target reset, got %d", m.ResetCount())
	}

	clock.Advance(2 * time.Minute) // original five-minute window would expire now
	if err := m.Check(); err != nil {
		t.Fatalf("progress should have been reset: %v", err)
	}
	clock.Advance(3 * time.Minute) // eight minutes after start, five after target change
	requireAgentSpecKind(t, m.Check(), KindProgress)
}

func TestAgentSpecMonitor_GenerationChangeResetsProgress(t *testing.T) {
	clock := agentSpecClock()
	m, err := NewMonitorAt("deploy/app", Config{ProgressTimeout: 5 * time.Minute, Clock: clock}, clock.Now())
	if err != nil {
		t.Fatal(err)
	}
	m.UpdateTarget(Target{Replicas: 10, Generation: 7})

	clock.Advance(3 * time.Minute)
	if !m.UpdateTarget(Target{Replicas: 10, Generation: 8}) {
		t.Fatal("generation change must reset progress even with unchanged replicas")
	}
	clock.Advance(4 * time.Minute)
	if err := m.Check(); err != nil {
		t.Fatalf("reset window should still be active: %v", err)
	}
	clock.Advance(time.Minute)
	requireAgentSpecKind(t, m.Check(), KindProgress)
}

func TestAgentSpecMonitor_ResetCannotCrossHardDeadline(t *testing.T) {
	clock := agentSpecClock()
	m, err := NewMonitorAt("deploy/app", Config{
		GlobalTimeout:   10 * time.Minute,
		ProgressTimeout: 5 * time.Minute,
		Clock:           clock,
	}, clock.Now())
	if err != nil {
		t.Fatal(err)
	}
	m.UpdateTarget(Target{Replicas: 3})

	clock.Advance(9 * time.Minute)
	m.UpdateTarget(Target{Replicas: 10})

	// The fresh progress window would end at t=14, but the global deadline
	// remains t=10 and must win.
	clock.Advance(time.Minute)
	requireAgentSpecKind(t, m.Check(), KindGlobal)
}

func TestAgentSpecMonitor_MultipleResetsAreRecorded(t *testing.T) {
	clock := agentSpecClock()
	var logs []string
	m, err := NewMonitorAt("deploy/app", Config{
		GlobalTimeout:   30 * time.Minute,
		ProgressTimeout: 5 * time.Minute,
		Clock:           clock,
		Logf:            func(format string, args ...any) { logs = append(logs, format) },
	}, clock.Now())
	if err != nil {
		t.Fatal(err)
	}

	for _, replicas := range []int64{3, 5, 8, 10} {
		m.UpdateTarget(Target{Replicas: replicas})
		clock.Advance(2 * time.Minute)
	}
	if m.ResetCount() != 3 {
		t.Fatalf("expected three changes after baseline, got %d", m.ResetCount())
	}
	if len(logs) != 3 {
		t.Fatalf("expected one log per reset, got %d logs: %v", len(logs), logs)
	}

	clock.Advance(2 * time.Minute)
	if err := m.Check(); err != nil {
		t.Fatalf("last reset should still be inside progress window: %v", err)
	}
	clock.Advance(time.Minute)
	requireAgentSpecKind(t, m.Check(), KindProgress)
}

func TestAgentSpecMonitor_LegacyAnyEventSemantics(t *testing.T) {
	clock := agentSpecClock()
	m, err := NewMonitorAt("generic/cr", Config{
		ProgressTimeout:            5 * time.Minute,
		LegacyEventActivityTimeout: true,
		Clock:                      clock,
	}, clock.Now())
	if err != nil {
		t.Fatal(err)
	}

	clock.Advance(4 * time.Minute)
	if !m.ObserveEvent() {
		t.Fatal("legacy mode must treat any event as activity")
	}
	clock.Advance(4 * time.Minute)
	if err := m.Check(); err != nil {
		t.Fatalf("event should have extended old activity window: %v", err)
	}
	clock.Advance(time.Minute)
	requireAgentSpecKind(t, m.Check(), KindProgress)
}

func TestAgentSpecMonitor_IndicatorMovementAndPodPhaseCountAsProgress(t *testing.T) {
	clock := agentSpecClock()
	m, err := NewMonitorAt("deploy/app", Config{ProgressTimeout: time.Minute, Clock: clock}, clock.Now())
	if err != nil {
		t.Fatal(err)
	}

	// First sample establishes the baseline and does not itself need to reset.
	m.ObserveIndicatorAt(clock.Now(), IndicatorAvailable, 1, 3)
	clock.Advance(30 * time.Second)
	if !m.ObserveIndicatorAt(clock.Now(), IndicatorAvailable, 2, 3) {
		t.Fatal("available replicas moving toward target must be progress")
	}
	clock.Advance(30 * time.Second)
	if err := m.Check(); err != nil {
		t.Fatalf("indicator progress should reset deadline: %v", err)
	}

	clock.Advance(30 * time.Second)
	if !m.ObservePodPhaseAt(clock.Now(), "app-new", "Pending") {
		t.Fatal("new pod must count as progress")
	}
	clock.Advance(30 * time.Second)
	if !m.ObservePodPhaseAt(clock.Now(), "app-new", "Running") {
		t.Fatal("pod phase transition must count as progress")
	}
}

func TestAgentSpecConfig_NegativeDurationsAreErrorsAndZeroIsUnlimited(t *testing.T) {
	for _, cfg := range []Config{
		{GlobalTimeout: -time.Second},
		{PerResourceTimeout: -time.Second},
		{ProgressTimeout: -time.Second},
		{ResourceTimeouts: map[string]time.Duration{"x": -time.Second}},
	} {
		if _, err := cfg.Normalize(); err == nil || !IsConfigError(err) {
			t.Fatalf("expected ConfigError for %#v, got %v", cfg, err)
		}
	}

	resolved, err := Config{DisableProgressTimeout: true}.Resolve("x")
	if err != nil {
		t.Fatal(err)
	}
	if resolved.GlobalTimeout != 0 || resolved.PerResourceTimeout != 0 || resolved.ProgressTimeout != 0 {
		t.Fatalf("zero durations should mean unlimited after explicit progress disable: %+v", resolved)
	}
}
