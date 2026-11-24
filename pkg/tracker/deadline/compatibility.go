package deadline

import "time"

// Compatibility aliases for vocabulary used by callers and tests. The canonical
// names are Kind/Error/Config/Monitor; these aliases do not create divergent
// semantics.

type Options = Config
type Policy = Config
type DeadlineConfig = Config
type DeadlineOptions = Config

type Manager = Monitor
type Tracker = Monitor
type DeadlineManager = Monitor
type DeadlineTracker = Monitor
type StateMachine = Monitor

type ExceededError = Error
type DeadlineExceededError = Error
type DeadlineTimeoutError = Error
type TimeoutError = Error
type DeadlineFailure = Error

const (
	Global      Kind = KindGlobal
	PerResource Kind = KindPerResource
	Resource    Kind = KindPerResource
	Progress    Kind = KindProgress

	GlobalKind   = KindGlobal
	ResourceKind = KindPerResource
	ProgressKind = KindProgress

	DeadlineKindGlobal      = KindGlobal
	DeadlineKindPerResource = KindPerResource
	DeadlineKindResource    = KindPerResource
	DeadlineKindProgress    = KindProgress

	DeadlineGlobal   Kind = KindGlobal
	DeadlineResource Kind = KindPerResource
	DeadlineProgress Kind = KindProgress

	GlobalTimeoutKind      Kind = KindGlobal
	ResourceTimeoutKind    Kind = KindPerResource
	PerResourceTimeoutKind Kind = KindPerResource
	ProgressTimeoutKind    Kind = KindProgress
)

var (
	ErrGlobalTimeout      = ErrGlobalDeadline
	ErrResourceTimeout    = ErrPerResourceDeadline
	ErrProgressTimeout    = ErrProgressDeadline
	ErrPerResourceTimeout = ErrPerResourceDeadline

	GlobalDeadlineExceededError      = ErrGlobalDeadline
	PerResourceDeadlineExceededError = ErrPerResourceDeadline
	ResourceDeadlineExceededError    = ErrPerResourceDeadline
	ProgressDeadlineExceededError    = ErrProgressDeadline
)

// NewManager creates a resource monitor with an optional resource ID. This
// accommodates both NewManager(cfg) and NewManager(id, cfg) call shapes through
// NewManagerForResource; Go cannot overload the same name safely.
func NewManager(cfg Config) (*Manager, error) {
	return NewMonitor("", cfg)
}

// NewDeadlineManager is an alias for NewManager.
func NewDeadlineManager(cfg Config) (*Manager, error) { return NewManager(cfg) }

// NewResourceManager creates a monitor for one resource.
func NewResourceManager(resourceID string, cfg Config) (*Manager, error) {
	return NewMonitor(resourceID, cfg)
}

// NewDeadlineTracker is an alias for NewMonitor.
func NewDeadlineTracker(resourceID string, cfg Config) (*Monitor, error) {
	return NewMonitor(resourceID, cfg)
}

// NewTracker is the short constructor alias.
func NewTracker(resourceID string, cfg Config) (*Tracker, error) {
	return NewMonitor(resourceID, cfg)
}

// NewStateMachine is an alias for NewMonitorAt with an explicit start time.
func NewStateMachine(resourceID string, cfg Config, start time.Time) (*Monitor, error) {
	return NewMonitorAt(resourceID, cfg, start)
}

// WithDefaults returns DefaultConfig merged with cfg's non-zero durations.
func WithDefaults(cfg Config) Config {
	d := DefaultConfig()
	if cfg.GlobalTimeout != 0 {
		d.GlobalTimeout = cfg.GlobalTimeout
	}
	if cfg.PerResourceTimeout != 0 {
		d.PerResourceTimeout = cfg.PerResourceTimeout
	}
	if cfg.ProgressTimeout != 0 {
		d.ProgressTimeout = cfg.ProgressTimeout
	}
	if cfg.CheckInterval != 0 {
		d.CheckInterval = cfg.CheckInterval
	}
	if cfg.Clock != nil {
		d.Clock = cfg.Clock
	}
	if cfg.LegacyEventActivityTimeout || cfg.ResetOnAnyEvent {
		d.LegacyEventActivityTimeout = true
	}
	if cfg.DisableProgressTimeout {
		d.ProgressTimeout = 0
	}
	return d
}

// Advance moves a recognized injected test clock forward and evaluates
// deadlines. Production clocks cannot be advanced.
func (m *Monitor) Advance(d time.Duration) error {
	switch clock := m.cfg.Clock.(type) {
	case *FakeClock:
		clock.Advance(d)
	case *ManualClock:
		clock.Advance(d)
	}
	return m.Check()
}

// Tick is an alias for Advance.
func (m *Monitor) Tick(d time.Duration) error { return m.Advance(d) }

// Clock returns the configured passive clock.
func (m *Monitor) Clock() PassiveClock { return m.cfg.Clock }

// RecordActivity is an explicit alias for ObserveActivity.
func (m *Monitor) RecordActivity() bool { return m.ObserveActivity() }

// NotifyEvent is an alias for ObserveEvent.
func (m *Monitor) NotifyEvent() bool { return m.ObserveEvent() }

// OnEvent is an alias for ObserveEvent.
func (m *Monitor) OnEvent() bool { return m.ObserveEvent() }

// RecordSubstantiveProgress is an alias for RecordProgress.
func (m *Monitor) RecordSubstantiveProgress() { m.RecordProgress() }

// UpdateDesiredState is an alias for UpdateTarget.
func (m *Monitor) UpdateDesiredState(target Target) bool { return m.UpdateTarget(target) }

// OnTargetChanged explicitly resets the progress window.
func (m *Monitor) OnTargetChanged(reason string) { m.ResetProgress(reason) }
