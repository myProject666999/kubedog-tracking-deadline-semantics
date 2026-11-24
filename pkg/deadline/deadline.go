// Package deadline is the top-level compatibility import for tracking deadline
// semantics. The implementation lives under pkg/tracker/deadline; this package
// re-exports its stable API.
package deadline

import (
	"time"

	trackingdeadline "github.com/werf/kubedog/pkg/tracker/deadline"
)

type (
	Kind                   = trackingdeadline.Kind
	Type                   = trackingdeadline.Type
	Config                 = trackingdeadline.Config
	Options                = trackingdeadline.Options
	Policy                 = trackingdeadline.Policy
	ResolvedConfig         = trackingdeadline.ResolvedConfig
	ResolvedResourceConfig = trackingdeadline.ResolvedResourceConfig
	Error                  = trackingdeadline.Error
	DeadlineError          = trackingdeadline.DeadlineError
	ExceededError          = trackingdeadline.ExceededError
	TimeoutError           = trackingdeadline.TimeoutError
	GlobalDeadlineError    = trackingdeadline.GlobalDeadlineError
	ResourceDeadlineError  = trackingdeadline.ResourceDeadlineError
	ProgressDeadlineError  = trackingdeadline.ProgressDeadlineError
	Monitor                = trackingdeadline.Monitor
	Tracker                = trackingdeadline.Tracker
	Manager                = trackingdeadline.Manager
	Registry               = trackingdeadline.Registry
	Report                 = trackingdeadline.Report
	SharedDeadline         = trackingdeadline.SharedDeadline
	Clock                  = trackingdeadline.Clock
	PassiveClock           = trackingdeadline.PassiveClock
	FakeClock              = trackingdeadline.FakeClock
	ManualClock            = trackingdeadline.ManualClock
	StateSummary           = trackingdeadline.StateSummary
	Target                 = trackingdeadline.Target
	Observation            = trackingdeadline.Observation
	IndicatorCount         = trackingdeadline.IndicatorCount
	PodPhaseSummary        = trackingdeadline.PodPhaseSummary
	ResetRecord            = trackingdeadline.ResetRecord
	ActivityTimeoutMode    = trackingdeadline.ActivityTimeoutMode
)

const (
	KindGlobal                = trackingdeadline.KindGlobal
	KindPerResource           = trackingdeadline.KindPerResource
	KindProgress              = trackingdeadline.KindProgress
	Global                    = trackingdeadline.Global
	PerResource               = trackingdeadline.PerResource
	Progress                  = trackingdeadline.Progress
	DefaultProgressTimeout    = trackingdeadline.DefaultProgressTimeout
	DefaultPresenceTimeout    = trackingdeadline.DefaultPresenceTimeout
	DefaultCheckInterval      = trackingdeadline.DefaultCheckInterval
	IndicatorReplicas         = trackingdeadline.IndicatorReplicas
	IndicatorUpToDate         = trackingdeadline.IndicatorUpToDate
	IndicatorAvailable        = trackingdeadline.IndicatorAvailable
	IndicatorReady            = trackingdeadline.IndicatorReady
	IndicatorSucceeded        = trackingdeadline.IndicatorSucceeded
	ActivityTimeoutOnProgress = trackingdeadline.ActivityTimeoutOnProgress
	ActivityTimeoutOnAnyEvent = trackingdeadline.ActivityTimeoutOnAnyEvent
)

var (
	ErrGlobalDeadline      = trackingdeadline.ErrGlobalDeadline
	ErrPerResourceDeadline = trackingdeadline.ErrPerResourceDeadline
	ErrResourceDeadline    = trackingdeadline.ErrResourceDeadline
	ErrProgressDeadline    = trackingdeadline.ErrProgressDeadline
	ErrDeadlineExceeded    = trackingdeadline.ErrDeadlineExceeded
)

func NewFakeClock(now time.Time) *FakeClock {
	return trackingdeadline.NewFakeClock(now)
}

func NewManualClock(now time.Time) *ManualClock {
	return trackingdeadline.NewManualClock(now)
}

func NewSharedDeadline(timeout time.Duration, clock ...PassiveClock) *SharedDeadline {
	return trackingdeadline.NewSharedDeadline(timeout, clock...)
}

func NewMonitor(resourceID string, cfg Config) (*Monitor, error) {
	return trackingdeadline.NewMonitor(resourceID, cfg)
}

func NewTracker(resourceID string, cfg Config) (*Tracker, error) {
	return trackingdeadline.NewTracker(resourceID, cfg)
}

func NewMonitorAt(resourceID string, cfg Config, start time.Time) (*Monitor, error) {
	return trackingdeadline.NewMonitorAt(resourceID, cfg, start)
}

func NewManager(cfg Config) (*Manager, error) { return trackingdeadline.NewManager(cfg) }

func NewRegistry(cfg Config) (*Registry, error) { return trackingdeadline.NewRegistry(cfg) }

func DefaultConfig() Config { return trackingdeadline.DefaultConfig() }

func NormalizeConfig(cfg Config) (ResolvedConfig, error) {
	return trackingdeadline.NormalizeConfig(cfg)
}

func ResolveConfig(cfg Config, resourceID string) (ResolvedResourceConfig, error) {
	return trackingdeadline.ResolveConfig(cfg, resourceID)
}

func NewError(kind Kind, resourceID string, elapsed, timeout, sinceProgress time.Duration, resetCount int, summary StateSummary) *Error {
	return trackingdeadline.NewError(kind, resourceID, elapsed, timeout, sinceProgress, resetCount, summary)
}

func AsKind(err error) (Kind, bool) { return trackingdeadline.AsKind(err) }

func AsError(err error) (*Error, bool) { return trackingdeadline.AsError(err) }
