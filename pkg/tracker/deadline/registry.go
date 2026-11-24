package deadline

import (
	"errors"
	"sync"
)

// IgnoredDeadline records a deadline that was intentionally ignored for one
// resource but must still appear in the final report.
type IgnoredDeadline struct {
	ResourceID string `json:"resource_id"`
	Err        *Error `json:"error"`
}

// Report lists deadline outcomes that survive ignore-failure mode.
type Report struct {
	IgnoredDeadlines []IgnoredDeadline `json:"ignored_deadlines"`
}

// Registry coordinates the optional shared global deadline and retains
// deadlines that were ignored at resource level.
type Registry struct {
	mu sync.Mutex

	global   *SharedDeadline
	monitors map[string]*Monitor
	ignored  []IgnoredDeadline
}

// NewRegistry creates a registry. If cfg contains a shared global deadline it
// is reused; otherwise a shared deadline is created from GlobalTimeout.
func NewRegistry(cfg Config) (*Registry, error) {
	normalized, err := cfg.Normalize()
	if err != nil {
		return nil, err
	}
	global := cfg.Shared
	if global == nil {
		global = cfg.GlobalDeadline
	}
	if global == nil && normalized.GlobalTimeout > 0 {
		global = NewSharedDeadline(normalized.GlobalTimeout, normalized.Clock)
	}
	return &Registry{global: global, monitors: make(map[string]*Monitor)}, nil
}

// Global returns the shared global deadline, if any.
func (r *Registry) Global() *SharedDeadline { return r.global }

// Monitor creates and registers a resource monitor.
func (r *Registry) Monitor(resourceID string, cfg Config) (*Monitor, error) {
	if r.global != nil && cfg.Shared == nil && cfg.GlobalDeadline == nil {
		cfg.Shared = r.global
	}
	monitor, err := NewMonitor(resourceID, cfg)
	if err != nil {
		return nil, err
	}
	r.mu.Lock()
	r.monitors[resourceID] = monitor
	r.mu.Unlock()
	return monitor, nil
}

// IgnoreDeadline retains an ignored deadline for the final report.
func (r *Registry) IgnoreDeadline(resourceID string, err error) *Error {
	deadlineErr, ok := AsError(err)
	if !ok {
		return nil
	}
	if deadlineErr.ResourceID == "" {
		deadlineErr.ResourceID = resourceID
		deadlineErr.Resource = resourceID
	}
	r.mu.Lock()
	r.ignored = append(r.ignored, IgnoredDeadline{ResourceID: resourceID, Err: deadlineErr})
	r.mu.Unlock()
	return deadlineErr
}

// IgnoredDeadlines returns a copy of ignored deadlines.
func (r *Registry) IgnoredDeadlines() []IgnoredDeadline {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]IgnoredDeadline(nil), r.ignored...)
}

// Report returns the final deadline report.
func (r *Registry) Report() Report {
	r.mu.Lock()
	defer r.mu.Unlock()
	return Report{IgnoredDeadlines: append([]IgnoredDeadline(nil), r.ignored...)}
}

// AsError performs errors.As for *Error and returns false when err is not a
// structured deadline error.
func AsError(err error) (*Error, bool) {
	var deadlineErr *Error
	if errors.As(err, &deadlineErr) {
		return deadlineErr, true
	}
	return nil, false
}
