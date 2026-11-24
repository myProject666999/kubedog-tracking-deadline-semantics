package deadline

import (
	"fmt"
	"reflect"
	"sort"
	"sync"
	"time"
)

// Indicator names used when deciding whether a status update is substantive
// progress.
const (
	IndicatorReplicas  = "Replicas"
	IndicatorUpToDate  = "UpToDate"
	IndicatorAvailable = "Available"
	IndicatorReady     = "Ready"
	IndicatorSucceeded = "Succeeded"
)

// Target is the desired target state whose change restarts the progress
// window.
//
// The fields treated as target-state inputs are:
//   - Generation: metadata.generation, which increases for a new rollout even
//     when spec.replicas is unchanged;
//   - Replicas: the desired replica count (including scale-to-zero);
//   - Completions: Job desired successful completions;
//   - TemplateHash/Revision: pod-template or controller revision hints, when
//     available.
//
// Unrelated metadata or status updates must not be put in this value.
type Target struct {
	Generation   int64  `json:"generation,omitempty"`
	Replicas     int64  `json:"replicas,omitempty"`
	Completions  int64  `json:"completions,omitempty"`
	TemplateHash string `json:"template_hash,omitempty"`
	Revision     string `json:"revision,omitempty"`
}

// ResetRecord describes one progress-window reset.
type ResetRecord struct {
	At       time.Time `json:"at"`
	Elapsed  time.Duration
	Reason   string
	Old      Target
	New      Target
	Sequence int
}

// Observation is one status/event sample fed to a Monitor.
type Observation struct {
	// At optionally supplies the observation time. Zero means Clock.Now.
	At time.Time

	// Indicators are current named count indicators.
	Indicators []IndicatorCount
	// Target is the latest desired target state.
	Target Target
	// HasTarget must be set when a zero-valued Target is intentional.
	HasTarget bool

	// Pods are current pod phase samples. New names and phase transitions
	// count as progress.
	Pods []PodPhaseSummary

	// Activity reports that any event/channel activity occurred. It only
	// resets progress when legacy any-event semantics are enabled.
	Activity bool

	// Progress forces a substantive-progress reset when true. This is useful
	// for callers that have already applied resource-specific judgment.
	Progress bool

	// Summary replaces/augments the error status summary.
	Summary StateSummary
}

// Monitor tracks the three deadlines for one resource.
type Monitor struct {
	mu sync.Mutex

	id  string
	cfg ResolvedResourceConfig

	start        time.Time
	lastProgress time.Time
	stopped      bool
	firedErr     *Error

	target       Target
	targetKnown  bool
	prevCounts   map[string]int64
	hasCounts    map[string]bool
	podPhases    map[string]string
	resetRecords []ResetRecord

	summary StateSummary
}

// NewMonitor resolves cfg for resourceID and creates a monitor starting now.
func NewMonitor(resourceID string, cfg Config) (*Monitor, error) {
	resolved, err := cfg.Resolve(resourceID)
	if err != nil {
		return nil, err
	}
	now := resolved.Clock.Now()
	return newMonitorAt(resourceID, resolved, now), nil
}

// NewMonitorAt creates a monitor at an explicit time. This is primarily useful
// in deterministic tests with explicit tick values.
func NewMonitorAt(resourceID string, cfg Config, start time.Time) (*Monitor, error) {
	resolved, err := cfg.Resolve(resourceID)
	if err != nil {
		return nil, err
	}
	return newMonitorAt(resourceID, resolved, start), nil
}

func newMonitorAt(resourceID string, cfg ResolvedResourceConfig, start time.Time) *Monitor {
	summary := StateSummary{ResourceID: resourceID}
	return &Monitor{
		id:           resourceID,
		cfg:          cfg,
		start:        start,
		lastProgress: start,
		prevCounts:   make(map[string]int64),
		hasCounts:    make(map[string]bool),
		podPhases:    make(map[string]string),
		summary:      summary,
	}
}

// ID returns the resource identifier.
func (m *Monitor) ID() string { return m.id }

// StartTime returns the monitor start.
func (m *Monitor) StartTime() time.Time { return m.start }

// LastProgress returns the last progress time.
func (m *Monitor) LastProgress() time.Time {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.lastProgress
}

// ResetCount returns the number of target-state resets.
func (m *Monitor) ResetCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.resetRecords)
}

// Resets returns a copy of reset records.
func (m *Monitor) Resets() []ResetRecord {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := append([]ResetRecord(nil), m.resetRecords...)
	return out
}

// Summary returns the current state summary.
func (m *Monitor) Summary() StateSummary {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.summary
}

// Stop permanently stops deadline evaluation. It is called when the resource
// reaches success or a fatal failure so a timeout cannot replace the real
// terminal outcome.
func (m *Monitor) Stop() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.stopped = true
}

// Stopped reports whether Stop was called or a deadline fired.
func (m *Monitor) Stopped() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.stopped
}

// ResetProgress restarts the progress window at the current clock time.
func (m *Monitor) ResetProgress(reason string) {
	m.resetProgressAt(m.cfg.Clock.Now(), reason, Target{}, Target{}, false)
}

// ResetProgressAt restarts the progress window at at.
func (m *Monitor) ResetProgressAt(at time.Time, reason string) {
	m.resetProgressAt(at, reason, Target{}, Target{}, false)
}

func (m *Monitor) resetProgressAt(at time.Time, reason string, old, next Target, targetChange bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.stopped {
		return
	}
	if at.Before(m.lastProgress) {
		at = m.lastProgress
	}
	m.lastProgress = at
	record := ResetRecord{
		At:       at,
		Elapsed:  at.Sub(m.start),
		Reason:   reason,
		Old:      old,
		New:      next,
		Sequence: len(m.resetRecords) + 1,
	}
	m.resetRecords = append(m.resetRecords, record)
	m.logResetLocked(record, targetChange)
}

func (m *Monitor) logResetLocked(record ResetRecord, targetChange bool) {
	if m.cfg.Logf != nil {
		if targetChange {
			m.cfg.Logf("deadline: progress window reset #%d for %q at %s: target state changed: %+v -> %+v", record.Sequence, m.id, record.Elapsed, record.Old, record.New)
		} else {
			m.cfg.Logf("deadline: progress window reset #%d for %q at %s: %s", record.Sequence, m.id, record.Elapsed, record.Reason)
		}
		return
	}
	if m.cfg.Logger != nil {
		if targetChange {
			m.cfg.Logger.Printf("deadline: progress window reset #%d for %q at %s: target state changed: %+v -> %+v", record.Sequence, m.id, record.Elapsed, record.Old, record.New)
		} else {
			m.cfg.Logger.Printf("deadline: progress window reset #%d for %q at %s: %s", record.Sequence, m.id, record.Elapsed, record.Reason)
		}
	}
}

// SetSummary updates the status attached to a potential deadline error.
func (m *Monitor) SetSummary(summary StateSummary) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if summary.ResourceID == "" {
		summary.ResourceID = m.id
	}
	m.summary = summary
}

// UpdateTarget compares target with the previous target. The first target is
// treated as baseline and does not create a reset record. Later changes reset
// the progress window. It returns true when a target change caused a reset.
func (m *Monitor) UpdateTarget(target Target) bool {
	return m.ObserveTargetAt(target, m.cfg.Clock.Now())
}

// ObserveTargetAt is the explicit-time form of UpdateTarget.
func (m *Monitor) ObserveTargetAt(target Target, at time.Time) bool {
	m.mu.Lock()
	if m.stopped {
		m.mu.Unlock()
		return false
	}
	if !m.targetKnown {
		m.target = target
		m.targetKnown = true
		m.mu.Unlock()
		return false
	}
	old := m.target
	if old == target {
		m.mu.Unlock()
		return false
	}
	m.target = target
	m.mu.Unlock()

	m.resetProgressAt(at, "target state changed", old, target, true)
	return true
}

// ObserveEvent implements the old any-event reset behavior when configured.
// In the default progress mode it does nothing.
func (m *Monitor) ObserveEvent() bool {
	return m.ObserveActivity()
}

// ObserveActivity resets the window only under legacy any-event semantics.
func (m *Monitor) ObserveActivity() bool {
	if !m.cfg.LegacyEventActivityTimeout {
		return false
	}
	at := m.cfg.Clock.Now()
	m.mu.Lock()
	stopped := m.stopped
	m.mu.Unlock()
	if stopped {
		return false
	}
	m.ResetProgressAt(at, "activity event")
	return true
}

// RecordProgress records substantive progress at the current clock time.
func (m *Monitor) RecordProgress() {
	at := m.cfg.Clock.Now()
	m.recordProgressAt(at, "substantive progress")
}

// RecordProgressAt records substantive progress at at.
func (m *Monitor) RecordProgressAt(at time.Time) {
	m.recordProgressAt(at, "substantive progress")
}

func (m *Monitor) recordProgressAt(at time.Time, reason string) {
	m.mu.Lock()
	stopped := m.stopped
	m.mu.Unlock()
	if stopped {
		return
	}
	m.ResetProgressAt(at, reason)
}

// ObserveIndicator feeds one named indicator and resets progress when its
// value moves toward target.
func (m *Monitor) ObserveIndicator(name string, value, target int64) bool {
	return m.ObserveIndicatorAt(m.cfg.Clock.Now(), name, value, target)
}

// ObserveIndicatorAt is the explicit-time form of ObserveIndicator.
func (m *Monitor) ObserveIndicatorAt(at time.Time, name string, value, target int64) bool {
	progress := m.updateIndicator(name, value, target)
	if progress {
		m.recordProgressAt(at, fmt.Sprintf("indicator %s moved toward target", name))
	}
	return progress
}

func (m *Monitor) updateIndicator(name string, value, target int64) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.stopped {
		return false
	}

	m.updateSummaryIndicatorLocked(IndicatorCount{Name: name, Value: value, Target: target})

	had := m.hasCounts[name]
	old := m.prevCounts[name]
	m.prevCounts[name] = value
	m.hasCounts[name] = true
	if !had {
		return false
	}
	return movesToward(old, value, target)
}

func movesToward(old, cur, target int64) bool {
	if old == cur {
		return false
	}
	oldDistance := abs64(target - old)
	curDistance := abs64(target - cur)
	if curDistance < oldDistance {
		return true
	}
	// Count monotonic movement in the target direction even if a transient
	// overshoot leaves absolute distance equal/larger due to a later rebound.
	if old < target && cur > old {
		return true
	}
	if old > target && cur < old {
		return true
	}
	return false
}

func abs64(v int64) int64 {
	if v < 0 {
		return -v
	}
	return v
}

// ObservePodPhase records a pod phase sample. A newly seen pod or a phase
// transition counts as progress.
func (m *Monitor) ObservePodPhase(podName, phase string) bool {
	return m.ObservePodPhaseAt(m.cfg.Clock.Now(), podName, phase)
}

// ObservePodPhaseAt is the explicit-time form of ObservePodPhase.
func (m *Monitor) ObservePodPhaseAt(at time.Time, podName, phase string) bool {
	progress, old, seen := m.updatePodPhase(podName, phase)
	if progress {
		reason := "new pod observed"
		if seen {
			reason = fmt.Sprintf("pod phase advanced %q -> %q", old, phase)
		}
		m.recordProgressAt(at, reason)
	}
	return progress
}

func (m *Monitor) updatePodPhase(podName, phase string) (progress bool, oldPhase string, seen bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.stopped {
		return false, "", false
	}
	old, seen := m.podPhases[podName]
	m.podPhases[podName] = phase
	m.upsertPodSummaryLocked(PodPhaseSummary{Name: podName, Phase: phase})
	return !seen || old != phase, old, seen
}

// Observe feeds a composite observation using the monitor's clock.
func (m *Monitor) Observe(o Observation) bool {
	if o.At.IsZero() {
		o.At = m.cfg.Clock.Now()
	}
	return m.ObserveAt(o.At, o)
}

// ObserveAt feeds a composite observation at an explicit time.
func (m *Monitor) ObserveAt(at time.Time, o Observation) bool {
	progress := o.Progress
	targetReset := false

	if o.HasTarget {
		targetReset = m.ObserveTargetAt(o.Target, at)
		if targetReset {
			progress = true
		}
	}

	for _, in := range o.Indicators {
		if m.updateIndicator(in.Name, in.Value, in.Target) {
			progress = true
		}
	}
	for _, pod := range o.Pods {
		if progressed, _, _ := m.updatePodPhase(pod.Name, pod.Phase); progressed {
			progress = true
		}
	}
	if o.Activity && m.cfg.LegacyEventActivityTimeout {
		progress = true
	}
	// ObserveTargetAt already recorded the target-change reset. Record one
	// reset for a composite snapshot so several moving indicators in the same
	// object do not create multiple reset records.
	if progress && !targetReset {
		m.ResetProgressAt(at, "status observation showed substantive progress")
	}
	if !reflect.DeepEqual(o.Summary, StateSummary{}) {
		m.SetSummary(o.Summary)
	}
	return progress
}

// Check uses the injected clock and returns the first structured deadline
// error, if any.
func (m *Monitor) Check() error {
	return m.CheckAt(m.cfg.Clock.Now())
}

// CheckAt evaluates all three deadlines at now. It never sleeps or creates
// timers and is therefore the deterministic test entry point.
func (m *Monitor) CheckAt(now time.Time) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.stopped {
		if m.firedErr != nil {
			return m.firedErr
		}
		return nil
	}

	globalTimeout := m.cfg.GlobalTimeout
	globalStart := m.start
	if m.cfg.Shared != nil {
		globalTimeout = m.cfg.Shared.timeout
		globalStart = m.cfg.Shared.start
	}

	type candidate struct {
		kind     Kind
		at       time.Time
		timeout  time.Duration
		priority int
	}
	var candidates []candidate
	if globalTimeout > 0 {
		candidates = append(candidates, candidate{kind: KindGlobal, at: globalStart.Add(globalTimeout), timeout: globalTimeout, priority: 0})
	}
	if m.cfg.PerResourceTimeout > 0 {
		candidates = append(candidates, candidate{kind: KindPerResource, at: m.start.Add(m.cfg.PerResourceTimeout), timeout: m.cfg.PerResourceTimeout, priority: 1})
	}
	if m.cfg.ProgressTimeout > 0 {
		candidates = append(candidates, candidate{kind: KindProgress, at: m.lastProgress.Add(m.cfg.ProgressTimeout), timeout: m.cfg.ProgressTimeout, priority: 2})
	}
	if len(candidates) == 0 {
		return nil
	}

	sort.SliceStable(candidates, func(i, j int) bool {
		if candidates[i].at.Equal(candidates[j].at) {
			return candidates[i].priority < candidates[j].priority
		}
		return candidates[i].at.Before(candidates[j].at)
	})

	first := candidates[0]
	if now.Before(first.at) {
		return nil
	}

	elapsed := now.Sub(m.start)
	sinceProgress := now.Sub(m.lastProgress)
	if sinceProgress < 0 {
		sinceProgress = 0
	}
	summary := m.summary
	err := NewError(first.kind, m.id, elapsed, first.timeout, sinceProgress, len(m.resetRecords), summary)
	m.firedErr = err
	m.stopped = true
	return err
}

// NextDeadline returns the next deadline kind and absolute time for tests and
// scheduler setup.
func (m *Monitor) NextDeadline(now time.Time) (Kind, time.Time, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.stopped {
		return "", time.Time{}, false
	}
	globalTimeout := m.cfg.GlobalTimeout
	globalStart := m.start
	if m.cfg.Shared != nil {
		globalTimeout = m.cfg.Shared.timeout
		globalStart = m.cfg.Shared.start
	}
	var bestKind Kind
	var bestAt time.Time
	found := false
	consider := func(kind Kind, at time.Time, enabled bool) {
		if !enabled {
			return
		}
		if !found || at.Before(bestAt) {
			bestKind, bestAt, found = kind, at, true
		}
	}
	consider(KindGlobal, globalStart.Add(globalTimeout), globalTimeout > 0)
	consider(KindPerResource, m.start.Add(m.cfg.PerResourceTimeout), m.cfg.PerResourceTimeout > 0)
	consider(KindProgress, m.lastProgress.Add(m.cfg.ProgressTimeout), m.cfg.ProgressTimeout > 0)
	if !found || bestAt.Before(now) {
		return bestKind, bestAt, found
	}
	return bestKind, bestAt, true
}

func (m *Monitor) updateSummaryIndicatorLocked(in IndicatorCount) {
	switch in.Name {
	case IndicatorReplicas, "replicas", "ReplicasIndicator":
		m.summary.Replicas = &in
	case IndicatorUpToDate, "up_to_date", "UpToDateIndicator":
		m.summary.UpToDate = &in
	case IndicatorAvailable, "available", "AvailableIndicator":
		m.summary.Available = &in
	case IndicatorReady, "ready", "ReadyIndicator":
		m.summary.Ready = &in
	case IndicatorSucceeded, "succeeded", "SucceededIndicator":
		m.summary.Succeeded = &in
	}
}

func (m *Monitor) upsertPodSummaryLocked(pod PodPhaseSummary) {
	for i := range m.summary.Pods {
		if m.summary.Pods[i].Name == pod.Name {
			m.summary.Pods[i].Phase = pod.Phase
			return
		}
	}
	m.summary.Pods = append(m.summary.Pods, pod)
	sort.Slice(m.summary.Pods, func(i, j int) bool { return m.summary.Pods[i].Name < m.summary.Pods[j].Name })
}
