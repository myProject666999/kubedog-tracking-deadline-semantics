// Package deadline defines the three timeout semantics used by resource
// trackers: the shared global deadline, the per-resource deadline, and the
// progress (silence) deadline.
//
// The package is intentionally clock-driven and mostly free of Kubernetes
// types so that its ordering and reset semantics can be tested on fake clocks.
package deadline

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"
)

// Kind identifies which of the three deadlines fired.
type Kind string

const (
	// KindGlobal is the shared budget for one tracking operation that may
	// contain several resources. It is never reset by target changes.
	KindGlobal Kind = "global"
	// KindPerResource is the lifetime budget of one resource. It can be
	// shorter than the global budget and is never reset.
	KindPerResource Kind = "per_resource"
	// KindProgress bounds the time since the last substantive progress. It is
	// reset when the desired target state changes.
	KindProgress Kind = "progress"
)

// Type is an alias retained for code that refers to the deadline category as
// its type rather than its kind.
type Type = Kind

const (
	GlobalDeadlineKind      Kind = KindGlobal
	PerResourceDeadlineKind Kind = KindPerResource
	ResourceDeadlineKind    Kind = KindPerResource
	ProgressDeadlineKind    Kind = KindProgress

	// Compatibility aliases for callers/tests naming the constants Type*.
	TypeGlobal      Type = KindGlobal
	TypePerResource Type = KindPerResource
	TypeResource    Type = KindPerResource
	TypeProgress    Type = KindProgress
)

// Sentinel errors allow errors.Is to distinguish the three deadline classes.
// A concrete *Error matches the corresponding sentinel through its Is method.
var (
	ErrGlobalDeadline      = errors.New("global deadline exceeded")
	ErrPerResourceDeadline = errors.New("per-resource deadline exceeded")
	ErrResourceDeadline    = ErrPerResourceDeadline
	ErrProgressDeadline    = errors.New("progress deadline exceeded")

	// ErrDeadlineExceeded matches every structured deadline error and is also
	// matched by context.DeadlineExceeded.
	ErrDeadlineExceeded = errors.New("tracking deadline exceeded")
)

// IndicatorCount is a named replica/count indicator in a state summary.
type IndicatorCount struct {
	Name   string `json:"name"`
	Value  int64  `json:"value"`
	Target int64  `json:"target"`
}

// PodPhaseSummary is the last known phase of a Pod.
type PodPhaseSummary struct {
	Name  string `json:"name"`
	Phase string `json:"phase"`
}

// StateSummary is the stable, timestamp-free status attached to a deadline
// error. It contains replica counts and wait information.
type StateSummary struct {
	// Text can be supplied directly by tracker integrations. When empty, the
	// structured fields below are rendered by String.
	Text string `json:"text,omitempty"`

	ResourceID string `json:"resource_id,omitempty"`

	Replicas  *IndicatorCount `json:"replicas,omitempty"`
	UpToDate  *IndicatorCount `json:"up_to_date,omitempty"`
	Available *IndicatorCount `json:"available,omitempty"`
	Ready     *IndicatorCount `json:"ready,omitempty"`
	Succeeded *IndicatorCount `json:"succeeded,omitempty"`

	Indicators []IndicatorCount  `json:"indicators,omitempty"`
	Pods       []PodPhaseSummary `json:"pods,omitempty"`

	WaitingFor []string `json:"waiting_for,omitempty"`
	Reason     string   `json:"reason,omitempty"`
}

// String renders a stable one-line summary without absolute timestamps.
func (s StateSummary) String() string {
	if strings.TrimSpace(s.Text) != "" {
		return s.Text
	}

	var b strings.Builder
	first := true
	add := func(label, value string) {
		if value == "" {
			return
		}
		if !first {
			b.WriteString("; ")
		}
		first = false
		b.WriteString(label)
		b.WriteByte('=')
		b.WriteString(value)
	}

	if s.ResourceID != "" {
		add("resource", strconvQuote(s.ResourceID))
	}
	if s.Replicas != nil {
		add("replicas", fmt.Sprintf("%d/%d", s.Replicas.Value, s.Replicas.Target))
	}
	if s.UpToDate != nil {
		add("up_to_date", fmt.Sprintf("%d/%d", s.UpToDate.Value, s.UpToDate.Target))
	}
	if s.Available != nil {
		add("available", fmt.Sprintf("%d/%d", s.Available.Value, s.Available.Target))
	}
	if s.Ready != nil {
		add("ready", fmt.Sprintf("%d/%d", s.Ready.Value, s.Ready.Target))
	}
	if s.Succeeded != nil {
		add("succeeded", fmt.Sprintf("%d/%d", s.Succeeded.Value, s.Succeeded.Target))
	}
	if len(s.Indicators) > 0 {
		parts := make([]string, 0, len(s.Indicators))
		for _, in := range s.Indicators {
			parts = append(parts, fmt.Sprintf("%s=%d/%d", in.Name, in.Value, in.Target))
		}
		add("indicators", strings.Join(parts, ","))
	}
	if len(s.Pods) > 0 {
		parts := make([]string, 0, len(s.Pods))
		for _, pod := range s.Pods {
			if pod.Phase == "" {
				parts = append(parts, pod.Name)
			} else {
				parts = append(parts, pod.Name+":"+pod.Phase)
			}
		}
		add("pods", strings.Join(parts, ","))
	}
	if len(s.WaitingFor) > 0 {
		add("waiting", "["+strings.Join(s.WaitingFor, "; ")+"]")
	}
	if s.Reason != "" {
		add("reason", strconvQuote(s.Reason))
	}
	if first {
		return "no status summary available"
	}
	return b.String()
}

func strconvQuote(s string) string {
	return fmt.Sprintf("%q", s)
}

// Error is the structured error returned when a tracking deadline fires.
//
// Field names intentionally cover the vocabulary used in callers and tests:
// Kind/Type, ResourceID/Resource, Timeout/Limit, and
// SinceLastProgress/LastProgressAge.
type Error struct {
	// Kind identifies the deadline class.
	Kind Kind `json:"kind"`
	// Type is an alias for Kind.
	Type Kind `json:"type"`
	// Category, Class and DeadlineType are additional Kind aliases.
	Category     Kind `json:"category"`
	Class        Kind `json:"class"`
	DeadlineType Kind `json:"deadline_type"`

	// ResourceID is the stable resource identifier. It may be empty for a
	// purely global error not attributed to one resource.
	ResourceID string `json:"resource_id,omitempty"`
	Resource   string `json:"resource,omitempty"`

	// Elapsed is the time from tracking start to expiry.
	Elapsed time.Duration `json:"elapsed"`
	// ElapsedTime and Duration are aliases for Elapsed.
	ElapsedTime time.Duration `json:"elapsed_time"`
	Duration    time.Duration `json:"duration"`

	// Timeout is the configured duration for the firing deadline.
	Timeout time.Duration `json:"timeout"`
	// Limit is an alias for Timeout.
	Limit time.Duration `json:"limit"`
	// ConfiguredTimeout is an alias for Timeout.
	ConfiguredTimeout time.Duration `json:"configured_timeout"`

	// SinceLastProgress is the interval between the last substantive progress
	// and expiry. For global/per-resource errors it is still populated.
	SinceLastProgress time.Duration `json:"since_last_progress"`
	// LastProgressAge and ProgressAge are aliases.
	LastProgressAge time.Duration `json:"last_progress_age"`
	ProgressAge     time.Duration `json:"progress_age"`

	// ResetCount is the number of target-state resets observed before expiry.
	ResetCount int `json:"reset_count,omitempty"`

	// Summary is the structured status summary.
	Summary StateSummary `json:"summary"`
	// StateSummary is the rendered stable summary.
	StateSummary string `json:"state_summary"`
	// StatusSummary is an alias for StateSummary.
	StatusSummary string `json:"status_summary"`
	// State is an alias for StateSummary.
	State string `json:"state"`
}

// DeadlineError is an alias for Error.
type DeadlineError = Error

// NewError constructs a structured deadline error and keeps alias fields in
// sync.
func NewError(kind Kind, resourceID string, elapsed, timeout, sinceLastProgress time.Duration, resetCount int, summary StateSummary) *Error {
	if kind == "" {
		kind = KindProgress
	}
	if summary.ResourceID == "" {
		summary.ResourceID = resourceID
	}
	rendered := summary.String()
	return &Error{
		Kind:              kind,
		Type:              kind,
		Category:          kind,
		Class:             kind,
		DeadlineType:      kind,
		ResourceID:        resourceID,
		Resource:          resourceID,
		Elapsed:           elapsed,
		ElapsedTime:       elapsed,
		Duration:          elapsed,
		Timeout:           timeout,
		Limit:             timeout,
		ConfiguredTimeout: timeout,
		SinceLastProgress: sinceLastProgress,
		LastProgressAge:   sinceLastProgress,
		ProgressAge:       sinceLastProgress,
		ResetCount:        resetCount,
		Summary:           summary,
		StateSummary:      rendered,
		StatusSummary:     rendered,
		State:             rendered,
	}
}

func (e *Error) Error() string {
	if e == nil {
		return ErrDeadlineExceeded.Error()
	}
	resource := e.ResourceID
	if resource == "" {
		resource = e.Resource
	}

	var b strings.Builder
	b.WriteString(string(e.Kind))
	b.WriteString(" deadline exceeded")
	if resource != "" {
		b.WriteString(" for resource ")
		b.WriteString(strconvQuote(resource))
	}
	fmt.Fprintf(&b, " after %s", e.Elapsed)
	if e.Timeout > 0 {
		fmt.Fprintf(&b, " (configured %s)", e.Timeout)
	}
	fmt.Fprintf(&b, "; last progress %s ago", e.SinceLastProgress)
	if e.ResetCount > 0 {
		fmt.Fprintf(&b, "; target resets=%d", e.ResetCount)
	}
	b.WriteString("; state: ")
	b.WriteString(e.StateSummary)
	return b.String()
}

// Is supports errors.Is against ErrGlobalDeadline, ErrPerResourceDeadline,
// ErrProgressDeadline, ErrDeadlineExceeded, and context.DeadlineExceeded.
func (e *Error) Is(target error) bool {
	switch target {
	case ErrDeadlineExceeded, context.DeadlineExceeded:
		return true
	case ErrGlobalDeadline:
		return e.Kind == KindGlobal
	case ErrPerResourceDeadline, ErrResourceDeadline:
		return e.Kind == KindPerResource
	case ErrProgressDeadline:
		return e.Kind == KindProgress
	default:
		return false
	}
}

// Unwrap keeps context.DeadlineExceeded discoverable for callers that only use
// errors.Is(err, context.DeadlineExceeded).
func (e *Error) Unwrap() error {
	return context.DeadlineExceeded
}

// AsKind is a small helper for callers that do not need errors.As.
func AsKind(err error) (Kind, bool) {
	var deadlineErr *Error
	if errors.As(err, &deadlineErr) {
		return deadlineErr.Kind, true
	}
	if errors.Is(err, ErrGlobalDeadline) {
		return KindGlobal, true
	}
	if errors.Is(err, ErrPerResourceDeadline) {
		return KindPerResource, true
	}
	if errors.Is(err, ErrProgressDeadline) {
		return KindProgress, true
	}
	return "", false
}
