package deadline

import (
	"context"
	"sync"
	"time"
)

// SharedDeadline is one global deadline shared by all resources in a batch.
// Its start time is fixed when it is created, not when each resource's tracker
// starts.
type SharedDeadline struct {
	start   time.Time
	timeout time.Duration
	clock   PassiveClock

	mu     sync.Mutex
	cancel context.CancelCauseFunc
}

// NewSharedDeadline starts a shared global budget. A zero timeout means the
// global deadline is unlimited.
func NewSharedDeadline(timeout time.Duration, clocks ...PassiveClock) *SharedDeadline {
	clock := passiveClockOrSystem(firstClock(clocks))
	return &SharedDeadline{start: clock.Now(), timeout: timeout, clock: clock}
}

// Start is an alias for NewSharedDeadline for callers that prefer
// deadline.StartGlobal(...).
func Start(timeout time.Duration, clock PassiveClock) *SharedDeadline {
	return NewSharedDeadline(timeout, clock)
}

func firstClock(clocks []PassiveClock) PassiveClock {
	if len(clocks) > 0 {
		return clocks[0]
	}
	return nil
}

// StartTime returns the fixed batch start.
func (g *SharedDeadline) StartTime() time.Time { return g.start }

// Timeout returns the configured global budget.
func (g *SharedDeadline) Timeout() time.Duration { return g.timeout }

// Deadline returns the absolute global deadline. It returns false when the
// global deadline is unlimited.
func (g *SharedDeadline) Deadline() (time.Time, bool) {
	if g == nil || g.timeout <= 0 {
		return time.Time{}, false
	}
	return g.start.Add(g.timeout), true
}

// Remaining returns the time left before the global deadline. An unlimited
// deadline returns math.MaxInt64-ish duration and false.
func (g *SharedDeadline) Remaining(now time.Time) (time.Duration, bool) {
	deadline, ok := g.Deadline()
	if !ok {
		return 0, false
	}
	remaining := deadline.Sub(now)
	if remaining < 0 {
		return 0, true
	}
	return remaining, true
}

// ArmContext attaches cancellation of the shared deadline to ctx. Under the
// real clock the context deadline carries ErrGlobalDeadline as its cause.
// With a fake clock, callers drive a Monitor and call Cancel with the
// structured error because context itself cannot use a fake time source.
func (g *SharedDeadline) ArmContext(parent context.Context) (context.Context, context.CancelFunc) {
	if g == nil || g.timeout <= 0 {
		return context.WithCancel(parent)
	}

	remaining := time.Until(g.start.Add(g.timeout))
	if g.clock != SystemClock {
		ctx, cancelCause := context.WithCancelCause(parent)
		g.mu.Lock()
		g.cancel = cancelCause
		g.mu.Unlock()
		return ctx, func() { cancelCause(context.Canceled) }
	}

	ctx, cancel := context.WithTimeoutCause(parent, remaining, ErrGlobalDeadline)
	g.mu.Lock()
	g.cancel = func(error) { cancel() }
	g.mu.Unlock()
	return ctx, cancel
}

// Cancel cancels any context armed by ArmContext with cause.
func (g *SharedDeadline) Cancel(cause error) {
	g.mu.Lock()
	cancel := g.cancel
	g.mu.Unlock()
	if cancel != nil {
		cancel(cause)
	}
}
