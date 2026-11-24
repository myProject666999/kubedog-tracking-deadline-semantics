package deadline

import (
	"time"

	kclock "k8s.io/utils/clock"
	ktesting "k8s.io/utils/clock/testing"
)

// PassiveClock reports the current time without creating timers. It is the
// minimal clock required by the pure Monitor.CheckAt path; clocks that also
// implement Since remain assignable because Go interfaces are structural.
type PassiveClock interface {
	Now() time.Time
}

// Clock is the schedulable clock used by deadline runners. Kubernetes'
// k8s.io/utils/clock.Clock and its fake clock implement this interface.
type Clock = kclock.WithTicker

// Timer is the stoppable timer abstraction returned by Clock.NewTimer.
type Timer = kclock.Timer

// Ticker is the stoppable ticker abstraction returned by Clock.NewTicker.
type Ticker = kclock.Ticker

// RealClock is the production time source.
type RealClock = kclock.RealClock

// FakeClock is the controllable Kubernetes-compatible fake clock. Advance is
// provided as a descriptive alias for Step.
type FakeClock struct {
	*ktesting.FakeClock
}

// NewFakeClock returns a fake clock initialized at now.
func NewFakeClock(now time.Time) *FakeClock {
	return &FakeClock{FakeClock: ktesting.NewFakeClock(now)}
}

// Advance moves the clock forward and fires due timers/tickers.
func (c *FakeClock) Advance(d time.Duration) {
	if d > 0 {
		c.FakeClock.Step(d)
	}
}

// Step moves the clock forward and fires due timers/tickers.
func (c *FakeClock) Step(d time.Duration) {
	c.FakeClock.Step(d)
}

// ManualClock is a passive clock whose time is set explicitly. It is useful
// for table tests that call Monitor.CheckAt and do not need timer channels.
type ManualClock struct {
	now time.Time
}

// NewManualClock returns a passive clock starting at now.
func NewManualClock(now time.Time) *ManualClock {
	return &ManualClock{now: now}
}

// Now returns the manually controlled time.
func (c *ManualClock) Now() time.Time { return c.now }

// Since returns t-to-current difference.
func (c *ManualClock) Since(t time.Time) time.Duration { return c.now.Sub(t) }

// Set sets the current time.
func (c *ManualClock) Set(t time.Time) { c.now = t }

// Advance moves the current time by d.
func (c *ManualClock) Advance(d time.Duration) { c.now = c.now.Add(d) }

// standardPassiveClock adapts the system clock to PassiveClock.
type standardPassiveClock struct{}

func (standardPassiveClock) Now() time.Time                   { return time.Now() }
func (standardPassiveClock) Since(t time.Time) time.Duration  { return time.Since(t) }
func (standardPassiveClock) NewTicker(d time.Duration) Ticker { return kclock.RealClock{}.NewTicker(d) }
func (standardPassiveClock) NewTimer(d time.Duration) Timer   { return kclock.RealClock{}.NewTimer(d) }
func (standardPassiveClock) After(d time.Duration) <-chan time.Time {
	return kclock.RealClock{}.After(d)
}
func (standardPassiveClock) Sleep(d time.Duration)                 { kclock.RealClock{}.Sleep(d) }
func (standardPassiveClock) Tick(d time.Duration) <-chan time.Time { return kclock.RealClock{}.Tick(d) }

// SystemClock is the default schedulable clock.
var SystemClock Clock = standardPassiveClock{}

func passiveClockOrSystem(c PassiveClock) PassiveClock {
	if c == nil {
		return SystemClock
	}
	return c
}
