package dyntracker

import (
	"context"
	"time"

	"github.com/werf/kubedog/pkg/tracker/deadline"
)

// pollCondition mirrors wait.ConditionFunc without binding polling to the real
// clock inside k8s wait.PollImmediate.
type pollCondition func() (bool, error)

// pollWithDeadlines runs an immediate condition check followed by interval
// ticks. All timers come from clock, making presence/absence tests deterministic
// with a fake clock. A zero perResourceTimeout means unlimited. A positive
// globalTimeout or shared deadline arms the global timer.
func pollWithDeadlines(
	ctx context.Context,
	clock deadline.Clock,
	interval time.Duration,
	resourceID string,
	perResourceTimeout time.Duration,
	globalTimeout time.Duration,
	shared *deadline.SharedDeadline,
	condition pollCondition,
) error {
	if clock == nil {
		clock = deadline.SystemClock
	}
	if interval <= 0 {
		interval = time.Second
	}

	start := clock.Now()

	check := func() (bool, error) {
		done, err := condition()
		if err != nil || done {
			return done, err
		}
		return false, nil
	}

	if done, err := check(); err != nil {
		return err
	} else if done {
		return nil
	}

	ticker := clock.NewTicker(interval)
	defer ticker.Stop()

	var perTimer deadline.Timer
	var perCh <-chan time.Time
	if perResourceTimeout > 0 {
		perTimer = clock.NewTimer(perResourceTimeout)
		perCh = perTimer.C()
		defer perTimer.Stop()
	}

	globalDuration := globalTimeout
	if shared != nil {
		if remaining, ok := shared.Remaining(clock.Now()); ok {
			globalDuration = remaining
		} else if shared.Timeout() > 0 {
			globalDuration = shared.Timeout()
		}
	}

	var globalTimer deadline.Timer
	var globalCh <-chan time.Time
	if globalDuration > 0 {
		globalTimer = clock.NewTimer(globalDuration)
		globalCh = globalTimer.C()
		defer globalTimer.Stop()
	}

	for {
		select {
		case <-ctx.Done():
			return context.Cause(ctx)
		case <-perCh:
			now := clock.Now()
			return deadline.NewError(
				deadline.KindPerResource,
				resourceID,
				now.Sub(start),
				perResourceTimeout,
				now.Sub(start),
				0,
				deadline.StateSummary{ResourceID: resourceID, Text: "presence/absence poll did not reach desired state before per-resource deadline"},
			)
		case <-globalCh:
			now := clock.Now()
			configured := globalTimeout
			if shared != nil {
				configured = shared.Timeout()
			}
			return deadline.NewError(
				deadline.KindGlobal,
				resourceID,
				now.Sub(start),
				configured,
				now.Sub(start),
				0,
				deadline.StateSummary{ResourceID: resourceID, Text: "presence/absence poll did not reach desired state before global deadline"},
			)
		case <-ticker.C():
			if done, err := check(); err != nil {
				return err
			} else if done {
				return nil
			}
		}
	}
}

func schedulableClock(value any) deadline.Clock {
	if clock, ok := value.(deadline.Clock); ok && clock != nil {
		return clock
	}
	return deadline.SystemClock
}

func validatePollDeadlineConfig(perResourceTimeout, globalTimeout time.Duration) error {
	if perResourceTimeout < 0 {
		return &deadline.ConfigError{Field: "Timeout", Value: perResourceTimeout}
	}
	if globalTimeout < 0 {
		return &deadline.ConfigError{Field: "GlobalTimeout", Value: globalTimeout}
	}
	return nil
}
