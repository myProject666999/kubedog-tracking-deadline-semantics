# Tracking deadline semantics

Kubedog uses three distinct deadline classes. They always race, and the first
one to expire ends tracking.

| Kind | Scope | Reset by target change? | Default |
| --- | --- | --- | --- |
| `global` | One tracking operation, potentially shared by many resources | No | unlimited |
| `per_resource` / `resource` | One resource lifetime; may be shorter than global | No | `tracker.Options.Timeout` / `DynamicReadinessTrackerOptions.Timeout`, zero unlimited |
| `progress` | Silence since the last substantive progress | Yes | 4 minutes |

## Substantive progress

For Deployment, StatefulSet, DaemonSet and Job, progress is:

- one of `Replicas`, `UpToDate`, `Available`, `Ready`, or Job `Succeeded`
  moving toward its target;
- a newly observed Pod, or a Pod phase transition;
- Job success count increasing.

Normal events, log output, and unrelated status-field updates are activity but
are not substantive progress. For standalone Pod and generic resources there
are no replica indicators, so their progress deadline retains the historical
"time since any activity" meaning.

`LegacyEventActivityTimeout` / `ResetOnAnyEvent` restores the old event-interval
behavior for all resource kinds for one compatibility window.

## Target-state changes

The progress window restarts when the desired target state changes. The target
fields are:

- `Generation` (`metadata.generation`);
- desired replica count;
- Job completion target;
- template hash/revision when supplied by the tracker.

This includes scaling down: any target change restarts progress. The number of
restarts is unbounded, but every restart is recorded in `Monitor.Resets()` and
emits a log record when `Config.Logf` or `Config.Logger` is configured. Restart
never extends either the per-resource or global deadline; hard deadlines remain
fixed from their original start time.

## Configuration merge

- Durations are Go `time.Duration` values.
- Positive values enable a deadline.
- Zero means unlimited in `deadline.Config` after default construction. Use
  `deadline.DefaultConfig()` for the built-in four-minute progress default.
- Negative durations are configuration errors.
- A per-resource override longer than the global deadline is clamped to the
  global deadline and produces a warning.
- A `SharedDeadline` or `Registry` gives multiple resources one common start
  time and global budget.

## Time source

Pass a `Clock` implementation. `k8s.io/utils/clock/testing.FakeClock` and the
package's `NewFakeClock` / `NewManualClock` types are supported. Tests should
advance the clock in explicit ticks and call `Monitor.Check`/`CheckAt`; no real
sleep is required. Timers created by runners are stopped on every re-arm and on
exit.

## Errors

Deadline failures return `*deadline.Error` (also aliased as
`DeadlineError`/`ExceededError`). It contains the kind, resource ID, elapsed
time, configured limit, age of the last progress, reset count, and a stable
status summary with replica counts and waiting messages.

Callers can use:

```go
var deadlineErr *deadline.Error
errors.As(err, &deadlineErr)

errors.Is(err, deadline.ErrGlobalDeadline)
errors.Is(err, deadline.ErrPerResourceDeadline)
errors.Is(err, deadline.ErrProgressDeadline)
errors.Is(err, context.DeadlineExceeded)
```

The structured error is also used as a `context.WithTimeoutCause` /
`context.WithCancelCause` cause, so existing `context.Cause(ctx)` consumers keep
working.

## Failure and termination modes

- `WaitUntilResourceReady` arms the deadlines.
- `NonBlocking` does not wait for readiness and therefore does not arm progress
  or waiting-oriented global/per-resource deadlines.
- `IgnoreAndContinueDeployProcess` records a deadline on the resource state and
  in `Report.IgnoredDeadlines`, but does not turn that resource's timeout into a
  process-wide failure. Ignored deadlines remain in the final report.
- Once a resource is in its fatal/failed state, `Monitor.Stop` stops all three
  deadline checks so a later timeout cannot overwrite the real failure.

## Behavior change

The default four-minute timer now means "no substantive progress" for
replica-bearing workloads, not "no event at all". Noisy resources that emit
frequent events but make no progress can therefore begin timing out where the
old event-reset timer did not. Set `LegacyEventActivityTimeout: true` to retain
the old event-interval semantics during migration.
