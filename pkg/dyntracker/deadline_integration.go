package dyntracker

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"time"

	"github.com/werf/kubedog/pkg/dyntracker/statestore"
	commontracker "github.com/werf/kubedog/pkg/tracker"
	"github.com/werf/kubedog/pkg/tracker/daemonset"
	"github.com/werf/kubedog/pkg/tracker/deadline"
	"github.com/werf/kubedog/pkg/tracker/deployment"
	"github.com/werf/kubedog/pkg/tracker/job"
	"github.com/werf/kubedog/pkg/tracker/pod"
	"github.com/werf/kubedog/pkg/tracker/statefulset"
)

func firstDuration(values ...time.Duration) time.Duration {
	for _, value := range values {
		if value != 0 {
			return value
		}
	}
	return 0
}

func mergeAnyDurationMaps(values ...any) map[string]time.Duration {
	merged := make(map[string]time.Duration)
	for _, value := range values {
		for id, d := range durationMapFromAny(value) {
			merged[id] = d
		}
	}
	if len(merged) == 0 {
		return nil
	}
	return merged
}

func durationMapFromAny(value any) map[string]time.Duration {
	if value == nil {
		return nil
	}
	if m, ok := value.(map[string]time.Duration); ok {
		return m
	}

	result := make(map[string]time.Duration)
	rv := reflect.ValueOf(value)
	if rv.Kind() != reflect.Map {
		return nil
	}
	for _, key := range rv.MapKeys() {
		d, ok := rv.MapIndex(key).Interface().(time.Duration)
		if !ok {
			continue
		}
		id := fmt.Sprint(key.Interface())
		if stringer, ok := key.Interface().(fmt.Stringer); ok {
			id = stringer.String()
		} else if key.Kind() == reflect.Struct {
			ptr := reflect.New(key.Type())
			ptr.Elem().Set(key)
			if stringer, ok := ptr.Interface().(fmt.Stringer); ok {
				id = stringer.String()
			}
		}
		result[id] = d
	}
	return result
}

func buildReadinessDeadlineConfig(opts DynamicReadinessTrackerOptions, timeout, noActivityTimeout time.Duration) deadline.Config {
	cfg := opts.DeadlineConfig

	if cfg.Clock == nil {
		cfg.Clock = opts.Clock
	}
	if cfg.GlobalTimeout == 0 {
		cfg.GlobalTimeout = firstDuration(cfg.GlobalTimeout, opts.GlobalTimeout, opts.GlobalDeadlineTimeout, opts.DeadlineConfig.GlobalTimeout, opts.DeadlineConfig.Global)
	}
	if cfg.PerResourceTimeout == 0 {
		cfg.PerResourceTimeout = firstDuration(cfg.PerResourceTimeout, opts.PerResourceTimeout, opts.DefaultPerResourceTimeout, opts.DefaultTimeout, opts.Timeout, timeout, opts.DeadlineConfig.PerResourceTimeout, opts.DeadlineConfig.PerResource, opts.DeadlineConfig.DefaultPerResourceTimeout)
	}
	if cfg.ProgressTimeout == 0 && !cfg.DisableProgressTimeout {
		cfg.ProgressTimeout = firstDuration(cfg.ProgressTimeout, opts.ProgressTimeout, opts.ProgressDeadline, opts.ProgressDeadlineTimeout, opts.NoActivityTimeout, noActivityTimeout, opts.DeadlineConfig.ProgressTimeout, opts.DeadlineConfig.Progress)
	}
	if opts.DisableProgressTimeout || opts.NoProgressTimeout {
		cfg.DisableProgressTimeout = true
		cfg.ProgressTimeout = 0
	}
	if cfg.CheckInterval == 0 {
		cfg.CheckInterval = firstDuration(opts.ProgressCheckInterval, opts.DeadlineConfig.CheckInterval, opts.DeadlineConfig.PollInterval)
	}
	if cfg.CheckInterval == 0 {
		cfg.CheckInterval = deadline.DefaultCheckInterval
	}
	if cfg.Shared == nil {
		switch value := opts.GlobalDeadline.(type) {
		case *deadline.SharedDeadline:
			cfg.Shared = value
		case deadline.SharedDeadline:
			cfg.Shared = &value
		case time.Duration:
			cfg.GlobalTimeout = firstDuration(cfg.GlobalTimeout, value)
		}
		cfg.Shared = firstSharedDeadline(cfg.Shared, opts.DeadlineConfig.Shared, opts.DeadlineConfig.GlobalDeadline)
	}
	if !cfg.LegacyEventActivityTimeout {
		cfg.LegacyEventActivityTimeout = opts.LegacyEventActivityTimeout || opts.LegacyNoActivityEventSemantics ||
			opts.DeadlineConfig.LegacyEventActivityTimeout || opts.DeadlineConfig.ResetOnAnyEvent ||
			opts.DeadlineConfig.ActivityTimeoutMode == deadline.ActivityTimeoutOnAnyEvent
	}
	if cfg.ResourceTimeouts == nil && cfg.ResourceOverrides == nil && cfg.Overrides == nil {
		cfg.ResourceTimeouts = mergeAnyDurationMaps(opts.ResourceTimeouts, opts.PerResourceTimeouts, opts.ResourceTimeoutOverrides, opts.PerResourceTimeoutOverrides, opts.DeadlineOverrides, opts.DeadlineConfig.ResourceTimeouts, opts.DeadlineConfig.ResourceTimeoutOverrides, opts.DeadlineConfig.ResourceOverrides, opts.DeadlineConfig.Overrides)
	}
	if cfg.Clock == nil {
		cfg.Clock = deadline.SystemClock
	}
	if cfg.Warnf == nil {
		cfg.Warnf = func(format string, args ...any) {
			fmt.Printf("WARNING: "+format+"\n", args...)
		}
	}
	if cfg.Logf == nil {
		cfg.Logf = func(format string, args ...any) {
			fmt.Printf(format+"\n", args...)
		}
	}

	return cfg
}

func firstSharedDeadline(values ...*deadline.SharedDeadline) *deadline.SharedDeadline {
	for _, value := range values {
		if value != nil {
			return value
		}
	}
	return nil
}

func globalDeadlineFromOption(value any, timeout time.Duration) (*deadline.SharedDeadline, time.Duration) {
	switch typed := value.(type) {
	case *deadline.SharedDeadline:
		return typed, timeout
	case deadline.SharedDeadline:
		return &typed, timeout
	case time.Duration:
		return nil, firstDuration(timeout, typed)
	default:
		return nil, timeout
	}
}

func (t *DynamicReadinessTracker) deadlineTrackingContext(parent context.Context) (context.Context, context.CancelFunc) {
	if t == nil || t.deadlineMonitor == nil {
		return context.WithCancel(parent)
	}

	trackCtx, cancelCause := context.WithCancelCause(parent)
	go t.runDeadlineWatcher(trackCtx, cancelCause)
	return trackCtx, func() {
		cancelCause(context.Canceled)
	}
}

func (t *DynamicReadinessTracker) runDeadlineWatcher(ctx context.Context, cancelCause context.CancelCauseFunc) {
	monitor := t.deadlineMonitor

	clock, ok := t.deadlineConfig.Clock.(deadline.Clock)
	if !ok || clock == nil {
		clock = deadline.SystemClock
	}

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		now := clock.Now()
		_, at, found := monitor.NextDeadline(now)
		if !found {
			<-ctx.Done()
			return
		}

		d := at.Sub(now)
		if d < 0 {
			d = 0
		}
		// Cap sleep at the coarse check interval. Event processing also calls
		// into the monitor on every status sample; this timer only catches
		// silence and does not need millisecond precision.
		if interval := t.deadlineConfig.CheckInterval; interval > 0 && d > interval {
			d = interval
		}
		timer := clock.NewTimer(d)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case tickAt := <-timer.C():
			timer.Stop()
			checkAt := tickAt
			if checkAt.IsZero() {
				checkAt = clock.Now()
			}
			err := monitor.CheckAt(checkAt)
			if err == nil {
				// A target/activity reset can move the next deadline. Re-arm
				// with one stopped/created timer per loop iteration.
				continue
			}
			cancelCause(err)
			return
		}
	}
}

func (t *DynamicReadinessTracker) trackWaitError(trackCtx context.Context, err error) error {
	// A deadline cancellation may make a watcher return nil or ErrStopTrack.
	// Inspect the context cause before treating that as successful completion.
	var causeDeadlineErr *deadline.Error
	if cause := context.Cause(trackCtx); cause != nil {
		_ = errors.As(cause, &causeDeadlineErr)
	}

	if err == nil || errors.Is(err, commontracker.ErrStopTrack) {
		if causeDeadlineErr != nil {
			return t.deadlineOutcome(causeDeadlineErr)
		}
		return nil
	}

	var deadlineErr *deadline.Error
	if !errors.As(err, &deadlineErr) && causeDeadlineErr != nil {
		deadlineErr = causeDeadlineErr
	}
	if deadlineErr != nil {
		return t.deadlineOutcome(deadlineErr)
	}

	return fmt.Errorf("track resource %q: %w", t.resourceHumanID, err)
}

func (t *DynamicReadinessTracker) deadlineOutcome(deadlineErr *deadline.Error) error {
	ignore := t.failMode == statestore.IgnoreAndContinueDeployProcess
	if t.taskState != nil {
		t.taskState.RTransaction(func(ts *statestore.ReadinessTaskState) {
			if t.failMode == "" {
				ignore = ts.FailMode() == statestore.IgnoreAndContinueDeployProcess
			}
			root := ts.ResourceState(ts.Name(), ts.Namespace(), ts.GroupVersionKind())
			root.RWTransaction(func(rs *statestore.ResourceState) {
				rs.AddError(deadlineErr, "", t.deadlineConfig.Clock.Now())
				rs.SetStatus(statestore.ResourceStatusFailed)
			})
		})
	}
	if !ignore {
		return deadlineErr
	}

	t.ignoredDeadlinesMu.Lock()
	t.ignoredDeadlines = append(t.ignoredDeadlines, deadlineErr)
	t.ignoredDeadlinesMu.Unlock()
	if t.deadlineRegistry != nil {
		t.deadlineRegistry.IgnoreDeadline(t.resourceHumanID, deadlineErr)
	}
	return nil
}

// IgnoredDeadlineReport returns deadline failures retained even when their
// fail mode is IgnoreAndContinueDeployProcess.
func (t *DynamicReadinessTracker) IgnoredDeadlineReport() deadline.Report {
	t.ignoredDeadlinesMu.Lock()
	local := append([]*deadline.Error(nil), t.ignoredDeadlines...)
	t.ignoredDeadlinesMu.Unlock()

	if t.deadlineRegistry == nil {
		items := make([]deadline.IgnoredDeadline, 0, len(local))
		for _, err := range local {
			items = append(items, deadline.IgnoredDeadline{ResourceID: t.resourceHumanID, Err: err})
		}
		return deadline.Report{IgnoredDeadlines: items}
	}
	return t.deadlineRegistry.Report()
}

// IgnoredDeadlineErrors returns structured deadlines ignored for this tracker.
func (t *DynamicReadinessTracker) IgnoredDeadlineErrors() []*deadline.Error {
	t.ignoredDeadlinesMu.Lock()
	defer t.ignoredDeadlinesMu.Unlock()
	return append([]*deadline.Error(nil), t.ignoredDeadlines...)
}

func (t *DynamicReadinessTracker) noteActivityDeadline() {
	if t != nil && t.deadlineMonitor != nil {
		t.deadlineMonitor.ObserveActivity()
	}
}

func (t *DynamicReadinessTracker) stopDeadlineMonitor() {
	if t != nil && t.deadlineMonitor != nil {
		t.deadlineMonitor.Stop()
	}
}

func indicatorCount(name string, value, target int64) deadline.IndicatorCount {
	return deadline.IndicatorCount{Name: name, Value: value, Target: target}
}

func deploymentDeadlineObservation(status *deployment.DeploymentStatus) deadline.Observation {
	obs := deadline.Observation{
		Summary: deadline.StateSummary{
			WaitingFor: append([]string(nil), status.WaitingForMessages...),
		},
	}
	target := deadline.Target{Generation: status.Generation}
	if status.ReplicasIndicator != nil {
		target.Replicas = int64(status.ReplicasIndicator.TargetValue)
		obs.HasTarget = true
		obs.Target = target
		obs.Indicators = append(obs.Indicators, indicatorCount(deadline.IndicatorReplicas, int64(status.ReplicasIndicator.Value), int64(status.ReplicasIndicator.TargetValue)))
	}
	if status.UpToDateIndicator != nil {
		obs.Indicators = append(obs.Indicators, indicatorCount(deadline.IndicatorUpToDate, int64(status.UpToDateIndicator.Value), int64(status.UpToDateIndicator.TargetValue)))
	}
	if status.AvailableIndicator != nil {
		obs.Indicators = append(obs.Indicators, indicatorCount(deadline.IndicatorAvailable, int64(status.AvailableIndicator.Value), int64(status.AvailableIndicator.TargetValue)))
	}
	obs.Pods = podPhaseSummaries(status.Pods, status.NewPodsNames)
	obs.Summary.Indicators = obs.Indicators
	obs.Summary.Pods = obs.Pods
	return obs
}

func statefulSetDeadlineObservation(status *statefulset.StatefulSetStatus) deadline.Observation {
	obs := deadline.Observation{Summary: deadline.StateSummary{WaitingFor: append([]string(nil), status.WaitingForMessages...)}}
	obs.Target = deadline.Target{Generation: status.Generation}
	if status.ReplicasIndicator != nil {
		obs.HasTarget = true
		obs.Target.Replicas = status.ReplicasIndicator.TargetValue
		obs.Indicators = append(obs.Indicators, indicatorCount(deadline.IndicatorReplicas, status.ReplicasIndicator.Value, status.ReplicasIndicator.TargetValue))
	}
	if status.ReadyIndicator != nil {
		obs.Indicators = append(obs.Indicators, indicatorCount(deadline.IndicatorReady, status.ReadyIndicator.Value, status.ReadyIndicator.TargetValue))
	}
	if status.UpToDateIndicator != nil {
		obs.Indicators = append(obs.Indicators, indicatorCount(deadline.IndicatorUpToDate, status.UpToDateIndicator.Value, status.UpToDateIndicator.TargetValue))
	}
	obs.Pods = podPhaseSummaries(status.Pods, status.NewPodsNames)
	obs.Summary.Indicators = obs.Indicators
	obs.Summary.Pods = obs.Pods
	return obs
}

func daemonSetDeadlineObservation(status *daemonset.DaemonSetStatus) deadline.Observation {
	obs := deadline.Observation{Summary: deadline.StateSummary{WaitingFor: append([]string(nil), status.WaitingForMessages...)}}
	obs.Target = deadline.Target{Generation: status.Generation}
	if status.ReplicasIndicator != nil {
		obs.HasTarget = true
		obs.Target.Replicas = int64(status.ReplicasIndicator.TargetValue)
		obs.Indicators = append(obs.Indicators, indicatorCount(deadline.IndicatorReplicas, int64(status.ReplicasIndicator.Value), int64(status.ReplicasIndicator.TargetValue)))
	}
	if status.UpToDateIndicator != nil {
		obs.Indicators = append(obs.Indicators, indicatorCount(deadline.IndicatorUpToDate, int64(status.UpToDateIndicator.Value), int64(status.UpToDateIndicator.TargetValue)))
	}
	if status.AvailableIndicator != nil {
		obs.Indicators = append(obs.Indicators, indicatorCount(deadline.IndicatorAvailable, int64(status.AvailableIndicator.Value), int64(status.AvailableIndicator.TargetValue)))
	}
	obs.Pods = podPhaseSummaries(status.Pods, status.NewPodsNames)
	obs.Summary.Indicators = obs.Indicators
	obs.Summary.Pods = obs.Pods
	return obs
}

func jobDeadlineObservation(status *job.JobStatus) deadline.Observation {
	obs := deadline.Observation{Summary: deadline.StateSummary{WaitingFor: append([]string(nil), status.WaitingForMessages...)}}
	obs.Target = deadline.Target{Generation: status.Generation}
	if status.SucceededIndicator != nil {
		obs.HasTarget = true
		obs.Target.Completions = int64(status.SucceededIndicator.TargetValue)
		obs.Indicators = append(obs.Indicators, indicatorCount(deadline.IndicatorSucceeded, int64(status.SucceededIndicator.Value), int64(status.SucceededIndicator.TargetValue)))
	}
	obs.Pods = podPhaseSummaries(status.Pods, nil)
	obs.Summary.Indicators = obs.Indicators
	obs.Summary.Pods = obs.Pods
	return obs
}

func podPhaseSummaries(pods map[string]pod.PodStatus, onlyNames []string) []deadline.PodPhaseSummary {
	allow := make(map[string]struct{}, len(onlyNames))
	for _, name := range onlyNames {
		allow[name] = struct{}{}
	}
	result := make([]deadline.PodPhaseSummary, 0, len(pods))
	for name, status := range pods {
		if len(allow) > 0 {
			if _, ok := allow[name]; !ok {
				continue
			}
		}
		phase := string(status.Phase)
		if phase == "" && status.StatusIndicator != nil {
			phase = status.StatusIndicator.Value
		}
		result = append(result, deadline.PodPhaseSummary{Name: name, Phase: phase})
	}
	return result
}

func (t *DynamicReadinessTracker) observeDeploymentDeadline(status *deployment.DeploymentStatus) {
	if t.deadlineMonitor != nil {
		t.deadlineMonitor.Observe(deploymentDeadlineObservation(status))
	}
}

func (t *DynamicReadinessTracker) observeStatefulSetDeadline(status *statefulset.StatefulSetStatus) {
	if t.deadlineMonitor != nil {
		t.deadlineMonitor.Observe(statefulSetDeadlineObservation(status))
	}
}

func (t *DynamicReadinessTracker) observeDaemonSetDeadline(status *daemonset.DaemonSetStatus) {
	if t.deadlineMonitor != nil {
		t.deadlineMonitor.Observe(daemonSetDeadlineObservation(status))
	}
}

func (t *DynamicReadinessTracker) observeJobDeadline(status *job.JobStatus) {
	if t.deadlineMonitor != nil {
		t.deadlineMonitor.Observe(jobDeadlineObservation(status))
	}
}

func (t *DynamicReadinessTracker) observePodDeadline(status *pod.PodStatus) {
	if t.deadlineMonitor == nil {
		return
	}
	t.noteActivityDeadline()
	if status != nil {
		phase := string(status.Phase)
		if phase == "" && status.StatusIndicator != nil {
			phase = status.StatusIndicator.Value
		}
		// Pods use activity semantics in this layer; record the phase only as
		// error context and do not treat it as a separate progress reset.
		t.deadlineMonitor.SetSummary(deadline.StateSummary{
			ResourceID: t.resourceHumanID,
			Pods:       []deadline.PodPhaseSummary{{Name: status.Name, Phase: phase}},
			Reason:     status.FailedReason,
		})
	}
}
