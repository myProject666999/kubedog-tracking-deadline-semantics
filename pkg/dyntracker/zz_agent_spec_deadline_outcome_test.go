package dyntracker

import (
	"errors"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/runtime/schema"

	"github.com/werf/kubedog/pkg/dyntracker/statestore"
	"github.com/werf/kubedog/pkg/dyntracker/util"
	"github.com/werf/kubedog/pkg/tracker/deadline"
)

func agentSpecDeadlineError(kind deadline.Kind) *deadline.Error {
	return deadline.NewError(
		kind,
		"default/Deployment/demo",
		time.Minute,
		time.Minute,
		time.Minute,
		0,
		deadline.StateSummary{ResourceID: "default/Deployment/demo", Text: "test summary"},
	)
}

func TestAgentSpecDeadlineOutcome_IgnoreFailureRecordsButDoesNotFail(t *testing.T) {
	taskState := util.NewConcurrent(statestore.NewReadinessTaskState(
		"demo", "default",
		schema.GroupVersionKind{Group: "apps", Version: "v1", Kind: "Deployment"},
		statestore.ReadinessTaskStateOptions{FailMode: statestore.IgnoreAndContinueDeployProcess},
	))
	tr := &DynamicReadinessTracker{
		resourceHumanID: "default/Deployment/demo",
		taskState:       taskState,
		deadlineConfig:  deadline.Config{Clock: deadline.NewManualClock(time.Now())},
	}

	deadlineErr := agentSpecDeadlineError(deadline.KindGlobal)
	if err := tr.deadlineOutcome(deadlineErr); err != nil {
		t.Fatalf("ignored deadline must not terminate tracking: %v", err)
	}

	report := tr.IgnoredDeadlineReport()
	if len(report.IgnoredDeadlines) != 1 {
		t.Fatalf("expected ignored deadline in report, got %+v", report)
	}
	if !errors.Is(report.IgnoredDeadlines[0].Err, deadline.ErrGlobalDeadline) {
		t.Fatalf("report must preserve structured deadline kind: %v", report.IgnoredDeadlines[0].Err)
	}

	var rootStatus statestore.ResourceStatus
	taskState.RTransaction(func(ts *statestore.ReadinessTaskState) {
		root := ts.ResourceState(ts.Name(), ts.Namespace(), ts.GroupVersionKind())
		root.RTransaction(func(rs *statestore.ResourceState) { rootStatus = rs.Status() })
	})
	if rootStatus != statestore.ResourceStatusFailed {
		t.Fatalf("ignored deadline must still mark resource failed, got %q", rootStatus)
	}
}

func TestAgentSpecDeadlineOutcome_FailFastReturnsStructuredError(t *testing.T) {
	taskState := util.NewConcurrent(statestore.NewReadinessTaskState(
		"demo", "default",
		schema.GroupVersionKind{Group: "apps", Version: "v1", Kind: "Deployment"},
		statestore.ReadinessTaskStateOptions{FailMode: statestore.FailWholeDeployProcessImmediately},
	))
	tr := &DynamicReadinessTracker{
		resourceHumanID: "default/Deployment/demo",
		taskState:       taskState,
		deadlineConfig:  deadline.Config{Clock: deadline.NewManualClock(time.Now())},
	}

	deadlineErr := agentSpecDeadlineError(deadline.KindProgress)
	err := tr.deadlineOutcome(deadlineErr)
	if err == nil {
		t.Fatal("expected fail-fast mode to return deadline error")
	}
	var got *deadline.Error
	if !errors.As(err, &got) {
		t.Fatalf("expected structured *deadline.Error, got %T: %v", err, err)
	}
	if got.Kind != deadline.KindProgress {
		t.Fatalf("unexpected kind %q", got.Kind)
	}
}
