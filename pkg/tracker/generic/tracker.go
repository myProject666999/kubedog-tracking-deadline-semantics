package generic

import (
	"context"
	"fmt"
	"os"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/dynamic"

	"github.com/werf/kubedog/pkg/dyntracker/util"
	"github.com/werf/kubedog/pkg/informer"
	"github.com/werf/kubedog/pkg/log"
	"github.com/werf/kubedog/pkg/resid"
	"github.com/werf/kubedog/pkg/tracker/deadline"
)

const ResourceStatusStabilizingDuration time.Duration = 2 * time.Second

type TrackerState string

const (
	TrackerStateInitial           TrackerState = "TrackerStateInitial"
	TrackerStateStatusStabilizing TrackerState = "TrackerStateStatusStabilizing"
	TrackerStateStarted           TrackerState = "TrackerStateStarted"
	TrackerStateResourceAdded     TrackerState = "TrackerStateResourceAdded"
	TrackerStateResourceSucceeded TrackerState = "TrackerStateResourceSucceeded"
	TrackerStateResourceFailed    TrackerState = "TrackerStateResourceFailed"
	TrackerStateResourceDeleted   TrackerState = "TrackerStateResourceDeleted"
)

type Tracker struct {
	ResourceID *resid.ResourceID

	lastState    TrackerState
	lastStateMux sync.Mutex

	statusStableAt    time.Time
	statusStableAtMux sync.Mutex

	lastObjDuringStatusStabilization    *unstructured.Unstructured
	lastObjDuringStatusStabilizationMux sync.Mutex

	dynamicClient   dynamic.Interface
	discoveryClient discovery.CachedDiscoveryInterface
	mapper          meta.RESTMapper
	informerFactory *util.Concurrent[*informer.InformerFactory]

	// Clock controls the activity timer. It is exported so tests can inject a
	// fake clock after NewTracker. When nil, the real system clock is used.
	Clock deadline.Clock

	lastStatusSummary deadline.StateSummary
}

func NewTracker(
	resID *resid.ResourceID,
	dynClient dynamic.Interface,
	discClient discovery.CachedDiscoveryInterface,
	informerFactory *util.Concurrent[*informer.InformerFactory],
	mapper meta.RESTMapper,
) *Tracker {
	return &Tracker{
		ResourceID:      resID,
		lastState:       TrackerStateInitial,
		dynamicClient:   dynClient,
		discoveryClient: discClient,
		mapper:          mapper,
		informerFactory: informerFactory,
	}
}

func (t *Tracker) Track(ctx context.Context, noActivityTimeout time.Duration, addedCh, succeededCh, failedCh, regularCh chan<- *ResourceStatus, eventCh chan<- *corev1.Event) error {
	resAddedCh := make(chan *unstructured.Unstructured)
	resModifiedCh := make(chan *unstructured.Unstructured)
	resDeletedCh := make(chan *unstructured.Unstructured)

	resourceStateWatcher := NewResourceStateWatcher(t.ResourceID, t.dynamicClient, t.mapper, t.informerFactory)
	resourceInformerCleanupFn, err := resourceStateWatcher.Run(ctx, resAddedCh, resModifiedCh, resDeletedCh)
	if err != nil {
		return fmt.Errorf("run resource state watcher: %w", err)
	}
	defer resourceInformerCleanupFn()

	statusStabilizingTicker := time.NewTicker(time.Second)
	statusStabilizingDoneCh := make(chan bool)
	defer func() {
		statusStabilizingTicker.Stop()
		close(statusStabilizingDoneCh)
	}()
	go func() {
		for {
			select {
			case <-statusStabilizingTicker.C:
				if t.handleStatusStabilized(resModifiedCh) {
					statusStabilizingTicker.Stop()
					return
				}
			case <-statusStabilizingDoneCh:
				return
			}
		}
	}()

	eventWatcherCh := make(chan *corev1.Event)

	clock := t.Clock
	if clock == nil {
		clock = deadline.SystemClock
	}
	trackStartedAt := clock.Now()
	lastActivityAt := trackStartedAt

	// A single persistent timer is reset after every observed event. The old
	// time.After call allocated a fresh timer on every loop iteration and both
	// leaked until firing and made the semantics hard to test.
	var activityTimer deadline.Timer
	var activityCh <-chan time.Time
	if noActivityTimeout > 0 {
		activityTimer = clock.NewTimer(noActivityTimeout)
		activityCh = activityTimer.C()
		defer activityTimer.Stop()
	}

	for {
		select {
		case obj := <-resAddedCh:
			cleanupFn, err := t.handleResourceAddedModified(ctx, obj, addedCh, succeededCh, failedCh, regularCh, eventWatcherCh)
			if err != nil {
				return fmt.Errorf("handle resource added: %w", err)
			}
			defer cleanupFn()
		case obj := <-resModifiedCh:
			cleanupFn, err := t.handleResourceAddedModified(ctx, obj, addedCh, succeededCh, failedCh, regularCh, eventWatcherCh)
			if err != nil {
				return fmt.Errorf("handle resource modified: %w", err)
			}
			defer cleanupFn()
		case <-resDeletedCh:
			t.handleResourceDeleted(regularCh)
		case event := <-eventWatcherCh:
			t.handleEvent(event, eventCh, failedCh)
		case <-activityCh:
			now := clock.Now()
			summary := t.lastStatusSummary
			if summary.ResourceID == "" {
				summary.ResourceID = t.ResourceID.String()
			}
			if summary.Text == "" {
				summary.Text = "generic resource: no recognized replica progress indicator; activity timeout applies"
			}
			deadlineErr := deadline.NewError(
				deadline.KindProgress,
				t.ResourceID.String(),
				now.Sub(trackStartedAt),
				noActivityTimeout,
				now.Sub(lastActivityAt),
				0,
				summary,
			)
			failedCh <- NewFailedResourceStatus(deadlineErr.Error())
			return deadlineErr
		case <-ctx.Done():
			if log.Debug() {
				fmt.Printf("`%s` tracker context canceled: %s\n", t.ResourceID, context.Cause(ctx))
			}

			return context.Cause(ctx)
		}

		if activityTimer != nil {
			lastActivityAt = clock.Now()
			if !activityTimer.Stop() {
				select {
				case <-activityTimer.C():
				default:
				}
			}
			activityTimer.Reset(noActivityTimeout)
		}
	}
}

func (t *Tracker) handleStatusStabilized(resModCh chan<- *unstructured.Unstructured) (done bool) {
	if t.statusStabilizeAt().IsZero() {
		return false
	}

	if t.statusStabilizeAt().Before(time.Now()) {
		t.setLastState(TrackerStateStarted)
		resModCh <- t.lastObjectDuringStatusStabilization()
		return true
	}

	return false
}

func (t *Tracker) handleResourceAddedModified(ctx context.Context, object *unstructured.Unstructured, addedCh, succeededCh, failedCh, regularCh chan<- *ResourceStatus, eventCh chan<- *corev1.Event) (cleanupFn func(), err error) {
	cleanupFn = func() {}

	if t.getLastState() == TrackerStateInitial {
		eventsInformerCleanupFn := func() {}
		if os.Getenv("KUBEDOG_DISABLE_EVENTS") != "1" {
			resourceEventsWatcher := NewResourceEventsWatcher(object, t.ResourceID, t.mapper, t.informerFactory)
			eventsInformerCleanupFn, err = resourceEventsWatcher.Run(ctx, eventCh)
			if err != nil {
				return nil, fmt.Errorf("run resource events watcher: %w", err)
			}
		}

		cleanupFn = func() {
			eventsInformerCleanupFn()
		}

		t.setStatusStabilizeAt(time.Now().Add(ResourceStatusStabilizingDuration))
		t.setLastState(TrackerStateStatusStabilizing)
		t.setLastObjectDuringStatusStabilization(object)

		return cleanupFn, err
	}

	if t.getLastState() == TrackerStateStatusStabilizing {
		t.setLastObjectDuringStatusStabilization(object)

		return cleanupFn, err
	}

	resourceStatus, err := NewResourceStatus(object)
	if err != nil {
		return nil, fmt.Errorf("construct resource status: %w", err)
	}

	switch t.getLastState() {
	case TrackerStateResourceSucceeded:
		regularCh <- resourceStatus
	case TrackerStateStarted, TrackerStateResourceAdded, TrackerStateResourceFailed:
		if resourceStatus.IsFailed() {
			t.setLastState(TrackerStateResourceFailed)
			failedCh <- resourceStatus
		} else if resourceStatus.IsReady() {
			t.setLastState(TrackerStateResourceSucceeded)
			succeededCh <- resourceStatus
		} else {
			regularCh <- resourceStatus
		}
	case TrackerStateResourceDeleted:
		if resourceStatus.IsFailed() {
			t.setLastState(TrackerStateResourceFailed)
			failedCh <- resourceStatus
		} else if resourceStatus.IsReady() {
			t.setLastState(TrackerStateResourceSucceeded)
			succeededCh <- resourceStatus
		} else {
			t.setLastState(TrackerStateResourceAdded)
			addedCh <- resourceStatus
		}
	}

	return cleanupFn, nil
}

func (t *Tracker) handleResourceDeleted(regularCh chan<- *ResourceStatus) {
	t.setLastState(TrackerStateResourceDeleted)
	regularCh <- NewDeletedResourceStatus()
}

func (t *Tracker) handleEvent(event *corev1.Event, eventCh chan<- *corev1.Event, failedCh chan<- *ResourceStatus) {
	eventStatus := NewEventStatus(event)

	if eventStatus.IsFailure() {
		t.setLastState(TrackerStateResourceFailed)
		failedCh <- NewFailedResourceStatus(eventStatus.FailureReason())
	}

	eventCh <- event
}

func (t *Tracker) lastObjectDuringStatusStabilization() *unstructured.Unstructured {
	t.lastObjDuringStatusStabilizationMux.Lock()
	defer t.lastObjDuringStatusStabilizationMux.Unlock()

	return t.lastObjDuringStatusStabilization
}

func (t *Tracker) setLastObjectDuringStatusStabilization(obj *unstructured.Unstructured) {
	t.lastObjDuringStatusStabilizationMux.Lock()
	defer t.lastObjDuringStatusStabilizationMux.Unlock()

	t.lastObjDuringStatusStabilization = obj
}

func (t *Tracker) getLastState() TrackerState {
	t.lastStateMux.Lock()
	defer t.lastStateMux.Unlock()

	return t.lastState
}

func (t *Tracker) setLastState(state TrackerState) {
	t.lastStateMux.Lock()
	defer t.lastStateMux.Unlock()

	t.lastState = state
}

func (t *Tracker) statusStabilizeAt() time.Time {
	t.statusStableAtMux.Lock()
	defer t.statusStableAtMux.Unlock()

	return t.statusStableAt
}

func (t *Tracker) setStatusStabilizeAt(at time.Time) {
	t.statusStableAtMux.Lock()
	defer t.statusStableAtMux.Unlock()

	t.statusStableAt = at
}
