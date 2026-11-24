package dyntracker

import (
	"context"
	"fmt"
	"io"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"

	"github.com/werf/kubedog/pkg/dyntracker/statestore"
	"github.com/werf/kubedog/pkg/dyntracker/util"
	"github.com/werf/kubedog/pkg/informer"
	"github.com/werf/kubedog/pkg/tracker/deadline"
)

type DynamicAbsenceTracker struct {
	taskState       *util.Concurrent[*statestore.AbsenceTaskState]
	informerFactory *util.Concurrent[*informer.InformerFactory]
	dynamicClient   dynamic.Interface
	mapper          meta.ResettableRESTMapper

	timeout                    time.Duration
	globalTimeout              time.Duration
	globalDeadline             *deadline.SharedDeadline
	clock                      deadline.Clock
	pollPeriod                 time.Duration
	caseInsensitiveGVKMatching bool
}

func NewDynamicAbsenceTracker(
	taskState *util.Concurrent[*statestore.AbsenceTaskState],
	informerFactory *util.Concurrent[*informer.InformerFactory],
	dynamicClient dynamic.Interface,
	mapper meta.ResettableRESTMapper,
	opts DynamicAbsenceTrackerOptions,
) *DynamicAbsenceTracker {
	timeout := opts.Timeout
	var pollPeriod time.Duration
	if opts.PollPeriod != 0 {
		pollPeriod = opts.PollPeriod
	} else {
		pollPeriod = 1 * time.Second
	}

	globalDeadline, globalTimeout := globalDeadlineFromOption(opts.GlobalDeadline, opts.GlobalTimeout)

	return &DynamicAbsenceTracker{
		taskState:                  taskState,
		informerFactory:            informerFactory,
		dynamicClient:              dynamicClient,
		mapper:                     mapper,
		timeout:                    timeout,
		globalTimeout:              globalTimeout,
		globalDeadline:             globalDeadline,
		clock:                      schedulableClock(opts.Clock),
		pollPeriod:                 pollPeriod,
		caseInsensitiveGVKMatching: opts.CaseInsensitiveGVKMatching,
	}
}

type DynamicAbsenceTrackerOptions struct {
	Timeout                    time.Duration
	GlobalTimeout              time.Duration
	GlobalDeadline             any
	Clock                      any
	PollPeriod                 time.Duration
	CaseInsensitiveGVKMatching bool
}

func (t *DynamicAbsenceTracker) Track(ctx context.Context) error {
	var (
		name             string
		namespace        string
		groupVersionKind schema.GroupVersionKind
	)
	t.taskState.RTransaction(func(ts *statestore.AbsenceTaskState) {
		name = ts.Name()
		namespace = ts.Namespace()
		groupVersionKind = ts.GroupVersionKind()
	})

	if err := validatePollDeadlineConfig(t.timeout, t.globalTimeout); err != nil {
		return err
	}

	lookupGVK := groupVersionKind
	if t.caseInsensitiveGVKMatching {
		lookupGVK = util.LowercaseGVK(groupVersionKind)
	}

	namespaced, err := util.IsNamespaced(lookupGVK, t.mapper)
	if err != nil {
		return fmt.Errorf("check if namespaced: %w", err)
	}

	gvr, err := util.GVRFromGVK(lookupGVK, t.mapper)
	if err != nil {
		return fmt.Errorf("get GroupVersionResource: %w", err)
	}

	var resourceClient dynamic.ResourceInterface
	if namespaced {
		resourceClient = t.dynamicClient.Resource(gvr).Namespace(namespace)
	} else {
		resourceClient = t.dynamicClient.Resource(gvr)
	}

	resourceHumanID := util.ResourceHumanID(name, namespace, groupVersionKind, t.mapper)

	if err := pollWithDeadlines(ctx, t.clock, t.pollPeriod, resourceHumanID, t.timeout, t.globalTimeout, t.globalDeadline, func() (bool, error) {
		if _, err := resourceClient.Get(ctx, name, metav1.GetOptions{}); err != nil {
			if apierrors.IsResourceExpired(err) || apierrors.IsGone(err) || err == io.EOF || err == io.ErrUnexpectedEOF {
				return false, nil
			}

			if apierrors.IsNotFound(err) {
				return true, nil
			}

			return false, fmt.Errorf("get resource %q: %w", resourceHumanID, err)
		}

		t.taskState.RWTransaction(func(ats *statestore.AbsenceTaskState) {
			ats.ResourceState().RWTransaction(func(rs *statestore.ResourceState) {
				rs.SetStatus(statestore.ResourceStatusCreated)
			})
		})

		return false, nil
	}); err != nil {
		return err
	}

	t.taskState.RWTransaction(func(ats *statestore.AbsenceTaskState) {
		ats.ResourceState().RWTransaction(func(rs *statestore.ResourceState) {
			rs.SetStatus(statestore.ResourceStatusDeleted)
		})
	})

	return nil
}
