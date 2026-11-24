package tracker

import (
	"context"
	"errors"
	"time"

	"k8s.io/client-go/kubernetes"

	"github.com/werf/kubedog/pkg/dyntracker/util"
	"github.com/werf/kubedog/pkg/informer"
	"github.com/werf/kubedog/pkg/tracker/deadline"
)

var ErrStopTrack = errors.New("stop tracking now")

const (
	Initial           TrackerState = ""
	ResourceAdded     TrackerState = "ResourceAdded"
	ResourceSucceeded TrackerState = "ResourceSucceeded"
	ResourceReady     TrackerState = "ResourceReady"
	ResourceFailed    TrackerState = "ResourceFailed"
	ResourceDeleted   TrackerState = "ResourceDeleted"

	FollowingContainerLogs TrackerState = "FollowingContainerLogs"
	ContainerTrackerDone   TrackerState = "ContainerTrackerDone"
)

type TrackerState string

type Tracker struct {
	Kube                            kubernetes.Interface
	Namespace                       string
	ResourceName                    string
	FullResourceName                string // full resource name with resource kind (deploy/superapp)
	SaveLogsOnlyForNumberOfReplicas int
	LogsFromTime                    time.Time
	InformerFactory                 *util.Concurrent[*informer.InformerFactory]

	StatusGeneration uint64

	// Clock is an optional injectable time source. Trackers that create
	// timers use it; nil means the real clock.
	Clock deadline.Clock
}

type Options struct {
	ParentContext                            context.Context
	Timeout                                  time.Duration
	GlobalTimeout                            time.Duration
	ProgressTimeout                          time.Duration
	Clock                                    deadline.Clock
	DeadlineConfig                           deadline.Config
	LogsFromTime                             time.Time
	SaveLogsOnlyForNumberOfReplicas          int
	IgnoreLogs                               bool
	IgnoreReadinessProbeFailsByContainerName map[string]time.Duration
}
