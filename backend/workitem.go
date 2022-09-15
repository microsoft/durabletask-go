package backend

import (
	"errors"
	"fmt"
	"time"

	"github.com/cgillum/durabletask-go/api"
	"github.com/cgillum/durabletask-go/internal/protos"
)

var ErrNoWorkItems = errors.New("no work items were found")

type WorkItem interface {
	Description() string
}

type OrchestrationWorkItem struct {
	InstanceID api.InstanceID
	NewEvents  []*protos.HistoryEvent
	LockedBy   string
	RetryCount int32
	State      *OrchestrationRuntimeState
}

func (wi *OrchestrationWorkItem) Description() string {
	return fmt.Sprintf("%v (%d event(s))", wi.InstanceID, len(wi.NewEvents))
}

func (wi *OrchestrationWorkItem) GetAbandonDelay() time.Duration {
	if wi.RetryCount == 0 {
		return time.Duration(0) // no delay
	} else {
		if wi.RetryCount > 100 {
			return 5 * time.Minute // max delay
		} else {
			return time.Duration(wi.RetryCount) * time.Second // linear backoff
		}
	}
}

type ActivityWorkItem struct {
	SequenceNumber int64
	InstanceID     api.InstanceID
	NewEvent       *protos.HistoryEvent
	Result         *protos.HistoryEvent
	LockedBy       string
}

// Description implements core.WorkItem
func (wi *ActivityWorkItem) Description() string {
	name := wi.NewEvent.GetTaskScheduled().GetName()
	taskID := wi.NewEvent.EventId
	return fmt.Sprintf("%s/%s#%d", wi.InstanceID, name, taskID)
}
