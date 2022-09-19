package backend

import (
	context "context"
	"fmt"

	"github.com/microsoft/durabletask-go/api"
	"github.com/microsoft/durabletask-go/internal/protos"
)

type activityProcessor struct {
	be       Backend
	executor ActivityExecutor
}

type ActivityExecutor interface {
	ExecuteActivity(context.Context, api.InstanceID, *protos.HistoryEvent) (*protos.HistoryEvent, error)
}

func NewActivityTaskWorker(be Backend, executor ActivityExecutor, logger Logger, options *WorkerOptions) TaskWorker {
	processor := newActivityProcessor(be, executor)
	return NewTaskWorker(be, processor, logger, options)
}

func newActivityProcessor(be Backend, executor ActivityExecutor) TaskProcessor {
	return &activityProcessor{
		be:       be,
		executor: executor,
	}
}

// Name implements TaskProcessor
func (*activityProcessor) Name() string {
	return "activity-processor"
}

// FetchWorkItem implements TaskDispatcher
func (ap *activityProcessor) FetchWorkItem(ctx context.Context) (WorkItem, error) {
	return ap.be.GetActivityWorkItem(ctx)
}

// ProcessWorkItem implements TaskDispatcher
func (p *activityProcessor) ProcessWorkItem(ctx context.Context, wi WorkItem) error {
	awi := wi.(*ActivityWorkItem)
	result, err := p.executor.ExecuteActivity(ctx, awi.InstanceID, awi.NewEvent)
	if err != nil {
		return err
	}

	awi.Result = result
	return nil
}

// CompleteWorkItem implements TaskDispatcher
func (ap *activityProcessor) CompleteWorkItem(ctx context.Context, wi WorkItem) error {
	awi := wi.(*ActivityWorkItem)
	if awi.Result.GetTaskCompleted() == nil && awi.Result.GetTaskFailed() == nil {
		return fmt.Errorf("invalid result on activity work item '%s'", wi.Description())
	}

	return ap.be.CompleteActivityWorkItem(ctx, awi)
}

// AbandonWorkItem implements TaskDispatcher
func (ap *activityProcessor) AbandonWorkItem(ctx context.Context, wi WorkItem) error {
	awi := wi.(*ActivityWorkItem)
	return ap.be.AbandonActivityWorkItem(ctx, awi)
}
