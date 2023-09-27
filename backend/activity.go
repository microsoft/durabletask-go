package backend

import (
	"context"
	"fmt"

	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/microsoft/durabletask-go/api"
	"github.com/microsoft/durabletask-go/internal/helpers"
	"github.com/microsoft/durabletask-go/internal/protos"
)

type activityProcessor struct {
	be       Backend
	executor ActivityExecutor
}

type ActivityExecutor interface {
	ExecuteActivity(context.Context, api.InstanceID, *protos.HistoryEvent) (*protos.HistoryEvent, error)
}

func NewActivityTaskWorker(be Backend, executor ActivityExecutor, logger Logger, opts ...NewTaskWorkerOptions) TaskWorker {
	processor := newActivityProcessor(be, executor)
	return NewTaskWorker(be, processor, logger, opts...)
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

	ts := awi.NewEvent.GetTaskScheduled()
	if ts == nil {
		return fmt.Errorf("%v: invalid TaskScheduled event", awi.InstanceID)
	}

	// Create span as child of spanContext found in TaskScheduledEvent
	ctx, err := helpers.ContextFromTraceContext(ctx, ts.ParentTraceContext)
	if err != nil {
		return fmt.Errorf("%v: failed to parse activity trace context: %w", awi.InstanceID, err)
	}
	var span trace.Span
	ctx, span = helpers.StartNewActivitySpan(ctx, ts.Name, ts.Version.GetValue(), string(awi.InstanceID), awi.NewEvent.EventId)
	if span != nil {
		defer func() {
			if r := recover(); r != nil {
				span.SetStatus(codes.Error, fmt.Sprintf("%v", r))
			}
			span.End()
		}()
	}

	// Execute the activity and get its result
	result, err := p.executor.ExecuteActivity(ctx, awi.InstanceID, awi.NewEvent)
	if err != nil {
		if span != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
		}
		return err
	}

	awi.Result = result
	return nil
}

// CompleteWorkItem implements TaskDispatcher
func (ap *activityProcessor) CompleteWorkItem(ctx context.Context, wi WorkItem) error {
	awi := wi.(*ActivityWorkItem)
	if awi.Result == nil {
		return fmt.Errorf("can't complete work item '%s' with nil result", wi.Description())
	}
	if awi.Result.GetTaskCompleted() == nil && awi.Result.GetTaskFailed() == nil {
		return fmt.Errorf("can't complete work item '%s', which isn't TaskCompleted or TaskFailed", wi.Description())
	}

	return ap.be.CompleteActivityWorkItem(ctx, awi)
}

// AbandonWorkItem implements TaskDispatcher
func (ap *activityProcessor) AbandonWorkItem(ctx context.Context, wi WorkItem) error {
	awi := wi.(*ActivityWorkItem)
	return ap.be.AbandonActivityWorkItem(ctx, awi)
}
