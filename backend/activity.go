package backend

import (
	"context"
	"fmt"

	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/dapr/durabletask-go/api"
	"github.com/dapr/durabletask-go/api/helpers"
	"github.com/dapr/durabletask-go/api/protos"
)

type activityProcessor struct {
	be       Backend
	executor ActivityExecutor
}

type ActivityExecutor interface {
	ExecuteActivity(context.Context, api.InstanceID, *protos.HistoryEvent) (*protos.HistoryEvent, error)
}

func NewActivityTaskWorker(be Backend, executor ActivityExecutor, logger Logger, opts ...NewTaskWorkerOptions) TaskWorker[*ActivityWorkItem] {
	processor := newActivityProcessor(be, executor)
	return NewTaskWorker(processor, logger, opts...)
}

func newActivityProcessor(be Backend, executor ActivityExecutor) TaskProcessor[*ActivityWorkItem] {
	return &activityProcessor{
		be:       be,
		executor: executor,
	}
}

// Name implements TaskProcessor
func (*activityProcessor) Name() string {
	return "activity-processor"
}

// NextWorkItem implements TaskDispatcher
func (ap *activityProcessor) NextWorkItem(ctx context.Context) (*ActivityWorkItem, error) {
	return ap.be.NextActivityWorkItem(ctx)
}

// ProcessWorkItem implements TaskDispatcher
func (p *activityProcessor) ProcessWorkItem(ctx context.Context, awi *ActivityWorkItem) error {
	ts := awi.NewEvent.GetTaskScheduled()
	if ts == nil {
		return fmt.Errorf("%v: invalid TaskScheduled event", awi.InstanceID)
	}

	if awi.NewEvent.Router == nil {
		awi.NewEvent.Router = &protos.TaskRouter{}
	}

	if eventRouter := awi.NewEvent.GetRouter(); eventRouter != nil {
		awi.NewEvent.Router.Source = eventRouter.GetSource()
		awi.NewEvent.Router.Target = eventRouter.GetTarget()
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

	if result != nil {
		if result.Router == nil {
			result.Router = &protos.TaskRouter{}
		}
		// Copy Router info from the NewEvent to preserve cross-app routing context
		if awi.NewEvent.Router != nil {
			result.Router.Source = awi.NewEvent.Router.Source
			result.Router.Target = awi.NewEvent.Router.Target
		}
	}

	awi.Result = result
	return nil
}

// CompleteWorkItem implements TaskDispatcher
func (ap *activityProcessor) CompleteWorkItem(ctx context.Context, awi *ActivityWorkItem) error {
	if awi.Result == nil {
		return fmt.Errorf("can't complete work item '%s' with nil result", awi)
	}
	if awi.Result.GetTaskCompleted() == nil && awi.Result.GetTaskFailed() == nil {
		return fmt.Errorf("can't complete work item '%s', which isn't TaskCompleted or TaskFailed", awi)
	}

	return ap.be.CompleteActivityWorkItem(ctx, awi)
}

// AbandonWorkItem implements TaskDispatcher
func (ap *activityProcessor) AbandonWorkItem(ctx context.Context, awi *ActivityWorkItem) error {
	return ap.be.AbandonActivityWorkItem(ctx, awi)
}
