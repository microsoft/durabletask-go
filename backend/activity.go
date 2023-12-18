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
	be                   Backend
	executor             ActivityExecutor
	logger               Logger
	maxOutputSizeInBytes int
}

type activityWorkerConfig struct {
	workerOptions        []NewTaskWorkerOptions
	maxOutputSizeInBytes int
}

// ActivityWorkerOption is a function that configures an activity worker.
type ActivityWorkerOption func(*activityWorkerConfig)

// WithMaxConcurrentActivityInvocations sets the maximum number of concurrent activity invocations
// that the worker can process. If this limit is exceeded, the worker will block until the number of
// concurrent invocations drops below the limit.
func WithMaxConcurrentActivityInvocations(n int32) ActivityWorkerOption {
	return func(o *activityWorkerConfig) {
		o.workerOptions = append(o.workerOptions, WithMaxParallelism(n))
	}
}

// WithMaxActivityOutputSizeInKB sets the maximum size of an activity's output.
// If an activity's output exceeds this size, the activity execution will fail with an error.
func WithMaxActivityOutputSizeInKB(n int) ActivityWorkerOption {
	return func(o *activityWorkerConfig) {
		o.maxOutputSizeInBytes = n * 1024
	}
}

type ActivityExecutor interface {
	ExecuteActivity(context.Context, api.InstanceID, *protos.HistoryEvent) (*protos.HistoryEvent, error)
}

func NewActivityTaskWorker(be Backend, executor ActivityExecutor, logger Logger, opts ...ActivityWorkerOption) TaskWorker {
	config := &activityWorkerConfig{}
	for _, configure := range opts {
		configure(config)
	}

	processor := &activityProcessor{
		be:                   be,
		executor:             executor,
		logger:               logger,
		maxOutputSizeInBytes: config.maxOutputSizeInBytes,
	}

	return NewTaskWorker(be, processor, logger, config.workerOptions...)
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
	if result == nil {
		return fmt.Errorf("can't complete work item '%s' with nil result", wi.Description())
	}
	if result.GetTaskCompleted() == nil && result.GetTaskFailed() == nil {
		return fmt.Errorf("can't complete work item '%s', which isn't TaskCompleted or TaskFailed", wi.Description())
	}

	if p.maxOutputSizeInBytes > 0 && helpers.GetProtoSize(result) > p.maxOutputSizeInBytes {
		err = fmt.Errorf("activity output size %d exceeds limit of %d bytes", helpers.GetProtoSize(result), p.maxOutputSizeInBytes)
		awi.Result = helpers.NewTaskFailedEvent(awi.NewEvent.EventId, helpers.NewTaskFailureDetails(err))
	} else {
		awi.Result = result
	}

	return p.be.CompleteActivityWorkItem(ctx, awi)
}

// AbandonWorkItem implements TaskDispatcher
func (ap *activityProcessor) AbandonWorkItem(ctx context.Context, wi WorkItem) error {
	awi := wi.(*ActivityWorkItem)
	return ap.be.AbandonActivityWorkItem(ctx, awi)
}
