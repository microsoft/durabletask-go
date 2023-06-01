package backend

import (
	"context"
	"fmt"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/microsoft/durabletask-go/api"
	"github.com/microsoft/durabletask-go/internal/helpers"
	"github.com/microsoft/durabletask-go/internal/protos"
)

type OrchestratorExecutor interface {
	ExecuteOrchestrator(
		ctx context.Context,
		iid api.InstanceID,
		oldEvents []*protos.HistoryEvent,
		newEvents []*protos.HistoryEvent) (*ExecutionResults, error)
}

type orchestratorProcessor struct {
	be       Backend
	executor OrchestratorExecutor
	logger   Logger
}

func NewOrchestrationWorker(be Backend, executor OrchestratorExecutor, logger Logger, opts ...NewTaskWorkerOptions) TaskWorker {
	processor := &orchestratorProcessor{
		be:       be,
		executor: executor,
		logger:   logger,
	}
	return NewTaskWorker(be, processor, logger, opts...)
}

// Name implements TaskProcessor
func (*orchestratorProcessor) Name() string {
	return "orchestration-processor"
}

// FetchWorkItem implements TaskProcessor
func (p *orchestratorProcessor) FetchWorkItem(ctx context.Context) (WorkItem, error) {
	return p.be.GetOrchestrationWorkItem(ctx)
}

// ProcessWorkItem implements TaskProcessor
func (w *orchestratorProcessor) ProcessWorkItem(ctx context.Context, cwi WorkItem) error {
	wi := cwi.(*OrchestrationWorkItem)
	w.logger.Debugf("%v: received work item with %d new event(s): %v", wi.InstanceID, len(wi.NewEvents), helpers.HistoryListSummary(wi.NewEvents))

	// TODO: Caching
	// In the fullness of time, we should consider caching executors and runtime state
	// so that we can skip the loading of state and/or the creation of executors. A cached
	// executor should allow us to 1) skip runtime state loading and 2) execute only new events.
	if wi.State == nil {
		if state, err := w.be.GetOrchestrationRuntimeState(ctx, wi); err != nil {
			return fmt.Errorf("failed to load orchestration state: %w", err)
		} else {
			wi.State = state
		}
	}
	w.logger.Debugf("%v: got orchestration runtime state: %s", wi.InstanceID, getOrchestrationStateDescription(wi))

	if ctx, span, ok := w.applyWorkItem(ctx, wi); ok {
		defer func() {
			// Note that the span and ctx references may be updated inside the continue-as-new loop.
			w.endOrchestratorSpan(ctx, wi, span, false)
		}()

		for continueAsNewCount := 0; ; continueAsNewCount++ {
			if continueAsNewCount > 0 {
				w.logger.Debugf("%v: continuing-as-new with %d event(s): %s", wi.InstanceID, len(wi.State.NewEvents()), helpers.HistoryListSummary(wi.State.NewEvents()))
			} else {
				w.logger.Debugf("%v: invoking orchestrator", wi.InstanceID)
			}

			// Run the user orchestrator code, providing the old history and new events together.
			results, err := w.executor.ExecuteOrchestrator(ctx, wi.InstanceID, wi.State.OldEvents(), wi.State.NewEvents())
			if err != nil {
				return fmt.Errorf("error executing orchestrator: %w", err)
			}
			w.logger.Debugf("%v: orchestrator returned %d action(s): %s", wi.InstanceID, len(results.Response.Actions), helpers.ActionListSummary(results.Response.Actions))

			// Apply the orchestrator outputs to the orchestration state.
			continuedAsNew, err := wi.State.ApplyActions(results.Response.Actions, helpers.TraceContextFromSpan(span))
			if err != nil {
				return fmt.Errorf("failed to apply the execution result actions: %w", err)
			}
			wi.State.CustomStatus = results.Response.CustomStatus

			// When continuing-as-new, we re-execute the orchestrator from the beginning with a truncated state in a tight loop
			// until the orchestrator performs some non-continue-as-new action.
			if continuedAsNew {
				const MaxContinueAsNewCount = 20
				if continueAsNewCount >= MaxContinueAsNewCount {
					return fmt.Errorf("exceeded tight-loop continue-as-new limit of %d iterations", MaxContinueAsNewCount)
				}

				// We create a new trace span for every continue-as-new
				w.endOrchestratorSpan(ctx, wi, span, true)
				ctx, span = w.startOrResumeOrchestratorSpan(ctx, wi)
				continue
			}

			if wi.State.IsCompleted() {
				name, _ := wi.State.Name()
				w.logger.Infof("%v: '%s' completed with a %s status.", wi.InstanceID, name, helpers.ToRuntimeStatusString(wi.State.RuntimeStatus()))
			}
			break
		}
	}
	return nil
}

// CompleteWorkItem implements TaskProcessor
func (p *orchestratorProcessor) CompleteWorkItem(ctx context.Context, wi WorkItem) error {
	owi := wi.(*OrchestrationWorkItem)
	return p.be.CompleteOrchestrationWorkItem(ctx, owi)
}

// AbandonWorkItem implements TaskProcessor
func (p *orchestratorProcessor) AbandonWorkItem(ctx context.Context, wi WorkItem) error {
	owi := wi.(*OrchestrationWorkItem)
	return p.be.AbandonOrchestrationWorkItem(ctx, owi)
}

func (w *orchestratorProcessor) applyWorkItem(ctx context.Context, wi *OrchestrationWorkItem) (context.Context, trace.Span, bool) {
	// Ignore work items for orchestrations that are completed or are in a corrupted state.
	if !wi.State.IsValid() {
		w.logger.Warnf("%v: orchestration state is invalid; dropping work item", wi.InstanceID)
		return nil, nil, false
	} else if wi.State.IsCompleted() {
		w.logger.Warnf("%v: orchestration already completed; dropping work item", wi.InstanceID)
		return nil, nil, false
	} else if len(wi.NewEvents) == 0 {
		w.logger.Warnf("%v: the work item had no events!", wi.InstanceID)
	}

	// The orchestrator started event is used primarily for updating the current time as reported
	// by the orchestration context APIs.
	wi.State.AddEvent(helpers.NewOrchestratorStartedEvent())

	// Each orchestration instance gets its own distributed tracing span. However, the implementation of
	// endOrchestratorSpan will "cancel" the span mark the span as "unsampled" if the orchestration isn't
	// complete. This is part of the strategy for producing one span for the entire orchestration execution,
	// which isn't something that's natively supported by OTel today.
	ctx, span := w.startOrResumeOrchestratorSpan(ctx, wi)

	// New events from the work item are appended to the orchestration state, with duplicates automatically
	// filtered out. If all events are filtered out, return false so that the caller knows not to execute
	// the orchestration logic for an empty set of events.
	added := 0
	for _, e := range wi.NewEvents {
		if err := wi.State.AddEvent(e); err != nil {
			if err == ErrDuplicateEvent {
				w.logger.Warnf("%v: dropping duplicate event: %v", wi.InstanceID, e)
			} else {
				w.logger.Warnf("%v: dropping event: %v, %v", wi.InstanceID, e, err)
			}
		} else {
			added++
		}

		// Special case logic for specific event types
		if es := e.GetExecutionStarted(); es != nil {
			w.logger.Infof("%v: starting new '%s' instance with ID = '%s'.", wi.InstanceID, es.Name, es.OrchestrationInstance.InstanceId)
		} else if timerFired := e.GetTimerFired(); timerFired != nil {
			// Timer spans are created and completed once the TimerFired event is received.
			// TODO: Ideally we don't emit spans for cancelled timers. Is there a way to support this?
			if err := helpers.StartAndEndNewTimerSpan(ctx, timerFired, e.Timestamp.AsTime(), string(wi.InstanceID)); err != nil {
				w.logger.Warnf("%v: failed to generate distributed trace span for durable timer: %v", wi.InstanceID, err)
			}
		}
	}

	if added == 0 {
		w.logger.Warnf("%v: all new events were dropped", wi.InstanceID)
		return ctx, span, false
	}

	return ctx, span, true
}

func getOrchestrationStateDescription(wi *OrchestrationWorkItem) string {
	name, err := wi.State.Name()
	if err != nil {
		if len(wi.NewEvents) > 0 {
			name = wi.NewEvents[0].GetExecutionStarted().GetName()
		}
	}
	if name == "" {
		name = "(unknown)"
	}

	ageStr := "(new)"
	createdAt, err := wi.State.CreatedTime()
	if err == nil {
		age := time.Since(createdAt)

		if age > 0 {
			ageStr = age.Round(time.Second).String()
		}
	}
	status := helpers.ToRuntimeStatusString(wi.State.RuntimeStatus())
	return fmt.Sprintf("name=%s, status=%s, events=%d, age=%s", name, status, len(wi.State.OldEvents()), ageStr)
}

func (w *orchestratorProcessor) startOrResumeOrchestratorSpan(ctx context.Context, wi *OrchestrationWorkItem) (context.Context, trace.Span) {
	// Get the trace context from the ExecutionStarted history event
	var ptc *protos.TraceContext
	var es *protos.ExecutionStartedEvent
	if es = wi.State.startEvent; es != nil {
		ptc = wi.State.startEvent.ParentTraceContext
	} else {
		for _, e := range wi.NewEvents {
			if es = e.GetExecutionStarted(); es != nil {
				ptc = es.ParentTraceContext
				break
			}
		}
	}

	if ptc == nil {
		return ctx, helpers.NoopSpan()
	}

	ctx, err := helpers.ContextFromTraceContext(ctx, ptc)
	if err != nil {
		w.logger.Warnf("%v: failed to parse trace context: %v", wi.InstanceID, err)
		return ctx, helpers.NoopSpan()
	}

	// start a new span from the updated go context
	var span trace.Span
	ctx, span = helpers.StartNewRunOrchestrationSpan(ctx, es, wi.State.getStartedTime())

	// Assign or rehydrate the long-running orchestration span ID
	if es.OrchestrationSpanID == nil {
		// On the initial execution, assign the orchestration span ID to be the
		// randomly generated span ID value. This will be persisted in the orchestration history
		// and referenced on the next replay.
		es.OrchestrationSpanID = wrapperspb.String(span.SpanContext().SpanID().String())
	} else {
		// On subsequent executions, replace the auto-generated span ID with the orchestration
		// span ID. This allows us to have one long-running span that survives multiple replays
		// and process failures.
		if orchestratorSpanID, err := trace.SpanIDFromHex(es.OrchestrationSpanID.Value); err == nil {
			helpers.ChangeSpanID(span, orchestratorSpanID)
		}
	}

	return ctx, span
}

func (w *orchestratorProcessor) endOrchestratorSpan(ctx context.Context, wi *OrchestrationWorkItem, span trace.Span, continuedAsNew bool) {
	if wi.State.IsCompleted() {
		if fd, err := wi.State.FailureDetails(); err == nil {
			span.SetStatus(codes.Error, fd.ErrorMessage)
		}
		span.SetAttributes(attribute.KeyValue{
			Key:   "durabletask.runtime_status",
			Value: attribute.StringValue(helpers.ToRuntimeStatusString(wi.State.RuntimeStatus())),
		})
		addNotableEventsToSpan(wi.State.OldEvents(), span)
		addNotableEventsToSpan(wi.State.NewEvents(), span)
	} else if continuedAsNew {
		span.SetAttributes(attribute.KeyValue{
			Key:   "durabletask.runtime_status",
			Value: attribute.StringValue(helpers.ToRuntimeStatusString(protos.OrchestrationStatus_ORCHESTRATION_STATUS_CONTINUED_AS_NEW)),
		})
	} else {
		// Cancel the span - we want to publish it only when an orchestration
		// completes or when it continue-as-new's.
		helpers.CancelSpan(span)
	}

	// We must always call End() on a span to ensure we don't leak resources.
	// See https://github.com/open-telemetry/opentelemetry-specification/blob/main/specification/trace/api.md#span-creation
	span.End()
}

// Adds notable events to the span that are interesting to the user.
// More info: https://opentelemetry.io/docs/instrumentation/go/manual/#events
func addNotableEventsToSpan(events []*protos.HistoryEvent, span trace.Span) {
	for _, e := range events {
		if eventRaised := e.GetEventRaised(); eventRaised != nil {
			eventByteCount := len(eventRaised.Input.GetValue())
			span.AddEvent(
				"Received external event",
				trace.WithTimestamp(e.Timestamp.AsTime()),
				trace.WithAttributes(attribute.String("name", eventRaised.Name), attribute.Int("size", eventByteCount)))
		} else if suspended := e.GetExecutionSuspended(); suspended != nil {
			span.AddEvent(
				"Execution suspended",
				trace.WithTimestamp(e.Timestamp.AsTime()),
				trace.WithAttributes(attribute.String("reason", suspended.Input.GetValue())))
		} else if resumed := e.GetExecutionResumed(); resumed != nil {
			span.AddEvent(
				"Execution resumed",
				trace.WithTimestamp(e.Timestamp.AsTime()),
				trace.WithAttributes(attribute.String("reason", resumed.Input.GetValue())))
		}
	}
}
