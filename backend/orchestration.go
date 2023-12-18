package backend

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
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

// OrchestrationWorkerOption is a function that configures an orchestrator worker.
type OrchestrationWorkerOption func(*orchestrationWorkerConfig)

type orchestrationWorkerConfig struct {
	workerOptions  []NewTaskWorkerOptions
	chunkingConfig ChunkingConfiguration
}

// WithChunkingConfiguration sets the chunking configuration for the orchestrator worker.
// Use this option to configure how the orchestrator worker will chunk large history lists.
func WithChunkingConfiguration(config ChunkingConfiguration) OrchestrationWorkerOption {
	return func(p *orchestrationWorkerConfig) {
		p.chunkingConfig = config
	}
}

// WithMaxConcurrentOrchestratorInvocations sets the maximum number of orchestrations that can be invoked
// concurrently by the orchestrator worker. If this limit is exceeded, the worker will block until the number
// of concurrent orchestrations drops below the limit.
//
// Note that this limit is applied to the number of orchestrations that are being invoked concurrently, not
// the number of orchestrations that are in a running state. For example, if an orchestration is waiting for
// an external event, a timer, or an activity, it will not count against this limit.
//
// If this value is set to 0 or less, then the number of parallel orchestrations is unlimited.
func WithMaxConcurrentOrchestratorInvocations(n int32) OrchestrationWorkerOption {
	return func(p *orchestrationWorkerConfig) {
		p.workerOptions = append(p.workerOptions, WithMaxParallelism(n))
	}
}

type orchestratorProcessor struct {
	be             Backend
	executor       OrchestratorExecutor
	logger         Logger
	chunkingConfig ChunkingConfiguration
}

func NewOrchestrationWorker(be Backend, executor OrchestratorExecutor, logger Logger, opts ...OrchestrationWorkerOption) TaskWorker {
	config := &orchestrationWorkerConfig{}
	for _, configure := range opts {
		configure(config)
	}

	processor := &orchestratorProcessor{
		be:             be,
		executor:       executor,
		logger:         logger,
		chunkingConfig: config.chunkingConfig,
	}
	return NewTaskWorker(be, processor, logger, config.workerOptions...)
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

	continueAsNewLoop:
		for continueAsNewCount := 0; ; continueAsNewCount++ {
			if continueAsNewCount > 0 {
				w.logger.Debugf("%v: continuing-as-new with %d event(s): %s", wi.InstanceID, len(wi.State.NewEvents()), helpers.HistoryListSummary(wi.State.NewEvents()))
			} else {
				w.logger.Debugf("%v: invoking orchestrator with %d event(s): %s", wi.InstanceID, len(wi.State.NewEvents()), helpers.HistoryListSummary(wi.State.NewEvents()))
			}

			// Run the user orchestrator code, providing the old history and new events together.
			results, err := w.executor.ExecuteOrchestrator(ctx, wi.InstanceID, wi.State.OldEvents(), wi.State.NewEvents())
			if err != nil {
				return fmt.Errorf("error executing orchestrator: %w", err)
			}
			w.logger.Debugf("%v: orchestrator returned %d action(s): %s", wi.InstanceID, len(results.Response.Actions), helpers.ActionListSummary(results.Response.Actions))

			// Apply the results of the orchestrator execution to the orchestration state.
			wi.State.AddActions(results.Response.Actions)

			// Now we need to take all the changes we just made to the state and persist them to the backend storage.
			// This is done in a loop because the list of actions may be too large to be committed in a single transaction.
			// If this happens, we'll commit the changes in chunks until we've committed all of them.
			addedEvents := 0
			for {
				tc := helpers.TraceContextFromSpan(span)
				changes, err := wi.State.ProcessChanges(w.chunkingConfig, tc, w.logger)
				if errors.Is(err, ErrContinuedAsNew) {
					// The orchestrator did a continue-as-new, which means we should re-run the orchestator with the new state.
					// No changes are committed to the backend until the orchestration returns a non-continue-as-new result.
					// Safety check: see if the user code might be an infinite continue-as-new loop. 10K is the arbitrary threshold we use.
					const MaxContinueAsNewCount = 10000
					if continueAsNewCount >= MaxContinueAsNewCount {
						// Fail the orchestration since we don't want it to be stuck in an infinite loop
						continueAsNewError := fmt.Errorf("exceeded tight-loop continue-as-new limit of %d iterations", MaxContinueAsNewCount)
						w.logger.Warnf("%v: terminating orchestration: %v", wi.InstanceID, err)
						return w.failOrchestration(ctx, wi, continueAsNewError, tc)
					}

					// We create a new trace span for every continue-as-new
					w.endOrchestratorSpan(ctx, wi, span, true)
					ctx, span = w.startOrResumeOrchestratorSpan(ctx, wi)
					continue continueAsNewLoop
				} else if err != nil {
					// Any other error is assumed to be non-recoverable, so we fail the orchestration
					return w.failOrchestration(ctx, wi, err, tc)
				}

				if !changes.IsEmpty() {
					// Commit the changes to the backend
					if len(changes.NewEvents) > 0 {
						w.logger.Debugf("%v: committing %d new history event(s) to the backend (partial=%v): %v", wi.InstanceID, len(changes.NewEvents), changes.IsPartial, helpers.HistoryListSummary(changes.NewEvents))
					}
					if len(changes.NewTasks) > 0 {
						w.logger.Debugf("%v: committing %d new scheduled task(s) to the backend (partial=%v): %v", wi.InstanceID, len(changes.NewTasks), changes.IsPartial, helpers.HistoryListSummary(changes.NewTasks))
					}
					if len(changes.NewTimers) > 0 {
						w.logger.Debugf("%v: committing %d new timer(s) to the backend (partial=%v): %v", wi.InstanceID, len(changes.NewTimers), changes.IsPartial, helpers.HistoryListSummary(changes.NewTimers))
					}
					if len(changes.NewMessages) > 0 {
						w.logger.Debugf("%v: committing %d new message(s) to the backend (partial=%v): %v", wi.InstanceID, len(changes.NewMessages), changes.IsPartial, messageListSummary(changes.NewMessages))
					}

					changes.HistoryStartIndex = addedEvents
					if err := w.be.CompleteOrchestrationWorkItem(ctx, wi, changes); err != nil {
						return fmt.Errorf("failed to complete orchestration work item: %w", err)
					}

					addedEvents += len(changes.NewEvents)

					// Keep looping until we've committed all the changes
					continue
				}

				if wi.State.IsCompleted() {
					name, _ := wi.State.Name()
					w.logger.Infof("%v: '%s' completed with a %s status.", wi.InstanceID, name, helpers.ToRuntimeStatusString(wi.State.RuntimeStatus()))
				}

				break continueAsNewLoop // break out of the process results loop if no errors
			}
		}
	}
	return nil
}

func (p *orchestratorProcessor) failOrchestration(ctx context.Context, wi *OrchestrationWorkItem, err error, tc *protos.TraceContext) error {
	p.logger.Warnf("%v: setting orchestration as failed: %v", wi.InstanceID, err)
	wi.State.SetFailed(err)

	changes, err := wi.State.ProcessChanges(p.chunkingConfig, tc, p.logger)
	if err != nil {
		// This is assumed to be non-recoverable, so we swallow it and log a message
		p.logger.Errorf("%v: failed to fail orchestration: %v", wi.InstanceID, err)
		return nil
	}

	return p.be.CompleteOrchestrationWorkItem(ctx, wi, changes)
}

// AbandonWorkItem implements TaskProcessor
func (p *orchestratorProcessor) AbandonWorkItem(ctx context.Context, wi WorkItem) error {
	owi := wi.(*OrchestrationWorkItem)
	return p.be.AbandonOrchestrationWorkItem(ctx, owi)
}

// applyWorkItem adds the new events from the work item to the orchestration state.
//
// The returned context will contain a new distributed tracing span that should be used for all
// subsequent operations. The returned span will be nil if the work item was dropped.
//
// The returned boolean will be false if the work item was dropped.
//
// The caller is responsible for calling endOrchestratorSpan on the returned span.
func (w *orchestratorProcessor) applyWorkItem(ctx context.Context, wi *OrchestrationWorkItem) (context.Context, trace.Span, bool) {
	// Ignore work items for orchestrations that are completed or are in a corrupted state.
	if !wi.State.IsValid() {
		w.logger.Warnf("%v: orchestration state is invalid; dropping work item", wi.InstanceID)
		return nil, nil, false
	} else if wi.State.IsCompleted() {
		w.logger.Infof("%v: dropping work item(s) for %s orchestration: %s", wi.InstanceID, helpers.ToRuntimeStatusString(wi.State.RuntimeStatus()), helpers.HistoryListSummary(wi.NewEvents))
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

func messageListSummary(messages []OrchestratorMessage) string {
	var sb strings.Builder
	sb.WriteString("[")
	for i, m := range messages {
		if i > 0 {
			sb.WriteString(", ")
		}
		if i >= 10 {
			sb.WriteString("...")
			break
		}
		name := helpers.GetHistoryEventTypeName(m.HistoryEvent)
		sb.WriteString(name)
		taskID := helpers.GetTaskId(m.HistoryEvent)
		if taskID >= 0 {
			sb.WriteRune('#')
			sb.WriteString(strconv.FormatInt(int64(taskID), 10))
		}
	}
	sb.WriteString("]")
	return sb.String()
}
