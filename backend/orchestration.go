package backend

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
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

// EntityExecutor is an optional extension of [Executor] that adds entity execution support.
// If the executor passed to [NewOrchestrationWorker] implements this interface,
// entity work items will be automatically dispatched.
type EntityExecutor interface {
	Executor
	ExecuteEntity(context.Context, api.InstanceID, *protos.EntityBatchRequest) (*protos.EntityBatchResult, error)
}

type orchestratorProcessor struct {
	be             Backend
	executor       OrchestratorExecutor
	entityExecutor EntityExecutor
	logger         Logger
}

func NewOrchestrationWorker(be Backend, executor OrchestratorExecutor, logger Logger, opts ...NewTaskWorkerOptions) TaskWorker {
	processor := &orchestratorProcessor{
		be:       be,
		executor: executor,
		logger:   logger,
	}
	// If the executor also implements EntityExecutor, use it for entity dispatch
	if ee, ok := executor.(EntityExecutor); ok {
		processor.entityExecutor = ee
	}
	return NewTaskWorker(processor, logger, opts...)
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

	// Detect entity instances by their "@name@key" prefix and route to entity executor
	if w.entityExecutor != nil {
		if _, err := api.EntityIDFromString(string(wi.InstanceID)); err == nil {
			return w.processEntityWorkItem(ctx, wi)
		}
	}

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

	var terminateEvent *protos.ExecutionTerminatedEvent = nil
	for _, e := range wi.NewEvents {
		if et := e.GetExecutionTerminated(); et != nil {
			terminateEvent = et
			break
		}
	}
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
	if terminateEvent != nil && wi.State.IsCompleted() {
		if err := terminateSubOrchestrationInstances(ctx, w.be, wi.InstanceID, wi.State, terminateEvent); err != nil {
			return err
		}
	}
	return nil
}

// CompleteWorkItem implements TaskProcessor
func (p *orchestratorProcessor) CompleteWorkItem(ctx context.Context, wi WorkItem) error {
	owi := wi.(*OrchestrationWorkItem)

	// Auto-create entity instances for any pending messages targeting entity IDs.
	// This ensures CallEntity from orchestrations works even when the target entity
	// doesn't exist yet, without requiring backend-specific entity support.
	for _, msg := range owi.State.PendingMessages() {
		if msg.HistoryEvent.GetExecutionStarted() != nil {
			continue // sub-orchestration creation, handled by the backend
		}
		entityID, err := api.EntityIDFromString(msg.TargetInstanceID)
		if err != nil {
			continue // not an entity ID
		}
		startEvent := helpers.NewExecutionStartedEvent(entityID.Name, msg.TargetInstanceID, nil, nil, nil, nil)
		if createErr := p.be.CreateOrchestrationInstance(ctx, startEvent, WithOrchestrationIdReusePolicy(&protos.OrchestrationIdReusePolicy{
			Action:          protos.CreateOrchestrationAction_IGNORE,
			OperationStatus: []protos.OrchestrationStatus{protos.OrchestrationStatus_ORCHESTRATION_STATUS_RUNNING},
		})); createErr != nil && !errors.Is(createErr, api.ErrDuplicateInstance) && !errors.Is(createErr, api.ErrIgnoreInstance) {
			p.logger.Warnf("%v: failed to auto-create entity instance %s: %v", owi.InstanceID, msg.TargetInstanceID, createErr)
		}
	}

	return p.be.CompleteOrchestrationWorkItem(ctx, owi)
}

// AbandonWorkItem implements TaskProcessor
func (p *orchestratorProcessor) AbandonWorkItem(ctx context.Context, wi WorkItem) error {
	owi := wi.(*OrchestrationWorkItem)
	return p.be.AbandonOrchestrationWorkItem(ctx, owi)
}

// processEntityWorkItem handles orchestration work items that represent entity instances.
// Entity instances are identified by their "@name@key" instance ID format.
// This method converts incoming orchestration events into entity operations,
// executes them via the EntityExecutor, and writes the results back as orchestration state.
func (w *orchestratorProcessor) processEntityWorkItem(ctx context.Context, wi *OrchestrationWorkItem) error {
	iid := string(wi.InstanceID)
	w.logger.Debugf("%v: processing as entity work item", wi.InstanceID)

	// Load existing state if needed
	if wi.State == nil {
		state, err := w.be.GetOrchestrationRuntimeState(ctx, wi)
		if err != nil {
			return fmt.Errorf("failed to load entity state: %w", err)
		}
		wi.State = state
	}

	// Extract entity state from the orchestration metadata (stored as CustomStatus)
	var entityState *wrapperspb.StringValue
	meta, err := w.be.GetOrchestrationMetadata(ctx, wi.InstanceID)
	if err == nil && meta != nil && meta.SerializedCustomStatus != "" {
		entityState = wrapperspb.String(meta.SerializedCustomStatus)
	}

	// entityCallInfo tracks response routing for CallEntity requests.
	type entityCallInfo struct {
		callerInstanceID string
		requestID        string
		isSignal         bool
	}

	// Convert new EventRaised and EventSent events into entity OperationRequests.
	// Events use the .NET-compatible protocol: event name is "op" and the payload
	// is a JSON EntityRequestMessage containing routing and operation information.
	var operations []*protos.OperationRequest
	var callInfos []entityCallInfo // parallel array
	for _, e := range wi.NewEvents {
		var eventName string
		var eventInput *wrapperspb.StringValue

		if er := e.GetEventRaised(); er != nil {
			eventName = er.Name
			eventInput = er.Input
		} else if es := e.GetEventSent(); es != nil {
			eventName = es.Name
			eventInput = es.Input
		} else {
			continue
		}

		if !strings.EqualFold(eventName, helpers.EntityRequestEventName) {
			continue
		}

		var reqMsg helpers.EntityRequestMessage
		if eventInput == nil || eventInput.GetValue() == "" {
			w.logger.Warnf("%v: received 'op' event with no payload, skipping", wi.InstanceID)
			continue
		}
		if err := json.Unmarshal([]byte(eventInput.GetValue()), &reqMsg); err != nil {
			w.logger.Warnf("%v: failed to parse RequestMessage: %v", wi.InstanceID, err)
			continue
		}

		var inputVal *wrapperspb.StringValue
		if reqMsg.Input != "" {
			inputVal = wrapperspb.String(reqMsg.Input)
		}

		operations = append(operations, &protos.OperationRequest{
			Operation: reqMsg.Operation,
			RequestId: reqMsg.ID,
			Input:     inputVal,
		})
		callInfos = append(callInfos, entityCallInfo{
			callerInstanceID: reqMsg.ParentInstanceID,
			requestID:        reqMsg.ID,
			isSignal:         reqMsg.IsSignal,
		})
	}

	// Ensure the entity orchestration instance exists in state
	if wi.State.startEvent == nil {
		entityID, err := api.EntityIDFromString(iid)
		if err != nil {
			return fmt.Errorf("invalid entity instance ID format: %w", err)
		}
		startEvent := helpers.NewExecutionStartedEvent(entityID.Name, iid, nil, nil, nil, nil)
		if err := wi.State.AddEvent(helpers.NewOrchestratorStartedEvent()); err != nil {
			return fmt.Errorf("failed to add orchestrator started event: %w", err)
		}
		if err := wi.State.AddEvent(startEvent); err != nil {
			return fmt.Errorf("failed to initialize entity state: %w", err)
		}
	}

	// Add incoming events to state history
	for _, e := range wi.NewEvents {
		if err := wi.State.AddEvent(e); err != nil {
			if !errors.Is(err, ErrDuplicateEvent) {
				return fmt.Errorf("failed to add event to entity state: %w", err)
			}
			w.logger.Debugf("%v: skipping duplicate event in entity history", wi.InstanceID)
		}
	}

	if len(operations) == 0 {
		w.logger.Debugf("%v: no entity operations to process", wi.InstanceID)
		return nil
	}

	// Build and execute the entity batch
	batchReq := &protos.EntityBatchRequest{
		InstanceId:  iid,
		EntityState: entityState,
		Operations:  operations,
	}

	batchResult, err := w.entityExecutor.ExecuteEntity(ctx, wi.InstanceID, batchReq)
	if err != nil {
		return fmt.Errorf("failed to execute entity: %w", err)
	}
	if batchResult.FailureDetails != nil {
		w.logger.Errorf("%v: non-retriable entity execution failure: %s", wi.InstanceID, batchResult.FailureDetails.ErrorMessage)
		return nil
	}

	// Save entity state as the orchestration's custom status
	wi.State.CustomStatus = batchResult.EntityState

	// Send results back to calling orchestrations (for CallEntity requests)
	for i, info := range callInfos {
		if info.isSignal || info.callerInstanceID == "" || info.requestID == "" {
			continue // signal, no response needed
		}
		if i >= len(batchResult.Results) {
			break
		}

		// Build the .NET-compatible EntityResponseMessage payload.
		var resp helpers.EntityResponseMessage
		if success := batchResult.Results[i].GetSuccess(); success != nil {
			if success.Result != nil {
				resp.Result = success.Result.GetValue()
			}
		} else if failure := batchResult.Results[i].GetFailure(); failure != nil {
			resp.ErrorMessage = failure.FailureDetails.GetErrorMessage()
		}

		respJSON, err := json.Marshal(resp)
		if err != nil {
			w.logger.Warnf("%v: failed to marshal entity response: %v", wi.InstanceID, err)
			continue
		}

		// Send the result as an EventRaised to the caller orchestration, using the requestID
		// as the event name so it matches the WaitForSingleEvent in CallEntity.
		responseEvent := helpers.NewEventRaisedEvent(info.requestID, wrapperspb.String(string(respJSON)))
		if err := w.be.AddNewOrchestrationEvent(ctx, api.InstanceID(info.callerInstanceID), responseEvent); err != nil {
			w.logger.Warnf("%v: failed to send entity response to %s: %v", wi.InstanceID, info.callerInstanceID, err)
		}
	}

	// Process actions from the entity batch result (signals to other entities, new orchestrations)
	for _, action := range batchResult.Actions {
		if signal := action.GetSendSignal(); signal != nil {
			// Wrap entity-to-entity signals in the .NET-compatible EntityRequestMessage format.
			sigMsg := helpers.EntityRequestMessage{
				ID:        uuid.New().String(),
				IsSignal:  true,
				Operation: signal.Name,
			}
			if signal.Input != nil {
				sigMsg.Input = signal.Input.GetValue()
			}
			sigJSON, err := json.Marshal(sigMsg)
			if err != nil {
				w.logger.Warnf("%v: failed to marshal signal request: %v", wi.InstanceID, err)
				continue
			}
			e := helpers.NewEventRaisedEvent(helpers.EntityRequestEventName, wrapperspb.String(string(sigJSON)))
			if signal.ScheduledTime != nil {
				e.Timestamp = signal.ScheduledTime
			}
			if err := w.be.AddNewOrchestrationEvent(ctx, api.InstanceID(signal.InstanceId), e); err != nil {
				w.logger.Warnf("%v: failed to send entity signal to %s: %v", wi.InstanceID, signal.InstanceId, err)
			}
		} else if startOrch := action.GetStartNewOrchestration(); startOrch != nil {
			orchInstanceID := startOrch.InstanceId
			if orchInstanceID == "" {
				id := uuid.New()
				orchInstanceID = hex.EncodeToString(id[:])
			}
			e := helpers.NewExecutionStartedEvent(startOrch.Name, orchInstanceID, startOrch.Input, nil, nil, startOrch.ScheduledTime)
			if err := w.be.CreateOrchestrationInstance(ctx, e); err != nil {
				w.logger.Warnf("%v: failed to start orchestration %s: %v", wi.InstanceID, orchInstanceID, err)
			}
		}
	}

	w.logger.Debugf("%v: entity processed %d operation(s)", wi.InstanceID, len(operations))
	return nil
}

func (w *orchestratorProcessor) applyWorkItem(ctx context.Context, wi *OrchestrationWorkItem) (context.Context, trace.Span, bool) {
	// Ignore work items for orchestrations that are completed or are in a corrupted state.
	switch {
	case !wi.State.IsValid():
		w.logger.Warnf("%v: orchestration state is invalid; dropping work item", wi.InstanceID)
		return nil, nil, false
	case wi.State.IsCompleted():
		w.logger.Warnf("%v: orchestration already completed; dropping work item", wi.InstanceID)
		return nil, nil, false
	case len(wi.NewEvents) == 0:
		w.logger.Warnf("%v: the work item had no events!", wi.InstanceID)
	}

	// The orchestrator started event is used primarily for updating the current time as reported
	// by the orchestration context APIs.
	if err := wi.State.AddEvent(helpers.NewOrchestratorStartedEvent()); err != nil {
		w.logger.Warnf("%v: failed to add orchestrator started event: %v", wi.InstanceID, err)
	}

	// Each orchestration instancegets its own distributed tracing span. However, the implementation of
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
			if errors.Is(err, ErrDuplicateEvent) {
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
	switch {
	case wi.State.IsCompleted():
		if fd, err := wi.State.FailureDetails(); err == nil {
			span.SetStatus(codes.Error, fd.ErrorMessage)
		}
		span.SetAttributes(attribute.KeyValue{
			Key:   "durabletask.runtime_status",
			Value: attribute.StringValue(helpers.ToRuntimeStatusString(wi.State.RuntimeStatus())),
		})
		addNotableEventsToSpan(wi.State.OldEvents(), span)
		addNotableEventsToSpan(wi.State.NewEvents(), span)
	case continuedAsNew:
		span.SetAttributes(attribute.KeyValue{
			Key:   "durabletask.runtime_status",
			Value: attribute.StringValue(helpers.ToRuntimeStatusString(protos.OrchestrationStatus_ORCHESTRATION_STATUS_CONTINUED_AS_NEW)),
		})
	default:
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
