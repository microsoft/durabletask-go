package client

import (
	"context"
	"errors"
	"fmt"
	"io"
	"runtime/debug"
	"slices"
	"strings"
	"time"

	"github.com/microsoft/durabletask-go/api"
	"github.com/microsoft/durabletask-go/internal/contextprop"
	"github.com/microsoft/durabletask-go/internal/failure"
	"github.com/microsoft/durabletask-go/internal/helpers"
	"github.com/microsoft/durabletask-go/internal/largepayload"
	"github.com/microsoft/durabletask-go/internal/protos"
	"github.com/microsoft/durabletask-go/task"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

func (w *TaskHubGrpcWorker) consumeConnection(run *grpcWorkerRun, connection *grpcWorkerConnection) (bool, error) {
	observedMessage := false
	for {
		workItem, err := w.receiveWorkItem(connection)
		if err != nil {
			return observedMessage, err
		}
		observedMessage = true

		switch request := workItem.Request.(type) {
		case *protos.WorkItem_HealthPing:
			continue
		case *protos.WorkItem_OrchestratorRequest:
			if err := w.dispatchOrchestration(run, connection, workItem.GetCompletionToken(), request.OrchestratorRequest); err != nil {
				return observedMessage, err
			}
		case *protos.WorkItem_ActivityRequest:
			if err := w.dispatchActivity(run, connection, workItem.GetCompletionToken(), request.ActivityRequest); err != nil {
				return observedMessage, err
			}
		case *protos.WorkItem_EntityRequest:
			if err := w.dispatchEntity(run, connection, workItem.GetCompletionToken(), func(ctx context.Context) {
				w.processEntityBatch(ctx, connection.client, workItem.GetCompletionToken(), request.EntityRequest, nil)
			}); err != nil {
				return observedMessage, err
			}
		case *protos.WorkItem_EntityRequestV2:
			if err := w.dispatchEntity(run, connection, workItem.GetCompletionToken(), func(ctx context.Context) {
				w.processEntityV2(ctx, connection.client, workItem.GetCompletionToken(), request.EntityRequestV2)
			}); err != nil {
				return observedMessage, err
			}
		default:
			w.logger.Warnf("received unknown work item type with completion token present=%t", workItem.GetCompletionToken() != "")
		}
	}
}

func (w *TaskHubGrpcWorker) receiveWorkItem(connection *grpcWorkerConnection) (*protos.WorkItem, error) {
	workItem, err := recvBeforeSilenceTimeout(connection.stream.Recv, connection.cancelStream, w.options.silentDisconnectTimeout)
	if err != nil {
		return nil, err
	}
	if workItem == nil {
		return nil, status.Error(codes.Internal, "received a nil work item")
	}
	return workItem, nil
}

// recvBeforeSilenceTimeout receives one message from a server stream, canceling
// the stream and reporting errSilentDisconnect if nothing arrives before the
// configured silent disconnect timeout. A message that is delivered
// concurrently with the timeout is still returned: the stream is already
// canceled and the following receive reports the disconnect, so the caller
// never silently drops a delivered work item that the service considers
// dispatched.
//
// A terminal error that races the timeout is only rewritten to
// errSilentDisconnect when it is the cancellation the timer itself induced.
// Any other status, such as Unauthenticated or PermissionDenied, is propagated
// rather than masked as a silent disconnect.
func recvBeforeSilenceTimeout[T any](recv func() (T, error), cancelStream context.CancelFunc, timeout time.Duration) (T, error) {
	timedOut := make(chan struct{})
	timer := time.AfterFunc(timeout, func() {
		cancelStream()
		close(timedOut)
	})
	message, err := recv()
	if timer.Stop() {
		return message, err
	}
	<-timedOut
	if err == nil {
		return message, nil
	}
	if !isStreamCancellation(err) {
		return message, err
	}
	var zero T
	return zero, errSilentDisconnect
}

// isStreamCancellation reports whether err is the context cancellation that
// canceling the stream produces, as opposed to a status the service returned on
// its own.
func isStreamCancellation(err error) bool {
	return errors.Is(err, context.Canceled) || status.Code(err) == codes.Canceled
}

func (w *TaskHubGrpcWorker) dispatchOrchestration(
	run *grpcWorkerRun,
	connection *grpcWorkerConnection,
	completionToken string,
	request *protos.OrchestratorRequest,
) error {
	return w.dispatch(run, connection, run.orchestrationSlots,
		func(ctx context.Context) { w.abandonOrchestration(ctx, connection.client, completionToken) },
		func(ctx context.Context) { w.processOrchestration(ctx, connection.client, completionToken, request) },
	)
}

func (w *TaskHubGrpcWorker) dispatchActivity(
	run *grpcWorkerRun,
	connection *grpcWorkerConnection,
	completionToken string,
	request *protos.ActivityRequest,
) error {
	return w.dispatch(run, connection, run.activitySlots,
		func(ctx context.Context) { w.abandonActivity(ctx, connection.client, completionToken) },
		func(ctx context.Context) { w.processActivity(ctx, connection.client, completionToken, request) },
	)
}

func (w *TaskHubGrpcWorker) dispatchEntity(
	run *grpcWorkerRun,
	connection *grpcWorkerConnection,
	completionToken string,
	process func(context.Context),
) error {
	return w.dispatch(
		run,
		connection,
		run.entitySlots,
		func(ctx context.Context) { w.abandonEntity(ctx, connection.client, completionToken) },
		process,
	)
}

// dispatch reserves a concurrency slot and runs process in the background under
// the processing context, so a graceful drain can still complete in-flight work.
// If intake is canceled before a slot is free, the work item is abandoned instead.
func (w *TaskHubGrpcWorker) dispatch(
	run *grpcWorkerRun,
	connection *grpcWorkerConnection,
	slots chan struct{},
	abandon func(context.Context),
	process func(context.Context),
) error {
	select {
	case slots <- struct{}{}:
	case <-run.intakeCtx.Done():
		abandon(run.processingCtx)
		return run.intakeCtx.Err()
	}

	run.pending.Add(1)
	connection.pending.Add(1)
	go func() {
		defer func() {
			<-slots
			connection.pending.Done()
			run.pending.Done()
		}()
		process(run.processingCtx)
	}()
	return nil
}

func (w *TaskHubGrpcWorker) processOrchestration(
	ctx context.Context,
	client protos.TaskHubSidecarServiceClient,
	completionToken string,
	request *protos.OrchestratorRequest,
) {
	pastEvents := request.PastEvents
	if request.RequiresHistoryStreaming {
		history, err := w.streamHistory(ctx, client, request)
		if err != nil {
			w.logger.Errorf("%s: failed to stream required orchestration history: %v", request.InstanceId, err)
			if errors.Is(err, ErrStreamedHistoryLimitExceeded) {
				_ = w.wait(ctx, w.options.transientRetryMaxDelay)
			}
			w.abandonOrchestration(ctx, client, completionToken)
			return
		}
		pastEvents = history
	}
	payloadRequest := &protos.OrchestratorRequest{
		PastEvents: pastEvents,
		NewEvents:  request.NewEvents,
	}
	if err := largepayload.TransformOrchestratorRequest(ctx, w.options.largePayloads, payloadRequest); err != nil {
		w.logger.Errorf("%s: failed to hydrate orchestration work item payloads: %v", request.InstanceId, err)
		w.abandonOrchestration(ctx, client, completionToken)
		return
	}
	if w.options.workItemFilters != nil {
		name, version, ok := orchestrationWorkItemIdentity(pastEvents, request.NewEvents)
		if ok && !matchesWorkItemFilters(w.options.workItemFilters, true, name, version) {
			w.logger.Warnf("%s: orchestration work item does not match configured filters; abandoning it", request.InstanceId)
			_ = w.wait(ctx, w.options.reconnectBaseDelay)
			w.abandonOrchestration(ctx, client, completionToken)
			return
		}
	}

	results, err := w.executor.ExecuteOrchestrator(
		ctx,
		api.InstanceID(request.InstanceId),
		pastEvents,
		request.NewEvents,
		request.EntityParameters,
	)
	var delayed workItemAbandonDelayError
	if errors.As(err, &delayed) {
		w.logger.Warnf("%s: orchestration work item rejected; abandoning it: %v", request.InstanceId, err)
		_ = w.wait(ctx, delayed.WorkItemAbandonDelay())
		w.abandonOrchestration(ctx, client, completionToken)
		return
	}
	if err != nil && ctx.Err() != nil {
		w.logger.Warnf("%s: orchestration execution canceled; abandoning work item", request.InstanceId)
		w.abandonOrchestration(ctx, client, completionToken)
		return
	}

	response := &protos.OrchestratorResponse{
		InstanceId:                request.InstanceId,
		CompletionToken:           completionToken,
		OrchestrationTraceContext: request.OrchestrationTraceContext,
	}
	switch {
	case err != nil:
		response.Actions = []*protos.OrchestratorAction{helpers.NewCompleteOrchestrationAction(
			-1,
			protos.OrchestrationStatus_ORCHESTRATION_STATUS_FAILED,
			wrapperspb.String("An internal error occurred while executing the orchestration."),
			nil,
			failure.FromError(err),
		)}
	case results == nil || results.Response == nil:
		response.Actions = []*protos.OrchestratorAction{helpers.NewCompleteOrchestrationAction(
			-1,
			protos.OrchestrationStatus_ORCHESTRATION_STATUS_FAILED,
			wrapperspb.String("The orchestration executor returned no response."),
			nil,
			&protos.TaskFailureDetails{
				ErrorType:    "MissingOrchestratorResponse",
				ErrorMessage: "the orchestration executor returned no response",
			},
		)}
	default:
		response = results.Response
		response.InstanceId = request.InstanceId
		response.CompletionToken = completionToken
		if response.OrchestrationTraceContext == nil {
			response.OrchestrationTraceContext = request.OrchestrationTraceContext
		}
		if err := populateOrchestratorActionTraceContexts(
			response.Actions,
			pastEvents,
			request.NewEvents,
		); err != nil {
			w.logger.Warnf("%s: failed to populate action trace contexts: %v", request.InstanceId, err)
		}
	}
	if err := largepayload.TransformOrchestratorResponse(ctx, w.options.largePayloads, response); err != nil {
		w.logger.Errorf("%s: failed to externalize orchestration response payloads: %v", request.InstanceId, err)
		w.abandonOrchestration(ctx, client, completionToken)
		return
	}

	response = failOversizedOrchestratorResponse(response, w.options.maxOrchestratorCompletionBytes)
	err = w.executeRPCWithRetry(ctx, "complete orchestration task", func(callCtx context.Context) error {
		_, callErr := client.CompleteOrchestratorTask(callCtx, response)
		return callErr
	})
	if err != nil {
		if isWorkItemGone(err) {
			w.logger.Warnf("%s: orchestration work item was no longer available before completion", request.InstanceId)
			return
		}
		w.logger.Errorf("%s: failed to complete orchestration work item: %v", request.InstanceId, err)
		w.abandonOrchestration(ctx, client, completionToken)
	}
}

// failOversizedOrchestratorResponse converts a response that modern DTS cannot
// accept into a small, non-retriable orchestration failure. The Go worker does
// not use the deprecated legacy response-chunking fields; validation against the
// current DTS emulator confirmed that splitting is not a portable recovery path.
func failOversizedOrchestratorResponse(
	response *protos.OrchestratorResponse,
	maxBytes int,
) *protos.OrchestratorResponse {
	if response == nil || maxBytes <= 0 {
		return response
	}
	responseSize := proto.Size(response)
	if responseSize <= maxBytes {
		return response
	}
	boundMiB := float64(maxBytes) / (1024 * 1024)
	message := fmt.Sprintf(
		"orchestrator response with %d actions exceeds the %.2f MiB worker completion bound: %.2f MiB; reduce per-turn fan-out or configure large-payload externalization for action payloads",
		len(response.Actions), boundMiB, float64(responseSize)/(1024*1024),
	)
	for _, action := range response.Actions {
		if actionSize := proto.Size(action); actionSize > maxBytes {
			message = fmt.Sprintf(
				"orchestrator action %d exceeds the %.2f MiB worker completion bound: %.2f MiB; configure large-payload externalization for oversized action payloads",
				action.GetId(), boundMiB, float64(actionSize)/(1024*1024),
			)
			break
		}
	}
	return &protos.OrchestratorResponse{
		InstanceId:                response.InstanceId,
		CompletionToken:           response.CompletionToken,
		OrchestrationTraceContext: response.OrchestrationTraceContext,
		Actions: []*protos.OrchestratorAction{helpers.NewCompleteOrchestrationAction(
			-1,
			protos.OrchestrationStatus_ORCHESTRATION_STATUS_FAILED,
			nil,
			nil,
			&protos.TaskFailureDetails{
				ErrorType:      string(api.ErrorTypeOrchestratorResponseTooLarge),
				ErrorMessage:   message,
				IsNonRetriable: true,
			},
		)},
	}
}

func populateOrchestratorActionTraceContexts(
	actions []*protos.OrchestratorAction,
	pastEvents []*protos.HistoryEvent,
	newEvents []*protos.HistoryEvent,
) error {
	parent := executionStartedTraceContext(pastEvents, newEvents)
	if parent == nil {
		return nil
	}
	for _, action := range actions {
		if action == nil {
			continue
		}
		switch {
		case action.GetScheduleTask() != nil:
			scheduled := action.GetScheduleTask()
			if scheduled.ParentTraceContext == nil {
				traceContext, err := helpers.OrchestratorActionTraceContext(parent)
				if err != nil {
					return fmt.Errorf("schedule task action %d: %w", action.Id, err)
				}
				scheduled.ParentTraceContext = traceContext
			}
		case action.GetCreateSubOrchestration() != nil:
			created := action.GetCreateSubOrchestration()
			if created.ParentTraceContext == nil {
				traceContext, err := helpers.OrchestratorActionTraceContext(parent)
				if err != nil {
					return fmt.Errorf("create sub-orchestration action %d: %w", action.Id, err)
				}
				created.ParentTraceContext = traceContext
			}
		}
	}
	return nil
}

func executionStartedTraceContext(eventLists ...[]*protos.HistoryEvent) *protos.TraceContext {
	for _, events := range eventLists {
		for _, event := range events {
			if started := event.GetExecutionStarted(); started != nil {
				return started.GetParentTraceContext()
			}
		}
	}
	return nil
}

func (w *TaskHubGrpcWorker) streamHistory(
	ctx context.Context,
	client protos.TaskHubSidecarServiceClient,
	request *protos.OrchestratorRequest,
) ([]*protos.HistoryEvent, error) {
	historyCtx, cancelHistory := context.WithCancel(ctx)
	defer cancelHistory()
	stream, err := client.StreamInstanceHistory(historyCtx, &protos.StreamInstanceHistoryRequest{
		InstanceId:            request.InstanceId,
		ExecutionId:           request.ExecutionId,
		ForWorkItemProcessing: true,
	})
	if err != nil {
		return nil, err
	}

	var (
		history      []*protos.HistoryEvent
		historyBytes int64
	)
	for {
		chunk, recvErr := recvBeforeSilenceTimeout(stream.Recv, cancelHistory, w.options.silentDisconnectTimeout)
		if errors.Is(recvErr, io.EOF) {
			return history, nil
		}
		if recvErr != nil {
			return nil, recvErr
		}
		if chunk == nil {
			return nil, status.Error(codes.Internal, "received a nil history chunk")
		}
		if limit := w.options.maxStreamedHistoryEvents; limit > 0 &&
			len(chunk.Events) > limit-len(history) {
			return nil, newStreamedHistoryLimitError(
				"streamed history exceeds %d events",
				limit,
			)
		}
		chunkBytes := int64(proto.Size(chunk))
		if limit := w.options.maxStreamedHistoryBytes; limit > 0 &&
			chunkBytes > limit-historyBytes {
			return nil, newStreamedHistoryLimitError(
				"streamed history exceeds %d bytes",
				limit,
			)
		}
		history = append(history, chunk.Events...)
		historyBytes += chunkBytes
	}
}

func (w *TaskHubGrpcWorker) processActivity(
	ctx context.Context,
	client protos.TaskHubSidecarServiceClient,
	completionToken string,
	request *protos.ActivityRequest,
) {
	if request.OrchestrationInstance == nil {
		w.logger.Error("received activity work item without an orchestration instance; abandoning it")
		w.abandonActivity(ctx, client, completionToken)
		return
	}
	if err := largepayload.TransformActivityRequest(ctx, w.options.largePayloads, request); err != nil {
		w.logger.Errorf(
			"%s/%s#%d: failed to hydrate activity payload: %v",
			request.OrchestrationInstance.InstanceId,
			request.Name,
			request.TaskId,
			err,
		)
		w.abandonActivity(ctx, client, completionToken)
		return
	}
	if !matchesWorkItemFilters(w.options.workItemFilters, false, request.Name, request.Version.GetValue()) {
		w.logger.Warnf(
			"%s/%s#%d: activity work item does not match configured filters; abandoning it",
			request.OrchestrationInstance.InstanceId,
			request.Name,
			request.TaskId,
		)
		_ = w.wait(ctx, w.options.reconnectBaseDelay)
		w.abandonActivity(ctx, client, completionToken)
		return
	}

	event := helpers.NewTaskScheduledEvent(
		request.TaskId,
		request.Name,
		request.Version,
		request.Input,
		request.ParentTraceContext,
	)
	event.GetTaskScheduled().Tags = contextprop.Clone(request.Tags)
	result, err := w.executor.ExecuteActivity(ctx, api.InstanceID(request.OrchestrationInstance.InstanceId), event)
	var delayed workItemAbandonDelayError
	if errors.As(err, &delayed) {
		w.logger.Warnf(
			"%s/%s#%d: activity work item rejected; abandoning it: %v",
			request.OrchestrationInstance.InstanceId,
			request.Name,
			request.TaskId,
			err,
		)
		_ = w.wait(ctx, delayed.WorkItemAbandonDelay())
		w.abandonActivity(ctx, client, completionToken)
		return
	}

	if err != nil && ctx.Err() != nil {
		w.logger.Warnf("%s/%s#%d: activity execution canceled; abandoning work item", request.OrchestrationInstance.InstanceId, request.Name, request.TaskId)
		w.abandonActivity(ctx, client, completionToken)
		return
	}

	response := &protos.ActivityResponse{
		InstanceId:      request.OrchestrationInstance.InstanceId,
		TaskId:          request.TaskId,
		CompletionToken: completionToken,
	}
	if err != nil {
		response.FailureDetails = failure.FromError(err)
	} else if completed := result.GetTaskCompleted(); completed != nil {
		response.Result = completed.Result
	} else if failed := result.GetTaskFailed(); failed != nil {
		response.FailureDetails = failed.FailureDetails
	} else {
		response.FailureDetails = &protos.TaskFailureDetails{
			ErrorType:    "UnknownTaskResult",
			ErrorMessage: "activity executor returned an unknown task result",
		}
	}
	if err := largepayload.TransformActivityResponse(ctx, w.options.largePayloads, response); err != nil {
		w.logger.Errorf(
			"%s/%s#%d: failed to externalize activity response payload: %v",
			request.OrchestrationInstance.InstanceId,
			request.Name,
			request.TaskId,
			err,
		)
		w.abandonActivity(ctx, client, completionToken)
		return
	}

	err = w.executeRPCWithRetry(ctx, "complete activity task", func(callCtx context.Context) error {
		_, callErr := client.CompleteActivityTask(callCtx, response)
		return callErr
	})
	if err != nil {
		if isWorkItemGone(err) {
			w.logger.Warnf(
				"%s/%s#%d: activity work item was no longer available before completion",
				request.OrchestrationInstance.InstanceId,
				request.Name,
				request.TaskId,
			)
			return
		}
		w.logger.Errorf("%s/%s#%d: failed to complete activity work item: %v", request.OrchestrationInstance.InstanceId, request.Name, request.TaskId, err)
		w.abandonActivity(ctx, client, completionToken)
	}
}

func (w *TaskHubGrpcWorker) processEntityV2(
	ctx context.Context,
	client protos.TaskHubSidecarServiceClient,
	completionToken string,
	request *protos.EntityRequest,
) {
	batch, operationInfos, err := entityBatchFromRequestV2(request)
	if err != nil {
		w.logger.Errorf("invalid V2 entity work item: %v", err)
		w.abandonEntity(ctx, client, completionToken)
		return
	}
	w.processEntityBatch(ctx, client, completionToken, batch, operationInfos)
}

func (w *TaskHubGrpcWorker) processEntityBatch(
	ctx context.Context,
	client protos.TaskHubSidecarServiceClient,
	completionToken string,
	request *protos.EntityBatchRequest,
	operationInfos []*protos.OperationInfo,
) {
	entityID, parseErr := api.EntityIDFromString(request.GetInstanceId())
	if parseErr != nil {
		w.logger.Errorf("%s: invalid entity instance ID: %v", request.GetInstanceId(), parseErr)
		w.abandonEntity(ctx, client, completionToken)
		return
	}
	if !matchesEntityWorkItemFilters(w.options.workItemFilters, entityID.Name) {
		w.logger.Warnf("%s: entity work item does not match configured filters; abandoning it", request.GetInstanceId())
		w.abandonEntity(ctx, client, completionToken)
		return
	}
	if err := largepayload.TransformEntityBatchRequest(ctx, w.options.largePayloads, request); err != nil {
		w.logger.Errorf("%s: failed to hydrate entity batch payloads: %v", request.GetInstanceId(), err)
		if entityProcessingCanceled(ctx, err) {
			w.abandonEntity(ctx, client, completionToken)
			return
		}
		w.completeEntityBatchFailure(ctx, client, request.GetInstanceId(), completionToken, err)
		return
	}
	executor, ok := w.executor.(task.EntityExecutor)
	if !ok {
		w.logger.Error("task executor does not support entity work items")
		w.abandonEntity(ctx, client, completionToken)
		return
	}
	result, err := executeEntitySafely(ctx, executor, request)
	if err != nil {
		if entityProcessingCanceled(ctx, err) {
			w.logger.Warnf("%s: entity execution canceled; abandoning work item", request.GetInstanceId())
			w.abandonEntity(ctx, client, completionToken)
			return
		}
		w.logger.Errorf("%s: entity execution failed: %v", request.GetInstanceId(), err)
		result = newEntityBatchFailure(completionToken, err)
	} else if result == nil {
		missingResultErr := errors.New("entity executor returned no result")
		w.logger.Errorf("%s: %v", request.GetInstanceId(), missingResultErr)
		result = newEntityBatchFailure(completionToken, missingResultErr)
	}
	result.CompletionToken = completionToken
	if err := validateEntityBatchResult(result, len(request.Operations), operationInfos); err != nil {
		w.logger.Errorf("%s: invalid entity executor result: %v", request.GetInstanceId(), err)
		result = newEntityBatchFailure(completionToken, err)
	}
	if err := largepayload.TransformEntityBatchResult(ctx, w.options.largePayloads, result); err != nil {
		w.logger.Errorf("%s: failed to externalize entity batch payloads: %v", request.GetInstanceId(), err)
		if entityProcessingCanceled(ctx, err) {
			w.abandonEntity(ctx, client, completionToken)
			return
		}
		w.completeEntityBatchFailure(ctx, client, request.GetInstanceId(), completionToken, err)
		return
	}

	w.completeEntityBatchResult(ctx, client, request.GetInstanceId(), result)
}

func executeEntitySafely(
	ctx context.Context,
	executor task.EntityExecutor,
	request *protos.EntityBatchRequest,
) (result *protos.EntityBatchResult, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = &entityExecutorPanicError{
				message: fmt.Sprintf("entity executor panic: %v", recovered),
				stack:   string(debug.Stack()),
			}
		}
	}()
	return executor.ExecuteEntity(ctx, request)
}

type entityExecutorPanicError struct {
	message string
	stack   string
}

func (e *entityExecutorPanicError) Error() string {
	return e.message
}

func (e *entityExecutorPanicError) DurableTaskStackTrace() string {
	return e.stack
}

func newEntityBatchFailure(completionToken string, err error) *protos.EntityBatchResult {
	return &protos.EntityBatchResult{
		CompletionToken: completionToken,
		FailureDetails:  failure.FromError(err),
	}
}

func (w *TaskHubGrpcWorker) completeEntityBatchFailure(
	ctx context.Context,
	client protos.TaskHubSidecarServiceClient,
	instanceID string,
	completionToken string,
	err error,
) {
	w.completeEntityBatchResult(ctx, client, instanceID, newEntityBatchFailure(completionToken, err))
}

func validateEntityBatchResult(
	result *protos.EntityBatchResult,
	operationCount int,
	operationInfos []*protos.OperationInfo,
) error {
	if result.FailureDetails != nil || result.RequiresState {
		if result.FailureDetails != nil && result.RequiresState {
			return fmt.Errorf("entity result cannot contain both failure details and a state request")
		}
		if len(result.Results) != 0 || len(result.Actions) != 0 || result.EntityState != nil ||
			len(result.OperationInfos) != 0 {
			return fmt.Errorf("entity batch failures and state requests must not contain partial effects")
		}
		result.OperationInfos = nil
		return nil
	}
	if len(result.Results) != operationCount {
		return fmt.Errorf(
			"entity result count %d does not match operation count %d",
			len(result.Results),
			operationCount,
		)
	}
	if len(result.OperationInfos) != 0 {
		return fmt.Errorf("entity executor result must not set operation routing metadata")
	}
	if operationInfos == nil {
		return nil
	}
	if len(operationInfos) != len(result.Results) {
		return fmt.Errorf(
			"V2 entity result count %d does not match operation count %d",
			len(result.Results),
			len(operationInfos),
		)
	}
	result.OperationInfos = append([]*protos.OperationInfo(nil), operationInfos...)
	return nil
}

func entityProcessingCanceled(ctx context.Context, err error) bool {
	return ctx.Err() != nil ||
		errors.Is(err, context.Canceled) ||
		errors.Is(err, context.DeadlineExceeded)
}

func (w *TaskHubGrpcWorker) completeEntityBatchResult(
	ctx context.Context,
	client protos.TaskHubSidecarServiceClient,
	instanceID string,
	result *protos.EntityBatchResult,
) {
	err := w.executeRPCWithRetry(ctx, "complete entity task", func(callCtx context.Context) error {
		_, callErr := client.CompleteEntityTask(callCtx, result)
		return callErr
	})
	if err != nil {
		if isWorkItemGone(err) {
			w.logger.Warnf("%s: entity work item was no longer available before completion", instanceID)
			return
		}
		w.logger.Errorf("%s: failed to complete entity work item: %v", instanceID, err)
		w.abandonEntity(ctx, client, result.CompletionToken)
	}
}

func orchestrationWorkItemIdentity(pastEvents, newEvents []*protos.HistoryEvent) (string, string, bool) {
	for _, events := range [][]*protos.HistoryEvent{newEvents, pastEvents} {
		for i := len(events) - 1; i >= 0; i-- {
			event := events[i]
			if event == nil {
				continue
			}
			if rewound := event.GetExecutionRewound(); rewound != nil && rewound.GetName().GetValue() != "" {
				return rewound.GetName().GetValue(), rewound.GetVersion().GetValue(), true
			}
			if started := event.GetExecutionStarted(); started != nil {
				return started.GetName(), started.GetVersion().GetValue(), true
			}
		}
	}
	return "", "", false
}

func matchesWorkItemFilters(filters *WorkItemFilters, orchestration bool, name, version string) bool {
	if filters == nil {
		return true
	}
	candidates := filters.Activities
	rejectAll := filters.RejectAllActivities
	if orchestration {
		candidates = filters.Orchestrations
		rejectAll = filters.RejectAllOrchestrations
	}
	if rejectAll {
		return false
	}
	if len(candidates) == 0 {
		return true
	}
	for _, filter := range candidates {
		if !strings.EqualFold(filter.Name, name) {
			continue
		}
		if len(filter.Versions) == 0 || slices.ContainsFunc(filter.Versions, func(candidate string) bool {
			return strings.EqualFold(candidate, version)
		}) {
			return true
		}
	}
	return false
}

func matchesEntityWorkItemFilters(filters *WorkItemFilters, name string) bool {
	if filters == nil {
		return true
	}
	if filters.RejectAllEntities {
		return false
	}
	if len(filters.Entities) == 0 {
		return true
	}
	return slices.Contains(filters.Entities, name)
}

func (w *TaskHubGrpcWorker) abandonOrchestration(
	ctx context.Context,
	client protos.TaskHubSidecarServiceClient,
	completionToken string,
) {
	if completionToken == "" {
		w.logger.Warn("cannot abandon orchestration work item without a completion token")
		return
	}
	if err := w.executeRPCWithRetry(ctx, "abandon orchestration task", func(callCtx context.Context) error {
		_, callErr := client.AbandonTaskOrchestratorWorkItem(callCtx, &protos.AbandonOrchestrationTaskRequest{
			CompletionToken: completionToken,
		})
		return callErr
	}); err != nil {
		w.logger.Errorf("failed to abandon orchestration work item: %v", err)
	}
}

func (w *TaskHubGrpcWorker) abandonActivity(
	ctx context.Context,
	client protos.TaskHubSidecarServiceClient,
	completionToken string,
) {
	if completionToken == "" {
		w.logger.Warn("cannot abandon activity work item without a completion token")
		return
	}
	if err := w.executeRPCWithRetry(ctx, "abandon activity task", func(callCtx context.Context) error {
		_, callErr := client.AbandonTaskActivityWorkItem(callCtx, &protos.AbandonActivityTaskRequest{
			CompletionToken: completionToken,
		})
		return callErr
	}); err != nil {
		w.logger.Errorf("failed to abandon activity work item: %v", err)
	}
}

func (w *TaskHubGrpcWorker) abandonEntity(
	ctx context.Context,
	client protos.TaskHubSidecarServiceClient,
	completionToken string,
) {
	if completionToken == "" {
		w.logger.Warn("cannot abandon unsupported entity work item without a completion token")
		return
	}
	abandonCtx := context.WithoutCancel(ctx)
	if err := w.executeRPCWithRetry(abandonCtx, "abandon entity task", func(callCtx context.Context) error {
		_, callErr := client.AbandonTaskEntityWorkItem(callCtx, &protos.AbandonEntityTaskRequest{
			CompletionToken: completionToken,
		})
		return callErr
	}); err != nil {
		w.logger.Errorf("failed to abandon unsupported entity work item: %v", err)
	}
}

func (w *TaskHubGrpcWorker) executeRPCWithRetry(
	ctx context.Context,
	operation string,
	action func(context.Context) error,
) error {
	var lastErr error
	for attempt := 1; attempt <= w.options.transientRetryMaxAttempts; attempt++ {
		callCtx, cancel := context.WithTimeout(ctx, w.options.rpcTimeout)
		lastErr = action(callCtx)
		cancel()
		if lastErr == nil {
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if !isTransientWorkerRPCError(lastErr) {
			return fmt.Errorf("%s failed with a non-retryable error: %w", operation, lastErr)
		}
		if attempt == w.options.transientRetryMaxAttempts {
			break
		}
		if err := w.wait(ctx, w.retryDelay(attempt)); err != nil {
			return err
		}
	}
	return fmt.Errorf("%s failed after %d attempts: %w", operation, w.options.transientRetryMaxAttempts, lastErr)
}

// retryDelay returns the deterministic delay before the given attempt. Delays
// double from the configured base and never leave [base, max].
func (w *TaskHubGrpcWorker) retryDelay(attempt int) time.Duration {
	delay := w.options.transientRetryBaseDelay
	for i := 1; i < attempt; i++ {
		next := doubleDurationBounded(delay, w.options.transientRetryMaxDelay)
		if next == delay {
			break
		}
		delay = next
	}
	return delay
}

func isTransientWorkerRPCError(err error) bool {
	if errors.Is(err, ErrStreamedHistoryLimitExceeded) {
		return false
	}
	grpcStatus, ok := status.FromError(err)
	if !ok {
		return false
	}
	if grpcStatus.Code() == codes.NotFound {
		return false
	}
	return isTransientWorkerGRPCStatus(grpcStatus)
}

func isWorkItemGone(err error) bool {
	return status.Code(err) == codes.NotFound
}
