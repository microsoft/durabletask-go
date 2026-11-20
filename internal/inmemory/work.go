package inmemory

import (
	"context"
	"slices"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/microsoft/durabletask-go/internal/helpers"
	"github.com/microsoft/durabletask-go/internal/protos"
)

// GetWorkItems streams orchestration, activity, and entity work items to a worker.
// Work items that are still in flight when the stream ends are returned to the queue.
func (s *Server) GetWorkItems(req *protos.GetWorkItemsRequest, stream grpc.ServerStreamingServer[protos.WorkItem]) error {
	ctx := stream.Context()
	s.mu.Lock()
	s.nextStreamID++
	streamID := s.nextStreamID
	s.mu.Unlock()
	defer s.releaseStream(streamID)

	filters := req.GetWorkItemFilters()
	ping := time.NewTicker(s.healthPingInterval)
	defer ping.Stop()
	for {
		s.mu.Lock()
		if s.closed {
			s.mu.Unlock()
			return status.Error(codes.Unavailable, "in-memory service is closed")
		}
		item := s.nextWorkItemLocked(filters, streamID)
		changed := s.changed
		s.mu.Unlock()

		if item != nil {
			if err := stream.Send(item); err != nil {
				return err
			}
			continue
		}
		select {
		case <-ctx.Done():
			return nil
		case <-ping.C:
			if err := stream.Send(&protos.WorkItem{Request: &protos.WorkItem_HealthPing{HealthPing: &protos.HealthPing{}}}); err != nil {
				return err
			}
		case <-changed:
		}
	}
}

func (s *Server) nextWorkItemLocked(filters *protos.WorkItemFilters, streamID uint64) *protos.WorkItem {
	for i, inst := range s.orchQueue {
		if !matchesOrchestrationFilter(filters, inst) {
			continue
		}
		s.orchQueue = slices.Delete(s.orchQueue, i, i+1)
		inst.queued = false
		if s.instances[inst.id] != inst || inst.leased || len(inst.pending) == 0 {
			// A stale queue entry; look again from the start.
			return s.nextWorkItemLocked(filters, streamID)
		}
		return s.leaseOrchestrationLocked(inst, streamID)
	}
	for i, item := range s.actQueue {
		if !matchesActivityFilter(filters, item.request) {
			continue
		}
		s.actQueue = slices.Delete(s.actQueue, i, i+1)
		token := newID()
		s.actLeases[token] = &activityLease{item: item, streamID: streamID}
		return &protos.WorkItem{
			Request:         &protos.WorkItem_ActivityRequest{ActivityRequest: item.request},
			CompletionToken: token,
		}
	}
	return s.nextEntityWorkItemLocked(filters, streamID)
}

func (s *Server) leaseOrchestrationLocked(inst *instance, streamID uint64) *protos.WorkItem {
	events := inst.pending
	inst.pending = nil
	if events[0].GetOrchestratorStarted() == nil {
		events = append([]*protos.HistoryEvent{helpers.NewOrchestratorStartedEvent()}, events...)
	}
	inst.leased = true
	token := newID()
	s.orchLeases[token] = &orchestrationLease{inst: inst, events: events, streamID: streamID}
	return &protos.WorkItem{
		Request: &protos.WorkItem_OrchestratorRequest{OrchestratorRequest: &protos.OrchestratorRequest{
			InstanceId:  inst.id,
			ExecutionId: wrapperspb.String(inst.executionID),
			PastEvents:  slices.Clone(inst.history),
			NewEvents:   events,
			// Entity messages are delivered in order, so no reorder window is needed.
			EntityParameters: &protos.OrchestratorEntityParameters{EntityMessageReorderWindow: durationpb.New(0)},
		}},
		CompletionToken: token,
	}
}

func matchesOrchestrationFilter(filters *protos.WorkItemFilters, inst *instance) bool {
	if filters == nil {
		return true
	}
	for _, f := range filters.GetOrchestrations() {
		if f.GetName() == inst.name && (len(f.GetVersions()) == 0 || slices.Contains(f.GetVersions(), inst.version.GetValue())) {
			return true
		}
	}
	return false
}

func matchesActivityFilter(filters *protos.WorkItemFilters, req *protos.ActivityRequest) bool {
	if filters == nil {
		return true
	}
	for _, f := range filters.GetActivities() {
		if f.GetName() == req.GetName() && (len(f.GetVersions()) == 0 || slices.Contains(f.GetVersions(), req.GetVersion().GetValue())) {
			return true
		}
	}
	return false
}

// releaseStream returns the work items leased by a closed stream to the queue.
func (s *Server) releaseStream(streamID uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for token, lease := range s.orchLeases {
		if lease.streamID == streamID {
			s.abandonOrchestrationLocked(token, lease)
		}
	}
	for token, lease := range s.actLeases {
		if lease.streamID == streamID {
			delete(s.actLeases, token)
			s.actQueue = append(s.actQueue, lease.item)
		}
	}
	for token, lease := range s.entityLeases {
		if lease.streamID == streamID {
			s.abandonEntityLocked(token, lease)
		}
	}
	s.notifyLocked()
}

func (s *Server) abandonOrchestrationLocked(token string, lease *orchestrationLease) {
	delete(s.orchLeases, token)
	inst := lease.inst
	inst.leased = false
	if s.instances[inst.id] != inst || isTerminal(inst.status) {
		return
	}
	inst.pending = append(lease.events, inst.pending...)
	s.enqueueLocked(inst)
}

// AbandonTaskOrchestratorWorkItem returns an orchestration work item to the queue.
func (s *Server) AbandonTaskOrchestratorWorkItem(
	_ context.Context,
	req *protos.AbandonOrchestrationTaskRequest,
) (*protos.AbandonOrchestrationTaskResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	lease, ok := s.orchLeases[req.GetCompletionToken()]
	if !ok {
		return nil, status.Error(codes.NotFound, "orchestration work item not found")
	}
	s.abandonOrchestrationLocked(req.GetCompletionToken(), lease)
	return &protos.AbandonOrchestrationTaskResponse{}, nil
}

// AbandonTaskActivityWorkItem returns an activity work item to the queue.
func (s *Server) AbandonTaskActivityWorkItem(
	_ context.Context,
	req *protos.AbandonActivityTaskRequest,
) (*protos.AbandonActivityTaskResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	lease, ok := s.actLeases[req.GetCompletionToken()]
	if !ok {
		return nil, status.Error(codes.NotFound, "activity work item not found")
	}
	delete(s.actLeases, req.GetCompletionToken())
	s.actQueue = append(s.actQueue, lease.item)
	s.notifyLocked()
	return &protos.AbandonActivityTaskResponse{}, nil
}

// CompleteActivityTask records an activity result in its orchestration's inbox.
func (s *Server) CompleteActivityTask(_ context.Context, resp *protos.ActivityResponse) (*protos.CompleteTaskResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	lease, ok := s.actLeases[resp.GetCompletionToken()]
	if !ok {
		return nil, status.Error(codes.NotFound, "activity work item not found")
	}
	delete(s.actLeases, resp.GetCompletionToken())
	item := lease.item
	inst := item.inst
	if s.instances[inst.id] != inst || inst.executionID != item.executionID {
		return &protos.CompleteTaskResponse{}, nil
	}
	taskID := item.request.GetTaskId()
	if resp.GetFailureDetails() != nil {
		s.deliverLocked(inst, helpers.NewTaskFailedEvent(taskID, resp.GetFailureDetails()))
	} else {
		s.deliverLocked(inst, helpers.NewTaskCompletedEvent(taskID, resp.GetResult()))
	}
	return &protos.CompleteTaskResponse{}, nil
}

// CompleteOrchestratorTask commits an orchestration turn and applies its actions.
func (s *Server) CompleteOrchestratorTask(_ context.Context, resp *protos.OrchestratorResponse) (*protos.CompleteTaskResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	token := resp.GetCompletionToken()
	lease, ok := s.orchLeases[token]
	if !ok {
		return nil, status.Error(codes.NotFound, "orchestration work item not found")
	}
	inst := lease.inst
	if resp.GetRequiresHistory() {
		s.abandonOrchestrationLocked(token, lease)
		return &protos.CompleteTaskResponse{}, nil
	}
	var rewind *protos.RewindOrchestrationAction
	for _, action := range resp.GetActions() {
		if replacement := action.GetRewindOrchestration(); replacement != nil {
			if len(resp.GetActions()) != 1 {
				return nil, status.Error(codes.InvalidArgument, "rewind must be the only orchestration action")
			}
			if err := validateRewindHistory(replacement.GetNewHistory()); err != nil {
				return nil, err
			}
			rewind = replacement
		}
	}
	delete(s.orchLeases, token)
	inst.leased = false
	if s.instances[inst.id] != inst || isTerminal(inst.status) {
		return &protos.CompleteTaskResponse{}, nil
	}

	processed := lease.events
	if n := resp.GetNumEventsProcessed(); n != nil && int(n.GetValue()) < len(processed) {
		// Events that the orchestrator did not process go back to the inbox.
		inst.pending = append(slices.Clone(processed[n.GetValue():]), inst.pending...)
		processed = processed[:n.GetValue()]
	}
	inst.history = append(inst.history, processed...)
	if inst.status == protos.OrchestrationStatus_ORCHESTRATION_STATUS_PENDING {
		inst.status = protos.OrchestrationStatus_ORCHESTRATION_STATUS_RUNNING
	}
	inst.customStatus = resp.GetCustomStatus()
	inst.updatedAt = time.Now()

	if rewind != nil {
		s.applyRewindLocked(inst, rewind)
		s.enqueueLocked(inst)
		s.notifyLocked()
		return &protos.CompleteTaskResponse{}, nil
	}
	s.applyActionsLocked(inst, resp.GetActions())
	s.enqueueLocked(inst)
	s.notifyLocked()
	return &protos.CompleteTaskResponse{}, nil
}

func (s *Server) applyActionsLocked(inst *instance, actions []*protos.OrchestratorAction) {
	for _, action := range actions {
		switch action.GetOrchestratorActionType().(type) {
		case *protos.OrchestratorAction_ScheduleTask,
			*protos.OrchestratorAction_CreateTimer,
			*protos.OrchestratorAction_CreateSubOrchestration,
			*protos.OrchestratorAction_SendEvent,
			*protos.OrchestratorAction_TerminateOrchestration,
			*protos.OrchestratorAction_SendEntityMessage,
			*protos.OrchestratorAction_CompleteOrchestration:
		default:
			s.failUnsupportedLocked(inst, "orchestrator action %T is not supported by the in-memory test backend",
				action.GetOrchestratorActionType())
			return
		}
	}

	var complete *protos.CompleteOrchestrationAction
	for _, action := range actions {
		id := action.GetId()
		switch a := action.GetOrchestratorActionType().(type) {
		case *protos.OrchestratorAction_ScheduleTask:
			task := a.ScheduleTask
			event := helpers.NewTaskScheduledEvent(id, task.GetName(), task.GetVersion(), task.GetInput(), task.GetParentTraceContext())
			event.GetTaskScheduled().Tags = task.GetTags()
			inst.history = append(inst.history, event)
			s.actQueue = append(s.actQueue, &activityItem{
				inst:        inst,
				executionID: inst.executionID,
				request: &protos.ActivityRequest{
					Name:    task.GetName(),
					Version: task.GetVersion(),
					Input:   task.GetInput(),
					OrchestrationInstance: &protos.OrchestrationInstance{
						InstanceId:  inst.id,
						ExecutionId: wrapperspb.String(inst.executionID),
					},
					TaskId:             id,
					ParentTraceContext: task.GetParentTraceContext(),
					Tags:               task.GetTags(),
				},
			})
		case *protos.OrchestratorAction_CreateTimer:
			fireAt := a.CreateTimer.GetFireAt()
			inst.history = append(inst.history, helpers.NewTimerCreatedEvent(id, fireAt))
			s.startTimerLocked(inst, fireAt.AsTime(), func() {
				s.deliverLocked(inst, helpers.NewTimerFiredEvent(id, fireAt, nil))
			})
		case *protos.OrchestratorAction_CreateSubOrchestration:
			s.createSubOrchestrationLocked(inst, id, a.CreateSubOrchestration)
		case *protos.OrchestratorAction_SendEvent:
			send := a.SendEvent
			targetID := send.GetInstance().GetInstanceId()
			inst.history = append(inst.history, helpers.NewSendEventEvent(id, targetID, send.GetName(), send.GetData()))
			if target, ok := s.instances[targetID]; ok {
				s.deliverLocked(target, helpers.NewEventRaisedEvent(send.GetName(), send.GetData()))
			}
		case *protos.OrchestratorAction_TerminateOrchestration:
			terminate := a.TerminateOrchestration
			if target, ok := s.instances[terminate.GetInstanceId()]; ok {
				s.terminateLocked(target, terminate.GetReason(), terminate.GetRecurse())
			}
		case *protos.OrchestratorAction_SendEntityMessage:
			s.sendEntityMessageLocked(inst, id, a.SendEntityMessage)
		case *protos.OrchestratorAction_CompleteOrchestration:
			complete = a.CompleteOrchestration
		}
	}
	inst.history = append(inst.history, &protos.HistoryEvent{
		EventId:   -1,
		Timestamp: timestamppb.Now(),
		EventType: &protos.HistoryEvent_OrchestratorCompleted{OrchestratorCompleted: &protos.OrchestratorCompletedEvent{}},
	})

	if complete == nil {
		return
	}
	if complete.GetOrchestrationStatus() == protos.OrchestrationStatus_ORCHESTRATION_STATUS_CONTINUED_AS_NEW {
		s.continueAsNewLocked(inst, complete)
		return
	}
	s.completeLocked(inst, complete.GetOrchestrationStatus(), complete.GetResult(), complete.GetFailureDetails())
}

func (s *Server) createSubOrchestrationLocked(parent *instance, taskID int32, action *protos.CreateSubOrchestrationAction) {
	childID := action.GetInstanceId()
	created := helpers.NewSubOrchestrationCreatedEvent(
		taskID, action.GetName(), action.GetVersion(), action.GetInput(), childID, action.GetParentTraceContext())
	created.GetSubOrchestrationInstanceCreated().Tags = action.GetTags()
	parent.history = append(parent.history, created)

	if existing, ok := s.instances[childID]; ok && !isTerminal(existing.status) {
		s.deliverLocked(parent, &protos.HistoryEvent{
			EventId:   -1,
			Timestamp: timestamppb.Now(),
			EventType: &protos.HistoryEvent_SubOrchestrationInstanceFailed{
				SubOrchestrationInstanceFailed: &protos.SubOrchestrationInstanceFailedEvent{
					TaskScheduledId: taskID,
					FailureDetails: failureDetails("OrchestrationAlreadyExists",
						"orchestration instance %q already exists", childID),
				},
			},
		})
		return
	}
	parentInfo := helpers.NewParentInfo(taskID, parent.name, parent.id)
	parentInfo.Version = parent.version
	parentInfo.OrchestrationInstance.ExecutionId = wrapperspb.String(parent.executionID)
	s.newInstanceLocked(childID, action.GetName(), action.GetVersion(), action.GetInput(), action.GetTags(),
		parentInfo, action.GetParentTraceContext(), nil)
	if !slices.Contains(parent.children, childID) {
		parent.children = append(parent.children, childID)
	}
}

func (s *Server) continueAsNewLocked(inst *instance, complete *protos.CompleteOrchestrationAction) {
	inst.stopTimers()
	inst.history = nil
	inst.input = complete.GetResult()
	if complete.GetNewVersion() != nil {
		inst.version = complete.GetNewVersion()
	}
	if complete.GetTags() != nil {
		inst.tags = complete.GetTags()
	}
	inst.output = nil
	inst.failure = nil
	pending := []*protos.HistoryEvent{inst.newExecutionStartedEvent()}
	pending = append(pending, complete.GetCarryoverEvents()...)
	// Results for the previous execution's tasks would be matched to the new
	// execution's task IDs, so only events addressed to the instance carry over.
	for _, event := range inst.pending {
		switch event.GetEventType().(type) {
		case *protos.HistoryEvent_EventRaised,
			*protos.HistoryEvent_ExecutionSuspended,
			*protos.HistoryEvent_ExecutionResumed,
			*protos.HistoryEvent_ExecutionTerminated:
			pending = append(pending, event)
		}
	}
	inst.pending = pending
}

func (s *Server) failUnsupportedLocked(inst *instance, format string, args ...any) {
	failure := failureDetails("NotSupported", format, args...)
	failure.IsNonRetriable = true
	inst.history = append(inst.history, &protos.HistoryEvent{
		EventId:   -1,
		Timestamp: timestamppb.Now(),
		EventType: &protos.HistoryEvent_OrchestratorCompleted{OrchestratorCompleted: &protos.OrchestratorCompletedEvent{}},
	})
	s.completeLocked(inst, protos.OrchestrationStatus_ORCHESTRATION_STATUS_FAILED, nil, failure)
}

// completeLocked moves an instance to a terminal state and notifies its parent.
func (s *Server) completeLocked(
	inst *instance,
	runtimeStatus protos.OrchestrationStatus,
	output *wrapperspb.StringValue,
	failure *protos.TaskFailureDetails,
) {
	now := time.Now()
	inst.stopTimers()
	inst.status = runtimeStatus
	inst.output = output
	inst.failure = failure
	inst.completedAt = now
	inst.updatedAt = now
	inst.pending = nil
	inst.history = append(inst.history, &protos.HistoryEvent{
		EventId:   -1,
		Timestamp: timestamppb.New(now),
		EventType: &protos.HistoryEvent_ExecutionCompleted{ExecutionCompleted: &protos.ExecutionCompletedEvent{
			OrchestrationStatus: runtimeStatus,
			Result:              output,
			FailureDetails:      failure,
		}},
	})
	s.notifyLocked()

	parentRef := inst.parent.GetOrchestrationInstance()
	parent, ok := s.instances[parentRef.GetInstanceId()]
	if !ok || parent.executionID != parentRef.GetExecutionId().GetValue() {
		return
	}
	taskID := inst.parent.GetTaskScheduledId()
	event := &protos.HistoryEvent{EventId: -1, Timestamp: timestamppb.New(now)}
	if runtimeStatus == protos.OrchestrationStatus_ORCHESTRATION_STATUS_COMPLETED {
		event.EventType = &protos.HistoryEvent_SubOrchestrationInstanceCompleted{
			SubOrchestrationInstanceCompleted: &protos.SubOrchestrationInstanceCompletedEvent{
				TaskScheduledId: taskID,
				Result:          output,
			},
		}
	} else {
		if failure == nil {
			failure = failureDetails("OrchestrationTerminated",
				"sub-orchestration %q ended with status %s", inst.id, helpers.ToRuntimeStatusString(runtimeStatus))
		}
		event.EventType = &protos.HistoryEvent_SubOrchestrationInstanceFailed{
			SubOrchestrationInstanceFailed: &protos.SubOrchestrationInstanceFailedEvent{
				TaskScheduledId: taskID,
				FailureDetails:  failure,
			},
		}
	}
	s.deliverLocked(parent, event)
}
