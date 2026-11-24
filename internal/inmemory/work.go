package inmemory

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/microsoft/durabletask-go/internal/helpers"
	"github.com/microsoft/durabletask-go/internal/protos"
)

// GetWorkItems streams orchestration and activity work items to a worker.
// Work items that are still in flight when the stream ends are returned to the queue.
func (s *Server) GetWorkItems(req *protos.GetWorkItemsRequest, stream grpc.ServerStreamingServer[protos.WorkItem]) error {
	ctx := stream.Context()
	s.mu.Lock()
	s.nextStreamID++
	streamID := s.nextStreamID
	s.mu.Unlock()
	defer s.releaseStream(streamID)

	filters := req.GetWorkItemFilters()
	limits := workItemLimits{
		orchestrations: req.GetMaxConcurrentOrchestrationWorkItems(),
		activities:     req.GetMaxConcurrentActivityWorkItems(),
	}
	ping := time.NewTicker(s.healthPingInterval)
	defer ping.Stop()
	for {
		s.mu.Lock()
		if s.closed {
			s.mu.Unlock()
			return status.Error(codes.Unavailable, "in-memory service is closed")
		}
		item := s.nextWorkItemLocked(filters, streamID, limits)
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

type workItemLimits struct {
	orchestrations int32
	activities     int32
}

func (s *Server) nextWorkItemLocked(
	filters *protos.WorkItemFilters,
	streamID uint64,
	limits workItemLimits,
) *protos.WorkItem {
	if streamHasCapacity(limits.orchestrations, countOrchestrationLeases(s.orchLeases, streamID)) {
		for i, inst := range s.orchQueue {
			if !matchesOrchestrationFilter(filters, inst) {
				continue
			}
			s.orchQueue = slices.Delete(s.orchQueue, i, i+1)
			inst.queued = false
			if s.instances[inst.id] != inst || inst.leased || len(inst.pending) == 0 {
				// A stale queue entry; look again from the start.
				return s.nextWorkItemLocked(filters, streamID, limits)
			}
			if item := s.leaseOrchestrationLocked(inst, streamID); item != nil {
				return item
			}
			return s.nextWorkItemLocked(filters, streamID, limits)
		}
	}
	if streamHasCapacity(limits.activities, countActivityLeases(s.actLeases, streamID)) {
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
	}
	return nil
}

func streamHasCapacity(limit int32, inFlight int) bool {
	return limit <= 0 || inFlight < int(limit)
}

func countOrchestrationLeases(leases map[string]*orchestrationLease, streamID uint64) int {
	count := 0
	for _, lease := range leases {
		if lease.streamID == streamID {
			count++
		}
	}
	return count
}

func countActivityLeases(leases map[string]*activityLease, streamID uint64) int {
	count := 0
	for _, lease := range leases {
		if lease.streamID == streamID {
			count++
		}
	}
	return count
}

// leaseOrchestrationLocked leases an instance's pending events as a work item
// that fits in one gRPC message. History that doesn't fit is streamed, and new
// events that don't fit stay pending for later turns. If the first new event
// can't fit on its own, the instance fails and nil is returned.
func (s *Server) leaseOrchestrationLocked(inst *instance, streamID uint64) *protos.WorkItem {
	events := inst.pending
	prepended := 0
	if events[0].GetOrchestratorStarted() == nil {
		events = append([]*protos.HistoryEvent{helpers.NewOrchestratorStartedEvent()}, events...)
		prepended = 1
	}
	token := newID()
	request := &protos.OrchestratorRequest{
		InstanceId:  inst.id,
		ExecutionId: wrapperspb.String(inst.executionID),
		PastEvents:  slices.Clone(inst.history),
		NewEvents:   events,
	}
	item := &protos.WorkItem{
		Request:         &protos.WorkItem_OrchestratorRequest{OrchestratorRequest: request},
		CompletionToken: token,
	}
	if len(request.PastEvents) > 0 && proto.Size(item) > maxOrchestrationWorkItemSize {
		request.PastEvents = nil
		request.RequiresHistoryStreaming = true
	}
	if proto.Size(item) > maxOrchestrationWorkItemSize {
		n := fittingNewEventCount(item, events)
		// The batch must hold the OrchestratorStarted marker and at least one event.
		if n < 2 {
			s.completeLocked(inst, protos.OrchestrationStatus_ORCHESTRATION_STATUS_FAILED, nil, &protos.TaskFailureDetails{
				ErrorType: "OrchestrationWorkItemTooLarge",
				ErrorMessage: fmt.Sprintf(
					"a %d-byte history event can't be delivered within the %d-byte gRPC message limit of the in-memory test backend",
					proto.Size(events[1]), maxOrchestrationWorkItemSize),
				IsNonRetriable: true,
			})
			return nil
		}
		events = slices.Clip(events[:n])
		request.NewEvents = events
	}
	inst.pending = slices.Clone(inst.pending[len(events)-prepended:])
	inst.leased = true
	s.orchLeases[token] = &orchestrationLease{inst: inst, events: events, streamID: streamID}
	return item
}

// fittingNewEventCount returns how many leading events fit in the work item's
// new events without exceeding the message limit.
func fittingNewEventCount(item *protos.WorkItem, events []*protos.HistoryEvent) int {
	request := item.GetOrchestratorRequest()
	request.NewEvents = nil
	size := proto.Size(item)
	n := 0
	for _, event := range events {
		// newEvents is field 4 of OrchestratorRequest.
		size += protowire.SizeTag(4) + protowire.SizeBytes(proto.Size(event))
		if size > maxOrchestrationWorkItemSize {
			break
		}
		n++
	}
	// Nested length prefixes also grow, so confirm the estimate.
	for ; n > 0; n-- {
		request.NewEvents = events[:n]
		if proto.Size(item) <= maxOrchestrationWorkItemSize {
			break
		}
	}
	return n
}

func matchesOrchestrationFilter(filters *protos.WorkItemFilters, inst *instance) bool {
	if filters == nil || len(filters.GetOrchestrations()) == 0 {
		return true
	}
	if slices.ContainsFunc(filters.GetOrchestrations(), func(filter *protos.OrchestrationFilter) bool {
		return strings.EqualFold(filter.GetName(), helpers.RejectAllWorkItemFilterName)
	}) {
		return false
	}
	for _, f := range filters.GetOrchestrations() {
		if !strings.EqualFold(f.GetName(), inst.name) {
			continue
		}
		if len(f.GetVersions()) == 0 || slices.ContainsFunc(f.GetVersions(), func(version string) bool {
			return strings.EqualFold(version, inst.version.GetValue())
		}) {
			return true
		}
	}
	return false
}

func matchesActivityFilter(filters *protos.WorkItemFilters, req *protos.ActivityRequest) bool {
	if filters == nil || len(filters.GetActivities()) == 0 {
		return true
	}
	if slices.ContainsFunc(filters.GetActivities(), func(filter *protos.ActivityFilter) bool {
		return strings.EqualFold(filter.GetName(), helpers.RejectAllWorkItemFilterName)
	}) {
		return false
	}
	for _, f := range filters.GetActivities() {
		if !strings.EqualFold(f.GetName(), req.GetName()) {
			continue
		}
		if len(f.GetVersions()) == 0 || slices.ContainsFunc(f.GetVersions(), func(version string) bool {
			return strings.EqualFold(version, req.GetVersion().GetValue())
		}) {
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
	s.notifyLocked()
}

func (s *Server) abandonOrchestrationLocked(token string, lease *orchestrationLease) {
	delete(s.orchLeases, token)
	inst := lease.inst
	inst.leased = false
	if s.instances[inst.id] != inst || isTerminal(inst.status) {
		s.notifyLocked()
		return
	}
	inst.pending = append(lease.events, inst.pending...)
	s.enqueueLocked(inst)
	s.notifyLocked()
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
		s.notifyLocked()
		return &protos.CompleteTaskResponse{}, nil
	}
	taskID := item.request.GetTaskId()
	if failure := oversizedCompletionFailure(errorTypeActivityResponseTooLarge, "activity response", resp); failure != nil {
		// Fail the task rather than store a result over the receive limit.
		s.deliverLocked(inst, helpers.NewTaskFailedEvent(taskID, failure))
	} else if resp.GetFailureDetails() != nil {
		s.deliverLocked(inst, helpers.NewTaskFailedEvent(taskID, resp.GetFailureDetails()))
	} else {
		s.deliverLocked(inst, helpers.NewTaskCompletedEvent(taskID, resp.GetResult()))
	}
	s.notifyLocked()
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
	delete(s.orchLeases, token)
	inst.leased = false
	if s.instances[inst.id] != inst || isTerminal(inst.status) {
		s.notifyLocked()
		return &protos.CompleteTaskResponse{}, nil
	}

	processed := lease.events
	if n := resp.GetNumEventsProcessed(); n != nil && int(n.GetValue()) < len(processed) {
		// NumEventsProcessed counts work events, not control markers in the
		// response. Keep control markers between consumed events in the prefix.
		split := processedEventPrefixLength(processed, int(n.GetValue()))
		inst.pending = append(slices.Clone(processed[split:]), inst.pending...)
		processed = processed[:split]
	}
	inst.history = append(inst.history, processed...)
	inst.started = true
	if inst.status == protos.OrchestrationStatus_ORCHESTRATION_STATUS_PENDING {
		inst.status = protos.OrchestrationStatus_ORCHESTRATION_STATUS_RUNNING
	}
	inst.customStatus = resp.GetCustomStatus()
	inst.updatedAt = time.Now()

	s.applyActionsLocked(inst, resp.GetActions())
	s.enqueueLocked(inst)
	s.notifyLocked()
	return &protos.CompleteTaskResponse{}, nil
}

func processedEventPrefixLength(events []*protos.HistoryEvent, processedCount int) int {
	processed := 0
	for i, event := range events {
		if !countsTowardTurnBudget(event) {
			continue
		}
		if processed == processedCount {
			return i
		}
		processed++
	}
	return len(events)
}

func countsTowardTurnBudget(event *protos.HistoryEvent) bool {
	return event.GetOrchestratorStarted() == nil &&
		event.GetOrchestratorCompleted() == nil &&
		event.GetGenericEvent() == nil &&
		event.GetExecutionRewound() == nil &&
		event.GetExecutionTerminated() == nil &&
		event.GetExecutionSuspended() == nil &&
		event.GetExecutionResumed() == nil
}

func (s *Server) applyActionsLocked(inst *instance, actions []*protos.OrchestratorAction) {
	for _, action := range actions {
		switch action.GetOrchestratorActionType().(type) {
		case *protos.OrchestratorAction_ScheduleTask,
			*protos.OrchestratorAction_CreateTimer,
			*protos.OrchestratorAction_CreateSubOrchestration,
			*protos.OrchestratorAction_SendEvent,
			*protos.OrchestratorAction_TerminateOrchestration,
			*protos.OrchestratorAction_CompleteOrchestration:
		case *protos.OrchestratorAction_SendEntityMessage:
			s.failUnsupportedLocked(inst, "durable entities are not supported by the in-memory test backend")
			return
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
	child := s.newInstanceLocked(childID, action.GetName(), action.GetVersion(), action.GetInput(), action.GetTags(),
		parentInfo, action.GetParentTraceContext(), nil)
	if !slices.Contains(parent.children, childID) {
		parent.children = append(parent.children, childID)
	}
	if terminated := parent.recursiveTermination(); terminated != nil {
		// The parent was terminated recursively while this turn was in flight,
		// after its existing children were terminated.
		s.terminateLocked(child, terminated.GetInput(), true)
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
