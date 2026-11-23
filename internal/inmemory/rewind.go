package inmemory

import (
	"context"
	"fmt"
	"slices"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/microsoft/durabletask-go/internal/grpcerrors"
	"github.com/microsoft/durabletask-go/internal/helpers"
	"github.com/microsoft/durabletask-go/internal/protos"
)

// RewindInstance enqueues recovery of a failed instance through the worker.
func (s *Server) RewindInstance(_ context.Context, req *protos.RewindInstanceRequest) (*protos.RewindInstanceResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	inst, err := s.lookupLocked(req.GetInstanceId())
	if err != nil {
		return nil, err
	}
	if inst.status != protos.OrchestrationStatus_ORCHESTRATION_STATUS_FAILED {
		return nil, grpcerrors.New(codes.FailedPrecondition,
			fmt.Sprintf("orchestration instance %q is not in a failed state", inst.id), grpcerrors.ReasonInvalidState)
	}
	if err := s.validateRewindChildrenLocked(inst, inst.history, make(map[*instance]bool)); err != nil {
		return nil, err
	}
	s.prepareRewindLocked(inst, req.GetReason(), nil)
	return &protos.RewindInstanceResponse{}, nil
}

func (s *Server) prepareRewindLocked(inst *instance, reason, parentExecutionID *wrapperspb.StringValue) {
	inst.stopTimers()
	inst.status = protos.OrchestrationStatus_ORCHESTRATION_STATUS_RUNNING
	inst.output = nil
	inst.failure = nil
	inst.completedAt = time.Time{}
	inst.updatedAt = time.Now()
	inst.rewind = &protos.HistoryEvent{
		EventId:   -1,
		Timestamp: timestamppb.Now(),
		EventType: &protos.HistoryEvent_ExecutionRewound{ExecutionRewound: &protos.ExecutionRewoundEvent{
			Reason: reason, ParentExecutionId: parentExecutionID,
		}},
	}
	s.enqueueLocked(inst)
}

func validateRewindHistory(history []*protos.HistoryEvent, oldExecutionID string) error {
	starts := 0
	for _, event := range history {
		if event.GetExecutionCompleted() != nil {
			return status.Error(codes.InvalidArgument, "rewind history must not contain execution completion")
		}
		if started := event.GetExecutionStarted(); started != nil {
			starts++
			executionID := started.GetOrchestrationInstance().GetExecutionId().GetValue()
			if executionID == "" || executionID == oldExecutionID {
				return status.Error(codes.InvalidArgument, "rewind history requires a new execution ID")
			}
		}
		if timer := event.GetTimerCreated(); timer != nil {
			if timer.GetFireAt() == nil || timer.GetFireAt().CheckValid() != nil {
				return status.Error(codes.InvalidArgument, "rewind history contains an invalid timer deadline")
			}
		}
	}
	if starts != 1 {
		return status.Error(codes.InvalidArgument, "rewind history requires exactly one execution start")
	}
	return nil
}

func unfinishedSubOrchestrations(history []*protos.HistoryEvent) []*protos.HistoryEvent {
	completed := make(map[int32]bool)
	for _, event := range history {
		if child := event.GetSubOrchestrationInstanceCompleted(); child != nil {
			completed[child.GetTaskScheduledId()] = true
		}
	}
	var unfinished []*protos.HistoryEvent
	for _, event := range history {
		if event.GetSubOrchestrationInstanceCreated() != nil && !completed[event.GetEventId()] {
			unfinished = append(unfinished, event)
		}
	}
	return unfinished
}

func failedSubOrchestrations(history []*protos.HistoryEvent) map[int32]bool {
	failed := make(map[int32]bool)
	for _, event := range history {
		if child := event.GetSubOrchestrationInstanceFailed(); child != nil {
			failed[child.GetTaskScheduledId()] = true
		}
	}
	return failed
}

func (s *Server) pendingRewindChildResults(
	inst *instance,
	history []*protos.HistoryEvent,
	failed map[int32]bool,
) map[int32]*protos.HistoryEvent {
	created := make(map[int32]string)
	for _, event := range unfinishedSubOrchestrations(history) {
		created[event.GetEventId()] = event.GetSubOrchestrationInstanceCreated().GetInstanceId()
	}
	results := make(map[int32]*protos.HistoryEvent)
	for _, event := range inst.pending {
		var id int32
		failedResult := false
		switch {
		case event.GetSubOrchestrationInstanceCompleted() != nil:
			id = event.GetSubOrchestrationInstanceCompleted().GetTaskScheduledId()
		case event.GetSubOrchestrationInstanceFailed() != nil:
			id = event.GetSubOrchestrationInstanceFailed().GetTaskScheduledId()
			failedResult = true
			if failed[id] {
				continue
			}
		default:
			continue
		}
		source := inst.childResultSources[event]
		if source.instanceID == "" || source.instanceID != created[id] {
			continue
		}
		if child, ok := s.instances[source.instanceID]; ok && failedResult {
			if childMatchesParent(child, inst, inst.executionID, id) &&
				(child.executionID != source.executionID || !isTerminal(child.status) ||
					child.status == protos.OrchestrationStatus_ORCHESTRATION_STATUS_COMPLETED) {
				continue
			}
		}
		if previous := results[id]; previous == nil || previous.GetSubOrchestrationInstanceFailed() != nil {
			results[id] = event
		}
	}
	return results
}

func (s *Server) validateRewindChildrenLocked(inst *instance, history []*protos.HistoryEvent, seen map[*instance]bool) error {
	if seen[inst] {
		return status.Errorf(codes.FailedPrecondition, "cannot rewind cyclic sub-orchestration %q", inst.id)
	}
	seen[inst] = true
	defer delete(seen, inst)

	failed := failedSubOrchestrations(inst.history)
	results := s.pendingRewindChildResults(inst, history, failed)
	for _, event := range unfinishedSubOrchestrations(history) {
		if results[event.GetEventId()] != nil {
			continue
		}
		childID := event.GetSubOrchestrationInstanceCreated().GetInstanceId()
		child, ok := s.instances[childID]
		if !ok {
			continue
		}
		if !childMatchesParent(child, inst, inst.executionID, event.GetEventId()) {
			return status.Errorf(codes.AlreadyExists,
				"cannot rewind orchestration %q: sub-orchestration ID %q belongs to another parent or execution",
				inst.id, childID)
		}
		if child.status == protos.OrchestrationStatus_ORCHESTRATION_STATUS_FAILED && failed[event.GetEventId()] {
			if err := s.validateRewindChildrenLocked(child, child.history, seen); err != nil {
				return err
			}
		}
	}
	return nil
}

func childMatchesParent(child, parent *instance, parentExecutionID string, taskID int32) bool {
	ref := child.parent.GetOrchestrationInstance()
	return ref.GetInstanceId() == parent.id &&
		ref.GetExecutionId().GetValue() == parentExecutionID &&
		child.parent.GetTaskScheduledId() == taskID
}

func (s *Server) applyRewindIdentityLocked(inst *instance, history []*protos.HistoryEvent) {
	inst.history = slices.Clone(history)
	parentExecutionID := inst.rewind.GetExecutionRewound().GetParentExecutionId()
	for i, event := range inst.history {
		started := event.GetExecutionStarted()
		if started == nil {
			continue
		}
		if parentExecutionID.GetValue() != "" && started.GetParentInstance() != nil {
			event = proto.CloneOf(event)
			started = event.GetExecutionStarted()
			if started.ParentInstance.OrchestrationInstance == nil {
				started.ParentInstance.OrchestrationInstance = &protos.OrchestrationInstance{}
			}
			started.ParentInstance.OrchestrationInstance.ExecutionId = proto.CloneOf(parentExecutionID)
			inst.history[i] = event
		}
		inst.executionID = started.GetOrchestrationInstance().GetExecutionId().GetValue()
		inst.parent = started.GetParentInstance()
	}
}

func (s *Server) retargetRewindParentLocked(inst *instance) {
	parentExecutionID := inst.rewind.GetExecutionRewound().GetParentExecutionId()
	if parentExecutionID.GetValue() != "" && inst.parent != nil {
		parent := proto.CloneOf(inst.parent)
		if parent.OrchestrationInstance == nil {
			parent.OrchestrationInstance = &protos.OrchestrationInstance{}
		}
		parent.OrchestrationInstance.ExecutionId = proto.CloneOf(parentExecutionID)
		s.retargetChildLocked(inst, parent)
	}
}

func (s *Server) failRewindLocked(inst *instance, err error) {
	s.retargetRewindParentLocked(inst)
	for i := len(inst.history) - 1; i >= 0; i-- {
		if inst.history[i].GetExecutionCompleted() != nil {
			inst.history = inst.history[:i]
			break
		}
	}
	inst.rewind = nil
	errorType := "InvalidRewindState"
	if status.Code(err) == codes.AlreadyExists {
		errorType = "OrchestrationAlreadyExists"
	}
	s.failLocked(inst, failureDetails(errorType, "%s", status.Convert(err).Message()))
}

// Persist the worker's replacement, then reconcile retained work before replay.
func (s *Server) applyRewindLocked(inst *instance, action *protos.RewindOrchestrationAction) {
	oldExecutionID := inst.executionID
	failed := failedSubOrchestrations(inst.history)
	childResults := s.pendingRewindChildResults(inst, action.GetNewHistory(), failed)
	s.applyRewindIdentityLocked(inst, action.GetNewHistory())
	var reason *wrapperspb.StringValue
	for _, event := range inst.history {
		if rewound := event.GetExecutionRewound(); rewound != nil {
			reason = rewound.GetReason()
		}
	}
	inst.rewind = nil
	s.reconcileRewindTasksLocked(inst, oldExecutionID, childResults)

	for _, event := range unfinishedSubOrchestrations(inst.history) {
		created := event.GetSubOrchestrationInstanceCreated()
		parent := helpers.NewParentInfo(event.GetEventId(), inst.name, inst.id)
		parent.Version = inst.version
		parent.OrchestrationInstance.ExecutionId = wrapperspb.String(inst.executionID)
		child, ok := s.instances[created.GetInstanceId()]
		if result := childResults[event.GetEventId()]; result != nil {
			source := inst.childResultSources[result]
			// A buffered result can outlive its child and a reused ID.
			if ok && child.executionID == source.executionID &&
				childMatchesParent(child, inst, oldExecutionID, event.GetEventId()) {
				s.retargetChildLocked(child, parent)
			}
			continue
		}
		switch {
		case !ok:
			child = s.newInstanceLocked(created.GetInstanceId(), created.GetName(), created.GetVersion(),
				created.GetInput(), created.GetTags(), parent, created.GetParentTraceContext(), nil)
		case child.status == protos.OrchestrationStatus_ORCHESTRATION_STATUS_FAILED && failed[event.GetEventId()]:
			s.prepareRewindLocked(child, reason, wrapperspb.String(inst.executionID))
		case isTerminal(child.status):
			s.retargetChildLocked(child, parent)
			result := subOrchestrationResultEvent(child)
			inst.recordChildResult(result, child)
			index := len(inst.pending)
			for i, pending := range inst.pending {
				if result.GetTimestamp().AsTime().Before(pending.GetTimestamp().AsTime()) {
					index = i
					break
				}
			}
			inst.pending = slices.Insert(inst.pending, index, result)
		default:
			s.retargetChildLocked(child, parent)
		}
		if !slices.Contains(inst.children, child.id) {
			inst.children = append(inst.children, child.id)
		}
	}
	inst.pending = append([]*protos.HistoryEvent{helpers.NewOrchestratorStartedEvent()}, inst.pending...)
}

func (s *Server) retargetChildLocked(child *instance, parent *protos.ParentInstanceInfo) {
	child.parent = parent
	if child.rewind != nil {
		child.rewind = proto.CloneOf(child.rewind)
		child.rewind.GetExecutionRewound().ParentExecutionId = proto.CloneOf(parent.GetOrchestrationInstance().GetExecutionId())
	}
	updateParent := func(events []*protos.HistoryEvent) {
		for i, event := range events {
			if event.GetExecutionStarted() != nil {
				cloned := proto.CloneOf(event)
				cloned.GetExecutionStarted().ParentInstance = parent
				events[i] = cloned
			}
		}
	}
	updateParent(child.history)
	updateParent(child.pending)
	for _, lease := range s.orchLeases {
		if lease.inst == child {
			lease.events = slices.Clone(lease.events)
			updateParent(lease.events)
		}
	}
}

func (s *Server) reconcileRewindTasksLocked(
	inst *instance,
	oldExecutionID string,
	childResults map[int32]*protos.HistoryEvent,
) {
	activities := make(map[int32]*protos.TaskScheduledEvent)
	timers := make(map[int32]*timestamppb.Timestamp)
	for _, event := range inst.history {
		switch {
		case event.GetTaskScheduled() != nil:
			activities[event.GetEventId()] = event.GetTaskScheduled()
		case event.GetTaskCompleted() != nil:
			delete(activities, event.GetTaskCompleted().GetTaskScheduledId())
		case event.GetTaskFailed() != nil:
			delete(activities, event.GetTaskFailed().GetTaskScheduledId())
		case event.GetTimerCreated() != nil:
			timers[event.GetEventId()] = event.GetTimerCreated().GetFireAt()
		case event.GetTimerFired() != nil:
			delete(timers, event.GetTimerFired().GetTimerId())
		}
	}

	pending := inst.pending[:0]
	for _, event := range inst.pending {
		switch {
		case event.GetTaskCompleted() != nil:
			id := event.GetTaskCompleted().GetTaskScheduledId()
			if _, ok := activities[id]; ok {
				pending = append(pending, event)
				delete(activities, id)
			}
		case event.GetTaskFailed() != nil:
			id := event.GetTaskFailed().GetTaskScheduledId()
			if _, ok := activities[id]; ok {
				pending = append(pending, event)
				delete(activities, id)
			}
		case event.GetTimerFired() != nil:
			id := event.GetTimerFired().GetTimerId()
			if _, ok := timers[id]; ok {
				pending = append(pending, event)
				delete(timers, id)
			}
		case event.GetSubOrchestrationInstanceCompleted() != nil:
			id := event.GetSubOrchestrationInstanceCompleted().GetTaskScheduledId()
			if childResults[id] == event {
				pending = append(pending, event)
			} else {
				delete(inst.childResultSources, event)
			}
		case event.GetSubOrchestrationInstanceFailed() != nil:
			id := event.GetSubOrchestrationInstanceFailed().GetTaskScheduledId()
			if childResults[id] == event {
				pending = append(pending, event)
			} else {
				delete(inst.childResultSources, event)
			}
		default:
			pending = append(pending, event)
		}
	}
	inst.pending = pending

	for _, lease := range s.actLeases {
		item := lease.item
		if item.inst == inst && item.executionID == oldExecutionID {
			if _, ok := activities[item.request.GetTaskId()]; ok {
				item.retarget(inst.executionID)
				delete(activities, item.request.GetTaskId())
			}
		}
	}
	queue := s.actQueue
	kept := queue[:0]
	for _, item := range queue {
		if item.inst == inst && item.executionID == oldExecutionID {
			if _, ok := activities[item.request.GetTaskId()]; !ok {
				continue
			}
			item.retarget(inst.executionID)
			delete(activities, item.request.GetTaskId())
		}
		kept = append(kept, item)
	}
	clear(queue[len(kept):])
	s.actQueue = kept

	for _, event := range inst.history {
		if task := event.GetTaskScheduled(); task != nil {
			if _, ok := activities[event.GetEventId()]; ok {
				s.queueActivityLocked(inst, event.GetEventId(), task)
				delete(activities, event.GetEventId())
			}
		}
		if event.GetTimerCreated() != nil {
			if fireAt, ok := timers[event.GetEventId()]; ok {
				id := event.GetEventId()
				s.startTimerLocked(inst, fireAt.AsTime(), func() {
					s.deliverLocked(inst, helpers.NewTimerFiredEvent(id, fireAt, nil))
				})
				delete(timers, id)
			}
		}
	}
}

func (item *activityItem) retarget(executionID string) {
	item.executionID = executionID
	item.request = proto.CloneOf(item.request)
	if item.request.OrchestrationInstance == nil {
		item.request.OrchestrationInstance = &protos.OrchestrationInstance{}
	}
	item.request.OrchestrationInstance.ExecutionId = wrapperspb.String(executionID)
}
