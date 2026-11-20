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
	inst.pending = []*protos.HistoryEvent{{
		EventId:   -1,
		Timestamp: timestamppb.Now(),
		EventType: &protos.HistoryEvent_ExecutionRewound{ExecutionRewound: &protos.ExecutionRewoundEvent{
			Reason: reason, ParentExecutionId: parentExecutionID,
		}},
	}}
	s.enqueueLocked(inst)
}

func validateRewindHistory(history []*protos.HistoryEvent) error {
	starts := 0
	for _, event := range history {
		if event.GetExecutionCompleted() != nil {
			return status.Error(codes.InvalidArgument, "rewind history must not contain execution completion")
		}
		if started := event.GetExecutionStarted(); started != nil {
			starts++
			if started.GetOrchestrationInstance().GetExecutionId().GetValue() == "" {
				return status.Error(codes.InvalidArgument, "rewind history requires an execution ID")
			}
		}
	}
	if starts != 1 {
		return status.Error(codes.InvalidArgument, "rewind history requires exactly one execution start")
	}
	return nil
}

// The worker owns history selection. Persist its replacement without adding
// markers, then wake normal replay and recover unfinished children.
func (s *Server) applyRewindLocked(inst *instance, action *protos.RewindOrchestrationAction) {
	inst.history = slices.Clone(action.GetNewHistory())
	var reason *wrapperspb.StringValue
	completed := make(map[int32]bool)
	for _, event := range inst.history {
		if started := event.GetExecutionStarted(); started != nil {
			inst.executionID = started.GetOrchestrationInstance().GetExecutionId().GetValue()
			inst.parent = started.GetParentInstance()
		}
		if rewound := event.GetExecutionRewound(); rewound != nil {
			reason = rewound.GetReason()
		}
		if child := event.GetSubOrchestrationInstanceCompleted(); child != nil {
			completed[child.GetTaskScheduledId()] = true
		}
	}
	for _, event := range inst.history {
		created := event.GetSubOrchestrationInstanceCreated()
		if created == nil || completed[event.GetEventId()] {
			continue
		}
		parent := helpers.NewParentInfo(event.GetEventId(), inst.name, inst.id)
		parent.Version = inst.version
		parent.OrchestrationInstance.ExecutionId = wrapperspb.String(inst.executionID)
		child, ok := s.instances[created.GetInstanceId()]
		switch {
		case !ok:
			child = s.newInstanceLocked(created.GetInstanceId(), created.GetName(), created.GetVersion(),
				created.GetInput(), created.GetTags(), parent, created.GetParentTraceContext(), nil)
			if !slices.Contains(inst.children, child.id) {
				inst.children = append(inst.children, child.id)
			}
		case child.status == protos.OrchestrationStatus_ORCHESTRATION_STATUS_FAILED:
			s.prepareRewindLocked(child, reason, wrapperspb.String(inst.executionID))
		default:
			// An unfinished child may still be running. Its completion must
			// target the renewed parent, including on subsequent replay.
			child.parent = parent
			updateParent := func(events []*protos.HistoryEvent) {
				for i, childEvent := range events {
					if childEvent.GetExecutionStarted() != nil {
						cloned := proto.CloneOf(childEvent)
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
	}
	inst.pending = append([]*protos.HistoryEvent{helpers.NewOrchestratorStartedEvent()}, inst.pending...)
}
