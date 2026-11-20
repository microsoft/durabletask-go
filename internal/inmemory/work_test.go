package inmemory

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/microsoft/durabletask-go/internal/helpers"
	"github.com/microsoft/durabletask-go/internal/protos"
)

func TestContinueAsNewDropsPreviousExecutionResults(t *testing.T) {
	s := NewServer()
	defer s.Close()
	inst := &instance{id: "id", name: "orchestrator", executionID: "old"}
	raised := helpers.NewEventRaisedEvent("event", nil)
	inst.pending = []*protos.HistoryEvent{
		helpers.NewTaskCompletedEvent(0, nil),
		raised,
		helpers.NewTimerFiredEvent(1, nil, nil),
	}
	carryover := helpers.NewEventRaisedEvent("carryover", nil)

	s.continueAsNewLocked(inst, &protos.CompleteOrchestrationAction{CarryoverEvents: []*protos.HistoryEvent{carryover}})

	require.Len(t, inst.pending, 3)
	assert.NotNil(t, inst.pending[0].GetExecutionStarted())
	assert.NotEqual(t, "old", inst.executionID)
	assert.Same(t, carryover, inst.pending[1])
	assert.Same(t, raised, inst.pending[2])
}

func TestRewindReplacementAndStaleActivity(t *testing.T) {
	s := NewServer()
	defer s.Close()
	inst := s.newInstanceLocked("id", "orchestrator", nil, nil, nil, nil, nil, nil)
	started := inst.pending[0]
	oldExecution := inst.executionID
	inst.history = []*protos.HistoryEvent{started}
	s.completeLocked(inst, protos.OrchestrationStatus_ORCHESTRATION_STATUS_FAILED, nil, failureDetails("failure", "boom"))
	s.actLeases["old-activity"] = &activityLease{item: &activityItem{
		inst: inst, executionID: oldExecution, request: &protos.ActivityRequest{TaskId: 0},
	}}
	_, err := s.RewindInstance(context.Background(), &protos.RewindInstanceRequest{
		InstanceId: inst.id, Reason: wrapperspb.String("repaired"),
	})
	require.NoError(t, err)
	require.True(t, inst.completedAt.IsZero())
	require.Nil(t, inst.failure)
	require.Equal(t, oldExecution, inst.executionID, "identity changes only when the worker replaces history")
	item := s.nextWorkItemLocked(nil, 1)
	req := item.GetOrchestratorRequest()
	require.Len(t, req.GetNewEvents(), 2)
	require.NotNil(t, req.NewEvents[0].GetOrchestratorStarted())
	require.Equal(t, "repaired", req.NewEvents[1].GetExecutionRewound().GetReason().GetValue())

	response := &protos.OrchestratorResponse{
		CompletionToken: item.GetCompletionToken(),
		Actions: []*protos.OrchestratorAction{{OrchestratorActionType: &protos.OrchestratorAction_RewindOrchestration{
			RewindOrchestration: &protos.RewindOrchestrationAction{},
		}}},
	}
	_, err = s.CompleteOrchestratorTask(context.Background(), response)
	require.Equal(t, codes.InvalidArgument, status.Code(err))
	require.True(t, inst.leased, "invalid replacement must not consume the lease")
	replacementStart := proto.CloneOf(started)
	replacementStart.GetExecutionStarted().OrchestrationInstance.ExecutionId = wrapperspb.String("renewed")
	replacement := []*protos.HistoryEvent{replacementStart, req.NewEvents[0], req.NewEvents[1]}
	response.Actions[0].GetRewindOrchestration().NewHistory = replacement
	arrival := helpers.NewEventRaisedEvent("during-rewrite", nil)
	s.deliverLocked(inst, arrival)
	_, err = s.CompleteOrchestratorTask(context.Background(), response)
	require.NoError(t, err)
	require.Equal(t, "renewed", inst.executionID)
	require.Equal(t, replacement, inst.history, "persist replacement without extra markers")
	require.Len(t, inst.pending, 2)
	require.NotNil(t, inst.pending[0].GetOrchestratorStarted())
	require.Same(t, arrival, inst.pending[1], "retain events arriving during replacement")
	_, err = s.CompleteActivityTask(context.Background(), &protos.ActivityResponse{CompletionToken: "old-activity"})
	require.NoError(t, err)
	require.Len(t, inst.pending, 2, "old execution results must not reach the renewed execution")
}

func TestRewindRejectsNonFailedStates(t *testing.T) {
	for _, state := range []protos.OrchestrationStatus{
		protos.OrchestrationStatus_ORCHESTRATION_STATUS_PENDING,
		protos.OrchestrationStatus_ORCHESTRATION_STATUS_RUNNING,
		protos.OrchestrationStatus_ORCHESTRATION_STATUS_SUSPENDED,
		protos.OrchestrationStatus_ORCHESTRATION_STATUS_COMPLETED,
		protos.OrchestrationStatus_ORCHESTRATION_STATUS_TERMINATED,
		protos.OrchestrationStatus_ORCHESTRATION_STATUS_CANCELED,
	} {
		t.Run(state.String(), func(t *testing.T) {
			s := NewServer()
			defer s.Close()
			inst := &instance{id: "id", status: state, completedAt: time.Now()}
			s.instances[inst.id] = inst
			_, err := s.RewindInstance(context.Background(), &protos.RewindInstanceRequest{InstanceId: inst.id})
			require.Equal(t, codes.FailedPrecondition, status.Code(err))
			require.Equal(t, state, inst.status)
			require.Empty(t, inst.pending)
		})
	}
}

func TestRewindPreservesSuccessfulChildAndRetargetsRunningChild(t *testing.T) {
	s := NewServer()
	defer s.Close()
	parent := s.newInstanceLocked("parent", "parent", nil, nil, nil, nil, nil, nil)
	oldParent := helpers.NewParentInfo(1, parent.name, parent.id)
	oldParent.OrchestrationInstance.ExecutionId = wrapperspb.String(parent.executionID)
	running := s.newInstanceLocked("running", "child", nil, nil, nil, oldParent, nil, nil)
	runningItem := s.leaseOrchestrationLocked(running, 1)
	dispatched := runningItem.GetOrchestratorRequest().GetNewEvents()
	completed := &instance{id: "completed", status: protos.OrchestrationStatus_ORCHESTRATION_STATUS_COMPLETED}
	s.instances[completed.id] = completed
	started := proto.CloneOf(parent.pending[0])
	started.GetExecutionStarted().OrchestrationInstance.ExecutionId = wrapperspb.String("renewed-parent")
	history := []*protos.HistoryEvent{
		started,
		helpers.NewSubOrchestrationCreatedEvent(0, "child", nil, nil, completed.id, nil),
		{EventType: &protos.HistoryEvent_SubOrchestrationInstanceCompleted{
			SubOrchestrationInstanceCompleted: &protos.SubOrchestrationInstanceCompletedEvent{TaskScheduledId: 0},
		}},
		helpers.NewSubOrchestrationCreatedEvent(1, "child", nil, nil, running.id, nil),
	}
	s.applyRewindLocked(parent, &protos.RewindOrchestrationAction{NewHistory: history})
	require.Equal(t, protos.OrchestrationStatus_ORCHESTRATION_STATUS_COMPLETED, completed.status)
	require.Empty(t, completed.pending)
	require.Equal(t, "renewed-parent", running.parent.GetOrchestrationInstance().GetExecutionId().GetValue())
	lease := s.orchLeases[runningItem.GetCompletionToken()]
	require.NotEqual(t, "renewed-parent", dispatched[1].GetExecutionStarted().GetParentInstance().GetOrchestrationInstance().GetExecutionId().GetValue(),
		"do not mutate the dispatched request")
	require.Equal(t, "renewed-parent", lease.events[1].GetExecutionStarted().GetParentInstance().GetOrchestrationInstance().GetExecutionId().GetValue())
}
