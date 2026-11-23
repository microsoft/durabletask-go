package inmemory

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
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
	item := s.nextWorkItemLocked(nil, 1, workItemLimits{})
	req := item.GetOrchestratorRequest()
	require.Len(t, req.GetNewEvents(), 2)
	require.NotNil(t, req.NewEvents[0].GetOrchestratorStarted())
	require.Equal(t, "repaired", req.NewEvents[1].GetExecutionRewound().GetReason().GetValue())

	_, err = s.CompleteOrchestratorTask(context.Background(), &protos.OrchestratorResponse{
		CompletionToken: item.GetCompletionToken(),
	})
	require.Equal(t, codes.InvalidArgument, status.Code(err))
	require.True(t, inst.leased, "an incomplete rewind must not consume the lease")
	response := &protos.OrchestratorResponse{
		CompletionToken: item.GetCompletionToken(),
		Actions: []*protos.OrchestratorAction{{OrchestratorActionType: &protos.OrchestratorAction_RewindOrchestration{
			RewindOrchestration: &protos.RewindOrchestrationAction{},
		}}},
	}
	_, err = s.CompleteOrchestratorTask(context.Background(), response)
	require.Equal(t, codes.InvalidArgument, status.Code(err))
	require.True(t, inst.leased, "invalid replacement must not consume the lease")
	response.Actions[0].GetRewindOrchestration().NewHistory = []*protos.HistoryEvent{started}
	_, err = s.CompleteOrchestratorTask(context.Background(), response)
	require.Equal(t, codes.InvalidArgument, status.Code(err), "replacement must renew the execution ID")
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

func finishTestRewind(
	t *testing.T,
	s *Server,
	item *protos.WorkItem,
	started *protos.HistoryEvent,
	executionID string,
	retained ...*protos.HistoryEvent,
) {
	t.Helper()
	started = proto.CloneOf(started)
	started.GetExecutionStarted().OrchestrationInstance.ExecutionId = wrapperspb.String(executionID)
	history := append([]*protos.HistoryEvent{started}, retained...)
	history = append(history, item.GetOrchestratorRequest().GetNewEvents()...)
	_, err := s.CompleteOrchestratorTask(context.Background(), &protos.OrchestratorResponse{
		CompletionToken: item.GetCompletionToken(),
		Actions: []*protos.OrchestratorAction{{OrchestratorActionType: &protos.OrchestratorAction_RewindOrchestration{
			RewindOrchestration: &protos.RewindOrchestrationAction{NewHistory: history},
		}}},
	})
	require.NoError(t, err)
}

func TestRewindDiscardsLifecycleRequestsFromFailedTurn(t *testing.T) {
	ctx := context.Background()
	for name, request := range map[string]func(s *Server, id string) error{
		"suspend": func(s *Server, id string) error {
			_, err := s.SuspendInstance(ctx, &protos.SuspendRequest{InstanceId: id})
			return err
		},
		"terminate": func(s *Server, id string) error {
			_, err := s.TerminateInstance(ctx, &protos.TerminateRequest{InstanceId: id})
			return err
		},
	} {
		t.Run(name, func(t *testing.T) {
			s := NewServer()
			defer s.Close()
			inst := s.newInstanceLocked("id", "orchestrator", nil, nil, nil, nil, nil, nil)
			started := inst.pending[0]
			item := s.nextWorkItemLocked(nil, 1, workItemLimits{})
			require.NoError(t, request(s, inst.id))
			require.NotEmpty(t, inst.pending)
			_, err := s.CompleteOrchestratorTask(ctx, &protos.OrchestratorResponse{
				CompletionToken: item.GetCompletionToken(),
				Actions: []*protos.OrchestratorAction{{OrchestratorActionType: &protos.OrchestratorAction_CompleteOrchestration{
					CompleteOrchestration: &protos.CompleteOrchestrationAction{
						OrchestrationStatus: protos.OrchestrationStatus_ORCHESTRATION_STATUS_FAILED,
						FailureDetails:      failureDetails("failure", "boom"),
					},
				}}},
			})
			require.NoError(t, err)
			require.Equal(t, protos.OrchestrationStatus_ORCHESTRATION_STATUS_FAILED, inst.status)

			_, err = s.RewindInstance(ctx, &protos.RewindInstanceRequest{InstanceId: inst.id})
			require.NoError(t, err)
			item = s.nextWorkItemLocked(nil, 1, workItemLimits{})
			finishTestRewind(t, s, item, started, "renewed")
			require.Equal(t, protos.OrchestrationStatus_ORCHESTRATION_STATUS_RUNNING, inst.status)
			require.Len(t, inst.pending, 1, "a request superseded by failure must not reach the recovered worker")
			require.NotNil(t, inst.pending[0].GetOrchestratorStarted())
		})
	}
}

func TestRewindIsolatesInboxAcrossRedelivery(t *testing.T) {
	s := NewServer()
	defer s.Close()
	inst := s.newInstanceLocked("id", "orchestrator", nil, nil, nil, nil, nil, nil)
	started, oldExecutionID := inst.pending[0], inst.executionID
	inst.history = []*protos.HistoryEvent{started}
	s.completeLocked(inst, protos.OrchestrationStatus_ORCHESTRATION_STATUS_FAILED, nil, failureDetails("failure", "boom"))
	for _, token := range []string{"before", "during"} {
		s.actLeases[token] = &activityLease{item: &activityItem{
			inst: inst, executionID: oldExecutionID, request: &protos.ActivityRequest{TaskId: 99},
		}}
	}

	_, err := s.RewindInstance(context.Background(), &protos.RewindInstanceRequest{InstanceId: inst.id})
	require.NoError(t, err)
	_, err = s.RaiseEvent(context.Background(), &protos.RaiseEventRequest{InstanceId: inst.id, Name: "before"})
	require.NoError(t, err)
	_, err = s.CompleteActivityTask(context.Background(), &protos.ActivityResponse{CompletionToken: "before"})
	require.NoError(t, err)

	limits := workItemLimits{orchestrations: 1}
	item := s.nextWorkItemLocked(nil, 1, limits)
	require.Len(t, item.GetOrchestratorRequest().GetNewEvents(), 2)
	require.NotNil(t, item.GetOrchestratorRequest().NewEvents[0].GetOrchestratorStarted())
	require.NotNil(t, item.GetOrchestratorRequest().NewEvents[1].GetExecutionRewound())
	s.newInstanceLocked("other", "orchestrator", nil, nil, nil, nil, nil, nil)
	require.Nil(t, s.nextWorkItemLocked(nil, 1, limits), "a rewind lease consumes stream capacity")
	other := s.nextWorkItemLocked(nil, 2, limits)
	require.Equal(t, "other", other.GetOrchestratorRequest().GetInstanceId())
	_, err = s.AbandonTaskOrchestratorWorkItem(context.Background(), &protos.AbandonOrchestrationTaskRequest{
		CompletionToken: item.GetCompletionToken(),
	})
	require.NoError(t, err)
	item = s.nextWorkItemLocked(nil, 1, limits)
	require.Len(t, item.GetOrchestratorRequest().GetNewEvents(), 2)
	require.NotNil(t, item.GetOrchestratorRequest().NewEvents[1].GetExecutionRewound())

	_, err = s.RaiseEvent(context.Background(), &protos.RaiseEventRequest{InstanceId: inst.id, Name: "during"})
	require.NoError(t, err)
	_, err = s.CompleteActivityTask(context.Background(), &protos.ActivityResponse{CompletionToken: "during"})
	require.NoError(t, err)
	finishTestRewind(t, s, item, started, "renewed")

	require.Len(t, inst.pending, 3)
	require.NotNil(t, inst.pending[0].GetOrchestratorStarted())
	require.Equal(t, "before", inst.pending[1].GetEventRaised().GetName())
	require.Equal(t, "during", inst.pending[2].GetEventRaised().GetName())
}

func TestRewindWorkItemStreamsLargeHistory(t *testing.T) {
	s := NewServer()
	defer s.Close()
	inst := s.newInstanceLocked("id", "orchestrator", nil, nil, nil, nil, nil, nil)
	inst.history = []*protos.HistoryEvent{inst.pending[0]}
	data := wrapperspb.String(strings.Repeat("x", 1<<20))
	for range 5 {
		inst.history = append(inst.history, &protos.HistoryEvent{EventType: &protos.HistoryEvent_GenericEvent{
			GenericEvent: &protos.GenericEvent{Data: data},
		}})
	}
	s.completeLocked(inst, protos.OrchestrationStatus_ORCHESTRATION_STATUS_FAILED, nil, failureDetails("failure", "boom"))

	_, err := s.RewindInstance(context.Background(), &protos.RewindInstanceRequest{InstanceId: inst.id})
	require.NoError(t, err)
	item := s.nextWorkItemLocked(nil, 1, workItemLimits{})
	request := item.GetOrchestratorRequest()
	require.True(t, request.GetRequiresHistoryStreaming())
	require.Empty(t, request.GetPastEvents())
	require.Len(t, request.GetNewEvents(), 2)
	require.NotNil(t, request.NewEvents[0].GetOrchestratorStarted())
	require.NotNil(t, request.NewEvents[1].GetExecutionRewound())
	require.Equal(t, inst.executionID, request.GetExecutionId().GetValue())
}

func TestRewindRejectsOversizedWorkItem(t *testing.T) {
	ctx := context.Background()
	s := NewServer()
	defer s.Close()
	inst := s.newInstanceLocked("id", "orchestrator", nil, nil, nil, nil, nil, nil)
	inst.history = []*protos.HistoryEvent{inst.pending[0]}
	s.completeLocked(inst, protos.OrchestrationStatus_ORCHESTRATION_STATUS_FAILED, nil, failureDetails("failure", "boom"))
	history := append([]*protos.HistoryEvent(nil), inst.history...)
	reason := wrapperspb.String(strings.Repeat("x", maxOrchestrationWorkItemSize))

	_, err := s.RewindInstance(ctx, &protos.RewindInstanceRequest{InstanceId: inst.id, Reason: reason})
	require.Equal(t, codes.InvalidArgument, status.Code(err))
	require.Equal(t, protos.OrchestrationStatus_ORCHESTRATION_STATUS_FAILED, inst.status)
	require.Nil(t, inst.rewind)

	// A child's rewind carries its parent's reason, so leasing checks the size too.
	s.prepareRewindLocked(inst, newRewindEvent(reason, nil))
	require.Nil(t, s.nextWorkItemLocked(nil, 1, workItemLimits{}))
	require.Empty(t, s.orchLeases)
	require.False(t, inst.queued)
	require.Equal(t, protos.OrchestrationStatus_ORCHESTRATION_STATUS_FAILED, inst.status)
	require.Equal(t, "OrchestrationWorkItemTooLarge", inst.failure.GetErrorType())
	require.Nil(t, inst.rewind)
	require.Len(t, inst.history, len(history))
	require.Equal(t, history[:len(history)-1], inst.history[:len(history)-1], "only the failure is replaced")

	_, err = s.RewindInstance(ctx, &protos.RewindInstanceRequest{InstanceId: inst.id, Reason: wrapperspb.String("short")})
	require.NoError(t, err)
	item := s.nextWorkItemLocked(nil, 1, workItemLimits{})
	require.Equal(t, "short", item.GetOrchestratorRequest().GetNewEvents()[1].GetExecutionRewound().GetReason().GetValue())
}

func TestRewindReconcilesRetainedActivities(t *testing.T) {
	for _, mode := range []string{"before", "during", "in-flight", "queued", "missing"} {
		t.Run(mode, func(t *testing.T) {
			s := NewServer()
			defer s.Close()
			inst := s.newInstanceLocked("id", "orchestrator", nil, nil, nil, nil, nil, nil)
			started, oldExecutionID := inst.pending[0], inst.executionID
			scheduled := helpers.NewTaskScheduledEvent(3, "sibling", nil, wrapperspb.String(`"input"`), nil)
			inst.history = []*protos.HistoryEvent{started, scheduled}
			s.completeLocked(inst, protos.OrchestrationStatus_ORCHESTRATION_STATUS_FAILED, nil, failureDetails("failure", "boom"))

			request := &protos.ActivityRequest{
				Name: "sibling", TaskId: 3, Input: wrapperspb.String(`"input"`),
				OrchestrationInstance: &protos.OrchestrationInstance{
					InstanceId: inst.id, ExecutionId: wrapperspb.String(oldExecutionID),
				},
			}
			activity := &activityItem{inst: inst, executionID: oldExecutionID, request: request}
			switch mode {
			case "queued":
				s.actQueue = append(s.actQueue, activity)
			case "missing":
			default:
				s.actLeases["activity"] = &activityLease{item: activity}
			}
			complete := func() {
				t.Helper()
				_, err := s.CompleteActivityTask(context.Background(), &protos.ActivityResponse{
					CompletionToken: "activity", Result: wrapperspb.String(`"kept"`),
				})
				require.NoError(t, err)
			}
			if mode == "before" {
				complete()
			}
			_, err := s.RewindInstance(context.Background(), &protos.RewindInstanceRequest{InstanceId: inst.id})
			require.NoError(t, err)
			item := s.nextWorkItemLocked(nil, 1, workItemLimits{})
			require.Len(t, item.GetOrchestratorRequest().GetNewEvents(), 2)
			if mode == "during" {
				complete()
			}
			finishTestRewind(t, s, item, started, "renewed", scheduled)

			if mode == "in-flight" {
				require.Equal(t, "renewed", activity.executionID)
				require.Equal(t, oldExecutionID, request.GetOrchestrationInstance().GetExecutionId().GetValue())
				require.Equal(t, "renewed", activity.request.GetOrchestrationInstance().GetExecutionId().GetValue())
				complete()
			}
			switch mode {
			case "queued", "missing":
				require.Len(t, s.actQueue, 1)
				require.Equal(t, "renewed", s.actQueue[0].executionID)
				require.Equal(t, "sibling", s.actQueue[0].request.GetName())
				require.Equal(t, `"input"`, s.actQueue[0].request.GetInput().GetValue())
				require.Len(t, inst.pending, 1)
			default:
				require.Empty(t, s.actQueue)
				require.Len(t, inst.pending, 2)
				require.Equal(t, `"kept"`, inst.pending[1].GetTaskCompleted().GetResult().GetValue())
			}
		})
	}
}

func TestRewindRestoresOutstandingTimer(t *testing.T) {
	s := NewServer()
	defer s.Close()
	inst := s.newInstanceLocked("id", "orchestrator", nil, nil, nil, nil, nil, nil)
	started := inst.pending[0]
	fireAt := timestamppb.New(time.Now().Add(-time.Second))
	timer := helpers.NewTimerCreatedEvent(4, fireAt)
	inst.history = []*protos.HistoryEvent{started, timer, helpers.NewTaskFailedEvent(5, nil)}
	s.completeLocked(inst, protos.OrchestrationStatus_ORCHESTRATION_STATUS_FAILED, nil, failureDetails("failure", "boom"))

	_, err := s.RewindInstance(context.Background(), &protos.RewindInstanceRequest{InstanceId: inst.id})
	require.NoError(t, err)
	item := s.nextWorkItemLocked(nil, 1, workItemLimits{})
	finishTestRewind(t, s, item, started, "renewed", timer)
	require.Eventually(t, func() bool {
		s.mu.Lock()
		defer s.mu.Unlock()
		for _, event := range inst.pending {
			if fired := event.GetTimerFired(); fired != nil {
				return fired.GetTimerId() == 4 && proto.Equal(fired.GetFireAt(), fireAt)
			}
		}
		return false
	}, time.Second, time.Millisecond)
}

func TestRewindRejectsUnrelatedChildIdentities(t *testing.T) {
	for _, mismatch := range []string{"parent", "execution", "task"} {
		t.Run(mismatch, func(t *testing.T) {
			s := NewServer()
			defer s.Close()
			parent := s.newInstanceLocked("parent", "parent", nil, nil, nil, nil, nil, nil)
			started := parent.pending[0]
			created := helpers.NewSubOrchestrationCreatedEvent(7, "child", nil, nil, "child", nil)
			parent.history = []*protos.HistoryEvent{started, created, {
				EventType: &protos.HistoryEvent_SubOrchestrationInstanceFailed{
					SubOrchestrationInstanceFailed: &protos.SubOrchestrationInstanceFailedEvent{TaskScheduledId: 7},
				},
			}}
			s.completeLocked(parent, protos.OrchestrationStatus_ORCHESTRATION_STATUS_FAILED, nil, failureDetails("failure", "boom"))
			info := helpers.NewParentInfo(7, parent.name, parent.id)
			info.OrchestrationInstance.ExecutionId = wrapperspb.String(parent.executionID)
			switch mismatch {
			case "parent":
				info.OrchestrationInstance.InstanceId = "other"
			case "execution":
				info.OrchestrationInstance.ExecutionId = wrapperspb.String("other")
			case "task":
				info.TaskScheduledId = 8
			}
			child := &instance{id: "child", parent: info, status: protos.OrchestrationStatus_ORCHESTRATION_STATUS_RUNNING}
			s.instances[child.id] = child

			_, err := s.RewindInstance(context.Background(), &protos.RewindInstanceRequest{InstanceId: parent.id})
			require.Equal(t, codes.AlreadyExists, status.Code(err))
			require.Contains(t, status.Convert(err).Message(), "child")
			require.Equal(t, protos.OrchestrationStatus_ORCHESTRATION_STATUS_FAILED, parent.status)
			require.Nil(t, parent.rewind)
			require.Same(t, child, s.instances[child.id])
			require.Same(t, info, child.parent)
		})
	}
}

func TestRewindReportsChildCollisionDuringRewrite(t *testing.T) {
	s := NewServer()
	defer s.Close()
	parent := s.newInstanceLocked("parent", "parent", nil, nil, nil, nil, nil, nil)
	started, oldExecutionID := parent.pending[0], parent.executionID
	created := helpers.NewSubOrchestrationCreatedEvent(1, "child", nil, nil, "child", nil)
	parent.history = []*protos.HistoryEvent{started, created, helpers.NewTaskFailedEvent(2, nil)}
	s.completeLocked(parent, protos.OrchestrationStatus_ORCHESTRATION_STATUS_FAILED, nil, failureDetails("failure", "boom"))

	_, err := s.RewindInstance(context.Background(), &protos.RewindInstanceRequest{InstanceId: parent.id})
	require.NoError(t, err)
	item := s.nextWorkItemLocked(nil, 1, workItemLimits{})
	unrelated := &instance{id: "child", status: protos.OrchestrationStatus_ORCHESTRATION_STATUS_RUNNING}
	s.instances[unrelated.id] = unrelated
	finishTestRewind(t, s, item, started, "discarded", created)
	require.Equal(t, protos.OrchestrationStatus_ORCHESTRATION_STATUS_FAILED, parent.status)
	require.Equal(t, oldExecutionID, parent.executionID)
	require.Equal(t, "OrchestrationAlreadyExists", parent.failure.GetErrorType())
	require.Same(t, unrelated, s.instances[unrelated.id])
	require.Nil(t, unrelated.parent)

	delete(s.instances, unrelated.id)
	_, err = s.RewindInstance(context.Background(), &protos.RewindInstanceRequest{InstanceId: parent.id})
	require.NoError(t, err)
	item = s.nextWorkItemLocked(nil, 2, workItemLimits{})
	finishTestRewind(t, s, item, started, "renewed", created)
	require.Equal(t, protos.OrchestrationStatus_ORCHESTRATION_STATUS_RUNNING, parent.status)
	child := s.instances["child"]
	require.NotNil(t, child)
	require.Equal(t, "parent", child.parent.GetOrchestrationInstance().GetInstanceId())
	require.Equal(t, "renewed", child.parent.GetOrchestrationInstance().GetExecutionId().GetValue())
}

func TestRewindRestoresLateChildResults(t *testing.T) {
	for _, state := range []protos.OrchestrationStatus{
		protos.OrchestrationStatus_ORCHESTRATION_STATUS_COMPLETED,
		protos.OrchestrationStatus_ORCHESTRATION_STATUS_FAILED,
		protos.OrchestrationStatus_ORCHESTRATION_STATUS_TERMINATED,
	} {
		t.Run(state.String(), func(t *testing.T) {
			s := NewServer()
			defer s.Close()
			parent := s.newInstanceLocked("parent", "parent", nil, nil, nil, nil, nil, nil)
			started := parent.pending[0]
			created := helpers.NewSubOrchestrationCreatedEvent(0, "child", nil, nil, "child", nil)
			parent.history = []*protos.HistoryEvent{started, created, helpers.NewTaskFailedEvent(1, nil)}
			s.completeLocked(parent, protos.OrchestrationStatus_ORCHESTRATION_STATUS_FAILED, nil, failureDetails("failure", "boom"))
			info := helpers.NewParentInfo(0, parent.name, parent.id)
			info.OrchestrationInstance.ExecutionId = wrapperspb.String(parent.executionID)
			child := &instance{
				id: "child", parent: info, status: state, completedAt: time.Now().Add(-time.Second),
				output: wrapperspb.String(`"child result"`),
			}
			if state == protos.OrchestrationStatus_ORCHESTRATION_STATUS_FAILED {
				child.failure = failureDetails("ChildFailed", "late failure")
			}
			s.instances[child.id] = child

			_, err := s.RewindInstance(context.Background(), &protos.RewindInstanceRequest{InstanceId: parent.id})
			require.NoError(t, err)
			_, err = s.RaiseEvent(context.Background(), &protos.RaiseEventRequest{InstanceId: parent.id, Name: "later"})
			require.NoError(t, err)
			item := s.nextWorkItemLocked(nil, 1, workItemLimits{})
			finishTestRewind(t, s, item, started, "renewed", created)
			require.Len(t, parent.pending, 3)
			if state == protos.OrchestrationStatus_ORCHESTRATION_STATUS_COMPLETED {
				require.Equal(t, `"child result"`, parent.pending[1].GetSubOrchestrationInstanceCompleted().GetResult().GetValue())
			} else {
				require.NotNil(t, parent.pending[1].GetSubOrchestrationInstanceFailed().GetFailureDetails())
			}
			require.Equal(t, "later", parent.pending[2].GetEventRaised().GetName())
			require.Equal(t, state, child.status)
		})
	}
}

func TestRewindRetargetsLateFailedChild(t *testing.T) {
	for _, mode := range []string{"buffered", "terminal-state"} {
		t.Run(mode, func(t *testing.T) {
			s := NewServer()
			defer s.Close()
			parent := s.newInstanceLocked("parent", "parent", nil, nil, nil, nil, nil, nil)
			started := parent.pending[0]
			created := helpers.NewSubOrchestrationCreatedEvent(0, "child", nil, nil, "child", nil)
			parent.history = []*protos.HistoryEvent{started, created}
			s.completeLocked(parent, protos.OrchestrationStatus_ORCHESTRATION_STATUS_FAILED, nil, failureDetails("failure", "parent"))
			info := helpers.NewParentInfo(0, parent.name, parent.id)
			info.OrchestrationInstance.ExecutionId = wrapperspb.String(parent.executionID)
			child := s.newInstanceLocked("child", "child", nil, nil, nil, info, nil, nil)
			child.history = []*protos.HistoryEvent{child.pending[0]}
			s.completeLocked(child, protos.OrchestrationStatus_ORCHESTRATION_STATUS_FAILED, nil, failureDetails("failure", "child"))
			if mode == "terminal-state" {
				parent.pending = nil
				parent.childResultSources = nil
			}

			_, err := s.RewindInstance(context.Background(), &protos.RewindInstanceRequest{InstanceId: parent.id})
			require.NoError(t, err)
			item := s.leaseOrchestrationLocked(parent, 1)
			finishTestRewind(t, s, item, started, "new-parent", created)
			require.Equal(t, "new-parent", child.parent.GetOrchestrationInstance().GetExecutionId().GetValue())
			require.Len(t, parent.pending, 2)
			require.NotNil(t, parent.pending[1].GetSubOrchestrationInstanceFailed())

			parent.history = append(parent.history, parent.pending[1])
			parent.pending = nil
			s.completeLocked(parent, protos.OrchestrationStatus_ORCHESTRATION_STATUS_FAILED, nil, failureDetails("failure", "child"))
			_, err = s.RewindInstance(context.Background(), &protos.RewindInstanceRequest{InstanceId: parent.id})
			require.NoError(t, err)
			item = s.leaseOrchestrationLocked(parent, 1)
			finishTestRewind(t, s, item, started, "new-parent-again", created)
			require.Equal(t, "new-parent-again", child.rewind.GetExecutionRewound().GetParentExecutionId().GetValue())
			require.Equal(t, "new-parent-again", child.parent.GetOrchestrationInstance().GetExecutionId().GetValue())

			// The renewed parent can fail again before the child's recovery is committed.
			parent.pending = nil
			s.completeLocked(parent, protos.OrchestrationStatus_ORCHESTRATION_STATUS_FAILED, nil, failureDetails("failure", "sibling"))
			_, err = s.RewindInstance(context.Background(), &protos.RewindInstanceRequest{InstanceId: parent.id})
			require.NoError(t, err)
			item = s.leaseOrchestrationLocked(parent, 1)
			finishTestRewind(t, s, item, started, "new-parent-third", created)
			require.Equal(t, "new-parent-third", child.parent.GetOrchestrationInstance().GetExecutionId().GetValue())
			require.Equal(t, "new-parent-third", child.rewind.GetExecutionRewound().GetParentExecutionId().GetValue())
		})
	}
}

func TestRewindDoesNotRetargetReusedChildWithBufferedResult(t *testing.T) {
	for _, mode := range []string{"unrelated", "new-execution"} {
		t.Run(mode, func(t *testing.T) {
			s := NewServer()
			defer s.Close()
			parent := s.newInstanceLocked("parent", "parent", nil, nil, nil, nil, nil, nil)
			started := parent.pending[0]
			created := helpers.NewSubOrchestrationCreatedEvent(0, "child", nil, nil, "child", nil)
			parent.history = []*protos.HistoryEvent{started, created}
			s.completeLocked(parent, protos.OrchestrationStatus_ORCHESTRATION_STATUS_FAILED, nil, failureDetails("failure", "parent"))
			info := helpers.NewParentInfo(0, parent.name, parent.id)
			info.OrchestrationInstance.ExecutionId = wrapperspb.String(parent.executionID)
			child := s.newInstanceLocked("child", "child", nil, nil, nil, info, nil, nil)
			s.completeLocked(child, protos.OrchestrationStatus_ORCHESTRATION_STATUS_COMPLETED, wrapperspb.String(`"done"`), nil)
			s.purgeLocked(child, false)
			var replacementParent *protos.ParentInstanceInfo
			if mode == "new-execution" {
				replacementParent = proto.CloneOf(info)
			}
			replacement := s.newInstanceLocked("child", "unrelated", nil, nil, nil, replacementParent, nil, nil)
			replacementExecutionID := replacement.executionID
			parentExecutionID := replacement.parent.GetOrchestrationInstance().GetExecutionId().GetValue()

			_, err := s.RewindInstance(context.Background(), &protos.RewindInstanceRequest{InstanceId: parent.id})
			require.NoError(t, err)
			item := s.leaseOrchestrationLocked(parent, 1)
			finishTestRewind(t, s, item, started, "new-parent", created)
			require.Same(t, replacement, s.instances["child"])
			require.Equal(t, replacementExecutionID, replacement.executionID)
			require.Equal(t, parentExecutionID, replacement.parent.GetOrchestrationInstance().GetExecutionId().GetValue())
			require.Len(t, parent.pending, 2)
			require.Equal(t, `"done"`, parent.pending[1].GetSubOrchestrationInstanceCompleted().GetResult().GetValue())
		})
	}
}

func TestRewindPreservesBufferedResultAfterChildPurge(t *testing.T) {
	for _, phase := range []string{"failed", "rewriting"} {
		t.Run(phase, func(t *testing.T) {
			s := NewServer()
			defer s.Close()
			parent := s.newInstanceLocked("parent", "parent", nil, nil, nil, nil, nil, nil)
			started := parent.pending[0]
			created := helpers.NewSubOrchestrationCreatedEvent(0, "child", nil, nil, "child", nil)
			parent.history = []*protos.HistoryEvent{started, created, helpers.NewTaskFailedEvent(1, nil)}
			s.completeLocked(parent, protos.OrchestrationStatus_ORCHESTRATION_STATUS_FAILED, nil, failureDetails("failure", "boom"))
			info := helpers.NewParentInfo(0, parent.name, parent.id)
			info.OrchestrationInstance.ExecutionId = wrapperspb.String(parent.executionID)
			child := s.newInstanceLocked("child", "child", nil, nil, nil, info, nil, nil)

			var item *protos.WorkItem
			if phase == "rewriting" {
				_, err := s.RewindInstance(context.Background(), &protos.RewindInstanceRequest{InstanceId: parent.id})
				require.NoError(t, err)
				item = s.leaseOrchestrationLocked(parent, 1)
			}
			s.completeLocked(child, protos.OrchestrationStatus_ORCHESTRATION_STATUS_COMPLETED, wrapperspb.String(`"done"`), nil)
			s.purgeLocked(child, false)
			if phase == "failed" {
				_, err := s.RewindInstance(context.Background(), &protos.RewindInstanceRequest{InstanceId: parent.id})
				require.NoError(t, err)
				item = s.leaseOrchestrationLocked(parent, 1)
			}
			finishTestRewind(t, s, item, started, "renewed", created)
			require.NotContains(t, s.instances, "child")
			require.Len(t, parent.pending, 2)
			require.Equal(t, `"done"`, parent.pending[1].GetSubOrchestrationInstanceCompleted().GetResult().GetValue())
		})
	}
}

func TestRewindPrefersRecoveredChildResultAfterPurge(t *testing.T) {
	s := NewServer()
	defer s.Close()
	parent := s.newInstanceLocked("parent", "parent", nil, nil, nil, nil, nil, nil)
	started := parent.pending[0]
	created := helpers.NewSubOrchestrationCreatedEvent(0, "child", nil, nil, "child", nil)
	parent.history = []*protos.HistoryEvent{started, created, helpers.NewTaskFailedEvent(1, nil)}
	s.completeLocked(parent, protos.OrchestrationStatus_ORCHESTRATION_STATUS_FAILED, nil, failureDetails("failure", "boom"))
	info := helpers.NewParentInfo(0, parent.name, parent.id)
	info.OrchestrationInstance.ExecutionId = wrapperspb.String(parent.executionID)
	child := s.newInstanceLocked("child", "child", nil, nil, nil, info, nil, nil)
	childStart := child.pending[0]
	child.history = []*protos.HistoryEvent{childStart}
	s.completeLocked(child, protos.OrchestrationStatus_ORCHESTRATION_STATUS_FAILED, nil, failureDetails("failure", "child"))

	_, err := s.RewindInstance(context.Background(), &protos.RewindInstanceRequest{InstanceId: child.id})
	require.NoError(t, err)
	childItem := s.leaseOrchestrationLocked(child, 1)
	finishTestRewind(t, s, childItem, childStart, "recovered-child")
	s.completeLocked(child, protos.OrchestrationStatus_ORCHESTRATION_STATUS_COMPLETED, wrapperspb.String(`"done"`), nil)
	s.purgeLocked(child, false)

	_, err = s.RewindInstance(context.Background(), &protos.RewindInstanceRequest{InstanceId: parent.id})
	require.NoError(t, err)
	parentItem := s.leaseOrchestrationLocked(parent, 1)
	finishTestRewind(t, s, parentItem, started, "renewed", created)
	require.Len(t, parent.pending, 2)
	require.Equal(t, `"done"`, parent.pending[1].GetSubOrchestrationInstanceCompleted().GetResult().GetValue())
}

func TestRewindRetargetsChildAlreadyRewriting(t *testing.T) {
	s := NewServer()
	defer s.Close()
	parent := s.newInstanceLocked("parent", "parent", nil, nil, nil, nil, nil, nil)
	parentStart := parent.pending[0]
	created := helpers.NewSubOrchestrationCreatedEvent(0, "child", nil, nil, "child", nil)
	parent.history = []*protos.HistoryEvent{parentStart, created, helpers.NewTaskFailedEvent(1, nil)}
	s.completeLocked(parent, protos.OrchestrationStatus_ORCHESTRATION_STATUS_FAILED, nil, failureDetails("failure", "boom"))
	info := helpers.NewParentInfo(0, parent.name, parent.id)
	info.OrchestrationInstance.ExecutionId = wrapperspb.String(parent.executionID)
	child := s.newInstanceLocked("child", "child", nil, nil, nil, info, nil, nil)
	childStart := child.pending[0]
	child.history = []*protos.HistoryEvent{childStart}
	s.completeLocked(child, protos.OrchestrationStatus_ORCHESTRATION_STATUS_FAILED, nil, failureDetails("failure", "boom"))

	_, err := s.RewindInstance(context.Background(), &protos.RewindInstanceRequest{InstanceId: child.id})
	require.NoError(t, err)
	childItem := s.leaseOrchestrationLocked(child, 1)
	_, err = s.RewindInstance(context.Background(), &protos.RewindInstanceRequest{InstanceId: parent.id})
	require.NoError(t, err)
	parentItem := s.leaseOrchestrationLocked(parent, 1)
	finishTestRewind(t, s, parentItem, parentStart, "new-parent", created)
	finishTestRewind(t, s, childItem, childStart, "new-child")
	require.Equal(t, "new-parent", child.parent.GetOrchestrationInstance().GetExecutionId().GetValue())
	require.Equal(t, "new-parent", child.history[0].GetExecutionStarted().GetParentInstance().GetOrchestrationInstance().GetExecutionId().GetValue())

	s.completeLocked(child, protos.OrchestrationStatus_ORCHESTRATION_STATUS_COMPLETED, wrapperspb.String(`"done"`), nil)
	require.Len(t, parent.pending, 2)
	require.Equal(t, `"done"`, parent.pending[1].GetSubOrchestrationInstanceCompleted().GetResult().GetValue())
}

func TestCompleteOrchestratorTaskSplitsByProcessedEvents(t *testing.T) {
	s := NewServer()
	defer s.Close()

	inst := &instance{
		id:          "id",
		executionID: "execution",
		status:      protos.OrchestrationStatus_ORCHESTRATION_STATUS_RUNNING,
		leased:      true,
	}
	started := &protos.HistoryEvent{
		EventType: &protos.HistoryEvent_ExecutionStarted{ExecutionStarted: &protos.ExecutionStartedEvent{}},
	}
	suspended := helpers.NewSuspendOrchestrationEvent("pause")
	pending := helpers.NewTimerFiredEvent(1, nil, nil)
	s.instances[inst.id] = inst
	s.orchLeases["token"] = &orchestrationLease{
		inst: inst,
		events: []*protos.HistoryEvent{
			helpers.NewOrchestratorStartedEvent(),
			started,
			suspended,
			pending,
		},
	}

	_, err := s.CompleteOrchestratorTask(t.Context(), &protos.OrchestratorResponse{
		CompletionToken:    "token",
		NumEventsProcessed: wrapperspb.Int32(1),
	})
	require.NoError(t, err)
	require.Len(t, inst.history, 4)
	assert.Same(t, started, inst.history[1])
	assert.Same(t, suspended, inst.history[2])
	require.Len(t, inst.pending, 1)
	assert.Same(t, pending, inst.pending[0])
}

func TestRecursiveTerminationDoesNotTerminateReusedChildID(t *testing.T) {
	s := NewServer()
	defer s.Close()

	parent := &instance{
		id:       "parent",
		status:   protos.OrchestrationStatus_ORCHESTRATION_STATUS_RUNNING,
		children: []string{"child"},
		history:  []*protos.HistoryEvent{{}},
	}
	replacement := &instance{
		id:     "child",
		status: protos.OrchestrationStatus_ORCHESTRATION_STATUS_RUNNING,
	}
	s.instances[parent.id] = parent
	s.instances[replacement.id] = replacement

	s.mu.Lock()
	s.terminateLocked(parent, nil, true)
	s.mu.Unlock()

	assert.Equal(t, protos.OrchestrationStatus_ORCHESTRATION_STATUS_RUNNING, replacement.status)
}

func TestWorkItemTooLargeForOneMessageFailsInstance(t *testing.T) {
	s := NewServer()
	defer s.Close()

	inst := &instance{
		id:          "id",
		executionID: "execution",
		status:      protos.OrchestrationStatus_ORCHESTRATION_STATUS_RUNNING,
		pending: []*protos.HistoryEvent{
			helpers.NewEventRaisedEvent("big", wrapperspb.String(strings.Repeat("x", maxOrchestrationWorkItemSize))),
		},
	}
	s.instances[inst.id] = inst

	s.mu.Lock()
	item := s.leaseOrchestrationLocked(inst, 1)
	s.mu.Unlock()

	assert.Nil(t, item)
	assert.Empty(t, s.orchLeases)
	assert.Equal(t, protos.OrchestrationStatus_ORCHESTRATION_STATUS_FAILED, inst.status)
	require.NotNil(t, inst.failure)
	assert.Equal(t, "OrchestrationWorkItemTooLarge", inst.failure.GetErrorType())
}

func TestWorkItemFiltersMatchWorkerSemantics(t *testing.T) {
	inst := &instance{name: "MyOrchestrator", version: wrapperspb.String("V1")}
	activity := &protos.ActivityRequest{Name: "MyActivity", Version: wrapperspb.String("V1")}

	require.True(t, matchesOrchestrationFilter(&protos.WorkItemFilters{
		Activities: []*protos.ActivityFilter{{Name: "unrelated"}},
	}, inst), "an empty orchestration filter must not filter out orchestrations")
	require.True(t, matchesActivityFilter(&protos.WorkItemFilters{
		Orchestrations: []*protos.OrchestrationFilter{{Name: "unrelated"}},
	}, activity), "an empty activity filter must not filter out activities")
	require.True(t, matchesOrchestrationFilter(&protos.WorkItemFilters{
		Orchestrations: []*protos.OrchestrationFilter{{Name: "myorchestrator", Versions: []string{"v1"}}},
	}, inst), "orchestration names and versions should match case-insensitively")
	require.True(t, matchesActivityFilter(&protos.WorkItemFilters{
		Activities: []*protos.ActivityFilter{{Name: "myactivity", Versions: []string{"v1"}}},
	}, activity), "activity names and versions should match case-insensitively")
	require.False(t, matchesOrchestrationFilter(&protos.WorkItemFilters{
		Orchestrations: []*protos.OrchestrationFilter{{Name: helpers.RejectAllWorkItemFilterName}},
	}, inst), "the reject-all sentinel must remain honored")
}
