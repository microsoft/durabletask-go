package inmemory

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/microsoft/durabletask-go/internal/helpers"
	"github.com/microsoft/durabletask-go/internal/protos"
)

type testWorkItemStream struct {
	grpc.ServerStream
	ctx   context.Context
	items chan *protos.WorkItem
}

func (s *testWorkItemStream) Context() context.Context { return s.ctx }

func (s *testWorkItemStream) Send(item *protos.WorkItem) error {
	select {
	case s.items <- item:
		return nil
	case <-s.ctx.Done():
		return s.ctx.Err()
	}
}

// openTestStream serves GetWorkItems on a test stream. The returned function
// closes the stream and waits until its leases are released.
func openTestStream(t *testing.T, s *Server) (*testWorkItemStream, func()) {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	stream := &testWorkItemStream{ctx: ctx, items: make(chan *protos.WorkItem)}
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = s.GetWorkItems(&protos.GetWorkItemsRequest{}, stream)
	}()
	closeStream := func() {
		cancel()
		<-done
	}
	t.Cleanup(closeStream)
	return stream, closeStream
}

func (s *testWorkItemStream) receive(t *testing.T) *protos.WorkItem {
	t.Helper()
	select {
	case item := <-s.items:
		return item
	case <-time.After(10 * time.Second):
		require.FailNow(t, "timed out waiting for a work item")
		return nil
	}
}

func TestClosedStreamLeasesAreRedeliveredAndOldTokensRejected(t *testing.T) {
	s := NewServer()
	defer s.Close()
	ctx := t.Context()

	_, err := s.StartInstance(ctx, &protos.CreateInstanceRequest{InstanceId: "id", Name: "orchestrator"})
	require.NoError(t, err)
	first, closeFirst := openTestStream(t, s)
	start := first.receive(t)
	require.NotNil(t, start.GetOrchestratorRequest())
	_, err = s.CompleteOrchestratorTask(ctx, &protos.OrchestratorResponse{
		CompletionToken: start.GetCompletionToken(),
		Actions: []*protos.OrchestratorAction{{
			Id: 0,
			OrchestratorActionType: &protos.OrchestratorAction_ScheduleTask{
				ScheduleTask: &protos.ScheduleTaskAction{Name: "activity"},
			},
		}},
	})
	require.NoError(t, err)
	oldActivity := first.receive(t)
	require.NotNil(t, oldActivity.GetActivityRequest())
	_, err = s.RaiseEvent(ctx, &protos.RaiseEventRequest{InstanceId: "id", Name: "A"})
	require.NoError(t, err)
	oldOrchestration := first.receive(t)
	require.NotNil(t, oldOrchestration.GetOrchestratorRequest())
	// B arrives while A's turn is leased.
	_, err = s.RaiseEvent(ctx, &protos.RaiseEventRequest{InstanceId: "id", Name: "B"})
	require.NoError(t, err)

	closeFirst()
	second, _ := openTestStream(t, s)
	var orchestration, activity *protos.WorkItem
	for orchestration == nil || activity == nil {
		item := second.receive(t)
		if item.GetOrchestratorRequest() != nil {
			orchestration = item
		} else {
			activity = item
		}
	}

	var raised []string
	for _, event := range orchestration.GetOrchestratorRequest().GetNewEvents() {
		if e := event.GetEventRaised(); e != nil {
			raised = append(raised, e.GetName())
		}
	}
	assert.Equal(t, []string{"A", "B"}, raised)
	assert.Equal(t, "activity", activity.GetActivityRequest().GetName())
	assert.NotEqual(t, oldOrchestration.GetCompletionToken(), orchestration.GetCompletionToken())
	assert.NotEqual(t, oldActivity.GetCompletionToken(), activity.GetCompletionToken())

	_, err = s.CompleteOrchestratorTask(ctx, &protos.OrchestratorResponse{CompletionToken: oldOrchestration.GetCompletionToken()})
	assert.Equal(t, codes.NotFound, status.Code(err))
	_, err = s.CompleteActivityTask(ctx, &protos.ActivityResponse{CompletionToken: oldActivity.GetCompletionToken()})
	assert.Equal(t, codes.NotFound, status.Code(err))
	_, err = s.CompleteOrchestratorTask(ctx, &protos.OrchestratorResponse{CompletionToken: orchestration.GetCompletionToken()})
	require.NoError(t, err)
	_, err = s.CompleteActivityTask(ctx, &protos.ActivityResponse{CompletionToken: activity.GetCompletionToken()})
	require.NoError(t, err)
}

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

func TestRecursiveTerminationDuringTurnTerminatesNewChildren(t *testing.T) {
	s := NewServer()
	defer s.Close()
	ctx := t.Context()

	_, err := s.StartInstance(ctx, &protos.CreateInstanceRequest{InstanceId: "parent", Name: "orchestrator"})
	require.NoError(t, err)
	s.mu.Lock()
	item := s.nextWorkItemLocked(nil, 1, workItemLimits{})
	s.mu.Unlock()
	require.NotNil(t, item.GetOrchestratorRequest())

	_, err = s.TerminateInstance(ctx, &protos.TerminateRequest{InstanceId: "parent", Recursive: true})
	require.NoError(t, err)
	_, err = s.CompleteOrchestratorTask(ctx, &protos.OrchestratorResponse{
		CompletionToken: item.GetCompletionToken(),
		Actions: []*protos.OrchestratorAction{{
			Id: 0,
			OrchestratorActionType: &protos.OrchestratorAction_CreateSubOrchestration{
				CreateSubOrchestration: &protos.CreateSubOrchestrationAction{InstanceId: "child", Name: "orchestrator"},
			},
		}},
	})
	require.NoError(t, err)

	resp, err := s.GetInstance(ctx, &protos.GetInstanceRequest{InstanceId: "child"})
	require.NoError(t, err)
	require.True(t, resp.GetExists())
	assert.Equal(t, protos.OrchestrationStatus_ORCHESTRATION_STATUS_TERMINATED, resp.GetOrchestrationState().GetOrchestrationStatus())
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
