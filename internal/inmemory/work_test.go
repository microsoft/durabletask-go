package inmemory

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
