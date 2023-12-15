package tests

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/microsoft/durabletask-go/api"
	"github.com/microsoft/durabletask-go/backend"
	"github.com/microsoft/durabletask-go/internal/helpers"
	"github.com/microsoft/durabletask-go/internal/protos"
	"github.com/microsoft/durabletask-go/tests/mocks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

// https://github.com/stretchr/testify/issues/519
var (
	anyContext = mock.Anything
)

func Test_TryProcessSingleOrchestrationWorkItem_BasicFlow(t *testing.T) {
	ctx := context.Background()
	wi := &backend.OrchestrationWorkItem{
		InstanceID: "test123",
		NewEvents:  []*protos.HistoryEvent{helpers.NewExecutionStartedEvent("MyOrch", "test123", nil, nil, nil)},
	}
	state := &backend.OrchestrationRuntimeState{}
	result := &backend.ExecutionResults{Response: &protos.OrchestratorResponse{}}

	be := mocks.NewBackend(t)
	ex := mocks.NewExecutor(t)
	be.EXPECT().GetOrchestrationWorkItem(anyContext).Return(wi, nil).Once()
	be.EXPECT().GetOrchestrationRuntimeState(anyContext, wi).Return(state, nil).Once()
	ex.EXPECT().ExecuteOrchestrator(anyContext, wi.InstanceID, state.OldEvents(), mock.Anything).Return(result, nil).Once()
	be.EXPECT().CompleteOrchestrationWorkItem(anyContext, wi, mock.Anything).Return(nil).Once()

	worker := backend.NewOrchestrationWorker(be, ex, logger)
	ok, err := worker.ProcessNext(ctx)
	worker.StopAndDrain()

	// Successfully processing a work-item should result in a nil error
	assert.Nil(t, err)
	assert.True(t, ok)
}

func Test_TryProcessSingleOrchestrationWorkItem_NoWorkItems(t *testing.T) {
	ctx := context.Background()
	be := mocks.NewBackend(t)
	be.EXPECT().GetOrchestrationWorkItem(anyContext).Return(nil, backend.ErrNoWorkItems).Once()

	w := backend.NewOrchestrationWorker(be, nil, logger)
	ok, err := w.ProcessNext(ctx)
	assert.Nil(t, err)
	assert.False(t, ok)
}

func Test_TryProcessSingleOrchestrationWorkItem_ExecutionStartedAndCompleted(t *testing.T) {
	ctx := context.Background()
	iid := api.InstanceID("test123")

	// Simulate getting an ExecutionStarted message from the orchestration queue
	startEvent := helpers.NewExecutionStartedEvent("MyOrchestration", string(iid), nil, nil, nil)
	wi := &backend.OrchestrationWorkItem{
		InstanceID: iid,
		NewEvents:  []*protos.HistoryEvent{startEvent},
	}

	// Empty orchestration runtime state since we're starting a new execution from scratch
	state := backend.NewOrchestrationRuntimeState(iid, []*protos.HistoryEvent{})

	be := mocks.NewBackend(t)
	be.EXPECT().GetOrchestrationWorkItem(anyContext).Return(wi, nil).Once()
	be.EXPECT().GetOrchestrationRuntimeState(anyContext, wi).Return(state, nil).Once()

	ex := mocks.NewExecutor(t)

	// Return an execution completed action to simulate the completion of the orchestration (a no-op)
	resultValue := "done"
	result := &backend.ExecutionResults{
		Response: &protos.OrchestratorResponse{
			Actions: []*protos.OrchestratorAction{
				helpers.NewCompleteOrchestrationAction(
					-1,
					protos.OrchestrationStatus_ORCHESTRATION_STATUS_COMPLETED,
					wrapperspb.String(resultValue),
					nil,
					nil),
			},
		},
	}

	// Execute should be called with an empty oldEvents list. NewEvents should contain two items,
	// but there doesn't seem to be a good way to assert this.
	ex.EXPECT().ExecuteOrchestrator(anyContext, iid, []*protos.HistoryEvent{}, mock.Anything).Return(result, nil).Once()

	// The work item should be completed with the result of the execution
	be.EXPECT().CompleteOrchestrationWorkItem(anyContext, wi, mock.Anything).Return(nil).Once()

	// Set up and run the test
	worker := backend.NewOrchestrationWorker(be, ex, logger)
	ok, err := worker.ProcessNext(ctx)
	worker.StopAndDrain()

	// Successfully processing a work-item should result in a nil error
	assert.Nil(t, err)
	assert.True(t, ok)
}

func Test_ChunkRuntimeStateChanges(t *testing.T) {
	iid := api.InstanceID("test123")

	run := func(
		wi *backend.OrchestrationWorkItem,
		config backend.ChunkingConfiguration,
		actions []*protos.OrchestratorAction,
		expectedChunks int,
		continuesAsNew bool,
	) {
		state := backend.NewOrchestrationRuntimeState(wi.InstanceID, nil)

		be := mocks.NewBackend(t)
		be.EXPECT().GetOrchestrationWorkItem(anyContext).Return(wi, nil).Once()
		be.EXPECT().GetOrchestrationRuntimeState(anyContext, wi).Return(state, nil).Once()
		be.EXPECT().CompleteOrchestrationWorkItem(anyContext, wi, mock.Anything).Return(nil).Times(expectedChunks)

		result := &backend.ExecutionResults{
			Response: &protos.OrchestratorResponse{Actions: actions},
		}

		ex := mocks.NewExecutor(t)
		ex.EXPECT().ExecuteOrchestrator(anyContext, wi.InstanceID, mock.Anything, mock.Anything).Return(result, nil).Once()
		if continuesAsNew {
			// There will be one more execution cycle after the first one and we need to return an empty action set
			// to prevent execution from going forever.
			nextResult := &backend.ExecutionResults{Response: &protos.OrchestratorResponse{}}
			ex.EXPECT().ExecuteOrchestrator(anyContext, wi.InstanceID, mock.Anything, mock.Anything).Return(nextResult, nil).Once()
		}

		// Set up and run the test
		worker := backend.NewOrchestrationWorker(be, ex, logger, backend.WithChunkingConfiguration(config))
		ok, err := worker.ProcessNext(ctx)
		require.NoError(t, err)
		require.True(t, ok)
		worker.StopAndDrain()
	}

	t.Run("OnlyNewEvents", func(t *testing.T) {
		t.Run("EventCount", func(t *testing.T) {
			config := backend.ChunkingConfiguration{MaxHistoryEventCount: 10}
			wi := &backend.OrchestrationWorkItem{
				InstanceID: iid,
				NewEvents:  []*protos.HistoryEvent{helpers.NewExecutionStartedEvent("MyOrch", string(iid), nil, nil, nil)},
			}

			// Append a bunch of new events to the work item
			// 2 events (orchestratorStarted and executionStarted) + 48 external events = 50 total
			for i := 0; i < 48; i++ {
				wi.NewEvents = append(wi.NewEvents, helpers.NewEventRaisedEvent("MyEvent", nil))
			}

			// No actions returned by the orchestrator for this test
			actions := make([]*protos.OrchestratorAction, 0)

			// We expect that completion will be called 5 times, once for each chunk (50 / 10 = 5)
			expectedCompletionCalls := 5
			run(wi, config, actions, expectedCompletionCalls, false)
		})

		t.Run("EventSize", func(t *testing.T) {
			config := backend.ChunkingConfiguration{MaxHistoryEventSizeInKB: 1}
			wi := &backend.OrchestrationWorkItem{
				InstanceID: iid,
				NewEvents:  []*protos.HistoryEvent{helpers.NewExecutionStartedEvent("MyOrch", string(iid), nil, nil, nil)},
			}

			// Append a bunch of new events to the work item that are ~900 bytes in size
			for i := 0; i < 4; i++ {
				payload1KB := strings.Repeat("a", 900) // 900 UTF-8 bytes
				wi.NewEvents = append(wi.NewEvents, helpers.NewEventRaisedEvent("MyEvent", wrapperspb.String(payload1KB)))
			}

			// No actions returned by the orchestrator for this test
			actions := make([]*protos.OrchestratorAction, 0)

			// We expect that completion will be called 5 times, once for each chunk.
			// Chunk 1: OrchestratorStarted (27), ExecutionStarted (86)
			// Chunk 2: EventRaised (944)
			// Chunk 3: EventRaised (944)
			// Chunk 4: EventRaised (944)
			// Chunk 5: EventRaised (944)
			expectedCompletionCalls := 5
			run(wi, config, actions, expectedCompletionCalls, false)
		})
	})

	t.Run("OnlyActionEvents", func(t *testing.T) {
		config := backend.ChunkingConfiguration{MaxHistoryEventCount: 10}
		wi := &backend.OrchestrationWorkItem{
			InstanceID: iid,
			NewEvents:  []*protos.HistoryEvent{helpers.NewExecutionStartedEvent("MyOrch", string(iid), nil, nil, nil)},
		}

		// The orchestrator will return 48 actions, which will be chunked into 5 chunks
		// 2 new events (orchestratorStarted and executionStarted) + 48 create timer actions = 50 total
		actions := make([]*protos.OrchestratorAction, 48)
		for i := 0; i < 48; i++ {
			actions[i] = helpers.NewCreateTimerAction(int32(i), time.Now())
		}

		// We expect that completion will be called 5 times, once for each chunk (50 / 10 = 5)
		expectedCompletionCalls := 5
		run(wi, config, actions, expectedCompletionCalls, false)
	})

	t.Run("OnlyCarryoverEvents", func(t *testing.T) {
		config := backend.ChunkingConfiguration{MaxHistoryEventCount: 10}
		wi := &backend.OrchestrationWorkItem{
			InstanceID: iid,
			NewEvents:  []*protos.HistoryEvent{helpers.NewExecutionStartedEvent("MyOrch", string(iid), nil, nil, nil)},
		}

		// The orchestrator will return a single ContinueAsNew action with 48 carryover events, which will be chunked into 5 chunks
		// 2 new events (orchestratorStarted and executionStarted) + 48 external event = 50 total
		carroverEvents := make([]*protos.HistoryEvent, 48)
		for i := 0; i < 48; i++ {
			carroverEvents[i] = helpers.NewEventRaisedEvent("MyEvent", nil)
		}
		continueAsNewAction := helpers.NewCompleteOrchestrationAction(
			-1,
			protos.OrchestrationStatus_ORCHESTRATION_STATUS_CONTINUED_AS_NEW,
			nil,
			carroverEvents,
			nil)
		actions := []*protos.OrchestratorAction{continueAsNewAction}

		// We expect that completion will be called 5 times, once for each chunk (50 / 10 = 5)
		expectedCompletionCalls := 5
		run(wi, config, actions, expectedCompletionCalls, true)
	})

	t.Run("NewEventsAndActionEvents", func(t *testing.T) {
		config := backend.ChunkingConfiguration{MaxHistoryEventCount: 10}
		wi := &backend.OrchestrationWorkItem{
			InstanceID: iid,
			NewEvents:  []*protos.HistoryEvent{helpers.NewExecutionStartedEvent("MyOrch", string(iid), nil, nil, nil)},
		}

		// Append a bunch of new events to the work item
		// 2 events (orchestratorStarted and executionStarted) + 24 external events + 24 task scheduled actions = 50 total
		for i := 0; i < 24; i++ {
			wi.NewEvents = append(wi.NewEvents, helpers.NewEventRaisedEvent("MyEvent", nil))
		}
		actions := make([]*protos.OrchestratorAction, 24)
		for i := 0; i < 24; i++ {
			actions[i] = helpers.NewScheduleTaskAction(int32(i), "MyAct", nil)
		}

		// We expect that completion will be called 5 times, once for each chunk (50 / 10 = 5)
		expectedCompletionCalls := 5
		run(wi, config, actions, expectedCompletionCalls, false)
	})
}
