package tests

import (
	"context"
	"testing"

	"github.com/microsoft/durabletask-go/api"
	"github.com/microsoft/durabletask-go/backend"
	"github.com/microsoft/durabletask-go/internal/helpers"
	"github.com/microsoft/durabletask-go/internal/protos"
	"github.com/microsoft/durabletask-go/tests/mocks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
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
	be.EXPECT().GetOrchestrationWorkItem(anyContext).Return(wi, nil).Once()
	be.EXPECT().GetOrchestrationRuntimeState(anyContext, wi).Return(state, nil).Once()
	be.EXPECT().CompleteOrchestrationWorkItem(anyContext, wi).Return(nil).Once()

	ex := mocks.NewExecutor(t)
	ex.EXPECT().ExecuteOrchestrator(anyContext, wi.InstanceID, state.OldEvents(), mock.Anything).Return(result, nil).Once()

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

	// After execution, the Complete action should be called
	be.EXPECT().CompleteOrchestrationWorkItem(anyContext, wi).Return(nil).Once()

	// Set up and run the test
	worker := backend.NewOrchestrationWorker(be, ex, logger)
	ok, err := worker.ProcessNext(ctx)
	worker.StopAndDrain()

	// Successfully processing a work-item should result in a nil error
	assert.Nil(t, err)
	assert.True(t, ok)
}
