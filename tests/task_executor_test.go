// This file contains tests for the task executor, which is used only for orchestrations authored in Go.
package tests

import (
	"testing"
	"time"

	"github.com/dapr/durabletask-go/api"
	"github.com/dapr/durabletask-go/internal/helpers"
	"github.com/dapr/durabletask-go/internal/protos"
	"github.com/dapr/durabletask-go/task"
	"github.com/stretchr/testify/require"
)

// Verifies that the WaitForSingleEvent API implicitly creates a timer when the timeout is non-zero.
func Test_Executor_WaitForEventSchedulesTimer(t *testing.T) {
	timerDuration := 5 * time.Second
	r := task.NewTaskRegistry()
	r.AddOrchestratorN("Orchestration", func(ctx *task.OrchestrationContext) (any, error) {
		var value int
		ctx.WaitForSingleEvent("MyEvent", timerDuration).Await(&value)
		return value, nil
	})

	iid := api.InstanceID("abc123")
	startEvent := helpers.NewOrchestratorStartedEvent() // determines the current orchestrator time
	oldEvents := []*protos.HistoryEvent{}
	newEvents := []*protos.HistoryEvent{
		startEvent,
		helpers.NewExecutionStartedEvent("Orchestration", string(iid), nil, nil, nil, nil),
	}

	// Execute the orchestrator function and expect to get back a single timer action
	executor := task.NewTaskExecutor(r)
	results, err := executor.ExecuteOrchestrator(ctx, iid, oldEvents, newEvents)
	require.NoError(t, err)
	require.Equal(t, 1, len(results.Response.Actions), "Expected a single action to be scheduled")
	createTimerAction := results.Response.Actions[0].GetCreateTimer()
	require.NotNil(t, createTimerAction, "Expected the scheduled action to be a timer")
	require.WithinDuration(t, startEvent.Timestamp.AsTime().Add(timerDuration), createTimerAction.FireAt.AsTime(), 0)
}

// This is a regression test for an issue where suspended orchestrations would continue to return
// actions prior to being resumed. In this case, the `WaitForSingleEvent` action would continue
// return a timer action even after the orchestration was suspended, which is not correct.
// The correct behavior is that a suspended orchestration should not return any actions.
func Test_Executor_SuspendStopsAllActions(t *testing.T) {
	r := task.NewTaskRegistry()
	r.AddOrchestratorN("SuspendResumeOrchestration", func(ctx *task.OrchestrationContext) (any, error) {
		var value int
		ctx.WaitForSingleEvent("MyEvent", 5*time.Second).Await(&value)
		return value, nil
	})

	executor := task.NewTaskExecutor(r)
	iid := api.InstanceID("abc123")
	oldEvents := []*protos.HistoryEvent{}
	newEvents := []*protos.HistoryEvent{
		helpers.NewOrchestratorStartedEvent(),
		helpers.NewExecutionStartedEvent("SuspendResumeOrchestration", string(iid), nil, nil, nil, nil),
		helpers.NewSuspendOrchestrationEvent(""),
	}

	// Execute the orchestrator function and expect to get back no actions
	results, err := executor.ExecuteOrchestrator(ctx, iid, oldEvents, newEvents)
	require.NoError(t, err)
	require.Empty(t, results.Response.Actions, "Suspended orchestrations should not have any actions")
}
