// This file contains tests for the task executor, which is used only for orchestrations authored in Go.
package tests

import (
	"testing"
	"time"

	"github.com/dapr/durabletask-go/api"
	"github.com/dapr/durabletask-go/api/protos"
	"github.com/dapr/durabletask-go/task"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"
	"google.golang.org/protobuf/types/known/wrapperspb"
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
	startEvent := &protos.HistoryEvent{
		EventId:   -1,
		Timestamp: timestamppb.Now(),
		EventType: &protos.HistoryEvent_OrchestratorStarted{
			OrchestratorStarted: &protos.OrchestratorStartedEvent{},
		},
	}
	oldEvents := []*protos.HistoryEvent{}
	newEvents := []*protos.HistoryEvent{
		startEvent,
		{
			EventId:   -1,
			Timestamp: timestamppb.New(time.Now()),
			EventType: &protos.HistoryEvent_ExecutionStarted{
				ExecutionStarted: &protos.ExecutionStartedEvent{
					Name: "Orchestration",
					OrchestrationInstance: &protos.OrchestrationInstance{
						InstanceId:  string(iid),
						ExecutionId: wrapperspb.String(uuid.New().String()),
					},
				},
			},
		},
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
		{
			EventId:   -1,
			Timestamp: timestamppb.Now(),
			EventType: &protos.HistoryEvent_OrchestratorStarted{
				OrchestratorStarted: &protos.OrchestratorStartedEvent{},
			},
		},
		{
			EventId:   -1,
			Timestamp: timestamppb.New(time.Now()),
			EventType: &protos.HistoryEvent_ExecutionStarted{
				ExecutionStarted: &protos.ExecutionStartedEvent{
					Name: "SuspendResumeOrchestration",
					OrchestrationInstance: &protos.OrchestrationInstance{
						InstanceId:  string(iid),
						ExecutionId: wrapperspb.String(uuid.New().String()),
					},
				},
			},
		},
		{
			EventId:   -1,
			Timestamp: timestamppb.New(time.Now()),
			EventType: &protos.HistoryEvent_ExecutionSuspended{
				ExecutionSuspended: &protos.ExecutionSuspendedEvent{},
			},
		},
	}

	// Execute the orchestrator function and expect to get back no actions
	results, err := executor.ExecuteOrchestrator(ctx, iid, oldEvents, newEvents)
	require.NoError(t, err)
	require.Empty(t, results.Response.Actions, "Suspended orchestrations should not have any actions")
}
