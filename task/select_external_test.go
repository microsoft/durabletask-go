package task_test

import (
	"context"
	"testing"

	"github.com/microsoft/durabletask-go/internal/helpers"
	"github.com/microsoft/durabletask-go/internal/protos"
	"github.com/microsoft/durabletask-go/task"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

type wrappedSelectCase struct {
	task.SelectCase
}

func TestEmbeddedSelectCase(t *testing.T) {
	for _, kind := range []string{"task", "event", "channel"} {
		t.Run(kind, func(t *testing.T) {
			registry := task.NewTaskRegistry()
			require.NoError(t, registry.AddOrchestratorN("wrapped", func(ctx *task.OrchestrationContext) (any, error) {
				calls := 0
				switch kind {
				case "task":
					activity := ctx.CallActivity("work")
					if err := activity.Await(nil); err != nil {
						return nil, err
					}
					ctx.Select(wrappedSelectCase{task.OnTask(activity, func(task.Task) { calls++ })})
				case "event":
					events := task.NewEventChannel[int](ctx, "value")
					if err := ctx.CallActivity("work").Await(nil); err != nil {
						return nil, err
					}
					ctx.Select(wrappedSelectCase{task.OnEvent(events, func(value int) { calls += value })})
				case "channel":
					channel := task.NewChannel[int](ctx, 0)
					group := ctx.NewWaitGroup()
					group.Add(1)
					ctx.Go(func(branch *task.OrchestrationContext) {
						defer group.Done()
						send := task.OnSend(channel, 1, nil)
						branch.Select(wrappedSelectCase{send}, send)
					})
					ctx.Select(wrappedSelectCase{task.OnReceive(channel, func(value int) { calls += value })})
					group.Wait(ctx)
				}
				return calls, nil
			}))
			events := []*protos.HistoryEvent{
				helpers.NewOrchestratorStartedEvent(),
				helpers.NewExecutionStartedEvent("wrapped", "instance", nil, nil, nil, nil),
			}
			if kind != "channel" {
				events = append(events,
					helpers.NewTaskScheduledEvent(0, "work", nil, nil, nil),
					helpers.NewEventRaisedEvent("value", wrapperspb.String("1")),
					helpers.NewTaskCompletedEvent(0, nil))
			}
			for _, replay := range []bool{false, true} {
				var oldEvents, newEvents []*protos.HistoryEvent
				if replay {
					oldEvents = events
				} else {
					newEvents = events
				}
				result, err := task.NewTaskExecutor(registry).ExecuteOrchestrator(
					context.Background(), "instance", oldEvents, newEvents, nil)
				require.NoError(t, err)
				require.Len(t, result.Response.Actions, 1)
				completed := result.Response.Actions[0].GetCompleteOrchestration()
				require.Equal(t, protos.OrchestrationStatus_ORCHESTRATION_STATUS_COMPLETED,
					completed.GetOrchestrationStatus(), "%+v", completed.GetFailureDetails())
				require.Equal(t, "1", completed.GetResult().GetValue())
			}
		})
	}
}
