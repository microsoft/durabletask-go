package task

import (
	"errors"
	"testing"

	"github.com/microsoft/durabletask-go/internal/helpers"
	"github.com/microsoft/durabletask-go/internal/protos"
	"github.com/stretchr/testify/require"
)

func TestCanceledScopeSkipsUnstartedCoroutines(t *testing.T) {
	for _, ordering := range []string{"cancel-before-enqueue", "cancel-after-enqueue", "already-canceled"} {
		t.Run(ordering, func(t *testing.T) {
			registry := NewTaskRegistry()
			require.NoError(t, registry.AddOrchestratorN("canceled-callback", func(ctx *OrchestrationContext) (any, error) {
				child, cancel := ctx.WithCancel()
				if ordering != "cancel-after-enqueue" {
					cancel()
				}
				if ordering == "already-canceled" {
					if err := child.WaitForSingleEvent("never", -1).Await(nil); !errors.Is(err, ErrTaskCanceled) {
						return nil, errors.New("cancellation was not applied")
					}
				}
				ran, siblingRan := false, false
				child.Go(func(ctx *OrchestrationContext) {
					ran = true
					ctx.SetCustomStatus("canceled callback ran")
				})
				cancel()
				group := ctx.NewWaitGroup()
				group.Add(1)
				ctx.Go(func(*OrchestrationContext) {
					siblingRan = true
					group.Done()
				})
				group.Wait(ctx)
				return !ran && siblingRan, nil
			}))
			events := []*protos.HistoryEvent{
				helpers.NewOrchestratorStartedEvent(),
				helpers.NewExecutionStartedEvent("canceled-callback", "instance", nil, nil, nil, nil),
			}
			for _, replay := range []bool{false, true} {
				var oldEvents, newEvents []*protos.HistoryEvent
				if replay {
					oldEvents = events
				} else {
					newEvents = events
				}
				response := executeOrchestrationTurn(t, registry, "instance", oldEvents, newEvents)
				require.Equal(t, "true", completionResult(t, response), "replay=%t", replay)
				require.Empty(t, response.GetCustomStatus().GetValue(), "replay=%t", replay)
			}
		})
	}
}
