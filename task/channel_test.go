package task

import (
	"errors"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/microsoft/durabletask-go/internal/helpers"
	"github.com/microsoft/durabletask-go/internal/protos"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

func channelRegistry(t *testing.T, fn Orchestrator) *TaskRegistry {
	t.Helper()
	registry := NewTaskRegistry()
	require.NoError(t, registry.AddOrchestratorN("channels", fn))
	return registry
}

func channelStart() []*protos.HistoryEvent {
	return []*protos.HistoryEvent{
		helpers.NewOrchestratorStartedEvent(),
		helpers.NewExecutionStartedEvent("channels", "instance", nil, nil, nil, nil),
	}
}

func TestChannelRendezvousAndBufferedFIFO(t *testing.T) {
	for _, capacity := range []int{0, 1, 3} {
		t.Run(fmt.Sprint(capacity), func(t *testing.T) {
			registry := channelRegistry(t, func(ctx *OrchestrationContext) (any, error) {
				channel := NewChannel[int](ctx, capacity)
				wg := ctx.NewWaitGroup()
				wg.Add(1)
				ctx.Go(func(ctx *OrchestrationContext) {
					defer wg.Done()
					for _, value := range []int{1, 2, 3} {
						channel.Send(ctx, value)
					}
				})
				results := []int{channel.Receive(ctx), channel.Receive(ctx), channel.Receive(ctx)}
				wg.Wait(ctx)
				assert.Empty(t, channel.senders)
				assert.Empty(t, channel.receivers)
				assert.Empty(t, channel.buffer)
				return results, nil
			})
			for range 10 {
				response := executeOrchestrationTurn(t, registry, "instance", nil, channelStart())
				require.Equal(t, "[1,2,3]", completionResult(t, response))
				require.Len(t, response.Actions, 1)
			}
		})
	}
}

func TestChannelSelectRendezvousCommitsOnlyWinningCases(t *testing.T) {
	registry := channelRegistry(t, func(ctx *OrchestrationContext) (any, error) {
		first := NewChannel[int](ctx, 0)
		second := NewChannel[int](ctx, 0)
		sent := ""
		received := 0
		wg := ctx.NewWaitGroup()
		wg.Add(1)
		ctx.Go(func(ctx *OrchestrationContext) {
			defer wg.Done()
			ctx.Select(
				OnSend(first, 1, func() { sent = "first" }),
				OnSend(second, 2, func() { sent = "second" }),
			)
		})
		ctx.Select(
			OnReceive(second, func(value int) { received = value }),
			OnReceive(first, func(value int) { received = value }),
		)
		wg.Wait(ctx)
		assert.Empty(t, first.senders)
		assert.Empty(t, first.receivers)
		assert.Empty(t, second.senders)
		assert.Empty(t, second.receivers)
		return []any{sent, received}, nil
	})
	response := executeOrchestrationTurn(t, registry, "instance", nil, channelStart())
	require.Equal(t, `["first",1]`, completionResult(t, response))
}

func TestChannelSelectDoesNotStealPeerFromEarlierReadyTask(t *testing.T) {
	registry := channelRegistry(t, func(ctx *OrchestrationContext) (any, error) {
		channel := NewChannel[int](ctx, 0)
		ready := newTaskInScope(ctx, ctx.scope)
		ready.complete(nil)
		sent := false
		wg := ctx.NewWaitGroup()
		wg.Add(1)
		ctx.Go(func(ctx *OrchestrationContext) {
			defer wg.Done()
			ctx.Select(
				OnSend(channel, 42, func() { sent = true }),
				OnTask(ready, nil),
			)
		})
		ctx.Select(OnReceive(channel, func(int) { panic("losing send executed") }), OnTask(ready, nil))
		wg.Wait(ctx)
		return sent, nil
	})
	response := executeOrchestrationTurn(t, registry, "instance", nil, channelStart())
	require.Equal(t, "false", completionResult(t, response))
}

func TestChannelRendezvousRespectsWaitingPeersOtherReadyCase(t *testing.T) {
	registry := channelRegistry(t, func(ctx *OrchestrationContext) (any, error) {
		channel := NewChannel[int](ctx, 0)
		ready := newTaskInScope(ctx, ctx.scope)
		sent := false
		value := 0
		wg := ctx.NewWaitGroup()
		wg.Add(3)
		ctx.Go(func(ctx *OrchestrationContext) {
			defer wg.Done()
			ctx.Select(OnSend(channel, 1, func() { sent = true }), OnTask(ready, nil))
		})
		ctx.Go(func(ctx *OrchestrationContext) {
			defer wg.Done()
			ready.complete(nil)
			value = channel.Receive(ctx)
		})
		ctx.Go(func(ctx *OrchestrationContext) {
			defer wg.Done()
			channel.Send(ctx, 2)
		})
		wg.Wait(ctx)
		return []any{sent, value}, nil
	})
	response := executeOrchestrationTurn(t, registry, "instance", nil, channelStart())
	require.Equal(t, "[false,2]", completionResult(t, response))
}

func TestChannelCompetingSelectsMakeProgressBeforeDurableWait(t *testing.T) {
	selected := 0
	registry := channelRegistry(t, func(ctx *OrchestrationContext) (any, error) {
		selected = 0
		a, b, c := NewChannel[int](ctx, 0), NewChannel[int](ctx, 0), NewChannel[int](ctx, 0)
		timer := ctx.CreateTimer(time.Hour)
		wg := ctx.NewWaitGroup()
		wg.Add(3)
		for _, pair := range [][2]*Channel[int]{{a, c}, {b, a}, {c, b}} {
			ctx.Go(func(ctx *OrchestrationContext) {
				defer wg.Done()
				ctx.Select(
					OnSend(pair[0], 1, func() { selected++ }),
					OnReceive(pair[1], func(int) { selected++ }),
					OnTask(timer, func(Task) { selected++ }),
				)
			})
		}
		wg.Wait(ctx)
		return selected, nil
	})
	start := channelStart()
	first := executeOrchestrationTurn(t, registry, "instance", nil, start)
	require.Equal(t, 2, selected, "ready rendezvous must not stall behind the timer")
	require.Len(t, first.Actions, 1)
	require.NotNil(t, first.Actions[0].GetCreateTimer())
	history := append(slices.Clone(start), helpers.NewTimerCreatedEvent(0, first.Actions[0].GetCreateTimer().FireAt))
	second := executeOrchestrationTurn(t, registry, "instance", history, []*protos.HistoryEvent{
		helpers.NewOrchestratorStartedEvent(),
		helpers.NewTimerFiredEvent(0, first.Actions[0].GetCreateTimer().FireAt, nil),
	})
	require.Equal(t, "3", completionResult(t, second))
}

func TestChannelBufferedBlockedSendersKeepQueueOrder(t *testing.T) {
	registry := channelRegistry(t, func(ctx *OrchestrationContext) (any, error) {
		channel := NewChannel[int](ctx, 1)
		gate := NewChannel[struct{}](ctx, 0)
		drained := NewChannel[struct{}](ctx, 1)
		channel.Send(ctx, 0)
		wg := ctx.NewWaitGroup()
		wg.Add(3)
		received := []int{}
		ctx.Go(func(ctx *OrchestrationContext) {
			defer wg.Done()
			gate.Receive(ctx)
			value := channel.Receive(ctx)
			if value != 0 {
				panic("unexpected initial buffer value")
			}
			drained.Send(ctx, struct{}{})
			channel.Send(ctx, 2)
		})
		ctx.Go(func(ctx *OrchestrationContext) {
			defer wg.Done()
			channel.Send(ctx, 1)
		})
		ctx.Go(func(ctx *OrchestrationContext) {
			defer wg.Done()
			gate.Send(ctx, struct{}{})
			drained.Receive(ctx)
			received = append(received, channel.Receive(ctx), channel.Receive(ctx))
		})
		wg.Wait(ctx)
		return received, nil
	})
	response := executeOrchestrationTurn(t, registry, "instance", nil, channelStart())
	require.Equal(t, "[1,2]", completionResult(t, response))
}

func TestChannelSelectLosingBufferedCasesHaveNoSideEffects(t *testing.T) {
	registry := channelRegistry(t, func(ctx *OrchestrationContext) (any, error) {
		ready := newTaskInScope(ctx, ctx.scope)
		ready.complete(nil)
		channel := NewChannel[int](ctx, 1)
		channel.Send(ctx, 7)
		ctx.Select(OnReceive(channel, func(int) { panic("losing receive ran") }), OnTask(ready, nil))
		value := channel.Receive(ctx)
		ctx.Select(OnSend(channel, 99, func() { panic("losing send ran") }), OnTask(ready, nil))
		assert.Empty(t, channel.buffer)
		return value, nil
	})
	response := executeOrchestrationTurn(t, registry, "instance", nil, channelStart())
	require.Equal(t, "7", completionResult(t, response))
}

func TestChannelSelectCannotRendezvousWithItself(t *testing.T) {
	registry := channelRegistry(t, func(ctx *OrchestrationContext) (any, error) {
		channel := NewChannel[int](ctx, 0)
		ctx.Select(OnSend(channel, 1, nil), OnReceive(channel, nil))
		return nil, nil
	})
	response := executeOrchestrationTurn(t, registry, "instance", nil, channelStart())
	require.Equal(t, protos.OrchestrationStatus_ORCHESTRATION_STATUS_FAILED,
		completionAction(t, response).OrchestrationStatus)
	require.Contains(t, completionAction(t, response).FailureDetails.ErrorMessage, ErrOrchestrationDeadlock.Error())
}

func TestChannelWaitsYieldDurableActionsAndReplay(t *testing.T) {
	registry := channelRegistry(t, func(ctx *OrchestrationContext) (any, error) {
		channel := NewChannel[int](ctx, 0)
		wg := ctx.NewWaitGroup()
		wg.Add(2)
		for _, name := range []string{"first", "second"} {
			ctx.Go(func(ctx *OrchestrationContext) {
				defer wg.Done()
				var result int
				if err := ctx.CallActivity(name).Await(&result); err != nil {
					panic(err)
				}
				channel.Send(ctx, result)
			})
		}
		result := []int{channel.Receive(ctx), channel.Receive(ctx)}
		wg.Wait(ctx)
		return result, nil
	})
	start := channelStart()
	first := executeOrchestrationTurn(t, registry, "instance", nil, start)
	require.Len(t, first.Actions, 2)
	require.Equal(t, "first", first.Actions[0].GetScheduleTask().Name)
	require.Equal(t, "second", first.Actions[1].GetScheduleTask().Name)

	history := append(slices.Clone(start),
		helpers.NewTaskScheduledEvent(0, "first", nil, nil, nil),
		helpers.NewTaskScheduledEvent(1, "second", nil, nil, nil),
	)
	secondEvents := []*protos.HistoryEvent{
		helpers.NewOrchestratorStartedEvent(),
		helpers.NewTaskCompletedEvent(1, wrapperspb.String("20")),
	}
	second := executeOrchestrationTurn(t, registry, "instance", history, secondEvents)
	require.Empty(t, second.Actions)
	history = append(history, secondEvents...)
	lastEvents := []*protos.HistoryEvent{
		helpers.NewOrchestratorStartedEvent(),
		helpers.NewTaskCompletedEvent(0, wrapperspb.String("10")),
	}
	last := executeOrchestrationTurn(t, registry, "instance", history, lastEvents)
	require.Equal(t, "[20,10]", completionResult(t, last))
	for range 20 {
		replay := executeOrchestrationTurn(t, registry, "instance", append(slices.Clone(history), lastEvents...), nil)
		require.True(t, proto.Equal(last, replay))
	}
}

func TestChannelWaitCancellationAndReuse(t *testing.T) {
	for _, capacity := range []int{0, 1} {
		for _, sending := range []bool{false, true} {
			t.Run(fmt.Sprintf("capacity=%d/send=%t", capacity, sending), func(t *testing.T) {
				registry := channelRegistry(t, func(ctx *OrchestrationContext) (any, error) {
					child, cancel := ctx.WithCancel()
					channel := NewChannel[int](ctx, capacity)
					if sending && capacity > 0 {
						channel.Send(ctx, 1)
					}
					canceled := false
					wg := ctx.NewWaitGroup()
					wg.Add(1)
					ctx.Go(func(*OrchestrationContext) {
						defer wg.Done()
						var err error
						if sending {
							err = channel.SendErr(child, 2)
						} else {
							_, err = channel.ReceiveErr(child)
						}
						canceled = errors.Is(err, ErrTaskCanceled)
					})
					// Yield until the wrapper has registered its channel wait.
					ctx.Go(func(*OrchestrationContext) { cancel() })
					wg.Wait(ctx)
					assert.Empty(t, channel.senders)
					assert.Empty(t, channel.receivers)
					assert.True(t, child.IsCanceled())
					assert.False(t, ctx.IsCanceled())
					if sending && capacity > 0 {
						assert.Equal(t, 1, channel.Receive(ctx))
					}
					ctx.Go(func(ctx *OrchestrationContext) { channel.Send(ctx, 3) })
					return []any{canceled, channel.Receive(ctx)}, nil
				})
				response := executeOrchestrationTurn(t, registry, "instance", nil, channelStart())
				require.Equal(t, "[true,3]", completionResult(t, response))
			})
		}
	}
}

func TestChannelCommittedRendezvousSurvivesCancellation(t *testing.T) {
	registry := channelRegistry(t, func(ctx *OrchestrationContext) (any, error) {
		child, cancel := ctx.WithCancel()
		channel := NewChannel[int](ctx, 0)
		var received int
		var receiveErr error
		wg := ctx.NewWaitGroup()
		wg.Add(1)
		ctx.Go(func(*OrchestrationContext) {
			defer wg.Done()
			received, receiveErr = channel.ReceiveErr(child)
		})
		ctx.Go(func(ctx *OrchestrationContext) {
			channel.Send(ctx, 42)
			cancel()
		})
		wg.Wait(ctx)
		return received, receiveErr
	})
	response := executeOrchestrationTurn(t, registry, "instance", nil, channelStart())
	require.Equal(t, "42", completionResult(t, response))
}

func TestChannelCycleFailsWithoutBlockingWorker(t *testing.T) {
	registry := channelRegistry(t, func(ctx *OrchestrationContext) (any, error) {
		a, b := NewChannel[int](ctx, 0), NewChannel[int](ctx, 0)
		wg := ctx.NewWaitGroup()
		wg.Add(2)
		ctx.Go(func(ctx *OrchestrationContext) {
			defer wg.Done()
			a.Send(ctx, b.Receive(ctx))
		})
		ctx.Go(func(ctx *OrchestrationContext) {
			defer wg.Done()
			b.Send(ctx, a.Receive(ctx))
		})
		wg.Wait(ctx)
		return nil, nil
	})
	response := executeOrchestrationTurn(t, registry, "instance", nil, channelStart())
	completed := completionAction(t, response)
	require.Equal(t, protos.OrchestrationStatus_ORCHESTRATION_STATUS_FAILED, completed.OrchestrationStatus)
	require.Contains(t, completed.FailureDetails.ErrorMessage, ErrOrchestrationDeadlock.Error())
}

func TestChannelWaitWithExternalEventUnloadsAndResumes(t *testing.T) {
	var channel *Channel[int]
	registry := channelRegistry(t, func(ctx *OrchestrationContext) (any, error) {
		channel = NewChannel[int](ctx, 0)
		ctx.Go(func(ctx *OrchestrationContext) {
			var value int
			if err := ctx.WaitForSingleEvent("value", -1).Await(&value); err != nil {
				panic(err)
			}
			channel.Send(ctx, value)
		})
		return channel.Receive(ctx), nil
	})
	first := executeOrchestrationTurn(t, registry, "instance", nil, channelStart())
	require.Empty(t, first.Actions)
	require.Empty(t, channel.senders)
	require.Empty(t, channel.receivers)
	second := executeOrchestrationTurn(t, registry, "instance", channelStart(), []*protos.HistoryEvent{
		helpers.NewOrchestratorStartedEvent(),
		helpers.NewEventRaisedEvent("value", wrapperspb.String("9")),
	})
	require.Equal(t, "9", completionResult(t, second))
}

func TestChannelSelectWithStopEventAndTimer(t *testing.T) {
	registry := channelRegistry(t, func(ctx *OrchestrationContext) (any, error) {
		channel := NewChannel[int](ctx, 0)
		stop := NewEventChannel[string](ctx, "stop")
		timer := ctx.CreateTimer(time.Hour)
		selected := ""
		ctx.Select(
			OnReceive(channel, func(int) { selected = "channel" }),
			OnEvent(stop, func(string) { selected = "stop" }),
			OnTask(timer, func(Task) { selected = "timer" }),
		)
		return selected, nil
	})
	first := executeOrchestrationTurn(t, registry, "instance", nil, channelStart())
	require.Len(t, first.Actions, 1)
	require.NotNil(t, first.Actions[0].GetCreateTimer())
	second := executeOrchestrationTurn(t, registry, "instance", channelStart(), []*protos.HistoryEvent{
		helpers.NewOrchestratorStartedEvent(),
		helpers.NewEventRaisedEvent("stop", wrapperspb.String(`"requested"`)),
	})
	require.Equal(t, `"stop"`, completionResult(t, second))
}

func TestIsCanceledObservesAppliedNotRequestedCancellation(t *testing.T) {
	registry := channelRegistry(t, func(ctx *OrchestrationContext) (any, error) {
		child, cancel := ctx.WithCancel()
		cancel()
		assert.False(t, child.IsCanceled())
		assert.ErrorIs(t, child.WaitForSingleEvent("never", -1).Await(nil), ErrTaskCanceled)
		return child.IsCanceled(), nil
	})
	response := executeOrchestrationTurn(t, registry, "instance", nil, channelStart())
	require.Equal(t, "true", completionResult(t, response))
}

func TestChannelCancellationConvenienceFormsUnwindChildBranches(t *testing.T) {
	registry := channelRegistry(t, func(ctx *OrchestrationContext) (any, error) {
		child, cancel := ctx.WithCancel()
		channel := NewChannel[int](ctx, 0)
		other := NewChannel[int](ctx, 0)
		finished := 0
		wg := ctx.NewWaitGroup()
		wg.Add(2)
		child.Go(func(ctx *OrchestrationContext) {
			defer wg.Done()
			defer func() { finished++ }()
			channel.Send(ctx, 1)
		})
		child.Go(func(ctx *OrchestrationContext) {
			defer wg.Done()
			defer func() { finished++ }()
			other.Receive(ctx)
		})
		ctx.Go(func(*OrchestrationContext) { cancel() })
		wg.Wait(ctx)
		return finished, nil
	})
	response := executeOrchestrationTurn(t, registry, "instance", nil, channelStart())
	require.Equal(t, "2", completionResult(t, response))
}

func TestChannelRejectsForeignOwnersBeforeRegistering(t *testing.T) {
	registry := channelRegistry(t, func(ctx *OrchestrationContext) (any, error) {
		foreign := newTestOrchestrationContext(NewTaskRegistry(), "instance", nil, nil)
		channel := &Channel[int]{ctx: foreign, capacity: 1}
		var recovered any
		func() {
			defer func() { recovered = recover() }()
			ctx.Select(OnSend(channel, 1, nil), OnReceive(channel, nil))
		}()
		assert.Empty(t, channel.senders)
		assert.Empty(t, channel.receivers)
		assert.Empty(t, channel.buffer)
		return recovered, nil
	})
	response := executeOrchestrationTurn(t, registry, "instance", nil, channelStart())
	require.Equal(t, `"Select case belongs to a different orchestration"`, completionResult(t, response))
}

func TestChannelSelectCasesCanBeReusedAcrossBranches(t *testing.T) {
	registry := channelRegistry(t, func(ctx *OrchestrationContext) (any, error) {
		channel := NewChannel[int](ctx, 0)
		sum := 0
		receive := OnReceive(channel, func(value int) { sum += value })
		wg := ctx.NewWaitGroup()
		wg.Add(2)
		for range 2 {
			ctx.Go(func(ctx *OrchestrationContext) {
				defer wg.Done()
				ctx.Select(receive)
			})
		}
		channel.Send(ctx, 1)
		channel.Send(ctx, 2)
		wg.Wait(ctx)
		assert.Empty(t, receive.(*channelSelectCase[int]).operations)
		return sum, nil
	})
	response := executeOrchestrationTurn(t, registry, "instance", nil, channelStart())
	require.Equal(t, "3", completionResult(t, response))
}

func TestChannelCancellationDoesNotModifyBuffer(t *testing.T) {
	registry := channelRegistry(t, func(ctx *OrchestrationContext) (any, error) {
		child, cancel := ctx.WithCancel()
		channel := NewChannel[int](ctx, 1)
		channel.Send(ctx, 1)
		cancel()
		_ = child.WaitForSingleEvent("never", -1).Await(nil)
		assert.ErrorIs(t, channel.SendErr(child, 2), ErrTaskCanceled)
		_, err := channel.ReceiveErr(child)
		assert.ErrorIs(t, err, ErrTaskCanceled)
		return channel.Receive(ctx), nil
	})
	response := executeOrchestrationTurn(t, registry, "instance", nil, channelStart())
	require.Equal(t, "1", completionResult(t, response))
}

func TestChannelSelectSnapshotsCasesBeforeYielding(t *testing.T) {
	registry := channelRegistry(t, func(ctx *OrchestrationContext) (any, error) {
		channel := NewChannel[int](ctx, 0)
		cases := []SelectCase{OnReceive(channel, nil)}
		wg := ctx.NewWaitGroup()
		wg.Add(1)
		ctx.Go(func(ctx *OrchestrationContext) {
			defer wg.Done()
			ctx.Select(cases...)
		})
		ctx.Go(func(ctx *OrchestrationContext) {
			cases[0] = OnTask(newTaskInScope(ctx, ctx.scope), nil)
			channel.Send(ctx, 1)
		})
		wg.Wait(ctx)
		assert.Empty(t, channel.senders)
		assert.Empty(t, channel.receivers)
		return "done", nil
	})
	response := executeOrchestrationTurn(t, registry, "instance", nil, channelStart())
	require.Equal(t, `"done"`, completionResult(t, response))
}
