package task

import (
	"context"
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
				assert.Zero(t, channel.senders.Len())
				assert.Zero(t, channel.receivers.Len())
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
		assert.Zero(t, first.senders.Len())
		assert.Zero(t, first.receivers.Len())
		assert.Zero(t, second.senders.Len())
		assert.Zero(t, second.receivers.Len())
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
					assert.Zero(t, channel.senders.Len())
					assert.Zero(t, channel.receivers.Len())
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
	require.Zero(t, channel.senders.Len())
	require.Zero(t, channel.receivers.Len())
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
		assert.Zero(t, channel.senders.Len())
		assert.Zero(t, channel.receivers.Len())
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
		assert.Zero(t, channel.senders.Len())
		assert.Zero(t, channel.receivers.Len())
		return "done", nil
	})
	response := executeOrchestrationTurn(t, registry, "instance", nil, channelStart())
	require.Equal(t, `"done"`, completionResult(t, response))
}

type countedChannelCase struct {
	SelectCase
	polls *int
}

func (c *countedChannelCase) ready(s *selection) (bool, uint64) {
	(*c.polls)++
	return c.SelectCase.ready(s)
}

func TestChannelSameSideWaitersDoNotResumeBeforeDurableDispatch(t *testing.T) {
	const count = 128
	for _, capacity := range []int{0, 1} {
		for _, send := range []bool{false, true} {
			t.Run(fmt.Sprintf("capacity=%d/send=%t", capacity, send), func(t *testing.T) {
				polls := 0
				registry := channelRegistry(t, func(ctx *OrchestrationContext) (any, error) {
					channel := NewChannel[int](ctx, capacity)
					if send && capacity > 0 {
						channel.Send(ctx, 0)
					}
					for range count {
						ctx.Go(func(branch *OrchestrationContext) {
							candidate := OnReceive(channel, nil)
							if send {
								candidate = OnSend(channel, 1, nil)
							}
							branch.Select(&countedChannelCase{SelectCase: candidate, polls: &polls})
						})
					}
					return nil, ctx.CallActivity("pending").Await(nil)
				})
				response := executeOrchestrationTurn(t, registry, "instance", nil, channelStart())
				require.Len(t, response.Actions, 1)
				require.Equal(t, "pending", response.Actions[0].GetScheduleTask().Name)
				require.Equal(t, count, polls, "each blocked branch should run once, not resume for other blocked waiters")
			})
		}
	}
}

func TestChannelBufferedDrainClearsValuesWithoutMovingTail(t *testing.T) {
	registry := channelRegistry(t, func(ctx *OrchestrationContext) (any, error) {
		channel := NewChannel[*int](ctx, 256)
		for i := range 256 {
			channel.Send(ctx, &i)
		}
		backing := channel.buffer
		for i := range 256 {
			value := channel.Receive(ctx)
			assert.Equal(t, i, *value)
			assert.Nil(t, backing[i].value)
			if i < 255 {
				assert.Equal(t, i+1, *backing[i+1].value, "unconsumed values must not move")
			}
		}
		return len(channel.buffer), nil
	})
	response := executeOrchestrationTurn(t, registry, "instance", nil, channelStart())
	require.Equal(t, "0", completionResult(t, response))
}

func TestChannelMixedSelectionGroupsAlternativesAndResumesFromActivity(t *testing.T) {
	registry := channelRegistry(t, func(ctx *OrchestrationContext) (any, error) {
		channel := NewChannel[int](ctx, 0)
		cases := make([]SelectCase, 0, 257)
		for i := range 128 {
			cases = append(cases,
				OnSend(channel, i, func() { panic("selection sent to itself") }),
				OnReceive(channel, func(int) { panic("selection received from itself") }))
		}
		inspected := false
		ctx.Go(func(*OrchestrationContext) {
			assert.Equal(t, 1, channel.senders.Len())
			assert.Equal(t, 1, channel.receivers.Len())
			assert.Equal(t, 128, channel.senders.Front().Value.(*channelOperationGroup[int]).operations.Len())
			assert.Equal(t, 128, channel.receivers.Front().Value.(*channelOperationGroup[int]).operations.Len())
			inspected = true
		})
		selected := 0
		cases = append(cases, OnTask(ctx.CallActivity("pending"), func(Task) { selected++ }))
		ctx.Select(cases...)
		assert.True(t, inspected)
		assert.Zero(t, channel.senders.Len())
		assert.Zero(t, channel.receivers.Len())
		return selected, nil
	})
	first := executeOrchestrationTurn(t, registry, "instance", nil, channelStart())
	require.Len(t, first.Actions, 1)
	require.Equal(t, "pending", first.Actions[0].GetScheduleTask().GetName())
	history := append(channelStart(), helpers.NewTaskScheduledEvent(0, "pending", nil, nil, nil))
	completion := []*protos.HistoryEvent{
		helpers.NewOrchestratorStartedEvent(),
		helpers.NewTaskCompletedEvent(0, nil),
	}
	response := executeOrchestrationTurn(t, registry, "instance", history, completion)
	require.Equal(t, "1", completionResult(t, response))
	replay := executeOrchestrationTurn(t, registry, "instance", append(history, completion...), nil)
	require.True(t, proto.Equal(response, replay))
}

func BenchmarkChannelMixedDirectionSelect(b *testing.B) {
	for _, count := range []int{128, 512, 2048} {
		b.Run(fmt.Sprint(count), func(b *testing.B) {
			registry := NewTaskRegistry()
			if err := registry.AddOrchestratorN("channels", func(ctx *OrchestrationContext) (any, error) {
				channel := NewChannel[int](ctx, 0)
				cases := make([]SelectCase, 0, 2*count+1)
				for i := range count {
					cases = append(cases, OnSend(channel, i, nil), OnReceive(channel, nil))
				}
				cases = append(cases, OnTask(ctx.CallActivity("pending"), nil))
				ctx.Select(cases...)
				return nil, nil
			}); err != nil {
				b.Fatal(err)
			}
			executor := NewTaskExecutor(registry)
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				result, err := executor.ExecuteOrchestrator(
					context.Background(), "instance", nil, channelStart(), nil)
				if err != nil {
					b.Fatal(err)
				}
				if len(result.Response.Actions) != 1 ||
					result.Response.Actions[0].GetScheduleTask().GetName() != "pending" {
					b.Fatal("mixed selection failed to dispatch durable work")
				}
			}
		})
	}
}

func TestChannelMixedAlternativesSkipOwnGroupAndKeepPeerOrder(t *testing.T) {
	registry := channelRegistry(t, func(ctx *OrchestrationContext) (any, error) {
		channel := NewChannel[int](ctx, 0)
		group := ctx.NewWaitGroup()
		group.Add(2)
		values := []int{}
		for _, offset := range []int{100, 200} {
			ctx.Go(func(branch *OrchestrationContext) {
				defer group.Done()
				cases := make([]SelectCase, 0, 256)
				for i := range 128 {
					cases = append(cases,
						OnSend(channel, offset+i, nil),
						OnReceive(channel, func(int) { panic("branches must not receive their own sends") }))
				}
				branch.Select(cases...)
			})
		}
		// The root queues its receive first, so the first branch must pair
		// with it rather than itself or the later branch.
		values = append(values, channel.Receive(ctx), channel.Receive(ctx))
		group.Wait(ctx)
		assert.Zero(t, channel.senders.Len())
		assert.Zero(t, channel.receivers.Len())
		assert.Empty(t, channel.senders.groups)
		assert.Empty(t, channel.receivers.groups)
		return values, nil
	})
	for _, replay := range []bool{false, true} {
		var oldEvents, newEvents []*protos.HistoryEvent
		if replay {
			oldEvents = channelStart()
		} else {
			newEvents = channelStart()
		}
		response := executeOrchestrationTurn(t, registry, "instance", oldEvents, newEvents)
		require.Equal(t, "[100,200]", completionResult(t, response))
	}
}

func BenchmarkChannelCompetingSelects(b *testing.B) {
	for _, count := range []int{16, 128, 1024} {
		b.Run(fmt.Sprint(count), func(b *testing.B) {
			registry := NewTaskRegistry()
			if err := registry.AddOrchestratorN("channels", func(ctx *OrchestrationContext) (any, error) {
				channel := NewChannel[int](ctx, 0)
				sends := make([]SelectCase, count)
				receives := make([]SelectCase, count)
				for i := range count {
					sends[i] = OnSend(channel, i, nil)
					receives[i] = OnReceive(channel, nil)
				}
				group := ctx.NewWaitGroup()
				group.Add(1)
				ctx.Go(func(branch *OrchestrationContext) {
					defer group.Done()
					branch.Select(sends...)
				})
				ctx.Select(receives...)
				group.Wait(ctx)
				return nil, nil
			}); err != nil {
				b.Fatal(err)
			}
			executor := NewTaskExecutor(registry)
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				result, err := executor.ExecuteOrchestrator(
					context.Background(), "instance", nil, channelStart(), nil)
				if err != nil {
					b.Fatal(err)
				}
				if len(result.Response.Actions) != 1 ||
					result.Response.Actions[0].GetCompleteOrchestration().GetOrchestrationStatus() !=
						protos.OrchestrationStatus_ORCHESTRATION_STATUS_COMPLETED {
					b.Fatal("competing selects did not complete")
				}
			}

		})
	}
}

func TestChannelManyAlternativesCommitOnePairInRegistrationOrder(t *testing.T) {
	registry := channelRegistry(t, func(ctx *OrchestrationContext) (any, error) {
		channel := NewChannel[int](ctx, 0)
		received, sends, receives := -1, 0, 0
		sendCases, receiveCases := make([]SelectCase, 128), make([]SelectCase, 128)
		for i := range sendCases {
			sendCases[i] = OnSend(channel, i, func() { sends++ })
			receiveCases[i] = OnReceive(channel, func(value int) {
				received = value
				receives++
			})
		}
		group := ctx.NewWaitGroup()
		group.Add(1)
		ctx.Go(func(branch *OrchestrationContext) {
			defer group.Done()
			branch.Select(sendCases...)
		})
		ctx.Select(receiveCases...)
		group.Wait(ctx)
		return []int{received, sends, receives, channel.senders.Len(), channel.receivers.Len()}, nil
	})
	for range 10 {
		for _, replay := range []bool{false, true} {
			var oldEvents, newEvents []*protos.HistoryEvent
			if replay {
				oldEvents = channelStart()
			} else {
				newEvents = channelStart()
			}
			response := executeOrchestrationTurn(t, registry, "instance", oldEvents, newEvents)
			require.Equal(t, "[0,1,1,0,0]", completionResult(t, response))
		}
	}
}
