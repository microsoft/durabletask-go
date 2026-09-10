package task

import (
	"errors"
	"testing"
	"time"

	"github.com/microsoft/durabletask-go/api"
	"github.com/microsoft/durabletask-go/internal/helpers"
	"github.com/microsoft/durabletask-go/internal/protos"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

func TestCanceledScopeNewChildDoesNotScheduleWork(t *testing.T) {
	entityID := api.NewEntityID("counter", "canceled")
	tests := []struct {
		name string
		call func(*OrchestrationContext) error
	}{
		{"activity", func(ctx *OrchestrationContext) error { return ctx.CallActivity("unused").Await(nil) }},
		{"sub-orchestration", func(ctx *OrchestrationContext) error { return ctx.CallSubOrchestrator("unused").Await(nil) }},
		{"timer", func(ctx *OrchestrationContext) error { return ctx.CreateTimer(time.Hour).Await(nil) }},
		{"entity call", func(ctx *OrchestrationContext) error { return ctx.CallEntity(entityID, "get").Await(nil) }},
		{"entity signal", func(ctx *OrchestrationContext) error { return ctx.SignalEntity(entityID, "add") }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			registry := NewTaskRegistry()
			require.NoError(t, registry.AddOrchestratorN("late-child", func(ctx *OrchestrationContext) (any, error) {
				child, cancel := ctx.WithCancel()
				cancel()
				if err := child.WaitForSingleEvent("never", -1).Await(nil); !errors.Is(err, ErrTaskCanceled) {
					return nil, errors.New("parent cancellation was not applied")
				}
				grandchild, _ := child.WithCancel()
				return errors.Is(test.call(grandchild), ErrTaskCanceled), nil
			}))
			events := []*protos.HistoryEvent{
				helpers.NewOrchestratorStartedEvent(),
				helpers.NewExecutionStartedEvent("late-child", "instance", nil, nil, nil, nil),
			}
			for _, replay := range []bool{false, true} {
				var oldEvents, newEvents []*protos.HistoryEvent
				if replay {
					oldEvents = events
				} else {
					newEvents = events
				}
				response := executeOrchestrationTurn(t, registry, "instance", oldEvents, newEvents)
				require.Len(t, response.Actions, 1)
				require.NotNil(t, response.Actions[0].GetCompleteOrchestration(), "replay=%t", replay)
				require.Equal(t, "true", completionResult(t, response), "replay=%t", replay)
			}
		})
	}
}

func TestCancellationScopeChildPreservesSchedulerBoundary(t *testing.T) {
	registry := NewTaskRegistry()
	require.NoError(t, registry.AddOrchestratorN("pending-cancel", func(ctx *OrchestrationContext) (any, error) {
		child, cancel := ctx.WithCancel()
		cancel()
		grandchild, _ := child.WithCancel()
		if grandchild.scope.isCanceled() {
			return nil, errors.New("pending cancellation was applied before the scheduler boundary")
		}
		task := grandchild.CreateTimer(time.Hour)
		if err := task.Await(nil); !errors.Is(err, ErrTaskCanceled) {
			return nil, errors.New("descendant did not receive cancellation")
		}
		derivedCount := len(ctx.derived)
		lateChild, _ := grandchild.WithCancel()
		return lateChild.scope.isCanceled() && len(grandchild.scope.children) == 0 && len(ctx.derived) == derivedCount, nil
	}))
	response := executeOrchestrationTurn(t, registry, "instance", nil, []*protos.HistoryEvent{
		helpers.NewOrchestratorStartedEvent(),
		helpers.NewExecutionStartedEvent("pending-cancel", "instance", nil, nil, nil, nil),
	})
	require.Len(t, response.Actions, 2)
	require.NotNil(t, response.Actions[0].GetCreateTimer())
	require.Equal(t, "true", completionResult(t, response))
}

func TestCancelScopeCancelsTaskButKeepsScheduledAction(t *testing.T) {
	registry := NewTaskRegistry()
	if err := registry.AddOrchestratorN("cancel-task", func(ctx *OrchestrationContext) (any, error) {
		child, cancel := ctx.WithCancel()
		timer := child.CreateTimer(time.Hour)
		cancel()
		return errors.Is(timer.Await(nil), ErrTaskCanceled), nil
	}); err != nil {
		t.Fatal(err)
	}

	instanceID := api.InstanceID("cancel-task-instance")
	firstTurn := executeOrchestrationTurn(
		t,
		registry,
		instanceID,
		nil,
		[]*protos.HistoryEvent{
			helpers.NewOrchestratorStartedEvent(),
			helpers.NewExecutionStartedEvent("cancel-task", string(instanceID), nil, nil, nil, nil),
		},
	)
	if len(firstTurn.Actions) != 2 {
		t.Fatalf("first-turn action count = %d, want timer and completion", len(firstTurn.Actions))
	}
	if firstTurn.Actions[0].GetCreateTimer() == nil {
		t.Fatal("cancel removed an already-scheduled timer action")
	}
	if got, want := completionResult(t, firstTurn), "true"; got != want {
		t.Fatalf("result = %s, want %s", got, want)
	}

	oldEvents := []*protos.HistoryEvent{
		helpers.NewOrchestratorStartedEvent(),
		helpers.NewExecutionStartedEvent("cancel-task", string(instanceID), nil, nil, nil, nil),
		helpers.NewTimerCreatedEvent(0, firstTurn.Actions[0].GetCreateTimer().GetFireAt()),
	}
	secondTurn := executeOrchestrationTurn(
		t,
		registry,
		instanceID,
		oldEvents,
		[]*protos.HistoryEvent{
			helpers.NewOrchestratorStartedEvent(),
			helpers.NewTimerFiredEvent(0, firstTurn.Actions[0].GetCreateTimer().GetFireAt(), nil),
		},
	)
	if len(secondTurn.Actions) != 1 || secondTurn.Actions[0].GetCompleteOrchestration() == nil {
		t.Fatalf("late timer completion produced unexpected actions: %v", secondTurn.Actions)
	}
}

func TestCancelScopeCancelsNestedScopes(t *testing.T) {
	registry := NewTaskRegistry()
	if err := registry.AddOrchestratorN("nested-cancel", func(ctx *OrchestrationContext) (any, error) {
		child, cancel := ctx.WithCancel()
		grandchild, _ := child.WithCancel()
		timer := grandchild.CreateTimer(time.Hour)
		cancel()
		return errors.Is(timer.Await(nil), ErrTaskCanceled), nil
	}); err != nil {
		t.Fatal(err)
	}

	instanceID := api.InstanceID("nested-cancel-instance")
	result := executeOrchestrationTurn(
		t,
		registry,
		instanceID,
		nil,
		[]*protos.HistoryEvent{
			helpers.NewOrchestratorStartedEvent(),
			helpers.NewExecutionStartedEvent("nested-cancel", string(instanceID), nil, nil, nil, nil),
		},
	)
	if got, want := completionResult(t, result), "true"; got != want {
		t.Fatalf("result = %s, want %s", got, want)
	}
}

func TestCancelScopeUnblocksChildCoroutine(t *testing.T) {
	registry := NewTaskRegistry()
	if err := registry.AddOrchestratorN("cancel-coroutine", func(ctx *OrchestrationContext) (any, error) {
		child, cancel := ctx.WithCancel()
		wg := ctx.NewWaitGroup()
		wg.Add(2)
		canceled := false

		child.Go(func(child *OrchestrationContext) {
			defer wg.Done()
			canceled = errors.Is(child.CreateTimer(time.Hour).Await(nil), ErrTaskCanceled)
		})
		ctx.Go(func(ctx *OrchestrationContext) {
			defer wg.Done()
			if err := ctx.CreateTimer(time.Second).Await(nil); err != nil {
				panic(err)
			}
			cancel()
		})

		wg.Wait(ctx)
		return canceled, nil
	}); err != nil {
		t.Fatal(err)
	}

	instanceID := api.InstanceID("cancel-coroutine-instance")
	started := helpers.NewOrchestratorStartedEvent()
	executionStarted := helpers.NewExecutionStartedEvent("cancel-coroutine", string(instanceID), nil, nil, nil, nil)
	firstTurn := executeOrchestrationTurn(
		t,
		registry,
		instanceID,
		nil,
		[]*protos.HistoryEvent{started, executionStarted},
	)
	if len(firstTurn.Actions) != 2 {
		t.Fatalf("first-turn action count = %d, want two timers", len(firstTurn.Actions))
	}

	shortTimer := firstTurn.Actions[1].GetCreateTimer()
	result := executeOrchestrationTurn(
		t,
		registry,
		instanceID,
		[]*protos.HistoryEvent{
			started,
			executionStarted,
			helpers.NewTimerCreatedEvent(0, firstTurn.Actions[0].GetCreateTimer().GetFireAt()),
			helpers.NewTimerCreatedEvent(1, shortTimer.GetFireAt()),
		},
		[]*protos.HistoryEvent{
			helpers.NewOrchestratorStartedEvent(),
			helpers.NewTimerFiredEvent(1, shortTimer.GetFireAt(), nil),
		},
	)
	if got, want := completionResult(t, result), "true"; got != want {
		t.Fatalf("result = %s, want %s", got, want)
	}
}

func TestCancelScopeCompletionOrderIsDeterministic(t *testing.T) {
	registry := NewTaskRegistry()
	if err := registry.AddOrchestratorN("cancel-order", func(ctx *OrchestrationContext) (any, error) {
		child, cancel := ctx.WithCancel()
		first := child.CreateTimer(time.Hour)
		second := child.CreateTimer(2 * time.Hour)
		cancel()
		if ctx.WhenAny(first, second) == first {
			return "first", nil
		}
		return "second", nil
	}); err != nil {
		t.Fatal(err)
	}
	instanceID := api.InstanceID("cancel-order-instance")
	events := []*protos.HistoryEvent{
		helpers.NewOrchestratorStartedEvent(),
		helpers.NewExecutionStartedEvent("cancel-order", string(instanceID), nil, nil, nil, nil),
	}
	for i := 0; i < 200; i++ {
		result := executeOrchestrationTurn(t, registry, instanceID, nil, events)
		if got, want := completionResult(t, result), `"first"`; got != want {
			t.Fatalf("iteration %d result = %s, want %s", i, got, want)
		}
	}
}

func TestCanceledChildWaitingOnRootWaitGroupUnwinds(t *testing.T) {
	registry := NewTaskRegistry()
	if err := registry.AddOrchestratorN("cancel-waitgroup", func(ctx *OrchestrationContext) (any, error) {
		child, cancel := ctx.WithCancel()
		blocked := ctx.NewWaitGroup()
		blocked.Add(1)
		completed := ctx.NewWaitGroup()
		completed.Add(2)

		child.Go(func(child *OrchestrationContext) {
			defer completed.Done()
			blocked.Wait(child)
		})
		ctx.Go(func(*OrchestrationContext) {
			defer completed.Done()
			cancel()
		})
		completed.Wait(ctx)
		return true, nil
	}); err != nil {
		t.Fatal(err)
	}
	instanceID := api.InstanceID("cancel-waitgroup-instance")
	result := executeOrchestrationTurn(
		t,
		registry,
		instanceID,
		nil,
		[]*protos.HistoryEvent{
			helpers.NewOrchestratorStartedEvent(),
			helpers.NewExecutionStartedEvent("cancel-waitgroup", string(instanceID), nil, nil, nil, nil),
		},
	)
	if got, want := completionResult(t, result), "true"; got != want {
		t.Fatalf("result = %s, want %s", got, want)
	}
}

func TestCanceledScopeDoesNotConsumeBufferedEvent(t *testing.T) {
	registry := NewTaskRegistry()
	if err := registry.AddOrchestratorN("cancel-buffer", func(ctx *OrchestrationContext) (any, error) {
		child, cancel := ctx.WithCancel()
		completed := ctx.NewWaitGroup()
		completed.Add(2)
		childCanceled := false

		child.Go(func(child *OrchestrationContext) {
			defer completed.Done()
			_ = child.CreateTimer(time.Hour).Await(nil)
			var ignored string
			childCanceled = errors.Is(
				child.WaitForSingleEvent("payload", -1).Await(&ignored),
				ErrTaskCanceled,
			)
		})
		ctx.Go(func(ctx *OrchestrationContext) {
			defer completed.Done()
			if err := ctx.CreateTimer(time.Second).Await(nil); err != nil {
				panic(err)
			}
			cancel()
		})

		completed.Wait(ctx)
		var payload string
		if err := ctx.WaitForSingleEvent("payload", 0).Await(&payload); err != nil {
			return nil, err
		}
		return []any{childCanceled, payload}, nil
	}); err != nil {
		t.Fatal(err)
	}

	instanceID := api.InstanceID("cancel-buffer-instance")
	started := helpers.NewOrchestratorStartedEvent()
	executionStarted := helpers.NewExecutionStartedEvent("cancel-buffer", string(instanceID), nil, nil, nil, nil)
	firstTurn := executeOrchestrationTurn(
		t,
		registry,
		instanceID,
		nil,
		[]*protos.HistoryEvent{started, executionStarted},
	)
	shortTimer := firstTurn.Actions[1].GetCreateTimer()
	result := executeOrchestrationTurn(
		t,
		registry,
		instanceID,
		[]*protos.HistoryEvent{
			started,
			executionStarted,
			helpers.NewTimerCreatedEvent(0, firstTurn.Actions[0].GetCreateTimer().GetFireAt()),
			helpers.NewTimerCreatedEvent(1, shortTimer.GetFireAt()),
		},
		[]*protos.HistoryEvent{
			helpers.NewOrchestratorStartedEvent(),
			helpers.NewEventRaisedEvent("payload", wrapperspb.String(`"value"`)),
			helpers.NewTimerFiredEvent(1, shortTimer.GetFireAt(), nil),
		},
	)
	if got, want := completionResult(t, result), `[true,"value"]`; got != want {
		t.Fatalf("result = %s, want %s", got, want)
	}
}

func TestCanceledSelectRemovesEventSubscription(t *testing.T) {
	var captured *OrchestrationContext
	registry := NewTaskRegistry()
	if err := registry.AddOrchestratorN("cancel-select", func(ctx *OrchestrationContext) (any, error) {
		captured = ctx
		child, cancel := ctx.WithCancel()
		completed := ctx.NewWaitGroup()
		completed.Add(2)
		child.Go(func(child *OrchestrationContext) {
			defer completed.Done()
			child.Select(OnEvent(NewEventChannel[int](child, "event"), nil))
		})
		ctx.Go(func(*OrchestrationContext) {
			defer completed.Done()
			cancel()
		})
		completed.Wait(ctx)
		return nil, nil
	}); err != nil {
		t.Fatal(err)
	}
	instanceID := api.InstanceID("cancel-select-instance")
	executeOrchestrationTurn(
		t,
		registry,
		instanceID,
		nil,
		[]*protos.HistoryEvent{
			helpers.NewOrchestratorStartedEvent(),
			helpers.NewExecutionStartedEvent("cancel-select", string(instanceID), nil, nil, nil, nil),
		},
	)
	if len(captured.eventWaiters) != 0 {
		t.Fatalf("event subscriptions remain after cancellation: %v", captured.eventWaiters)
	}
}

func TestCanceledPendingEventWaiterDoesNotConsumeEvent(t *testing.T) {
	registry := NewTaskRegistry()
	if err := registry.AddOrchestratorN("cancel-pending-event", func(ctx *OrchestrationContext) (any, error) {
		child, cancel := ctx.WithCancel()
		completed := ctx.NewWaitGroup()
		completed.Add(2)

		child.Go(func(child *OrchestrationContext) {
			defer completed.Done()
			_ = child.WaitForSingleEvent("payload", -1).Await(nil)
		})
		ctx.Go(func(ctx *OrchestrationContext) {
			defer completed.Done()
			if err := ctx.CreateTimer(time.Second).Await(nil); err != nil {
				panic(err)
			}
			cancel()
		})

		completed.Wait(ctx)
		var payload string
		if err := ctx.WaitForSingleEvent("payload", -1).Await(&payload); err != nil {
			return nil, err
		}
		return payload, nil
	}); err != nil {
		t.Fatal(err)
	}

	instanceID := api.InstanceID("cancel-pending-event-instance")
	started := helpers.NewOrchestratorStartedEvent()
	executionStarted := helpers.NewExecutionStartedEvent(
		"cancel-pending-event",
		string(instanceID),
		nil,
		nil,
		nil,
		nil,
	)
	firstTurn := executeOrchestrationTurn(
		t,
		registry,
		instanceID,
		nil,
		[]*protos.HistoryEvent{started, executionStarted},
	)
	timer := firstTurn.Actions[0].GetCreateTimer()
	result := executeOrchestrationTurn(
		t,
		registry,
		instanceID,
		[]*protos.HistoryEvent{
			started,
			executionStarted,
			helpers.NewTimerCreatedEvent(0, timer.GetFireAt()),
		},
		[]*protos.HistoryEvent{
			helpers.NewOrchestratorStartedEvent(),
			helpers.NewTimerFiredEvent(0, timer.GetFireAt(), nil),
			helpers.NewEventRaisedEvent("payload", wrapperspb.String(`"value"`)),
		},
	)
	if got, want := completionResult(t, result), `"value"`; got != want {
		t.Fatalf("result = %s, want %s", got, want)
	}
}

func TestAlreadyCanceledSelectDoesNotWaitOrConsume(t *testing.T) {
	for _, scope := range []string{"current", "supplied", "both"} {
		for _, buffered := range []bool{false, true} {
			t.Run(scope+map[bool]string{false: "/empty", true: "/buffered"}[buffered], func(t *testing.T) {
				var root, child *OrchestrationContext
				registry := NewTaskRegistry()
				require.NoError(t, registry.AddOrchestratorN("canceled-select", func(ctx *OrchestrationContext) (any, error) {
					root = ctx
					channel := NewEventChannel[int](ctx, "payload")
					if err := ctx.WaitForSingleEvent("ready", -1).Await(nil); err != nil {
						return nil, err
					}
					var cancel func()
					child, cancel = ctx.WithCancel()
					runner, selector := child, child
					switch scope {
					case "current":
						selector = ctx
					case "supplied":
						runner = ctx
					}
					done := ctx.NewWaitGroup()
					done.Add(1)
					canceled, invoked := false, false
					runner.Go(func(*OrchestrationContext) {
						defer done.Done()
						defer func() { canceled = isTaskCanceled(recover()) }()
						// Start the callback before cancellation; an unstarted
						// callback in a canceled scope must not run at all.
						cancel()
						if err := child.WaitForSingleEvent("cancel-boundary", -1).Await(nil); !errors.Is(err, ErrTaskCanceled) {
							panic("cancellation was not applied at the scheduler boundary")
						}
						selector.Select(OnEvent(channel, func(int) { invoked = true }))
					})
					done.Wait(ctx)
					value, received, err := channel.TryReceiveErr()
					_, again, nextErr := channel.TryReceiveErr()
					return canceled && !invoked && received == buffered &&
						(!buffered || value == 42) && err == nil && !again && nextErr == nil, nil
				}))
				events := []*protos.HistoryEvent{
					helpers.NewOrchestratorStartedEvent(),
					helpers.NewExecutionStartedEvent("canceled-select", "instance", nil, nil, nil, nil),
				}
				if buffered {
					events = append(events, helpers.NewEventRaisedEvent("payload", wrapperspb.String("42")))
				}
				events = append(events, helpers.NewEventRaisedEvent("ready", nil))
				for _, replay := range []bool{false, true} {
					var oldEvents, newEvents []*protos.HistoryEvent
					if replay {
						oldEvents = events
					} else {
						newEvents = events
					}
					response := executeOrchestrationTurn(t, registry, "instance", oldEvents, newEvents)
					require.Equal(t, "true", completionResult(t, response), "replay=%t", replay)
					require.Empty(t, root.eventWaiters)
					require.Empty(t, root.scope.waiters)
					require.Empty(t, child.scope.waiters)
				}
			})
		}
	}
}

func TestSelectPreservesPendingCancellationAndRootTaskObservation(t *testing.T) {
	registry := NewTaskRegistry()
	require.NoError(t, registry.AddOrchestratorN("select-cancellation-boundary", func(ctx *OrchestrationContext) (any, error) {
		channel := NewEventChannel[int](ctx, "payload")
		if err := ctx.WaitForSingleEvent("ready", -1).Await(nil); err != nil {
			return nil, err
		}
		child, cancel := ctx.WithCancel()
		canceledTask := child.WaitForSingleEvent("never", -1)
		cancel()
		value := 0
		child.Select(OnEvent(channel, func(received int) { value = received }))
		if err := canceledTask.Await(nil); !errors.Is(err, ErrTaskCanceled) {
			return nil, errors.New("cancellation was not applied")
		}
		observed := false
		ctx.Select(OnTask(canceledTask, func(task Task) {
			observed = errors.Is(task.Await(nil), ErrTaskCanceled)
		}))
		return value == 42 && observed, nil
	}))
	events := []*protos.HistoryEvent{
		helpers.NewOrchestratorStartedEvent(),
		helpers.NewExecutionStartedEvent("select-cancellation-boundary", "instance", nil, nil, nil, nil),
		helpers.NewEventRaisedEvent("payload", wrapperspb.String("42")),
		helpers.NewEventRaisedEvent("ready", nil),
	}
	require.Equal(t, "true", completionResult(t, executeOrchestrationTurn(t, registry, "instance", nil, events)))
	require.Equal(t, "true", completionResult(t, executeOrchestrationTurn(t, registry, "instance", events, nil)))
}

func TestCanceledEventChannelReceivePreservesBufferedEvent(t *testing.T) {
	for _, method := range []string{"Receive", "ReceiveErr"} {
		for _, payload := range []string{"42", `"not an integer"`} {
			t.Run(method+"/"+payload, func(t *testing.T) {
				registry := NewTaskRegistry()
				require.NoError(t, registry.AddOrchestratorN("canceled-receive", func(ctx *OrchestrationContext) (any, error) {
					channel := NewEventChannel[int](ctx, "payload")
					if err := ctx.WaitForSingleEvent("ready", -1).Await(nil); err != nil {
						return nil, err
					}
					child, cancel := ctx.WithCancel()
					cancel()
					if err := child.WaitForSingleEvent("never", -1).Await(nil); !errors.Is(err, ErrTaskCanceled) {
						return nil, errors.New("cancellation was not applied")
					}
					canceled := false
					if method == "ReceiveErr" {
						value, err := channel.ReceiveErr(child)
						canceled = value == 0 && errors.Is(err, ErrTaskCanceled)
					} else {
						func() {
							defer func() { canceled = isTaskCanceled(recover()) }()
							channel.Receive(child)
						}()
					}
					value, received, err := channel.TryReceiveErr()
					preserved := received && ((payload == "42" && value == 42 && err == nil) ||
						(payload != "42" && err != nil))
					_, again, nextErr := channel.TryReceiveErr()
					return canceled && preserved && !again && nextErr == nil, nil
				}))
				events := []*protos.HistoryEvent{
					helpers.NewOrchestratorStartedEvent(),
					helpers.NewExecutionStartedEvent("canceled-receive", "instance", nil, nil, nil, nil),
					helpers.NewEventRaisedEvent("payload", wrapperspb.String(payload)),
					helpers.NewEventRaisedEvent("ready", nil),
				}
				require.Equal(t, "true", completionResult(t, executeOrchestrationTurn(t, registry, "instance", nil, events)))
				require.Equal(t, "true", completionResult(t, executeOrchestrationTurn(t, registry, "instance", events, nil)))
			})
		}
	}
}
