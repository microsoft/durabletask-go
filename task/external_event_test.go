package task

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/microsoft/durabletask-go/api"
	"github.com/microsoft/durabletask-go/internal/helpers"
	"github.com/microsoft/durabletask-go/internal/protos"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

func TestExternalEventNewestLiveWaiterReceivesNextEvent(t *testing.T) {
	registry := NewTaskRegistry()
	if err := registry.AddOrchestratorN("event-stack", func(ctx *OrchestrationContext) (any, error) {
		firstWaiter := ctx.WaitForSingleEvent("signal", -1)
		secondWaiter := ctx.WaitForSingleEvent("SIGNAL", -1)
		var first, second string
		if err := secondWaiter.Await(&second); err != nil {
			return nil, err
		}
		if err := firstWaiter.Await(&first); err != nil {
			return nil, err
		}
		return []string{first, second}, nil
	}); err != nil {
		t.Fatal(err)
	}

	instanceID := api.InstanceID("event-stack-instance")
	result := executeOrchestrationTurn(
		t,
		registry,
		instanceID,
		[]*protos.HistoryEvent{
			helpers.NewOrchestratorStartedEvent(),
			helpers.NewExecutionStartedEvent("event-stack", string(instanceID), nil, nil, nil, nil),
		},
		[]*protos.HistoryEvent{
			helpers.NewOrchestratorStartedEvent(),
			helpers.NewEventRaisedEvent("signal", wrapperspb.String(`"event-1"`)),
			helpers.NewEventRaisedEvent("signal", wrapperspb.String(`"event-2"`)),
		},
	)
	if got, want := completionResult(t, result), `["event-2","event-1"]`; got != want {
		t.Fatalf("result = %s, want %s", got, want)
	}
}

func TestExternalEventMultipleLiveWaitersReceiveEventsInReverseAssignmentOrder(t *testing.T) {
	registry := NewTaskRegistry()
	if err := registry.AddOrchestratorN("event-stack-many", func(ctx *OrchestrationContext) (any, error) {
		waiters := []Task{
			ctx.WaitForSingleEvent("signal", -1),
			ctx.WaitForSingleEvent("signal", -1),
			ctx.WaitForSingleEvent("signal", -1),
		}
		results := make([]string, len(waiters))
		for i := len(waiters) - 1; i >= 0; i-- {
			if err := waiters[i].Await(&results[i]); err != nil {
				return nil, err
			}
		}
		return results, nil
	}); err != nil {
		t.Fatal(err)
	}

	instanceID := api.InstanceID("event-stack-many-instance")
	result := executeOrchestrationTurn(
		t,
		registry,
		instanceID,
		[]*protos.HistoryEvent{
			helpers.NewOrchestratorStartedEvent(),
			helpers.NewExecutionStartedEvent("event-stack-many", string(instanceID), nil, nil, nil, nil),
		},
		[]*protos.HistoryEvent{
			helpers.NewOrchestratorStartedEvent(),
			helpers.NewEventRaisedEvent("signal", wrapperspb.String(`"event-1"`)),
			helpers.NewEventRaisedEvent("signal", wrapperspb.String(`"event-2"`)),
			helpers.NewEventRaisedEvent("signal", wrapperspb.String(`"event-3"`)),
		},
	)
	if got, want := completionResult(t, result), `["event-3","event-2","event-1"]`; got != want {
		t.Fatalf("result = %s, want %s", got, want)
	}
}

func TestCanceledExternalEventWaiterDoesNotPoisonDelivery(t *testing.T) {
	var captured *OrchestrationContext
	registry := NewTaskRegistry()
	if err := registry.AddOrchestratorN("event-stack-cancel", func(ctx *OrchestrationContext) (any, error) {
		captured = ctx
		active := ctx.WaitForSingleEvent("signal", -1)
		child, cancel := ctx.WithCancel()
		canceled := child.WaitForSingleEvent("signal", -1)
		cancel()
		if err := canceled.Await(nil); err == nil {
			return nil, errors.New("canceled waiter returned no error")
		} else if !errors.Is(err, ErrTaskCanceled) {
			return nil, fmt.Errorf("canceled waiter returned unexpected error: %w", err)
		}
		var value string
		if err := active.Await(&value); err != nil {
			return nil, err
		}
		return value, nil
	}); err != nil {
		t.Fatal(err)
	}

	instanceID := api.InstanceID("event-stack-cancel-instance")
	started := helpers.NewOrchestratorStartedEvent()
	executionStarted := helpers.NewExecutionStartedEvent(
		"event-stack-cancel",
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
	if len(firstTurn.Actions) != 0 {
		t.Fatalf("first turn actions = %v, want none", firstTurn.Actions)
	}
	pending := captured.pendingExternalEventTasks["SIGNAL"]
	if pending == nil || pending.Len() != 1 {
		t.Fatalf("live pending event waiters = %v, want 1", pending)
	}

	result := executeOrchestrationTurn(
		t,
		registry,
		instanceID,
		[]*protos.HistoryEvent{started, executionStarted},
		[]*protos.HistoryEvent{
			helpers.NewOrchestratorStartedEvent(),
			helpers.NewEventRaisedEvent("signal", wrapperspb.String(`"delivered"`)),
		},
	)
	if got, want := completionResult(t, result), `"delivered"`; got != want {
		t.Fatalf("result = %s, want %s", got, want)
	}
}

func TestExternalEventsBufferedBeforeWaitersRemainFIFO(t *testing.T) {
	registry := NewTaskRegistry()
	if err := registry.AddOrchestratorN("event-buffer", func(ctx *OrchestrationContext) (any, error) {
		if err := ctx.CreateTimer(time.Minute).Await(nil); err != nil {
			return nil, err
		}
		var first, second string
		if err := ctx.WaitForSingleEvent("signal", -1).Await(&first); err != nil {
			return nil, err
		}
		if err := ctx.WaitForSingleEvent("signal", -1).Await(&second); err != nil {
			return nil, err
		}
		return []string{first, second}, nil
	}); err != nil {
		t.Fatal(err)
	}

	instanceID := api.InstanceID("event-buffer-instance")
	started := helpers.NewOrchestratorStartedEvent()
	executionStarted := helpers.NewExecutionStartedEvent("event-buffer", string(instanceID), nil, nil, nil, nil)
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
			helpers.NewEventRaisedEvent("signal", wrapperspb.String(`"buffered-1"`)),
			helpers.NewEventRaisedEvent("signal", wrapperspb.String(`"buffered-2"`)),
			helpers.NewTimerFiredEvent(0, timer.GetFireAt(), nil),
		},
	)
	if got, want := completionResult(t, result), `["buffered-1","buffered-2"]`; got != want {
		t.Fatalf("result = %s, want %s", got, want)
	}
}

func TestTypedEventChannelLiveReceiversUseLIFO(t *testing.T) {
	registry := NewTaskRegistry()
	if err := registry.AddOrchestratorN("typed-event-stack", func(ctx *OrchestrationContext) (any, error) {
		channel := NewEventChannel[int](ctx, "value")
		results := make([]int, 2)
		completed := ctx.NewWaitGroup()
		completed.Add(len(results))
		for i := range results {
			index := i
			ctx.Go(func(ctx *OrchestrationContext) {
				defer completed.Done()
				results[index] = channel.Receive(ctx)
			})
		}
		completed.Wait(ctx)
		return results, nil
	}); err != nil {
		t.Fatal(err)
	}

	instanceID := api.InstanceID("typed-event-stack-instance")
	result := executeOrchestrationTurn(
		t,
		registry,
		instanceID,
		[]*protos.HistoryEvent{
			helpers.NewOrchestratorStartedEvent(),
			helpers.NewExecutionStartedEvent("typed-event-stack", string(instanceID), nil, nil, nil, nil),
		},
		[]*protos.HistoryEvent{
			helpers.NewOrchestratorStartedEvent(),
			helpers.NewEventRaisedEvent("value", wrapperspb.String("1")),
			helpers.NewEventRaisedEvent("value", wrapperspb.String("2")),
		},
	)
	if got, want := completionResult(t, result), `[2,1]`; got != want {
		t.Fatalf("result = %s, want %s", got, want)
	}
}

func TestExternalEventSelectRaceUsesHistoryOrder(t *testing.T) {
	permutations := []struct {
		name     string
		order    [3]string
		expected string
	}{
		{name: "event-timeout-cancel", order: [3]string{"event", "timeout", "cancel"}, expected: `"event:value"`},
		{name: "event-cancel-timeout", order: [3]string{"event", "cancel", "timeout"}, expected: `"event:value"`},
		{name: "timeout-event-cancel", order: [3]string{"timeout", "event", "cancel"}, expected: `"timeout"`},
		{name: "timeout-cancel-event", order: [3]string{"timeout", "cancel", "event"}, expected: `"timeout"`},
		{name: "cancel-event-timeout", order: [3]string{"cancel", "event", "timeout"}, expected: `"canceled"`},
		{name: "cancel-timeout-event", order: [3]string{"cancel", "timeout", "event"}, expected: `"canceled"`},
	}

	for _, test := range permutations {
		t.Run(test.name, func(t *testing.T) {
			var captured *OrchestrationContext
			registry := NewTaskRegistry()
			if err := registry.AddOrchestratorN("event-select-race", func(ctx *OrchestrationContext) (any, error) {
				captured = ctx
				child, cancel := ctx.WithCancel()
				channel := NewEventChannel[string](child, "signal")
				timer := child.CreateTimer(time.Hour)
				selected := ""
				completed := ctx.NewWaitGroup()
				completed.Add(1)
				child.Go(func(child *OrchestrationContext) {
					defer completed.Done()
					child.Select(
						OnEvent(channel, func(value string) {
							selected = "event:" + value
						}),
						OnTask(timer, func(Task) {
							selected = "timeout"
						}),
					)
				})
				ctx.Go(func(ctx *OrchestrationContext) {
					if err := ctx.WaitForSingleEvent("cancel", -1).Await(nil); err == nil {
						cancel()
					}
				})
				completed.Wait(ctx)
				if selected == "" {
					selected = "canceled"
				}
				return selected, nil
			}); err != nil {
				t.Fatal(err)
			}

			instanceID := api.InstanceID("event-select-race-" + test.name)
			started := helpers.NewOrchestratorStartedEvent()
			executionStarted := helpers.NewExecutionStartedEvent(
				"event-select-race",
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
			events := []*protos.HistoryEvent{helpers.NewOrchestratorStartedEvent()}
			for _, event := range test.order {
				switch event {
				case "event":
					events = append(events, helpers.NewEventRaisedEvent("signal", wrapperspb.String(`"value"`)))
				case "timeout":
					events = append(events, helpers.NewTimerFiredEvent(0, timer.GetFireAt(), nil))
				case "cancel":
					events = append(events, helpers.NewEventRaisedEvent("cancel", nil))
				default:
					t.Fatalf("unknown event %q", event)
				}
			}

			result := executeOrchestrationTurn(
				t,
				registry,
				instanceID,
				[]*protos.HistoryEvent{
					started,
					executionStarted,
					helpers.NewTimerCreatedEvent(0, timer.GetFireAt()),
				},
				events,
			)
			if got := completionResult(t, result); got != test.expected {
				t.Fatalf("result = %s, want %s", got, test.expected)
			}
			if len(captured.eventWaiters) != 0 {
				t.Fatalf("event subscriptions remain after selection: %v", captured.eventWaiters)
			}
		})
	}
}
