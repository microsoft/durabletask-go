package task

import (
	"container/list"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/microsoft/durabletask-go/api"
	"github.com/microsoft/durabletask-go/internal/helpers"
	"github.com/microsoft/durabletask-go/internal/protos"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

func TestSelectRejectsForeignCases(t *testing.T) {
	for _, test := range []struct {
		name    string
		ready   bool
		event   bool
		whenAny bool
	}{
		{name: "pending-task"},
		{name: "ready-task", ready: true},
		{name: "empty-event", event: true},
		{name: "buffered-event", event: true, ready: true},
		{name: "when-any-pending", whenAny: true},
		{name: "when-any-ready", whenAny: true, ready: true},
	} {
		for _, localReady := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/local-ready=%t", test.name, localReady), func(t *testing.T) {
				// Matching instance IDs must not make distinct execution engines interchangeable.
				foreign := newTestOrchestrationContext(NewTaskRegistry(), "instance", nil, nil)
				foreignTask := newTaskInScope(foreign, foreign.scope)
				channel := NewEventChannel[int](foreign, "value")
				if test.ready {
					if test.event {
						queue := list.New()
						queue.PushBack(&bufferedEvent{
							event: helpers.NewEventRaisedEvent("value", wrapperspb.String("42")),
							order: 0,
						})
						foreign.bufferedExternalEvents["VALUE"] = queue
					} else {
						foreignTask.complete(nil)
					}
				}
				var owner *OrchestrationContext
				var local *completableTask
				invoked := false
				registry := NewTaskRegistry()
				if err := registry.AddOrchestratorN("select-owner", func(ctx *OrchestrationContext) (any, error) {
					owner = ctx
					local = newTaskInScope(ctx, ctx.scope)
					if localReady {
						local.complete(nil)
					}
					var recovered any
					func() {
						defer func() { recovered = recover() }()
						if test.whenAny {
							ctx.WhenAny(local, foreignTask)
							invoked = true
							return
						}
						candidate := OnTask(foreignTask, func(Task) { invoked = true })
						if test.event {
							candidate = OnEvent(channel, func(int) { invoked = true })
						}
						ctx.Select(OnTask(local, func(Task) { invoked = true }), candidate)
					}()
					return recovered == "Select case belongs to a different orchestration" && !invoked, nil
				}); err != nil {
					t.Fatal(err)
				}
				response := executeOrchestrationTurn(t, registry, "instance", nil, []*protos.HistoryEvent{
					helpers.NewOrchestratorStartedEvent(),
					helpers.NewExecutionStartedEvent("select-owner", "instance", nil, nil, nil, nil),
				})
				if got := completionResult(t, response); got != "true" {
					t.Fatalf("foreign selection was not rejected: %s", got)
				}
				if len(foreignTask.waiters) != 0 || len(foreign.eventWaiters) != 0 ||
					len(local.waiters) != 0 || len(owner.scope.waiters) != 0 {
					t.Fatal("invalid selection registered a waiter")
				}
				if test.event && test.ready {
					if value, ok, err := channel.TryReceiveErr(); !ok || err != nil || value != 42 {
						t.Fatalf("foreign event was not preserved: value=%d received=%t err=%v", value, ok, err)
					}
				}
			})
		}
	}
}

func TestSelectAllowsSameEngineChildScopes(t *testing.T) {
	registry := NewTaskRegistry()
	if err := registry.AddOrchestratorN("select-scopes", func(ctx *OrchestrationContext) (any, error) {
		child, _ := ctx.WithCancel()
		rootTask := newTaskInScope(ctx, ctx.scope)
		childTask := newTaskInScope(child, child.scope)
		rootTask.complete(nil)
		childTask.complete(nil)
		selected := 0
		ctx.Select(OnTask(childTask, func(Task) { selected++ }))
		child.Select(OnTask(rootTask, func(Task) { selected++ }))
		child.Select(OnEvent(NewEventChannel[int](child, "value"), func(value int) { selected += value }))
		return selected, nil
	}); err != nil {
		t.Fatal(err)
	}
	response := executeOrchestrationTurn(t, registry, "instance", nil, []*protos.HistoryEvent{
		helpers.NewOrchestratorStartedEvent(),
		helpers.NewExecutionStartedEvent("select-scopes", "instance", nil, nil, nil, nil),
		helpers.NewEventRaisedEvent("value", wrapperspb.String("3")),
	})
	if got := completionResult(t, response); got != "5" {
		t.Fatalf("same-engine selection = %s, want 5", got)
	}
}

func TestEventChannelDecodeErrorNamesGenericType(t *testing.T) {
	t.Run("interface", func(t *testing.T) { testEventDecodeType[any](t, "interface {}") })
	t.Run("pointer", func(t *testing.T) { testEventDecodeType[*int](t, "*int") })
	t.Run("slice", func(t *testing.T) { testEventDecodeType[[]int](t, "[]int") })
	t.Run("map", func(t *testing.T) { testEventDecodeType[map[string]int](t, "map[string]int") })
}

func testEventDecodeType[T any](t *testing.T, want string) {
	t.Helper()
	ctx := newTestOrchestrationContext(NewTaskRegistry(), "instance", nil, nil)
	queue := list.New()
	queue.PushBack(&bufferedEvent{event: helpers.NewEventRaisedEvent("value", wrapperspb.String("{invalid"))})
	ctx.bufferedExternalEvents["VALUE"] = queue
	_, received, err := NewEventChannel[T](ctx, "value").TryReceiveErr()
	var syntaxError *json.SyntaxError
	if !received || err == nil || !strings.Contains(err.Error(), "as "+want+":") || !errors.As(err, &syntaxError) {
		t.Fatalf("expected type %q and wrapped syntax error, received=%t err=%v", want, received, err)
	}
}

func TestWhenAnyUsesHistoryOrder(t *testing.T) {
	registry := NewTaskRegistry()
	if err := registry.AddOrchestratorN("when-any", func(ctx *OrchestrationContext) (any, error) {
		first := ctx.CallActivity("first")
		second := ctx.CallActivity("second")
		switch ctx.WhenAny(first, second) {
		case first:
			return "first", nil
		case second:
			return "second", nil
		default:
			panic("WhenAny returned an unknown task")
		}
	}); err != nil {
		t.Fatal(err)
	}

	instanceID := api.InstanceID("when-any-instance")
	result := executeOrchestrationTurn(
		t,
		registry,
		instanceID,
		[]*protos.HistoryEvent{
			helpers.NewOrchestratorStartedEvent(),
			helpers.NewExecutionStartedEvent("when-any", string(instanceID), nil, nil, nil, nil),
			helpers.NewTaskScheduledEvent(0, "first", nil, nil, nil),
			helpers.NewTaskScheduledEvent(1, "second", nil, nil, nil),
		},
		[]*protos.HistoryEvent{
			helpers.NewOrchestratorStartedEvent(),
			helpers.NewTaskCompletedEvent(1, nil),
			helpers.NewTaskCompletedEvent(0, nil),
		},
	)
	if got, want := completionResult(t, result), `"second"`; got != want {
		t.Fatalf("result = %s, want %s", got, want)
	}
}

func TestWhenAllReturnsFirstFailureByHistory(t *testing.T) {
	registry := NewTaskRegistry()
	if err := registry.AddOrchestratorN("when-all", func(ctx *OrchestrationContext) (any, error) {
		first := ctx.CallActivity("first")
		second := ctx.CallActivity("second")
		return nil, ctx.WhenAll(first, second)
	}); err != nil {
		t.Fatal(err)
	}

	instanceID := api.InstanceID("when-all-instance")
	result := executeOrchestrationTurn(
		t,
		registry,
		instanceID,
		[]*protos.HistoryEvent{
			helpers.NewOrchestratorStartedEvent(),
			helpers.NewExecutionStartedEvent("when-all", string(instanceID), nil, nil, nil, nil),
			helpers.NewTaskScheduledEvent(0, "first", nil, nil, nil),
			helpers.NewTaskScheduledEvent(1, "second", nil, nil, nil),
		},
		[]*protos.HistoryEvent{
			helpers.NewOrchestratorStartedEvent(),
			helpers.NewTaskFailedEvent(1, &protos.TaskFailureDetails{ErrorType: "second", ErrorMessage: "second failed"}),
			helpers.NewTaskFailedEvent(0, &protos.TaskFailureDetails{ErrorType: "first", ErrorMessage: "first failed"}),
		},
	)
	completed := completionAction(t, result)
	if got := completed.GetFailureDetails().GetErrorMessage(); got != "Task 'second' (#1) failed with an unhandled exception: second failed" {
		t.Fatalf("failure = %q", got)
	}
}

func TestSelectChoosesEventBeforeLaterTaskCompletion(t *testing.T) {
	registry := NewTaskRegistry()
	if err := registry.AddOrchestratorN("select", func(ctx *OrchestrationContext) (any, error) {
		activity := ctx.CallActivity("activity")
		channel := NewEventChannel[string](ctx, "signal")
		selected := ""
		ctx.Select(
			OnTask(activity, func(Task) { selected = "activity" }),
			OnEvent(channel, func(value string) { selected = value }),
		)
		return selected, nil
	}); err != nil {
		t.Fatal(err)
	}

	instanceID := api.InstanceID("select-instance")
	result := executeOrchestrationTurn(
		t,
		registry,
		instanceID,
		[]*protos.HistoryEvent{
			helpers.NewOrchestratorStartedEvent(),
			helpers.NewExecutionStartedEvent("select", string(instanceID), nil, nil, nil, nil),
			helpers.NewTaskScheduledEvent(0, "activity", nil, nil, nil),
		},
		[]*protos.HistoryEvent{
			helpers.NewOrchestratorStartedEvent(),
			helpers.NewEventRaisedEvent("signal", wrapperspb.String(`"event"`)),
			helpers.NewTaskCompletedEvent(0, nil),
		},
	)
	if got, want := completionResult(t, result), `"event"`; got != want {
		t.Fatalf("result = %s, want %s", got, want)
	}
}

func TestEventChannelReceivesRepeatedValues(t *testing.T) {
	registry := NewTaskRegistry()
	if err := registry.AddOrchestratorN("channel", func(ctx *OrchestrationContext) (any, error) {
		channel := NewEventChannel[int](ctx, "value")
		if channel != NewEventChannel[int](ctx, "VALUE") {
			return nil, errors.New("same event name returned a different channel")
		}
		return []int{channel.Receive(ctx), channel.Receive(ctx)}, nil
	}); err != nil {
		t.Fatal(err)
	}

	instanceID := api.InstanceID("channel-instance")
	result := executeOrchestrationTurn(
		t,
		registry,
		instanceID,
		nil,
		[]*protos.HistoryEvent{
			helpers.NewOrchestratorStartedEvent(),
			helpers.NewExecutionStartedEvent("channel", string(instanceID), nil, nil, nil, nil),
			helpers.NewEventRaisedEvent("value", wrapperspb.String("1")),
			helpers.NewEventRaisedEvent("VALUE", wrapperspb.String("2")),
		},
	)
	if got, want := completionResult(t, result), `[1,2]`; got != want {
		t.Fatalf("result = %s, want %s", got, want)
	}
}

func TestContinueAsNewCarriesEventsInArrivalOrderAcrossNames(t *testing.T) {
	registry := NewTaskRegistry()
	if err := registry.AddOrchestratorN("carryover", func(ctx *OrchestrationContext) (any, error) {
		NewEventChannel[int](ctx, "first")
		NewEventChannel[int](ctx, "second")
		ctx.ContinueAsNew(nil, WithKeepUnprocessedEvents())
		return nil, nil
	}); err != nil {
		t.Fatal(err)
	}

	instanceID := api.InstanceID("carryover-instance")
	result := executeOrchestrationTurn(
		t,
		registry,
		instanceID,
		nil,
		[]*protos.HistoryEvent{
			helpers.NewOrchestratorStartedEvent(),
			helpers.NewExecutionStartedEvent("carryover", string(instanceID), nil, nil, nil, nil),
			helpers.NewEventRaisedEvent("second", wrapperspb.String("2")),
			helpers.NewEventRaisedEvent("first", wrapperspb.String("1")),
		},
	)
	carryover := completionAction(t, result).GetCarryoverEvents()
	if len(carryover) != 2 {
		t.Fatalf("carryover count = %d, want 2", len(carryover))
	}
	if got := carryover[0].GetEventRaised().GetName(); got != "second" {
		t.Fatalf("first carryover event = %q, want second", got)
	}
	if got := carryover[1].GetEventRaised().GetName(); got != "first" {
		t.Fatalf("second carryover event = %q, want first", got)
	}
}

func TestCoroutinesSendEventsInDeterministicOrder(t *testing.T) {
	registry := NewTaskRegistry()
	if err := registry.AddOrchestratorN("send-events", func(ctx *OrchestrationContext) (any, error) {
		wg := ctx.NewWaitGroup()
		wg.Add(2)
		ctx.Go(func(ctx *OrchestrationContext) {
			defer wg.Done()
			if err := ctx.SendEvent("target", "first", 1); err != nil {
				panic(err)
			}
		})
		ctx.Go(func(ctx *OrchestrationContext) {
			defer wg.Done()
			if err := ctx.SendEvent("target", "second", 2); err != nil {
				panic(err)
			}
		})
		wg.Wait(ctx)
		return nil, nil
	}); err != nil {
		t.Fatal(err)
	}

	instanceID := api.InstanceID("send-events-instance")
	result := executeOrchestrationTurn(
		t,
		registry,
		instanceID,
		nil,
		[]*protos.HistoryEvent{
			helpers.NewOrchestratorStartedEvent(),
			helpers.NewExecutionStartedEvent("send-events", string(instanceID), nil, nil, nil, nil),
		},
	)
	if len(result.Actions) != 3 {
		t.Fatalf("action count = %d, want two events and completion", len(result.Actions))
	}
	for i, name := range []string{"first", "second"} {
		action := result.Actions[i]
		if action.GetId() != int32(i) || action.GetSendEvent().GetName() != name {
			t.Fatalf("action %d = %v", i, action)
		}
	}
}

func TestSendEventReplaysFromEventSentHistory(t *testing.T) {
	registry := NewTaskRegistry()
	if err := registry.AddOrchestratorN("send-and-wait", func(ctx *OrchestrationContext) (any, error) {
		if err := ctx.SendEvent("target", "ping", 1); err != nil {
			return nil, err
		}
		return nil, ctx.WaitForSingleEvent("done", -1).Await(nil)
	}); err != nil {
		t.Fatal(err)
	}
	instanceID := api.InstanceID("send-and-wait-instance")
	oldEvents := []*protos.HistoryEvent{
		helpers.NewOrchestratorStartedEvent(),
		helpers.NewExecutionStartedEvent("send-and-wait", string(instanceID), nil, nil, nil, nil),
		helpers.NewSendEventEvent(0, "target", "ping", wrapperspb.String("1")),
	}
	result := executeOrchestrationTurn(t, registry, instanceID, oldEvents, nil)
	if len(result.Actions) != 0 {
		t.Fatalf("replay produced unexpected actions: %v", result.Actions)
	}
}

func executeOrchestrationTurn(
	t *testing.T,
	registry *TaskRegistry,
	instanceID api.InstanceID,
	oldEvents []*protos.HistoryEvent,
	newEvents []*protos.HistoryEvent,
) *protos.OrchestratorResponse {
	t.Helper()
	result, err := NewTaskExecutor(registry).ExecuteOrchestrator(
		context.Background(),
		instanceID,
		oldEvents,
		newEvents,
		supportedEntityParameters(),
	)
	if err != nil {
		t.Fatal(err)
	}
	return result.Response
}

func completionAction(t *testing.T, response *protos.OrchestratorResponse) *protos.CompleteOrchestrationAction {
	t.Helper()
	for _, action := range response.Actions {
		if completed := action.GetCompleteOrchestration(); completed != nil {
			return completed
		}
	}
	t.Fatal("orchestration did not produce a completion action")
	return nil
}

func completionResult(t *testing.T, response *protos.OrchestratorResponse) string {
	t.Helper()
	return completionAction(t, response).GetResult().GetValue()
}

func TestEventChannelReceiveErrReturnsPayloadError(t *testing.T) {
	registry := NewTaskRegistry()
	if err := registry.AddOrchestratorN("event-error", func(ctx *OrchestrationContext) (any, error) {
		_, err := NewEventChannel[int](ctx, "value").ReceiveErr(ctx)
		return err != nil, nil
	}); err != nil {
		t.Fatal(err)
	}
	instanceID := api.InstanceID("event-error-instance")
	result := executeOrchestrationTurn(
		t,
		registry,
		instanceID,
		nil,
		[]*protos.HistoryEvent{
			helpers.NewOrchestratorStartedEvent(),
			helpers.NewExecutionStartedEvent("event-error", string(instanceID), nil, nil, nil, nil),
			helpers.NewEventRaisedEvent("value", wrapperspb.String(`"not-an-int"`)),
		},
	)
	if got, want := completionResult(t, result), "true"; got != want {
		t.Fatalf("result = %s, want %s", got, want)
	}
}
