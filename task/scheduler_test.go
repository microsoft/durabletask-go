package task

import (
	"context"
	"runtime"
	"testing"
	"time"

	"github.com/microsoft/durabletask-go/api"
	"github.com/microsoft/durabletask-go/internal/helpers"
	"github.com/microsoft/durabletask-go/internal/protos"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

func TestCoroutineSchedulerRunsReadyCoroutinesInIDOrder(t *testing.T) {
	registry := NewTaskRegistry()
	if err := registry.AddOrchestratorN("parallel", func(ctx *OrchestrationContext) (any, error) {
		results := make([]string, 0, 2)
		wg := ctx.NewWaitGroup()
		wg.Add(2)

		ctx.Go(func(ctx *OrchestrationContext) {
			defer wg.Done()
			var value string
			if err := ctx.CallActivity("first").Await(&value); err != nil {
				panic(err)
			}
			results = append(results, value)
		})
		ctx.Go(func(ctx *OrchestrationContext) {
			defer wg.Done()
			var value string
			if err := ctx.CallActivity("second").Await(&value); err != nil {
				panic(err)
			}
			results = append(results, value)
		})

		wg.Wait(ctx)
		return results, nil
	}); err != nil {
		t.Fatal(err)
	}

	executor := NewTaskExecutor(registry)
	instanceID := api.InstanceID("parallel-instance")
	started := helpers.NewOrchestratorStartedEvent()
	executionStarted := helpers.NewExecutionStartedEvent("parallel", string(instanceID), nil, nil, nil, nil)

	firstTurn, err := executor.ExecuteOrchestrator(
		context.Background(),
		instanceID,
		nil,
		[]*protos.HistoryEvent{started, executionStarted}, nil)

	if err != nil {
		t.Fatal(err)
	}
	if len(firstTurn.Response.Actions) != 2 {
		t.Fatalf("got %d first-turn actions, want 2", len(firstTurn.Response.Actions))
	}
	if got := firstTurn.Response.Actions[0].GetScheduleTask().GetName(); got != "first" {
		t.Fatalf("first action = %q, want first", got)
	}
	if got := firstTurn.Response.Actions[1].GetScheduleTask().GetName(); got != "second" {
		t.Fatalf("second action = %q, want second", got)
	}

	oldEvents := []*protos.HistoryEvent{
		started,
		executionStarted,
		helpers.NewTaskScheduledEvent(0, "first", nil, nil, nil),
		helpers.NewTaskScheduledEvent(1, "second", nil, nil, nil),
	}
	newEvents := []*protos.HistoryEvent{
		helpers.NewOrchestratorStartedEvent(),
		helpers.NewTaskCompletedEvent(0, wrapperspb.String(`"one"`)),
		helpers.NewTaskCompletedEvent(1, wrapperspb.String(`"two"`)),
	}
	secondTurn, err := executor.ExecuteOrchestrator(context.Background(), instanceID, oldEvents, newEvents, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(secondTurn.Response.Actions) != 1 {
		t.Fatalf("got %d second-turn actions, want completion only", len(secondTurn.Response.Actions))
	}
	completed := secondTurn.Response.Actions[0].GetCompleteOrchestration()
	if completed == nil {
		t.Fatal("second turn did not complete the orchestration")
	}
	if got, want := completed.GetResult().GetValue(), `["one","two"]`; got != want {
		t.Fatalf("result = %s, want %s", got, want)
	}
}

func TestCoroutineSchedulerUsesHistoryCompletionOrder(t *testing.T) {
	registry := NewTaskRegistry()
	if err := registry.AddOrchestratorN("completion-order", func(ctx *OrchestrationContext) (any, error) {
		results := make([]string, 0, 2)
		wg := ctx.NewWaitGroup()
		wg.Add(2)
		for _, name := range []string{"first", "second"} {
			name := name
			ctx.Go(func(ctx *OrchestrationContext) {
				defer wg.Done()
				var value string
				if err := ctx.CallActivity(name).Await(&value); err != nil {
					panic(err)
				}
				results = append(results, value)
			})
		}
		wg.Wait(ctx)
		return results, nil
	}); err != nil {
		t.Fatal(err)
	}

	instanceID := api.InstanceID("completion-order-instance")
	oldEvents := []*protos.HistoryEvent{
		helpers.NewOrchestratorStartedEvent(),
		helpers.NewExecutionStartedEvent("completion-order", string(instanceID), nil, nil, nil, nil),
		helpers.NewTaskScheduledEvent(0, "first", nil, nil, nil),
		helpers.NewTaskScheduledEvent(1, "second", nil, nil, nil),
	}
	newEvents := []*protos.HistoryEvent{
		helpers.NewOrchestratorStartedEvent(),
		helpers.NewTaskCompletedEvent(1, wrapperspb.String(`"two"`)),
		helpers.NewTaskCompletedEvent(0, wrapperspb.String(`"one"`)),
	}

	result, err := NewTaskExecutor(registry).ExecuteOrchestrator(
		context.Background(),
		instanceID,
		oldEvents,
		newEvents, nil)

	if err != nil {
		t.Fatal(err)
	}
	completed := result.Response.Actions[0].GetCompleteOrchestration()
	if got, want := completed.GetResult().GetValue(), `["two","one"]`; got != want {
		t.Fatalf("result = %s, want %s", got, want)
	}
}

func TestCoroutinePanicFailsOrchestration(t *testing.T) {
	registry := NewTaskRegistry()
	if err := registry.AddOrchestratorN("panic", func(ctx *OrchestrationContext) (any, error) {
		ctx.Go(func(*OrchestrationContext) {
			panic("boom")
		})
		_ = ctx.WaitForSingleEvent("never", -1).Await(nil)
		return nil, nil
	}); err != nil {
		t.Fatal(err)
	}

	instanceID := api.InstanceID("panic-instance")
	result, err := NewTaskExecutor(registry).ExecuteOrchestrator(
		context.Background(),
		instanceID,
		nil,
		[]*protos.HistoryEvent{
			helpers.NewOrchestratorStartedEvent(),
			helpers.NewExecutionStartedEvent("panic", string(instanceID), nil, nil, nil, nil),
		}, nil)

	if err != nil {
		t.Fatal(err)
	}
	if len(result.Response.Actions) != 1 {
		t.Fatalf("got %d actions, want one failure action", len(result.Response.Actions))
	}
	completed := result.Response.Actions[0].GetCompleteOrchestration()
	if completed == nil || completed.GetOrchestrationStatus() != protos.OrchestrationStatus_ORCHESTRATION_STATUS_FAILED {
		t.Fatalf("unexpected completion action: %v", result.Response.Actions[0])
	}
	if got := completed.GetFailureDetails().GetErrorMessage(); got != "coroutine 1 panicked: boom" {
		t.Fatalf("failure = %q", got)
	}
}

func TestRootCoroutinePanicFailsOrchestration(t *testing.T) {
	registry := NewTaskRegistry()
	if err := registry.AddOrchestratorN("root-panic", func(*OrchestrationContext) (any, error) {
		panic("root boom")
	}); err != nil {
		t.Fatal(err)
	}
	instanceID := api.InstanceID("root-panic-instance")
	result := executeOrchestrationTurn(
		t,
		registry,
		instanceID,
		nil,
		[]*protos.HistoryEvent{
			helpers.NewOrchestratorStartedEvent(),
			helpers.NewExecutionStartedEvent("root-panic", string(instanceID), nil, nil, nil, nil),
		},
	)
	completed := completionAction(t, result)
	if completed.GetOrchestrationStatus() != protos.OrchestrationStatus_ORCHESTRATION_STATUS_FAILED {
		t.Fatalf("status = %v, want failed", completed.GetOrchestrationStatus())
	}
	if got := completed.GetFailureDetails().GetErrorMessage(); got != "coroutine 0 panicked: root boom" {
		t.Fatalf("failure = %q", got)
	}
}

func TestCoroutineReplayIsByteDeterministic(t *testing.T) {
	registry := NewTaskRegistry()
	if err := registry.AddOrchestratorN("deterministic", func(ctx *OrchestrationContext) (any, error) {
		wg := ctx.NewWaitGroup()
		wg.Add(4)
		for i := 0; i < 4; i++ {
			ctx.Go(func(ctx *OrchestrationContext) {
				defer wg.Done()
				ctx.CallActivity("activity")
			})
		}
		wg.Wait(ctx)
		return nil, nil
	}); err != nil {
		t.Fatal(err)
	}

	instanceID := api.InstanceID("deterministic-instance")
	events := []*protos.HistoryEvent{
		helpers.NewOrchestratorStartedEvent(),
		helpers.NewExecutionStartedEvent("deterministic", string(instanceID), nil, nil, nil, nil),
	}
	executor := NewTaskExecutor(registry)

	var expected []byte
	for i := 0; i < 100; i++ {
		result, err := executor.ExecuteOrchestrator(context.Background(), instanceID, nil, events, nil)
		if err != nil {
			t.Fatal(err)
		}
		data, err := proto.Marshal(&protos.OrchestratorResponse{Actions: result.Response.Actions})
		if err != nil {
			t.Fatal(err)
		}
		if i == 0 {
			expected = data
		} else if !proto.Equal(
			&protos.OrchestratorResponse{Actions: result.Response.Actions},
			mustUnmarshalResponse(t, expected),
		) {
			t.Fatalf("iteration %d produced different actions", i)
		}
	}
}

func TestCoroutineSchedulerDoesNotLeakAcrossTurns(t *testing.T) {
	registry := NewTaskRegistry()
	if err := registry.AddOrchestratorN("blocked", func(ctx *OrchestrationContext) (any, error) {
		defer ctx.Go(func(*OrchestrationContext) {})
		_ = ctx.WaitForSingleEvent("never", -1).Await(nil)
		return nil, nil
	}); err != nil {
		t.Fatal(err)
	}
	instanceID := api.InstanceID("blocked-instance")
	events := []*protos.HistoryEvent{
		helpers.NewOrchestratorStartedEvent(),
		helpers.NewExecutionStartedEvent("blocked", string(instanceID), nil, nil, nil, nil),
	}
	executor := NewTaskExecutor(registry)
	before := runtime.NumGoroutine()

	for i := 0; i < 10_000; i++ {
		if _, err := executor.ExecuteOrchestrator(context.Background(), instanceID, nil, events, nil); err != nil {
			t.Fatal(err)
		}
	}

	runtime.GC()
	time.Sleep(50 * time.Millisecond)
	if after := runtime.NumGoroutine(); after > before+2 {
		t.Fatalf("goroutines grew from %d to %d", before, after)
	}
}

func mustUnmarshalResponse(t *testing.T, data []byte) *protos.OrchestratorResponse {
	t.Helper()
	response := new(protos.OrchestratorResponse)
	if err := proto.Unmarshal(data, response); err != nil {
		t.Fatal(err)
	}
	return response
}
