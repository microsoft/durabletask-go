package task

import (
	"encoding/json"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/microsoft/durabletask-go/api"
	"github.com/microsoft/durabletask-go/internal/helpers"
	"github.com/microsoft/durabletask-go/internal/protos"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

func TestExecuteGatesPatternSignalWinsTimerAndCancelsScope(t *testing.T) {
	registry := NewTaskRegistry()
	if err := registry.AddOrchestratorN("execute-gates", func(ctx *OrchestrationContext) (any, error) {
		approval := NewEventChannel[string](ctx, "approval")
		timerContext, cancelTimer := ctx.WithCancel()
		timer := timerContext.CreateTimer(time.Hour)
		result := "timeout"
		ctx.Select(
			OnEvent(approval, func(value string) {
				result = value
				cancelTimer()
			}),
			OnTask(timer, func(Task) {
				result = "timeout"
			}),
		)
		return result, nil
	}); err != nil {
		t.Fatal(err)
	}

	instanceID := api.InstanceID("execute-gates")
	started := helpers.NewOrchestratorStartedEvent()
	executionStarted := helpers.NewExecutionStartedEvent("execute-gates", string(instanceID), nil, nil, nil, nil)
	firstTurn := executeOrchestrationTurn(
		t,
		registry,
		instanceID,
		nil,
		[]*protos.HistoryEvent{started, executionStarted},
	)
	if len(firstTurn.Actions) != 1 || firstTurn.Actions[0].GetCreateTimer() == nil {
		t.Fatalf("expected one timer action, got %v", firstTurn.Actions)
	}

	timerCreated := helpers.NewTimerCreatedEvent(0, firstTurn.Actions[0].GetCreateTimer().GetFireAt())
	secondTurn := executeOrchestrationTurn(
		t,
		registry,
		instanceID,
		[]*protos.HistoryEvent{started, executionStarted, timerCreated},
		[]*protos.HistoryEvent{
			helpers.NewOrchestratorStartedEvent(),
			helpers.NewEventRaisedEvent("approval", wrapperspb.String(`"approved"`)),
		},
	)
	if got := completionResult(t, secondTurn); got != `"approved"` {
		t.Fatalf("gate result = %s, want approved", got)
	}
}

func TestExecuteJobPatternManySignalsSharedStateAndWideFanOut(t *testing.T) {
	const (
		signalCount = 15
		fanOutCount = 32
	)
	type output struct {
		Signals []int
		Sum     int
		FanOut  int
	}

	registry := NewTaskRegistry()
	if err := registry.AddOrchestratorN("execute-job", func(ctx *OrchestrationContext) (any, error) {
		signals := make([]int, signalCount)
		waitGroup := ctx.NewWaitGroup()
		waitGroup.Add(signalCount)
		for i := 0; i < signalCount; i++ {
			index := i
			channel := NewEventChannel[int](ctx, fmt.Sprintf("signal-%02d", index))
			ctx.Go(func(ctx *OrchestrationContext) {
				defer waitGroup.Done()
				signals[index] = channel.Receive(ctx)
			})
		}
		waitGroup.Wait(ctx)

		tasks := make([]Task, fanOutCount)
		for i := 0; i < fanOutCount; i++ {
			tasks[i] = ctx.CallActivity("work", WithActivityInput(i))
		}
		if err := ctx.WhenAll(tasks...); err != nil {
			return nil, err
		}

		sum := 0
		for _, value := range signals {
			sum += value
		}
		return output{Signals: signals, Sum: sum, FanOut: len(tasks)}, nil
	}); err != nil {
		t.Fatal(err)
	}

	instanceID := api.InstanceID("execute-job")
	started := helpers.NewOrchestratorStartedEvent()
	executionStarted := helpers.NewExecutionStartedEvent("execute-job", string(instanceID), nil, nil, nil, nil)
	firstTurn := executeOrchestrationTurn(
		t,
		registry,
		instanceID,
		nil,
		[]*protos.HistoryEvent{started, executionStarted},
	)
	if len(firstTurn.Actions) != 0 {
		t.Fatalf("signal wait produced unexpected actions: %v", firstTurn.Actions)
	}

	signalEvents := make([]*protos.HistoryEvent, 0, signalCount+1)
	signalEvents = append(signalEvents, helpers.NewOrchestratorStartedEvent())
	for i := 0; i < signalCount; i++ {
		signalEvents = append(signalEvents, helpers.NewEventRaisedEvent(
			fmt.Sprintf("signal-%02d", i),
			wrapperspb.String(strconv.Itoa(i+1)),
		))
	}
	secondTurn := executeOrchestrationTurn(
		t,
		registry,
		instanceID,
		[]*protos.HistoryEvent{started, executionStarted},
		signalEvents,
	)
	if len(secondTurn.Actions) != fanOutCount {
		t.Fatalf("fan-out action count = %d, want %d", len(secondTurn.Actions), fanOutCount)
	}
	for i, action := range secondTurn.Actions {
		scheduled := action.GetScheduleTask()
		if scheduled == nil || scheduled.GetName() != "work" || action.GetId() != int32(i) {
			t.Fatalf("unexpected fan-out action %d: %v", i, action)
		}
	}

	oldEvents := []*protos.HistoryEvent{started, executionStarted}
	oldEvents = append(oldEvents, signalEvents...)
	for i := 0; i < fanOutCount; i++ {
		oldEvents = append(oldEvents, helpers.NewTaskScheduledEvent(
			int32(i),
			"work",
			nil,
			wrapperspb.String(strconv.Itoa(i)),
			nil,
		))
	}
	newEvents := []*protos.HistoryEvent{helpers.NewOrchestratorStartedEvent()}
	for i := fanOutCount - 1; i >= 0; i-- {
		newEvents = append(newEvents, helpers.NewTaskCompletedEvent(int32(i), nil))
	}
	thirdTurn := executeOrchestrationTurn(t, registry, instanceID, oldEvents, newEvents)

	var result output
	if err := json.Unmarshal([]byte(completionResult(t, thirdTurn)), &result); err != nil {
		t.Fatal(err)
	}
	if result.FanOut != fanOutCount || result.Sum != signalCount*(signalCount+1)/2 {
		t.Fatalf("unexpected execute-job result: %+v", result)
	}
	for i, value := range result.Signals {
		if value != i+1 {
			t.Fatalf("signal[%d] = %d, want %d", i, value, i+1)
		}
	}
}
