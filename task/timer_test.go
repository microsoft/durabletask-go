package task

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/microsoft/durabletask-go/api"
	"github.com/microsoft/durabletask-go/internal/helpers"
	"github.com/microsoft/durabletask-go/internal/protos"
	"google.golang.org/protobuf/types/known/timestamppb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

func TestLongTimerSplitsIntoDeterministicSequentialChunks(t *testing.T) {
	const (
		maximumInterval = 3 * time.Hour
		delay           = 7 * time.Hour
	)
	registry := NewTaskRegistry()
	if err := registry.AddOrchestratorN("long-timer", func(ctx *OrchestrationContext) (any, error) {
		return "done", ctx.CreateTimer(delay).Await(nil)
	}); err != nil {
		t.Fatal(err)
	}
	executor := NewTaskExecutor(registry, WithMaximumTimerInterval(maximumInterval))
	instanceID := api.InstanceID("long-timer-instance")
	startTime := time.Date(2026, time.August, 27, 12, 0, 0, 0, time.UTC)
	started := timerOrchestratorStartedAt(startTime)
	executionStarted := helpers.NewExecutionStartedEvent(
		"long-timer",
		string(instanceID),
		nil,
		nil,
		nil,
		nil,
	)

	var oldEvents []*protos.HistoryEvent
	newEvents := []*protos.HistoryEvent{started, executionStarted}
	response := executeTimerTurn(t, executor, instanceID, oldEvents, newEvents)
	expectedFireTimes := []time.Time{
		startTime.Add(maximumInterval),
		startTime.Add(2 * maximumInterval),
		startTime.Add(delay),
	}
	for timerID, expectedFireTime := range expectedFireTimes {
		timer := onlyTimerAction(t, response)
		requireTimerFireAt(t, timer, expectedFireTime)

		oldEvents = append(oldEvents, newEvents...)
		oldEvents = append(
			oldEvents,
			helpers.NewTimerCreatedEvent(int32(timerID), timer.GetFireAt()),
		)
		newEvents = []*protos.HistoryEvent{
			timerOrchestratorStartedAt(timer.GetFireAt().AsTime()),
			helpers.NewTimerFiredEvent(int32(timerID), timer.GetFireAt(), nil),
		}
		response = executeTimerTurn(t, executor, instanceID, oldEvents, newEvents)
	}
	if got, want := completionResult(t, response), `"done"`; got != want {
		t.Fatalf("result = %s, want %s", got, want)
	}
}

func TestLongTimerReplaysHistoryCreatedBeforeTimerSplitting(t *testing.T) {
	const delay = 7 * time.Hour
	registry := NewTaskRegistry()
	if err := registry.AddActivityN("after-timer", func(ActivityContext) (any, error) {
		return "done", nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := registry.AddOrchestratorN("legacy-long-timer", func(ctx *OrchestrationContext) (any, error) {
		if err := ctx.CreateTimer(delay).Await(nil); err != nil {
			return nil, err
		}
		var result string
		if err := ctx.CallActivity("after-timer").Await(&result); err != nil {
			return nil, err
		}
		return result, nil
	}); err != nil {
		t.Fatal(err)
	}

	instanceID := api.InstanceID("legacy-long-timer-instance")
	startTime := time.Date(2026, time.August, 27, 12, 0, 0, 0, time.UTC)
	deadline := startTime.Add(delay)
	history := []*protos.HistoryEvent{
		timerOrchestratorStartedAt(startTime),
		helpers.NewExecutionStartedEvent(
			"legacy-long-timer",
			string(instanceID),
			nil,
			nil,
			nil,
			nil,
		),
		helpers.NewTimerCreatedEvent(0, timestamppb.New(deadline)),
		timerOrchestratorStartedAt(deadline.Add(-time.Second)),
		helpers.NewTimerFiredEvent(0, timestamppb.New(deadline), nil),
		helpers.NewTaskScheduledEvent(1, "after-timer", nil, nil, nil),
		timerOrchestratorStartedAt(deadline),
		helpers.NewTaskCompletedEvent(1, wrapperspb.String(`"done"`)),
	}
	response := executeTimerTurn(
		t,
		NewTaskExecutor(registry, WithMaximumTimerInterval(3*time.Hour)),
		instanceID,
		history,
		nil,
	)
	if got, want := completionResult(t, response), `"done"`; got != want {
		t.Fatalf("result = %s, want %s", got, want)
	}
	for _, action := range response.Actions {
		if action.GetCreateTimer() != nil || action.GetScheduleTask() != nil {
			t.Fatalf("legacy timer replay produced unexpected action: %v", action)
		}
	}
}

func TestCreateTimerPreservesZeroAndNegativeDeadlines(t *testing.T) {
	tests := []struct {
		name  string
		delay time.Duration
	}{
		{name: "zero", delay: 0},
		{name: "negative", delay: -time.Hour},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			registry := NewTaskRegistry()
			if err := registry.AddOrchestratorN("timer-edge", func(ctx *OrchestrationContext) (any, error) {
				return nil, ctx.CreateTimer(test.delay).Await(nil)
			}); err != nil {
				t.Fatal(err)
			}
			startTime := time.Date(2026, time.August, 27, 12, 0, 0, 0, time.UTC)
			instanceID := api.InstanceID("timer-edge-" + test.name)
			response := executeTimerTurn(
				t,
				NewTaskExecutor(registry),
				instanceID,
				nil,
				[]*protos.HistoryEvent{
					timerOrchestratorStartedAt(startTime),
					helpers.NewExecutionStartedEvent("timer-edge", string(instanceID), nil, nil, nil, nil),
				},
			)
			requireTimerFireAt(t, onlyTimerAction(t, response), startTime.Add(test.delay))
		})
	}
}

func TestCreateTimerHandlesExtremeDurationsAndOverflow(t *testing.T) {
	const maximumDuration = time.Duration(1<<63 - 1)
	startTime := time.Date(2000, time.January, 1, 0, 0, 0, 0, time.UTC)
	registry := NewTaskRegistry()
	if err := registry.AddOrchestratorN("extreme-timer", func(ctx *OrchestrationContext) (any, error) {
		return nil, ctx.CreateTimer(maximumDuration).Await(nil)
	}); err != nil {
		t.Fatal(err)
	}
	instanceID := api.InstanceID("extreme-timer-instance")
	response := executeTimerTurn(
		t,
		NewTaskExecutor(registry, WithMaximumTimerInterval(maximumDuration)),
		instanceID,
		nil,
		[]*protos.HistoryEvent{
			timerOrchestratorStartedAt(startTime),
			helpers.NewExecutionStartedEvent("extreme-timer", string(instanceID), nil, nil, nil, nil),
		},
	)
	requireTimerFireAt(t, onlyTimerAction(t, response), startTime.Add(maximumDuration))

	overflowRegistry := NewTaskRegistry()
	if err := overflowRegistry.AddOrchestratorN("overflow-timer", func(ctx *OrchestrationContext) (any, error) {
		err := ctx.CreateTimer(time.Hour).Await(nil)
		return errors.Is(err, api.ErrInvalidArgument), nil
	}); err != nil {
		t.Fatal(err)
	}
	overflowStart := time.Date(9999, time.December, 31, 23, 30, 0, 0, time.UTC)
	overflowID := api.InstanceID("overflow-timer-instance")
	overflow := executeTimerTurn(
		t,
		NewTaskExecutor(overflowRegistry),
		overflowID,
		nil,
		[]*protos.HistoryEvent{
			timerOrchestratorStartedAt(overflowStart),
			helpers.NewExecutionStartedEvent("overflow-timer", string(overflowID), nil, nil, nil, nil),
		},
	)
	if got, want := completionResult(t, overflow), "true"; got != want {
		t.Fatalf("overflow result = %s, want %s", got, want)
	}
	for _, action := range overflow.Actions {
		if action.GetCreateTimer() != nil {
			t.Fatalf("overflow scheduled timer action %v", action)
		}
	}
}

func TestMaximumTimerIntervalConfiguration(t *testing.T) {
	registry := NewTaskRegistry()
	if err := registry.AddOrchestratorN("default-timer", func(ctx *OrchestrationContext) (any, error) {
		return nil, ctx.CreateTimer(7 * 24 * time.Hour).Await(nil)
	}); err != nil {
		t.Fatal(err)
	}
	startTime := time.Date(2026, time.August, 27, 12, 0, 0, 0, time.UTC)
	instanceID := api.InstanceID("default-timer-instance")
	response := executeTimerTurn(
		t,
		NewTaskExecutor(registry, WithMaximumTimerInterval(0)),
		instanceID,
		nil,
		[]*protos.HistoryEvent{
			timerOrchestratorStartedAt(startTime),
			helpers.NewExecutionStartedEvent("default-timer", string(instanceID), nil, nil, nil, nil),
		},
	)
	requireTimerFireAt(t, onlyTimerAction(t, response), startTime.Add(DefaultMaximumTimerInterval))

	defer func() {
		if recovered := recover(); recovered == nil {
			t.Fatal("negative maximum timer interval did not panic")
		}
	}()
	NewTaskExecutor(registry, WithMaximumTimerInterval(-time.Second))
}

func TestLongTimerCancellationStopsFutureChunks(t *testing.T) {
	registry := NewTaskRegistry()
	if err := registry.AddOrchestratorN("cancel-long-timer", func(ctx *OrchestrationContext) (any, error) {
		child, cancel := ctx.WithCancel()
		timer := child.CreateTimer(7 * time.Hour)
		cancel()
		return errors.Is(timer.Await(nil), ErrTaskCanceled), nil
	}); err != nil {
		t.Fatal(err)
	}
	instanceID := api.InstanceID("cancel-long-timer-instance")
	response := executeTimerTurn(
		t,
		NewTaskExecutor(registry, WithMaximumTimerInterval(3*time.Hour)),
		instanceID,
		nil,
		[]*protos.HistoryEvent{
			timerOrchestratorStartedAt(time.Date(2026, time.August, 27, 12, 0, 0, 0, time.UTC)),
			helpers.NewExecutionStartedEvent("cancel-long-timer", string(instanceID), nil, nil, nil, nil),
		},
	)
	if got, want := completionResult(t, response), "true"; got != want {
		t.Fatalf("result = %s, want %s", got, want)
	}
	timerCount := 0
	for _, action := range response.Actions {
		if action.GetCreateTimer() != nil {
			timerCount++
		}
	}
	if timerCount != 1 {
		t.Fatalf("timer actions = %d, want 1", timerCount)
	}
}

func TestLongTimerHonorsHistoryLimits(t *testing.T) {
	registry := NewTaskRegistry()
	if err := registry.AddOrchestratorN("limited-long-timer", func(ctx *OrchestrationContext) (any, error) {
		return nil, ctx.CreateTimer(7 * time.Hour).Await(nil)
	}); err != nil {
		t.Fatal(err)
	}
	executor := NewTaskExecutor(
		registry,
		WithOrchestrationOptions(OrchestrationOptions{
			MaximumTimerInterval: 3 * time.Hour,
			MaxHistoryEvents:     4,
		}),
	)
	instanceID := api.InstanceID("limited-long-timer-instance")
	startTime := time.Date(2026, time.August, 27, 12, 0, 0, 0, time.UTC)
	started := timerOrchestratorStartedAt(startTime)
	executionStarted := helpers.NewExecutionStartedEvent(
		"limited-long-timer",
		string(instanceID),
		nil,
		nil,
		nil,
		nil,
	)
	first := executeTimerTurn(
		t,
		executor,
		instanceID,
		nil,
		[]*protos.HistoryEvent{started, executionStarted},
	)
	timer := onlyTimerAction(t, first)
	second := executeTimerTurn(
		t,
		executor,
		instanceID,
		[]*protos.HistoryEvent{
			started,
			executionStarted,
			helpers.NewTimerCreatedEvent(0, timer.GetFireAt()),
		},
		[]*protos.HistoryEvent{
			timerOrchestratorStartedAt(timer.GetFireAt().AsTime()),
			helpers.NewTimerFiredEvent(0, timer.GetFireAt(), nil),
		},
	)
	failure := completionAction(t, second).GetFailureDetails()
	if got, want := failure.GetErrorType(), string(api.ErrorTypeHistoryLimitExceeded); got != want {
		t.Fatalf("failure type = %q, want %q", got, want)
	}
}

func executeTimerTurn(
	t *testing.T,
	executor Executor,
	instanceID api.InstanceID,
	oldEvents []*protos.HistoryEvent,
	newEvents []*protos.HistoryEvent,
) *protos.OrchestratorResponse {
	t.Helper()
	result, err := executor.ExecuteOrchestrator(
		context.Background(),
		instanceID,
		oldEvents,
		newEvents, nil)

	if err != nil {
		t.Fatal(err)
	}
	return result.Response
}

func timerOrchestratorStartedAt(timestamp time.Time) *protos.HistoryEvent {
	event := helpers.NewOrchestratorStartedEvent()
	event.Timestamp = timestamppb.New(timestamp)
	return event
}

func onlyTimerAction(t *testing.T, response *protos.OrchestratorResponse) *protos.CreateTimerAction {
	t.Helper()
	var timer *protos.CreateTimerAction
	for _, action := range response.Actions {
		if candidate := action.GetCreateTimer(); candidate != nil {
			if timer != nil {
				t.Fatalf("multiple timer actions in one turn: %v", response.Actions)
			}
			timer = candidate
		}
	}
	if timer == nil {
		t.Fatalf("no timer action in response: %v", response.Actions)
	}
	return timer
}

func requireTimerFireAt(t *testing.T, timer *protos.CreateTimerAction, expected time.Time) {
	t.Helper()
	if actual := timer.GetFireAt().AsTime(); !actual.Equal(expected) {
		t.Fatalf("timer fire time = %v, want %v", actual, expected)
	}
}
