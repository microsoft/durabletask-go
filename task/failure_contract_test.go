package task

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/microsoft/durabletask-go/api"
	"github.com/microsoft/durabletask-go/internal/helpers"
	"github.com/microsoft/durabletask-go/internal/protos"
	"github.com/stretchr/testify/require"
)

func TestDurableFailureMessagesMatchDotNet(t *testing.T) {
	details := &api.FailureDetails{ErrorType: "Example.Error", ErrorMessage: "boom"}

	taskErr := &TaskFailedError{TaskName: "ChargeCard", TaskID: 3, FailureDetails: details}
	if got, want := taskErr.Error(), "Task 'ChargeCard' (#3) failed with an unhandled exception: boom"; got != want {
		t.Fatalf("TaskFailedError.Error() = %q, want %q", got, want)
	}

	entityID := api.NewEntityID("account", "123")
	entityErr := &EntityOperationFailedError{
		EntityID:       entityID,
		OperationName:  "withdraw",
		FailureDetails: details,
	}
	want := "Operation 'withdraw' of entity '@account@123' failed: boom"
	if got := entityErr.Error(); got != want {
		t.Fatalf("EntityOperationFailedError.Error() = %q, want %q", got, want)
	}
}

func TestDurableFailuresMatchWireCategories(t *testing.T) {
	notFound := &TaskFailedError{FailureDetails: &api.FailureDetails{
		ErrorType:      activityTaskNotFoundErrorType,
		ErrorMessage:   "missing",
		IsNonRetriable: true,
	}}
	if !errors.Is(notFound, api.ErrTaskNotRegistered) {
		t.Fatal("task failure does not match ErrTaskNotRegistered")
	}
	if !notFound.NonRetriable() {
		t.Fatal("task failure should be non-retriable")
	}

	version := &TaskFailedError{FailureDetails: &api.FailureDetails{ErrorType: versionMismatchErrorType}}
	if !errors.Is(version, api.ErrVersionMismatch) {
		t.Fatal("task failure does not match ErrVersionMismatch")
	}
}

func TestLocalTaskErrorsAreNotWrappedAsDurableFailures(t *testing.T) {
	ctx := &OrchestrationContext{scope: newCancellationScope(nil)}
	task := newTaskInScope(ctx, ctx.scope)
	task.failLocal(api.WrapInvalidArgument(errors.New("bad option")))

	err := task.Await(nil)
	if !errors.Is(err, api.ErrInvalidArgument) {
		t.Fatalf("Await() error = %v", err)
	}
	var taskFailure *TaskFailedError
	if errors.As(err, &taskFailure) {
		t.Fatal("local validation error was wrapped as TaskFailedError")
	}
}

func TestRetryBypassesHandlerForCancellationAndNonRetriableFailure(t *testing.T) {
	handlerCalls := 0
	policy := RetryPolicy{
		MaxAttempts:          3,
		InitialRetryInterval: time.Second,
		BackoffCoefficient:   1,
		MaxRetryInterval:     time.Second,
		RetryTimeout:         time.Minute,
		Handle: func(RetryContext) bool {
			handlerCalls++
			return true
		},
	}
	now := time.Now()
	nonRetriable := &TaskFailedError{FailureDetails: &api.FailureDetails{
		ErrorType:      activityTaskNotFoundErrorType,
		ErrorMessage:   "missing",
		IsNonRetriable: true,
	}}

	if delay := computeNextDelay(now, policy, 0, now, nonRetriable); delay != 0 {
		t.Fatalf("non-retriable delay = %v", delay)
	}
	if delay := computeNextDelay(now, policy, 0, now, ErrTaskCanceled); delay != 0 {
		t.Fatalf("canceled delay = %v", delay)
	}
	if handlerCalls != 0 {
		t.Fatalf("handler calls = %d", handlerCalls)
	}
}

func TestMissingActivityUsesCanonicalNonRetriableFailure(t *testing.T) {
	executor := NewTaskExecutor(NewTaskRegistry())
	event := helpers.NewTaskScheduledEvent(4, "missing", nil, nil, nil)

	result, err := executor.ExecuteActivity(context.Background(), "instance", event)
	if err != nil {
		t.Fatal(err)
	}
	details := result.GetTaskFailed().GetFailureDetails()
	if details.GetErrorType() != string(activityTaskNotFoundErrorType) {
		t.Fatalf("ErrorType = %q", details.GetErrorType())
	}
	if !details.GetIsNonRetriable() {
		t.Fatal("missing activity failure should be non-retriable")
	}
}

func TestMissingOrchestratorRejectsWorkItem(t *testing.T) {
	executor := NewTaskExecutor(
		NewTaskRegistry(),
		WithOrchestratorNotFoundStrategy(OrchestratorNotFoundReject),
	)
	events := []*protos.HistoryEvent{
		helpers.NewExecutionStartedEvent("missing", "instance", nil, nil, nil, nil),
	}

	_, err := executor.ExecuteOrchestrator(context.Background(), "instance", nil, events, nil)
	if !errors.Is(err, api.ErrTaskNotRegistered) {
		t.Fatalf("ExecuteOrchestrator() error = %v", err)
	}
}

func TestMissingOrchestratorFailsByDefault(t *testing.T) {
	executor := NewTaskExecutor(NewTaskRegistry())
	events := []*protos.HistoryEvent{
		helpers.NewExecutionStartedEvent("missing", "instance", nil, nil, nil, nil),
	}

	result, err := executor.ExecuteOrchestrator(context.Background(), "instance", nil, events, nil)
	if err != nil {
		t.Fatal(err)
	}
	completed := result.Response.Actions[0].GetCompleteOrchestration()
	if completed.GetFailureDetails().GetErrorType() != string(orchestratorTaskNotFoundErrorType) {
		t.Fatalf("failure = %#v", completed.GetFailureDetails())
	}
	if !completed.GetFailureDetails().GetIsNonRetriable() {
		t.Fatal("missing orchestrator failure should be non-retriable")
	}
}

func TestErrorPropertiesProviderEnrichesActivityFailure(t *testing.T) {
	registry := NewTaskRegistry()
	if err := registry.AddActivityN("failing", func(ActivityContext) (any, error) {
		return nil, errors.New("boom")
	}); err != nil {
		t.Fatal(err)
	}
	executor := NewTaskExecutor(registry, WithErrorPropertiesProvider(
		api.ErrorPropertiesProviderFunc(func(error) map[string]any {
			return map[string]any{"code": "E42"}
		}),
	))
	event := helpers.NewTaskScheduledEvent(1, "failing", nil, nil, nil)

	result, err := executor.ExecuteActivity(context.Background(), "instance", event)
	if err != nil {
		t.Fatal(err)
	}
	if got := result.GetTaskFailed().GetFailureDetails().GetProperties()["code"].GetStringValue(); got != "E42" {
		t.Fatalf("code property = %q", got)
	}
}

type hostileActivityPanic struct{}

func (hostileActivityPanic) Error() string { return strings.Repeat("panic detail ", 4096) }
func (hostileActivityPanic) Unwrap() error { panic("panic cause must not be traversed") }

func TestActivityPanicRecoveryDoesNotReenterApplicationCode(t *testing.T) {
	for _, test := range []struct {
		name          string
		activity      Activity
		providerCalls int
	}{
		{
			name: "error properties",
			activity: func(ActivityContext) (any, error) {
				return nil, errors.New("activity failed")
			},
			providerCalls: 1,
		},
		{
			name: "serialization error properties",
			activity: func(ActivityContext) (any, error) {
				return make(chan int), nil
			},
			providerCalls: 1,
		},
		{
			name: "activity panic cause",
			activity: func(ActivityContext) (any, error) {
				panic(hostileActivityPanic{})
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			registry := NewTaskRegistry()
			require.NoError(t, registry.AddActivityN("failing", test.activity))
			calls := 0
			executor := NewTaskExecutor(registry, WithErrorPropertiesProvider(
				api.ErrorPropertiesProviderFunc(func(error) map[string]any {
					calls++
					panic(hostileActivityPanic{})
				}),
			))
			var response *protos.HistoryEvent
			var err error
			require.NotPanics(t, func() {
				response, err = executor.ExecuteActivity(context.Background(), "instance",
					helpers.NewTaskScheduledEvent(7, "failing", nil, nil, nil))
			})
			require.NoError(t, err)
			require.Equal(t, test.providerCalls, calls)
			failed := response.GetTaskFailed()
			require.NotNil(t, failed)
			require.EqualValues(t, 7, failed.TaskScheduledId)
			details := failed.GetFailureDetails()
			require.Equal(t, string(api.ErrorTypeActivityPanic), details.GetErrorType())
			require.Contains(t, details.GetErrorMessage(), "panic: panic detail")
			require.LessOrEqual(t, len(details.GetErrorMessage()), 16*1024)
			require.NotEmpty(t, details.GetStackTrace().GetValue())
			require.LessOrEqual(t, len(details.GetStackTrace().GetValue()), 16*1024)
			require.Nil(t, details.GetInnerFailure())
			require.Empty(t, details.GetProperties())
			require.False(t, details.GetIsNonRetriable())
		})
	}
}
