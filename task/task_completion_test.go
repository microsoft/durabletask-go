package task

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/microsoft/durabletask-go/internal/helpers"
	"github.com/microsoft/durabletask-go/internal/protos"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

func TestCompletedTaskIgnoresLaterCancellationFailureAndCompletion(t *testing.T) {
	ctx := newTestOrchestrationContext(NewTaskRegistry(), "completed-task", nil, nil)
	scope := newCancellationScope(nil)
	task := newTaskInScope(ctx, scope)
	task.complete([]byte(`"first"`))

	scope.cancel(nil)
	task.fail(&protos.TaskFailureDetails{ErrorMessage: "late failure"})
	task.complete([]byte(`"second"`))

	var result string
	if err := task.Await(&result); err != nil {
		t.Fatal(err)
	}
	if result != "first" {
		t.Fatalf("result = %q, want first", result)
	}
}

func TestIsCompletedOutcomes(t *testing.T) {
	localErr := errors.New("local failure")
	for _, test := range []struct {
		name     string
		finish   func(*completableTask)
		checkErr func(error) bool
		want     string
	}{
		{
			name:   "success",
			finish: func(task *completableTask) { task.complete([]byte(`"done"`)) },
			checkErr: func(err error) bool {
				return err == nil
			},
			want: "done",
		},
		{
			name: "failure",
			finish: func(task *completableTask) {
				task.fail(&protos.TaskFailureDetails{ErrorMessage: "failed"})
			},
			checkErr: func(err error) bool {
				var failed *TaskFailedError
				return errors.As(err, &failed)
			},
		},
		{
			name:     "local failure",
			finish:   func(task *completableTask) { task.failLocal(localErr) },
			checkErr: func(err error) bool { return errors.Is(err, localErr) },
		},
		{
			name:     "cancellation",
			finish:   func(task *completableTask) { task.cancel() },
			checkErr: func(err error) bool { return errors.Is(err, ErrTaskCanceled) },
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx := newTestOrchestrationContext(NewTaskRegistry(), "completion-check", nil, nil)
			ctx.scheduler = newCoroutineScheduler(ctx)
			ctx.scheduler.current = &coroutine{}
			task := newTaskInScope(ctx, ctx.scope)
			for i := 0; i < 2; i++ {
				if IsCompleted(task) {
					t.Fatal("pending task reported complete")
				}
			}
			test.finish(task)
			for i := 0; i < 2; i++ {
				if !IsCompleted(task) {
					t.Fatal("finished task reported incomplete")
				}
				var result string
				if err := task.Await(&result); !test.checkErr(err) {
					t.Fatalf("Await() error = %v", err)
				}
				if result != test.want {
					t.Fatalf("result = %q, want %q", result, test.want)
				}
			}
		})
	}
}

type customCompletionTask struct{}

func (customCompletionTask) Await(any) error   { return nil }
func (customCompletionTask) IsCompleted() bool { return true }

func TestIsCompletedRejectsUnsupportedTasks(t *testing.T) {
	for _, test := range []struct {
		name string
		task Task
	}{
		{name: "nil"},
		{name: "typed nil", task: (*completableTask)(nil)},
		{name: "custom", task: customCompletionTask{}},
	} {
		t.Run(test.name, func(t *testing.T) {
			defer func() {
				if value := recover(); value == nil || !strings.Contains(fmt.Sprint(value), "cannot be checked for completion") {
					t.Fatalf("panic = %v, want unsupported-task message", value)
				}
			}()
			IsCompleted(test.task)
		})
	}
}

func TestIsCompletedRequiresOrchestratorExecution(t *testing.T) {
	ctx := newTestOrchestrationContext(NewTaskRegistry(), "completion-check", nil, nil)
	task := newTaskInScope(ctx, ctx.scope)
	task.complete([]byte(`"done"`))
	for _, test := range []struct {
		name  string
		setup func()
		want  string
	}{
		{name: "no scheduler", setup: func() {}, want: "outside orchestrator execution"},
		{
			name:  "no current coroutine",
			setup: func() { ctx.scheduler = newCoroutineScheduler(ctx) },
			want:  "outside an orchestration coroutine",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			test.setup()
			defer func() {
				if value := recover(); value == nil || !strings.Contains(fmt.Sprint(value), test.want) {
					t.Fatalf("panic = %v, want %q", value, test.want)
				}
			}()
			IsCompleted(task)
		})
	}
}

func TestIsCompletedAtReplayPosition(t *testing.T) {
	registry := NewTaskRegistry()
	if err := registry.AddOrchestratorN("completion-check", func(ctx *OrchestrationContext) (any, error) {
		first := ctx.CallActivity("first")
		second := ctx.CallActivity("second")
		before := IsCompleted(first) || IsCompleted(second)
		if ctx.WhenAny(first, second) != first {
			return nil, errors.New("unexpected first task")
		}
		firstReady := IsCompleted(first)
		firstReadyAgain := IsCompleted(first)
		if firstReady != firstReadyAgain {
			return nil, errors.New("first task completion changed between reads")
		}
		secondBeforeYield := IsCompleted(second)
		var firstResult string
		if err := first.Await(&firstResult); err != nil {
			return nil, err
		}
		secondAfterFirstAwait := IsCompleted(second)
		var secondResult string
		if err := second.Await(&secondResult); err != nil {
			return nil, err
		}
		return []any{before, firstReady, secondBeforeYield, secondAfterFirstAwait, IsCompleted(second), firstResult, secondResult}, nil
	}); err != nil {
		t.Fatal(err)
	}

	history := []*protos.HistoryEvent{
		helpers.NewOrchestratorStartedEvent(),
		helpers.NewExecutionStartedEvent("completion-check", "completion-replay", nil, nil, nil, nil),
		helpers.NewTaskScheduledEvent(0, "first", nil, nil, nil),
		helpers.NewTaskScheduledEvent(1, "second", nil, nil, nil),
		helpers.NewTaskCompletedEvent(0, wrapperspb.String(`"one"`)),
		helpers.NewTaskCompletedEvent(1, wrapperspb.String(`"two"`)),
	}
	for _, test := range []struct {
		name string
		old  []*protos.HistoryEvent
		new  []*protos.HistoryEvent
	}{
		{name: "replayed", old: history},
		{name: "new events", old: history[:4], new: history[4:]},
	} {
		t.Run(test.name, func(t *testing.T) {
			response := executeOrchestrationTurn(t, registry, "completion-replay", test.old, test.new)
			if got, want := completionResult(t, response), `[false,true,false,false,true,"one","two"]`; got != want {
				t.Fatalf("result = %s, want %s", got, want)
			}
		})
	}
}

func TestIsCompletedForFailedAndCanceledTasksInOrchestration(t *testing.T) {
	registry := NewTaskRegistry()
	if err := registry.AddOrchestratorN("completion-outcomes", func(ctx *OrchestrationContext) (any, error) {
		failed := ctx.CallActivity("fails")
		child, cancel := ctx.WithCancel()
		canceled := child.WaitForSingleEvent("never", -1)
		cancel()
		before := IsCompleted(failed) || IsCompleted(canceled)
		err := failed.Await(nil)
		var taskErr *TaskFailedError
		failureObserved := errors.As(err, &taskErr)
		cancelObserved := errors.Is(canceled.Await(nil), ErrTaskCanceled)
		return []bool{before, IsCompleted(failed), IsCompleted(canceled), failureObserved, cancelObserved}, nil
	}); err != nil {
		t.Fatal(err)
	}
	response := executeOrchestrationTurn(t, registry, "completion-outcomes", nil, []*protos.HistoryEvent{
		helpers.NewOrchestratorStartedEvent(),
		helpers.NewExecutionStartedEvent("completion-outcomes", "completion-outcomes", nil, nil, nil, nil),
		helpers.NewTaskScheduledEvent(0, "fails", nil, nil, nil),
		helpers.NewTaskFailedEvent(0, &protos.TaskFailureDetails{ErrorMessage: "failed"}),
	})
	if got, want := completionResult(t, response), `[false,true,true,true,true]`; got != want {
		t.Fatalf("result = %s, want %s", got, want)
	}
}
