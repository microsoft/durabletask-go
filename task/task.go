package task

import (
	"errors"
	"fmt"
	"time"

	"github.com/microsoft/durabletask-go/api"
	"github.com/microsoft/durabletask-go/internal/failure"
	"github.com/microsoft/durabletask-go/internal/protos"
)

// ErrTaskBlocked is not an error, but rather a control flow signal indicating that an orchestrator
// function has executed as far as it can and that it now needs to unload, dispatch any scheduled tasks,
// and commit its current execution progress to durable storage.
var ErrTaskBlocked = errors.New("the current task is blocked")

// ErrTaskCanceled is used to indicate that a task was canceled. Tasks can be canceled, for example,
// when configured timeouts expire.
var ErrTaskCanceled = errors.New("the task was canceled") // CONSIDER: More specific info about the task

// Task is an interface for asynchronous durable tasks. A task is conceptually similar to a future.
type Task interface {
	Await(v any) error
}

type completableTask struct {
	orchestrationCtx   *OrchestrationContext
	isCompleted        bool
	isCanceled         bool
	rawResult          []byte
	failureDetails     *protos.TaskFailureDetails
	localErr           error
	taskName           string
	taskVersion        string
	taskID             int32
	timerFireAt        time.Time
	entityID           api.EntityID
	entityOperation    string
	completionID       uint64
	completedCallbacks []func()
	waiters            map[*coroutine]struct{}
	scope              *cancellationScope
	scopeIndex         int
}

func newTaskInScope(ctx *OrchestrationContext, scope *cancellationScope) *completableTask {
	task := &completableTask{
		orchestrationCtx: ctx,
		waiters:          make(map[*coroutine]struct{}),
		scope:            scope,
		scopeIndex:       -1,
	}
	if scope != nil {
		scope.addTask(task)
	}
	return task
}

// Await blocks the current orchestrator until the task is complete and then saves the unmarshalled
// result of the task (if any) into [v].
//
// Await will return ErrTaskCanceled if the task was canceled - e.g. due to a timeout.
//
// Await may panic with ErrTaskBlocked as the panic value if called on a task that has not yet completed.
// This is normal control flow behavior for orchestrator functions and doesn't actually indicate a failure
// of any kind. However, orchestrator functions must never attempt to recover from such panics to ensure that
// the orchestration execution can procede normally.
func (t *completableTask) Await(v any) error {
	for {
		if t.isCompleted {
			if t.localErr != nil {
				return t.localErr
			}
			if t.isCanceled {
				return ErrTaskCanceled
			}
			if t.failureDetails != nil {
				details := failure.FromProto(t.failureDetails)
				if t.entityOperation != "" {
					return &EntityOperationFailedError{
						EntityID:       t.entityID,
						OperationName:  t.entityOperation,
						FailureDetails: details,
					}
				}
				return &TaskFailedError{
					TaskName:       t.taskName,
					TaskVersion:    t.taskVersion,
					TaskID:         t.taskID,
					FailureDetails: details,
				}
			}
			if v != nil && len(t.rawResult) > 0 {
				if err := unmarshalData(t.orchestrationCtx.converter, t.rawResult, v); err != nil {
					return fmt.Errorf("failed to decode task result: %w", err)
				}
			}
			return nil
		}

		if scheduler := t.orchestrationCtx.scheduler; scheduler != nil {
			if current := scheduler.mustCurrent(); current.scope.isCanceled() {
				return ErrTaskCanceled
			}
			scheduler.waitForTask(t)
			continue
		}

		ok, err := t.orchestrationCtx.processNextEvent()
		if err != nil {
			// TODO: If there is an error here, we need some kind of well-known panic to kill the orchestration
			panic(err)
		}
		if !ok {
			break
		}
	}
	// TODO: Need a rule about using "defer" in orchestrations because planned panics will invoke them unexpectedly
	panic(ErrTaskBlocked)
}

func (t *completableTask) onCompleted(callback func()) {
	if t.isCompleted {
		callback()
		return
	}
	t.completedCallbacks = append(t.completedCallbacks, callback)
}

func (t *completableTask) complete(rawResult []byte) {
	if t.isCompleted {
		return
	}
	t.rawResult = rawResult
	t.completeInternal()
}

func (t *completableTask) fail(fd *protos.TaskFailureDetails) {
	if t.isCompleted {
		return
	}
	t.failureDetails = fd
	t.completeInternal()
}

func (t *completableTask) failLocal(err error) {
	if t.isCompleted {
		return
	}
	t.localErr = err
	t.completeInternal()
}

func (t *completableTask) cancel() {
	if t.isCompleted {
		return
	}
	t.isCanceled = true
	t.completeInternal()
}

func (t *completableTask) completeInternal() {
	if t.isCompleted {
		return
	}
	t.isCompleted = true
	t.scope.removeTask(t)
	if scheduler := t.orchestrationCtx.scheduler; scheduler != nil {
		t.completionID = scheduler.nextCompletionID()
		for waiter := range t.waiters {
			scheduler.makeRunnable(waiter)
		}
		clear(t.waiters)
	}
	for _, callback := range t.completedCallbacks {
		callback()
	}
	t.completedCallbacks = nil
}

func (t *completableTask) addWaiter(c *coroutine) {
	if !t.isCompleted {
		t.waiters[c] = struct{}{}
	}
}

func (t *completableTask) removeWaiter(c *coroutine) {
	delete(t.waiters, c)
}
