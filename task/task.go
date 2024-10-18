package task

import (
	"errors"
	"fmt"

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
	orchestrationCtx  *OrchestrationContext
	isCompleted       bool
	isCanceled        bool
	rawResult         []byte
	failureDetails    *protos.TaskFailureDetails
	completedCallback func()
}

func newTask(ctx *OrchestrationContext) *completableTask {
	return &completableTask{
		orchestrationCtx: ctx,
	}
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
			if t.failureDetails != nil {
				return fmt.Errorf("task failed with an error: %v", t.failureDetails.ErrorMessage)
			} else if t.isCanceled {
				return ErrTaskCanceled
			}
			if v != nil && len(t.rawResult) > 0 {
				if err := unmarshalData(t.rawResult, v); err != nil {
					return fmt.Errorf("failed to decode task result: %w", err)
				}
			}
			return nil
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
	t.completedCallback = callback
}

func (t *completableTask) complete(rawResult []byte) {
	t.rawResult = rawResult
	t.completeInternal()
}

func (t *completableTask) fail(fd *protos.TaskFailureDetails) {
	t.failureDetails = fd
	t.completeInternal()
}

func (t *completableTask) cancel() {
	t.isCanceled = true
	t.completeInternal()
}

func (t *completableTask) completeInternal() {
	t.isCompleted = true
	if t.completedCallback != nil {
		t.completedCallback()
	}
}

type taskWrapper struct {
	delegate      Task
	onAwaitResult func(any, error) error
}

var _ Task = &taskWrapper{}

func (t *taskWrapper) Await(v any) error {
	return t.onAwaitResult(v, t.delegate.Await(v))
}
