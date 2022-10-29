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

// Task is an interface for asynchronous durable tasks. A task is conceptually similar to a future.
type Task interface {
	Await(v any) error
}

type completableTask struct {
	orchestrationCtx *OrchestrationContext
	isCompleted      bool
	rawResult        []byte
	failureDetails   *protos.TaskFailureDetails
}

func newTask(ctx *OrchestrationContext) *completableTask {
	return &completableTask{
		orchestrationCtx: ctx,
	}
}

// Await blocks the current orchestrator until the task is complete and then saves the unmarshalled
// result of the task (if any) into [v].
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

func (t *completableTask) complete(rawResult []byte) {
	t.isCompleted = true
	t.rawResult = rawResult
}

func (t *completableTask) fail(fd *protos.TaskFailureDetails) {
	t.isCompleted = true
	t.failureDetails = fd
}
