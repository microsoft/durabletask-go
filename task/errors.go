package task

import (
	"errors"
	"fmt"
	"time"

	"github.com/microsoft/durabletask-go/api"
)

const (
	activityTaskNotFoundErrorType     = api.ErrorTypeActivityTaskNotFound
	entityTaskNotFoundErrorType       = api.ErrorTypeEntityTaskNotFound
	orchestratorTaskNotFoundErrorType = api.ErrorTypeOrchestratorTaskNotFound
	taskFailedErrorType               = api.ErrorTypeTaskFailed
	entityOperationFailedErrorType    = api.ErrorTypeEntityOperationFailed
	versionMismatchErrorType          = api.ErrorTypeVersionMismatch
)

var errEntitiesUnsupported = errors.New("durable entities are not supported by the current backend configuration")

// TaskFailedError is returned when an activity or sub-orchestration fails.
type TaskFailedError struct {
	TaskName       string
	TaskVersion    string
	TaskID         int32
	FailureDetails *api.FailureDetails
}

func (e *TaskFailedError) Error() string {
	if e.FailureDetails == nil || e.FailureDetails.ErrorMessage == "" {
		return fmt.Sprintf("Task '%s' (#%d) failed with an unhandled exception.", e.TaskName, e.TaskID)
	}
	return fmt.Sprintf(
		"Task '%s' (#%d) failed with an unhandled exception: %s",
		e.TaskName,
		e.TaskID,
		e.FailureDetails.ErrorMessage,
	)
}

func (*TaskFailedError) DurableTaskErrorType() api.ErrorType {
	return taskFailedErrorType
}

func (e *TaskFailedError) DurableTaskFailureDetails() *api.FailureDetails {
	return e.FailureDetails
}

func (e *TaskFailedError) NonRetriable() bool {
	return e.FailureDetails.NonRetriable()
}

func (e *TaskFailedError) Is(target error) bool {
	return e.FailureDetails.Matches(target)
}

// EntityOperationFailedError is returned when a called entity operation fails.
type EntityOperationFailedError struct {
	EntityID       api.EntityID
	OperationName  string
	FailureDetails *api.FailureDetails
}

func (e *EntityOperationFailedError) Error() string {
	message := ""
	if e.FailureDetails != nil {
		message = e.FailureDetails.ErrorMessage
	}
	return fmt.Sprintf(
		"Operation '%s' of entity '%s' failed: %s",
		e.OperationName,
		e.EntityID,
		message,
	)
}

func (*EntityOperationFailedError) DurableTaskErrorType() api.ErrorType {
	return entityOperationFailedErrorType
}

func (e *EntityOperationFailedError) DurableTaskFailureDetails() *api.FailureDetails {
	return e.FailureDetails
}

func (e *EntityOperationFailedError) NonRetriable() bool {
	return e.FailureDetails.NonRetriable()
}

func (e *EntityOperationFailedError) Is(target error) bool {
	return e.FailureDetails.Matches(target)
}

func failureDetailsFromError(err error) *api.FailureDetails {
	var taskFailure *TaskFailedError
	if errors.As(err, &taskFailure) {
		return taskFailure.FailureDetails
	}
	var entityFailure *EntityOperationFailedError
	if errors.As(err, &entityFailure) {
		return entityFailure.FailureDetails
	}
	return nil
}

type taskNotRegisteredError struct {
	errorType api.ErrorType
	message   string
}

func newTaskNotRegisteredError(errorType api.ErrorType, name, version string) error {
	taskKind := "task"
	switch errorType {
	case activityTaskNotFoundErrorType:
		taskKind = "activity task"
	case entityTaskNotFoundErrorType:
		taskKind = "entity task"
	case orchestratorTaskNotFoundErrorType:
		taskKind = "orchestrator task"
	}
	message := fmt.Sprintf("No %s named '%s' was found.", taskKind, name)
	if version != "" && errorType != entityTaskNotFoundErrorType {
		message = fmt.Sprintf("No %s named '%s' with version '%s' was found.", taskKind, name, version)
	}
	return &taskNotRegisteredError{
		errorType: errorType,
		message:   message,
	}
}

func (e *taskNotRegisteredError) Error() string {
	return e.message
}

func (e *taskNotRegisteredError) DurableTaskErrorType() api.ErrorType {
	return e.errorType
}

func (*taskNotRegisteredError) NonRetriable() bool {
	return true
}

func (*taskNotRegisteredError) Is(target error) bool {
	return target == api.ErrTaskNotRegistered
}

func (e *taskNotRegisteredError) WorkItemAbandonDelay() time.Duration {
	return time.Second
}

type panicFailureError struct {
	errorType api.ErrorType
	message   string
	stack     string
	cause     error
}

func newPanicFailureError(errorType api.ErrorType, message, stack string, cause error) error {
	return &panicFailureError{
		errorType: errorType,
		message:   message,
		stack:     stack,
		cause:     cause,
	}
}

func (e *panicFailureError) Error() string {
	return e.message
}

func (e *panicFailureError) Unwrap() error {
	return e.cause
}

func (e *panicFailureError) DurableTaskErrorType() api.ErrorType {
	return e.errorType
}

func (e *panicFailureError) DurableTaskStackTrace() string {
	return e.stack
}

func panicCause(value any) error {
	err, _ := value.(error)
	return err
}

type workItemAbandonDelayProvider interface {
	WorkItemAbandonDelay() time.Duration
}

type unsupportedEntityParametersError struct {
	message string
}

func (e *unsupportedEntityParametersError) Error() string {
	return e.message
}

func (*unsupportedEntityParametersError) WorkItemAbandonDelay() time.Duration {
	return time.Second
}

var _ error = (*TaskFailedError)(nil)
var _ error = (*EntityOperationFailedError)(nil)
var _ api.DurableTaskErrorTypeProvider = (*TaskFailedError)(nil)
var _ api.DurableTaskFailureDetailsProvider = (*TaskFailedError)(nil)
var _ api.NonRetriable = (*TaskFailedError)(nil)
var _ api.DurableTaskErrorTypeProvider = (*EntityOperationFailedError)(nil)
var _ api.DurableTaskFailureDetailsProvider = (*EntityOperationFailedError)(nil)
var _ api.NonRetriable = (*EntityOperationFailedError)(nil)
var _ api.DurableTaskErrorTypeProvider = (*taskNotRegisteredError)(nil)
var _ api.NonRetriable = (*taskNotRegisteredError)(nil)
var _ workItemAbandonDelayProvider = (*taskNotRegisteredError)(nil)
var _ workItemAbandonDelayProvider = (*VersionMismatchError)(nil)
var _ workItemAbandonDelayProvider = (*unsupportedEntityParametersError)(nil)
var _ api.DurableTaskErrorTypeProvider = (*panicFailureError)(nil)
var _ api.DurableTaskStackTraceProvider = (*panicFailureError)(nil)
