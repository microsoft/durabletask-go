package exporthistory

import (
	"errors"
	"fmt"

	"github.com/microsoft/durabletask-go/api"
)

var (
	// ErrJobNotFound identifies attempts to read a missing export job.
	ErrJobNotFound = errors.New("export job not found")
	// ErrJobInvalidTransition identifies an invalid export job state change.
	ErrJobInvalidTransition = errors.New("invalid export job state transition")
	// ErrValidation identifies invalid export job configuration or client input.
	ErrValidation = errors.New("invalid export job configuration")
	// ErrJobOperationFailed identifies a failed export job mutation.
	ErrJobOperationFailed = errors.New("export job operation failed")
)

// Stable cross-language error types so a failure raised inside the entity keeps
// its identity when a client reconstructs it from orchestration failure details.
const (
	validationErrorType        api.ErrorType = "ExportJobClientValidationException"
	invalidTransitionErrorType api.ErrorType = "ExportJobInvalidTransitionException"
	notFoundErrorType          api.ErrorType = "ExportJobNotFoundException"
	operationFailedErrorType   api.ErrorType = "ExportJobOperationFailed"
)

// ValidationError reports invalid export job options or configuration.
type ValidationError struct {
	JobID   string
	Message string
}

func (e *ValidationError) Error() string {
	if e.JobID == "" {
		return "export job validation failed: " + e.Message
	}
	return fmt.Sprintf("validation failed for export job %q: %s", e.JobID, e.Message)
}

func (e *ValidationError) Unwrap() error { return ErrValidation }

func (*ValidationError) DurableTaskErrorType() api.ErrorType { return validationErrorType }

func (e *ValidationError) DurableTaskErrorProperties() map[string]any {
	return map[string]any{"jobId": e.JobID, "message": e.Message}
}

// NotFoundError reports an export job that does not exist.
type NotFoundError struct{ JobID string }

func (e *NotFoundError) Error() string {
	return fmt.Sprintf("export history job with ID %q was not found", e.JobID)
}

func (e *NotFoundError) Unwrap() error { return ErrJobNotFound }

func (*NotFoundError) DurableTaskErrorType() api.ErrorType { return notFoundErrorType }

func (e *NotFoundError) DurableTaskErrorProperties() map[string]any {
	return map[string]any{"jobId": e.JobID}
}

// InvalidTransitionError reports a lifecycle transition the job does not allow.
type InvalidTransitionError struct {
	JobID     string
	From      ExportJobStatus
	To        ExportJobStatus
	Operation string
}

func (e *InvalidTransitionError) Error() string {
	return fmt.Sprintf(
		"invalid state transition attempted for export job %q: cannot transition from %s to %s during %s operation",
		e.JobID, e.From, e.To, e.Operation)
}

func (e *InvalidTransitionError) Unwrap() error { return ErrJobInvalidTransition }

func (*InvalidTransitionError) DurableTaskErrorType() api.ErrorType {
	return invalidTransitionErrorType
}

func (e *InvalidTransitionError) DurableTaskErrorProperties() map[string]any {
	return map[string]any{
		"jobId":     e.JobID,
		"from":      int(e.From),
		"to":        int(e.To),
		"operation": e.Operation,
	}
}

// OperationError reports an export job mutation orchestration that did not
// complete successfully and whose failure could not be mapped to a more
// specific typed error.
type OperationError struct {
	JobID          string
	Operation      string
	RuntimeStatus  api.OrchestrationStatus
	FailureDetails *api.FailureDetails
}

func (e *OperationError) Error() string {
	message := ""
	if e.FailureDetails != nil {
		message = e.FailureDetails.ErrorMessage
	}
	if message == "" {
		return fmt.Sprintf("export job %q operation %s failed with status %v",
			e.JobID, e.Operation, e.RuntimeStatus)
	}
	return fmt.Sprintf("export job %q operation %s failed: %s", e.JobID, e.Operation, message)
}

func (e *OperationError) Unwrap() error { return ErrJobOperationFailed }

func (*OperationError) DurableTaskErrorType() api.ErrorType { return operationFailedErrorType }

// isValidTransition reports whether operation may move a job from `from` to `to`.
// Create is deliberately permitted from every terminal status so a failed or
// completed job can be recreated in place, but not from Active, which would
// start a second orchestration for a job that is still running.
func isValidTransition(operation string, from, to ExportJobStatus) bool {
	switch operation {
	case createOperation:
		return to == ExportJobStatusActive &&
			(from == ExportJobStatusPending ||
				from == ExportJobStatusFailed ||
				from == ExportJobStatusCompleted)
	case markAsCompletedOperation:
		return from == ExportJobStatusActive && to == ExportJobStatusCompleted
	case markAsFailedOperation:
		return from == ExportJobStatusActive && to == ExportJobStatusFailed
	default:
		return false
	}
}
