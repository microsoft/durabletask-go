package api

import (
	"encoding/json"
	"errors"
	"fmt"
)

// ErrorType is a stable cross-language durable failure identifier.
type ErrorType string

const (
	ErrorTypeActivityTaskNotFound         ErrorType = "ActivityTaskNotFound"
	ErrorTypeEntityTaskNotFound           ErrorType = "EntityTaskNotFound"
	ErrorTypeOrchestratorTaskNotFound     ErrorType = "OrchestratorTaskNotFound"
	ErrorTypeTaskFailed                   ErrorType = "TaskFailedException"
	ErrorTypeEntityOperationFailed        ErrorType = "EntityOperationFailedException"
	ErrorTypeVersionMismatch              ErrorType = "VersionMismatch"
	ErrorTypeVersionError                 ErrorType = "VersionError"
	ErrorTypeHistoryLimitExceeded         ErrorType = "HistoryLimitExceeded"
	ErrorTypeOrchestratorResponseTooLarge ErrorType = "OrchestratorResponseTooLarge"
	ErrorTypeActivityPanic                ErrorType = "TaskActivityPanic"
	ErrorTypeEntityOperationPanic         ErrorType = "EntityOperationPanic"
	ErrorTypeOrchestratorPanic            ErrorType = "OrchestratorPanic"
)

// MaxFailureDetailsDepth bounds failure chains in JSON and protobuf payloads.
const MaxFailureDetailsDepth = 32

var (
	ErrInvalidArgument   = errors.New("invalid argument")
	ErrTaskNotRegistered = errors.New("task is not registered")
	ErrVersionMismatch   = errors.New("task version is incompatible with this worker")
)

func invalidArgument(message string) error {
	return WrapInvalidArgument(errors.New(message))
}

// WrapInvalidArgument classifies an argument validation error without losing its cause.
func WrapInvalidArgument(err error) error {
	if err == nil || errors.Is(err, ErrInvalidArgument) {
		return err
	}
	return &invalidArgumentError{cause: err}
}

type invalidArgumentError struct {
	cause error
}

func (e *invalidArgumentError) Error() string {
	return fmt.Sprintf("%s: %v", ErrInvalidArgument, e.cause)
}

func (e *invalidArgumentError) Unwrap() error {
	return e.cause
}

func (*invalidArgumentError) Is(target error) bool {
	return target == ErrInvalidArgument
}

// FailureDetails describes a durable failure that can cross process and language boundaries.
type FailureDetails struct {
	ErrorType      ErrorType       `json:"type"`
	ErrorMessage   string          `json:"message"`
	StackTrace     string          `json:"stackTrace,omitempty"`
	InnerFailure   *FailureDetails `json:"innerFailure,omitempty"`
	IsNonRetriable bool            `json:"isNonRetriable,omitempty"`
	Properties     map[string]any  `json:"properties,omitempty"`
}

func (d *FailureDetails) String() string {
	if d == nil {
		return ""
	}
	if d.ErrorType == "" {
		return d.ErrorMessage
	}
	return fmt.Sprintf("%s: %s", d.ErrorType, d.ErrorMessage)
}

// NonRetriable reports whether this failure frame must not be retried.
func (d *FailureDetails) NonRetriable() bool {
	return d != nil && d.IsNonRetriable
}

// IsCausedBy reports whether this failure chain contains one of the supplied wire error types.
func (d *FailureDetails) IsCausedBy(errorTypes ...ErrorType) bool {
	for current := d; current != nil; current = current.InnerFailure {
		for _, errorType := range errorTypes {
			if current.ErrorType == errorType {
				return true
			}
		}
	}
	return false
}

// Matches reports whether this failure chain belongs to a stable Go error category.
func (d *FailureDetails) Matches(target error) bool {
	switch {
	case errors.Is(target, ErrTaskNotRegistered):
		return d.IsCausedBy(
			ErrorTypeActivityTaskNotFound,
			ErrorTypeEntityTaskNotFound,
			ErrorTypeOrchestratorTaskNotFound,
		)
	case errors.Is(target, ErrVersionMismatch):
		return d.IsCausedBy(ErrorTypeVersionMismatch, ErrorTypeVersionError)
	default:
		return false
	}
}

func (d *FailureDetails) MarshalJSON() ([]byte, error) {
	if err := d.validate(); err != nil {
		return nil, err
	}
	type alias FailureDetails
	return json.Marshal((*alias)(d))
}

func (d *FailureDetails) UnmarshalJSON(data []byte) error {
	type alias FailureDetails
	var decoded alias
	if err := json.Unmarshal(data, &decoded); err != nil {
		return err
	}
	*d = FailureDetails(decoded)
	return d.validate()
}

func (d *FailureDetails) validate() error {
	visited := make(map[*FailureDetails]struct{})
	for depth, current := 0, d; current != nil; depth, current = depth+1, current.InnerFailure {
		if depth >= MaxFailureDetailsDepth {
			return fmt.Errorf("failure details exceed the maximum depth of %d", MaxFailureDetailsDepth)
		}
		if _, ok := visited[current]; ok {
			return errors.New("failure details contain a cycle")
		}
		visited[current] = struct{}{}
	}
	return nil
}

// DurableTaskErrorTypeProvider supplies a stable cross-language error type.
type DurableTaskErrorTypeProvider interface {
	DurableTaskErrorType() ErrorType
}

// DurableTaskFailureDetailsProvider supplies failure details already received from durable history.
type DurableTaskFailureDetailsProvider interface {
	DurableTaskFailureDetails() *FailureDetails
}

// DurableTaskErrorPropertiesProvider supplies additional serializable failure properties.
type DurableTaskErrorPropertiesProvider interface {
	DurableTaskErrorProperties() map[string]any
}

// ErrorPropertiesProvider enriches failures without requiring application errors to implement an interface.
type ErrorPropertiesProvider interface {
	ErrorProperties(error) map[string]any
}

// ErrorPropertiesProviderFunc adapts a function into an ErrorPropertiesProvider.
type ErrorPropertiesProviderFunc func(error) map[string]any

func (provider ErrorPropertiesProviderFunc) ErrorProperties(err error) map[string]any {
	return provider(err)
}

// DurableTaskStackTraceProvider supplies a stack trace for a failure.
type DurableTaskStackTraceProvider interface {
	DurableTaskStackTrace() string
}

// NonRetriable marks an error that must bypass retry handlers.
type NonRetriable interface {
	error
	NonRetriable() bool
}
