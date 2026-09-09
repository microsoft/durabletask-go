package client

import (
	"errors"
	"fmt"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var (
	// ErrTaskHubExists indicates that task-hub creation targeted an existing hub.
	ErrTaskHubExists = errors.New("task hub already exists")
	// ErrTaskHubNotFound indicates that a task-hub operation targeted a missing hub.
	ErrTaskHubNotFound = errors.New("task hub not found")
	// ErrStreamedHistoryLimitExceeded indicates that a worker refused to retain
	// an orchestration history beyond its configured safety limit.
	ErrStreamedHistoryLimitExceeded = errors.New("streamed orchestration history limit exceeded")
)

type streamedHistoryLimitError struct {
	message string
}

func newStreamedHistoryLimitError(format string, args ...any) error {
	return &streamedHistoryLimitError{message: fmt.Sprintf(format, args...)}
}

func (e *streamedHistoryLimitError) Error() string {
	return e.message
}

func (*streamedHistoryLimitError) Unwrap() error {
	return ErrStreamedHistoryLimitExceeded
}

func (e *streamedHistoryLimitError) GRPCStatus() *status.Status {
	return status.New(codes.ResourceExhausted, e.message)
}
