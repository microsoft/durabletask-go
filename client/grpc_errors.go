package client

import (
	"context"
	"fmt"
	"strings"

	"github.com/microsoft/durabletask-go/api"
	"github.com/microsoft/durabletask-go/internal/grpcerrors"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type grpcRequestError struct {
	operation string
	category  error
	status    *status.Status
}

func (e *grpcRequestError) Error() string {
	message := e.status.Message()
	if e.category == nil {
		return fmt.Sprintf("%s: %s", e.operation, message)
	}
	categoryMessage := e.category.Error()
	message = strings.TrimPrefix(message, categoryMessage+": ")
	if message == "" || message == categoryMessage {
		return fmt.Sprintf("%s: %s", e.operation, categoryMessage)
	}
	return fmt.Sprintf("%s: %s: %s", e.operation, categoryMessage, message)
}

func (e *grpcRequestError) Unwrap() []error {
	if e.category == nil {
		return []error{e.status.Err()}
	}
	return []error{e.category, e.status.Err()}
}

func (e *grpcRequestError) GRPCStatus() *status.Status {
	return e.status
}

func clientRPCError(ctx context.Context, operation string, err error) error {
	if err == nil {
		return nil
	}
	var category error
	grpcStatus := status.Convert(err)
	code := grpcStatus.Code()
	if ctxErr := ctx.Err(); ctxErr != nil &&
		(code == codes.Canceled || code == codes.DeadlineExceeded || code == codes.Unknown) {
		category = ctxErr
	} else if reasonCategory := clientErrorReasonCategory(grpcerrors.Reason(grpcStatus)); reasonCategory != nil {
		category = reasonCategory
	} else {
		switch code {
		case codes.AlreadyExists:
			if operation == "failed to create task hub" {
				category = ErrTaskHubExists
			} else {
				category = api.ErrDuplicateInstance
			}
		case codes.InvalidArgument:
			category = api.ErrInvalidArgument
		case codes.NotFound:
			if operation == "failed to delete task hub" {
				category = ErrTaskHubNotFound
			} else {
				category = api.ErrInstanceNotFound
			}
		case codes.FailedPrecondition:
			if strings.HasPrefix(grpcStatus.Message(), api.ErrNotCompleted.Error()) {
				category = api.ErrNotCompleted
			} else {
				category = api.ErrInvalidState
			}
		case codes.Unimplemented:
			category = api.ErrFeatureNotSupported
		case codes.Canceled:
			category = context.Canceled
		case codes.DeadlineExceeded:
			category = context.DeadlineExceeded
		}
	}
	return &grpcRequestError{
		operation: operation,
		category:  category,
		status:    grpcStatus,
	}
}

func clientErrorReasonCategory(reason string) error {
	switch reason {
	case grpcerrors.ReasonDuplicateInstance:
		return api.ErrDuplicateInstance
	case grpcerrors.ReasonFeatureUnsupported:
		return api.ErrFeatureNotSupported
	case grpcerrors.ReasonInstanceNotFound:
		return api.ErrInstanceNotFound
	case grpcerrors.ReasonInvalidArgument:
		return api.ErrInvalidArgument
	case grpcerrors.ReasonInvalidState:
		return api.ErrInvalidState
	case grpcerrors.ReasonNotCompleted:
		return api.ErrNotCompleted
	case grpcerrors.ReasonTaskHubExists:
		return ErrTaskHubExists
	case grpcerrors.ReasonTaskHubNotFound:
		return ErrTaskHubNotFound
	default:
		return nil
	}
}

func retryableWaitRPCError(err error) bool {
	switch status.Code(err) {
	case codes.Canceled,
		codes.DeadlineExceeded,
		codes.ResourceExhausted,
		codes.Aborted,
		codes.Internal,
		codes.Unavailable,
		codes.Unknown:
		return true
	default:
		return false
	}
}
