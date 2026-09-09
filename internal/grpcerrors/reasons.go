package grpcerrors

import (
	"fmt"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	ReasonDuplicateInstance  = "DUPLICATE_INSTANCE"
	ReasonFeatureUnsupported = "FEATURE_UNSUPPORTED"
	ReasonInstanceNotFound   = "INSTANCE_NOT_FOUND"
	ReasonInvalidArgument    = "INVALID_ARGUMENT"
	ReasonInvalidState       = "INVALID_STATE"
	ReasonNotCompleted       = "NOT_COMPLETED"
	ReasonTaskHubExists      = "TASK_HUB_EXISTS"
	ReasonTaskHubNotFound    = "TASK_HUB_NOT_FOUND"
)

// New returns a gRPC status error with a machine-readable durable error reason.
func New(code codes.Code, message, reason string) error {
	result, err := status.New(code, message).WithDetails(&errdetails.ErrorInfo{Reason: reason})
	if err != nil {
		return status.Error(codes.Internal, fmt.Sprintf("failed to attach durable error reason %q: %v", reason, err))
	}
	return result.Err()
}

// Reason returns the durable error reason attached to a gRPC status.
func Reason(value *status.Status) string {
	for _, detail := range value.Details() {
		if info, ok := detail.(*errdetails.ErrorInfo); ok {
			return info.Reason
		}
	}
	return ""
}
