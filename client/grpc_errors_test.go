package client

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/microsoft/durabletask-go/api"
	"github.com/microsoft/durabletask-go/internal/grpcerrors"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestClientRPCErrorMappingsPreserveStatus(t *testing.T) {
	tests := []struct {
		code     codes.Code
		category error
	}{
		{codes.AlreadyExists, api.ErrDuplicateInstance},
		{codes.InvalidArgument, api.ErrInvalidArgument},
		{codes.NotFound, api.ErrInstanceNotFound},
		{codes.FailedPrecondition, api.ErrInvalidState},
		{codes.Unimplemented, api.ErrFeatureNotSupported},
		{codes.Canceled, context.Canceled},
		{codes.DeadlineExceeded, context.DeadlineExceeded},
	}

	for _, test := range tests {
		t.Run(test.code.String(), func(t *testing.T) {
			err := clientRPCError(context.Background(), "operation", status.Error(test.code, test.category.Error()))
			if !errors.Is(err, test.category) {
				t.Fatalf("error %v does not match %v", err, test.category)
			}
			if got := status.Code(err); got != test.code {
				t.Fatalf("status.Code() = %v, want %v", got, test.code)
			}
			if count := strings.Count(err.Error(), test.category.Error()); count != 1 {
				t.Fatalf("category appears %d times in %q", count, err)
			}
		})
	}
}

func TestClientRPCErrorPrefersCallerContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := clientRPCError(ctx, "operation", status.Error(codes.Canceled, "canceled"))
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v", err)
	}
	if status.Code(err) != codes.Canceled {
		t.Fatalf("status.Code() = %v", status.Code(err))
	}
}

func TestClientRPCErrorPreservesLifecycleAndStateCategories(t *testing.T) {
	tests := []struct {
		code     codes.Code
		message  string
		category error
	}{
		{codes.AlreadyExists, ErrTaskHubExists.Error(), ErrTaskHubExists},
		{codes.NotFound, ErrTaskHubNotFound.Error(), ErrTaskHubNotFound},
		{codes.FailedPrecondition, api.ErrNotCompleted.Error(), api.ErrNotCompleted},
	}
	for _, test := range tests {
		var reason string
		switch {
		case errors.Is(test.category, ErrTaskHubExists):
			reason = grpcerrors.ReasonTaskHubExists
		case errors.Is(test.category, ErrTaskHubNotFound):
			reason = grpcerrors.ReasonTaskHubNotFound
		case errors.Is(test.category, api.ErrNotCompleted):
			reason = grpcerrors.ReasonNotCompleted
		}
		err := clientRPCError(context.Background(), "operation", grpcerrors.New(test.code, test.message, reason))
		if !errors.Is(err, test.category) {
			t.Fatalf("error %v does not match %v", err, test.category)
		}
	}
}

func TestRetryableWaitRPCErrorIncludesServerLongPollAndTransientFailures(t *testing.T) {
	for _, code := range []codes.Code{
		codes.Canceled,
		codes.DeadlineExceeded,
		codes.ResourceExhausted,
		codes.Aborted,
		codes.Internal,
		codes.Unavailable,
		codes.Unknown,
	} {
		if !retryableWaitRPCError(status.Error(code, "transient")) {
			t.Fatalf("%v should be retryable", code)
		}
	}
	if retryableWaitRPCError(status.Error(codes.InvalidArgument, "invalid")) {
		t.Fatal("InvalidArgument should not be retryable")
	}
	if retryableWaitRPCError(status.Error(codes.NotFound, "missing instance")) {
		t.Fatal("NotFound should return ErrInstanceNotFound without retrying")
	}
}
