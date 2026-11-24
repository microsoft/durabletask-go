package inmemory

import (
	"context"
	"fmt"
	"math"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/microsoft/durabletask-go/internal/protos"
)

// maxMessageSize is the Durable Task Scheduler's 4 MiB gRPC receive limit,
// which the service enforces on requests itself.
const maxMessageSize = 4 << 20

// errorTypeActivityResponseTooLarge identifies an activity whose completion
// exceeded maxMessageSize.
const errorTypeActivityResponseTooLarge = "ActivityResponseTooLarge"

// selfLimitedMethods check their request size themselves. They fail the work
// item instead of rejecting the call, because a worker abandons a work item
// whose completion is rejected and the service would dispatch it again forever.
var selfLimitedMethods = map[string]bool{
	protos.TaskHubSidecarService_CompleteActivityTask_FullMethodName: true,
}

// ServerOptions returns the gRPC server options for the service. The transport
// accepts large messages so that oversized completions reach the service, which
// enforces the 4 MiB request limit itself.
func ServerOptions() []grpc.ServerOption {
	return []grpc.ServerOption{
		grpc.MaxRecvMsgSize(math.MaxInt32),
		grpc.UnaryInterceptor(limitRequestSize),
	}
}

func limitRequestSize(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
	if msg, ok := req.(proto.Message); ok && !selfLimitedMethods[info.FullMethod] {
		if size := proto.Size(msg); size > maxMessageSize {
			return nil, status.Errorf(codes.ResourceExhausted,
				"grpc: received message larger than max (%d vs. %d)", size, maxMessageSize)
		}
	}
	return handler(ctx, req)
}

// oversizedCompletionFailure returns a non-retriable failure with the given
// error type when a worker's completion exceeds maxMessageSize, or nil when it
// fits. kind describes the completion in the failure message.
func oversizedCompletionFailure(errorType, kind string, completion proto.Message) *protos.TaskFailureDetails {
	size := proto.Size(completion)
	if size <= maxMessageSize {
		return nil
	}
	return &protos.TaskFailureDetails{
		ErrorType: errorType,
		ErrorMessage: fmt.Sprintf(
			"the %d-byte %s exceeds the %d-byte gRPC message limit of the Durable Task service",
			size, kind, maxMessageSize),
		IsNonRetriable: true,
	}
}
