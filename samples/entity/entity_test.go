package main

import (
	"context"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/microsoft/durabletask-go/api"
	"github.com/microsoft/durabletask-go/internal/protos"
	"github.com/microsoft/durabletask-go/samples/internal/dtssample"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

func TestEntityCleanupAfterScenarioDeadline(t *testing.T) {
	var terminated, waited, purged, deleted, read atomic.Int32
	workerStarted := make(chan struct{})
	var workerCtx context.Context
	server := grpc.NewServer(
		grpc.UnaryInterceptor(func(ctx context.Context, request any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
			switch request := request.(type) {
			case *emptypb.Empty:
				return &emptypb.Empty{}, nil
			case *protos.SignalEntityRequest:
				if request.Name != "delete" {
					<-ctx.Done()
					return nil, status.FromContextError(ctx.Err()).Err()
				}
				if purged.Load() != 3 {
					t.Error("entities were deleted before owned orchestrations were cleaned up")
				}
				<-workerStarted
				if workerCtx.Err() != nil {
					t.Error("worker stopped before entity delete signals")
				}
				deleted.Add(1)
				return &protos.SignalEntityResponse{}, nil
			case *protos.GetInstanceRequest:
				runtimeStatus := api.RUNTIME_STATUS_RUNNING
				if info.FullMethod == protos.TaskHubSidecarService_WaitForInstanceCompletion_FullMethodName {
					waited.Add(1)
					runtimeStatus = api.RUNTIME_STATUS_TERMINATED
				}
				return &protos.GetInstanceResponse{
					Exists: true,
					OrchestrationState: &protos.OrchestrationState{
						InstanceId: request.InstanceId, OrchestrationStatus: runtimeStatus,
					},
				}, nil
			case *protos.TerminateRequest:
				terminated.Add(1)
				return &protos.TerminateResponse{}, nil
			case *protos.PurgeInstancesRequest:
				if waited.Load() != terminated.Load() {
					t.Error("owned orchestration purged before termination completed")
				}
				purged.Add(1)
				return &protos.PurgeInstancesResponse{DeletedInstanceCount: 1}, nil
			case *protos.GetEntityRequest:
				if deleted.Load() != 6 {
					t.Error("entity delete wait began before all delete signals were submitted")
				}
				if read.Add(1) == 1 {
					return nil, status.Error(codes.InvalidArgument, "injected entity delete wait failure")
				}
				return &protos.GetEntityResponse{}, nil
			default:
				return handler(ctx, request)
			}
		}),
		grpc.StreamInterceptor(func(_ any, stream grpc.ServerStream, _ *grpc.StreamServerInfo, _ grpc.StreamHandler) error {
			workerCtx = stream.Context()
			close(workerStarted)
			<-stream.Context().Done()
			return nil
		}),
	)
	protos.RegisterTaskHubSidecarServiceServer(server, &protos.UnimplementedTaskHubSidecarServiceServer{})
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(server.Stop)
	go func() { _ = server.Serve(listener) }()
	t.Setenv(dtssample.ConnectionStringVariable, "Endpoint=http://"+listener.Addr().String()+";TaskHub=test;Authentication=None")

	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	err = run(ctx)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.ErrorContains(t, err, "injected entity delete wait failure")
	require.EqualValues(t, 3, terminated.Load())
	require.EqualValues(t, 3, waited.Load())
	require.EqualValues(t, 3, purged.Load())
	require.EqualValues(t, 6, deleted.Load())
	require.EqualValues(t, 6, read.Load())
}
