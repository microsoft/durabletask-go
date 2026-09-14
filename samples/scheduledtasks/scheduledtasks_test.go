package main

import (
	"context"
	"encoding/json"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/microsoft/durabletask-go/api"
	"github.com/microsoft/durabletask-go/durabletaskscheduler"
	"github.com/microsoft/durabletask-go/internal/protos"
	"github.com/microsoft/durabletask-go/samples/internal/dtssample"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

func TestAcceptedScheduleIsDeletedAfterScenarioDeadline(t *testing.T) {
	for _, test := range []struct {
		name        string
		deleteFails bool
		lookupFails bool
	}{
		{name: "late creation is deleted"},
		{name: "delete failure is preserved", deleteFails: true},
		{name: "unknown creation is reported", lookupFails: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			var mu sync.Mutex
			var operations []durabletaskscheduler.ScheduleOperationRequest
			var createApplied, active bool
			operationIDs := make(map[string]string)
			workerStarted := make(chan struct{})
			var workerCtx context.Context
			server := grpc.NewServer(
				grpc.UnaryInterceptor(func(ctx context.Context, request any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
					switch request := request.(type) {
					case *emptypb.Empty:
						return &emptypb.Empty{}, nil
					case *protos.CreateInstanceRequest:
						var operation durabletaskscheduler.ScheduleOperationRequest
						if err := json.Unmarshal([]byte(request.GetInput().GetValue()), &operation); err != nil {
							return nil, err
						}
						mu.Lock()
						operations = append(operations, operation)
						operationIDs[request.InstanceId] = operation.OperationName
						if operation.OperationName == "delete" {
							active = false
							if !createApplied {
								// A late create after deletion would leave an active schedule.
								createApplied, active = true, true
							}
						}
						mu.Unlock()
						return &protos.CreateInstanceResponse{InstanceId: request.InstanceId}, nil
					case *protos.GetEntityRequest:
						if test.lookupFails {
							return nil, status.Error(codes.InvalidArgument, "injected creation lookup failure")
						}
						mu.Lock()
						defer mu.Unlock()
						if !createApplied {
							// The first read precedes execution of the queued creation.
							createApplied, active = true, true
							return &protos.GetEntityResponse{}, nil
						}
						return &protos.GetEntityResponse{Exists: active, Entity: &protos.EntityMetadata{
							InstanceId: request.InstanceId, SerializedState: wrapperspb.String(`{"Status":1}`),
						}}, nil
					case *protos.GetInstanceRequest:
						mu.Lock()
						operation := operationIDs[request.InstanceId]
						mu.Unlock()
						if operation == "CreateSchedule" {
							// The durable create was accepted, but its caller cannot confirm it.
							<-ctx.Done()
							return nil, status.FromContextError(ctx.Err()).Err()
						}
						<-workerStarted
						if workerCtx.Err() != nil {
							t.Error("worker stopped before durable schedule deletion")
						}
						if test.deleteFails {
							return nil, status.Error(codes.InvalidArgument, "injected delete wait failure")
						}
						return &protos.GetInstanceResponse{
							Exists: true,
							OrchestrationState: &protos.OrchestrationState{
								InstanceId: request.InstanceId, OrchestrationStatus: api.RUNTIME_STATUS_COMPLETED,
							},
						}, nil
					case *protos.QueryInstancesRequest:
						return &protos.QueryInstancesResponse{}, nil
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
			switch {
			case test.deleteFails:
				require.ErrorContains(t, err, "injected delete wait failure")
			case test.lookupFails:
				require.ErrorContains(t, err, "injected creation lookup failure")
				require.ErrorContains(t, err, "creation outcome")
			default:
				require.Equal(t, context.DeadlineExceeded.Error(), err.Error())
			}
			mu.Lock()
			defer mu.Unlock()
			if test.lookupFails {
				require.Len(t, operations, 1, "do not race an unconfirmed creation with deletion")
				return
			}
			require.Len(t, operations, 2)
			require.Equal(t, "CreateSchedule", operations[0].OperationName)
			require.Equal(t, "delete", operations[1].OperationName)
			require.Equal(t, operations[0].EntityID, operations[1].EntityID)
			require.False(t, active, "a delayed creation must not reactivate the deleted schedule")
		})
	}
}
