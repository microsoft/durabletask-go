package main

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/microsoft/durabletask-go/api"
	"github.com/microsoft/durabletask-go/durabletaskscheduler"
	"github.com/microsoft/durabletask-go/internal/protos"
	"github.com/microsoft/durabletask-go/task"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

func TestVerifyRunBoundsCleanupOnEveryExit(t *testing.T) {
	for _, test := range []struct {
		name     string
		canceled bool
		blocked  bool
	}{
		{name: "completed Run is not waited twice"},
		{name: "canceled before Run starts", canceled: true},
		{name: "accepted activity outlives cancellation", blocked: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			workerStarted := make(chan struct{})
			started := make(chan struct{})
			finished := make(chan struct{})
			releaseActivity := make(chan struct{})
			release := sync.OnceFunc(func() { close(releaseActivity) })
			items := make(chan *protos.WorkItem, 1)
			server := grpc.NewServer(
				grpc.UnaryInterceptor(func(ctx context.Context, request any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
					switch request := request.(type) {
					case *emptypb.Empty:
						return &emptypb.Empty{}, nil
					case *protos.CreateInstanceRequest:
						if test.blocked {
							items <- &protos.WorkItem{
								CompletionToken: "activity",
								Request: &protos.WorkItem_ActivityRequest{ActivityRequest: &protos.ActivityRequest{
									Name:                  "SampleWorkerEchoActivity",
									Input:                 wrapperspb.String(`"run"`),
									OrchestrationInstance: &protos.OrchestrationInstance{InstanceId: request.InstanceId},
								}},
							}
						}
						return &protos.CreateInstanceResponse{InstanceId: request.InstanceId}, nil
					case *protos.GetInstanceRequest:
						if test.blocked {
							select {
							case <-started:
								return nil, status.Error(codes.InvalidArgument, "injected completion wait failure")
							case <-ctx.Done():
								return nil, status.FromContextError(ctx.Err()).Err()
							}
						}
						select {
						case <-workerStarted:
						case <-ctx.Done():
							return nil, status.FromContextError(ctx.Err()).Err()
						}
						return &protos.GetInstanceResponse{
							Exists: true,
							OrchestrationState: &protos.OrchestrationState{
								InstanceId: request.InstanceId, OrchestrationStatus: api.RUNTIME_STATUS_COMPLETED,
								Output: wrapperspb.String(`"echo:run"`),
							},
						}, nil
					case *protos.ActivityResponse:
						return &protos.CompleteTaskResponse{}, nil
					case *protos.AbandonActivityTaskRequest:
						return &protos.AbandonActivityTaskResponse{}, nil
					default:
						return handler(ctx, request)
					}
				}),
				grpc.StreamInterceptor(func(_ any, stream grpc.ServerStream, _ *grpc.StreamServerInfo, _ grpc.StreamHandler) error {
					close(workerStarted)
					select {
					case item := <-items:
						if err := stream.SendMsg(item); err != nil {
							return err
						}
					case <-stream.Context().Done():
						return nil
					}
					<-stream.Context().Done()
					return nil
				}),
			)
			protos.RegisterTaskHubSidecarServiceServer(server, &protos.UnimplementedTaskHubSidecarServiceServer{})
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			require.NoError(t, err)
			t.Cleanup(server.Stop)
			t.Cleanup(release)
			go func() { _ = server.Serve(listener) }()
			options, err := durabletaskscheduler.NewOptionsFromConnectionString(
				"Endpoint=http://" + listener.Addr().String() + ";TaskHub=test;Authentication=None")
			require.NoError(t, err)
			client, err := durabletaskscheduler.NewClient(t.Context(), options, api.DefaultLogger())
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, client.Close()) })
			registry := task.NewTaskRegistry()
			require.NoError(t, registry.AddOrchestratorN("SampleWorkerEcho", workerEchoWorkflow))
			require.NoError(t, registry.AddActivityN("SampleWorkerEchoActivity", func(task.ActivityContext) (any, error) {
				close(started)
				defer close(finished)
				// Deliberately ignore cancellation to exercise the bounded final Run wait.
				<-releaseActivity
				return "echo:run", nil
			}))

			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			if test.canceled {
				cancel()
			}
			done := make(chan error, 1)
			go func() { done <- verifyRun(ctx, options, client, registry, "run-test") }()
			select {
			case err := <-done:
				switch {
				case test.canceled:
					require.ErrorIs(t, err, context.Canceled)
				case test.blocked:
					require.ErrorContains(t, err, "injected completion wait failure")
					require.ErrorIs(t, err, context.DeadlineExceeded)
					require.ErrorContains(t, err, "wait for Run worker shutdown")
					select {
					case <-finished:
						t.Fatal("activity should remain blocked until explicitly released")
					default:
					}
					release()
					select {
					case <-finished:
					case <-time.After(time.Second):
						t.Fatal("released activity did not finish")
					}
				default:
					require.NoError(t, err)
				}
			case <-time.After(15 * time.Second):
				t.Fatal("verifyRun did not bound its cleanup wait")
			}
		})
	}
}
