package durabletaskscheduler

import (
	"context"
	"os"
	"os/exec"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	durabletaskclient "github.com/microsoft/durabletask-go/client"
	"github.com/microsoft/durabletask-go/internal/protos"
	"github.com/microsoft/durabletask-go/task"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

type workerCompletionObservation struct {
	method   string
	metadata metadata.MD
	response *protos.ActivityResponse
}

type workerCompletionOptionsServer struct {
	protos.UnimplementedTaskHubSidecarServiceServer
	observed   chan workerCompletionObservation
	disconnect chan struct{}
	intakes    atomic.Int32
}

func (s *workerCompletionOptionsServer) record(ctx context.Context, method string, response *protos.ActivityResponse) {
	md, _ := metadata.FromIncomingContext(ctx)
	s.observed <- workerCompletionObservation{method: method, metadata: md, response: response}
}

func (s *workerCompletionOptionsServer) Hello(ctx context.Context, _ *emptypb.Empty) (*emptypb.Empty, error) {
	s.record(ctx, protos.TaskHubSidecarService_Hello_FullMethodName, nil)
	return &emptypb.Empty{}, nil
}

func (s *workerCompletionOptionsServer) GetWorkItems(request *protos.GetWorkItemsRequest, stream protos.TaskHubSidecarService_GetWorkItemsServer) error {
	s.intakes.Add(1)
	s.record(stream.Context(), protos.TaskHubSidecarService_GetWorkItems_FullMethodName, nil)
	if request.MaxConcurrentActivityWorkItems != 1000 || request.MaxConcurrentOrchestrationWorkItems != 1000 {
		return status.Error(codes.InvalidArgument, "execution limits changed")
	}
	for i := range 6 {
		if err := stream.Send(&protos.WorkItem{
			CompletionToken: "token",
			Request: &protos.WorkItem_ActivityRequest{ActivityRequest: &protos.ActivityRequest{
				Name:                  "echo",
				TaskId:                int32(i),
				OrchestrationInstance: &protos.OrchestrationInstance{InstanceId: "instance"},
			}},
		}); err != nil {
			return err
		}
	}
	select {
	case <-s.disconnect:
		return status.Error(codes.Unavailable, "test reconnect")
	case <-stream.Context().Done():
		return stream.Context().Err()
	}
}

func (s *workerCompletionOptionsServer) CompleteActivityTask(ctx context.Context, response *protos.ActivityResponse) (*protos.CompleteTaskResponse, error) {
	s.record(ctx, protos.TaskHubSidecarService_CompleteActivityTask_FullMethodName, response)
	return &protos.CompleteTaskResponse{}, nil
}

func (*workerCompletionOptionsServer) GetInstance(context.Context, *protos.GetInstanceRequest) (*protos.GetInstanceResponse, error) {
	return &protos.GetInstanceResponse{
		Exists:             true,
		OrchestrationState: &protos.OrchestrationState{Output: wrapperspb.String(strings.Repeat("x", 70*1024))},
	}, nil
}

func TestNewWorkerCompletionConnectionsPreservePreparedOptions(t *testing.T) {
	// Like the audience tests, isolate fallback roots in a subprocess instead
	// of modifying trust for other tests or installing a system certificate.
	if os.Getenv("DTS_COMPLETION_TLS_TEST") != "1" {
		command := exec.Command(os.Args[0], "-test.run=^TestNewWorkerCompletionConnectionsPreservePreparedOptions$", "-test.timeout=2m")
		command.Env = append(os.Environ(), "DTS_COMPLETION_TLS_TEST=1")
		output, err := command.CombinedOutput()
		require.NoError(t, err, "%s", output)
		return
	}
	t.Setenv("GODEBUG", os.Getenv("GODEBUG")+",x509usefallbackroots=1")
	server := &workerCompletionOptionsServer{
		observed:   make(chan workerCompletionObservation, 32),
		disconnect: make(chan struct{}, 1),
	}
	listener := bufconn.Listen(1024 * 1024)
	grpcServer := grpc.NewServer(grpc.Creds(credentials.NewTLS(audienceTestTLS())))
	protos.RegisterTaskHubSidecarServiceServer(grpcServer, server)
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = grpcServer.Serve(listener)
	}()
	defer func() {
		grpcServer.Stop()
		require.NoError(t, listener.Close())
		<-done
	}()

	credential := &recordingCredential{}
	options := NewOptionsWithCredential("https://127.0.0.1", "test-hub", credential)
	options.ResourceID = "api://completion-test/.DEFAULT/"
	options.WorkerID = "one-worker"
	options.UserAgent = "completion-test-agent"
	options.DataConverter = recreationDataConverter{}
	options.MaxReceiveMessageSize = 64 * 1024
	options.MaxSendMessageSize = 64 * 1024
	options.dialer = bufconnDialer(listener)
	connections := make(chan *grpc.ClientConn, 32)
	options.UnaryInterceptors = []grpc.UnaryClientInterceptor{
		func(ctx context.Context, method string, request, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, callOptions ...grpc.CallOption) error {
			connections <- cc
			return invoker(metadata.AppendToOutgoingContext(ctx, "test-unary", "preserved"), method, request, reply, cc, callOptions...)
		},
	}
	options.StreamInterceptors = []grpc.StreamClientInterceptor{
		func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, callOptions ...grpc.CallOption) (grpc.ClientStream, error) {
			return streamer(metadata.AppendToOutgoingContext(ctx, "test-stream", "preserved"), desc, cc, method, callOptions...)
		},
	}
	registry := task.NewTaskRegistry()
	require.NoError(t, registry.AddActivityN("echo", func(task.ActivityContext) (any, error) { return "done", nil }))
	worker, err := NewWorker(options, registry, nil,
		durabletaskclient.WithMaxConcurrentActivityWorkItems(1000),
		durabletaskclient.WithMaxConcurrentOrchestrationWorkItems(1000),
		durabletaskclient.WithWorkerReconnectBackoff(time.Millisecond, time.Millisecond))
	require.NoError(t, err)
	// A worker snapshots options once, including the credential and identity.
	options.ResourceID = "api://changed"
	options.WorkerID = "changed"
	options.UserAgent = "changed"
	options.MaxSendMessageSize = 128 * 1024
	options.UnaryInterceptors = nil
	require.Empty(t, recordedTokenOptions(credential), "authentication stays lazy before Start")
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	require.NoError(t, worker.Start(ctx))
	defer func() { require.NoError(t, worker.Shutdown(ctx)) }()

	allConnections := make(map[*grpc.ClientConn]struct{})
	for generation := range 2 {
		counts := make(map[string]int)
		for range 8 {
			var observation workerCompletionObservation
			select {
			case observation = <-server.observed:
			case <-ctx.Done():
				t.Fatal("worker did not complete all test work")
			}
			counts[observation.method]++
			require.Equal(t, []string{"test-hub"}, observation.metadata.Get("taskhub"))
			require.Equal(t, []string{"one-worker"}, observation.metadata.Get("workerid"))
			require.Equal(t, []string{"completion-test-agent"}, observation.metadata.Get("x-user-agent"))
			require.Equal(t, []string{"Bearer token"}, observation.metadata.Get("authorization"))
			if observation.method == protos.TaskHubSidecarService_GetWorkItems_FullMethodName {
				require.Equal(t, []string{"preserved"}, observation.metadata.Get("test-stream"))
			} else {
				require.Equal(t, []string{"preserved"}, observation.metadata.Get("test-unary"))
			}
			if observation.response != nil {
				require.Equal(t, "recreated:done", observation.response.Result.GetValue())
			}
		}
		require.Equal(t, map[string]int{
			protos.TaskHubSidecarService_Hello_FullMethodName:                1,
			protos.TaskHubSidecarService_GetWorkItems_FullMethodName:         1,
			protos.TaskHubSidecarService_CompleteActivityTask_FullMethodName: 6,
		}, counts)
		generationConnections := make(map[*grpc.ClientConn]struct{})
		for range 7 {
			select {
			case connection := <-connections:
				generationConnections[connection] = struct{}{}
				allConnections[connection] = struct{}{}
			case <-ctx.Done():
				t.Fatal("missing intercepted RPC")
			}
		}
		require.Len(t, generationConnections, 4, "one intake and three completion channels")
		require.Len(t, recordedTokenOptions(credential), 4*(generation+1))
		if generation == 0 {
			for connection := range generationConnections {
				client := protos.NewTaskHubSidecarServiceClient(connection)
				_, err = client.CompleteActivityTask(ctx, &protos.ActivityResponse{Result: wrapperspb.String(strings.Repeat("x", 70*1024))})
				require.Equal(t, codes.ResourceExhausted, status.Code(err), "send limits must match intake on every channel")
				_, err = client.GetInstance(ctx, &protos.GetInstanceRequest{})
				require.Equal(t, codes.ResourceExhausted, status.Code(err), "receive limits must match intake on every channel")
				<-connections
				<-connections
			}
			server.disconnect <- struct{}{}
		}
	}
	for _, recorded := range recordedTokenOptions(credential) {
		require.Equal(t, []string{"api://completion-test/.default"}, recorded.Scopes)
	}
	require.NoError(t, worker.Shutdown(ctx))
	require.EqualValues(t, 2, server.intakes.Load())
	require.Len(t, allConnections, 8)
	for connection := range allConnections {
		require.Equal(t, connectivity.Shutdown, connection.GetState(), "every owned channel must close after drain")
	}
}
