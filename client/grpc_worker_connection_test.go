package client

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/microsoft/durabletask-go/api"
	"github.com/microsoft/durabletask-go/internal/protos"
	"github.com/microsoft/durabletask-go/task"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/stats"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

type completionTestCall struct {
	method    string
	transport int
	token     string
}

type completionTestStream struct {
	request   *protos.GetWorkItemsRequest
	transport int
	commands  chan fakeWorkItemResult
}

type completionTestServer struct {
	protos.UnimplementedTaskHubSidecarServiceServer
	streams  chan *completionTestStream
	calls    chan completionTestCall
	complete func(context.Context, *protos.ActivityResponse) error
	hellos   atomic.Int32
}

func newCompletionTestServer() *completionTestServer {
	return &completionTestServer{
		streams: make(chan *completionTestStream, 16),
		calls:   make(chan completionTestCall, 2048),
	}
}

func completionTransportID(ctx context.Context) int {
	md, _ := metadata.FromIncomingContext(ctx)
	id, _ := strconv.Atoi(md.Get("test-transport")[0])
	return id
}

func (s *completionTestServer) Hello(context.Context, *emptypb.Empty) (*emptypb.Empty, error) {
	s.hellos.Add(1)
	return &emptypb.Empty{}, nil
}

func (s *completionTestServer) GetWorkItems(request *protos.GetWorkItemsRequest, stream protos.TaskHubSidecarService_GetWorkItemsServer) error {
	intake := &completionTestStream{
		request:   request,
		transport: completionTransportID(stream.Context()),
		commands:  make(chan fakeWorkItemResult, 1024),
	}
	select {
	case s.streams <- intake:
	case <-stream.Context().Done():
		return stream.Context().Err()
	}
	for {
		select {
		case <-stream.Context().Done():
			return stream.Context().Err()
		case command := <-intake.commands:
			if command.err != nil {
				return command.err
			}
			if err := stream.Send(command.item); err != nil {
				return err
			}
		}
	}
}

func (s *completionTestServer) CompleteActivityTask(ctx context.Context, request *protos.ActivityResponse) (*protos.CompleteTaskResponse, error) {
	s.calls <- completionTestCall{
		method:    protos.TaskHubSidecarService_CompleteActivityTask_FullMethodName,
		transport: completionTransportID(ctx),
		token:     request.CompletionToken,
	}
	if s.complete != nil {
		if err := s.complete(ctx, request); err != nil {
			return nil, err
		}
	}
	return &protos.CompleteTaskResponse{}, nil
}

func (s *completionTestServer) CompleteOrchestratorTask(context.Context, *protos.OrchestratorResponse) (*protos.CompleteTaskResponse, error) {
	return &protos.CompleteTaskResponse{}, nil
}

func (s *completionTestServer) CompleteEntityTask(context.Context, *protos.EntityBatchResult) (*protos.CompleteTaskResponse, error) {
	return &protos.CompleteTaskResponse{}, nil
}

func (s *completionTestServer) AbandonTaskActivityWorkItem(ctx context.Context, request *protos.AbandonActivityTaskRequest) (*protos.AbandonActivityTaskResponse, error) {
	s.calls <- completionTestCall{
		method:    protos.TaskHubSidecarService_AbandonTaskActivityWorkItem_FullMethodName,
		transport: completionTransportID(ctx),
		token:     request.CompletionToken,
	}
	return &protos.AbandonActivityTaskResponse{}, nil
}

func (s *completionTestServer) AbandonTaskOrchestratorWorkItem(context.Context, *protos.AbandonOrchestrationTaskRequest) (*protos.AbandonOrchestrationTaskResponse, error) {
	return &protos.AbandonOrchestrationTaskResponse{}, nil
}

func (s *completionTestServer) AbandonTaskEntityWorkItem(context.Context, *protos.AbandonEntityTaskRequest) (*protos.AbandonEntityTaskResponse, error) {
	return &protos.AbandonEntityTaskResponse{}, nil
}

func (s *completionTestServer) GetInstance(context.Context, *protos.GetInstanceRequest) (*protos.GetInstanceResponse, error) {
	return &protos.GetInstanceResponse{}, nil
}

func (s *completionTestServer) StreamInstanceHistory(_ *protos.StreamInstanceHistoryRequest, stream protos.TaskHubSidecarService_StreamInstanceHistoryServer) error {
	s.calls <- completionTestCall{
		method:    protos.TaskHubSidecarService_StreamInstanceHistory_FullMethodName,
		transport: completionTransportID(stream.Context()),
	}
	return nil
}

type completionMethodKey struct{}

type completionTestStats struct {
	begin   chan struct{}
	headers atomic.Int32
}

func (s *completionTestStats) TagRPC(ctx context.Context, info *stats.RPCTagInfo) context.Context {
	return context.WithValue(ctx, completionMethodKey{}, info.FullMethodName)
}

func (s *completionTestStats) HandleRPC(ctx context.Context, event stats.RPCStats) {
	if ctx.Value(completionMethodKey{}) != protos.TaskHubSidecarService_CompleteActivityTask_FullMethodName {
		return
	}
	switch event.(type) {
	case *stats.Begin:
		s.begin <- struct{}{}
	case *stats.OutHeader:
		s.headers.Add(1)
	}
}

func (*completionTestStats) TagConn(ctx context.Context, _ *stats.ConnTagInfo) context.Context {
	return ctx
}

func (*completionTestStats) HandleConn(context.Context, stats.ConnStats) {}

type completionTestCloser struct {
	connection *grpc.ClientConn
	closes     atomic.Int32
	closed     chan struct{}
}

func (c *completionTestCloser) Close() error {
	c.closes.Add(1)
	err := c.connection.Close()
	close(c.closed)
	return err
}

type completionTestHarness struct {
	listener *bufconn.Listener
	mu       sync.Mutex
	closers  []*completionTestCloser
	stats    *completionTestStats
}

func newCompletionTestHarness(t *testing.T, server *completionTestServer, options ...grpc.ServerOption) *completionTestHarness {
	t.Helper()
	harness := &completionTestHarness{
		listener: bufconn.Listen(1024 * 1024),
		stats:    &completionTestStats{begin: make(chan struct{}, 2048)},
	}
	grpcServer := grpc.NewServer(append([]grpc.ServerOption{grpc.MaxConcurrentStreams(100)}, options...)...)
	protos.RegisterTaskHubSidecarServiceServer(grpcServer, server)
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = grpcServer.Serve(harness.listener)
	}()
	t.Cleanup(func() {
		harness.mu.Lock()
		defer harness.mu.Unlock()
		for _, closer := range harness.closers {
			if closer.closes.Load() == 0 {
				require.NoError(t, closer.Close())
			}
		}
		grpcServer.Stop()
		require.NoError(t, harness.listener.Close())
		awaitCompletionTest(t, done)
	})
	return harness
}

func (h *completionTestHarness) factory(context.Context) (grpc.ClientConnInterface, io.Closer, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	id := strconv.Itoa(len(h.closers))
	connection, err := grpc.NewClient("passthrough:///completion-test",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return h.listener.Dial() }),
		grpc.WithStatsHandler(h.stats),
		grpc.WithUnaryInterceptor(func(ctx context.Context, method string, request, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
			return invoker(metadata.AppendToOutgoingContext(ctx, "test-transport", id, "workerid", "one-worker"), method, request, reply, cc, opts...)
		}),
		grpc.WithStreamInterceptor(func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
			return streamer(metadata.AppendToOutgoingContext(ctx, "test-transport", id, "workerid", "one-worker"), desc, cc, method, opts...)
		}),
	)
	if err != nil {
		return nil, nil, err
	}
	closer := &completionTestCloser{connection: connection, closed: make(chan struct{})}
	h.closers = append(h.closers, closer)
	return connection, closer, nil
}

func (h *completionTestHarness) connections() []*completionTestCloser {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]*completionTestCloser(nil), h.closers...)
}

func awaitCompletionTest[T any](t *testing.T, ch <-chan T) T {
	t.Helper()
	select {
	case value := <-ch:
		return value
	case <-time.After(10 * time.Second):
		t.Fatal("completion transport test did not make progress")
		var zero T
		return zero
	}
}

func completionTestActivity(token string) fakeWorkItemResult {
	return fakeWorkItemResult{item: &protos.WorkItem{
		CompletionToken: token,
		Request: &protos.WorkItem_ActivityRequest{ActivityRequest: &protos.ActivityRequest{
			Name:                  "echo",
			OrchestrationInstance: &protos.OrchestrationInstance{InstanceId: token},
		}},
	}}
}

func startCompletionTestWorker(t *testing.T, harness *completionTestHarness, additional, activityLimit int) (*TaskHubGrpcWorker, <-chan struct{}) {
	t.Helper()
	executed := make(chan struct{}, 1024)
	registry := task.NewTaskRegistry()
	require.NoError(t, registry.AddActivityN("echo", func(task.ActivityContext) (any, error) {
		executed <- struct{}{}
		return "done", nil
	}))
	options := []TaskHubGrpcWorkerOption{
		WithMaxConcurrentOrchestrationWorkItems(1000),
		WithMaxConcurrentActivityWorkItems(activityLimit),
		WithWorkerHelloTimeout(5 * time.Second),
		WithWorkerSilentDisconnectTimeout(time.Minute),
		WithWorkerRPCTimeout(time.Minute),
		WithWorkerReconnectBackoff(time.Millisecond, time.Millisecond),
		WithWorkerTransientRetryPolicy(3, time.Millisecond, time.Millisecond),
	}
	var worker *TaskHubGrpcWorker
	var err error
	if additional == 0 {
		// Shared transport exists only in this private diagnostic baseline.
		worker, err = newTaskHubGrpcWorker(func(ctx context.Context) (protos.TaskHubSidecarServiceClient, io.Closer, error) {
			cc, closer, err := harness.factory(ctx)
			return protos.NewTaskHubSidecarServiceClient(cc), closer, err
		}, registry, nil, options...)
	} else {
		if additional != DefaultWorkerCompletionConnections {
			options = append(options, WithWorkerCompletionConnections(additional))
		}
		worker, err = NewTaskHubGrpcWorkerWithConnectionFactory(harness.factory, registry, nil, options...)
	}
	require.NoError(t, err)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = worker.Shutdown(ctx)
		require.NoError(t, worker.Wait(ctx))
	})
	require.NoError(t, worker.Start(context.Background()))
	return worker, executed
}

func TestWorkerCompletionConnectionsRelieveHTTP2StreamAdmission(t *testing.T) {
	for _, additional := range []int{0, 3} {
		t.Run(fmt.Sprintf("additional_%d", additional), func(t *testing.T) {
			const tasks = 240
			release := make(chan struct{})
			var releaseOnce sync.Once
			defer releaseOnce.Do(func() { close(release) })
			server := newCompletionTestServer()
			server.complete = func(ctx context.Context, _ *protos.ActivityResponse) error {
				select {
				case <-release:
					return nil
				case <-ctx.Done():
					return ctx.Err()
				}
			}
			harness := newCompletionTestHarness(t, server)
			worker, executed := startCompletionTestWorker(t, harness, additional, 1000)
			intake := awaitCompletionTest(t, server.streams)
			require.Zero(t, intake.transport)
			require.EqualValues(t, 1000, intake.request.MaxConcurrentOrchestrationWorkItems)
			require.EqualValues(t, 1000, intake.request.MaxConcurrentActivityWorkItems)
			for i := range tasks {
				intake.commands <- completionTestActivity(strconv.Itoa(i))
			}
			for range tasks {
				awaitCompletionTest(t, executed)
				awaitCompletionTest(t, harness.stats.begin)
			}
			admitted := tasks
			if additional == 0 {
				admitted = 99 // The held intake stream uses the hundredth HTTP/2 slot.
			}
			byTransport := make(map[int]int)
			for range admitted {
				call := awaitCompletionTest(t, server.calls)
				byTransport[call.transport]++
			}
			require.EqualValues(t, admitted, harness.stats.headers.Load())
			require.EqualValues(t, 1, server.hellos.Load(), "additional channels must not register workers")
			require.Empty(t, server.streams, "only one intake may be open")
			require.Len(t, harness.connections(), additional+1)
			if additional == 0 {
				require.Equal(t, map[int]int{0: 99}, byTransport)
				require.Equal(t, 141, tasks-int(harness.stats.headers.Load()), "calls must wait before OutHeader on stock transport")
			} else {
				require.Equal(t, map[int]int{1: 80, 2: 80, 3: 80}, byTransport)
			}
			releaseOnce.Do(func() { close(release) })
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			require.NoError(t, worker.Shutdown(ctx))
			for range tasks - admitted {
				awaitCompletionTest(t, server.calls)
			}
			for _, closer := range harness.connections() {
				require.EqualValues(t, 1, closer.closes.Load())
			}
		})
	}
}

func TestWorkerCompletionConnectionsKeepPendingAcknowledgementsOnOriginalGeneration(t *testing.T) {
	for _, failCompletion := range []bool{false, true} {
		t.Run(fmt.Sprintf("abandon_%t", failCompletion), func(t *testing.T) {
			testWorkerCompletionGeneration(t, failCompletion)
		})
	}
}

func testWorkerCompletionGeneration(t *testing.T, failCompletion bool) {
	t.Helper()
	release := make(chan struct{})
	var releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(release) })
	server := newCompletionTestServer()
	server.complete = func(ctx context.Context, request *protos.ActivityResponse) error {
		if request.CompletionToken == "old" {
			select {
			case <-release:
			case <-ctx.Done():
				return ctx.Err()
			}
			if failCompletion {
				return status.Error(codes.InvalidArgument, "test failed acknowledgement")
			}
		}
		return nil
	}
	harness := newCompletionTestHarness(t, server)
	worker, _ := startCompletionTestWorker(t, harness, 3, 1000)
	first := awaitCompletionTest(t, server.streams)
	first.commands <- completionTestActivity("old")
	oldCall := awaitCompletionTest(t, server.calls)
	require.Equal(t, 1, oldCall.transport)
	first.commands <- fakeWorkItemResult{err: status.Error(codes.Unavailable, "reconnect")}
	second := awaitCompletionTest(t, server.streams)
	require.Equal(t, 4, second.transport)
	for _, closer := range harness.connections()[:4] {
		require.Zero(t, closer.closes.Load(), "a pending acknowledgement keeps the entire original group alive")
	}
	second.commands <- completionTestActivity("new")
	newCall := awaitCompletionTest(t, server.calls)
	require.Equal(t, "new", newCall.token)
	require.Equal(t, 5, newCall.transport)
	releaseOnce.Do(func() { close(release) })
	if failCompletion {
		abandon := awaitCompletionTest(t, server.calls)
		require.Equal(t, completionTestCall{
			method:    protos.TaskHubSidecarService_AbandonTaskActivityWorkItem_FullMethodName,
			transport: 2,
			token:     "old",
		}, abandon, "old work must also abandon on its original group")
	}
	for _, closer := range harness.connections()[:4] {
		awaitCompletionTest(t, closer.closed)
		require.EqualValues(t, 1, closer.closes.Load())
	}
	require.NoError(t, worker.Shutdown(context.Background()))
	require.EqualValues(t, 2, server.hellos.Load())
	for _, closer := range harness.connections() {
		require.EqualValues(t, 1, closer.closes.Load())
	}
}

func TestWorkerCompletionConnectionsBoundRetiringGenerations(t *testing.T) {
	for _, cancelDrain := range []bool{false, true} {
		t.Run(fmt.Sprintf("cancel_%t", cancelDrain), func(t *testing.T) {
			testWorkerCompletionRetirementBound(t, cancelDrain)
		})
	}
}

func testWorkerCompletionRetirementBound(t *testing.T, cancelDrain bool) {
	t.Helper()
	release := make(chan struct{})
	var releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(release) })
	server := newCompletionTestServer()
	server.complete = func(ctx context.Context, _ *protos.ActivityResponse) error {
		select {
		case <-release:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	harness := newCompletionTestHarness(t, server)
	worker, _ := startCompletionTestWorker(t, harness, 3, 1000)
	first := awaitCompletionTest(t, server.streams)
	first.commands <- completionTestActivity("held")
	awaitCompletionTest(t, server.calls)
	first.commands <- fakeWorkItemResult{err: status.Error(codes.Unavailable, "first disconnect")}
	second := awaitCompletionTest(t, server.streams)
	second.commands <- fakeWorkItemResult{err: status.Error(codes.Unavailable, "second disconnect")}
	for _, closer := range harness.connections()[4:8] {
		awaitCompletionTest(t, closer.closed)
	}
	require.Len(t, harness.connections(), 8, "must not allocate a third group while the oldest is draining")
	if cancelDrain {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		require.ErrorIs(t, worker.Shutdown(ctx), context.Canceled)
		waitCtx, cancelWait := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancelWait()
		require.NoError(t, worker.Wait(waitCtx))
		require.Len(t, harness.connections(), 8, "shutdown must unblock retirement without opening another group")
		for _, closer := range harness.connections() {
			require.EqualValues(t, 1, closer.closes.Load())
		}
		return
	}
	releaseOnce.Do(func() { close(release) })
	third := awaitCompletionTest(t, server.streams)
	require.Equal(t, 8, third.transport)
	require.Len(t, harness.connections(), 12)
	require.NoError(t, worker.Shutdown(context.Background()))
	for _, closer := range harness.connections() {
		require.EqualValues(t, 1, closer.closes.Load())
	}
}

func TestWorkerCompletionConnectionsCancelSaturatedShutdown(t *testing.T) {
	const tasks = 450
	server := newCompletionTestServer()
	server.complete = func(ctx context.Context, _ *protos.ActivityResponse) error {
		<-ctx.Done()
		return ctx.Err()
	}
	harness := newCompletionTestHarness(t, server)
	worker, executed := startCompletionTestWorker(t, harness, 3, 1000)
	intake := awaitCompletionTest(t, server.streams)
	for i := range tasks {
		intake.commands <- completionTestActivity(strconv.Itoa(i))
	}
	for range tasks {
		awaitCompletionTest(t, executed)
		awaitCompletionTest(t, harness.stats.begin)
	}
	for range 300 {
		awaitCompletionTest(t, server.calls)
	}
	require.EqualValues(t, 300, harness.stats.headers.Load(), "each of three completion transports is saturated at 100 streams")
	worker.mu.Lock()
	run := worker.run
	worker.mu.Unlock()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.ErrorIs(t, worker.Shutdown(ctx), context.Canceled)
	waitCtx, cancelWait := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelWait()
	require.NoError(t, worker.Wait(waitCtx))
	require.Empty(t, run.activitySlots, "canceled RPCs must release execution slots")
	for _, closer := range harness.connections() {
		require.EqualValues(t, 1, closer.closes.Load())
	}
}

func TestWorkerCompletionConnectionsNotFoundReleasesSlotWithoutAbandon(t *testing.T) {
	server := newCompletionTestServer()
	server.complete = func(_ context.Context, request *protos.ActivityResponse) error {
		if request.CompletionToken == "gone" {
			return status.Error(codes.NotFound, "lease expired")
		}
		return nil
	}
	harness := newCompletionTestHarness(t, server)
	worker, executed := startCompletionTestWorker(t, harness, 3, 1)
	intake := awaitCompletionTest(t, server.streams)
	intake.commands <- completionTestActivity("gone")
	intake.commands <- completionTestActivity("next")
	for _, token := range []string{"gone", "next"} {
		awaitCompletionTest(t, executed)
		call := awaitCompletionTest(t, server.calls)
		require.Equal(t, token, call.token)
		require.Equal(t, protos.TaskHubSidecarService_CompleteActivityTask_FullMethodName, call.method)
	}
	require.NoError(t, worker.Shutdown(context.Background()))
	require.Empty(t, server.calls, "terminal NotFound must not cause retry or abandon")
}

func TestWorkerCompletionTransportRoutesExactMethodsAndPreservesCallOptions(t *testing.T) {
	observed := make(chan completionTestCall, 16)
	server := newCompletionTestServer()
	harness := newCompletionTestHarness(t, server, grpc.UnaryInterceptor(
		func(ctx context.Context, request any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
			md, _ := metadata.FromIncomingContext(ctx)
			if md.Get("workerid")[0] != "one-worker" || md.Get("test-context")[0] != "preserved" {
				return nil, status.Error(codes.InvalidArgument, "metadata lost")
			}
			observed <- completionTestCall{method: info.FullMethod, transport: completionTransportID(ctx)}
			if err := grpc.SendHeader(ctx, metadata.Pairs("test-response", "header")); err != nil {
				return nil, err
			}
			if err := grpc.SetTrailer(ctx, metadata.Pairs("test-response", "trailer")); err != nil {
				return nil, err
			}
			return handler(ctx, request)
		}))
	ctx := metadata.AppendToOutgoingContext(context.Background(), "test-context", "preserved")
	group, err := newWorkerCompletionTransport(ctx, harness.factory, 3)
	require.NoError(t, err)
	defer func() { require.NoError(t, group.Close()) }()
	client := protos.NewTaskHubSidecarServiceClient(group)
	calls := []struct {
		method string
		call   func(...grpc.CallOption) error
		want   int
	}{
		{protos.TaskHubSidecarService_Hello_FullMethodName, func(opts ...grpc.CallOption) error {
			_, err := client.Hello(ctx, &emptypb.Empty{}, opts...)
			return err
		}, 0},
		{protos.TaskHubSidecarService_CompleteActivityTask_FullMethodName, func(opts ...grpc.CallOption) error {
			_, err := client.CompleteActivityTask(ctx, &protos.ActivityResponse{}, opts...)
			return err
		}, 1},
		{protos.TaskHubSidecarService_CompleteOrchestratorTask_FullMethodName, func(opts ...grpc.CallOption) error {
			_, err := client.CompleteOrchestratorTask(ctx, &protos.OrchestratorResponse{}, opts...)
			return err
		}, 2},
		{protos.TaskHubSidecarService_CompleteEntityTask_FullMethodName, func(opts ...grpc.CallOption) error {
			_, err := client.CompleteEntityTask(ctx, &protos.EntityBatchResult{}, opts...)
			return err
		}, 3},
		{protos.TaskHubSidecarService_AbandonTaskActivityWorkItem_FullMethodName, func(opts ...grpc.CallOption) error {
			_, err := client.AbandonTaskActivityWorkItem(ctx, &protos.AbandonActivityTaskRequest{}, opts...)
			return err
		}, 1},
		{protos.TaskHubSidecarService_AbandonTaskOrchestratorWorkItem_FullMethodName, func(opts ...grpc.CallOption) error {
			_, err := client.AbandonTaskOrchestratorWorkItem(ctx, &protos.AbandonOrchestrationTaskRequest{}, opts...)
			return err
		}, 2},
		{protos.TaskHubSidecarService_AbandonTaskEntityWorkItem_FullMethodName, func(opts ...grpc.CallOption) error {
			_, err := client.AbandonTaskEntityWorkItem(ctx, &protos.AbandonEntityTaskRequest{}, opts...)
			return err
		}, 3},
		{protos.TaskHubSidecarService_GetInstance_FullMethodName, func(opts ...grpc.CallOption) error {
			_, err := client.GetInstance(ctx, &protos.GetInstanceRequest{}, opts...)
			return err
		}, 0},
	}
	for _, test := range calls {
		var header, trailer metadata.MD
		require.NoError(t, test.call(grpc.Header(&header), grpc.Trailer(&trailer)))
		require.Equal(t, completionTestCall{method: test.method, transport: test.want}, awaitCompletionTest(t, observed))
		require.Equal(t, []string{"header"}, header.Get("test-response"))
		require.Equal(t, []string{"trailer"}, trailer.Get("test-response"))
		if test.method == protos.TaskHubSidecarService_CompleteActivityTask_FullMethodName ||
			test.method == protos.TaskHubSidecarService_AbandonTaskActivityWorkItem_FullMethodName {
			require.Equal(t, test.method, awaitCompletionTest(t, server.calls).method)
		}
	}
	require.False(t, isWorkerCompletionMethod("/TaskHubSidecarService/CompleteActivityTaskExtra"))
	require.False(t, isWorkerCompletionMethod("/OtherService/CompleteActivityTask"))
	require.False(t, isWorkerCompletionMethod("/TaskHubSidecarService/AbandonAnything"))
	require.False(t, isWorkerCompletionMethod(protos.TaskHubSidecarService_GetWorkItems_FullMethodName))
	stream, err := client.GetWorkItems(ctx, &protos.GetWorkItemsRequest{})
	require.NoError(t, err)
	require.Zero(t, awaitCompletionTest(t, server.streams).transport)
	require.NoError(t, stream.CloseSend())
	history, err := client.StreamInstanceHistory(ctx, &protos.StreamInstanceHistoryRequest{})
	require.NoError(t, err)
	_, err = history.Recv()
	require.ErrorIs(t, err, io.EOF)
	require.Equal(t, completionTestCall{
		method:    protos.TaskHubSidecarService_StreamInstanceHistory_FullMethodName,
		transport: 0,
	}, awaitCompletionTest(t, server.calls))
	_, err = client.CompleteActivityTask(ctx, &protos.ActivityResponse{Result: wrapperspb.String("too large")}, grpc.MaxCallSendMsgSize(1))
	require.Equal(t, codes.ResourceExhausted, status.Code(err), "per-call size limits must not be discarded")
}

func TestWorkerCompletionTransportRollbackAndOwnership(t *testing.T) {
	for _, failure := range []string{"factory error", "nil connection", "borrowed connection", "canceled"} {
		t.Run(failure, func(t *testing.T) {
			var calls int
			closers := []*countingCloser{{}, {}, {}}
			borrowed := &fakeClientConn{}
			factoryErr := errors.New("completion factory failed")
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			group, err := newWorkerCompletionTransport(ctx, func(context.Context) (grpc.ClientConnInterface, io.Closer, error) {
				index := calls
				calls++
				if index == 2 {
					switch failure {
					case "factory error":
						return borrowed, closers[index], factoryErr
					case "nil connection":
						return nil, closers[index], nil
					case "borrowed connection":
						return borrowed, nil, nil
					case "canceled":
						cancel()
					}
				}
				return &fakeClientConn{}, closers[index], nil
			}, 3)
			require.Error(t, err)
			require.Nil(t, group)
			require.Equal(t, 3, calls)
			require.Zero(t, borrowed.closes.Load(), "ownership must not be inferred from the connection type")
			for i, closer := range closers {
				if i == 2 && failure == "borrowed connection" {
					require.Zero(t, closer.closes.Load())
				} else {
					require.EqualValues(t, 1, closer.closes.Load())
				}
			}
			if failure == "factory error" {
				require.ErrorIs(t, err, factoryErr)
			}
			if failure == "canceled" {
				require.ErrorIs(t, err, context.Canceled)
			}
		})
	}
}

func TestWorkerCompletionConnectionsReadinessCancellationRollsBack(t *testing.T) {
	server := newCompletionTestServer()
	harness := newCompletionTestHarness(t, server)
	dialing := make(chan struct{})
	var dialOnce sync.Once
	var calls int
	var failedCloser *completionTestCloser
	factory := func(ctx context.Context) (grpc.ClientConnInterface, io.Closer, error) {
		calls++
		if calls == 1 {
			return harness.factory(ctx)
		}
		connection, err := grpc.NewClient("passthrough:///unreachable",
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
				dialOnce.Do(func() { close(dialing) })
				<-ctx.Done()
				return nil, ctx.Err()
			}))
		if err != nil {
			return nil, nil, err
		}
		failedCloser = &completionTestCloser{connection: connection, closed: make(chan struct{})}
		return connection, failedCloser, nil
	}
	worker, err := NewTaskHubGrpcWorkerWithConnectionFactory(factory, task.NewTaskRegistry(), nil, WithWorkerCompletionConnections(3))
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	result := make(chan error, 1)
	go func() { result <- worker.Start(ctx) }()
	awaitCompletionTest(t, dialing)
	cancel()
	err = awaitCompletionTest(t, result)
	require.Equal(t, codes.Canceled, status.Code(err))
	require.ErrorContains(t, err, "did not become ready")
	require.Equal(t, 2, calls, "a failing channel must not silently fall back or continue allocation")
	require.False(t, worker.Running())
	require.EqualValues(t, 1, failedCloser.closes.Load())
	require.EqualValues(t, 1, harness.connections()[0].closes.Load())
	require.Zero(t, server.hellos.Load(), "setup failure must not register a partial group")
}

func TestWorkerCompletionConnectionsHelloFailureClosesWholeGroup(t *testing.T) {
	server := newCompletionTestServer()
	harness := newCompletionTestHarness(t, server, grpc.UnaryInterceptor(
		func(context.Context, any, *grpc.UnaryServerInfo, grpc.UnaryHandler) (any, error) {
			return nil, status.Error(codes.PermissionDenied, "test Hello failure")
		}))
	worker, err := NewTaskHubGrpcWorkerWithConnectionFactory(
		harness.factory, task.NewTaskRegistry(), nil, WithWorkerCompletionConnections(3))
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	err = worker.Start(ctx)
	require.Equal(t, codes.PermissionDenied, status.Code(err))
	require.ErrorContains(t, err, "worker Hello failed")
	require.False(t, worker.Running())
	require.Len(t, harness.connections(), 4)
	require.Empty(t, server.streams)
	for _, closer := range harness.connections() {
		require.EqualValues(t, 1, closer.closes.Load())
	}
}

type completionErrorCloser struct {
	countingCloser
	err error
}

func (c *completionErrorCloser) Close() error {
	c.closes.Add(1)
	return c.err
}

func TestWorkerCompletionTransportClosesEveryConnectionOnceDespiteCloseErrors(t *testing.T) {
	closeErr := errors.New("test close failure")
	closers := []*completionErrorCloser{{err: closeErr}, {}, {}, {}}
	index := 0
	group, err := newWorkerCompletionTransport(context.Background(), func(context.Context) (grpc.ClientConnInterface, io.Closer, error) {
		closer := closers[index]
		index++
		return &fakeClientConn{}, closer, nil
	}, 3)
	require.NoError(t, err)
	for range 2 {
		require.ErrorIs(t, group.Close(), closeErr)
	}
	for _, closer := range closers {
		require.EqualValues(t, 1, closer.closes.Load())
	}
}

func TestWorkerCompletionConnectionsValidation(t *testing.T) {
	for _, count := range []int{-1, 0, 9} {
		_, err := NewTaskHubGrpcWorkerWithConnectionFactory(func(context.Context) (grpc.ClientConnInterface, io.Closer, error) {
			t.Fatal("invalid options must not dial")
			return nil, nil, nil
		}, task.NewTaskRegistry(), api.DefaultLogger(), WithWorkerCompletionConnections(count))
		require.ErrorContains(t, err, "between 1 and 8")
	}
	borrowed := &fakeClientConn{}
	_, err := NewTaskHubGrpcWorker(borrowed, task.NewTaskRegistry(), nil, WithWorkerCompletionConnections(3))
	require.ErrorContains(t, err, "require separate connections")
	require.Zero(t, borrowed.closes.Load())
	require.Zero(t, borrowed.streamCount())
	worker, err := NewTaskHubGrpcWorkerWithConnectionFactory(
		func(context.Context) (grpc.ClientConnInterface, io.Closer, error) { return borrowed, nil, nil },
		task.NewTaskRegistry(), nil)
	require.NoError(t, err)
	require.ErrorContains(t, worker.Start(context.Background()), "require an owning factory")
	require.Zero(t, borrowed.closes.Load(), "missing ownership must not close the caller's channel")
}

func TestWorkerCompletionConnectionsAcceptBoundedBudgets(t *testing.T) {
	for _, budget := range []int{1, 8} {
		t.Run(strconv.Itoa(budget), func(t *testing.T) {
			server := newCompletionTestServer()
			harness := newCompletionTestHarness(t, server)
			worker, _ := startCompletionTestWorker(t, harness, budget, 1000)
			intake := awaitCompletionTest(t, server.streams)
			intake.commands <- completionTestActivity("budget")
			call := awaitCompletionTest(t, server.calls)
			require.Equal(t, 1, call.transport)
			require.Len(t, harness.connections(), budget+1)
			require.EqualValues(t, 1, server.hellos.Load())
			require.NoError(t, worker.Shutdown(context.Background()))
			for _, closer := range harness.connections() {
				require.EqualValues(t, 1, closer.closes.Load())
			}
		})
	}
}

func TestWorkerCompletionTransportsValidateBorrowedIsolation(t *testing.T) {
	intake := &fakeClientConn{}
	completion := &fakeClientConn{}
	var typedNil *fakeClientConn
	for _, test := range []struct {
		name       string
		transports []grpc.ClientConnInterface
		options    []TaskHubGrpcWorkerOption
		message    string
	}{
		{"empty", nil, nil, "between 1 and 8"},
		{"nil", []grpc.ClientConnInterface{nil}, nil, "is nil"},
		{"typed nil", []grpc.ClientConnInterface{typedNil}, nil, "is nil"},
		{"shared intake", []grpc.ClientConnInterface{intake}, nil, "separate from intake"},
		{"duplicate", []grpc.ClientConnInterface{completion, completion}, nil, "must be distinct"},
		{"budget mismatch", []grpc.ClientConnInterface{completion}, []TaskHubGrpcWorkerOption{WithWorkerCompletionConnections(3)}, "budget must match"},
	} {
		t.Run(test.name, func(t *testing.T) {
			opts := append([]TaskHubGrpcWorkerOption{WithWorkerCompletionTransports(test.transports...)}, test.options...)
			_, err := NewTaskHubGrpcWorker(intake, task.NewTaskRegistry(), nil, opts...)
			require.ErrorContains(t, err, test.message)
		})
	}
	_, err := NewTaskHubGrpcWorkerWithConnectionFactory(
		func(context.Context) (grpc.ClientConnInterface, io.Closer, error) {
			t.Fatal("invalid ownership options must not dial")
			return nil, nil, nil
		}, task.NewTaskRegistry(), nil, WithWorkerCompletionTransports(completion))
	require.ErrorContains(t, err, "owning worker factories cannot use")
	require.Zero(t, intake.closes.Load())
	require.Zero(t, completion.closes.Load())
}

func TestWorkerCompletionConnectionsRejectReusedFactoryTransport(t *testing.T) {
	cc := &fakeClientConn{}
	closer := &countingCloser{}
	group, err := newWorkerCompletionTransport(context.Background(),
		func(context.Context) (grpc.ClientConnInterface, io.Closer, error) {
			return cc, closer, nil
		}, 3)
	require.ErrorContains(t, err, "separate from intake")
	require.Nil(t, group)
	require.EqualValues(t, 1, closer.closes.Load(), "aliased ownership must roll back only once")
}

func TestWorkerCompletionConnectionsRejectReusedFactoryCloser(t *testing.T) {
	closer := &countingCloser{}
	group, err := newWorkerCompletionTransport(context.Background(),
		func(context.Context) (grpc.ClientConnInterface, io.Closer, error) {
			return &fakeClientConn{}, closer, nil
		}, 3)
	require.ErrorContains(t, err, "distinct ownership closers")
	require.Nil(t, group)
	require.EqualValues(t, 1, closer.closes.Load())
}

func TestWorkerCompletionTransportsBorrowedIsolationAndSnapshot(t *testing.T) {
	server := newCompletionTestServer()
	harness := newCompletionTestHarness(t, server)
	connections := make([]grpc.ClientConnInterface, 4)
	for i := range connections {
		cc, _, err := harness.factory(context.Background())
		require.NoError(t, err)
		connections[i] = cc
	}
	transports := append([]grpc.ClientConnInterface(nil), connections[1:]...)
	option := WithWorkerCompletionTransports(transports...)
	transports[0] = connections[0]
	registry := task.NewTaskRegistry()
	require.NoError(t, registry.AddActivityN("echo", func(task.ActivityContext) (any, error) { return "done", nil }))
	worker, err := NewTaskHubGrpcWorker(connections[0], registry, nil,
		option, WithWorkerCompletionConnections(3),
		WithWorkerReconnectBackoff(time.Millisecond, time.Millisecond))
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	defer func() { require.NoError(t, worker.Shutdown(ctx)) }()
	require.NoError(t, worker.Start(ctx))
	for generation := range 2 {
		intake := awaitCompletionTest(t, server.streams)
		require.Zero(t, intake.transport)
		for i := range 3 {
			intake.commands <- completionTestActivity(fmt.Sprintf("%d/%d", generation, i))
		}
		transports := make(map[int]int)
		for range 3 {
			call := awaitCompletionTest(t, server.calls)
			transports[call.transport]++
		}
		require.Equal(t, map[int]int{1: 1, 2: 1, 3: 1}, transports)
		if generation == 0 {
			intake.commands <- fakeWorkItemResult{err: status.Error(codes.Unavailable, "borrowed reconnect")}
		}
	}
	require.NoError(t, worker.Shutdown(ctx))
	require.Len(t, harness.connections(), 4, "a borrowed worker must not synthesize channels")
	for _, closer := range harness.connections() {
		require.Zero(t, closer.closes.Load(), "worker retirement and shutdown must preserve caller ownership")
		require.NoError(t, closer.Close())
	}
}
