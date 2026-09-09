package durabletaskscheduler

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/microsoft/durabletask-go/api"
	"github.com/microsoft/durabletask-go/internal/protos"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type fakeClientConnection struct {
	invoke     func(context.Context, string) error
	newStream  func(context.Context, string, ...grpc.CallOption) (grpc.ClientStream, error)
	closeCount atomic.Int32
}

type closeRecorder struct {
	close      func() error
	closeCount atomic.Int32
}

func (c *closeRecorder) Close() error {
	c.closeCount.Add(1)
	return c.close()
}

func (c *fakeClientConnection) Invoke(
	ctx context.Context,
	method string,
	_ any,
	_ any,
	_ ...grpc.CallOption,
) error {
	if c.invoke == nil {
		return nil
	}
	return c.invoke(ctx, method)
}

func (c *fakeClientConnection) NewStream(
	ctx context.Context,
	_ *grpc.StreamDesc,
	method string,
	options ...grpc.CallOption,
) (grpc.ClientStream, error) {
	if c.newStream != nil {
		return c.newStream(ctx, method, options...)
	}
	return nil, errors.New("streaming is not configured")
}

func (c *fakeClientConnection) Close() error {
	c.closeCount.Add(1)
	return nil
}

func fakeTransport(connection *fakeClientConnection) *clientTransport {
	return &clientTransport{connection: connection, closer: connection}
}

func newTestRecreatingClientConn(
	t *testing.T,
	initial *clientTransport,
	factory clientTransportFactory,
	failureThreshold int,
	minRecreateInterval time.Duration,
) *recreatingClientConn {
	t.Helper()
	connection := newRecreatingClientConn(
		initial,
		factory,
		failureThreshold,
		minRecreateInterval,
		api.DefaultLogger(),
	)
	t.Cleanup(func() {
		require.NoError(t, connection.Close())
	})
	return connection
}

func TestRecreatingClientConnReplacesChannelAfterConsecutiveFailures(t *testing.T) {
	initialConnection := &fakeClientConnection{
		invoke: func(context.Context, string) error {
			return status.Error(codes.Unavailable, "initial unavailable")
		},
	}
	replacementConnection := &fakeClientConnection{}
	initial := fakeTransport(initialConnection)
	replacement := fakeTransport(replacementConnection)
	recreateStarted := make(chan struct{}, 1)
	recreateRelease := make(chan struct{})
	releaseRecreation := sync.OnceFunc(func() {
		close(recreateRelease)
	})
	defer releaseRecreation()
	var recreateCalls atomic.Int32
	connection := newTestRecreatingClientConn(
		t,
		initial,
		func(context.Context, *clientTransport) (*clientTransport, error) {
			recreateCalls.Add(1)
			recreateStarted <- struct{}{}
			<-recreateRelease
			return replacement, nil
		},
		2,
		0,
	)

	require.Error(t, connection.Invoke(context.Background(), "/test/fail", nil, nil))
	require.Error(t, connection.Invoke(context.Background(), "/test/fail", nil, nil))
	select {
	case <-recreateStarted:
	case <-time.After(time.Second):
		t.Fatal("channel recreation was not requested")
	}
	for range 3 {
		require.Error(t, connection.Invoke(context.Background(), "/test/fail", nil, nil))
	}
	require.Equal(t, int32(1), recreateCalls.Load())
	releaseRecreation()
	require.Eventually(t, func() bool {
		connection.mu.Lock()
		defer connection.mu.Unlock()
		return connection.current == replacement
	}, time.Second, time.Millisecond)
	require.NoError(t, connection.Invoke(context.Background(), "/test/success", nil, nil))
	require.Eventually(t, func() bool {
		return initialConnection.closeCount.Load() == 1
	}, time.Second, time.Millisecond)
}

func TestRecreatingClientConnSameChannelNoOpAndCooldown(t *testing.T) {
	connectionImpl := &fakeClientConnection{
		invoke: func(context.Context, string) error {
			return status.Error(codes.Unavailable, "unavailable")
		},
	}
	initial := fakeTransport(connectionImpl)
	var recreateCalls atomic.Int32
	connection := newTestRecreatingClientConn(
		t,
		initial,
		func(_ context.Context, previous *clientTransport) (*clientTransport, error) {
			recreateCalls.Add(1)
			return previous, nil
		},
		1,
		time.Hour,
	)

	require.Error(t, connection.Invoke(context.Background(), "/test/fail", nil, nil))
	require.Eventually(t, func() bool {
		connection.mu.Lock()
		defer connection.mu.Unlock()
		return !connection.recreateInFlight && connection.consecutiveFailures == 0
	}, time.Second, time.Millisecond)
	require.Same(t, initial, connection.current)
	require.Zero(t, connectionImpl.closeCount.Load())

	require.Error(t, connection.Invoke(context.Background(), "/test/fail", nil, nil))
	require.Never(t, func() bool {
		return recreateCalls.Load() > 1
	}, 50*time.Millisecond, time.Millisecond)
}

func TestRecreatingClientConnSuccessAndApplicationErrorResetFailureCount(t *testing.T) {
	var callCount atomic.Int32
	connectionImpl := &fakeClientConnection{
		invoke: func(context.Context, string) error {
			switch callCount.Add(1) {
			case 2:
				return nil
			case 4:
				return status.Error(codes.InvalidArgument, "application error")
			default:
				return status.Error(codes.Unavailable, "unavailable")
			}
		},
	}
	var recreateCalls atomic.Int32
	connection := newTestRecreatingClientConn(
		t,
		fakeTransport(connectionImpl),
		func(_ context.Context, previous *clientTransport) (*clientTransport, error) {
			recreateCalls.Add(1)
			return previous, nil
		},
		2,
		0,
	)

	require.Error(t, connection.Invoke(context.Background(), "/test/fail", nil, nil))
	require.NoError(t, connection.Invoke(context.Background(), "/test/success", nil, nil))
	require.Error(t, connection.Invoke(context.Background(), "/test/fail", nil, nil))
	require.Equal(
		t,
		codes.InvalidArgument,
		status.Code(connection.Invoke(context.Background(), "/test/application-error", nil, nil)),
	)
	require.Error(t, connection.Invoke(context.Background(), "/test/fail", nil, nil))
	require.Never(t, func() bool {
		return recreateCalls.Load() != 0
	}, 50*time.Millisecond, time.Millisecond)
	connection.mu.Lock()
	defer connection.mu.Unlock()
	require.Equal(t, 1, connection.consecutiveFailures)
}

func TestRecreatingClientConnLongPollDeadlineDoesNotTriggerRecreation(t *testing.T) {
	connectionImpl := &fakeClientConnection{
		invoke: func(_ context.Context, method string) error {
			if method == "/test/fail" {
				return status.Error(codes.Unavailable, "unavailable")
			}
			return status.Error(codes.DeadlineExceeded, "long poll elapsed")
		},
	}
	var recreateCalls atomic.Int32
	connection := newTestRecreatingClientConn(
		t,
		fakeTransport(connectionImpl),
		func(_ context.Context, previous *clientTransport) (*clientTransport, error) {
			recreateCalls.Add(1)
			return previous, nil
		},
		2,
		0,
	)

	require.Error(t, connection.Invoke(context.Background(), "/test/fail", nil, nil))
	for _, method := range []string{
		protos.TaskHubSidecarService_WaitForInstanceStart_FullMethodName,
		protos.TaskHubSidecarService_WaitForInstanceCompletion_FullMethodName,
	} {
		require.Error(t, connection.Invoke(context.Background(), method, nil, nil))
	}
	require.Never(t, func() bool {
		return recreateCalls.Load() != 0
	}, 50*time.Millisecond, time.Millisecond)
	connection.mu.Lock()
	defer connection.mu.Unlock()
	require.Equal(t, 1, connection.consecutiveFailures)
}

func TestRecreatingClientConnRegularDeadlineTriggersRecreation(t *testing.T) {
	connectionImpl := &fakeClientConnection{
		invoke: func(context.Context, string) error {
			return status.Error(codes.DeadlineExceeded, "server deadline")
		},
	}
	recreated := make(chan struct{}, 1)
	connection := newTestRecreatingClientConn(
		t,
		fakeTransport(connectionImpl),
		func(_ context.Context, previous *clientTransport) (*clientTransport, error) {
			recreated <- struct{}{}
			return previous, nil
		},
		1,
		0,
	)

	require.Equal(t, codes.DeadlineExceeded, status.Code(connection.Invoke(
		context.Background(),
		protos.TaskHubSidecarService_GetInstance_FullMethodName,
		nil,
		nil,
	)))
	select {
	case <-recreated:
	case <-time.After(time.Second):
		t.Fatal("regular deadline did not trigger channel recreation")
	}
}

func TestRecreatingClientConnCallerDeadlineDoesNotTriggerRecreation(t *testing.T) {
	connectionImpl := &fakeClientConnection{
		invoke: func(ctx context.Context, method string) error {
			if method == "/test/fail" {
				return status.Error(codes.Unavailable, "unavailable")
			}
			<-ctx.Done()
			return status.FromContextError(ctx.Err()).Err()
		},
	}
	var recreateCalls atomic.Int32
	connection := newTestRecreatingClientConn(
		t,
		fakeTransport(connectionImpl),
		func(_ context.Context, previous *clientTransport) (*clientTransport, error) {
			recreateCalls.Add(1)
			return previous, nil
		},
		2,
		0,
	)

	require.Error(t, connection.Invoke(context.Background(), "/test/fail", nil, nil))
	ctx, cancel := context.WithTimeout(context.Background(), time.Nanosecond)
	defer cancel()
	<-ctx.Done()
	require.Equal(t, codes.DeadlineExceeded, status.Code(connection.Invoke(
		ctx,
		protos.TaskHubSidecarService_GetInstance_FullMethodName,
		nil,
		nil,
	)))
	require.Never(t, func() bool {
		return recreateCalls.Load() != 0
	}, 50*time.Millisecond, time.Millisecond)
	connection.mu.Lock()
	defer connection.mu.Unlock()
	require.Equal(t, 1, connection.consecutiveFailures)
}

func TestRecreatingClientConnStreamingOutcomesAffectFailureCount(t *testing.T) {
	streamFailure := status.Error(codes.Unavailable, "stream unavailable")
	connectionImpl := &fakeClientConnection{
		newStream: func(
			_ context.Context,
			_ string,
			options ...grpc.CallOption,
		) (grpc.ClientStream, error) {
			return nil, finishStreamCall(options, streamFailure)
		},
	}
	recreated := make(chan struct{}, 1)
	connection := newTestRecreatingClientConn(
		t,
		fakeTransport(connectionImpl),
		func(_ context.Context, previous *clientTransport) (*clientTransport, error) {
			recreated <- struct{}{}
			return previous, nil
		},
		1,
		0,
	)

	_, err := connection.NewStream(
		context.Background(),
		&grpc.StreamDesc{ServerStreams: true},
		protos.TaskHubSidecarService_StreamInstanceHistory_FullMethodName,
	)
	require.Equal(t, codes.Unavailable, status.Code(err))
	select {
	case <-recreated:
	case <-time.After(time.Second):
		t.Fatal("stream failure did not trigger channel recreation")
	}
}

func TestRecreatingClientConnSuccessfulStreamResetsFailureCount(t *testing.T) {
	callCount := 0
	connectionImpl := &fakeClientConnection{
		invoke: func(context.Context, string) error {
			return status.Error(codes.Unavailable, "unavailable")
		},
		newStream: func(
			_ context.Context,
			_ string,
			options ...grpc.CallOption,
		) (grpc.ClientStream, error) {
			callCount++
			_ = finishStreamCall(options, nil)
			return nil, nil
		},
	}
	var recreateCalls atomic.Int32
	connection := newTestRecreatingClientConn(
		t,
		fakeTransport(connectionImpl),
		func(_ context.Context, previous *clientTransport) (*clientTransport, error) {
			recreateCalls.Add(1)
			return previous, nil
		},
		2,
		0,
	)

	require.Error(t, connection.Invoke(context.Background(), "/test/fail", nil, nil))
	connection.mu.Lock()
	require.Equal(t, 1, connection.consecutiveFailures)
	connection.mu.Unlock()
	stream, err := connection.NewStream(
		context.Background(),
		&grpc.StreamDesc{ServerStreams: true},
		protos.TaskHubSidecarService_StreamInstanceHistory_FullMethodName,
	)
	require.NoError(t, err)
	require.Nil(t, stream)
	require.Equal(t, 1, callCount)
	connection.mu.Lock()
	require.Zero(t, connection.consecutiveFailures)
	connection.mu.Unlock()
	require.Error(t, connection.Invoke(context.Background(), "/test/fail", nil, nil))
	require.Never(t, func() bool {
		return recreateCalls.Load() != 0
	}, 50*time.Millisecond, time.Millisecond)
}

func finishStreamCall(options []grpc.CallOption, err error) error {
	for _, option := range options {
		if onFinish, ok := option.(grpc.OnFinishCallOption); ok {
			onFinish.OnFinish(err)
		}
	}
	return err
}

func TestRecreatingClientConnRequestCancellationDoesNotTriggerRecreation(t *testing.T) {
	connectionImpl := &fakeClientConnection{
		invoke: func(ctx context.Context, _ string) error {
			<-ctx.Done()
			return status.FromContextError(ctx.Err()).Err()
		},
	}
	var recreateCalls atomic.Int32
	connection := newTestRecreatingClientConn(
		t,
		fakeTransport(connectionImpl),
		func(_ context.Context, previous *clientTransport) (*clientTransport, error) {
			recreateCalls.Add(1)
			return previous, nil
		},
		1,
		0,
	)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.Equal(t, codes.Canceled, status.Code(connection.Invoke(ctx, "/test/cancel", nil, nil)))
	require.Never(t, func() bool {
		return recreateCalls.Load() != 0
	}, 50*time.Millisecond, time.Millisecond)
}

func TestRecreatingClientConnDefersRetiredChannelCloseAndIgnoresItsOutcome(t *testing.T) {
	blockingStarted := make(chan struct{})
	releaseBlocking := make(chan struct{})
	var startOnce sync.Once
	initialConnection := &fakeClientConnection{
		invoke: func(_ context.Context, method string) error {
			if method == "/test/block" {
				startOnce.Do(func() { close(blockingStarted) })
				<-releaseBlocking
				return nil
			}
			return status.Error(codes.Unavailable, "unavailable")
		},
	}
	replacementConnection := &fakeClientConnection{
		invoke: func(context.Context, string) error {
			return status.Error(codes.Unavailable, "replacement unavailable")
		},
	}
	replacement := fakeTransport(replacementConnection)
	connection := newTestRecreatingClientConn(
		t,
		fakeTransport(initialConnection),
		func(context.Context, *clientTransport) (*clientTransport, error) {
			return replacement, nil
		},
		1,
		time.Hour,
	)

	blockingDone := make(chan error, 1)
	go func() {
		blockingDone <- connection.Invoke(context.Background(), "/test/block", nil, nil)
	}()
	<-blockingStarted
	require.Error(t, connection.Invoke(context.Background(), "/test/fail", nil, nil))
	require.Eventually(t, func() bool {
		connection.mu.Lock()
		defer connection.mu.Unlock()
		return connection.current == replacement
	}, time.Second, time.Millisecond)
	require.Zero(t, initialConnection.closeCount.Load())
	require.Error(t, connection.Invoke(context.Background(), "/test/fail", nil, nil))

	close(releaseBlocking)
	require.NoError(t, <-blockingDone)
	connection.mu.Lock()
	require.Equal(t, 1, connection.consecutiveFailures)
	connection.mu.Unlock()
	require.Eventually(t, func() bool {
		return initialConnection.closeCount.Load() == 1
	}, time.Second, time.Millisecond)
}

func TestRecreatingClientConnDefersRetiredChannelCloseUntilStreamCompletes(t *testing.T) {
	server := &metadataServer{
		metadata: make(chan metadata.MD, 1),
		getErr:   status.Error(codes.Unavailable, "replace this channel"),
	}
	listener, stop := startBufconnServer(t, server)
	defer stop()
	grpcConnection, err := grpc.NewClient(
		"passthrough:///bufconn",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(bufconnDialer(listener)),
	)
	require.NoError(t, err)
	initialCloser := &closeRecorder{close: grpcConnection.Close}
	replacement := fakeTransport(&fakeClientConnection{})
	connection := newTestRecreatingClientConn(
		t,
		&clientTransport{connection: grpcConnection, closer: initialCloser},
		func(context.Context, *clientTransport) (*clientTransport, error) {
			return replacement, nil
		},
		1,
		0,
	)
	client := protos.NewTaskHubSidecarServiceClient(connection)

	streamCtx, cancelStream := context.WithCancel(context.Background())
	_, err = client.GetWorkItems(streamCtx, &protos.GetWorkItemsRequest{})
	require.NoError(t, err)
	<-server.metadata

	_, err = client.GetInstance(context.Background(), &protos.GetInstanceRequest{})
	require.Equal(t, codes.Unavailable, status.Code(err))
	require.Eventually(t, func() bool {
		connection.mu.Lock()
		defer connection.mu.Unlock()
		return connection.current == replacement
	}, time.Second, time.Millisecond)
	require.Zero(t, initialCloser.closeCount.Load())

	cancelStream()
	require.Eventually(t, func() bool {
		return initialCloser.closeCount.Load() == 1
	}, time.Second, time.Millisecond)
}

func TestRecreatingClientConnDisposalClosesLateReplacement(t *testing.T) {
	initialConnection := &fakeClientConnection{
		invoke: func(context.Context, string) error {
			return status.Error(codes.Unavailable, "unavailable")
		},
	}
	replacementConnection := &fakeClientConnection{}
	replacement := fakeTransport(replacementConnection)
	recreateStarted := make(chan struct{})
	connection := newTestRecreatingClientConn(
		t,
		fakeTransport(initialConnection),
		func(ctx context.Context, _ *clientTransport) (*clientTransport, error) {
			close(recreateStarted)
			<-ctx.Done()
			return replacement, nil
		},
		1,
		0,
	)

	require.Error(t, connection.Invoke(context.Background(), "/test/fail", nil, nil))
	<-recreateStarted
	require.NoError(t, connection.Close())
	require.Equal(t, int32(1), initialConnection.closeCount.Load())
	require.Equal(t, int32(1), replacementConnection.closeCount.Load())
	require.NoError(t, connection.Close())
}
