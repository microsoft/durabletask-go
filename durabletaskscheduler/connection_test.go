package durabletaskscheduler

import (
	"context"
	"errors"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/microsoft/durabletask-go/api"
	durabletaskclient "github.com/microsoft/durabletask-go/client"
	"github.com/microsoft/durabletask-go/internal/largepayload"
	"github.com/microsoft/durabletask-go/internal/protos"
	"github.com/microsoft/durabletask-go/payload"
	"github.com/microsoft/durabletask-go/task"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

type recordingCredential struct {
	mu      sync.Mutex
	options []policy.TokenRequestOptions
}

type failingCredential struct{}

type blockingCredential struct {
	mu      sync.Mutex
	calls   int
	started chan struct{}
	release chan struct{}
	token   azcore.AccessToken
	err     error
}

func newBlockingCredential() *blockingCredential {
	return &blockingCredential{
		started: make(chan struct{}, 1),
		release: make(chan struct{}),
		token:   azcore.AccessToken{Token: "token", ExpiresOn: time.Now().Add(time.Hour)},
	}
}

func (c *blockingCredential) GetToken(
	ctx context.Context,
	_ policy.TokenRequestOptions,
) (azcore.AccessToken, error) {
	c.mu.Lock()
	c.calls++
	c.mu.Unlock()
	select {
	case c.started <- struct{}{}:
	default:
	}
	select {
	case <-ctx.Done():
		return azcore.AccessToken{}, ctx.Err()
	case <-c.release:
		return c.token, c.err
	}
}

func (c *blockingCredential) callCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.calls
}

func newTestSchedulerCredentials(credential azcore.TokenCredential) *schedulerPerRPCCredentials {
	return &schedulerPerRPCCredentials{
		credential: credential,
		scope:      "https://durabletask.io/.default",
		taskHub:    "hub",
		userAgent:  "agent",
	}
}

func callSchedulerCredentialsConcurrently(
	perRPC *schedulerPerRPCCredentials,
	callers int,
) <-chan error {
	start := make(chan struct{})
	errs := make(chan error, callers)
	for range callers {
		go func() {
			<-start
			_, err := perRPC.GetRequestMetadata(context.Background())
			errs <- err
		}()
	}
	close(start)
	return errs
}

func (failingCredential) GetToken(
	context.Context,
	policy.TokenRequestOptions,
) (azcore.AccessToken, error) {
	return azcore.AccessToken{}, errors.New("temporary token failure")
}

func (c *recordingCredential) GetToken(
	_ context.Context,
	options policy.TokenRequestOptions,
) (azcore.AccessToken, error) {
	c.mu.Lock()
	c.options = append(c.options, options)
	c.mu.Unlock()
	return azcore.AccessToken{Token: "token", ExpiresOn: time.Now().Add(time.Hour)}, nil
}

type metadataServer struct {
	protos.UnimplementedTaskHubSidecarServiceServer

	metadata chan metadata.MD
	helloErr error
	getErr   error
	state    *protos.OrchestrationState
}

func (s *metadataServer) GetInstance(
	ctx context.Context,
	_ *protos.GetInstanceRequest,
) (*protos.GetInstanceResponse, error) {
	incoming, _ := metadata.FromIncomingContext(ctx)
	s.metadata <- incoming
	if s.getErr != nil {
		return nil, s.getErr
	}
	return &protos.GetInstanceResponse{Exists: true, OrchestrationState: s.state}, nil
}

type recreationDataConverter struct{}

func (recreationDataConverter) Serialize(value any) (string, error) {
	text, ok := value.(string)
	if !ok {
		return "", errors.New("recreation converter only supports strings")
	}
	return "recreated:" + text, nil
}

func (recreationDataConverter) Deserialize(value string, target any) error {
	text, ok := target.(*string)
	if !ok {
		return errors.New("recreation converter target must be *string")
	}
	if !strings.HasPrefix(value, "recreated:") {
		return errors.New("recreation converter prefix is missing")
	}
	*text = strings.TrimPrefix(value, "recreated:")
	return nil
}

func (s *metadataServer) Hello(ctx context.Context, _ *emptypb.Empty) (*emptypb.Empty, error) {
	if s.helloErr != nil {
		return nil, s.helloErr
	}
	incoming, _ := metadata.FromIncomingContext(ctx)
	s.metadata <- incoming
	return &emptypb.Empty{}, nil
}

func (s *metadataServer) GetWorkItems(
	_ *protos.GetWorkItemsRequest,
	stream protos.TaskHubSidecarService_GetWorkItemsServer,
) error {
	incoming, _ := metadata.FromIncomingContext(stream.Context())
	s.metadata <- incoming
	<-stream.Context().Done()
	return nil
}

func recreationMetadataInterceptor(
	ctx context.Context,
	method string,
	request any,
	reply any,
	connection *grpc.ClientConn,
	invoker grpc.UnaryInvoker,
	options ...grpc.CallOption,
) error {
	ctx = metadata.AppendToOutgoingContext(ctx, "x-recreation-interceptor", "preserved")
	return invoker(ctx, method, request, reply, connection, options...)
}

func startBufconnServer(t *testing.T, server protos.TaskHubSidecarServiceServer) (*bufconn.Listener, func()) {
	t.Helper()
	listener := bufconn.Listen(1024 * 1024)
	grpcServer := grpc.NewServer()
	protos.RegisterTaskHubSidecarServiceServer(grpcServer, server)
	go func() {
		_ = grpcServer.Serve(listener)
	}()
	return listener, func() {
		grpcServer.Stop()
		require.NoError(t, listener.Close())
	}
}

func bufconnDialer(listener *bufconn.Listener) func(context.Context, string) (net.Conn, error) {
	return func(context.Context, string) (net.Conn, error) {
		return listener.Dial()
	}
}

func TestSchedulerCredentialsUseExpectedScope(t *testing.T) {
	credential := &recordingCredential{}
	perRPC := &schedulerPerRPCCredentials{
		credential: credential,
		scope:      "https://durabletask.io/.default",
		taskHub:    "hub",
		userAgent:  "agent",
		workerID:   "worker",
	}

	values, err := perRPC.GetRequestMetadata(context.Background())
	require.NoError(t, err)
	require.Equal(t, "hub", values["taskhub"])
	require.Equal(t, "agent", values["x-user-agent"])
	require.Equal(t, "worker", values["workerid"])
	require.Equal(t, "Bearer token", values["authorization"])
	require.True(t, perRPC.RequireTransportSecurity())

	credential.mu.Lock()
	defer credential.mu.Unlock()
	require.Equal(t, []string{"https://durabletask.io/.default"}, credential.options[0].Scopes)
}

func TestSchedulerCredentialFailureIsTransient(t *testing.T) {
	perRPC := newTestSchedulerCredentials(failingCredential{})
	_, err := perRPC.GetRequestMetadata(context.Background())
	require.Equal(t, codes.Unavailable, status.Code(err))
	require.ErrorContains(t, err, "temporary token failure")
}

func TestSchedulerCredentialsCacheAccessToken(t *testing.T) {
	credential := &recordingCredential{}
	perRPC := newTestSchedulerCredentials(credential)

	first, err := perRPC.GetRequestMetadata(context.Background())
	require.NoError(t, err)
	second, err := perRPC.GetRequestMetadata(context.Background())
	require.NoError(t, err)
	require.Equal(t, first["authorization"], second["authorization"])

	credential.mu.Lock()
	defer credential.mu.Unlock()
	require.Len(t, credential.options, 1)
}

func TestSchedulerCredentialsCoalesceConcurrentTokenRequests(t *testing.T) {
	credential := newBlockingCredential()
	perRPC := newTestSchedulerCredentials(credential)

	const callers = 64
	errs := callSchedulerCredentialsConcurrently(perRPC, callers)
	<-credential.started
	close(credential.release)
	for range callers {
		require.NoError(t, <-errs)
	}
	require.Equal(t, 1, credential.callCount())
}

func TestSchedulerCredentialsCoalesceShortLivedTokenRequests(t *testing.T) {
	credential := newBlockingCredential()
	credential.token.ExpiresOn = time.Time{}
	perRPC := newTestSchedulerCredentials(credential)

	const callers = 64
	errs := callSchedulerCredentialsConcurrently(perRPC, callers)
	<-credential.started
	close(credential.release)
	for range callers {
		require.NoError(t, <-errs)
	}
	require.Equal(t, 1, credential.callCount())
}

func TestSchedulerCredentialsCoalesceRefreshFailure(t *testing.T) {
	credential := newBlockingCredential()
	credential.err = errors.New("token unavailable")
	perRPC := newTestSchedulerCredentials(credential)

	const callers = 64
	errs := callSchedulerCredentialsConcurrently(perRPC, callers)
	<-credential.started
	close(credential.release)
	for range callers {
		require.ErrorContains(t, <-errs, "token unavailable")
	}
	_, err := perRPC.GetRequestMetadata(context.Background())
	require.ErrorContains(t, err, "token unavailable")
	require.Equal(t, 1, credential.callCount())
}

func TestSchedulerCredentialsCanceledWaiterDoesNotBlockOnRefresh(t *testing.T) {
	credential := newBlockingCredential()
	perRPC := newTestSchedulerCredentials(credential)

	firstDone := make(chan error, 1)
	go func() {
		_, err := perRPC.GetRequestMetadata(context.Background())
		firstDone <- err
	}()
	<-credential.started

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := perRPC.GetRequestMetadata(ctx)
	require.Equal(t, codes.Unavailable, status.Code(err))
	require.ErrorContains(t, err, context.Canceled.Error())

	close(credential.release)
	require.NoError(t, <-firstDone)
}

func TestSchedulerCredentialsCanceledLeaderDoesNotCancelSharedRefresh(t *testing.T) {
	credential := newBlockingCredential()
	perRPC := newTestSchedulerCredentials(credential)

	leaderCtx, cancelLeader := context.WithCancel(context.Background())
	leaderDone := make(chan error, 1)
	go func() {
		_, err := perRPC.GetRequestMetadata(leaderCtx)
		leaderDone <- err
	}()
	<-credential.started

	waiterDone := make(chan error, 1)
	go func() {
		_, err := perRPC.GetRequestMetadata(context.Background())
		waiterDone <- err
	}()
	cancelLeader()
	require.ErrorContains(t, <-leaderDone, context.Canceled.Error())

	select {
	case err := <-waiterDone:
		t.Fatalf("shared refresh ended with leader context: %v", err)
	case <-time.After(10 * time.Millisecond):
	}
	close(credential.release)
	require.NoError(t, <-waiterDone)
	require.Equal(t, 1, credential.callCount())
}

func TestSchedulerCredentialsUseValidTokenWhenRefreshFails(t *testing.T) {
	perRPC := newTestSchedulerCredentials(failingCredential{})
	perRPC.token.Store(perRPC.newCredentialState(azcore.AccessToken{
		Token:     "cached",
		ExpiresOn: time.Now().Add(time.Minute),
	}))

	values, err := perRPC.GetRequestMetadata(context.Background())
	require.NoError(t, err)
	require.Equal(t, "Bearer cached", values["authorization"])
}

func TestSchedulerCredentialsRefreshesStaleTokenWithoutMutatingOldMetadata(t *testing.T) {
	credential := &recordingCredential{}
	perRPC := newTestSchedulerCredentials(credential)
	oldState := perRPC.newCredentialState(azcore.AccessToken{
		Token:     "old",
		ExpiresOn: time.Now().Add(time.Hour),
	})
	oldState.refreshAfter = time.Now().Add(-time.Second)
	perRPC.token.Store(oldState)

	values, err := perRPC.GetRequestMetadata(context.Background())
	require.NoError(t, err)
	require.Equal(t, oldState.metadata["authorization"], values["authorization"])
	require.Eventually(t, func() bool {
		return perRPC.token.Load() != oldState
	}, time.Second, time.Millisecond)
	values, err = perRPC.GetRequestMetadata(context.Background())
	require.NoError(t, err)
	require.NotEqual(t, oldState.metadata["authorization"], values["authorization"])
	require.Equal(t, "Bearer old", oldState.metadata["authorization"])
	credential.mu.Lock()
	defer credential.mu.Unlock()
	require.Len(t, credential.options, 1)
}

func TestSchedulerCredentialStateHonorsRefreshOnAndExpiryBuffer(t *testing.T) {
	perRPC := newTestSchedulerCredentials(&recordingCredential{})
	expiresOn := time.Now().Add(time.Hour)
	refreshOn := time.Now().Add(10 * time.Minute)

	withRefreshOn := perRPC.newCredentialState(azcore.AccessToken{
		Token:     "token",
		ExpiresOn: expiresOn,
		RefreshOn: refreshOn,
	})
	require.Equal(t, refreshOn, withRefreshOn.refreshAfter)

	withoutRefreshOn := perRPC.newCredentialState(azcore.AccessToken{
		Token:     "token",
		ExpiresOn: expiresOn,
	})
	require.Equal(t, expiresOn.Add(-accessTokenRefreshBuffer), withoutRefreshOn.refreshAfter)
}

func TestSchedulerCredentialsDoNotUseExpiredTokenWhenRefreshFails(t *testing.T) {
	perRPC := newTestSchedulerCredentials(failingCredential{})
	perRPC.token.Store(perRPC.newCredentialState(azcore.AccessToken{
		Token:     "expired",
		ExpiresOn: time.Now().Add(-time.Minute),
	}))

	_, err := perRPC.GetRequestMetadata(context.Background())
	require.Equal(t, codes.Unavailable, status.Code(err))
	require.ErrorContains(t, err, "temporary token failure")
}

func TestSchedulerCredentialsRejectExpiredCredentialToken(t *testing.T) {
	credential := newBlockingCredential()
	credential.token.ExpiresOn = time.Now().Add(-time.Minute)
	close(credential.release)
	perRPC := newTestSchedulerCredentials(credential)

	_, err := perRPC.GetRequestMetadata(context.Background())
	require.Equal(t, codes.Unavailable, status.Code(err))
	require.ErrorContains(t, err, "expired access token")
	_, err = perRPC.GetRequestMetadata(context.Background())
	require.Equal(t, codes.Unavailable, status.Code(err))
	require.Equal(t, 1, credential.callCount())
}

func TestSchedulerCredentialsUseValidTokenWhileRefreshing(t *testing.T) {
	credential := newBlockingCredential()
	credential.token.Token = "refreshed"
	perRPC := newTestSchedulerCredentials(credential)
	cached := perRPC.newCredentialState(azcore.AccessToken{
		Token:     "cached",
		ExpiresOn: time.Now().Add(time.Hour),
	})
	cached.refreshAfter = time.Now().Add(-time.Second)
	perRPC.token.Store(cached)

	values, err := perRPC.GetRequestMetadata(context.Background())
	require.NoError(t, err)
	require.Equal(t, "Bearer cached", values["authorization"])
	<-credential.started
	close(credential.release)
	require.Eventually(t, func() bool {
		return perRPC.token.Load() != cached
	}, time.Second, time.Millisecond)
}

func BenchmarkSchedulerCredentialsCachedToken(b *testing.B) {
	perRPC := &schedulerPerRPCCredentials{
		credential: &recordingCredential{},
		scope:      "https://durabletask.io/.default",
		taskHub:    "hub",
		userAgent:  "agent",
		workerID:   "worker",
	}
	if _, err := perRPC.GetRequestMetadata(context.Background()); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if _, err := perRPC.GetRequestMetadata(context.Background()); err != nil {
				b.Error(err)
			}
		}
	})
}

func TestConfiguredClientAndWorkerUseSeparateMetadataAndConnections(t *testing.T) {
	server := &metadataServer{metadata: make(chan metadata.MD, 4)}
	listener, stop := startBufconnServer(t, server)
	defer stop()

	options, err := NewOptionsFromConnectionString(
		"Endpoint=http://bufconn;TaskHub=default;Authentication=None",
	)
	require.NoError(t, err)
	options.WorkerID = "worker-id"
	options.dialer = bufconnDialer(listener)

	managementClient, err := NewClient(context.Background(), options, api.DefaultLogger())
	require.NoError(t, err)
	defer func() {
		require.NoError(t, managementClient.Close())
	}()
	clientMetadata := <-server.metadata
	require.Equal(t, []string{"default"}, clientMetadata.Get("taskhub"))
	require.Empty(t, clientMetadata.Get("workerid"))
	require.Contains(t, clientMetadata.Get("x-user-agent")[0], "DurableTaskClient")

	worker, err := NewWorker(
		options,
		task.NewTaskRegistry(),
		api.DefaultLogger(),
		durabletaskclient.WithWorkerSilentDisconnectTimeout(time.Second),
	)
	require.NoError(t, err)
	require.NoError(t, worker.Start(context.Background()))
	workerHelloMetadata := <-server.metadata
	workerStreamMetadata := <-server.metadata
	for _, incoming := range []metadata.MD{workerHelloMetadata, workerStreamMetadata} {
		require.Equal(t, []string{"default"}, incoming.Get("taskhub"))
		require.Equal(t, []string{"worker-id"}, incoming.Get("workerid"))
		require.True(t, strings.Contains(incoming.Get("x-user-agent")[0], "DurableTaskWorker"))
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	require.NoError(t, worker.Shutdown(shutdownCtx))
}

func TestClientCloseStopsCompatibilityListener(t *testing.T) {
	server := &metadataServer{metadata: make(chan metadata.MD, 4)}
	listener, stop := startBufconnServer(t, server)
	defer stop()

	options, err := NewOptionsFromConnectionString(
		"Endpoint=http://bufconn;TaskHub=default;Authentication=None",
	)
	require.NoError(t, err)
	options.dialer = bufconnDialer(listener)
	managementClient, err := NewClient(context.Background(), options, api.DefaultLogger())
	require.NoError(t, err)
	<-server.metadata
	require.NoError(t, managementClient.StartWorkItemListener(context.Background(), task.NewTaskRegistry()))
	<-server.metadata
	<-server.metadata
	require.NoError(t, managementClient.Close())

	shutdownCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	require.NoError(t, managementClient.StopWorkItemListener(shutdownCtx))
}

func TestNewClientFailsFastWhenHelloIsRejected(t *testing.T) {
	server := &metadataServer{
		metadata: make(chan metadata.MD, 1),
		helloErr: status.Error(codes.PermissionDenied, "forbidden"),
	}
	listener, stop := startBufconnServer(t, server)
	defer stop()

	options, err := NewOptionsFromConnectionString(
		"Endpoint=http://bufconn;TaskHub=default;Authentication=None",
	)
	require.NoError(t, err)
	options.dialer = bufconnDialer(listener)

	managementClient, err := NewClient(context.Background(), options, api.DefaultLogger())
	require.Nil(t, managementClient)
	require.ErrorContains(t, err, "Hello")
	require.ErrorContains(t, err, "PermissionDenied")
}

func TestNewClientReceivesMessagesLargerThanGrpcDefault(t *testing.T) {
	const payloadSize = 5 * 1024 * 1024
	server := &metadataServer{
		metadata: make(chan metadata.MD, 4),
		state: &protos.OrchestrationState{
			InstanceId:           "large-message",
			Name:                 "orchestrator",
			OrchestrationStatus:  protos.OrchestrationStatus_ORCHESTRATION_STATUS_COMPLETED,
			CreatedTimestamp:     timestamppb.Now(),
			LastUpdatedTimestamp: timestamppb.Now(),
			Input:                wrapperspb.String(strings.Repeat("x", payloadSize)),
		},
	}
	listener, stop := startBufconnServer(t, server)
	defer stop()

	options := NewOptions("http://bufconn", "default")
	options.Authentication = AuthenticationNone
	options.AllowInsecureConnection = true
	options.dialer = bufconnDialer(listener)
	managementClient, err := NewClient(context.Background(), options, api.DefaultLogger())
	require.NoError(t, err)
	defer func() {
		require.NoError(t, managementClient.Close())
	}()

	result, err := managementClient.FetchOrchestrationMetadata(
		context.Background(),
		"large-message",
		api.WithFetchPayloads(true),
	)
	require.NoError(t, err)
	require.Len(t, result.SerializedInput, payloadSize)

	limitedOptions := *options
	limitedOptions.MaxReceiveMessageSize = 1024 * 1024
	limitedClient, err := NewClient(context.Background(), &limitedOptions, api.DefaultLogger())
	require.NoError(t, err)
	defer func() {
		require.NoError(t, limitedClient.Close())
	}()
	_, err = limitedClient.FetchOrchestrationMetadata(
		context.Background(),
		"large-message",
		api.WithFetchPayloads(true),
	)
	require.Equal(t, codes.ResourceExhausted, status.Code(err))
}

func TestNewClientRecreatesChannelAndPreservesConfiguration(t *testing.T) {
	firstServer := &metadataServer{
		getErr:   status.Error(codes.Unavailable, "replace this channel"),
		metadata: make(chan metadata.MD, 16),
	}
	firstListener, stopFirst := startBufconnServer(t, firstServer)
	defer stopFirst()

	converter := recreationDataConverter{}
	store := payload.NewMemoryStore()
	largePayloads := &api.LargePayloadOptions{
		Store:           store,
		Resolver:        store,
		ThresholdBytes:  1,
		MaxPayloadBytes: 1024,
	}
	serializedOutput, err := converter.Serialize("preserved")
	require.NoError(t, err)
	externalizedOutput, err := largepayload.Externalize(
		context.Background(),
		largePayloads,
		wrapperspb.String(serializedOutput),
	)
	require.NoError(t, err)
	secondServer := &metadataServer{
		state: &protos.OrchestrationState{
			InstanceId:           "recreated-instance",
			Name:                 "orchestrator",
			OrchestrationStatus:  protos.OrchestrationStatus_ORCHESTRATION_STATUS_COMPLETED,
			CreatedTimestamp:     timestamppb.Now(),
			LastUpdatedTimestamp: timestamppb.Now(),
			Output:               externalizedOutput,
		},
		metadata: make(chan metadata.MD, 16),
	}
	secondListener, stopSecond := startBufconnServer(t, secondServer)
	defer stopSecond()

	var listenerMu sync.RWMutex
	activeListener := firstListener
	options, err := NewOptionsFromConnectionString(
		"Endpoint=http://bufconn;TaskHub=default;Authentication=None",
	)
	require.NoError(t, err)
	options.ChannelRecreateFailureThreshold = 1
	options.ChannelRecreateMinInterval = 0
	options.DataConverter = converter
	options.LargePayloads = largePayloads
	options.UnaryInterceptors = []grpc.UnaryClientInterceptor{recreationMetadataInterceptor}
	options.dialer = func(context.Context, string) (net.Conn, error) {
		listenerMu.RLock()
		listener := activeListener
		listenerMu.RUnlock()
		return listener.Dial()
	}

	client, err := NewClient(context.Background(), options, api.DefaultLogger())
	require.NoError(t, err)
	defer func() {
		require.NoError(t, client.Close())
	}()
	<-firstServer.metadata

	listenerMu.Lock()
	activeListener = secondListener
	listenerMu.Unlock()

	_, err = client.FetchOrchestrationMetadata(
		context.Background(),
		"recreated-instance",
		api.WithFetchPayloads(true),
	)
	require.Error(t, err)

	var orchestration *api.OrchestrationMetadata
	require.Eventually(t, func() bool {
		orchestration, err = client.FetchOrchestrationMetadata(
			context.Background(),
			"recreated-instance",
			api.WithFetchPayloads(true),
		)
		return err == nil
	}, 5*time.Second, 10*time.Millisecond)
	var output string
	require.NoError(t, orchestration.ReadOutput(&output))
	require.Equal(t, "preserved", output)

	replacementMetadata := <-secondServer.metadata
	require.Equal(t, []string{"preserved"}, replacementMetadata.Get("x-recreation-interceptor"))
	require.Equal(t, []string{"default"}, replacementMetadata.Get("taskhub"))
	require.Contains(t, replacementMetadata.Get("x-user-agent")[0], "DurableTaskClient")
}

func TestNewClientNilLoggerSupportsRecoveryAndClose(t *testing.T) {
	server := &metadataServer{
		metadata: make(chan metadata.MD, 8),
		state:    &protos.OrchestrationState{InstanceId: "nil-logger"},
	}
	listener, stop := startBufconnServer(t, server)
	t.Cleanup(stop)
	options := insecureBufconnOptions(t, listener)
	options.ChannelRecreateFailureThreshold = 1
	options.ChannelRecreateMinInterval = 0
	var failFirst atomic.Bool
	failFirst.Store(true)
	options.UnaryInterceptors = []grpc.UnaryClientInterceptor{
		func(ctx context.Context, method string, request, response any, conn *grpc.ClientConn,
			invoker grpc.UnaryInvoker, callOptions ...grpc.CallOption) error {
			if method == protos.TaskHubSidecarService_GetInstance_FullMethodName && failFirst.Swap(false) {
				return status.Error(codes.Unavailable, "replace this channel")
			}
			return invoker(ctx, method, request, response, conn, callOptions...)
		},
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	client, err := NewClient(ctx, options, nil)
	require.NoError(t, err)
	t.Cleanup(func() {
		closed := make(chan error, 1)
		go func() { closed <- client.Close() }()
		select {
		case err := <-closed:
			require.NoError(t, err)
		case <-time.After(time.Second):
			t.Error("management client Close did not finish")
		}
	})
	initial := client.connection.current
	require.NotPanics(t, func() {
		_, err = client.FetchOrchestrationMetadata(ctx, "nil-logger")
	})
	require.Equal(t, codes.Unavailable, status.Code(err))
	require.Eventually(t, func() bool {
		client.connection.mu.Lock()
		defer client.connection.mu.Unlock()
		return client.connection.current != initial
	}, time.Second, time.Millisecond)
	result, err := client.FetchOrchestrationMetadata(ctx, "nil-logger")
	require.NoError(t, err)
	require.Equal(t, api.InstanceID("nil-logger"), result.InstanceID)
}

// delayedHelloServer blocks Hello so client and worker deadlines can be observed.
type delayedHelloServer struct {
	protos.UnimplementedTaskHubSidecarServiceServer

	delay time.Duration
}

func (s *delayedHelloServer) Hello(ctx context.Context, _ *emptypb.Empty) (*emptypb.Empty, error) {
	select {
	case <-time.After(s.delay):
		return &emptypb.Empty{}, nil
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	}
}

func (s *delayedHelloServer) GetWorkItems(
	_ *protos.GetWorkItemsRequest,
	stream protos.TaskHubSidecarService_GetWorkItemsServer,
) error {
	<-stream.Context().Done()
	return nil
}

// flakyHelloServer fails Hello with UNAVAILABLE a fixed number of times so the
// client channel retry policy can be observed.
type flakyHelloServer struct {
	protos.UnimplementedTaskHubSidecarServiceServer

	mu       sync.Mutex
	failures int
	attempts int
	lastMeta metadata.MD
}

func (s *flakyHelloServer) Hello(ctx context.Context, _ *emptypb.Empty) (*emptypb.Empty, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.attempts++
	s.lastMeta, _ = metadata.FromIncomingContext(ctx)
	if s.attempts <= s.failures {
		return nil, status.Error(codes.Unavailable, "try again")
	}
	return &emptypb.Empty{}, nil
}

func (s *flakyHelloServer) attemptCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.attempts
}

func insecureBufconnOptions(t *testing.T, listener *bufconn.Listener) *Options {
	t.Helper()
	options, err := NewOptionsFromConnectionString(
		"Endpoint=http://bufconn;TaskHub=default;Authentication=None",
	)
	require.NoError(t, err)
	options.dialer = bufconnDialer(listener)
	return options
}

// TestNewPerRPCCredentialsPreservesRoleMetadata pins the metadata each role
// sends, including the user-agent override and worker identity.
func TestNewPerRPCCredentialsPreservesRoleMetadata(t *testing.T) {
	tests := []struct {
		name          string
		role          connectionRole
		workerID      string
		userAgent     string
		resourceID    string
		credential    azcore.TokenCredential
		wantUserAgent string
		wantScope     string
	}{
		{
			name:          "client default user agent",
			role:          clientRole,
			wantUserAgent: "durabletask-go/" + sdkVersion() + " (DurableTaskClient)",
			wantScope:     "https://durabletask.io/.default",
		},
		{
			name:          "worker default user agent",
			role:          workerRole,
			workerID:      "worker-1",
			wantUserAgent: "durabletask-go/" + sdkVersion() + " (DurableTaskWorker)",
			wantScope:     "https://durabletask.io/.default",
		},
		{
			name:          "client user agent override",
			role:          clientRole,
			userAgent:     "contoso-app/2.1",
			wantUserAgent: "contoso-app/2.1",
			wantScope:     "https://durabletask.io/.default",
		},
		{
			name:          "worker user agent override",
			role:          workerRole,
			workerID:      "worker-2",
			userAgent:     "contoso-app/2.1",
			wantUserAgent: "contoso-app/2.1",
			wantScope:     "https://durabletask.io/.default",
		},
		{
			name:          "custom resource ID with credential",
			role:          clientRole,
			credential:    &recordingCredential{},
			resourceID:    "https://custom.example.com/",
			wantUserAgent: "durabletask-go/" + sdkVersion() + " (DurableTaskClient)",
			wantScope:     "https://custom.example.com/.default",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			options := NewOptions("scheduler.example.com", "hub")
			options.UserAgent = tt.userAgent
			options.Credential = tt.credential
			if tt.resourceID != "" {
				options.ResourceID = tt.resourceID
			}

			perRPC := newPerRPCCredentials(options, tt.role, tt.workerID)
			require.Equal(t, "hub", perRPC.taskHub)
			require.Equal(t, tt.workerID, perRPC.workerID)
			require.Equal(t, tt.wantUserAgent, perRPC.userAgent)
			require.Equal(t, tt.wantScope, perRPC.scope)
			require.Equal(t, tt.credential, perRPC.credential)
			require.Equal(t, tt.credential != nil, perRPC.RequireTransportSecurity())

			values, err := perRPC.GetRequestMetadata(context.Background())
			require.NoError(t, err)
			require.Equal(t, "hub", values["taskhub"])
			require.Equal(t, tt.wantUserAgent, values["x-user-agent"])
			if tt.workerID == "" {
				require.NotContains(t, values, "workerid")
			} else {
				require.Equal(t, tt.workerID, values["workerid"])
			}
			if tt.credential == nil {
				require.NotContains(t, values, "authorization")
			} else {
				require.Contains(t, values["authorization"], "Bearer ")
			}
		})
	}
}

func TestSchedulerCredentialsOmitAuthorizationWithoutCredential(t *testing.T) {
	perRPC := &schedulerPerRPCCredentials{taskHub: "hub", userAgent: "agent"}
	values, err := perRPC.GetRequestMetadata(context.Background())
	require.NoError(t, err)
	require.Equal(t, map[string]string{"taskhub": "hub", "x-user-agent": "agent"}, values)
	require.False(t, perRPC.RequireTransportSecurity())
}

// TestClientChannelRetriesUnavailable exercises the client-only retry service
// config: a transparent retry recovers a single Hello call.
func TestClientChannelRetriesUnavailable(t *testing.T) {
	server := &flakyHelloServer{failures: 3}
	listener, stop := startBufconnServer(t, server)
	defer stop()

	options := insecureBufconnOptions(t, listener)
	connection, err := connect(options, clientRole, "")
	require.NoError(t, err)
	defer func() {
		require.NoError(t, connection.Close())
	}()

	_, err = protos.NewTaskHubSidecarServiceClient(connection).Hello(context.Background(), &emptypb.Empty{})
	require.NoError(t, err)
	require.Equal(t, 4, server.attemptCount())
	require.Equal(t, []string{"default"}, server.lastMeta.Get("taskhub"))
}

// TestClientChannelRetriesStopAtMaxAttempts pins maxAttempts at five.
func TestClientChannelRetriesStopAtMaxAttempts(t *testing.T) {
	server := &flakyHelloServer{failures: 10}
	listener, stop := startBufconnServer(t, server)
	defer stop()

	options := insecureBufconnOptions(t, listener)
	connection, err := connect(options, clientRole, "")
	require.NoError(t, err)
	defer func() {
		require.NoError(t, connection.Close())
	}()

	_, err = protos.NewTaskHubSidecarServiceClient(connection).Hello(context.Background(), &emptypb.Empty{})
	require.Equal(t, codes.Unavailable, status.Code(err))
	require.Equal(t, 5, server.attemptCount())
}

// TestWorkerChannelDoesNotUseClientRetryConfig confirms the retry service config
// is applied to the client role only; worker resilience is the worker's own
// reconnect loop.
func TestWorkerChannelDoesNotUseClientRetryConfig(t *testing.T) {
	server := &flakyHelloServer{failures: 3}
	listener, stop := startBufconnServer(t, server)
	defer stop()

	options := insecureBufconnOptions(t, listener)
	connection, err := connect(options, workerRole, "worker-1")
	require.NoError(t, err)
	defer func() {
		require.NoError(t, connection.Close())
	}()

	_, err = protos.NewTaskHubSidecarServiceClient(connection).Hello(context.Background(), &emptypb.Empty{})
	require.Equal(t, codes.Unavailable, status.Code(err))
	require.Equal(t, 1, server.attemptCount())
	require.Equal(t, []string{"worker-1"}, server.lastMeta.Get("workerid"))
}

// TestNewClientHelloUsesConfiguredTimeout proves Options.HelloTimeout bounds the
// client fail-fast handshake.
func TestNewClientHelloUsesConfiguredTimeout(t *testing.T) {
	listener, stop := startBufconnServer(t, &delayedHelloServer{delay: 5 * time.Second})
	defer stop()

	options := insecureBufconnOptions(t, listener)
	options.HelloTimeout = 100 * time.Millisecond

	start := time.Now()
	client, err := NewClient(context.Background(), options, api.DefaultLogger())
	require.Nil(t, client)
	require.ErrorContains(t, err, "DTS client Hello failed")
	require.Equal(t, codes.DeadlineExceeded, status.Code(errors.Unwrap(err)))
	require.Less(t, time.Since(start), 3*time.Second)
}

// TestNewClientHelloSucceedsWithinTimeout is the positive counterpart: the same
// delayed server is accepted when the deadline allows it.
func TestNewClientHelloSucceedsWithinTimeout(t *testing.T) {
	listener, stop := startBufconnServer(t, &delayedHelloServer{delay: 50 * time.Millisecond})
	defer stop()

	options := insecureBufconnOptions(t, listener)
	options.HelloTimeout = 10 * time.Second

	client, err := NewClient(context.Background(), options, api.DefaultLogger())
	require.NoError(t, err)
	require.NoError(t, client.Close())
}

// TestNewClientHonorsCallerContextDeadline confirms the caller context still
// bounds the handshake when it is shorter than HelloTimeout.
func TestNewClientHonorsCallerContextDeadline(t *testing.T) {
	listener, stop := startBufconnServer(t, &delayedHelloServer{delay: 5 * time.Second})
	defer stop()

	options := insecureBufconnOptions(t, listener)
	options.HelloTimeout = 30 * time.Second

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	client, err := NewClient(ctx, options, api.DefaultLogger())
	require.Nil(t, client)
	require.ErrorContains(t, err, "DTS client Hello failed")
}

// TestNewWorkerHelloUsesConfiguredTimeout proves Options.HelloTimeout reaches the
// worker connection factory.
func TestNewWorkerHelloUsesConfiguredTimeout(t *testing.T) {
	listener, stop := startBufconnServer(t, &delayedHelloServer{delay: 5 * time.Second})
	defer stop()

	options := insecureBufconnOptions(t, listener)
	options.HelloTimeout = 100 * time.Millisecond

	worker, err := NewWorker(options, task.NewTaskRegistry(), api.DefaultLogger())
	require.NoError(t, err)

	start := time.Now()
	err = worker.Start(context.Background())
	require.ErrorContains(t, err, "Hello failed")
	require.Equal(t, codes.DeadlineExceeded, status.Code(errors.Unwrap(err)))
	require.Less(t, time.Since(start), 3*time.Second)
	require.False(t, worker.Running())
}

// TestNewWorkerHelloTimeoutOptionOverridesOptions confirms caller-supplied worker
// options are applied after the DTS-derived defaults.
func TestNewWorkerHelloTimeoutOptionOverridesOptions(t *testing.T) {
	listener, stop := startBufconnServer(t, &delayedHelloServer{delay: 5 * time.Second})
	defer stop()

	options := insecureBufconnOptions(t, listener)
	options.HelloTimeout = 30 * time.Second

	worker, err := NewWorker(
		options,
		task.NewTaskRegistry(),
		api.DefaultLogger(),
		durabletaskclient.WithWorkerHelloTimeout(100*time.Millisecond),
	)
	require.NoError(t, err)

	start := time.Now()
	require.ErrorContains(t, worker.Start(context.Background()), "Hello failed")
	require.Less(t, time.Since(start), 3*time.Second)
}

// TestNewClientAndNewWorkerRejectInvalidOptions confirms both entry points share
// the same validation before any connection is created.
func TestNewClientAndNewWorkerRejectInvalidOptions(t *testing.T) {
	tests := []struct {
		name    string
		options *Options
		wantErr string
	}{
		{name: "nil options", options: nil, wantErr: "options are required"},
		{
			name:    "missing task hub",
			options: &Options{EndpointAddress: "scheduler.example.com", Authentication: AuthenticationNone},
			wantErr: "task hub name is required",
		},
		{
			name: "plaintext without opt-in",
			options: &Options{
				EndpointAddress: "http://127.0.0.1:8080",
				TaskHubName:     "hub",
				Authentication:  AuthenticationNone,
			},
			wantErr: "AllowInsecureConnection",
		},
		{
			name: "identity fields with None",
			options: &Options{
				EndpointAddress:         "http://127.0.0.1:8080",
				TaskHubName:             "hub",
				Authentication:          AuthenticationNone,
				AllowInsecureConnection: true,
				TenantID:                "tenant",
			},
			wantErr: "Authentication None does not use TenantID",
		},
		{
			name: "worker ID injection",
			options: &Options{
				EndpointAddress:         "http://127.0.0.1:8080",
				TaskHubName:             "hub",
				Authentication:          AuthenticationNone,
				AllowInsecureConnection: true,
				WorkerID:                "worker\r\nid",
			},
			wantErr: "worker ID",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client, err := NewClient(context.Background(), tt.options, api.DefaultLogger())
			require.Nil(t, client)
			require.ErrorContains(t, err, tt.wantErr)

			worker, err := NewWorker(tt.options, task.NewTaskRegistry(), api.DefaultLogger())
			require.Nil(t, worker)
			require.ErrorContains(t, err, tt.wantErr)
		})
	}
}

// TestNewWorkerGeneratesStableWorkerIDWhenUnset covers the default worker
// identity shape and its reuse across reconnects.
func TestNewWorkerGeneratesStableWorkerIDWhenUnset(t *testing.T) {
	server := &metadataServer{metadata: make(chan metadata.MD, 4)}
	listener, stop := startBufconnServer(t, server)
	defer stop()

	options := insecureBufconnOptions(t, listener)
	worker, err := NewWorker(
		options,
		task.NewTaskRegistry(),
		api.DefaultLogger(),
		durabletaskclient.WithWorkerSilentDisconnectTimeout(time.Second),
	)
	require.NoError(t, err)
	require.NoError(t, worker.Start(context.Background()))

	helloMetadata := <-server.metadata
	streamMetadata := <-server.metadata
	workerIDs := helloMetadata.Get("workerid")
	require.Len(t, workerIDs, 1)
	require.Equal(t, workerIDs, streamMetadata.Get("workerid"))
	workerIDParts := strings.Split(workerIDs[0], ",")
	require.Len(t, workerIDParts, 3)
	require.NotEmpty(t, workerIDParts[0])

	shutdownCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	require.NoError(t, worker.Shutdown(shutdownCtx))
}

func TestDefaultWorkerIDIsUniquePerCall(t *testing.T) {
	first := defaultWorkerID()
	second := defaultWorkerID()
	require.NotEqual(t, first, second)
	firstParts := strings.Split(first, ",")
	secondParts := strings.Split(second, ",")
	require.Len(t, firstParts, 3)
	require.Equal(t, secondParts[0], firstParts[0])
	require.Equal(t, secondParts[1], firstParts[1])
	require.Len(t, firstParts[2], 32)
}
