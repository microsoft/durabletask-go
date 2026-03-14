package durabletaskscheduler

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"

	"github.com/microsoft/durabletask-go/api"
	"github.com/microsoft/durabletask-go/backend"
	"github.com/microsoft/durabletask-go/internal/protos"
)

type Backend struct {
	options    *Options
	logger     backend.Logger
	workerName string
	client     protos.TaskHubSidecarServiceClient
	conn       *grpc.ClientConn
	credential azcore.TokenCredential
	mu         sync.Mutex
	started    bool
}

// NewBackend creates a new Backend backed by a Durable Task Scheduler (DTS) service.
func NewBackend(opts *Options, logger backend.Logger) *Backend {
	hostname, _ := os.Hostname()
	pid := os.Getpid()
	workerName := fmt.Sprintf("%s,%d,%s", hostname, pid, uuid.NewString())

	return &Backend{
		options:    opts,
		logger:     logger,
		workerName: workerName,
	}
}

// CreateTaskHub implements backend.Backend.
// For DTS, task hubs are managed externally, so this is a no-op.
func (be *Backend) CreateTaskHub(ctx context.Context) error {
	return nil
}

// DeleteTaskHub implements backend.Backend.
// For DTS, task hubs are managed externally, so this is a no-op.
func (be *Backend) DeleteTaskHub(ctx context.Context) error {
	return nil
}

// Start implements backend.Backend.
func (be *Backend) Start(ctx context.Context) error {
	be.mu.Lock()
	defer be.mu.Unlock()

	if be.started {
		return backend.ErrBackendAlreadyStarted
	}

	conn, err := be.createConnection()
	if err != nil {
		return fmt.Errorf("failed to connect to DTS endpoint: %w", err)
	}

	be.conn = conn
	be.client = protos.NewTaskHubSidecarServiceClient(conn)
	be.started = true
	be.logger.Infof("durabletaskscheduler backend started: %s (task hub: %s)", be.options.EndpointAddress, be.options.TaskHubName)
	return nil
}

// Stop implements backend.Backend.
func (be *Backend) Stop(ctx context.Context) error {
	be.mu.Lock()
	defer be.mu.Unlock()

	if !be.started {
		return nil
	}

	be.started = false
	if be.conn != nil {
		if err := be.conn.Close(); err != nil {
			return fmt.Errorf("failed to close gRPC connection: %w", err)
		}
		be.conn = nil
	}
	be.logger.Info("durabletaskscheduler backend stopped")
	return nil
}

// CreateOrchestrationInstance implements backend.Backend.
func (be *Backend) CreateOrchestrationInstance(ctx context.Context, e *backend.HistoryEvent, opts ...backend.OrchestrationIdReusePolicyOptions) error {
	if err := be.ensureStarted(); err != nil {
		return err
	}

	startEvent := e.GetExecutionStarted()
	if startEvent == nil {
		return backend.ErrNotExecutionStarted
	}

	req := &protos.CreateInstanceRequest{
		Name:       startEvent.Name,
		InstanceId: startEvent.OrchestrationInstance.GetInstanceId(),
		Input:      startEvent.Input,
	}

	if startEvent.GetScheduledStartTimestamp() != nil {
		req.ScheduledStartTimestamp = startEvent.GetScheduledStartTimestamp()
	}

	policy := &protos.OrchestrationIdReusePolicy{}
	for _, opt := range opts {
		if err := opt(policy); err != nil {
			return fmt.Errorf("failed to configure orchestration ID reuse policy: %w", err)
		}
	}
	if len(policy.OperationStatus) > 0 || policy.Action != 0 {
		req.OrchestrationIdReusePolicy = policy
	}

	_, err := be.client.StartInstance(ctx, req)
	if err != nil {
		return fmt.Errorf("failed to create orchestration instance: %w", err)
	}
	return nil
}

// AddNewOrchestrationEvent implements backend.Backend.
func (be *Backend) AddNewOrchestrationEvent(ctx context.Context, iid api.InstanceID, e *backend.HistoryEvent) error {
	if err := be.ensureStarted(); err != nil {
		return err
	}
	if e == nil {
		return backend.ErrNilHistoryEvent
	}

	// Route to the appropriate gRPC call based on event type
	if et := e.GetExecutionTerminated(); et != nil {
		_, err := be.client.TerminateInstance(ctx, &protos.TerminateRequest{
			InstanceId: string(iid),
			Output:     et.Input,
			Recursive:  et.Recurse,
		})
		if err != nil {
			return fmt.Errorf("failed to terminate instance: %w", err)
		}
		return nil
	}

	if er := e.GetEventRaised(); er != nil {
		_, err := be.client.RaiseEvent(ctx, &protos.RaiseEventRequest{
			InstanceId: string(iid),
			Name:       er.Name,
			Input:      er.Input,
		})
		if err != nil {
			return fmt.Errorf("failed to raise event: %w", err)
		}
		return nil
	}

	if e.GetExecutionSuspended() != nil {
		_, err := be.client.SuspendInstance(ctx, &protos.SuspendRequest{
			InstanceId: string(iid),
		})
		if err != nil {
			return fmt.Errorf("failed to suspend instance: %w", err)
		}
		return nil
	}

	if e.GetExecutionResumed() != nil {
		_, err := be.client.ResumeInstance(ctx, &protos.ResumeRequest{
			InstanceId: string(iid),
		})
		if err != nil {
			return fmt.Errorf("failed to resume instance: %w", err)
		}
		return nil
	}

	return fmt.Errorf("unsupported orchestration event type: %v", e)
}

// GetOrchestrationWorkItem implements backend.Backend.
func (be *Backend) GetOrchestrationWorkItem(ctx context.Context) (*backend.OrchestrationWorkItem, error) {
	// DTS uses a streaming model via GetWorkItems, which is handled by the executor layer.
	// This method is not used when the DTS scheduler is the backend since work items
	// are dispatched through the gRPC streaming protocol.
	return nil, backend.ErrNoWorkItems
}

// GetOrchestrationRuntimeState implements backend.Backend.
func (be *Backend) GetOrchestrationRuntimeState(ctx context.Context, wi *backend.OrchestrationWorkItem) (*backend.OrchestrationRuntimeState, error) {
	if err := be.ensureStarted(); err != nil {
		return nil, err
	}

	// The runtime state is populated from the work item's events, which are provided
	// by the DTS service via the GetWorkItems stream.
	return backend.NewOrchestrationRuntimeState(wi.InstanceID, wi.NewEvents), nil
}

// GetOrchestrationMetadata implements backend.Backend.
func (be *Backend) GetOrchestrationMetadata(ctx context.Context, iid api.InstanceID) (*api.OrchestrationMetadata, error) {
	if err := be.ensureStarted(); err != nil {
		return nil, err
	}

	resp, err := be.client.GetInstance(ctx, &protos.GetInstanceRequest{
		InstanceId:          string(iid),
		GetInputsAndOutputs: true,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to get orchestration metadata: %w", err)
	}

	if !resp.Exists || resp.OrchestrationState == nil {
		return nil, api.ErrInstanceNotFound
	}

	state := resp.OrchestrationState
	metadata := api.NewOrchestrationMetadata(
		iid,
		state.Name,
		state.OrchestrationStatus,
		state.CreatedTimestamp.AsTime(),
		state.LastUpdatedTimestamp.AsTime(),
		state.Input.GetValue(),
		state.Output.GetValue(),
		state.CustomStatus.GetValue(),
		state.FailureDetails,
	)

	return metadata, nil
}

// CompleteOrchestrationWorkItem implements backend.Backend.
func (be *Backend) CompleteOrchestrationWorkItem(ctx context.Context, wi *backend.OrchestrationWorkItem) error {
	// Work item completion is handled by the executor layer via CompleteOrchestratorTask gRPC call.
	// The scheduler backend doesn't need to do additional processing here.
	return nil
}

// AbandonOrchestrationWorkItem implements backend.Backend.
func (be *Backend) AbandonOrchestrationWorkItem(ctx context.Context, wi *backend.OrchestrationWorkItem) error {
	// Work item abandonment is handled by the DTS service automatically when the
	// streaming connection is dropped or the work item is not completed within the lock timeout.
	return nil
}

// GetActivityWorkItem implements backend.Backend.
func (be *Backend) GetActivityWorkItem(ctx context.Context) (*backend.ActivityWorkItem, error) {
	// Similar to GetOrchestrationWorkItem, activity work items are dispatched through
	// the gRPC streaming protocol by the DTS service.
	return nil, backend.ErrNoWorkItems
}

// CompleteActivityWorkItem implements backend.Backend.
func (be *Backend) CompleteActivityWorkItem(ctx context.Context, wi *backend.ActivityWorkItem) error {
	// Activity completion is handled by the executor layer via CompleteActivityTask gRPC call.
	return nil
}

// AbandonActivityWorkItem implements backend.Backend.
func (be *Backend) AbandonActivityWorkItem(ctx context.Context, wi *backend.ActivityWorkItem) error {
	// Activity abandonment is handled automatically by the DTS service.
	return nil
}

// PurgeOrchestrationState implements backend.Backend.
func (be *Backend) PurgeOrchestrationState(ctx context.Context, id api.InstanceID) error {
	if err := be.ensureStarted(); err != nil {
		return err
	}

	resp, err := be.client.PurgeInstances(ctx, &protos.PurgeInstancesRequest{
		Request: &protos.PurgeInstancesRequest_InstanceId{InstanceId: string(id)},
	})
	if err != nil {
		return fmt.Errorf("failed to purge orchestration state: %w", err)
	}

	if resp.GetDeletedInstanceCount() == 0 {
		return api.ErrInstanceNotFound
	}
	return nil
}

// String implements fmt.Stringer for logging.
func (be *Backend) String() string {
	return fmt.Sprintf("durabletaskscheduler::%s/%s", be.options.EndpointAddress, be.options.TaskHubName)
}

func (be *Backend) ensureStarted() error {
	be.mu.Lock()
	defer be.mu.Unlock()
	if !be.started {
		return backend.ErrNotInitialized
	}
	return nil
}

// Connection returns the underlying gRPC client connection. This can be used to create
// a [client.TaskHubGrpcClient] for streaming work item processing.
// Must be called after Start().
func (be *Backend) Connection() (grpc.ClientConnInterface, error) {
	if err := be.ensureStarted(); err != nil {
		return nil, err
	}
	return be.conn, nil
}

func (be *Backend) createConnection() (*grpc.ClientConn, error) {
	target := be.options.EndpointAddress
	isTLS := strings.HasPrefix(target, "https://")
	target = strings.TrimPrefix(target, "https://")
	target = strings.TrimPrefix(target, "http://")

	// Add default port if not specified
	if !strings.Contains(target, ":") {
		if isTLS {
			target += ":443"
		} else {
			target += ":80"
		}
	}

	var transportCreds credentials.TransportCredentials
	if isTLS {
		transportCreds = credentials.NewTLS(nil)
	} else {
		transportCreds = insecure.NewCredentials()
	}

	// Set up Azure credential if using DefaultAzure auth
	switch be.options.AuthType {
	case AuthTypeDefaultAzure:
		cred, err := azidentity.NewDefaultAzureCredential(nil)
		if err != nil {
			return nil, fmt.Errorf("failed to create Azure credential: %w", err)
		}
		be.credential = cred
	case AuthTypeNone:
		// No authentication needed
	default:
		return nil, fmt.Errorf("unsupported authentication type: %q", be.options.AuthType)
	}

	dialOpts := []grpc.DialOption{
		grpc.WithTransportCredentials(transportCreds),
		grpc.WithUnaryInterceptor(be.unaryMetadataInterceptor()),
		grpc.WithStreamInterceptor(be.streamMetadataInterceptor()),
	}

	conn, err := grpc.NewClient("dns:///"+target, dialOpts...)
	if err != nil {
		return nil, fmt.Errorf("failed to create gRPC connection: %w", err)
	}

	return conn, nil
}

func (be *Backend) unaryMetadataInterceptor() grpc.UnaryClientInterceptor {
	return func(
		ctx context.Context,
		method string,
		req, reply any,
		cc *grpc.ClientConn,
		invoker grpc.UnaryInvoker,
		opts ...grpc.CallOption,
	) error {
		ctx, err := be.injectMetadata(ctx)
		if err != nil {
			return fmt.Errorf("failed to inject metadata: %w", err)
		}
		return invoker(ctx, method, req, reply, cc, opts...)
	}
}

func (be *Backend) streamMetadataInterceptor() grpc.StreamClientInterceptor {
	return func(
		ctx context.Context,
		desc *grpc.StreamDesc,
		cc *grpc.ClientConn,
		method string,
		streamer grpc.Streamer,
		opts ...grpc.CallOption,
	) (grpc.ClientStream, error) {
		ctx, err := be.injectMetadata(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to inject metadata: %w", err)
		}
		return streamer(ctx, desc, cc, method, opts...)
	}
}

func (be *Backend) injectMetadata(ctx context.Context) (context.Context, error) {
	pairs := []string{
		"taskhub", be.options.TaskHubName,
		"workerid", be.workerName,
	}

	if be.credential != nil {
		token, err := be.credential.GetToken(ctx, policy.TokenRequestOptions{
			Scopes: []string{be.options.ResourceID + "/.default"},
		})
		if err != nil {
			return ctx, fmt.Errorf("failed to get access token: %w", err)
		}
		pairs = append(pairs, "authorization", "Bearer "+token.Token)
	}

	md := metadata.Pairs(pairs...)
	return metadata.NewOutgoingContext(ctx, md), nil
}
