package backend

import (
	context "context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/cenkalti/backoff/v4"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/microsoft/durabletask-go/api"
	"github.com/microsoft/durabletask-go/internal/helpers"
	"github.com/microsoft/durabletask-go/internal/protos"
)

var emptyCompleteTaskResponse = &protos.CompleteTaskResponse{}

var errShuttingDown error = status.Error(codes.Canceled, "shutting down")

type ExecutionResults struct {
	Response *protos.OrchestratorResponse
	complete chan interface{}
}

type activityExecutionResult struct {
	response *protos.ActivityResponse
	complete chan interface{}
}

type Executor interface {
	ExecuteOrchestrator(ctx context.Context, iid api.InstanceID, oldEvents []*protos.HistoryEvent, newEvents []*protos.HistoryEvent) (*ExecutionResults, error)

	ExecuteActivity(context.Context, api.InstanceID, *protos.HistoryEvent) (*protos.HistoryEvent, error)
}

type grpcExecutor struct {
	protos.UnimplementedTaskHubSidecarServiceServer
	workItemQueue        chan *protos.WorkItem
	pendingOrchestrators *sync.Map // map[api.InstanceID]*ExecutionResults
	pendingActivities    *sync.Map // map[string]*activityExecutionResult
	backend              Backend
	logger               Logger
	onWorkItemConnection func(context.Context) error
	streamShutdownChan   <-chan any
}

type grpcExecutorOptions func(g *grpcExecutor)

// IsDurableTaskGrpcRequest returns true if the specified gRPC method name represents an operation
// that is compatible with the gRPC executor.
func IsDurableTaskGrpcRequest(fullMethodName string) bool {
	return strings.Index(fullMethodName, "/TaskHubSidecarService") == 0
}

// WithOnGetWorkItemsConnectionCallback allows the caller to get a notification when an external process
// connects over gRPC and invokes the GetWorkItems operation. This can be useful for doing things like
// lazily auto-starting the task hub worker only when necessary.
func WithOnGetWorkItemsConnectionCallback(callback func(context.Context) error) grpcExecutorOptions {
	return func(g *grpcExecutor) {
		g.onWorkItemConnection = callback
	}
}

func WithStreamShutdownChannel(c <-chan any) grpcExecutorOptions {
	return func(g *grpcExecutor) {
		g.streamShutdownChan = c
	}
}

func NewGrpcExecutor(grpcServer *grpc.Server, be Backend, logger Logger, opts ...grpcExecutorOptions) Executor {
	executor := &grpcExecutor{
		workItemQueue:        make(chan *protos.WorkItem),
		backend:              be,
		logger:               logger,
		pendingOrchestrators: &sync.Map{},
		pendingActivities:    &sync.Map{},
	}

	for _, opt := range opts {
		opt(executor)
	}

	protos.RegisterTaskHubSidecarServiceServer(grpcServer, executor)
	return executor
}

// ExecuteOrchestrator implements Executor
func (executor *grpcExecutor) ExecuteOrchestrator(ctx context.Context, iid api.InstanceID, oldEvents []*protos.HistoryEvent, newEvents []*protos.HistoryEvent) (*ExecutionResults, error) {
	result := &ExecutionResults{complete: make(chan interface{})}
	executor.pendingOrchestrators.Store(iid, result)
	defer executor.pendingOrchestrators.Delete(iid)

	// Queue up the orchestration execution work-item for the connected worker to process
	executor.workItemQueue <- &protos.WorkItem{
		Request: &protos.WorkItem_OrchestratorRequest{
			OrchestratorRequest: &protos.OrchestratorRequest{
				InstanceId:  string(iid),
				ExecutionId: nil,
				PastEvents:  oldEvents,
				NewEvents:   newEvents,
			},
		},
	}

	// Wait for the connected worker to signal that it's done executing the work-item
	// TODO: Timeout logic - i.e. handle the case where we never hear back from the remote worker (due to a hang, etc.).
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-result.complete:
	}

	return result, nil
}

// ExecuteActivity implements Executor
func (executor grpcExecutor) ExecuteActivity(ctx context.Context, iid api.InstanceID, e *protos.HistoryEvent) (*protos.HistoryEvent, error) {
	key := getActivityExecutionKey(string(iid), e.EventId)
	result := &activityExecutionResult{complete: make(chan interface{})}
	executor.pendingActivities.Store(key, result)
	defer executor.pendingActivities.Delete(key)

	task := e.GetTaskScheduled()
	executor.workItemQueue <- &protos.WorkItem{
		Request: &protos.WorkItem_ActivityRequest{
			ActivityRequest: &protos.ActivityRequest{
				Name:                  task.Name,
				Version:               task.Version,
				Input:                 task.Input,
				OrchestrationInstance: &protos.OrchestrationInstance{InstanceId: string(iid)},
				TaskId:                e.EventId,
			},
		},
	}

	// Wait for the connected worker to signal that it's done executing the work-item
	// TODO: Timeout logic
	select {
	case <-ctx.Done():
	case <-result.complete:
	}

	var responseEvent *protos.HistoryEvent
	if failureDetails := result.response.GetFailureDetails(); failureDetails != nil {
		responseEvent = helpers.NewTaskFailedEvent(result.response.TaskId, result.response.FailureDetails)
	} else {
		responseEvent = helpers.NewTaskCompletedEvent(result.response.TaskId, result.response.Result)
	}

	return responseEvent, nil
}

// Shutdown implements Executor
func (g *grpcExecutor) Shutdown(ctx context.Context) {
	// closing the work item queue is a signal for shutdown
	close(g.workItemQueue)
}

// Hello implements protos.TaskHubSidecarServiceServer
func (grpcExecutor) Hello(ctx context.Context, empty *emptypb.Empty) (*emptypb.Empty, error) {
	return empty, nil
}

// GetWorkItems implements protos.TaskHubSidecarServiceServer
func (g *grpcExecutor) GetWorkItems(req *protos.GetWorkItemsRequest, stream protos.TaskHubSidecarService_GetWorkItemsServer) error {
	if md, ok := metadata.FromIncomingContext(stream.Context()); ok {
		g.logger.Infof("work item stream established by user-agent: %v", md.Get("user-agent"))
	}

	// There are some cases where the app may need to be notified when a client connects to fetch work items, like
	// for auto-starting the worker. The app also has an opportunity to set itself as unavailable by returning an error.
	callback := g.onWorkItemConnection
	if callback != nil {
		if err := callback(stream.Context()); err != nil {
			message := fmt.Sprint("unable to establish work item stream at this time: ", err)
			g.logger.Warn(message)
			return status.Errorf(codes.Unavailable, message)
		}
	}

	// The worker client invokes this method, which streams back work-items as they arrive.
	for {
		select {
		case <-stream.Context().Done():
			g.logger.Infof("work item stream closed")
			return nil
		case wi := <-g.workItemQueue:
			if err := stream.Send(wi); err != nil {
				return err
			}
		case <-g.streamShutdownChan:
			return errShuttingDown
		}
	}
}

// CompleteOrchestratorTask implements protos.TaskHubSidecarServiceServer
func (g *grpcExecutor) CompleteOrchestratorTask(ctx context.Context, res *protos.OrchestratorResponse) (*protos.CompleteTaskResponse, error) {
	iid := api.InstanceID(res.InstanceId)
	if p, ok := g.pendingOrchestrators.Load(iid); ok {
		pending := p.(*ExecutionResults)
		pending.Response = res
		pending.complete <- true
		return emptyCompleteTaskResponse, nil
	}

	return emptyCompleteTaskResponse, fmt.Errorf("unknown instance ID: %s", res.InstanceId)
}

// CompleteActivityTask implements protos.TaskHubSidecarServiceServer
func (g *grpcExecutor) CompleteActivityTask(ctx context.Context, res *protos.ActivityResponse) (*protos.CompleteTaskResponse, error) {
	key := getActivityExecutionKey(res.InstanceId, res.TaskId)
	if p, ok := g.pendingActivities.Load(key); ok {
		pending := p.(*activityExecutionResult)
		pending.response = res
		pending.complete <- true
		return emptyCompleteTaskResponse, nil
	}

	return emptyCompleteTaskResponse, fmt.Errorf("unknown instance ID/task ID combo: %s", key)
}

func getActivityExecutionKey(iid string, taskID int32) string {
	return fmt.Sprintf("%s/%d", iid, taskID)
}

// CreateTaskHub implements protos.TaskHubSidecarServiceServer
func (grpcExecutor) CreateTaskHub(context.Context, *protos.CreateTaskHubRequest) (*protos.CreateTaskHubResponse, error) {
	return nil, errors.New("unimplemented")
}

// DeleteTaskHub implements protos.TaskHubSidecarServiceServer
func (grpcExecutor) DeleteTaskHub(context.Context, *protos.DeleteTaskHubRequest) (*protos.DeleteTaskHubResponse, error) {
	return nil, errors.New("unimplemented")
}

// GetInstance implements protos.TaskHubSidecarServiceServer
func (g *grpcExecutor) GetInstance(ctx context.Context, req *protos.GetInstanceRequest) (*protos.GetInstanceResponse, error) {
	metadata, err := g.backend.GetOrchestrationMetadata(ctx, api.InstanceID(req.InstanceId))
	if err != nil {
		return nil, err
	}
	if metadata == nil {
		return &protos.GetInstanceResponse{Exists: false}, nil
	}

	return createGetInstanceResponse(req, metadata), nil
}

// PurgeInstances implements protos.TaskHubSidecarServiceServer
func (g *grpcExecutor) PurgeInstances(ctx context.Context, req *protos.PurgeInstancesRequest) (*protos.PurgeInstancesResponse, error) {
	if req.GetPurgeInstanceFilter() != nil {
		return nil, errors.New("multi-instance purge is not unimplemented")
	}

	if err := g.backend.PurgeOrchestrationState(ctx, api.InstanceID(req.GetInstanceId())); err != nil {
		return nil, err
	}
	return &protos.PurgeInstancesResponse{DeletedInstanceCount: 1}, nil
}

// QueryInstances implements protos.TaskHubSidecarServiceServer
func (grpcExecutor) QueryInstances(context.Context, *protos.QueryInstancesRequest) (*protos.QueryInstancesResponse, error) {
	return nil, errors.New("unimplemented")
}

// RaiseEvent implements protos.TaskHubSidecarServiceServer
func (g *grpcExecutor) RaiseEvent(ctx context.Context, req *protos.RaiseEventRequest) (*protos.RaiseEventResponse, error) {
	e := helpers.NewEventRaisedEvent(req.Name, req.Input)
	if err := g.backend.AddNewOrchestrationEvent(ctx, api.InstanceID(req.InstanceId), e); err != nil {
		return nil, err
	}

	return &protos.RaiseEventResponse{}, nil
}

// StartInstance implements protos.TaskHubSidecarServiceServer
func (g *grpcExecutor) StartInstance(ctx context.Context, req *protos.CreateInstanceRequest) (*protos.CreateInstanceResponse, error) {
	instanceID := req.InstanceId
	ctx, span := helpers.StartNewCreateOrchestrationSpan(ctx, req.Name, req.Version.GetValue(), instanceID)
	defer span.End()

	e := helpers.NewExecutionStartedEvent(req.Name, instanceID, req.Input, nil, helpers.TraceContextFromSpan(span))
	if err := g.backend.CreateOrchestrationInstance(ctx, e); err != nil {
		return nil, err
	}

	return &protos.CreateInstanceResponse{InstanceId: instanceID}, nil
}

// TerminateInstance implements protos.TaskHubSidecarServiceServer
func (g *grpcExecutor) TerminateInstance(ctx context.Context, req *protos.TerminateRequest) (*protos.TerminateResponse, error) {
	e := helpers.NewExecutionTerminatedEvent(req.Output)
	if err := g.backend.AddNewOrchestrationEvent(ctx, api.InstanceID(req.InstanceId), e); err != nil {
		return nil, err
	}

	return &protos.TerminateResponse{}, nil
}

// SuspendInstance implements protos.TaskHubSidecarServiceServer
func (g *grpcExecutor) SuspendInstance(ctx context.Context, req *protos.SuspendRequest) (*protos.SuspendResponse, error) {
	e := helpers.NewSuspendOrchestrationEvent(req.Reason.GetValue())
	if err := g.backend.AddNewOrchestrationEvent(ctx, api.InstanceID(req.InstanceId), e); err != nil {
		return nil, err
	}

	return &protos.SuspendResponse{}, nil
}

// ResumeInstance implements protos.TaskHubSidecarServiceServer
func (g *grpcExecutor) ResumeInstance(ctx context.Context, req *protos.ResumeRequest) (*protos.ResumeResponse, error) {
	e := helpers.NewResumeOrchestrationEvent(req.Reason.GetValue())
	if err := g.backend.AddNewOrchestrationEvent(ctx, api.InstanceID(req.InstanceId), e); err != nil {
		return nil, err
	}

	return &protos.ResumeResponse{}, nil
}

// WaitForInstanceCompletion implements protos.TaskHubSidecarServiceServer
func (g *grpcExecutor) WaitForInstanceCompletion(ctx context.Context, req *protos.GetInstanceRequest) (*protos.GetInstanceResponse, error) {
	return g.waitForInstance(ctx, req, func(m *api.OrchestrationMetadata) bool {
		return m.IsComplete()
	})
}

// WaitForInstanceStart implements protos.TaskHubSidecarServiceServer
func (g *grpcExecutor) WaitForInstanceStart(ctx context.Context, req *protos.GetInstanceRequest) (*protos.GetInstanceResponse, error) {
	return g.waitForInstance(ctx, req, func(m *api.OrchestrationMetadata) bool {
		return m.RuntimeStatus != protos.OrchestrationStatus_ORCHESTRATION_STATUS_PENDING
	})
}

func (g *grpcExecutor) waitForInstance(ctx context.Context, req *protos.GetInstanceRequest, condition func(*api.OrchestrationMetadata) bool) (*protos.GetInstanceResponse, error) {
	iid := api.InstanceID(req.InstanceId)

	b := backoff.ExponentialBackOff{
		InitialInterval:     1 * time.Millisecond,
		MaxInterval:         3 * time.Second,
		Multiplier:          1.5,
		RandomizationFactor: 0.5,
		Stop:                backoff.Stop,
		Clock:               backoff.SystemClock,
	}
	b.Reset()

	ticker := backoff.NewTicker(&b)
	defer ticker.Stop()

loop:
	for range ticker.C {
		select {
		case <-ctx.Done():
			break loop
		default:
			metadata, err := g.backend.GetOrchestrationMetadata(ctx, iid)
			if err != nil {
				return nil, err
			}
			if metadata == nil {
				return &protos.GetInstanceResponse{Exists: false}, nil
			}
			if condition(metadata) {
				return createGetInstanceResponse(req, metadata), nil
			}
		}
	}

	return nil, status.Errorf(codes.Canceled, "instance hasn't completed")
}

// mustEmbedUnimplementedTaskHubSidecarServiceServer implements protos.TaskHubSidecarServiceServer
func (grpcExecutor) mustEmbedUnimplementedTaskHubSidecarServiceServer() {
}

func createGetInstanceResponse(req *protos.GetInstanceRequest, metadata *api.OrchestrationMetadata) *protos.GetInstanceResponse {
	state := &protos.OrchestrationState{
		InstanceId:           req.InstanceId,
		Name:                 metadata.Name,
		OrchestrationStatus:  metadata.RuntimeStatus,
		CreatedTimestamp:     timestamppb.New(metadata.CreatedAt),
		LastUpdatedTimestamp: timestamppb.New(metadata.LastUpdatedAt),
	}

	if req.GetInputsAndOutputs {
		state.Input = wrapperspb.String(metadata.SerializedInput)
		state.CustomStatus = wrapperspb.String(metadata.SerializedCustomStatus)
		state.Output = wrapperspb.String(metadata.SerializedOutput)
		state.FailureDetails = metadata.FailureDetails
	}

	return &protos.GetInstanceResponse{Exists: true, OrchestrationState: state}
}
