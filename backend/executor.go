package backend

import (
	context "context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/dapr/durabletask-go/api"
	"github.com/dapr/durabletask-go/api/helpers"
	"github.com/dapr/durabletask-go/api/protos"
	"github.com/dapr/kit/concurrency"
)

var emptyCompleteTaskResponse = &protos.CompleteTaskResponse{}

var errShuttingDown error = status.Error(codes.Canceled, "shutting down")

type ExecutionResults struct {
	Response *protos.OrchestratorResponse
	complete chan struct{}
	pending  chan string
}

type activityExecutionResult struct {
	response *protos.ActivityResponse
	complete chan struct{}
	pending  chan string
}

type Executor interface {
	ExecuteOrchestrator(ctx context.Context, iid api.InstanceID, oldEvents []*protos.HistoryEvent, newEvents []*protos.HistoryEvent) (*ExecutionResults, error)
	ExecuteActivity(context.Context, api.InstanceID, *protos.HistoryEvent) (*protos.HistoryEvent, error)
	Shutdown(ctx context.Context) error
}

type grpcExecutor struct {
	protos.UnimplementedTaskHubSidecarServiceServer
	workItemQueue        chan *protos.WorkItem
	pendingOrchestrators *sync.Map // map[api.InstanceID]*ExecutionResults
	pendingActivities    *sync.Map // map[string]*activityExecutionResult
	backend              Backend
	logger               Logger
	onWorkItemConnection func(context.Context) error
	onWorkItemDisconnect func(context.Context) error
	streamShutdownChan   <-chan any
}

type grpcExecutorOptions func(g *grpcExecutor)

// IsDurableTaskGrpcRequest returns true if the specified gRPC method name represents an operation
// that is compatible with the gRPC executor.
func IsDurableTaskGrpcRequest(fullMethodName string) bool {
	return strings.HasPrefix(fullMethodName, "/TaskHubSidecarService/")
}

// WithOnGetWorkItemsConnectionCallback allows the caller to get a notification when an external process
// connects over gRPC and invokes the GetWorkItems operation. This can be useful for doing things like
// lazily auto-starting the task hub worker only when necessary.
func WithOnGetWorkItemsConnectionCallback(callback func(context.Context) error) grpcExecutorOptions {
	return func(g *grpcExecutor) {
		g.onWorkItemConnection = callback
	}
}

// WithOnGetWorkItemsDisconnectCallback allows the caller to get a notification when an external process
// disconnects from the GetWorkItems operation. This can be useful for doing things like shutting down
// the task hub worker when the client disconnects.
func WithOnGetWorkItemsDisconnectCallback(callback func(context.Context) error) grpcExecutorOptions {
	return func(g *grpcExecutor) {
		g.onWorkItemDisconnect = callback
	}
}

func WithStreamShutdownChannel(c <-chan any) grpcExecutorOptions {
	return func(g *grpcExecutor) {
		g.streamShutdownChan = c
	}
}

// NewGrpcExecutor returns the Executor object and a method to invoke to register the gRPC server in the executor.
func NewGrpcExecutor(be Backend, logger Logger, opts ...grpcExecutorOptions) (executor Executor, registerServerFn func(grpcServer grpc.ServiceRegistrar)) {
	grpcExecutor := &grpcExecutor{
		workItemQueue:        make(chan *protos.WorkItem),
		backend:              be,
		logger:               logger,
		pendingOrchestrators: &sync.Map{},
		pendingActivities:    &sync.Map{},
	}

	for _, opt := range opts {
		opt(grpcExecutor)
	}

	return grpcExecutor, func(grpcServer grpc.ServiceRegistrar) {
		protos.RegisterTaskHubSidecarServiceServer(grpcServer, grpcExecutor)
	}
}

// ExecuteOrchestrator implements Executor
func (executor *grpcExecutor) ExecuteOrchestrator(ctx context.Context, iid api.InstanceID, oldEvents []*protos.HistoryEvent, newEvents []*protos.HistoryEvent) (*ExecutionResults, error) {
	result := &ExecutionResults{complete: make(chan struct{})}
	executor.pendingOrchestrators.Store(iid, result)

	workItem := &protos.WorkItem{
		Request: &protos.WorkItem_OrchestratorRequest{
			OrchestratorRequest: &protos.OrchestratorRequest{
				InstanceId:  string(iid),
				ExecutionId: nil,
				PastEvents:  oldEvents,
				NewEvents:   newEvents,
			},
		},
	}

	// Send the orchestration execution work-item to the connected worker.
	// This will block if the worker isn't listening for work items.
	select {
	case <-ctx.Done():
		executor.logger.Warnf("%s: context canceled before dispatching orchestrator work item", iid)
		return nil, ctx.Err()
	case executor.workItemQueue <- workItem:
	}

	// Wait for the connected worker to signal that it's done executing the work-item
	select {
	case <-ctx.Done():
		executor.logger.Warnf("%s: context canceled before receiving orchestrator result", iid)
		return nil, ctx.Err()
	case <-result.complete:
		executor.logger.Debugf("%s: orchestrator got result", iid)
		if result.Response == nil {
			return nil, errors.New("operation aborted")
		}
	}

	return result, nil
}

// ExecuteActivity implements Executor
func (executor *grpcExecutor) ExecuteActivity(ctx context.Context, iid api.InstanceID, e *protos.HistoryEvent) (*protos.HistoryEvent, error) {
	key := getActivityExecutionKey(string(iid), e.EventId)
	result := &activityExecutionResult{complete: make(chan struct{})}
	executor.pendingActivities.Store(key, result)

	task := e.GetTaskScheduled()
	workItem := &protos.WorkItem{
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

	// Send the activity execution work-item to the connected worker.
	// This will block if the worker isn't listening for work items.
	select {
	case <-ctx.Done():
		executor.logger.Warnf("%s/%s#%d: context canceled before dispatching activity work item", iid, task.Name, e.EventId)
		return nil, ctx.Err()
	case executor.workItemQueue <- workItem:
	}

	// Wait for the connected worker to signal that it's done executing the work-item
	select {
	case <-ctx.Done():
		executor.logger.Warnf("%s/%s#%d: context canceled before receiving activity result", iid, task.Name, e.EventId)
		return nil, ctx.Err()
	case <-result.complete:
		executor.logger.Debugf("%s: activity got result", key)
		if result.response == nil {
			return nil, errors.New("operation aborted")
		}
	}

	var responseEvent *protos.HistoryEvent
	if failureDetails := result.response.GetFailureDetails(); failureDetails != nil {
		responseEvent = &protos.HistoryEvent{
			EventId:   -1,
			Timestamp: timestamppb.Now(),
			EventType: &protos.HistoryEvent_TaskFailed{
				TaskFailed: &protos.TaskFailedEvent{
					TaskScheduledId: result.response.TaskId,
					FailureDetails:  failureDetails,
				},
			},
		}
	} else {
		responseEvent = &protos.HistoryEvent{
			EventId:   -1,
			Timestamp: timestamppb.New(time.Now()),
			EventType: &protos.HistoryEvent_TaskCompleted{
				TaskCompleted: &protos.TaskCompletedEvent{
					TaskScheduledId: result.response.TaskId,
					Result:          result.response.Result,
				},
			},
		}
	}

	return responseEvent, nil
}

// Shutdown implements Executor
func (g *grpcExecutor) Shutdown(ctx context.Context) error {
	// closing the work item queue is a signal for shutdown
	close(g.workItemQueue)

	// Iterate through all pending items and close them to unblock the goroutines waiting on this
	g.pendingActivities.Range(func(_, value any) bool {
		p, ok := value.(*activityExecutionResult)
		if ok {
			close(p.complete)
		}
		return true
	})
	g.pendingOrchestrators.Range(func(_, value any) bool {
		p, ok := value.(*ExecutionResults)
		if ok {
			close(p.complete)
		}
		return true
	})

	return nil
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
			message := "unable to establish work item stream at this time: " + err.Error()
			g.logger.Warn(message)
			return status.Errorf(codes.Unavailable, message)
		}
	}

	// Collect all pending activities on this stream
	// Note: we don't need sync.Map's here because access is only on this thread
	pendingActivities := make(map[string]struct{})
	pendingActivityCh := make(chan string, 1)
	pendingOrchestrators := make(map[string]struct{})
	pendingOrchestratorCh := make(chan string, 1)
	defer func() {
		// If there's any pending activity left, remove them
		for key := range pendingActivities {
			g.logger.Debugf("cleaning up pending activity: %s", key)
			p, ok := g.pendingActivities.LoadAndDelete(key)
			if ok {
				pending := p.(*activityExecutionResult)
				close(pending.complete)
			}
		}
		for key := range pendingOrchestrators {
			g.logger.Debugf("cleaning up pending orchestrator: %s", key)
			p, ok := g.pendingOrchestrators.LoadAndDelete(api.InstanceID(key))
			if ok {
				pending := p.(*ExecutionResults)
				close(pending.complete)
			}
		}

		if callback := g.onWorkItemDisconnect; callback != nil {
			if err := callback(stream.Context()); err != nil {
				g.logger.Warnf("error while disconnecting work item stream: %v", err)
			}
		}
	}()

	// The worker client invokes this method, which streams back work-items as they arrive.
	for {
		select {
		case <-stream.Context().Done():
			g.logger.Info("work item stream closed")
			return nil
		case wi, ok := <-g.workItemQueue:
			if !ok {
				continue
			}
			switch x := wi.Request.(type) {
			case *protos.WorkItem_OrchestratorRequest:
				key := x.OrchestratorRequest.GetInstanceId()
				pendingOrchestrators[key] = struct{}{}
				p, ok := g.pendingOrchestrators.Load(api.InstanceID(key))
				if ok {
					p.(*ExecutionResults).pending = pendingOrchestratorCh
				}
			case *protos.WorkItem_ActivityRequest:
				key := getActivityExecutionKey(x.ActivityRequest.GetOrchestrationInstance().GetInstanceId(), x.ActivityRequest.GetTaskId())
				pendingActivities[key] = struct{}{}
				p, ok := g.pendingActivities.Load(key)
				if ok {
					p.(*activityExecutionResult).pending = pendingActivityCh
				}
			}

			if err := stream.Send(wi); err != nil {
				g.logger.Errorf("encountered an error while sending work item: %v", err)
				return err
			}
		case key := <-pendingActivityCh:
			delete(pendingActivities, key)
		case key := <-pendingOrchestratorCh:
			delete(pendingOrchestrators, key)
		case <-g.streamShutdownChan:
			return errShuttingDown
		}
	}
}

// CompleteOrchestratorTask implements protos.TaskHubSidecarServiceServer
func (g *grpcExecutor) CompleteOrchestratorTask(ctx context.Context, res *protos.OrchestratorResponse) (*protos.CompleteTaskResponse, error) {
	iid := api.InstanceID(res.InstanceId)
	if g.deletePendingOrchestrator(iid, res) {
		return emptyCompleteTaskResponse, nil
	}

	return emptyCompleteTaskResponse, fmt.Errorf("unknown instance ID: %s", res.InstanceId)
}

func (g *grpcExecutor) deletePendingOrchestrator(iid api.InstanceID, res *protos.OrchestratorResponse) bool {
	p, ok := g.pendingOrchestrators.LoadAndDelete(iid)
	if !ok {
		return false
	}

	// Note that res can be nil in case of certain failures
	pending := p.(*ExecutionResults)
	pending.Response = res
	if pending.pending != nil {
		pending.pending <- string(iid)
	}
	close(pending.complete)
	return true
}

// CompleteActivityTask implements protos.TaskHubSidecarServiceServer
func (g *grpcExecutor) CompleteActivityTask(ctx context.Context, res *protos.ActivityResponse) (*protos.CompleteTaskResponse, error) {
	key := getActivityExecutionKey(res.InstanceId, res.TaskId)
	if g.deletePendingActivityTask(key, res) {
		return emptyCompleteTaskResponse, nil
	}

	return emptyCompleteTaskResponse, fmt.Errorf("unknown instance ID/task ID combo: %s", key)
}

func (g *grpcExecutor) deletePendingActivityTask(key string, res *protos.ActivityResponse) bool {
	p, ok := g.pendingActivities.LoadAndDelete(key)
	if !ok {
		return false
	}

	// Note that res can be nil in case of certain failures
	pending := p.(*activityExecutionResult)
	pending.response = res
	if pending.pending != nil {
		pending.pending <- key
	}
	close(pending.complete)
	return true
}

func getActivityExecutionKey(iid string, taskID int32) string {
	return iid + "/" + strconv.FormatInt(int64(taskID), 10)
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
	count, err := purgeOrchestrationState(ctx, g.backend, api.InstanceID(req.GetInstanceId()), req.Recursive)
	resp := &protos.PurgeInstancesResponse{DeletedInstanceCount: int32(count)}
	if err != nil {
		return resp, fmt.Errorf("failed to purge orchestration state: %w", err)
	}

	_, err = g.WaitForInstanceCompletion(ctx, &protos.GetInstanceRequest{InstanceId: req.GetInstanceId()})

	return resp, err
}

// QueryInstances implements protos.TaskHubSidecarServiceServer
func (grpcExecutor) QueryInstances(context.Context, *protos.QueryInstancesRequest) (*protos.QueryInstancesResponse, error) {
	return nil, errors.New("unimplemented")
}

// RaiseEvent implements protos.TaskHubSidecarServiceServer
func (g *grpcExecutor) RaiseEvent(ctx context.Context, req *protos.RaiseEventRequest) (*protos.RaiseEventResponse, error) {
	e := &protos.HistoryEvent{
		EventId:   -1,
		Timestamp: timestamppb.New(time.Now()),
		EventType: &protos.HistoryEvent_EventRaised{
			EventRaised: &protos.EventRaisedEvent{Name: req.Name, Input: req.Input},
		},
	}
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

	e := &protos.HistoryEvent{
		EventId:   -1,
		Timestamp: timestamppb.New(time.Now()),
		EventType: &protos.HistoryEvent_ExecutionStarted{
			ExecutionStarted: &protos.ExecutionStartedEvent{
				Name:  req.Name,
				Input: req.Input,
				OrchestrationInstance: &protos.OrchestrationInstance{
					InstanceId:  instanceID,
					ExecutionId: wrapperspb.String(uuid.New().String()),
				},
				ParentTraceContext:      helpers.TraceContextFromSpan(span),
				ScheduledStartTimestamp: req.ScheduledStartTimestamp,
			},
		},
	}
	if err := g.backend.CreateOrchestrationInstance(ctx, e, WithOrchestrationIdReusePolicy(req.OrchestrationIdReusePolicy)); err != nil {
		return nil, err
	}

	return &protos.CreateInstanceResponse{InstanceId: instanceID}, nil
}

// TerminateInstance implements protos.TaskHubSidecarServiceServer
func (g *grpcExecutor) TerminateInstance(ctx context.Context, req *protos.TerminateRequest) (*protos.TerminateResponse, error) {
	e := &protos.HistoryEvent{
		EventId:   -1,
		Timestamp: timestamppb.Now(),
		EventType: &protos.HistoryEvent_ExecutionTerminated{
			ExecutionTerminated: &protos.ExecutionTerminatedEvent{
				Input:   req.Output,
				Recurse: req.Recursive,
			},
		},
	}
	if err := g.backend.AddNewOrchestrationEvent(ctx, api.InstanceID(req.InstanceId), e); err != nil {
		return nil, fmt.Errorf("failed to submit termination request: %w", err)
	}

	_, err := g.WaitForInstanceCompletion(ctx, &protos.GetInstanceRequest{InstanceId: req.InstanceId})

	return &protos.TerminateResponse{}, err
}

// SuspendInstance implements protos.TaskHubSidecarServiceServer
func (g *grpcExecutor) SuspendInstance(ctx context.Context, req *protos.SuspendRequest) (*protos.SuspendResponse, error) {
	var input *wrapperspb.StringValue
	if req.Reason.GetValue() != "" {
		input = wrapperspb.String(req.Reason.GetValue())
	}
	e := &protos.HistoryEvent{
		EventId:   -1,
		Timestamp: timestamppb.New(time.Now()),
		EventType: &protos.HistoryEvent_ExecutionSuspended{
			ExecutionSuspended: &protos.ExecutionSuspendedEvent{
				Input: input,
			},
		},
	}
	if err := g.backend.AddNewOrchestrationEvent(ctx, api.InstanceID(req.InstanceId), e); err != nil {
		return nil, err
	}

	_, err := g.waitForInstance(ctx, &protos.GetInstanceRequest{
		InstanceId: req.InstanceId,
	}, func(metadata *OrchestrationMetadata) bool {
		return metadata.RuntimeStatus == protos.OrchestrationStatus_ORCHESTRATION_STATUS_SUSPENDED ||
			api.OrchestrationMetadataIsComplete(metadata)
	})

	return &protos.SuspendResponse{}, err
}

// ResumeInstance implements protos.TaskHubSidecarServiceServer
func (g *grpcExecutor) ResumeInstance(ctx context.Context, req *protos.ResumeRequest) (*protos.ResumeResponse, error) {
	var input *wrapperspb.StringValue
	if req.Reason.GetValue() != "" {
		input = wrapperspb.String(req.Reason.GetValue())
	}
	e := &protos.HistoryEvent{
		EventId:   -1,
		Timestamp: timestamppb.New(time.Now()),
		EventType: &protos.HistoryEvent_ExecutionResumed{
			ExecutionResumed: &protos.ExecutionResumedEvent{
				Input: input,
			},
		},
	}
	if err := g.backend.AddNewOrchestrationEvent(ctx, api.InstanceID(req.InstanceId), e); err != nil {
		return nil, err
	}

	_, err := g.waitForInstance(ctx, &protos.GetInstanceRequest{
		InstanceId: req.InstanceId,
	}, func(metadata *OrchestrationMetadata) bool {
		return metadata.RuntimeStatus == protos.OrchestrationStatus_ORCHESTRATION_STATUS_RUNNING ||
			api.OrchestrationMetadataIsComplete(metadata)
	})

	return &protos.ResumeResponse{}, err
}

// WaitForInstanceCompletion implements protos.TaskHubSidecarServiceServer
func (g *grpcExecutor) WaitForInstanceCompletion(ctx context.Context, req *protos.GetInstanceRequest) (*protos.GetInstanceResponse, error) {
	return g.waitForInstance(ctx, req, api.OrchestrationMetadataIsComplete)
}

// WaitForInstanceStart implements protos.TaskHubSidecarServiceServer
func (g *grpcExecutor) WaitForInstanceStart(ctx context.Context, req *protos.GetInstanceRequest) (*protos.GetInstanceResponse, error) {
	return g.waitForInstance(ctx, req, func(m *OrchestrationMetadata) bool {
		return m.RuntimeStatus != protos.OrchestrationStatus_ORCHESTRATION_STATUS_PENDING
	})
}

func (g *grpcExecutor) waitForInstance(ctx context.Context, req *protos.GetInstanceRequest, condition func(*OrchestrationMetadata) bool) (*protos.GetInstanceResponse, error) {
	iid := api.InstanceID(req.InstanceId)

	ch := make(chan *protos.OrchestrationMetadata)
	var metadata *protos.OrchestrationMetadata
	err := concurrency.NewRunnerManager(
		func(ctx context.Context) error {
			return g.backend.WatchOrchestrationRuntimeStatus(ctx, iid, ch)
		},
		func(ctx context.Context) error {
			for {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case metadata = <-ch:
					if condition(metadata) {
						return nil
					}
				}
			}
		},
	).Run(ctx)

	if err != nil || ctx.Err() != nil {
		return nil, errors.Join(err, ctx.Err())
	}

	if metadata == nil {
		return &protos.GetInstanceResponse{Exists: false}, nil
	}

	return createGetInstanceResponse(req, metadata), nil
}

// mustEmbedUnimplementedTaskHubSidecarServiceServer implements protos.TaskHubSidecarServiceServer
func (grpcExecutor) mustEmbedUnimplementedTaskHubSidecarServiceServer() {
}

func createGetInstanceResponse(req *protos.GetInstanceRequest, metadata *OrchestrationMetadata) *protos.GetInstanceResponse {
	state := &protos.OrchestrationState{
		InstanceId:           req.InstanceId,
		Name:                 metadata.Name,
		OrchestrationStatus:  metadata.RuntimeStatus,
		CreatedTimestamp:     metadata.CreatedAt,
		LastUpdatedTimestamp: metadata.LastUpdatedAt,
	}

	if req.GetInputsAndOutputs {
		state.Input = metadata.Input
		state.CustomStatus = metadata.CustomStatus
		state.Output = metadata.Output
		state.FailureDetails = metadata.FailureDetails
	}

	return &protos.GetInstanceResponse{Exists: true, OrchestrationState: state}
}
