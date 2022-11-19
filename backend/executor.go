package backend

import (
	context "context"
	"fmt"
	"strings"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/microsoft/durabletask-go/api"
	"github.com/microsoft/durabletask-go/internal/helpers"
	"github.com/microsoft/durabletask-go/internal/protos"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	emptypb "google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

var emptyCompleteTaskResponse = &protos.CompleteTaskResponse{}

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
	pendingOrchestrators map[api.InstanceID]*ExecutionResults
	pendingActivities    map[string]*activityExecutionResult
	backend              Backend
	logger               Logger
}

// IsDurableTaskGrpcRequest returns true if the specified gRPC method name represents an operation
// that is compatible with the gRPC executor.
func IsDurableTaskGrpcRequest(fullMethodName string) bool {
	return strings.Index(fullMethodName, "/TaskHubSidecarService") == 0
}

func NewGrpcExecutor(grpcServer *grpc.Server, be Backend, logger Logger) Executor {
	executor := &grpcExecutor{
		workItemQueue:        make(chan *protos.WorkItem),
		pendingOrchestrators: make(map[api.InstanceID]*ExecutionResults),
		pendingActivities:    make(map[string]*activityExecutionResult),
		backend:              be,
		logger:               logger,
	}
	protos.RegisterTaskHubSidecarServiceServer(grpcServer, executor)
	return executor
}

// ExecuteOrchestrator implements Executor
func (executor *grpcExecutor) ExecuteOrchestrator(ctx context.Context, iid api.InstanceID, oldEvents []*protos.HistoryEvent, newEvents []*protos.HistoryEvent) (*ExecutionResults, error) {
	result := &ExecutionResults{complete: make(chan interface{})}
	executor.pendingOrchestrators[iid] = result
	defer delete(executor.pendingOrchestrators, iid)

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
	executor.pendingActivities[key] = result
	defer delete(executor.pendingActivities, key)

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

// Hello implements protos.TaskHubSidecarServiceServer
func (grpcExecutor) Hello(ctx context.Context, empty *emptypb.Empty) (*emptypb.Empty, error) {
	return empty, nil
}

// GetWorkItems implements protos.TaskHubSidecarServiceServer
func (e grpcExecutor) GetWorkItems(req *protos.GetWorkItemsRequest, stream protos.TaskHubSidecarService_GetWorkItemsServer) error {
	if md, ok := metadata.FromIncomingContext(stream.Context()); ok {
		e.logger.Infof("work item stream established by user-agent: %v", md.Get("user-agent"))
	}
	// The worker client invokes this method, which streams back work-items as they arrive.
	for {
		select {
		case <-stream.Context().Done():
			e.logger.Infof("work item stream closed")
			return nil
		case wi := <-e.workItemQueue:
			if err := stream.Send(wi); err != nil {
				return err
			}
		}
	}
}

// CompleteOrchestratorTask implements protos.TaskHubSidecarServiceServer
func (g *grpcExecutor) CompleteOrchestratorTask(ctx context.Context, res *protos.OrchestratorResponse) (*protos.CompleteTaskResponse, error) {
	iid := api.InstanceID(res.InstanceId)
	if pending, ok := g.pendingOrchestrators[iid]; ok {
		pending.Response = res
		pending.complete <- true
		return emptyCompleteTaskResponse, nil
	}

	return emptyCompleteTaskResponse, fmt.Errorf("unknown instance ID: %s", res.InstanceId)
}

// CompleteActivityTask implements protos.TaskHubSidecarServiceServer
func (g *grpcExecutor) CompleteActivityTask(ctx context.Context, res *protos.ActivityResponse) (*protos.CompleteTaskResponse, error) {
	key := getActivityExecutionKey(res.InstanceId, res.TaskId)
	if pending, ok := g.pendingActivities[key]; ok {
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
	panic("unimplemented")
}

// DeleteTaskHub implements protos.TaskHubSidecarServiceServer
func (grpcExecutor) DeleteTaskHub(context.Context, *protos.DeleteTaskHubRequest) (*protos.DeleteTaskHubResponse, error) {
	panic("unimplemented")
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
func (grpcExecutor) PurgeInstances(context.Context, *protos.PurgeInstancesRequest) (*protos.PurgeInstancesResponse, error) {
	panic("unimplemented")
}

// QueryInstances implements protos.TaskHubSidecarServiceServer
func (grpcExecutor) QueryInstances(context.Context, *protos.QueryInstancesRequest) (*protos.QueryInstancesResponse, error) {
	panic("unimplemented")
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
	e := helpers.NewExecutionStartedEvent(-1, req.Name, instanceID, req.Input, nil)
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
	panic("unimplemented")
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
