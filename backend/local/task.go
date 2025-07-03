package local

import (
	"context"
	"fmt"
	"strconv"
	"sync"

	"github.com/dapr/durabletask-go/api"
	"github.com/dapr/durabletask-go/api/protos"
	"github.com/dapr/durabletask-go/backend"
)

type pendingOrchestrator struct {
	response *protos.OrchestratorResponse
	complete chan struct{}
}

type pendingActivity struct {
	response *protos.ActivityResponse
	complete chan struct{}
}

type TasksBackend struct {
	pendingOrchestrators *sync.Map
	pendingActivities    *sync.Map
}

func NewTasksBackend() *TasksBackend {
	return &TasksBackend{
		pendingOrchestrators: &sync.Map{},
		pendingActivities:    &sync.Map{},
	}
}

func (be *TasksBackend) CompleteActivityTask(ctx context.Context, response *protos.ActivityResponse) error {
	if be.deletePendingActivityTask(response.GetInstanceId(), response.GetTaskId(), response) {
		return nil
	}
	return fmt.Errorf("unknown instance ID/task ID combo: %s", response.GetInstanceId()+"/"+strconv.FormatInt(int64(response.GetTaskId()), 10))
}

func (be *TasksBackend) CancelActivityTask(ctx context.Context, instanceID api.InstanceID, taskID int32) error {
	if be.deletePendingActivityTask(string(instanceID), taskID, nil) {
		return nil
	}
	return fmt.Errorf("unknown instance ID/task ID combo: %s", string(instanceID)+"/"+strconv.FormatInt(int64(taskID), 10))
}

func (be *TasksBackend) WaitForActivityCompletion(ctx context.Context, request *protos.ActivityRequest) (*protos.ActivityResponse, error) {
	key := backend.GetActivityExecutionKey(request.GetOrchestrationInstance().GetInstanceId(), request.GetTaskId())
	pending := &pendingActivity{
		response: nil,
		complete: make(chan struct{}, 1),
	}
	be.pendingActivities.Store(key, pending)

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-pending.complete:
		if pending.response == nil {
			return nil, api.ErrTaskCancelled
		}
		return pending.response, nil
	}
}

func (be *TasksBackend) CompleteOrchestratorTask(ctx context.Context, response *protos.OrchestratorResponse) error {
	if be.deletePendingOrchestrator(response.GetInstanceId(), response) {
		return nil
	}
	return fmt.Errorf("unknown instance ID: %s", response.GetInstanceId())
}

func (be *TasksBackend) CancelOrchestratorTask(ctx context.Context, instanceID api.InstanceID) error {
	if be.deletePendingOrchestrator(string(instanceID), nil) {
		return nil
	}
	return fmt.Errorf("unknown instance ID: %s", instanceID)
}

func (be *TasksBackend) WaitForOrchestratorCompletion(ctx context.Context, request *protos.OrchestratorRequest) (*protos.OrchestratorResponse, error) {
	pending := &pendingOrchestrator{
		response: nil,
		complete: make(chan struct{}, 1),
	}
	be.pendingOrchestrators.Store(request.GetInstanceId(), pending)

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-pending.complete:
		if pending.response == nil {
			return nil, api.ErrTaskCancelled
		}
		return pending.response, nil
	}
}

func (be *TasksBackend) deletePendingActivityTask(iid string, taskID int32, res *protos.ActivityResponse) bool {
	key := backend.GetActivityExecutionKey(iid, taskID)
	p, ok := be.pendingActivities.LoadAndDelete(key)
	if !ok {
		return false
	}

	// Note that res can be nil in case of certain failures
	pending := p.(*pendingActivity)
	pending.response = res
	close(pending.complete)
	return true
}

func (be *TasksBackend) deletePendingOrchestrator(instanceID string, res *protos.OrchestratorResponse) bool {
	p, ok := be.pendingOrchestrators.LoadAndDelete(instanceID)
	if !ok {
		return false
	}

	// Note that res can be nil in case of certain failures
	pending := p.(*pendingOrchestrator)
	pending.response = res
	close(pending.complete)
	return true
}
