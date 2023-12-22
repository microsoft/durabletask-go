package backend

import (
	"context"
	"errors"
	"fmt"

	"github.com/microsoft/durabletask-go/api"
	"github.com/microsoft/durabletask-go/internal/helpers"
	"github.com/microsoft/durabletask-go/internal/protos"
	"google.golang.org/protobuf/proto"
)

var (
	ErrTaskHubExists         = errors.New("task hub already exists")
	ErrTaskHubNotFound       = errors.New("task hub not found")
	ErrNotInitialized        = errors.New("backend not initialized")
	ErrWorkItemLockLost      = errors.New("lock on work-item was lost")
	ErrBackendAlreadyStarted = errors.New("backend is already started")
)

type (
	HistoryEvent       = protos.HistoryEvent
	TaskFailureDetails = protos.TaskFailureDetails
)

type OrchestrationIdReusePolicyOptions func(*protos.OrchestrationIdReusePolicy) error

func WithOrchestrationIdReusePolicy(policy *protos.OrchestrationIdReusePolicy) OrchestrationIdReusePolicyOptions {
	return func(po *protos.OrchestrationIdReusePolicy) error {
		if policy != nil {
			po.Action = policy.Action
			po.OperationStatus = policy.OperationStatus
		}
		return nil
	}
}

type Backend interface {
	// CreateTaskHub creates a new task hub for the current backend. Task hub creation must be idempotent.
	//
	// If the task hub for this backend already exists, an error of type [ErrTaskHubExists] is returned.
	CreateTaskHub(context.Context) error

	// DeleteTaskHub deletes an existing task hub configured for the current backend. It's up to the backend
	// implementation to determine how the task hub data is deleted.
	//
	// If the task hub for this backend doesn't exist, an error of type [ErrTaskHubNotFound] is returned.
	DeleteTaskHub(context.Context) error

	// Start starts any background processing done by this backend.
	Start(context.Context) error

	// Stop stops any background processing done by this backend.
	Stop(context.Context) error

	// CreateOrchestrationInstance creates a new orchestration instance with a history event that
	// wraps a ExecutionStarted event.
	CreateOrchestrationInstance(context.Context, *HistoryEvent, ...OrchestrationIdReusePolicyOptions) error

	// AddNewEvent adds a new orchestration event to the specified orchestration instance.
	AddNewOrchestrationEvent(context.Context, api.InstanceID, *HistoryEvent) error

	// GetOrchestrationWorkItem gets a pending work item from the task hub or returns [ErrNoOrchWorkItems]
	// if there are no pending work items.
	GetOrchestrationWorkItem(context.Context) (*OrchestrationWorkItem, error)

	// GetOrchestrationRuntimeState gets the runtime state of an orchestration instance.
	GetOrchestrationRuntimeState(context.Context, *OrchestrationWorkItem) (*OrchestrationRuntimeState, error)

	// GetOrchestrationMetadata gets the metadata associated with the given orchestration instance ID.
	//
	// Returns [api.ErrInstanceNotFound] if the orchestration instance doesn't exist.
	GetOrchestrationMetadata(context.Context, api.InstanceID) (*api.OrchestrationMetadata, error)

	// CompleteOrchestrationWorkItem completes a work item by saving the updated runtime state to durable storage.
	//
	// Returns [ErrWorkItemLockLost] if the work-item couldn't be completed due to a lock-lost conflict (e.g., split-brain).
	CompleteOrchestrationWorkItem(context.Context, *OrchestrationWorkItem) error

	// AbandonOrchestrationWorkItem undoes any state changes and returns the work item to the work item queue.
	//
	// This is called if an internal failure happens in the processing of an orchestration work item. It is
	// not called if the orchestration work item is processed successfully (note that an orchestration that
	// completes with a failure is still considered a successfully processed work item).
	AbandonOrchestrationWorkItem(context.Context, *OrchestrationWorkItem) error

	// GetActivityWorkItem gets a pending activity work item from the task hub or returns [ErrNoWorkItems]
	// if there are no pending activity work items.
	GetActivityWorkItem(context.Context) (*ActivityWorkItem, error)

	// CompleteActivityWorkItem sends a message to the parent orchestration indicating activity completion.
	//
	// Returns [ErrWorkItemLockLost] if the work-item couldn't be completed due to a lock-lost conflict (e.g., split-brain).
	CompleteActivityWorkItem(context.Context, *ActivityWorkItem) error

	// AbandonActivityWorkItem returns the work-item back to the queue without committing any other chances.
	//
	// This is called when an internal failure occurs during activity work-item processing.
	AbandonActivityWorkItem(context.Context, *ActivityWorkItem) error

	// PurgeOrchestrationState deletes all saved state for the specified orchestration instance.
	//
	// [api.ErrInstanceNotFound] is returned if the specified orchestration instance doesn't exist.
	// [api.ErrNotCompleted] is returned if the specified orchestration instance is still running.
	PurgeOrchestrationState(context.Context, api.InstanceID) error
}

// MarshalHistoryEvent serializes the [HistoryEvent] into a protobuf byte array.
func MarshalHistoryEvent(e *HistoryEvent) ([]byte, error) {
	if bytes, err := proto.Marshal(e); err != nil {
		return nil, fmt.Errorf("failed to marshal history event: %w", err)
	} else {
		return bytes, nil
	}
}

// UnmarshalHistoryEvent deserializes a [HistoryEvent] from a protobuf byte array.
func UnmarshalHistoryEvent(bytes []byte) (*HistoryEvent, error) {
	e := &protos.HistoryEvent{}
	if err := proto.Unmarshal(bytes, e); err != nil {
		return nil, fmt.Errorf("unreadable history event payload: %w", err)
	}
	return e, nil
}

// purgeOrchestrationState purges the orchestration state, including sub-orchestrations if [recursive] is true.
// Returns (deletedInstanceCount, error), where deletedInstanceCount is the number of instances deleted.
func purgeOrchestrationState(ctx context.Context, be Backend, iid api.InstanceID, recursive bool) (int, error) {
	deletedInstanceCount := 0
	if recursive {
		owi := &OrchestrationWorkItem{
			InstanceID: iid,
		}
		state, err := be.GetOrchestrationRuntimeState(ctx, owi)
		if err != nil {
			return 0, fmt.Errorf("failed to fetch orchestration state: %w", err)
		}
		if len(state.NewEvents())+len(state.oldEvents) == 0 {
			// If there are no events, the orchestration instance doesn't exist
			return 0, api.ErrInstanceNotFound
		}
		if !state.IsCompleted() {
			// Orchestration must be completed before purging its state
			return 0, api.ErrNotCompleted
		}
		subOrchestrationInstances := getSubOrchestrationInstances(state.OldEvents(), state.NewEvents())
		for _, subOrchestrationInstance := range subOrchestrationInstances {
			// Recursively purge sub-orchestrations
			count, err := purgeOrchestrationState(ctx, be, subOrchestrationInstance, recursive)
			// `count` sub-orchestrations have been successfully purged (even in case of error)
			deletedInstanceCount += count
			if err != nil {
				return deletedInstanceCount, fmt.Errorf("failed to purge sub-orchestration: %w", err)
			}
		}
	}
	// Purging root orchestration
	if err := be.PurgeOrchestrationState(ctx, iid); err != nil {
		return deletedInstanceCount, err
	}
	return deletedInstanceCount + 1, nil
}

// terminateSubOrchestrationInstances submits termination requests to sub-orchestrations if [et.Recurse] is true.
func terminateSubOrchestrationInstances(ctx context.Context, be Backend, iid api.InstanceID, state *OrchestrationRuntimeState, et *protos.ExecutionTerminatedEvent) error {
	if !et.Recurse {
		return nil
	}
	subOrchestrationInstances := getSubOrchestrationInstances(state.OldEvents(), state.NewEvents())
	for _, subOrchestrationInstance := range subOrchestrationInstances {
		e := helpers.NewExecutionTerminatedEvent(et.Input, et.Recurse)
		// Adding terminate event to sub-orchestration instance
		if err := be.AddNewOrchestrationEvent(ctx, subOrchestrationInstance, e); err != nil {
			return fmt.Errorf("failed to submit termination request to sub-orchestration: %w", err)
		}
	}
	return nil
}

// getSubOrchestrationInstances returns the instance IDs of all sub-orchestrations in the specified events.
func getSubOrchestrationInstances(oldEvents []*HistoryEvent, newEvents []*HistoryEvent) []api.InstanceID {
	subOrchestrationInstances := make([]api.InstanceID, 0, len(oldEvents)+len(newEvents))
	for _, e := range oldEvents {
		if created := e.GetSubOrchestrationInstanceCreated(); created != nil {
			subOrchestrationInstances = append(subOrchestrationInstances, api.InstanceID(created.InstanceId))
		}
	}
	for _, e := range newEvents {
		if created := e.GetSubOrchestrationInstanceCreated(); created != nil {
			subOrchestrationInstances = append(subOrchestrationInstances, api.InstanceID(created.InstanceId))
		}
	}
	return subOrchestrationInstances
}
