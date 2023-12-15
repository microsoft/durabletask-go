package backend

import (
	"context"
	"errors"
	"fmt"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/microsoft/durabletask-go/api"
	"github.com/microsoft/durabletask-go/internal/protos"
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

type OrchestrationStateChanges struct {
	NewEvents         []*HistoryEvent
	NewTasks          []*HistoryEvent
	NewTimers         []*HistoryEvent
	NewMessages       []OrchestratorMessage
	CustomStatus      *wrapperspb.StringValue
	RuntimeStatus     protos.OrchestrationStatus
	ContinuedAsNew    bool
	IsPartial         bool
	HistoryStartIndex int
}

func (c *OrchestrationStateChanges) IsEmpty() bool {
	return len(c.NewEvents) == 0 &&
		len(c.NewTasks) == 0 &&
		len(c.NewTimers) == 0 &&
		len(c.NewMessages) == 0
}

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

// Backend is the interface that must be implemented by all task hub backends.
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
	// The [OrchestrationStateChanges] parameter contains the changes to the orchestration state that should be
	// saved to durable storage. The [HistoryStartIndex] field indicates the index of the first history event
	// in the [OrchestrationStateChanges.NewEvents] slice. This is used to determine the index of the first
	// history event in the [OrchestrationRuntimeState.History] slice, which is useful for backends that store
	// the history events as an append log.
	//
	// The [OrchestrationStateChanges.IsPartial] field indicates whether this is a partial completion operation,
	// in which case more calls to this function are expected to follow with the same work item. Partial completion
	// is used to commit state updates in chunks to avoid overly large transactions. The final chunk will be committed
	// with [OrchestrationStateChanges.IsPartial] set to [false].
	//
	// Implementations of this function should not attempt to delete the work item from storage until the final chunk
	// is committed (i.e., until [OrchestrationStateChanges.IsPartial] is [false]) to ensure that the work item can be
	// recovered if the process crashes before the final chunk is committed.
	//
	// Returns [ErrWorkItemLockLost] if the work-item couldn't be completed due to a lock-lost conflict (e.g., split-brain).
	CompleteOrchestrationWorkItem(context.Context, *OrchestrationWorkItem, OrchestrationStateChanges) error

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
