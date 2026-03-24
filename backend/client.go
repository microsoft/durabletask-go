package backend

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/google/uuid"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/microsoft/durabletask-go/api"
	"github.com/microsoft/durabletask-go/internal/helpers"
	"github.com/microsoft/durabletask-go/internal/protos"
)

type TaskHubClient interface {
	ScheduleNewOrchestration(ctx context.Context, orchestrator any, opts ...api.NewOrchestrationOptions) (api.InstanceID, error)
	FetchOrchestrationMetadata(ctx context.Context, id api.InstanceID) (*api.OrchestrationMetadata, error)
	WaitForOrchestrationStart(ctx context.Context, id api.InstanceID) (*api.OrchestrationMetadata, error)
	WaitForOrchestrationCompletion(ctx context.Context, id api.InstanceID) (*api.OrchestrationMetadata, error)
	TerminateOrchestration(ctx context.Context, id api.InstanceID, opts ...api.TerminateOptions) error
	RaiseEvent(ctx context.Context, id api.InstanceID, eventName string, opts ...api.RaiseEventOptions) error
	SuspendOrchestration(ctx context.Context, id api.InstanceID, reason string) error
	ResumeOrchestration(ctx context.Context, id api.InstanceID, reason string) error
	PurgeOrchestrationState(ctx context.Context, id api.InstanceID, opts ...api.PurgeOptions) error
}

// EntityTaskHubClient is an optional extension of [TaskHubClient] that adds entity-specific
// operations. Clients returned by [NewTaskHubClient] always implement this interface.
// The gRPC client ([TaskHubGrpcClient]) also implements this interface.
type EntityTaskHubClient interface {
	TaskHubClient
	SignalEntity(ctx context.Context, entityID api.EntityID, operationName string, opts ...api.SignalEntityOptions) error
	FetchEntityMetadata(ctx context.Context, entityID api.EntityID, includeState bool) (*api.EntityMetadata, error)
	QueryEntities(ctx context.Context, query api.EntityQuery) (*api.EntityQueryResults, error)
	CleanEntityStorage(ctx context.Context, req api.CleanEntityStorageRequest) (*api.CleanEntityStorageResult, error)
}

type backendClient struct {
	be Backend
}

func NewTaskHubClient(be Backend) TaskHubClient {
	return &backendClient{
		be: be,
	}
}

func (c *backendClient) ScheduleNewOrchestration(ctx context.Context, orchestrator any, opts ...api.NewOrchestrationOptions) (api.InstanceID, error) {
	name := helpers.GetTaskFunctionName(orchestrator)
	req := &protos.CreateInstanceRequest{Name: name}
	for _, configure := range opts {
		if err := configure(req); err != nil {
			return api.EmptyInstanceID, fmt.Errorf("failed to configure create instance request: %w", err)
		}
	}
	if req.InstanceId == "" {
		u, err := uuid.NewRandom()
		if err != nil {
			return api.EmptyInstanceID, fmt.Errorf("failed to generate instance ID: %w", err)
		}
		req.InstanceId = u.String()
	}

	var span trace.Span
	ctx, span = helpers.StartNewCreateOrchestrationSpan(ctx, req.Name, req.Version.GetValue(), req.InstanceId)
	defer span.End()

	tc := helpers.TraceContextFromSpan(span)
	e := helpers.NewExecutionStartedEvent(req.Name, req.InstanceId, req.Input, nil, tc, req.ScheduledStartTimestamp)
	if err := c.be.CreateOrchestrationInstance(ctx, e, WithOrchestrationIdReusePolicy(req.OrchestrationIdReusePolicy)); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return api.EmptyInstanceID, fmt.Errorf("failed to start orchestration: %w", err)
	}
	return api.InstanceID(req.InstanceId), nil
}

// FetchOrchestrationMetadata fetches metadata for the specified orchestration from the configured task hub.
//
// ErrInstanceNotFound is returned when the specified orchestration doesn't exist.
func (c *backendClient) FetchOrchestrationMetadata(ctx context.Context, id api.InstanceID) (*api.OrchestrationMetadata, error) {
	metadata, err := c.be.GetOrchestrationMetadata(ctx, id)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch orchestration metadata: %w", err)
	}
	return metadata, nil
}

// WaitForOrchestrationStart waits for an orchestration to start running and returns an [OrchestrationMetadata] object that contains
// metadata about the started instance.
//
// ErrInstanceNotFound is returned when the specified orchestration doesn't exist.
func (c *backendClient) WaitForOrchestrationStart(ctx context.Context, id api.InstanceID) (*api.OrchestrationMetadata, error) {
	return c.waitForOrchestrationCondition(ctx, id, func(metadata *api.OrchestrationMetadata) bool {
		return metadata.RuntimeStatus != protos.OrchestrationStatus_ORCHESTRATION_STATUS_PENDING
	})
}

// WaitForOrchestrationCompletion waits for an orchestration to complete and returns an [OrchestrationMetadata] object that contains
// metadata about the completed instance.
//
// ErrInstanceNotFound is returned when the specified orchestration doesn't exist.
func (c *backendClient) WaitForOrchestrationCompletion(ctx context.Context, id api.InstanceID) (*api.OrchestrationMetadata, error) {
	return c.waitForOrchestrationCondition(ctx, id, func(metadata *api.OrchestrationMetadata) bool {
		return metadata.IsComplete()
	})
}

func (c *backendClient) waitForOrchestrationCondition(ctx context.Context, id api.InstanceID, condition func(metadata *api.OrchestrationMetadata) bool) (*api.OrchestrationMetadata, error) {
	b := backoff.ExponentialBackOff{
		InitialInterval:     100 * time.Millisecond,
		MaxInterval:         10 * time.Second,
		Multiplier:          1.5,
		RandomizationFactor: 0.05,
		Stop:                backoff.Stop,
		Clock:               backoff.SystemClock,
	}
	b.Reset()

	for {
		t := time.NewTimer(b.NextBackOff())
		select {
		case <-ctx.Done():
			if !t.Stop() {
				<-t.C
			}
			return nil, ctx.Err()
		case <-t.C:
			metadata, err := c.FetchOrchestrationMetadata(ctx, id)
			if err != nil {
				return nil, err
			}
			if metadata != nil && condition(metadata) {
				return metadata, nil
			}
		}
	}
}

// TerminateOrchestration enqueues a message to terminate a running orchestration, causing it to stop receiving new events and
// go directly into the TERMINATED state. This operation is asynchronous. An orchestration worker must
// dequeue the termination event before the orchestration will be terminated.
func (c *backendClient) TerminateOrchestration(ctx context.Context, id api.InstanceID, opts ...api.TerminateOptions) error {
	req := &protos.TerminateRequest{InstanceId: string(id), Recursive: true}
	for _, configure := range opts {
		if err := configure(req); err != nil {
			return fmt.Errorf("failed to configure termination request: %w", err)
		}
	}
	e := helpers.NewExecutionTerminatedEvent(req.Output, req.Recursive)
	if err := c.be.AddNewOrchestrationEvent(ctx, id, e); err != nil {
		return fmt.Errorf("failed to submit termination request:: %w", err)
	}
	return nil
}

// RaiseEvent implements TaskHubClient and sends an asynchronous event notification to a waiting orchestration.
//
// In order to handle the event, the target orchestration instance must be waiting for an event named [eventName]
// using the [WaitForSingleEvent] method of the orchestration context parameter. If the target orchestration instance
// is not yet waiting for an event named [eventName], then the event will be bufferred in memory until a task
// subscribing to that event name is created.
//
// Raised events for a completed or non-existent orchestration instance will be silently discarded.
func (c *backendClient) RaiseEvent(ctx context.Context, id api.InstanceID, eventName string, opts ...api.RaiseEventOptions) error {
	req := &protos.RaiseEventRequest{InstanceId: string(id), Name: eventName}
	for _, configure := range opts {
		if err := configure(req); err != nil {
			return fmt.Errorf("failed to configure raise event request: %w", err)
		}
	}

	e := helpers.NewEventRaisedEvent(req.Name, req.Input)
	if err := c.be.AddNewOrchestrationEvent(ctx, id, e); err != nil {
		return fmt.Errorf("failed to raise event: %w", err)
	}
	return nil
}

// SuspendOrchestration suspends an orchestration instance, halting processing of its events until a "resume" operation resumes it.
//
// Note that suspended orchestrations are still considered to be "running" even though they will not process events.
func (c *backendClient) SuspendOrchestration(ctx context.Context, id api.InstanceID, reason string) error {
	e := helpers.NewSuspendOrchestrationEvent(reason)
	if err := c.be.AddNewOrchestrationEvent(ctx, id, e); err != nil {
		return fmt.Errorf("failed to suspend orchestration: %w", err)
	}
	return nil
}

// ResumeOrchestration resumes an orchestration instance that was previously suspended.
func (c *backendClient) ResumeOrchestration(ctx context.Context, id api.InstanceID, reason string) error {
	e := helpers.NewResumeOrchestrationEvent(reason)
	if err := c.be.AddNewOrchestrationEvent(ctx, id, e); err != nil {
		return fmt.Errorf("failed to resume orchestration: %w", err)
	}
	return nil
}

// PurgeOrchestrationState deletes the state of the specified orchestration instance.
//
// [api.ErrInstanceNotFound] is returned if the specified orchestration instance doesn't exist.
// [api.ErrNotCompleted] is returned if the specified orchestration instance is still running.
func (c *backendClient) PurgeOrchestrationState(ctx context.Context, id api.InstanceID, opts ...api.PurgeOptions) error {
	req := &protos.PurgeInstancesRequest{Request: &protos.PurgeInstancesRequest_InstanceId{InstanceId: string(id)}, Recursive: true}
	for _, configure := range opts {
		if err := configure(req); err != nil {
			return fmt.Errorf("failed to configure purge request: %w", err)
		}
	}
	if _, err := purgeOrchestrationState(ctx, c.be, id, req.Recursive); err != nil {
		return fmt.Errorf("failed to purge orchestration state: %w", err)
	}
	return nil
}

// SignalEntity sends a fire-and-forget signal to an entity, triggering the specified operation.
//
// If the entity doesn't exist, it will be created automatically when the signal is processed.
func (c *backendClient) SignalEntity(ctx context.Context, entityID api.EntityID, operationName string, opts ...api.SignalEntityOptions) error {
	req := &protos.SignalEntityRequest{
		InstanceId: entityID.String(),
		Name:       operationName,
	}
	for _, configure := range opts {
		if err := configure(req); err != nil {
			return fmt.Errorf("failed to configure signal entity request: %w", err)
		}
	}

	// Ensure the entity orchestration instance exists. Create with IGNORE policy
	// so it's a no-op if the instance already exists.
	startEvent := helpers.NewExecutionStartedEvent(entityID.Name, req.InstanceId, nil, nil, nil, nil)
	createErr := c.be.CreateOrchestrationInstance(ctx, startEvent, WithOrchestrationIdReusePolicy(&protos.OrchestrationIdReusePolicy{
		Action:          protos.CreateOrchestrationAction_IGNORE,
		OperationStatus: []protos.OrchestrationStatus{protos.OrchestrationStatus_ORCHESTRATION_STATUS_RUNNING},
	}))
	if createErr != nil && !errors.Is(createErr, api.ErrDuplicateInstance) && !errors.Is(createErr, api.ErrIgnoreInstance) {
		return fmt.Errorf("failed to create entity instance: %w", createErr)
	}

	// Build the .NET-compatible EntityRequestMessage payload with isSignal=true.
	requestID := req.RequestId
	if requestID == "" {
		requestID = uuid.New().String()
	}
	reqMsg := helpers.EntityRequestMessage{
		ID:        requestID,
		IsSignal:  true,
		Operation: req.Name,
	}
	if req.Input != nil {
		reqMsg.Input = req.Input.GetValue()
	}
	payload, err := json.Marshal(reqMsg)
	if err != nil {
		return fmt.Errorf("failed to marshal signal request: %w", err)
	}

	e := helpers.NewEventRaisedEvent(helpers.EntityRequestEventName, wrapperspb.String(string(payload)))
	if req.ScheduledTime != nil {
		e.Timestamp = req.ScheduledTime
	}
	if err := c.be.AddNewOrchestrationEvent(ctx, api.InstanceID(req.InstanceId), e); err != nil {
		return fmt.Errorf("failed to signal entity: %w", err)
	}
	return nil
}

// FetchEntityMetadata retrieves metadata about an entity instance.
//
// Returns nil if the entity doesn't exist.
// If the backend implements [EntityBackend], its native entity storage is used.
// Otherwise, falls back to orchestration metadata.
func (c *backendClient) FetchEntityMetadata(ctx context.Context, entityID api.EntityID, includeState bool) (*api.EntityMetadata, error) {
	if eb, ok := c.be.(EntityBackend); ok {
		return eb.GetEntityMetadata(ctx, entityID, includeState)
	}

	// Fallback: entities are backed by orchestrations
	iid := api.InstanceID(entityID.String())
	metadata, err := c.be.GetOrchestrationMetadata(ctx, iid)
	if err != nil {
		if errors.Is(err, api.ErrInstanceNotFound) {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to get entity metadata: %w", err)
	}
	if metadata == nil {
		return nil, nil
	}

	result := &api.EntityMetadata{
		InstanceID:       entityID,
		LastModifiedTime: metadata.LastUpdatedAt,
	}
	if includeState {
		result.SerializedState = metadata.SerializedCustomStatus
	}
	return result, nil
}

// QueryEntities queries entities matching the specified filter criteria.
//
// Requires the backend to implement [EntityBackend].
func (c *backendClient) QueryEntities(ctx context.Context, query api.EntityQuery) (*api.EntityQueryResults, error) {
	if eb, ok := c.be.(EntityBackend); ok {
		return eb.QueryEntities(ctx, query)
	}
	return nil, fmt.Errorf("QueryEntities requires the backend to implement EntityBackend")
}

// CleanEntityStorage performs garbage collection on entity storage.
//
// Requires the backend to implement [EntityBackend].
func (c *backendClient) CleanEntityStorage(ctx context.Context, req api.CleanEntityStorageRequest) (*api.CleanEntityStorageResult, error) {
	if eb, ok := c.be.(EntityBackend); ok {
		return eb.CleanEntityStorage(ctx, req)
	}
	return nil, fmt.Errorf("CleanEntityStorage requires the backend to implement EntityBackend")
}
