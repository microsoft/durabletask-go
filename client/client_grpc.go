package client

import (
	"context"
	"fmt"

	"github.com/cenkalti/backoff/v4"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/timestamppb"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/microsoft/durabletask-go/api"
	"github.com/microsoft/durabletask-go/backend"
	"github.com/microsoft/durabletask-go/internal/helpers"
	"github.com/microsoft/durabletask-go/internal/protos"
)

// REVIEW: Can this be merged with backend/client.go somehow?

type TaskHubGrpcClient struct {
	client protos.TaskHubSidecarServiceClient
	logger backend.Logger
}

// NewTaskHubGrpcClient creates a client that can be used to manage orchestrations over a gRPC connection.
// The gRPC connection must be to a task hub worker that understands the Durable Task gRPC protocol.
func NewTaskHubGrpcClient(cc grpc.ClientConnInterface, logger backend.Logger) *TaskHubGrpcClient {
	return &TaskHubGrpcClient{
		client: protos.NewTaskHubSidecarServiceClient(cc),
		logger: logger,
	}
}

// ScheduleNewOrchestration schedules a new orchestration instance with a specified set of options for execution.
func (c *TaskHubGrpcClient) ScheduleNewOrchestration(ctx context.Context, orchestrator string, opts ...api.NewOrchestrationOptions) (api.InstanceID, error) {
	req := &protos.CreateInstanceRequest{Name: orchestrator}
	for _, configure := range opts {
		if err := configure(req); err != nil {
			return api.EmptyInstanceID, fmt.Errorf("failed to configure orchestration request: %w", err)
		}
	}
	if req.InstanceId == "" {
		req.InstanceId = uuid.NewString()
	}
	if err := helpers.ValidateOrchestrationInstanceID(req.InstanceId); err != nil {
		return api.EmptyInstanceID, err
	}
	resp, err := c.client.StartInstance(ctx, req)
	if err != nil {
		if ctx.Err() != nil {
			return api.EmptyInstanceID, ctx.Err()
		}
		return api.EmptyInstanceID, fmt.Errorf("failed to start orchestrator: %w", err)
	}
	return api.InstanceID(resp.InstanceId), nil
}

// FetchOrchestrationMetadata fetches metadata for the specified orchestration from the configured task hub.
//
// api.ErrInstanceNotFound is returned when the specified orchestration doesn't exist.
func (c *TaskHubGrpcClient) FetchOrchestrationMetadata(ctx context.Context, id api.InstanceID, opts ...api.FetchOrchestrationMetadataOptions) (*api.OrchestrationMetadata, error) {
	req := makeGetInstanceRequest(id, opts)
	resp, err := c.client.GetInstance(ctx, req)
	if err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, fmt.Errorf("failed to fetch orchestration metadata: %w", err)
	}
	return makeOrchestrationMetadata(resp)
}

// WaitForOrchestrationStart waits for an orchestration to start running and returns an [api.OrchestrationMetadata] object that contains
// metadata about the started instance.
//
// api.ErrInstanceNotFound is returned when the specified orchestration doesn't exist.
func (c *TaskHubGrpcClient) WaitForOrchestrationStart(ctx context.Context, id api.InstanceID, opts ...api.FetchOrchestrationMetadataOptions) (*api.OrchestrationMetadata, error) {
	var resp *protos.GetInstanceResponse
	var err error
	err = backoff.Retry(func() error {
		req := makeGetInstanceRequest(id, opts)
		resp, err = c.client.WaitForInstanceStart(ctx, req)
		if err != nil {
			// if its context cancelled stop retrying
			if ctx.Err() != nil {
				return backoff.Permanent(ctx.Err())
			}
			return fmt.Errorf("failed to wait for orchestration start: %w", err)
		}
		return nil
	}, backoff.WithContext(newInfiniteRetries(), ctx))
	if err != nil {
		return nil, err
	}
	return makeOrchestrationMetadata(resp)
}

// WaitForOrchestrationCompletion waits for an orchestration to complete and returns an [api.OrchestrationMetadata] object that contains
// metadata about the completed instance.
//
// api.ErrInstanceNotFound is returned when the specified orchestration doesn't exist.
func (c *TaskHubGrpcClient) WaitForOrchestrationCompletion(ctx context.Context, id api.InstanceID, opts ...api.FetchOrchestrationMetadataOptions) (*api.OrchestrationMetadata, error) {
	var resp *protos.GetInstanceResponse
	var err error
	err = backoff.Retry(func() error {
		req := makeGetInstanceRequest(id, opts)
		resp, err = c.client.WaitForInstanceCompletion(ctx, req)
		if err != nil {
			// if its context cancelled stop retrying
			if ctx.Err() != nil {
				return backoff.Permanent(ctx.Err())
			}
			return fmt.Errorf("failed to wait for orchestration completion: %w", err)
		}
		return nil
	}, backoff.WithContext(newInfiniteRetries(), ctx))
	if err != nil {
		return nil, err
	}
	return makeOrchestrationMetadata(resp)
}

// TerminateOrchestration terminates a running orchestration by causing it to stop receiving new events and
// putting it directly into the TERMINATED state.
func (c *TaskHubGrpcClient) TerminateOrchestration(ctx context.Context, id api.InstanceID, opts ...api.TerminateOptions) error {
	req := &protos.TerminateRequest{InstanceId: string(id), Recursive: true}
	for _, configure := range opts {
		if err := configure(req); err != nil {
			return fmt.Errorf("failed to configure termination request: %w", err)
		}
	}

	_, err := c.client.TerminateInstance(ctx, req)
	if err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return fmt.Errorf("failed to terminate instance: %w", err)
	}
	return nil
}

// RaiseEvent sends an asynchronous event notification to a waiting orchestration.
func (c *TaskHubGrpcClient) RaiseEvent(ctx context.Context, id api.InstanceID, eventName string, opts ...api.RaiseEventOptions) error {
	req := &protos.RaiseEventRequest{InstanceId: string(id), Name: eventName}
	for _, configure := range opts {
		if err := configure(req); err != nil {
			return fmt.Errorf("failed to configure raise event request: %w", err)
		}
	}

	if _, err := c.client.RaiseEvent(ctx, req); err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return fmt.Errorf("failed to raise event: %w", err)
	}
	return nil
}

// SuspendOrchestration suspends an orchestration instance, halting processing of its events until a "resume" operation resumes it.
//
// Note that suspended orchestrations are still considered to be "running" even though they will not process events.
func (c *TaskHubGrpcClient) SuspendOrchestration(ctx context.Context, id api.InstanceID, reason string) error {
	req := &protos.SuspendRequest{
		InstanceId: string(id),
		Reason:     wrapperspb.String(reason),
	}
	if _, err := c.client.SuspendInstance(ctx, req); err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return fmt.Errorf("failed to suspend orchestration: %w", err)
	}
	return nil
}

// ResumeOrchestration resumes an orchestration instance that was previously suspended.
func (c *TaskHubGrpcClient) ResumeOrchestration(ctx context.Context, id api.InstanceID, reason string) error {
	req := &protos.ResumeRequest{
		InstanceId: string(id),
		Reason:     wrapperspb.String(reason),
	}
	if _, err := c.client.ResumeInstance(ctx, req); err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return fmt.Errorf("failed to resume orchestration: %w", err)
	}
	return nil
}

// PurgeOrchestrationState deletes the state of the specified orchestration instance.
//
// [api.api.ErrInstanceNotFound] is returned if the specified orchestration instance doesn't exist.
func (c *TaskHubGrpcClient) PurgeOrchestrationState(ctx context.Context, id api.InstanceID, opts ...api.PurgeOptions) error {
	req := &protos.PurgeInstancesRequest{
		Request: &protos.PurgeInstancesRequest_InstanceId{InstanceId: string(id)},
	}
	for _, configure := range opts {
		if err := configure(req); err != nil {
			return fmt.Errorf("failed to configure purge request: %w", err)
		}
	}

	res, err := c.client.PurgeInstances(ctx, req)
	if err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return fmt.Errorf("failed to purge orchestration state: %w", err)
	} else if res.GetDeletedInstanceCount() == 0 {
		return api.ErrInstanceNotFound
	}
	return nil
}

// SignalEntity sends a fire-and-forget signal to an entity, triggering the specified operation.
//
// If the entity doesn't exist, it will be created automatically when the signal is processed.
func (c *TaskHubGrpcClient) SignalEntity(ctx context.Context, entityID api.EntityID, operationName string, opts ...api.SignalEntityOptions) error {
	if err := helpers.ValidateEntityName(entityID.Name); err != nil {
		return err
	}

	req := &protos.SignalEntityRequest{
		InstanceId: entityID.String(),
		Name:       operationName,
	}
	for _, configure := range opts {
		if err := configure(req); err != nil {
			return fmt.Errorf("failed to configure signal entity request: %w", err)
		}
	}

	if _, err := c.client.SignalEntity(ctx, req); err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return fmt.Errorf("failed to signal entity: %w", err)
	}
	return nil
}

// FetchEntityMetadata retrieves metadata about an entity instance.
//
// Returns nil if the entity doesn't exist.
func (c *TaskHubGrpcClient) FetchEntityMetadata(ctx context.Context, entityID api.EntityID, includeState bool) (*api.EntityMetadata, error) {
	if err := helpers.ValidateEntityName(entityID.Name); err != nil {
		return nil, err
	}

	req := &protos.GetEntityRequest{
		InstanceId:   entityID.String(),
		IncludeState: includeState,
	}

	resp, err := c.client.GetEntity(ctx, req)
	if err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, fmt.Errorf("failed to get entity metadata: %w", err)
	}
	if !resp.Exists || resp.Entity == nil {
		return nil, nil
	}

	result := &api.EntityMetadata{
		InstanceID:       entityID,
		BacklogQueueSize: resp.Entity.BacklogQueueSize,
		LockedBy:         resp.Entity.LockedBy.GetValue(),
		SerializedState:  resp.Entity.SerializedState.GetValue(),
	}
	if resp.Entity.LastModifiedTime != nil {
		result.LastModifiedTime = resp.Entity.LastModifiedTime.AsTime()
	}
	return result, nil
}

// QueryEntities queries entities matching the specified filter criteria.
func (c *TaskHubGrpcClient) QueryEntities(ctx context.Context, query api.EntityQuery) (*api.EntityQueryResults, error) {
	protoQuery := &protos.EntityQuery{
		IncludeState:     query.IncludeState,
		IncludeTransient: query.IncludeTransient,
	}
	if query.InstanceIDStartsWith != "" {
		protoQuery.InstanceIdStartsWith = wrapperspb.String(query.InstanceIDStartsWith)
	}
	if !query.LastModifiedFrom.IsZero() {
		protoQuery.LastModifiedFrom = timestamppb.New(query.LastModifiedFrom)
	}
	if !query.LastModifiedTo.IsZero() {
		protoQuery.LastModifiedTo = timestamppb.New(query.LastModifiedTo)
	}
	if query.PageSize > 0 {
		protoQuery.PageSize = wrapperspb.Int32(query.PageSize)
	}
	if query.ContinuationToken != "" {
		protoQuery.ContinuationToken = wrapperspb.String(query.ContinuationToken)
	}

	resp, err := c.client.QueryEntities(ctx, &protos.QueryEntitiesRequest{Query: protoQuery})
	if err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, fmt.Errorf("failed to query entities: %w", err)
	}

	results := &api.EntityQueryResults{
		ContinuationToken: resp.ContinuationToken.GetValue(),
	}
	for _, e := range resp.Entities {
		entityID, parseErr := api.EntityIDFromString(e.InstanceId)
		if parseErr != nil {
			continue
		}
		meta := &api.EntityMetadata{
			InstanceID:       entityID,
			BacklogQueueSize: e.BacklogQueueSize,
			LockedBy:         e.LockedBy.GetValue(),
			SerializedState:  e.SerializedState.GetValue(),
		}
		if e.LastModifiedTime != nil {
			meta.LastModifiedTime = e.LastModifiedTime.AsTime()
		}
		results.Entities = append(results.Entities, meta)
	}
	return results, nil
}

// CleanEntityStorage performs garbage collection on entity storage.
func (c *TaskHubGrpcClient) CleanEntityStorage(ctx context.Context, req api.CleanEntityStorageRequest) (*api.CleanEntityStorageResult, error) {
	protoReq := &protos.CleanEntityStorageRequest{
		RemoveEmptyEntities:  req.RemoveEmptyEntities,
		ReleaseOrphanedLocks: req.ReleaseOrphanedLocks,
	}
	if req.ContinuationToken != "" {
		protoReq.ContinuationToken = wrapperspb.String(req.ContinuationToken)
	}

	resp, err := c.client.CleanEntityStorage(ctx, protoReq)
	if err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, fmt.Errorf("failed to clean entity storage: %w", err)
	}

	return &api.CleanEntityStorageResult{
		EmptyEntitiesRemoved:  resp.EmptyEntitiesRemoved,
		OrphanedLocksReleased: resp.OrphanedLocksReleased,
		ContinuationToken:     resp.ContinuationToken.GetValue(),
	}, nil
}

func makeGetInstanceRequest(id api.InstanceID, opts []api.FetchOrchestrationMetadataOptions) *protos.GetInstanceRequest {
	req := &protos.GetInstanceRequest{
		InstanceId:          string(id),
		GetInputsAndOutputs: true,
	}
	for _, configure := range opts {
		configure(req)
	}
	return req
}

// makeOrchestrationMetadata validates and converts protos.GetInstanceResponse to api.OrchestrationMetadata
// api.ErrInstanceNotFound is returned when the specified orchestration doesn't exist.
func makeOrchestrationMetadata(resp *protos.GetInstanceResponse) (*api.OrchestrationMetadata, error) {
	if !resp.Exists {
		return nil, api.ErrInstanceNotFound
	}
	if resp.OrchestrationState == nil {
		return nil, fmt.Errorf("orchestration state is nil")
	}
	metadata := &api.OrchestrationMetadata{
		InstanceID:             api.InstanceID(resp.OrchestrationState.InstanceId),
		Name:                   resp.OrchestrationState.Name,
		RuntimeStatus:          resp.OrchestrationState.OrchestrationStatus,
		SerializedInput:        resp.OrchestrationState.Input.GetValue(),
		SerializedCustomStatus: resp.OrchestrationState.CustomStatus.GetValue(),
		SerializedOutput:       resp.OrchestrationState.Output.GetValue(),
	}
	if resp.OrchestrationState.CreatedTimestamp != nil {
		metadata.CreatedAt = resp.OrchestrationState.CreatedTimestamp.AsTime()
	}
	if resp.OrchestrationState.LastUpdatedTimestamp != nil {
		metadata.LastUpdatedAt = resp.OrchestrationState.LastUpdatedTimestamp.AsTime()
	}
	return metadata, nil
}
