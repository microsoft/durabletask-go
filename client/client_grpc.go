package client

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/google/uuid"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/timestamppb"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/microsoft/durabletask-go/api"
	"github.com/microsoft/durabletask-go/internal/failure"
	"github.com/microsoft/durabletask-go/internal/helpers"
	"github.com/microsoft/durabletask-go/internal/largepayload"
	"github.com/microsoft/durabletask-go/internal/protos"
	"github.com/microsoft/durabletask-go/internal/tagcodec"
)

type TaskHubGrpcClient struct {
	client         protos.TaskHubSidecarServiceClient
	connection     grpc.ClientConnInterface
	logger         api.Logger
	largePayloads  *api.LargePayloadOptions
	defaultVersion string
	converter      api.DataConverter

	listenerMu sync.Mutex
	listener   *TaskHubGrpcWorker
}

func newInfiniteRetries() *backoff.ExponentialBackOff {
	retries := backoff.NewExponentialBackOff()
	retries.MaxInterval = 15 * time.Second
	retries.MaxElapsedTime = 0
	retries.Reset()
	return retries
}

type TaskHubGrpcClientOption func(*TaskHubGrpcClient)

// WithLargePayloads configures externalization and hydration for management payloads.
func WithLargePayloads(options *api.LargePayloadOptions) TaskHubGrpcClientOption {
	return func(c *TaskHubGrpcClient) {
		if options == nil {
			c.largePayloads = nil
			return
		}
		clone := *options
		c.largePayloads = &clone
	}
}

// WithDefaultVersion configures the version used when a top-level orchestration
// is scheduled without an explicit [api.WithVersion] option.
func WithDefaultVersion(version string) TaskHubGrpcClientOption {
	return func(c *TaskHubGrpcClient) {
		c.defaultVersion = version
	}
}

// WithDataConverter configures application payload serialization.
func WithDataConverter(converter api.DataConverter) TaskHubGrpcClientOption {
	return func(c *TaskHubGrpcClient) {
		c.converter = api.NormalizeDataConverter(converter)
	}
}

// NewTaskHubGrpcClient creates a client that can be used to manage orchestrations over a borrowed gRPC connection.
// The caller retains ownership of the connection; this client neither closes nor recreates it. DTS applications that
// want an owned, resilient management channel should use durabletaskscheduler.NewClient.
func NewTaskHubGrpcClient(cc grpc.ClientConnInterface, logger api.Logger, opts ...TaskHubGrpcClientOption) *TaskHubGrpcClient {
	c := &TaskHubGrpcClient{
		client:     protos.NewTaskHubSidecarServiceClient(cc),
		connection: cc,
		logger:     logger,
		converter:  api.DefaultDataConverter(),
	}
	for _, configure := range opts {
		configure(c)
	}
	return c
}

// ScheduleNewOrchestration schedules a new orchestration instance with a specified set of options for execution.
func (c *TaskHubGrpcClient) ScheduleNewOrchestration(ctx context.Context, orchestrator string, opts ...api.NewOrchestrationOptions) (api.InstanceID, error) {
	req := &protos.CreateInstanceRequest{Name: orchestrator}
	for _, configure := range opts {
		if err := configure(req, c.converter); err != nil {
			return api.EmptyInstanceID, fmt.Errorf(
				"failed to configure orchestration request: %w",
				api.WrapInvalidArgument(err),
			)
		}
	}
	if req.Version == nil && c.defaultVersion != "" {
		req.Version = wrapperspb.String(c.defaultVersion)
	}
	if req.InstanceId == "" {
		u, err := uuid.NewV7()
		if err == nil {
			req.InstanceId = u.String()
		} else {
			req.InstanceId = uuid.NewString()
		}
	}
	var err error
	req.Input, err = largepayload.Externalize(ctx, c.largePayloads, req.Input)
	if err != nil {
		return api.EmptyInstanceID, fmt.Errorf("failed to externalize orchestration input: %w", err)
	}

	// Propagate the caller's distributed trace context so the service can parent the
	// orchestration's trace to the code that scheduled it. Without this, every
	// orchestration starts a brand new, disconnected trace.
	if req.ParentTraceContext == nil {
		req.ParentTraceContext = helpers.TraceContextFromSpan(trace.SpanFromContext(ctx))
	}

	resp, err := c.client.StartInstance(ctx, req)
	if err != nil {
		return api.EmptyInstanceID, clientRPCError(ctx, "failed to start orchestration", err)
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
		return nil, clientRPCError(ctx, "failed to fetch orchestration metadata", err)
	}
	return c.makeOrchestrationMetadata(ctx, resp)
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
			if ctx.Err() != nil {
				return backoff.Permanent(ctx.Err())
			}
			mapped := clientRPCError(ctx, "failed to wait for orchestration start", err)
			if !retryableWaitRPCError(err) {
				return backoff.Permanent(mapped)
			}
			return mapped
		}
		return nil
	}, backoff.WithContext(newInfiniteRetries(), ctx))
	if err != nil {
		return nil, err
	}
	return c.makeOrchestrationMetadata(ctx, resp)
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
			if ctx.Err() != nil {
				return backoff.Permanent(ctx.Err())
			}
			mapped := clientRPCError(ctx, "failed to wait for orchestration completion", err)
			if !retryableWaitRPCError(err) {
				return backoff.Permanent(mapped)
			}
			return mapped
		}
		return nil
	}, backoff.WithContext(newInfiniteRetries(), ctx))
	if err != nil {
		return nil, err
	}
	return c.makeOrchestrationMetadata(ctx, resp)
}

// TerminateOrchestration terminates a running orchestration by causing it to stop receiving new events and
// putting it directly into the TERMINATED state.
func (c *TaskHubGrpcClient) TerminateOrchestration(ctx context.Context, id api.InstanceID, opts ...api.TerminateOptions) error {
	req := &protos.TerminateRequest{InstanceId: string(id), Recursive: true}
	for _, configure := range opts {
		if err := configure(req, c.converter); err != nil {
			return fmt.Errorf("failed to configure termination request: %w", api.WrapInvalidArgument(err))
		}
	}
	var err error
	req.Output, err = largepayload.Externalize(ctx, c.largePayloads, req.Output)
	if err != nil {
		return fmt.Errorf("failed to externalize termination output: %w", err)
	}

	_, err = c.client.TerminateInstance(ctx, req)
	if err != nil {
		return clientRPCError(ctx, "failed to terminate orchestration", err)
	}
	return nil
}

// RaiseEvent sends an asynchronous event notification to a waiting orchestration.
func (c *TaskHubGrpcClient) RaiseEvent(ctx context.Context, id api.InstanceID, eventName string, opts ...api.RaiseEventOptions) error {
	req := &protos.RaiseEventRequest{InstanceId: string(id), Name: eventName}
	for _, configure := range opts {
		if err := configure(req, c.converter); err != nil {
			return fmt.Errorf("failed to configure raise event request: %w", api.WrapInvalidArgument(err))
		}
	}
	var err error
	req.Input, err = largepayload.Externalize(ctx, c.largePayloads, req.Input)
	if err != nil {
		return fmt.Errorf("failed to externalize event payload: %w", err)
	}

	if _, err := c.client.RaiseEvent(ctx, req); err != nil {
		return clientRPCError(ctx, "failed to raise event", err)
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
		return clientRPCError(ctx, "failed to suspend orchestration", err)
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
		return clientRPCError(ctx, "failed to resume orchestration", err)
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
			return fmt.Errorf("failed to configure purge request: %w", api.WrapInvalidArgument(err))
		}
	}

	res, err := c.client.PurgeInstances(ctx, req)
	if err != nil {
		return clientRPCError(ctx, "failed to purge orchestration state", err)
	} else if res.GetDeletedInstanceCount() == 0 {
		return api.ErrInstanceNotFound
	}
	return nil
}

// SignalEntity sends a fire-and-forget operation to an entity.
func (c *TaskHubGrpcClient) SignalEntity(ctx context.Context, entityID api.EntityID, operationName string, opts ...api.SignalEntityOptions) error {
	if err := helpers.ValidateEntityName(entityID.Name); err != nil {
		return api.WrapInvalidArgument(err)
	}
	if operationName == "" {
		return api.WrapInvalidArgument(errors.New("entity operation name must not be empty"))
	}
	req := &protos.SignalEntityRequest{
		InstanceId:         entityID.String(),
		Name:               operationName,
		RequestId:          uuid.NewString(),
		RequestTime:        timestamppb.Now(),
		ParentTraceContext: helpers.TraceContextFromSpan(trace.SpanFromContext(ctx)),
	}
	for _, configure := range opts {
		if err := configure(req, c.converter); err != nil {
			return fmt.Errorf("failed to configure signal entity request: %w", api.WrapInvalidArgument(err))
		}
	}
	var err error
	req.Input, err = largepayload.Externalize(ctx, c.largePayloads, req.Input)
	if err != nil {
		return fmt.Errorf("failed to externalize entity signal input: %w", err)
	}
	if _, err := c.client.SignalEntity(ctx, req); err != nil {
		return clientRPCError(ctx, "failed to signal entity", err)
	}
	return nil
}

// GetEntity retrieves metadata for an entity instance or returns nil when it doesn't exist.
// State is included unless ExcludeState is set.
func (c *TaskHubGrpcClient) GetEntity(
	ctx context.Context,
	entityID api.EntityID,
	options ...api.GetEntityOptions,
) (*api.EntityMetadata, error) {
	if err := helpers.ValidateEntityName(entityID.Name); err != nil {
		return nil, api.WrapInvalidArgument(err)
	}
	if len(options) > 1 {
		return nil, api.WrapInvalidArgument(errors.New("at most one entity metadata options value may be supplied"))
	}
	includeState := len(options) == 0 || !options[0].ExcludeState
	response, err := c.client.GetEntity(ctx, &protos.GetEntityRequest{
		InstanceId:   entityID.String(),
		IncludeState: includeState,
	})
	if err != nil {
		return nil, clientRPCError(ctx, "failed to get entity metadata", err)
	}
	if !response.Exists || response.Entity == nil {
		return nil, nil
	}
	return entityMetadataFromProto(ctx, c.largePayloads, c.converter, response.Entity, includeState)
}

// QueryEntities queries entities matching the supplied filters.
func (c *TaskHubGrpcClient) QueryEntities(ctx context.Context, query api.EntityQuery) (*api.EntityQueryResults, error) {
	if query.PageSize < 0 {
		return nil, api.WrapInvalidArgument(errors.New("entity query page size must not be negative"))
	}
	if err := api.ValidateTimeRange(query.LastModifiedFrom, query.LastModifiedTo); err != nil {
		return nil, err
	}
	protoQuery := &protos.EntityQuery{
		IncludeState:     !query.ExcludeState,
		IncludeTransient: query.IncludeTransient,
	}
	if query.InstanceIDStartsWith != "" {
		protoQuery.InstanceIdStartsWith = wrapperspb.String(normalizeEntityQueryPrefix(query.InstanceIDStartsWith))
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
	response, err := c.client.QueryEntities(ctx, &protos.QueryEntitiesRequest{Query: protoQuery})
	if err != nil {
		return nil, clientRPCError(ctx, "failed to query entities", err)
	}
	result := &api.EntityQueryResults{
		Entities:          make([]*api.EntityMetadata, 0, len(response.Entities)),
		ContinuationToken: response.ContinuationToken.GetValue(),
	}
	for _, entity := range response.Entities {
		metadata, err := entityMetadataFromProto(
			ctx,
			c.largePayloads,
			c.converter,
			entity,
			!query.ExcludeState,
		)
		if err != nil {
			return nil, err
		}
		result.Entities = append(result.Entities, metadata)
	}
	return result, nil
}

// CleanEntityStorage removes empty entities and releases orphaned locks.
func (c *TaskHubGrpcClient) CleanEntityStorage(
	ctx context.Context,
	options ...api.CleanEntityStorageOptions,
) (*api.CleanEntityStorageResult, error) {
	if len(options) > 1 {
		return nil, api.WrapInvalidArgument(errors.New("at most one entity cleanup options value may be supplied"))
	}
	var option api.CleanEntityStorageOptions
	if len(options) == 1 {
		option = options[0]
	}
	result := new(api.CleanEntityStorageResult)
	continuationToken := option.ContinuationToken
	seenTokens := make(map[string]struct{})
	if continuationToken != "" {
		seenTokens[continuationToken] = struct{}{}
	}
	for {
		request := &protos.CleanEntityStorageRequest{
			RemoveEmptyEntities:  !option.PreserveEmptyEntities,
			ReleaseOrphanedLocks: !option.PreserveOrphanedLocks,
		}
		if continuationToken != "" {
			request.ContinuationToken = wrapperspb.String(continuationToken)
		}
		response, err := c.client.CleanEntityStorage(ctx, request)
		if err != nil {
			return nil, clientRPCError(ctx, "failed to clean entity storage", err)
		}
		result.EmptyEntitiesRemoved += response.EmptyEntitiesRemoved
		result.OrphanedLocksReleased += response.OrphanedLocksReleased
		result.ContinuationToken = response.ContinuationToken.GetValue()
		if option.SinglePage || result.ContinuationToken == "" {
			return result, nil
		}
		if _, duplicate := seenTokens[result.ContinuationToken]; duplicate {
			return nil, fmt.Errorf("entity cleanup returned repeated continuation token %q", result.ContinuationToken)
		}
		seenTokens[result.ContinuationToken] = struct{}{}
		continuationToken = result.ContinuationToken
	}
}

func entityMetadataFromProto(
	ctx context.Context,
	options *api.LargePayloadOptions,
	converter api.DataConverter,
	entity *protos.EntityMetadata,
	includeState bool,
) (*api.EntityMetadata, error) {
	if entity == nil {
		return nil, fmt.Errorf("entity metadata must not be nil")
	}
	entityID, err := api.EntityIDFromString(entity.InstanceId)
	if err != nil {
		return nil, fmt.Errorf("invalid entity metadata instance ID %q: %w", entity.InstanceId, err)
	}
	metadata := &api.EntityMetadata{
		InstanceID:       entityID,
		BacklogQueueSize: entity.BacklogQueueSize,
		LockedBy:         entity.LockedBy.GetValue(),
		StateIncluded:    includeState,
		HasState:         includeState && entity.SerializedState != nil,
		Converter:        converter,
	}
	if entity.LastModifiedTime != nil {
		metadata.LastModifiedTime = entity.LastModifiedTime.AsTime()
	}
	if !metadata.HasState {
		return metadata, nil
	}
	state, err := largepayload.Hydrate(ctx, options, entity.SerializedState)
	if err != nil {
		return nil, fmt.Errorf("failed to hydrate entity state: %w", err)
	}
	metadata.SerializedState = state.GetValue()
	return metadata, nil
}

func normalizeEntityQueryPrefix(value string) string {
	value = strings.TrimPrefix(value, "@")
	name, key, hasKey := strings.Cut(value, "@")
	prefix := "@" + helpers.ToLowerInvariant(name)
	if hasKey {
		return prefix + "@" + key
	}
	return prefix
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
func (c *TaskHubGrpcClient) makeOrchestrationMetadata(
	ctx context.Context,
	resp *protos.GetInstanceResponse,
) (*api.OrchestrationMetadata, error) {
	if !resp.Exists {
		return nil, api.ErrInstanceNotFound
	}
	if resp.OrchestrationState == nil {
		return nil, fmt.Errorf("orchestration state is nil")
	}
	if err := largepayload.TransformOrchestrationState(ctx, c.largePayloads, resp.OrchestrationState); err != nil {
		return nil, fmt.Errorf("failed to hydrate orchestration metadata: %w", err)
	}
	return orchestrationMetadataFromState(resp.OrchestrationState, c.converter)
}

func orchestrationMetadataFromState(
	state *protos.OrchestrationState,
	converter api.DataConverter,
) (*api.OrchestrationMetadata, error) {
	if state == nil {
		return nil, errors.New("orchestration state is nil")
	}
	metadata := &api.OrchestrationMetadata{
		InstanceID:             api.InstanceID(state.InstanceId),
		Name:                   state.Name,
		Version:                state.Version.GetValue(),
		ExecutionID:            state.ExecutionId.GetValue(),
		ParentInstanceID:       api.InstanceID(state.ParentInstanceId.GetValue()),
		RuntimeStatus:          state.OrchestrationStatus,
		SerializedInput:        state.Input.GetValue(),
		SerializedCustomStatus: state.CustomStatus.GetValue(),
		SerializedOutput:       state.Output.GetValue(),
		FailureDetails:         failure.FromProto(state.FailureDetails),
		Tags:                   tagcodec.DecodeUserTagsOrPlain(state.Tags),
		Converter:              converter,
	}
	if state.ScheduledStartTimestamp != nil {
		metadata.ScheduledStartAt = state.ScheduledStartTimestamp.AsTime()
	}
	if state.CreatedTimestamp != nil {
		metadata.CreatedAt = state.CreatedTimestamp.AsTime()
	}
	if state.LastUpdatedTimestamp != nil {
		metadata.LastUpdatedAt = state.LastUpdatedTimestamp.AsTime()
	}
	if state.CompletedTimestamp != nil {
		metadata.CompletedAt = state.CompletedTimestamp.AsTime()
	}
	return metadata, nil
}
