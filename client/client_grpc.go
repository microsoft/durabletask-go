package client

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/microsoft/durabletask-go/api"
	"github.com/microsoft/durabletask-go/backend"
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
	req := &protos.CreateInstanceRequest{
		Name: orchestrator,
		CreateInstanceOption: &protos.CreateInstanceOption{},
	}
	for _, configure := range opts {
		configure(req)
	}
	if req.InstanceId == "" {
		req.InstanceId = uuid.NewString()
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
	if !resp.Exists {
		return nil, api.ErrInstanceNotFound
	}

	metadata := makeOrchestrationMetadata(resp)
	return metadata, nil
}

// WaitForOrchestrationStart waits for an orchestration to start running and returns an [api.OrchestrationMetadata] object that contains
// metadata about the started instance.
//
// api.ErrInstanceNotFound is returned when the specified orchestration doesn't exist.
func (c *TaskHubGrpcClient) WaitForOrchestrationStart(ctx context.Context, id api.InstanceID, opts ...api.FetchOrchestrationMetadataOptions) (*api.OrchestrationMetadata, error) {
	req := makeGetInstanceRequest(id, opts)
	resp, err := c.client.WaitForInstanceStart(ctx, req)
	if err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, fmt.Errorf("failed to wait for orchestration start: %w", err)
	}
	if !resp.Exists {
		return nil, api.ErrInstanceNotFound
	}
	metadata := makeOrchestrationMetadata(resp)
	return metadata, nil
}

// WaitForOrchestrationCompletion waits for an orchestration to complete and returns an [api.OrchestrationMetadata] object that contains
// metadata about the completed instance.
//
// api.ErrInstanceNotFound is returned when the specified orchestration doesn't exist.
func (c *TaskHubGrpcClient) WaitForOrchestrationCompletion(ctx context.Context, id api.InstanceID, opts ...api.FetchOrchestrationMetadataOptions) (*api.OrchestrationMetadata, error) {
	req := makeGetInstanceRequest(id, opts)
	resp, err := c.client.WaitForInstanceCompletion(ctx, req)
	if err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, fmt.Errorf("failed to wait for orchestration completion: %w", err)
	}
	if !resp.Exists {
		return nil, api.ErrInstanceNotFound
	}
	metadata := makeOrchestrationMetadata(resp)
	return metadata, nil
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
func (c *TaskHubGrpcClient) PurgeOrchestrationState(ctx context.Context, id api.InstanceID) error {
	req := &protos.PurgeInstancesRequest{
		Request: &protos.PurgeInstancesRequest_InstanceId{InstanceId: string(id)},
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

func makeOrchestrationMetadata(resp *protos.GetInstanceResponse) *api.OrchestrationMetadata {
	metadata := &api.OrchestrationMetadata{
		InstanceID:             api.InstanceID(resp.OrchestrationState.InstanceId),
		Name:                   resp.OrchestrationState.Name,
		RuntimeStatus:          resp.OrchestrationState.OrchestrationStatus,
		CreatedAt:              resp.OrchestrationState.CreatedTimestamp.AsTime(),
		LastUpdatedAt:          resp.OrchestrationState.LastUpdatedTimestamp.AsTime(),
		SerializedInput:        resp.OrchestrationState.Input.GetValue(),
		SerializedCustomStatus: resp.OrchestrationState.CustomStatus.GetValue(),
		SerializedOutput:       resp.OrchestrationState.Output.GetValue(),
	}
	return metadata
}
