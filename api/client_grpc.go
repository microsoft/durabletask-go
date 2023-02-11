package api

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/timestamppb"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/microsoft/durabletask-go/internal/protos"
)

type grpcClient struct {
	client protos.TaskHubSidecarServiceClient
}

// NewOrchestrationOptions configures options for starting a new orchestration.
type NewOrchestrationOptions func(*protos.CreateInstanceRequest)

// GetOrchestrationMetadataOptions is a set of options for fetching orchestration metadata.
type FetchOrchestrationMetadataOptions func(*protos.GetInstanceRequest)

// WithInstanceID configures an explicit orchestration instance ID. If not specified,
// a random UUID value will be used for the orchestration instance ID.
func WithInstanceID(id InstanceID) NewOrchestrationOptions {
	return func(req *protos.CreateInstanceRequest) {
		req.InstanceId = string(id)
	}
}

// WithInput configures an input for the orchestration. The specified input must be serializable.
func WithInput(input any) NewOrchestrationOptions {
	return func(req *protos.CreateInstanceRequest) {
		// TODO: Make the encoder configurable
		// TODO: Error handling?
		bytes, _ := json.Marshal(input)
		req.Input = wrapperspb.String(string(bytes))
	}
}

// WithStartTime configures a start time at which the orchestration should start running.
// Note that the actual start time could be later than the specified start time if the
// task hub is under load or if the app is not running at the specified start time.
func WithStartTime(startTime time.Time) NewOrchestrationOptions {
	return func(req *protos.CreateInstanceRequest) {
		req.ScheduledStartTimestamp = timestamppb.New(startTime)
	}
}

// WithFetchPayloads configures whether to load orchestration inputs, outputs, and custom status values, which could be large.
func WithFetchPayloads(fetchPayloads bool) FetchOrchestrationMetadataOptions {
	return func(req *protos.GetInstanceRequest) {
		req.GetInputsAndOutputs = fetchPayloads
	}
}

// NewTaskHubGrpcClient creates a client that can be used to manage orchestrations over a gRPC connection.
// The gRPC connection must be to a task hub worker that understands the Durable Task gRPC protocol.
func NewTaskHubGrpcClient(cc grpc.ClientConnInterface) *grpcClient {
	return &grpcClient{
		client: protos.NewTaskHubSidecarServiceClient(cc),
	}
}

// ScheduleNewOrchestration schedules a new orchestration instance with a specified set of options for execution.
func (c *grpcClient) ScheduleNewOrchestration(ctx context.Context, orchestrator string, opts ...NewOrchestrationOptions) (InstanceID, error) {
	req := &protos.CreateInstanceRequest{Name: orchestrator}
	for _, configure := range opts {
		configure(req)
	}
	if req.InstanceId == "" {
		req.InstanceId = uuid.NewString()
	}

	resp, err := c.client.StartInstance(ctx, req)
	if err != nil {
		return EmptyInstanceID, fmt.Errorf("Failed to start orchestrator: %w", err)
	}
	return InstanceID(resp.InstanceId), nil
}

// FetchOrchestrationMetadata fetches metadata for the specified orchestration from the configured task hub.
//
// ErrInstanceNotFound is returned when the specified orchestration doesn't exist.
func (c *grpcClient) FetchOrchestrationMetadata(ctx context.Context, id InstanceID, opts ...FetchOrchestrationMetadataOptions) (*OrchestrationMetadata, error) {
	req := makeGetInstanceRequest(id, opts)
	resp, err := c.client.GetInstance(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("Failed to fetch orchestration metadata: %w", err)
	}
	if !resp.Exists {
		return nil, ErrInstanceNotFound
	}

	metadata := makeOrchestrationMetadata(resp)
	return metadata, nil
}

// WaitForOrchestrationStart waits for an orchestration to start running and returns an [OrchestrationMetadata] object that contains
// metadata about the started instance.
//
// ErrInstanceNotFound is returned when the specified orchestration doesn't exist.
func (c *grpcClient) WaitForOrchestrationStart(ctx context.Context, id InstanceID, opts ...FetchOrchestrationMetadataOptions) (*OrchestrationMetadata, error) {
	req := makeGetInstanceRequest(id, opts)
	resp, err := c.client.WaitForInstanceStart(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("Failed to wait for orchestration start: %w", err)
	}
	if !resp.Exists {
		return nil, ErrInstanceNotFound
	}
	metadata := makeOrchestrationMetadata(resp)
	return metadata, nil
}

// WaitForOrchestrationCompletion waits for an orchestration to complete and returns an [OrchestrationMetadata] object that contains
// metadata about the completed instance.
//
// ErrInstanceNotFound is returned when the specified orchestration doesn't exist.
func (c *grpcClient) WaitForOrchestrationCompletion(ctx context.Context, id InstanceID, opts ...FetchOrchestrationMetadataOptions) (*OrchestrationMetadata, error) {
	req := makeGetInstanceRequest(id, opts)
	resp, err := c.client.WaitForInstanceCompletion(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("Failed to wait for orchestration completion: %w", err)
	}
	if !resp.Exists {
		return nil, ErrInstanceNotFound
	}
	metadata := makeOrchestrationMetadata(resp)
	return metadata, nil
}

// TerminateOrchestration terminates a running orchestration by causing it to stop receiving new events and
// putting it directly into the TERMINATED state.
func (c *grpcClient) TerminateOrchestration(ctx context.Context, id InstanceID, reason string) error {
	req := &protos.TerminateRequest{
		InstanceId: string(id),
		Output:     wrapperspb.String(reason),
	}
	_, err := c.client.TerminateInstance(ctx, req)
	if err != nil {
		return fmt.Errorf("failed to terminate instance: %w", err)
	}
	return nil
}

// SuspendOrchestration suspends an orchestration instance, halting processing of its events until a "resume" operation resumes it.
//
// Note that suspended orchestrations are still considered to be "running" even though they will not process events.
func (c *grpcClient) SuspendOrchestration(ctx context.Context, id InstanceID, reason string) error {
	req := &protos.SuspendRequest{
		InstanceId: string(id),
		Reason:     wrapperspb.String(reason),
	}
	if _, err := c.client.SuspendInstance(ctx, req); err != nil {
		return fmt.Errorf("failed to suspend orchestration: %w", err)
	}
	return nil
}

// ResumeOrchestration resumes an orchestration instance that was previously suspended.
func (c *grpcClient) ResumeOrchestration(ctx context.Context, id InstanceID, reason string) error {
	req := &protos.ResumeRequest{
		InstanceId: string(id),
		Reason:     wrapperspb.String(reason),
	}
	if _, err := c.client.ResumeInstance(ctx, req); err != nil {
		return fmt.Errorf("failed to resume orchestration: %w", err)
	}
	return nil
}

func makeGetInstanceRequest(id InstanceID, opts []FetchOrchestrationMetadataOptions) *protos.GetInstanceRequest {
	req := &protos.GetInstanceRequest{InstanceId: string(id)}
	for _, configure := range opts {
		configure(req)
	}
	return req
}

func makeOrchestrationMetadata(resp *protos.GetInstanceResponse) *OrchestrationMetadata {
	metadata := &OrchestrationMetadata{
		InstanceID:             InstanceID(resp.OrchestrationState.InstanceId),
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
