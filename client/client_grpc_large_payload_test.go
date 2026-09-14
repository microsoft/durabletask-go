package client

import (
	"context"
	"encoding/base64"
	"fmt"
	"testing"

	"github.com/microsoft/durabletask-go/api"
	"github.com/microsoft/durabletask-go/internal/largepayload"
	"github.com/microsoft/durabletask-go/internal/protos"
	"github.com/microsoft/durabletask-go/payload"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/timestamppb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

type largePayloadSchedulerClient struct {
	protos.TaskHubSidecarServiceClient
	start     *protos.CreateInstanceRequest
	event     *protos.RaiseEventRequest
	terminate *protos.TerminateRequest
	signal    *protos.SignalEntityRequest
	state     *protos.OrchestrationState
}

func (c *largePayloadSchedulerClient) StartInstance(
	_ context.Context,
	req *protos.CreateInstanceRequest,
	_ ...grpc.CallOption,
) (*protos.CreateInstanceResponse, error) {
	c.start = req
	return &protos.CreateInstanceResponse{InstanceId: req.InstanceId}, nil
}

func (c *largePayloadSchedulerClient) RaiseEvent(
	_ context.Context,
	req *protos.RaiseEventRequest,
	_ ...grpc.CallOption,
) (*protos.RaiseEventResponse, error) {
	c.event = req
	return &protos.RaiseEventResponse{}, nil
}

func (c *largePayloadSchedulerClient) TerminateInstance(
	_ context.Context,
	req *protos.TerminateRequest,
	_ ...grpc.CallOption,
) (*protos.TerminateResponse, error) {
	c.terminate = req
	return &protos.TerminateResponse{}, nil
}

func (c *largePayloadSchedulerClient) SignalEntity(
	_ context.Context,
	req *protos.SignalEntityRequest,
	_ ...grpc.CallOption,
) (*protos.SignalEntityResponse, error) {
	c.signal = req
	return &protos.SignalEntityResponse{}, nil
}

func (c *largePayloadSchedulerClient) GetInstance(
	context.Context,
	*protos.GetInstanceRequest,
	...grpc.CallOption,
) (*protos.GetInstanceResponse, error) {
	return &protos.GetInstanceResponse{Exists: true, OrchestrationState: c.state}, nil
}

type textDataConverter struct{}

func (textDataConverter) Serialize(value any) (string, error) {
	return "text:" + base64.RawStdEncoding.EncodeToString([]byte(fmt.Sprint(value))), nil
}

func (textDataConverter) Deserialize(payload string, target any) error {
	if len(payload) < len("text:") || payload[:len("text:")] != "text:" {
		return fmt.Errorf("unexpected payload %q", payload)
	}
	data, err := base64.RawStdEncoding.DecodeString(payload[len("text:"):])
	if err != nil {
		return err
	}
	value, ok := target.(*string)
	if !ok {
		return fmt.Errorf("unsupported target %T", target)
	}
	*value = string(data)
	return nil
}

func TestTaskHubGrpcClientLargePayloadManagementFields(t *testing.T) {
	store := payload.NewMemoryStore()
	options := &api.LargePayloadOptions{
		Store:           store,
		Resolver:        store,
		ThresholdBytes:  1,
		MaxPayloadBytes: 1024,
	}
	fake := &largePayloadSchedulerClient{}
	client := &TaskHubGrpcClient{
		client:        fake,
		logger:        api.DefaultLogger(),
		largePayloads: options,
	}
	ctx := context.Background()
	id, err := client.ScheduleNewOrchestration(ctx, "orchestrator", api.WithRawInput("create-payload"))
	require.NoError(t, err)
	require.NotEmpty(t, id)
	requireLargePayloadValue(t, options, fake.start.Input, "create-payload")

	require.NoError(t, client.RaiseEvent(ctx, id, "event", api.WithRawEventData("event-payload")))
	requireLargePayloadValue(t, options, fake.event.Input, "event-payload")

	require.NoError(t, client.TerminateOrchestration(ctx, id, api.WithRawOutput("terminate-payload")))
	requireLargePayloadValue(t, options, fake.terminate.Output, "terminate-payload")

	input, err := largepayload.Externalize(ctx, options, wrapperspb.String("metadata-input"))
	require.NoError(t, err)
	output, err := largepayload.Externalize(ctx, options, wrapperspb.String("metadata-output"))
	require.NoError(t, err)
	status, err := largepayload.Externalize(ctx, options, wrapperspb.String("metadata-status"))
	require.NoError(t, err)
	fake.state = &protos.OrchestrationState{
		InstanceId:           string(id),
		Name:                 "orchestrator",
		CreatedTimestamp:     timestamppb.Now(),
		LastUpdatedTimestamp: timestamppb.Now(),
		Input:                input,
		Output:               output,
		CustomStatus:         status,
	}
	metadata, err := client.FetchOrchestrationMetadata(ctx, id, api.WithFetchPayloads(true))
	require.NoError(t, err)
	require.Equal(t, "metadata-input", metadata.SerializedInput)
	require.Equal(t, "metadata-output", metadata.SerializedOutput)
	require.Equal(t, "metadata-status", metadata.SerializedCustomStatus)
}

func TestTaskHubGrpcClientUsesConverterBeforeLargePayloadExternalization(t *testing.T) {
	store := payload.NewMemoryStore()
	options := &api.LargePayloadOptions{
		Store:           store,
		Resolver:        store,
		ThresholdBytes:  1,
		MaxPayloadBytes: 1024,
	}
	fake := &largePayloadSchedulerClient{}
	converter := textDataConverter{}
	client := &TaskHubGrpcClient{
		client:        fake,
		logger:        api.DefaultLogger(),
		largePayloads: options,
		converter:     converter,
	}
	ctx := context.Background()

	id, err := client.ScheduleNewOrchestration(ctx, "orchestrator", api.WithInput("start"))
	require.NoError(t, err)
	requireLargePayloadValue(t, options, fake.start.Input, mustSerialize(t, converter, "start"))

	require.NoError(t, client.RaiseEvent(ctx, id, "event", api.WithEventPayload("event")))
	requireLargePayloadValue(t, options, fake.event.Input, mustSerialize(t, converter, "event"))

	require.NoError(t, client.TerminateOrchestration(ctx, id, api.WithOutput("output")))
	requireLargePayloadValue(t, options, fake.terminate.Output, mustSerialize(t, converter, "output"))

	entityID := api.NewEntityID("counter", "one")
	require.NoError(t, client.SignalEntity(ctx, entityID, "add", api.WithSignalInput("signal")))
	requireLargePayloadValue(t, options, fake.signal.Input, mustSerialize(t, converter, "signal"))

	serializedInput := mustSerialize(t, converter, "metadata")
	externalizedInput, err := largepayload.Externalize(ctx, options, wrapperspb.String(serializedInput))
	require.NoError(t, err)
	fake.state = &protos.OrchestrationState{
		InstanceId:           string(id),
		Name:                 "orchestrator",
		CreatedTimestamp:     timestamppb.Now(),
		LastUpdatedTimestamp: timestamppb.Now(),
		Input:                externalizedInput,
	}
	metadata, err := client.FetchOrchestrationMetadata(ctx, id)
	require.NoError(t, err)
	var decoded string
	require.NoError(t, metadata.ReadInput(&decoded))
	require.Equal(t, "metadata", decoded)
}

func mustSerialize(t *testing.T, converter api.DataConverter, value any) string {
	t.Helper()
	payload, err := converter.Serialize(value)
	require.NoError(t, err)
	return payload
}

func requireLargePayloadValue(
	t *testing.T,
	options *api.LargePayloadOptions,
	value *wrapperspb.StringValue,
	expected string,
) {
	t.Helper()
	require.NotEqual(t, expected, value.GetValue())
	hydrated, err := largepayload.Hydrate(context.Background(), options, value)
	require.NoError(t, err)
	require.Equal(t, expected, hydrated.GetValue())
}
