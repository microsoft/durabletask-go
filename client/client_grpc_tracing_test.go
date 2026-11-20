package client

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"

	"github.com/microsoft/durabletask-go/api"
	"github.com/microsoft/durabletask-go/internal/helpers"
	"github.com/microsoft/durabletask-go/internal/protos"
	"github.com/microsoft/durabletask-go/task"
	"github.com/microsoft/durabletask-go/tracing"
)

type tracingSchedulerClient struct {
	protos.TaskHubSidecarServiceClient
	start *protos.CreateInstanceRequest
	event *protos.RaiseEventRequest
	err   error
}

func (c *tracingSchedulerClient) StartInstance(
	_ context.Context,
	req *protos.CreateInstanceRequest,
	_ ...grpc.CallOption,
) (*protos.CreateInstanceResponse, error) {
	c.start = req
	if c.err != nil {
		return nil, c.err
	}
	return &protos.CreateInstanceResponse{InstanceId: req.InstanceId}, nil
}

func (c *tracingSchedulerClient) RaiseEvent(
	_ context.Context,
	req *protos.RaiseEventRequest,
	_ ...grpc.CallOption,
) (*protos.RaiseEventResponse, error) {
	c.event = req
	if c.err != nil {
		return nil, c.err
	}
	return &protos.RaiseEventResponse{}, nil
}

func newTracedClient(fake *tracingSchedulerClient, opts ...TaskHubGrpcClientOption) *TaskHubGrpcClient {
	client := &TaskHubGrpcClient{client: fake, logger: api.DefaultLogger(), converter: api.DefaultDataConverter()}
	for _, configure := range opts {
		configure(client)
	}
	return client
}

func TestClientTracingProducerSpans(t *testing.T) {
	recorder := tracetest.NewSpanRecorder()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	fake := &tracingSchedulerClient{}
	client := newTracedClient(fake, WithTracerProvider(provider))
	caller, _ := newTraceParent(trace.FlagsSampled)
	ctx := trace.ContextWithRemoteSpanContext(context.Background(), caller)

	id, err := client.ScheduleNewOrchestration(ctx, "Orch",
		api.WithInstanceID("instance-1"), api.WithVersion("1.0"))
	require.NoError(t, err)
	require.NoError(t, client.RaiseEvent(ctx, id, "Approved"))

	spans := recorder.Ended()
	require.Len(t, spans, 2)
	create, event := spans[0], spans[1]

	require.Equal(t, "create_orchestration:Orch@(1.0)", create.Name())
	require.Equal(t, trace.SpanKindProducer, create.SpanKind())
	require.Equal(t, caller.SpanID(), create.Parent().SpanID())
	require.Equal(t, caller.TraceID(), create.SpanContext().TraceID())
	require.Equal(t, "orchestration", spanAttribute(create, "durabletask.type"))
	require.Equal(t, "Orch", spanAttribute(create, "durabletask.task.name"))
	require.Equal(t, "1.0", spanAttribute(create, "durabletask.task.version"))
	require.Equal(t, "instance-1", spanAttribute(create, "durabletask.task.instance_id"))
	require.Equal(t, codes.Unset, create.Status().Code)
	// The orchestration is parented to the PRODUCER span, keeping tracestate.
	propagated := helpers.SpanContextFromTraceContext(fake.start.ParentTraceContext)
	require.Equal(t, create.SpanContext().SpanID(), propagated.SpanID())
	require.Equal(t, caller.TraceState().String(), propagated.TraceState().String())

	require.Equal(t, "orchestration_event:Approved", event.Name())
	require.Equal(t, trace.SpanKindProducer, event.SpanKind())
	require.Equal(t, caller.SpanID(), event.Parent().SpanID())
	require.Equal(t, "event", spanAttribute(event, "durabletask.type"))
	require.Equal(t, "Approved", spanAttribute(event, "durabletask.task.name"))
	require.Equal(t, "instance-1", spanAttribute(event, "durabletask.event.target_instance_id"))
	require.Equal(t, codes.Unset, event.Status().Code)
}

func TestClientTracingFailedCallsMarkSpansAsErrors(t *testing.T) {
	recorder := tracetest.NewSpanRecorder()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	fake := &tracingSchedulerClient{err: errors.New("unavailable")}
	client := newTracedClient(fake, WithTracerProvider(provider))

	_, err := client.ScheduleNewOrchestration(context.Background(), "Orch")
	require.Error(t, err)
	require.Error(t, client.RaiseEvent(context.Background(), "instance-1", "Approved"))

	spans := recorder.Ended()
	require.Len(t, spans, 2)
	for _, span := range spans {
		require.Equal(t, codes.Error, span.Status().Code, span.Name())
		require.Contains(t, span.Status().Description, "unavailable")
		require.Len(t, span.Events(), 1)
		require.Equal(t, "exception", span.Events()[0].Name)
	}
	require.Equal(t, "create_orchestration:Orch", spans[0].Name())
	require.Empty(t, spanAttribute(spans[0], "durabletask.task.version"))
}

func TestClientTracingUnsampledCallerPropagatesProducerContext(t *testing.T) {
	recorder := tracetest.NewSpanRecorder()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	fake := &tracingSchedulerClient{}
	client := newTracedClient(fake, WithTracerProvider(provider))
	caller, _ := newTraceParent(0)
	ctx := trace.ContextWithRemoteSpanContext(context.Background(), caller)

	_, err := client.ScheduleNewOrchestration(ctx, "Orch")
	require.NoError(t, err)

	require.Empty(t, recorder.Ended())
	propagated := helpers.SpanContextFromTraceContext(fake.start.ParentTraceContext)
	require.True(t, propagated.IsValid())
	require.False(t, propagated.IsSampled())
	require.Equal(t, caller.TraceID(), propagated.TraceID())
	require.NotEqual(t, caller.SpanID(), propagated.SpanID())
}

func TestClientTracingParentsWorkerOrchestrationSpan(t *testing.T) {
	sampledCaller, _ := newTraceParent(trace.FlagsSampled)
	for name, caller := range map[string]trace.SpanContext{
		"caller-span": sampledCaller,
		"root":        {},
	} {
		t.Run(name, func(t *testing.T) {
			// The client accepts the provider configuration the worker requires,
			// so applications can share one provider.
			recorder := tracetest.NewSpanRecorder()
			provider := sdktrace.NewTracerProvider(
				tracing.WithIDGenerator(nil),
				tracing.WithSampler(nil),
				sdktrace.WithSpanProcessor(recorder),
			)
			fake := &tracingSchedulerClient{}
			client := newTracedClient(fake, WithTracerProvider(provider))
			ctx := context.Background()
			if caller.IsValid() {
				ctx = trace.ContextWithRemoteSpanContext(ctx, caller)
			}
			_, err := client.ScheduleNewOrchestration(ctx, "Linked", api.WithInstanceID("linked"))
			require.NoError(t, err)
			create := recorder.Ended()[0]
			if caller.IsValid() {
				require.Equal(t, caller.SpanID(), create.Parent().SpanID())
			} else {
				require.False(t, create.Parent().IsValid())
			}

			registry := task.NewTaskRegistry()
			require.NoError(t, registry.AddOrchestratorN("Linked", func(*task.OrchestrationContext) (any, error) {
				return nil, nil
			}))
			h := newTracingHarness(t, registry, true)
			backend := newFakeOrchestrationBackend("linked", helpers.NewExecutionStartedEvent(
				"Linked", "linked", nil, nil, fake.start.ParentTraceContext, nil))
			backend.dispatch(t, h)

			orchestration := spansNamed(h.spans(), "orchestration:Linked")
			require.Len(t, orchestration, 1)
			require.Equal(t, create.SpanContext().TraceID(), orchestration[0].SpanContext().TraceID())
			require.Equal(t, create.SpanContext().SpanID(), orchestration[0].Parent().SpanID())
		})
	}
}

func TestClientTracingDisabledKeepsPropagationOnly(t *testing.T) {
	for name, opts := range map[string][]TaskHubGrpcClientOption{
		"default":      nil,
		"nil-provider": {WithTracerProvider(nil)},
	} {
		t.Run(name, func(t *testing.T) {
			fake := &tracingSchedulerClient{}
			client := newTracedClient(fake, opts...)
			require.Nil(t, client.tracer)

			sampled, wire := newTraceParent(trace.FlagsSampled)
			ctx := trace.ContextWithRemoteSpanContext(context.Background(), sampled)
			_, err := client.ScheduleNewOrchestration(ctx, "Orch")
			require.NoError(t, err)
			require.Equal(t, wire.GetTraceParent(), fake.start.ParentTraceContext.GetTraceParent())

			unsampled, _ := newTraceParent(0)
			ctx = trace.ContextWithRemoteSpanContext(context.Background(), unsampled)
			_, err = client.ScheduleNewOrchestration(ctx, "Orch")
			require.NoError(t, err)
			require.Nil(t, fake.start.ParentTraceContext)
		})
	}
}
