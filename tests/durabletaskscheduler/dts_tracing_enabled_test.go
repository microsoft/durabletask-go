package durabletaskscheduler_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/microsoft/durabletask-go/api"
	durabletaskclient "github.com/microsoft/durabletask-go/client"
	"github.com/microsoft/durabletask-go/durabletaskscheduler"
	"github.com/microsoft/durabletask-go/task"
	"github.com/microsoft/durabletask-go/tests/tracingtree"
	"github.com/microsoft/durabletask-go/tracing"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

// This uses an explicit provider without changing the global provider used by
// the disabled-mode tests in dts_tracing_tree_test.go.
func TestDTSEmulatorTracingEnabledTreeAndEntityRoots(t *testing.T) {
	options := emulatorOptions(t)
	exporter := tracetest.NewInMemoryExporter()
	provider := sdktrace.NewTracerProvider(
		tracing.WithIDGenerator(nil),
		tracing.WithSampler(nil),
		sdktrace.WithSyncer(exporter),
	)
	t.Cleanup(func() { require.NoError(t, provider.Shutdown(context.Background())) })

	entityID := api.NewEntityID("DTSEnabledEntity", uuid.NewString())
	registry := task.NewTaskRegistry()
	require.NoError(t, registry.AddEntityN(entityID.Name, func(ctx *task.EntityContext) (any, error) {
		if ctx.Operation == "fail" {
			return nil, errors.New("traced entity failure")
		}
		return "entity", nil
	}))
	require.NoError(t, registry.AddActivityN("DTSEnabledEcho", func(task.ActivityContext) (any, error) {
		return "echo", nil
	}))
	require.NoError(t, registry.AddOrchestratorN("DTSEnabledChild", func(ctx *task.OrchestrationContext) (any, error) {
		var result string
		err := ctx.CallActivity("DTSEnabledEcho").Await(&result)
		return result, err
	}))
	require.NoError(t, registry.AddOrchestratorN("DTSEnabledReceiver", func(ctx *task.OrchestrationContext) (any, error) {
		var result string
		err := ctx.WaitForSingleEvent("ping", time.Minute).Await(&result)
		return result, err
	}))
	require.NoError(t, registry.AddOrchestratorN("DTSEnabledParent", func(ctx *task.OrchestrationContext) (any, error) {
		var receiver api.InstanceID
		if err := ctx.GetInput(&receiver); err != nil {
			return nil, err
		}
		var child string
		if err := ctx.CallSubOrchestrator("DTSEnabledChild",
			task.WithSubOrchestrationInstanceID(string(ctx.ID)+"_child")).Await(&child); err != nil {
			return nil, err
		}
		if err := ctx.CreateTimer(10 * time.Millisecond).Await(nil); err != nil {
			return nil, err
		}
		var proceed string
		if err := ctx.WaitForSingleEvent("proceed", time.Minute).Await(&proceed); err != nil {
			return nil, err
		}
		if err := ctx.SendEvent(receiver, "ping", "pong"); err != nil {
			return nil, err
		}
		if err := ctx.SignalEntity(entityID, "signal"); err != nil {
			return nil, err
		}
		var entity string
		if err := ctx.CallEntity(entityID, "get").Await(&entity); err != nil {
			return nil, err
		}
		if err := ctx.CallEntity(entityID, "fail").Await(nil); err == nil {
			return nil, errors.New("expected entity call failure")
		}
		return child + "+" + proceed + "+" + entity, nil
	}))

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	managementClient, err := durabletaskscheduler.NewClient(ctx, options, api.DefaultLogger(),
		durabletaskclient.WithTracerProvider(provider))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, managementClient.Close()) })
	worker, err := durabletaskscheduler.NewWorker(options, registry, api.DefaultLogger(),
		durabletaskclient.WithAutoWorkItemFilters(),
		durabletaskclient.WithWorkerTracerProvider(provider))
	require.NoError(t, err)
	require.NoError(t, worker.Start(ctx))
	t.Cleanup(func() {
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer shutdownCancel()
		require.NoError(t, worker.Shutdown(shutdownCtx))
	})

	receiverID, err := managementClient.ScheduleNewOrchestration(ctx, "DTSEnabledReceiver",
		api.WithInstanceID(uniqueInstanceID("go-enabled-receiver")))
	require.NoError(t, err)
	callerCtx, caller := provider.Tracer("dts-enabled-test").Start(ctx, "caller/enabled")
	parentID, err := managementClient.ScheduleNewOrchestration(callerCtx, "DTSEnabledParent",
		api.WithInstanceID(uniqueInstanceID("go-enabled-parent")), api.WithInput(receiverID))
	require.NoError(t, err)
	require.NoError(t, managementClient.SignalEntity(callerCtx, entityID, "clientSignal"))
	caller.End()
	_, err = managementClient.WaitForOrchestrationStart(ctx, parentID)
	require.NoError(t, err)
	require.NoError(t, managementClient.RaiseEvent(callerCtx, parentID, "proceed", api.WithEventPayload("go")))

	parent, err := managementClient.WaitForOrchestrationCompletion(ctx, parentID, api.WithFetchPayloads(true))
	require.NoError(t, err)
	require.Equal(t, api.RUNTIME_STATUS_COMPLETED, parent.RuntimeStatus)
	require.Equal(t, `"echo+go+entity"`, parent.SerializedOutput)
	receiver, err := managementClient.WaitForOrchestrationCompletion(ctx, receiverID, api.WithFetchPayloads(true))
	require.NoError(t, err)
	require.Equal(t, api.RUNTIME_STATUS_COMPLETED, receiver.RuntimeStatus)
	require.Equal(t, `"pong"`, receiver.SerializedOutput)

	// Completion can become visible to the client just before the worker exports
	// its accepted response. Multiple dispatches must not duplicate replay spans.
	require.Eventually(t, func() bool { return len(exporter.GetSpans()) == 19 },
		10*time.Second, 20*time.Millisecond, "exported spans: %s",
		tracingtree.Describe(exporter.GetSpans().Snapshots()))
	spans := exporter.GetSpans().Snapshots()
	find := func(name string, kind trace.SpanKind) sdktrace.ReadOnlySpan {
		t.Helper()
		var matches []sdktrace.ReadOnlySpan
		for _, span := range spans {
			if span.Name() == name && span.SpanKind() == kind {
				matches = append(matches, span)
			}
		}
		require.Len(t, matches, 1, "expected one %s %s; spans: %s", kind, name, tracingtree.Describe(spans))
		span := matches[0]
		require.True(t, span.SpanContext().IsSampled())
		require.False(t, span.StartTime().IsZero())
		require.False(t, span.EndTime().Before(span.StartTime()))
		if name != "caller/enabled" {
			require.Equal(t, "durabletask", span.InstrumentationScope().Name)
		}
		return span
	}
	childOf := func(child, parent sdktrace.ReadOnlySpan) {
		t.Helper()
		require.Equal(t, parent.SpanContext().TraceID(), child.SpanContext().TraceID(), child.Name())
		require.Equal(t, parent.SpanContext().SpanID(), child.Parent().SpanID(), child.Name())
	}
	callerSpan := find("caller/enabled", trace.SpanKindInternal)
	create := find("create_orchestration:DTSEnabledParent", trace.SpanKindProducer)
	childOf(create, callerSpan)
	orchestration := find("orchestration:DTSEnabledParent", trace.SpanKindServer)
	childOf(orchestration, create)
	childClient := find("orchestration:DTSEnabledChild", trace.SpanKindClient)
	childOf(childClient, orchestration)
	childServer := find("orchestration:DTSEnabledChild", trace.SpanKindServer)
	childOf(childServer, childClient)
	activityClient := find("activity:DTSEnabledEcho", trace.SpanKindClient)
	childOf(activityClient, childServer)
	childOf(find("activity:DTSEnabledEcho", trace.SpanKindServer), activityClient)
	childOf(find("orchestration:DTSEnabledParent:timer", trace.SpanKindInternal), orchestration)
	childOf(find("orchestration_event:proceed", trace.SpanKindProducer), callerSpan)
	childOf(find("orchestration_event:ping", trace.SpanKindProducer), orchestration)
	receiverCreate := find("create_orchestration:DTSEnabledReceiver", trace.SpanKindProducer)
	require.False(t, receiverCreate.Parent().IsValid())
	childOf(find("orchestration:DTSEnabledReceiver", trace.SpanKindServer), receiverCreate)

	clientSignal := "entity:" + entityID.Name + ":clientSignal"
	for _, span := range spans {
		require.False(t, span.Name() == clientSignal && span.SpanKind() == trace.SpanKindProducer,
			"client SignalEntity propagates caller context without emitting a span")
	}
	clientSignalExecution := find(clientSignal, trace.SpanKindConsumer)
	require.False(t, clientSignalExecution.Parent().IsValid(), "V2 entity executions must be independent roots")
	require.NotEqual(t, caller.SpanContext().TraceID(), clientSignalExecution.SpanContext().TraceID())

	for _, operation := range []struct {
		name          string
		send, execute trace.SpanKind
		attribute     string
		status        codes.Code
	}{
		{"signal", trace.SpanKindProducer, trace.SpanKindConsumer, "signal_entity", codes.Ok},
		{"get", trace.SpanKindClient, trace.SpanKindServer, "call_entity", codes.Ok},
		{"fail", trace.SpanKindClient, trace.SpanKindServer, "call_entity", codes.Error},
	} {
		name := "entity:" + entityID.Name + ":" + operation.name
		send := find(name, operation.send)
		childOf(send, orchestration)
		execute := find(name, operation.execute)
		require.False(t, execute.Parent().IsValid(), "V2 entity executions must be independent roots")
		require.NotEqual(t, caller.SpanContext().TraceID(), execute.SpanContext().TraceID())
		require.Empty(t, execute.Links(), "V2 has no propagated parent or synthetic request-ID link")
		require.Equal(t, operation.status, execute.Status().Code)
		for _, span := range []sdktrace.ReadOnlySpan{send, execute} {
			attrs := make(map[string]string)
			for _, attr := range span.Attributes() {
				attrs[string(attr.Key)] = attr.Value.AsString()
			}
			require.Equal(t, operation.attribute, attrs["durabletask.task.operation"])
		}
		if operation.status == codes.Error {
			require.Equal(t, codes.Error, send.Status().Code)
			require.Contains(t, execute.Status().Description, "traced entity failure")
		}
	}
	// Every exported span was accounted for above; event receipt emits no
	// CONSUMER span and does not reparent the receiving orchestration.
	require.Len(t, spans, 19)
}
