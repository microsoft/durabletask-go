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
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

// TestDTSEmulatorTracingEnabledTreeAndEntityRoots is the end-to-end check of
// opt-in tracing (see tracing/README.md). It asserts the exact exported span
// set: client/orchestration/activity/sub-orchestration/timer/event parentage,
// activity failure status, distinct span identities across continue-as-new,
// independent entity roots with application spans nested under them, and a
// client entity signal that emits no client-side span. The disabled-mode tests
// in dts_tracing_tree_test.go assert propagation without SDK spans. Both run
// against a DTS emulator through DTS_EMULATOR_ENDPOINT or DTS_CONNECTION_STRING
// and skip when neither is set.
//
// It uses an explicit provider without changing the global provider used by
// the disabled-mode tests.
func TestDTSEmulatorTracingEnabledTreeAndEntityRoots(t *testing.T) {
	const (
		durableScope = "Microsoft.DurableTask"
		testScope    = "dts-enabled-test"
		spanCount    = 25
	)
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
		switch ctx.Operation {
		case "fail":
			return nil, errors.New("traced entity failure")
		case "get":
			_, span := provider.Tracer(testScope).Start(ctx.Context(), "app/entity-get")
			span.End()
		}
		return "entity", nil
	}))
	require.NoError(t, registry.AddActivityN("DTSEnabledEcho", func(task.ActivityContext) (any, error) {
		return "echo", nil
	}))
	require.NoError(t, registry.AddActivityN("DTSEnabledFail", func(task.ActivityContext) (any, error) {
		return nil, errors.New("traced activity failure")
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
	require.NoError(t, registry.AddOrchestratorN("DTSEnabledLoop", func(ctx *task.OrchestrationContext) (any, error) {
		var generation int
		if err := ctx.GetInput(&generation); err != nil {
			return nil, err
		}
		if generation == 0 {
			ctx.ContinueAsNew(1)
			return nil, nil
		}
		return generation, nil
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
		if err := ctx.CallActivity("DTSEnabledFail").Await(nil); err == nil {
			return nil, errors.New("expected activity failure")
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
	require.NoError(t, worker.Start(context.Background()))
	t.Cleanup(func() {
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer shutdownCancel()
		require.NoError(t, worker.Shutdown(shutdownCtx))
	})

	receiverID, err := managementClient.ScheduleNewOrchestration(ctx, "DTSEnabledReceiver",
		api.WithInstanceID(uniqueInstanceID("go-enabled-receiver")))
	require.NoError(t, err)
	callerCtx, caller := provider.Tracer(testScope).Start(ctx, "caller/enabled")
	parentID, err := managementClient.ScheduleNewOrchestration(callerCtx, "DTSEnabledParent",
		api.WithInstanceID(uniqueInstanceID("go-enabled-parent")), api.WithInput(receiverID))
	require.NoError(t, err)
	loopID, err := managementClient.ScheduleNewOrchestration(callerCtx, "DTSEnabledLoop",
		api.WithInstanceID(uniqueInstanceID("go-enabled-loop")), api.WithInput(0))
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
	loop, err := managementClient.WaitForOrchestrationCompletion(ctx, loopID, api.WithFetchPayloads(true))
	require.NoError(t, err)
	require.Equal(t, api.RUNTIME_STATUS_COMPLETED, loop.RuntimeStatus)
	require.Equal(t, "1", loop.SerializedOutput)

	// Completion can become visible to the client just before the worker exports
	// its accepted response. Multiple dispatches must not duplicate replay spans,
	// including spans exported late.
	require.Eventually(t, func() bool { return len(exporter.GetSpans()) == spanCount },
		10*time.Second, 20*time.Millisecond, "exported spans: %s",
		tracingtree.Describe(exporter.GetSpans().Snapshots()))
	require.Never(t, func() bool { return len(exporter.GetSpans()) > spanCount },
		time.Second, 50*time.Millisecond, "late duplicate spans")
	spans := exporter.GetSpans().Snapshots()
	for _, span := range spans {
		scope := durableScope
		if span.Name() == "caller/enabled" || span.Name() == "app/entity-get" {
			scope = testScope
		}
		require.Equal(t, scope, span.InstrumentationScope().Name, span.Name())
		require.True(t, span.SpanContext().IsSampled(), span.Name())
		require.False(t, span.StartTime().IsZero(), span.Name())
		require.False(t, span.EndTime().Before(span.StartTime()), span.Name())
	}
	findAll := func(name string, kind trace.SpanKind) []sdktrace.ReadOnlySpan {
		var matches []sdktrace.ReadOnlySpan
		for _, span := range spans {
			if span.Name() == name && span.SpanKind() == kind {
				matches = append(matches, span)
			}
		}
		return matches
	}
	find := func(name string, kind trace.SpanKind) sdktrace.ReadOnlySpan {
		t.Helper()
		matches := findAll(name, kind)
		require.Len(t, matches, 1, "expected one %s %s; spans: %s", kind, name, tracingtree.Describe(spans))
		return matches[0]
	}
	childOf := func(child, parent sdktrace.ReadOnlySpan) {
		t.Helper()
		require.Equal(t, parent.SpanContext().TraceID(), child.SpanContext().TraceID(), child.Name())
		require.Equal(t, parent.SpanContext().SpanID(), child.Parent().SpanID(), child.Name())
	}
	attrs := func(span sdktrace.ReadOnlySpan) map[string]attribute.Value {
		values := make(map[string]attribute.Value)
		for _, attr := range span.Attributes() {
			values[string(attr.Key)] = attr.Value
		}
		return values
	}
	requireTask := func(span sdktrace.ReadOnlySpan, instanceID api.InstanceID) {
		t.Helper()
		values := attrs(span)
		require.Equal(t, string(instanceID), values["durabletask.task.instance_id"].AsString(), span.Name())
		require.Equal(t, attribute.INT64, values["durabletask.task.task_id"].Type(), span.Name())
	}
	status := func(span sdktrace.ReadOnlySpan) string {
		return attrs(span)["durabletask.task.status"].AsString()
	}

	callerSpan := find("caller/enabled", trace.SpanKindInternal)
	create := find("create_orchestration:DTSEnabledParent", trace.SpanKindProducer)
	childOf(create, callerSpan)
	orchestration := find("orchestration:DTSEnabledParent", trace.SpanKindServer)
	childOf(orchestration, create)
	require.Equal(t, "Completed", status(orchestration))
	require.Equal(t, codes.Unset, orchestration.Status().Code)
	childClient := find("orchestration:DTSEnabledChild", trace.SpanKindClient)
	childOf(childClient, orchestration)
	requireTask(childClient, parentID)
	childServer := find("orchestration:DTSEnabledChild", trace.SpanKindServer)
	childOf(childServer, childClient)
	activityClient := find("activity:DTSEnabledEcho", trace.SpanKindClient)
	childOf(activityClient, childServer)
	activityServer := find("activity:DTSEnabledEcho", trace.SpanKindServer)
	childOf(activityServer, activityClient)
	require.Equal(t, "Completed", status(activityServer))
	require.Equal(t, codes.Unset, activityServer.Status().Code)

	failClient := find("activity:DTSEnabledFail", trace.SpanKindClient)
	childOf(failClient, orchestration)
	requireTask(failClient, parentID)
	require.Equal(t, codes.Error, failClient.Status().Code)
	failServer := find("activity:DTSEnabledFail", trace.SpanKindServer)
	childOf(failServer, failClient)
	requireTask(failServer, parentID)
	require.Equal(t, "Failed", status(failServer))
	require.Equal(t, codes.Error, failServer.Status().Code)
	require.Contains(t, failServer.Status().Description, "traced activity failure")

	childOf(find("orchestration:DTSEnabledParent:timer", trace.SpanKindInternal), orchestration)
	childOf(find("orchestration_event:proceed", trace.SpanKindProducer), callerSpan)
	childOf(find("orchestration_event:ping", trace.SpanKindProducer), orchestration)
	receiverCreate := find("create_orchestration:DTSEnabledReceiver", trace.SpanKindProducer)
	require.False(t, receiverCreate.Parent().IsValid())
	childOf(find("orchestration:DTSEnabledReceiver", trace.SpanKindServer), receiverCreate)

	// Each continue-as-new generation is its own logical orchestration span.
	loopCreate := find("create_orchestration:DTSEnabledLoop", trace.SpanKindProducer)
	childOf(loopCreate, callerSpan)
	generations := findAll("orchestration:DTSEnabledLoop", trace.SpanKindServer)
	require.Len(t, generations, 2, tracingtree.Describe(spans))
	for _, generation := range generations {
		childOf(generation, loopCreate)
	}
	require.NotEqual(t, generations[0].SpanContext().SpanID(), generations[1].SpanContext().SpanID())
	require.ElementsMatch(t, []string{"ContinuedAsNew", "Completed"},
		[]string{status(generations[0]), status(generations[1])})

	clientSignal := "entity:" + entityID.Name + ":clientSignal"
	require.Empty(t, findAll(clientSignal, trace.SpanKindProducer),
		"client SignalEntity propagates caller context without emitting a span")
	clientSignalExecution := find(clientSignal, trace.SpanKindConsumer)
	require.False(t, clientSignalExecution.Parent().IsValid(), "V2 entity executions must be independent roots")
	require.NotEqual(t, caller.SpanContext().TraceID(), clientSignalExecution.SpanContext().TraceID())
	require.Equal(t, "clientSignal", attrs(clientSignalExecution)["durabletask.entity.operation"].AsString())

	for _, operation := range []struct {
		name          string
		send, execute trace.SpanKind
		attribute     string
		status        codes.Code
	}{
		{"signal", trace.SpanKindProducer, trace.SpanKindConsumer, "signal_entity", codes.Unset},
		{"get", trace.SpanKindClient, trace.SpanKindServer, "call_entity", codes.Unset},
		{"fail", trace.SpanKindClient, trace.SpanKindServer, "call_entity", codes.Error},
	} {
		name := "entity:" + entityID.Name + ":" + operation.name
		send := find(name, operation.send)
		childOf(send, orchestration)
		requireTask(send, parentID)
		require.Equal(t, operation.status, send.Status().Code, name)
		execute := find(name, operation.execute)
		require.False(t, execute.Parent().IsValid(), "V2 entity executions must be independent roots")
		require.NotEqual(t, caller.SpanContext().TraceID(), execute.SpanContext().TraceID())
		require.Empty(t, execute.Links(), "V2 has no propagated parent or synthetic request-ID link")
		require.Equal(t, operation.status, execute.Status().Code, name)
		require.Equal(t, entityID.String(), attrs(execute)["durabletask.task.instance_id"].AsString())
		for _, span := range []sdktrace.ReadOnlySpan{send, execute} {
			values := attrs(span)
			require.Equal(t, operation.attribute, values["durabletask.task.operation"].AsString(), name)
			require.Equal(t, operation.name, values["durabletask.entity.operation"].AsString(), name)
		}
		if operation.status == codes.Error {
			require.Contains(t, execute.Status().Description, "traced entity failure")
		}
		if operation.name == "get" {
			childOf(find("app/entity-get", trace.SpanKindInternal), execute)
		}
	}
	// Every exported span was accounted for above; event receipt emits no
	// CONSUMER span and does not reparent the receiving orchestration.
	require.Len(t, spans, spanCount)
}
