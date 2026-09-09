package durabletaskscheduler_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/microsoft/durabletask-go/api"
	durabletaskclient "github.com/microsoft/durabletask-go/client"
	"github.com/microsoft/durabletask-go/durabletaskscheduler"
	"github.com/microsoft/durabletask-go/task"
	"github.com/microsoft/durabletask-go/tests/tracingtree"
	"github.com/stretchr/testify/require"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

// Tracing on the Durable Task Scheduler surface is owned by the scheduler service, not by
// this SDK: the Go process only runs the client and the worker, and neither creates
// orchestration, activity, or timer spans. These tests therefore assert the two halves of
// the contract that this SDK is actually responsible for:
//
//  1. The caller's W3C trace context reaches the scheduler and is durably recorded on the
//     history, so the scheduler-side span tree is rooted in the caller's trace.
//  2. The worker process itself never emits duplicate durabletask spans for work it runs.
//
// Emulator limitations that are asserted loosely on purpose are called out inline.

func startDTSCallerSpan(t *testing.T, parent context.Context, name string) (context.Context, trace.Span) {
	t.Helper()
	return tracingtree.StartCallerSpan(t, "tests/durabletaskscheduler", parent, name)
}

// expectedTraceParent renders the traceparent header the SDK must send for a span.
func expectedTraceParent(span trace.Span) string {
	spanContext := span.SpanContext()
	return fmt.Sprintf("00-%s-%s-%s", spanContext.TraceID(), spanContext.SpanID(), spanContext.TraceFlags())
}

func traceParentSpanID(t *testing.T, traceParent string) string {
	t.Helper()
	require.Len(t, traceParent, 55, "unexpected traceparent format %q", traceParent)
	return traceParent[36:52]
}

func fetchHistory(t *testing.T, ctx context.Context, client *durabletaskscheduler.Client, id api.InstanceID, executionID string) []*api.HistoryEvent {
	t.Helper()
	history, err := client.GetOrchestrationHistory(ctx, id, api.HistoryQuery{ExecutionID: executionID})
	require.NoError(t, err)
	return history.Events
}

// executionStarted returns the single ExecutionStarted record of a history.
func executionStarted(t *testing.T, events []*api.HistoryEvent) *api.HistoryExecutionStartedEvent {
	t.Helper()
	var started *api.HistoryExecutionStartedEvent
	for _, event := range events {
		if event.ExecutionStarted != nil {
			require.Nil(t, started, "history contains more than one ExecutionStarted event")
			started = event.ExecutionStarted
		}
	}
	require.NotNil(t, started, "history has no ExecutionStarted event")
	return started
}

func countEvents(events []*api.HistoryEvent, eventType api.HistoryEventType) int {
	count := 0
	for _, event := range events {
		if event.Type == eventType {
			count++
		}
	}
	return count
}

// requireTraceContextsShareTrace asserts that every trace context recorded on a history
// belongs to wantTraceID. Records without a trace context are skipped: the emulator does
// not stamp one on every event type, and that gap is asserted separately where it matters.
func requireTraceContextsShareTrace(t *testing.T, events []*api.HistoryEvent, wantTraceID string) {
	t.Helper()
	check := func(label string, tc *api.HistoryTraceContext) {
		if tc == nil || tc.TraceParent == "" {
			return
		}
		require.Containsf(t, tc.TraceParent, wantTraceID,
			"%s carries trace context %q, which is outside the caller's trace %s", label, tc.TraceParent, wantTraceID)
	}
	for _, event := range events {
		switch {
		case event.ExecutionStarted != nil:
			check("ExecutionStarted", event.ExecutionStarted.ParentTraceContext)
		case event.TaskScheduled != nil:
			check("TaskScheduled("+event.TaskScheduled.Name+")", event.TaskScheduled.ParentTraceContext)
		case event.SubOrchestrationInstanceCreated != nil:
			check("SubOrchestrationInstanceCreated("+event.SubOrchestrationInstanceCreated.Name+")",
				event.SubOrchestrationInstanceCreated.ParentTraceContext)
		}
	}
}

// requireNoWorkerEmittedSpans asserts the documented Durable Task Scheduler limitation: the
// Go worker and client do not emit durabletask spans, because the scheduler service owns
// orchestration, activity, and timer span emission. This also guards against a regression
// that would duplicate the service-side spans locally.
func requireNoWorkerEmittedSpans(t *testing.T, exporter *tracetest.InMemoryExporter) {
	t.Helper()
	spans := exporter.GetSpans().Snapshots()
	durableSpans := make([]sdktrace.ReadOnlySpan, 0, len(spans))
	for _, span := range spans {
		if span.InstrumentationScope().Name == "durabletask" {
			durableSpans = append(durableSpans, span)
		}
	}
	require.Emptyf(t, durableSpans,
		"the Durable Task Scheduler worker must not emit durabletask spans locally; got: %s",
		tracingtree.Describe(durableSpans))
}

// TestDTSEmulatorTracingTreeActivityFailureSubOrchestrationsAndEvents drives one
// orchestration through every durable construct that participates in the trace tree
// (activity failure, sub-orchestration success and failure, durable timer, client-raised
// event, and an orchestration-sent event) and verifies the caller's trace context is what
// the scheduler persists.
func TestDTSEmulatorTracingTreeActivityFailureSubOrchestrationsAndEvents(t *testing.T) {
	exporter := tracingtree.Init()

	registry := task.NewTaskRegistry()
	require.NoError(t, registry.AddActivityN("DTSTraceEcho", func(ctx task.ActivityContext) (any, error) {
		var input string
		if err := ctx.GetInput(&input); err != nil {
			return nil, err
		}
		return input, nil
	}))
	require.NoError(t, registry.AddActivityN("DTSTraceFailingActivity", func(task.ActivityContext) (any, error) {
		return nil, errors.New("dts activity exploded")
	}))
	require.NoError(t, registry.AddOrchestratorN("DTSTraceChildOK", func(ctx *task.OrchestrationContext) (any, error) {
		var output string
		if err := ctx.CallActivity("DTSTraceEcho", task.WithActivityInput("child")).Await(&output); err != nil {
			return nil, err
		}
		return output, nil
	}))
	require.NoError(t, registry.AddOrchestratorN("DTSTraceChildFailed", func(ctx *task.OrchestrationContext) (any, error) {
		return nil, ctx.CallActivity("DTSTraceFailingActivity").Await(nil)
	}))
	require.NoError(t, registry.AddOrchestratorN("DTSTraceReceiver", func(ctx *task.OrchestrationContext) (any, error) {
		var payload string
		if err := ctx.WaitForSingleEvent("ping", 60*time.Second).Await(&payload); err != nil {
			return nil, err
		}
		return payload, nil
	}))
	require.NoError(t, registry.AddOrchestratorN("DTSTraceParent", func(ctx *task.OrchestrationContext) (any, error) {
		var receiver api.InstanceID
		if err := ctx.GetInput(&receiver); err != nil {
			return nil, err
		}
		var completed string
		if err := ctx.CallSubOrchestrator(
			"DTSTraceChildOK",
			task.WithSubOrchestrationInstanceID(string(ctx.ID)+"_ok"),
		).Await(&completed); err != nil {
			return nil, err
		}
		if err := ctx.CreateTimer(10 * time.Millisecond).Await(nil); err != nil {
			return nil, err
		}
		var signal string
		if err := ctx.WaitForSingleEvent("proceed", 60*time.Second).Await(&signal); err != nil {
			return nil, err
		}
		if err := ctx.SendEvent(receiver, "ping", "pong"); err != nil {
			return nil, err
		}
		if err := ctx.CallSubOrchestrator(
			"DTSTraceChildFailed",
			task.WithSubOrchestrationInstanceID(string(ctx.ID)+"_failed"),
		).Await(nil); err == nil {
			return nil, errors.New("expected the child orchestration to fail")
		}
		return completed + "+" + signal, nil
	}))
	managementClient, _, _ := startEmulatorClientAndWorker(t, registry)

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	receiverID, err := managementClient.ScheduleNewOrchestration(
		ctx,
		"DTSTraceReceiver",
		api.WithInstanceID(uniqueInstanceID("go-trace-receiver")),
	)
	require.NoError(t, err)
	_, err = managementClient.WaitForOrchestrationStart(ctx, receiverID)
	require.NoError(t, err)

	callerCtx, callerSpan := startDTSCallerSpan(t, ctx, "caller/dts-trace-tree")
	parentID, err := managementClient.ScheduleNewOrchestration(
		callerCtx,
		"DTSTraceParent",
		api.WithInstanceID(uniqueInstanceID("go-trace-parent")),
		api.WithInput(receiverID),
	)
	require.NoError(t, err)
	callerSpan.End()
	callerTraceID := callerSpan.SpanContext().TraceID().String()

	_, err = managementClient.WaitForOrchestrationStart(ctx, parentID)
	require.NoError(t, err)
	require.NoError(t, managementClient.RaiseEvent(ctx, parentID, "proceed", api.WithEventPayload("go")))

	parentMetadata, err := managementClient.WaitForOrchestrationCompletion(ctx, parentID, api.WithFetchPayloads(true))
	require.NoError(t, err)
	require.Equal(t, api.RUNTIME_STATUS_COMPLETED, parentMetadata.RuntimeStatus)
	require.Equal(t, `"child+go"`, parentMetadata.SerializedOutput)
	receiverMetadata, err := managementClient.WaitForOrchestrationCompletion(ctx, receiverID, api.WithFetchPayloads(true))
	require.NoError(t, err)
	require.Equal(t, `"pong"`, receiverMetadata.SerializedOutput)
	failedChild, err := managementClient.WaitForOrchestrationCompletion(ctx, parentID+"_failed")
	require.NoError(t, err)
	require.Equal(t, api.RUNTIME_STATUS_FAILED, failedChild.RuntimeStatus)
	require.NotNil(t, failedChild.FailureDetails)
	require.Contains(t, failedChild.FailureDetails.ErrorMessage, "dts activity exploded")

	// The caller's trace context must be persisted verbatim on the orchestration that the
	// client scheduled. This is the hand-off point between the caller's trace and the
	// scheduler-side span tree.
	parentEvents := fetchHistory(t, ctx, managementClient, parentID, parentMetadata.ExecutionID)
	started := executionStarted(t, parentEvents)
	require.NotNil(t, started.ParentTraceContext, "the scheduler did not persist the caller's trace context")
	require.Equal(t, expectedTraceParent(callerSpan), started.ParentTraceContext.TraceParent)
	requireTraceContextsShareTrace(t, parentEvents, callerTraceID)
	actionSpanIDs := make(map[string]struct{})
	childTraceParents := make(map[api.InstanceID]string)

	// Durable timers and raised events are recorded on the parent's history, but the
	// emulator does not stamp a trace context on those records, so they can only be
	// correlated through the orchestration instance.
	require.Equal(t, 1, countEvents(parentEvents, api.HistoryEventTimerCreated))
	require.Equal(t, 1, countEvents(parentEvents, api.HistoryEventTimerFired))
	require.Equal(t, 1, countEvents(parentEvents, api.HistoryEventEventRaised))
	require.Equal(t, 2, countEvents(parentEvents, api.HistoryEventSubOrchestrationInstanceCreated))
	for _, event := range parentEvents {
		if created := event.SubOrchestrationInstanceCreated; created != nil {
			require.NotNil(t, created.ParentTraceContext)
			require.Contains(t, created.ParentTraceContext.TraceParent, callerTraceID)
			spanID := traceParentSpanID(t, created.ParentTraceContext.TraceParent)
			require.NotEqual(t, callerSpan.SpanContext().SpanID().String(), spanID)
			require.NotEqual(t, started.OrchestrationSpanID, spanID)
			require.NotContains(t, actionSpanIDs, spanID)
			actionSpanIDs[spanID] = struct{}{}
			childTraceParents[created.InstanceID] = created.ParentTraceContext.TraceParent
		}
	}

	// The orchestration-sent event lands on the receiver as a raised event. Cross-instance
	// causality is not carried on the wire by the emulator, so the receiving instance keeps
	// its own trace; only the presence of the event is asserted here.
	receiverEvents := fetchHistory(t, ctx, managementClient, receiverID, receiverMetadata.ExecutionID)
	require.Equal(t, 1, countEvents(receiverEvents, api.HistoryEventEventRaised))

	for _, childID := range []api.InstanceID{parentID + "_ok", parentID + "_failed"} {
		childEvents := fetchHistory(t, ctx, managementClient, childID, "")
		childStarted := executionStarted(t, childEvents)
		require.NotNil(t, childStarted.ParentTraceContext)
		require.Contains(t, childStarted.ParentTraceContext.TraceParent, callerTraceID)
		require.Equal(t, childTraceParents[childID], childStarted.ParentTraceContext.TraceParent)
		var foundActivityTrace bool
		for _, event := range childEvents {
			if scheduled := event.TaskScheduled; scheduled != nil {
				require.NotNil(t, scheduled.ParentTraceContext)
				require.Contains(t, scheduled.ParentTraceContext.TraceParent, callerTraceID)
				spanID := traceParentSpanID(t, scheduled.ParentTraceContext.TraceParent)
				require.NotEqual(t, callerSpan.SpanContext().SpanID().String(), spanID)
				require.NotEqual(t, childStarted.OrchestrationSpanID, spanID)
				require.NotContains(t, actionSpanIDs, spanID)
				actionSpanIDs[spanID] = struct{}{}
				foundActivityTrace = true
			}
		}
		require.True(t, foundActivityTrace)
		requireTraceContextsShareTrace(t, childEvents, callerTraceID)
	}

	requireNoWorkerEmittedSpans(t, exporter)
}

// TestDTSEmulatorTracingTreeVersionMigration verifies that a version-migrating
// continue-as-new keeps the caller's trace context, so every generation of an instance
// stays inside the trace that started it.
func TestDTSEmulatorTracingTreeVersionMigration(t *testing.T) {
	exporter := tracingtree.Init()

	options := emulatorOptions(t)
	options.Versioning = &task.VersioningOptions{
		Version:         "1.0",
		DefaultVersion:  "1.0",
		MatchStrategy:   task.VersionMatchNone,
		FailureStrategy: task.VersionFailureFail,
	}
	registry := task.NewTaskRegistry()
	require.NoError(t, registry.AddActivityNVersion("DTSTraceVersionedActivity", "1.0", func(task.ActivityContext) (any, error) {
		return "v1-activity", nil
	}))
	require.NoError(t, registry.AddOrchestratorNVersion("DTSTraceVersioned", "1.0", func(ctx *task.OrchestrationContext) (any, error) {
		var output string
		if err := ctx.CallActivity("DTSTraceVersionedActivity").Await(&output); err != nil {
			return nil, err
		}
		ctx.ContinueAsNew(output, task.WithContinueAsNewVersion("2.0"))
		return nil, nil
	}))
	require.NoError(t, registry.AddOrchestratorNVersion("DTSTraceVersioned", "2.0", func(ctx *task.OrchestrationContext) (any, error) {
		var input string
		if err := ctx.GetInput(&input); err != nil {
			return nil, err
		}
		return input + "+v2", nil
	}))

	logger := api.DefaultLogger()
	managementClient, err := durabletaskscheduler.NewClient(context.Background(), options, logger)
	require.NoError(t, err)
	worker, err := durabletaskscheduler.NewWorker(options, registry, logger)
	require.NoError(t, err)
	require.NoError(t, worker.Start(context.Background()))
	t.Cleanup(func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		require.NoError(t, worker.Shutdown(shutdownCtx))
		require.NoError(t, managementClient.Close())
	})

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	callerCtx, callerSpan := startDTSCallerSpan(t, ctx, "caller/dts-version-migration")
	instanceID, err := managementClient.ScheduleNewOrchestration(
		callerCtx,
		"DTSTraceVersioned",
		api.WithInstanceID(uniqueInstanceID("go-trace-version")),
		api.WithTags(map[string]string{"migration": "preserved"}),
	)
	require.NoError(t, err)
	callerSpan.End()

	metadata, err := managementClient.WaitForOrchestrationCompletion(ctx, instanceID, api.WithFetchPayloads(true))
	require.NoError(t, err)
	require.Equal(t, api.RUNTIME_STATUS_COMPLETED, metadata.RuntimeStatus)
	require.Equal(t, "2.0", metadata.Version)
	require.Equal(t, `"v1-activity+v2"`, metadata.SerializedOutput)
	require.Equal(t, "preserved", metadata.Tags["migration"])

	// The migrated generation is a new execution, and it must still carry the trace context
	// of the client call that started generation 1.
	events := fetchHistory(t, ctx, managementClient, instanceID, metadata.ExecutionID)
	started := executionStarted(t, events)
	require.Equal(t, "2.0", started.Version)
	require.NotNil(t, started.ParentTraceContext, "continue-as-new dropped the caller's trace context")
	require.Equal(t, expectedTraceParent(callerSpan), started.ParentTraceContext.TraceParent)
	requireTraceContextsShareTrace(t, events, callerSpan.SpanContext().TraceID().String())

	// Emulator limitation: only the latest generation's history is retained, so the 1.0
	// generation cannot be re-read after the migration completes.
	require.Equal(t, 0, countEvents(events, api.HistoryEventTaskScheduled))

	requireNoWorkerEmittedSpans(t, exporter)
}

// TestDTSEmulatorTracingTreeScheduledTask verifies the trace shape of orchestrations
// started by the scheduled-task (schedule entity) surface rather than by a client call.
func TestDTSEmulatorTracingTreeScheduledTask(t *testing.T) {
	exporter := tracingtree.Init()

	options := emulatorOptions(t)
	options.Versioning = &task.VersioningOptions{
		Version:         "1.0",
		DefaultVersion:  "1.0",
		MatchStrategy:   task.VersionMatchStrict,
		FailureStrategy: task.VersionFailureFail,
	}
	registry := task.NewTaskRegistry()
	targetName := "DTSTraceScheduled" + uuid.NewString()[:8]
	require.NoError(t, registry.AddOrchestratorNVersion(targetName, "1.0", func(ctx *task.OrchestrationContext) (any, error) {
		var input string
		if err := ctx.GetInput(&input); err != nil {
			return nil, err
		}
		var output string
		if err := ctx.CallActivity("DTSTraceScheduledActivity", task.WithActivityInput(input)).Await(&output); err != nil {
			return nil, err
		}
		return output, nil
	}))
	require.NoError(t, registry.AddActivityNVersion("DTSTraceScheduledActivity", "1.0", func(ctx task.ActivityContext) (any, error) {
		var input string
		if err := ctx.GetInput(&input); err != nil {
			return nil, err
		}
		return input + "-done", nil
	}))
	require.NoError(t, durabletaskscheduler.RegisterScheduledTasksWithDefaultVersion(registry, "1.0"))

	logger := api.DefaultLogger()
	managementClient, err := durabletaskscheduler.NewClient(context.Background(), options, logger)
	require.NoError(t, err)
	worker, err := durabletaskscheduler.NewWorker(
		options,
		registry,
		logger,
		durabletaskscheduler.WithScheduledTasks(),
		durabletaskclient.WithAutoWorkItemFilters(),
	)
	require.NoError(t, err)
	require.NoError(t, worker.Start(context.Background()))
	t.Cleanup(func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		require.NoError(t, worker.Shutdown(shutdownCtx))
		require.NoError(t, managementClient.Close())
	})

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	createdFrom := time.Now().UTC().Add(-time.Second)
	scheduleID := "go-trace-schedule-" + uuid.NewString()
	scheduleTag := "trace-" + uuid.NewString()

	callerCtx, callerSpan := startDTSCallerSpan(t, ctx, "caller/dts-scheduled-task")
	handle, err := managementClient.ScheduledTasks().Create(callerCtx, durabletaskscheduler.ScheduleCreationOptions{
		ScheduleID:              scheduleID,
		OrchestrationName:       targetName,
		TypedOrchestrationInput: "scheduled",
		Interval:                2 * time.Second,
		StartImmediatelyIfLate:  true,
		Tags:                    map[string]string{"source": scheduleTag},
	})
	require.NoError(t, err)
	callerSpan.End()
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		_ = handle.Delete(cleanupCtx)
	})

	var target *api.OrchestrationMetadata
	deadline := time.Now().Add(60 * time.Second)
	for target == nil && time.Now().Before(deadline) {
		result, queryErr := managementClient.QueryInstances(ctx, api.OrchestrationQuery{
			CreatedTimeFrom: createdFrom,
			PageSize:        100,
		})
		if queryErr == nil {
			for _, orchestration := range result.Orchestrations {
				if orchestration.Name == targetName &&
					orchestration.RuntimeStatus == api.RUNTIME_STATUS_COMPLETED &&
					orchestration.Tags["source"] == scheduleTag {
					target = orchestration
					break
				}
			}
		}
		if target == nil {
			time.Sleep(200 * time.Millisecond)
		}
	}
	require.NotNil(t, target, "the scheduled orchestration did not complete")

	events := fetchHistory(t, ctx, managementClient, target.InstanceID, target.ExecutionID)
	started := executionStarted(t, events)
	require.Equal(t, "1.0", started.Version)
	require.Equal(t, 1, countEvents(events, api.HistoryEventTaskScheduled))

	// Emulator limitation: schedule-triggered runs are started by the schedule entity, not
	// by a client call, so they do not inherit the trace context of whoever created the
	// schedule. Each run is its own trace root. Assert that shape explicitly, and assert
	// that whatever trace context the run does get is used consistently across its history.
	if started.ParentTraceContext != nil && started.ParentTraceContext.TraceParent != "" {
		require.NotEqual(t, expectedTraceParent(callerSpan), started.ParentTraceContext.TraceParent,
			"schedule-triggered runs are not expected to inherit the schedule creator's span")
		traceID := traceIDFromTraceParent(t, started.ParentTraceContext.TraceParent)
		requireTraceContextsShareTrace(t, events, traceID)
	}

	requireNoWorkerEmittedSpans(t, exporter)
}

// traceIDFromTraceParent extracts the trace ID from a W3C traceparent header.
func traceIDFromTraceParent(t *testing.T, traceParent string) string {
	t.Helper()
	require.Len(t, traceParent, 55, "unexpected traceparent format %q", traceParent)
	return traceParent[3:35]
}
