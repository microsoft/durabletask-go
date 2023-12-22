package tests

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/microsoft/durabletask-go/api"
	"github.com/microsoft/durabletask-go/backend"
	"github.com/microsoft/durabletask-go/backend/sqlite"
	"github.com/microsoft/durabletask-go/internal/protos"
	"github.com/microsoft/durabletask-go/task"
)

func Test_EmptyOrchestration(t *testing.T) {
	// Registration
	r := task.NewTaskRegistry()
	r.AddOrchestratorN("EmptyOrchestrator", func(ctx *task.OrchestrationContext) (any, error) {
		return nil, nil
	})

	// Initialization
	ctx := context.Background()
	exporter := initTracing()
	client, worker := initTaskHubWorker(ctx, r)
	defer worker.Shutdown(ctx)

	// Run the orchestration
	id, err := client.ScheduleNewOrchestration(ctx, "EmptyOrchestrator")
	require.NoError(t, err)
	metadata, err := client.WaitForOrchestrationCompletion(ctx, id)
	require.NoError(t, err)
	require.Equal(t, protos.OrchestrationStatus_ORCHESTRATION_STATUS_COMPLETED, metadata.RuntimeStatus)

	// Validate the exported OTel traces
	spans := exporter.GetSpans().Snapshots()
	assertSpanSequence(t, spans,
		assertOrchestratorCreated("EmptyOrchestrator", id),
		assertOrchestratorExecuted("EmptyOrchestrator", id, "COMPLETED"),
	)
}

func Test_SingleTimer(t *testing.T) {
	// Registration
	r := task.NewTaskRegistry()
	r.AddOrchestratorN("SingleTimer", func(ctx *task.OrchestrationContext) (any, error) {
		err := ctx.CreateTimer(time.Duration(0)).Await(nil)
		return nil, err
	})

	// Initialization
	ctx := context.Background()
	exporter := initTracing()
	client, worker := initTaskHubWorker(ctx, r)
	defer worker.Shutdown(ctx)

	// Run the orchestration
	id, err := client.ScheduleNewOrchestration(ctx, "SingleTimer")
	if assert.NoError(t, err) {
		metadata, err := client.WaitForOrchestrationCompletion(ctx, id)
		if assert.NoError(t, err) {
			assert.Equal(t, protos.OrchestrationStatus_ORCHESTRATION_STATUS_COMPLETED, metadata.RuntimeStatus)
			assert.GreaterOrEqual(t, metadata.LastUpdatedAt, metadata.CreatedAt)
		}
	}

	// Validate the exported OTel traces
	spans := exporter.GetSpans().Snapshots()
	assertSpanSequence(t, spans,
		assertOrchestratorCreated("SingleTimer", id),
		assertTimer(id),
		assertOrchestratorExecuted("SingleTimer", id, "COMPLETED"),
	)
}

func Test_ConcurrentTimers(t *testing.T) {
	// Registration
	r := task.NewTaskRegistry()
	r.AddOrchestratorN("TimerFanOut", func(ctx *task.OrchestrationContext) (any, error) {
		tasks := []task.Task{}
		for i := 0; i < 3; i++ {
			tasks = append(tasks, ctx.CreateTimer(1*time.Second))
		}
		for _, t := range tasks {
			if err := t.Await(nil); err != nil {
				return nil, err
			}
		}
		return nil, nil
	})

	// Initialization
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	exporter := initTracing()
	client, worker := initTaskHubWorker(ctx, r)
	defer worker.Shutdown(ctx)

	// Run the orchestration
	id, err := client.ScheduleNewOrchestration(ctx, "TimerFanOut")
	if assert.NoError(t, err) {
		metadata, err := client.WaitForOrchestrationCompletion(ctx, id)
		if assert.NoError(t, err) {
			assert.Equal(t, protos.OrchestrationStatus_ORCHESTRATION_STATUS_COMPLETED, metadata.RuntimeStatus)
			assert.GreaterOrEqual(t, metadata.LastUpdatedAt, metadata.CreatedAt)
		}
	}

	// Validate the exported OTel traces
	spans := exporter.GetSpans().Snapshots()
	assertSpanSequence(t, spans,
		assertOrchestratorCreated("TimerFanOut", id),
		assertTimer(id),
		assertTimer(id),
		assertTimer(id),
		assertOrchestratorExecuted("TimerFanOut", id, "COMPLETED"),
	)
}

func Test_IsReplaying(t *testing.T) {
	// Registration
	r := task.NewTaskRegistry()
	r.AddOrchestratorN("IsReplayingOrch", func(ctx *task.OrchestrationContext) (any, error) {
		values := []bool{ctx.IsReplaying}
		ctx.CreateTimer(time.Duration(0)).Await(nil)
		values = append(values, ctx.IsReplaying)
		ctx.CreateTimer(time.Duration(0)).Await(nil)
		values = append(values, ctx.IsReplaying)
		return values, nil
	})

	// Initialization
	ctx := context.Background()
	exporter := initTracing()
	client, worker := initTaskHubWorker(ctx, r)
	defer worker.Shutdown(ctx)

	// Run the orchestration
	id, err := client.ScheduleNewOrchestration(ctx, "IsReplayingOrch")
	if assert.NoError(t, err) {
		metadata, err := client.WaitForOrchestrationCompletion(ctx, id)
		if assert.NoError(t, err) {
			assert.Equal(t, protos.OrchestrationStatus_ORCHESTRATION_STATUS_COMPLETED, metadata.RuntimeStatus)
			assert.Equal(t, `[true,true,false]`, metadata.SerializedOutput)
		}
	}

	// Validate the exported OTel traces
	spans := exporter.GetSpans().Snapshots()
	assertSpanSequence(t, spans,
		assertOrchestratorCreated("IsReplayingOrch", id),
		assertTimer(id),
		assertTimer(id),
		assertOrchestratorExecuted("IsReplayingOrch", id, "COMPLETED"),
	)
}

func Test_SingleActivity(t *testing.T) {
	// Registration
	r := task.NewTaskRegistry()
	r.AddOrchestratorN("SingleActivity", func(ctx *task.OrchestrationContext) (any, error) {
		var input string
		if err := ctx.GetInput(&input); err != nil {
			return nil, err
		}
		var output string
		err := ctx.CallActivity("SayHello", task.WithActivityInput(input)).Await(&output)
		return output, err
	})
	r.AddActivityN("SayHello", func(ctx task.ActivityContext) (any, error) {
		var name string
		if err := ctx.GetInput(&name); err != nil {
			return nil, err
		}
		return fmt.Sprintf("Hello, %s!", name), nil
	})

	// Initialization
	ctx := context.Background()
	exporter := initTracing()
	client, worker := initTaskHubWorker(ctx, r)
	defer worker.Shutdown(ctx)

	// Run the orchestration
	id, err := client.ScheduleNewOrchestration(ctx, "SingleActivity", api.WithInput("世界"))
	if assert.NoError(t, err) {
		metadata, err := client.WaitForOrchestrationCompletion(ctx, id)
		if assert.NoError(t, err) {
			assert.Equal(t, protos.OrchestrationStatus_ORCHESTRATION_STATUS_COMPLETED, metadata.RuntimeStatus)
			assert.Equal(t, `"Hello, 世界!"`, metadata.SerializedOutput)
		}
	}

	// Validate the exported OTel traces
	spans := exporter.GetSpans().Snapshots()
	assertSpanSequence(t, spans,
		assertOrchestratorCreated("SingleActivity", id),
		assertActivity("SayHello", id, 0),
		assertOrchestratorExecuted("SingleActivity", id, "COMPLETED"),
	)
}

func Test_ActivityChain(t *testing.T) {
	// Registration
	r := task.NewTaskRegistry()
	r.AddOrchestratorN("ActivityChain", func(ctx *task.OrchestrationContext) (any, error) {
		val := 0
		for i := 0; i < 10; i++ {
			if err := ctx.CallActivity("PlusOne", task.WithActivityInput(val)).Await(&val); err != nil {
				return nil, err
			}
		}
		return val, nil
	})
	r.AddActivityN("PlusOne", func(ctx task.ActivityContext) (any, error) {
		var input int
		if err := ctx.GetInput(&input); err != nil {
			return nil, err
		}
		return input + 1, nil
	})

	// Initialization
	ctx := context.Background()
	exporter := initTracing()
	client, worker := initTaskHubWorker(ctx, r)
	defer worker.Shutdown(ctx)

	// Run the orchestration
	id, err := client.ScheduleNewOrchestration(ctx, "ActivityChain")
	if assert.NoError(t, err) {
		metadata, err := client.WaitForOrchestrationCompletion(ctx, id)
		if assert.NoError(t, err) {
			assert.Equal(t, protos.OrchestrationStatus_ORCHESTRATION_STATUS_COMPLETED, metadata.RuntimeStatus)
			assert.Equal(t, `10`, metadata.SerializedOutput)
		}
	}

	// Validate the exported OTel traces
	spans := exporter.GetSpans().Snapshots()
	assertSpanSequence(t, spans,
		assertOrchestratorCreated("ActivityChain", id),
		assertActivity("PlusOne", id, 0), assertActivity("PlusOne", id, 1), assertActivity("PlusOne", id, 2),
		assertActivity("PlusOne", id, 3), assertActivity("PlusOne", id, 4), assertActivity("PlusOne", id, 5),
		assertActivity("PlusOne", id, 6), assertActivity("PlusOne", id, 7), assertActivity("PlusOne", id, 8),
		assertActivity("PlusOne", id, 9), assertOrchestratorExecuted("ActivityChain", id, "COMPLETED"),
	)
}

func Test_ActivityFanOut(t *testing.T) {
	// Registration
	r := task.NewTaskRegistry()
	r.AddOrchestratorN("ActivityFanOut", func(ctx *task.OrchestrationContext) (any, error) {
		tasks := []task.Task{}
		for i := 0; i < 10; i++ {
			tasks = append(tasks, ctx.CallActivity("ToString", task.WithActivityInput(i)))
		}
		results := []string{}
		for _, t := range tasks {
			var result string
			if err := t.Await(&result); err != nil {
				return nil, err
			}
			results = append(results, result)
		}
		sort.Sort(sort.Reverse(sort.StringSlice(results)))
		return results, nil
	})
	r.AddActivityN("ToString", func(ctx task.ActivityContext) (any, error) {
		var input int
		if err := ctx.GetInput(&input); err != nil {
			return nil, err
		}
		time.Sleep(1 * time.Second)
		return fmt.Sprintf("%d", input), nil
	})

	// Initialization
	ctx := context.Background()
	exporter := initTracing()
	client, worker := initTaskHubWorker(ctx, r, backend.WithMaxParallelism(10))
	defer worker.Shutdown(ctx)

	// Run the orchestration
	id, err := client.ScheduleNewOrchestration(ctx, "ActivityFanOut")
	if assert.NoError(t, err) {
		metadata, err := client.WaitForOrchestrationCompletion(ctx, id)
		if assert.NoError(t, err) {
			assert.Equal(t, protos.OrchestrationStatus_ORCHESTRATION_STATUS_COMPLETED, metadata.RuntimeStatus)
			assert.Equal(t, `["9","8","7","6","5","4","3","2","1","0"]`, metadata.SerializedOutput)

			// Because all the activities run in parallel, they should complete very quickly
			assert.Less(t, metadata.LastUpdatedAt.Sub(metadata.CreatedAt), 3*time.Second)
		}
	}

	// Validate the exported OTel traces
	spans := exporter.GetSpans().Snapshots()
	assertSpanSequence(t, spans,
		assertOrchestratorCreated("ActivityFanOut", id),
		// TODO: Find a way to assert an unordered sequence of traces since the order of activity traces is non-deterministic.
	)
}

func Test_SingleSubOrchestrator_Completed(t *testing.T) {
	r := task.NewTaskRegistry()
	r.AddOrchestratorN("Parent", func(ctx *task.OrchestrationContext) (any, error) {
		var input any
		if err := ctx.GetInput(&input); err != nil {
			return nil, err
		}
		var output any
		err := ctx.CallSubOrchestrator(
			"Child",
			task.WithSubOrchestrationInstanceID(string(ctx.ID)+"_child"),
			task.WithSubOrchestratorInput(input)).Await(&output)
		return output, err
	})
	r.AddOrchestratorN("Child", func(ctx *task.OrchestrationContext) (any, error) {
		var input string
		if err := ctx.GetInput(&input); err != nil {
			return "", err
		}
		return input, nil
	})

	ctx := context.Background()
	exporter := initTracing()
	client, worker := initTaskHubWorker(ctx, r)
	defer worker.Shutdown(ctx)

	id, err := client.ScheduleNewOrchestration(ctx, "Parent", api.WithInput("Hello, world!"))
	require.NoError(t, err)
	metadata, err := client.WaitForOrchestrationCompletion(ctx, id)
	require.NoError(t, err)
	assert.Equal(t, protos.OrchestrationStatus_ORCHESTRATION_STATUS_COMPLETED, metadata.RuntimeStatus)
	assert.Equal(t, `"Hello, world!"`, metadata.SerializedOutput)

	spans := exporter.GetSpans().Snapshots()
	assertSpanSequence(t, spans,
		assertOrchestratorCreated("Parent", id),
		assertOrchestratorExecuted("Child", id+"_child", "COMPLETED"),
		assertOrchestratorExecuted("Parent", id, "COMPLETED"),
	)
}

func Test_SingleSubOrchestrator_Failed(t *testing.T) {
	r := task.NewTaskRegistry()
	r.AddOrchestratorN("Parent", func(ctx *task.OrchestrationContext) (any, error) {
		err := ctx.CallSubOrchestrator(
			"Child",
			task.WithSubOrchestrationInstanceID(string(ctx.ID)+"_child")).Await(nil)
		return nil, err
	})
	r.AddOrchestratorN("Child", func(ctx *task.OrchestrationContext) (any, error) {
		return nil, errors.New("Child failed")
	})

	ctx := context.Background()
	exporter := initTracing()
	client, worker := initTaskHubWorker(ctx, r)
	defer worker.Shutdown(ctx)

	id, err := client.ScheduleNewOrchestration(ctx, "Parent")
	require.NoError(t, err)
	metadata, err := client.WaitForOrchestrationCompletion(ctx, id)
	require.NoError(t, err)
	assert.Equal(t, protos.OrchestrationStatus_ORCHESTRATION_STATUS_FAILED, metadata.RuntimeStatus)
	if assert.NotNil(t, metadata.FailureDetails) {
		assert.Contains(t, metadata.FailureDetails.ErrorMessage, "Child failed")
	}

	spans := exporter.GetSpans().Snapshots()
	assertSpanSequence(t, spans,
		assertOrchestratorCreated("Parent", id),
		assertOrchestratorExecuted("Child", id+"_child", "FAILED"),
		assertOrchestratorExecuted("Parent", id, "FAILED"),
	)
}

func Test_ContinueAsNew(t *testing.T) {
	// Registration
	r := task.NewTaskRegistry()
	r.AddOrchestratorN("ContinueAsNewTest", func(ctx *task.OrchestrationContext) (any, error) {
		var input int32
		if err := ctx.GetInput(&input); err != nil {
			return nil, err
		}

		if input < 10 {
			if err := ctx.CreateTimer(0).Await(nil); err != nil {
				return nil, err
			}
			ctx.ContinueAsNew(input + 1)
		}
		return input, nil
	})

	// Initialization
	ctx := context.Background()
	exporter := initTracing()
	client, worker := initTaskHubWorker(ctx, r)
	defer worker.Shutdown(ctx)

	// Run the orchestration
	id, err := client.ScheduleNewOrchestration(ctx, "ContinueAsNewTest", api.WithInput(0))
	if assert.NoError(t, err) {
		metadata, err := client.WaitForOrchestrationCompletion(ctx, id)
		if assert.NoError(t, err) {
			assert.Equal(t, protos.OrchestrationStatus_ORCHESTRATION_STATUS_COMPLETED, metadata.RuntimeStatus)
			assert.Equal(t, `10`, metadata.SerializedOutput)
		}
	}

	// Validate the exported OTel traces
	spans := exporter.GetSpans().Snapshots()
	assertSpanSequence(t, spans,
		assertOrchestratorCreated("ContinueAsNewTest", id),
		assertTimer(id), assertOrchestratorExecuted("ContinueAsNewTest", id, "CONTINUED_AS_NEW"),
		assertTimer(id), assertOrchestratorExecuted("ContinueAsNewTest", id, "CONTINUED_AS_NEW"),
		assertTimer(id), assertOrchestratorExecuted("ContinueAsNewTest", id, "CONTINUED_AS_NEW"),
		assertTimer(id), assertOrchestratorExecuted("ContinueAsNewTest", id, "CONTINUED_AS_NEW"),
		assertTimer(id), assertOrchestratorExecuted("ContinueAsNewTest", id, "CONTINUED_AS_NEW"),
		assertTimer(id), assertOrchestratorExecuted("ContinueAsNewTest", id, "CONTINUED_AS_NEW"),
		assertTimer(id), assertOrchestratorExecuted("ContinueAsNewTest", id, "CONTINUED_AS_NEW"),
		assertTimer(id), assertOrchestratorExecuted("ContinueAsNewTest", id, "CONTINUED_AS_NEW"),
		assertTimer(id), assertOrchestratorExecuted("ContinueAsNewTest", id, "CONTINUED_AS_NEW"),
		assertTimer(id), assertOrchestratorExecuted("ContinueAsNewTest", id, "CONTINUED_AS_NEW"),
		assertOrchestratorExecuted("ContinueAsNewTest", id, "COMPLETED"),
	)
}

func Test_ContinueAsNew_Events(t *testing.T) {
	// Registration
	r := task.NewTaskRegistry()
	r.AddOrchestratorN("ContinueAsNewTest", func(ctx *task.OrchestrationContext) (any, error) {
		var input int32
		if err := ctx.GetInput(&input); err != nil {
			return nil, err
		}
		var complete bool
		if err := ctx.WaitForSingleEvent("MyEvent", -1).Await(&complete); err != nil {
			return nil, err
		}
		if complete {
			return input, nil
		}
		ctx.ContinueAsNew(input+1, task.WithKeepUnprocessedEvents())
		return nil, nil
	})

	// Initialization
	ctx := context.Background()
	client, worker := initTaskHubWorker(ctx, r)
	defer worker.Shutdown(ctx)

	// Run the orchestration
	id, err := client.ScheduleNewOrchestration(ctx, "ContinueAsNewTest", api.WithInput(0))
	require.NoError(t, err)
	for i := 0; i < 10; i++ {
		require.NoError(t, client.RaiseEvent(ctx, id, "MyEvent", api.WithEventPayload(false)))
	}
	require.NoError(t, client.RaiseEvent(ctx, id, "MyEvent", api.WithEventPayload(true)))
	metadata, err := client.WaitForOrchestrationCompletion(ctx, id)
	require.NoError(t, err)
	assert.Equal(t, protos.OrchestrationStatus_ORCHESTRATION_STATUS_COMPLETED, metadata.RuntimeStatus)
	assert.Equal(t, `10`, metadata.SerializedOutput)
}

func Test_ExternalEventOrchestration(t *testing.T) {
	const eventCount = 10

	// Registration
	r := task.NewTaskRegistry()
	r.AddOrchestratorN("ExternalEventOrchestration", func(ctx *task.OrchestrationContext) (any, error) {
		for i := 0; i < eventCount; i++ {
			var value int
			ctx.WaitForSingleEvent("MyEvent", 5*time.Second).Await(&value)
			if value != i {
				return false, errors.New("Unexpected value")
			}
		}
		return true, nil
	})

	// Initialization
	ctx := context.Background()
	exporter := initTracing()
	client, worker := initTaskHubWorker(ctx, r)
	defer worker.Shutdown(ctx)

	// Run the orchestration
	id, err := client.ScheduleNewOrchestration(ctx, "ExternalEventOrchestration", api.WithInput(0))
	if assert.NoError(t, err) {
		for i := 0; i < eventCount; i++ {
			opts := api.WithEventPayload(i)
			require.NoError(t, client.RaiseEvent(ctx, id, "MyEvent", opts))
		}

		timeoutCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()

		metadata, err := client.WaitForOrchestrationCompletion(timeoutCtx, id)
		require.NoError(t, err)
		require.True(t, metadata.IsComplete())
		require.Equal(t, protos.OrchestrationStatus_ORCHESTRATION_STATUS_COMPLETED, metadata.RuntimeStatus)
	}

	// Validate the exported OTel traces
	eventSizeInBytes := 1
	spans := exporter.GetSpans().Snapshots()
	assertSpanSequence(t, spans,
		assertOrchestratorCreated("ExternalEventOrchestration", id),
		assertOrchestratorExecuted("ExternalEventOrchestration", id, "COMPLETED", assertSpanEvents(
			assertExternalEvent("MyEvent", eventSizeInBytes),
			assertExternalEvent("MyEvent", eventSizeInBytes),
			assertExternalEvent("MyEvent", eventSizeInBytes),
			assertExternalEvent("MyEvent", eventSizeInBytes),
			assertExternalEvent("MyEvent", eventSizeInBytes),
			assertExternalEvent("MyEvent", eventSizeInBytes),
			assertExternalEvent("MyEvent", eventSizeInBytes),
			assertExternalEvent("MyEvent", eventSizeInBytes),
			assertExternalEvent("MyEvent", eventSizeInBytes),
			assertExternalEvent("MyEvent", eventSizeInBytes),
		)),
	)
}

func Test_ExternalEventTimeout(t *testing.T) {
	// Registration
	r := task.NewTaskRegistry()
	r.AddOrchestratorN("ExternalEventOrchestrationWithTimeout", func(ctx *task.OrchestrationContext) (any, error) {
		if err := ctx.WaitForSingleEvent("MyEvent", 2*time.Second).Await(nil); err != nil {
			return nil, err
		}
		return nil, nil
	})

	// Initialization
	ctx := context.Background()
	exporter := initTracing()
	client, worker := initTaskHubWorker(ctx, r)
	defer worker.Shutdown(ctx)

	// Run two variations, one where we raise the external event and one where we don't (timeout)
	for _, raiseEvent := range []bool{true, false} {
		t.Run(fmt.Sprintf("RaiseEvent:%v", raiseEvent), func(t *testing.T) {
			// Run the orchestration
			id, err := client.ScheduleNewOrchestration(ctx, "ExternalEventOrchestrationWithTimeout")
			require.NoError(t, err)
			if raiseEvent {
				require.NoError(t, client.RaiseEvent(ctx, id, "MyEvent"))
			}

			timeoutCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			defer cancel()

			metadata, err := client.WaitForOrchestrationCompletion(timeoutCtx, id)
			require.NoError(t, err)

			// Validate the exported OTel traces
			spans := exporter.GetSpans().Snapshots()
			if raiseEvent {
				assert.True(t, metadata.IsComplete())
				assert.Equal(t, protos.OrchestrationStatus_ORCHESTRATION_STATUS_COMPLETED, metadata.RuntimeStatus)

				assertSpanSequence(t, spans,
					assertOrchestratorCreated("ExternalEventOrchestrationWithTimeout", id),
					assertOrchestratorExecuted("ExternalEventOrchestrationWithTimeout", id, "COMPLETED", assertSpanEvents(
						assertExternalEvent("MyEvent", 0),
					)),
				)
			} else {
				require.True(t, metadata.IsComplete())
				require.Equal(t, protos.OrchestrationStatus_ORCHESTRATION_STATUS_FAILED, metadata.RuntimeStatus)
				if assert.NotNil(t, metadata.FailureDetails) {
					// The exact message is not important - just make sure it's something clear
					// NOTE: In a future version, we might have a specifc ErrorType contract. For now, the
					//       caller shouldn't make any assumptions about this.
					assert.Equal(t, "the task was canceled", metadata.FailureDetails.ErrorMessage)
				}

				assertSpanSequence(t, spans,
					assertOrchestratorCreated("ExternalEventOrchestrationWithTimeout", id),
					// A timer is used to implement the event timeout
					assertTimer(id),
					assertOrchestratorExecuted("ExternalEventOrchestrationWithTimeout", id, "FAILED", assertSpanEvents()),
				)
			}
		})
		exporter.Reset()
	}
}

func Test_SuspendResumeOrchestration(t *testing.T) {
	const eventCount = 10

	// Registration
	r := task.NewTaskRegistry()
	r.AddOrchestratorN("SuspendResumeOrchestration", func(ctx *task.OrchestrationContext) (any, error) {
		for i := 0; i < eventCount; i++ {
			var value int
			ctx.WaitForSingleEvent("MyEvent", 5*time.Second).Await(&value)
			if value != i {
				return false, errors.New("Unexpected value")
			}
		}
		return true, nil
	})

	// Initialization
	ctx := context.Background()
	exporter := initTracing()
	client, worker := initTaskHubWorker(ctx, r)
	defer worker.Shutdown(ctx)

	// Run the orchestration, which will block waiting for external events
	id, err := client.ScheduleNewOrchestration(ctx, "SuspendResumeOrchestration", api.WithInput(0))
	require.NoError(t, err)

	// Wait for the orchestration to finish starting
	_, err = client.WaitForOrchestrationStart(ctx, id)
	require.NoError(t, err)

	// Suspend the orchestration
	require.NoError(t, client.SuspendOrchestration(ctx, id, ""))

	// Raise a bunch of events to the orchestration (they should get buffered but not consumed)
	for i := 0; i < eventCount; i++ {
		opts := api.WithEventPayload(i)
		require.NoError(t, client.RaiseEvent(ctx, id, "MyEvent", opts))
	}

	// Make sure the orchestration *doesn't* complete
	timeoutCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	_, err = client.WaitForOrchestrationCompletion(timeoutCtx, id)
	require.ErrorIs(t, err, timeoutCtx.Err())

	var metadata *api.OrchestrationMetadata
	metadata, err = client.FetchOrchestrationMetadata(ctx, id)
	if assert.NoError(t, err) {
		assert.True(t, metadata.IsRunning())
		assert.Equal(t, protos.OrchestrationStatus_ORCHESTRATION_STATUS_SUSPENDED, metadata.RuntimeStatus)
	}

	// Resume the orchestration and wait for it to complete
	require.NoError(t, client.ResumeOrchestration(ctx, id, ""))
	timeoutCtx, cancel = context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	_, err = client.WaitForOrchestrationCompletion(timeoutCtx, id)
	require.NoError(t, err)

	// Validate the exported OTel traces
	eventSizeInBytes := 1
	spans := exporter.GetSpans().Snapshots()
	assertSpanSequence(t, spans,
		assertOrchestratorCreated("SuspendResumeOrchestration", id),
		assertOrchestratorExecuted("SuspendResumeOrchestration", id, "COMPLETED", assertSpanEvents(
			assertSuspendedEvent(),
			assertExternalEvent("MyEvent", eventSizeInBytes),
			assertExternalEvent("MyEvent", eventSizeInBytes),
			assertExternalEvent("MyEvent", eventSizeInBytes),
			assertExternalEvent("MyEvent", eventSizeInBytes),
			assertExternalEvent("MyEvent", eventSizeInBytes),
			assertExternalEvent("MyEvent", eventSizeInBytes),
			assertExternalEvent("MyEvent", eventSizeInBytes),
			assertExternalEvent("MyEvent", eventSizeInBytes),
			assertExternalEvent("MyEvent", eventSizeInBytes),
			assertExternalEvent("MyEvent", eventSizeInBytes),
			assertResumedEvent(),
		)),
	)
}

func Test_TerminateOrchestration(t *testing.T) {
	// Registration
	r := task.NewTaskRegistry()
	r.AddOrchestratorN("MyOrchestrator", func(ctx *task.OrchestrationContext) (any, error) {
		ctx.CreateTimer(3 * time.Second).Await(nil)
		return nil, nil
	})

	// Initialization
	ctx := context.Background()
	exporter := initTracing()
	client, worker := initTaskHubWorker(ctx, r)
	defer worker.Shutdown(ctx)

	// Run the orchestration, which will block waiting for external events
	id, err := client.ScheduleNewOrchestration(ctx, "MyOrchestrator")
	require.NoError(t, err)

	// Terminate the orchestration before the timer expires
	require.NoError(t, client.TerminateOrchestration(ctx, id, api.WithOutput("You got terminated!")))

	// Wait for the orchestration to complete
	metadata, err := client.WaitForOrchestrationCompletion(ctx, id)
	require.NoError(t, err)
	require.True(t, metadata.IsComplete())
	require.Equal(t, protos.OrchestrationStatus_ORCHESTRATION_STATUS_TERMINATED, metadata.RuntimeStatus)
	require.Equal(t, `"You got terminated!"`, metadata.SerializedOutput)

	// Validate the exported OTel traces
	spans := exporter.GetSpans().Snapshots()
	assertSpanSequence(t, spans,
		assertOrchestratorCreated("MyOrchestrator", id),
		assertOrchestratorExecuted("MyOrchestrator", id, "TERMINATED"),
	)
}

func Test_TerminateOrchestration_Recursive(t *testing.T) {
	delayTime := 4 * time.Second
	executedActivity := false
	r := task.NewTaskRegistry()
	r.AddOrchestratorN("Root", func(ctx *task.OrchestrationContext) (any, error) {
		tasks := []task.Task{}
		for i := 0; i < 5; i++ {
			task := ctx.CallSubOrchestrator("L1", task.WithSubOrchestrationInstanceID(string(ctx.ID)+"_L1_"+strconv.Itoa(i)))
			tasks = append(tasks, task)
		}
		for _, task := range tasks {
			task.Await(nil)
		}
		return nil, nil
	})
	r.AddOrchestratorN("L1", func(ctx *task.OrchestrationContext) (any, error) {
		ctx.CallSubOrchestrator("L2", task.WithSubOrchestrationInstanceID(string(ctx.ID)+"_L2")).Await(nil)
		return nil, nil
	})
	r.AddOrchestratorN("L2", func(ctx *task.OrchestrationContext) (any, error) {
		ctx.CreateTimer(delayTime).Await(nil)
		ctx.CallActivity("Fail").Await(nil)
		return nil, nil
	})
	r.AddActivityN("Fail", func(ctx task.ActivityContext) (any, error) {
		executedActivity = true
		return nil, errors.New("Failed: Should not have executed the activity")
	})

	// Initialization
	ctx := context.Background()
	client, worker := initTaskHubWorker(ctx, r)
	defer worker.Shutdown(ctx)

	// Test terminating with and without recursion
	for _, recurse := range []bool{true, false} {
		t.Run(fmt.Sprintf("Recurse = %v", recurse), func(t *testing.T) {
			// Run the orchestration, which will block waiting for external events
			id, err := client.ScheduleNewOrchestration(ctx, "Root")
			require.NoError(t, err)

			// Wait long enough to ensure all orchestrations have started (but not longer than the timer delay)
			assert.Eventually(t, func() bool {
				// List of all orchestrations created
				orchestrationIDs := []string{string(id)}
				for i := 0; i < 5; i++ {
					orchestrationIDs = append(orchestrationIDs, string(id)+"_L1_"+strconv.Itoa(i), string(id)+"_L1_"+strconv.Itoa(i)+"_L2")
				}
				for _, orchID := range orchestrationIDs {
					metadata, err := client.FetchOrchestrationMetadata(ctx, api.InstanceID(orchID))
					require.NoError(t, err)
					// All orchestrations should be running
					if metadata.RuntimeStatus != protos.OrchestrationStatus_ORCHESTRATION_STATUS_RUNNING {
						return false
					}
				}
				return true
			}, 2*time.Second, 100*time.Millisecond)

			// Terminate the root orchestration and mark whether a recursive termination
			output := fmt.Sprintf("Recursive termination = %v", recurse)
			opts := []api.TerminateOptions{api.WithOutput(output), api.WithRecursiveTerminate(recurse)}
			require.NoError(t, client.TerminateOrchestration(ctx, id, opts...))

			// Wait for the root orchestration to complete and verify its terminated status
			metadata, err := client.WaitForOrchestrationCompletion(ctx, id)
			require.NoError(t, err)
			require.Equal(t, protos.OrchestrationStatus_ORCHESTRATION_STATUS_TERMINATED, metadata.RuntimeStatus)
			require.Equal(t, fmt.Sprintf("\"%s\"", output), metadata.SerializedOutput)

			// Wait for all L2 suborchestrations to complete
			orchIDs := []string{}
			for i := 0; i < 5; i++ {
				orchIDs = append(orchIDs, string(id)+"_L1_"+strconv.Itoa(i)+"_L2")
			}
			for _, orchID := range orchIDs {
				_, err := client.WaitForOrchestrationCompletion(ctx, api.InstanceID(orchID))
				require.NoError(t, err)
			}
			// Verify tht none of the L2 suborchestrations executed the activity in case of recursive termination
			assert.NotEqual(t, recurse, executedActivity)
		})
	}
}

func Test_TerminateOrchestration_Recursive_TerminateCompletedSubOrchestration(t *testing.T) {
	delayTime := 4 * time.Second
	r := task.NewTaskRegistry()
	r.AddOrchestratorN("Root", func(ctx *task.OrchestrationContext) (any, error) {
		// Create L1 sub-orchestration and wait for it to complete
		ctx.CallSubOrchestrator("L1", task.WithSubOrchestrationInstanceID(string(ctx.ID)+"_L1")).Await(nil)
		ctx.CreateTimer(delayTime).Await(nil)
		return nil, nil
	})
	r.AddOrchestratorN("L1", func(ctx *task.OrchestrationContext) (any, error) {
		// Create L2 sub-orchestration but don't wait for it to complete
		ctx.CallSubOrchestrator("L2", task.WithSubOrchestrationInstanceID(string(ctx.ID)+"_L2"))
		return nil, nil
	})
	r.AddOrchestratorN("L2", func(ctx *task.OrchestrationContext) (any, error) {
		// Wait for `delayTime`
		ctx.CreateTimer(delayTime).Await(nil)
		return nil, nil
	})

	// Initialization
	ctx := context.Background()
	client, worker := initTaskHubWorker(ctx, r)
	defer worker.Shutdown(ctx)

	// Test terminating with and without recursion
	for _, recurse := range []bool{true, false} {
		t.Run(fmt.Sprintf("Recurse = %v", recurse), func(t *testing.T) {
			// Run the orchestration, which will block waiting for external events
			id, err := client.ScheduleNewOrchestration(ctx, "Root")
			require.NoError(t, err)

			// Wait long enough to ensure that all L1 orchestrations have completed but Root and L2 are still running
			assert.Eventually(t, func() bool {
				// List of all orchestrations created
				orchestrationIDs := []string{string(id), string(id) + "_L1", string(id) + "_L1_L2"}
				for _, orchID := range orchestrationIDs {
					// Fetch orchestration metadata
					metadata, err := client.FetchOrchestrationMetadata(ctx, api.InstanceID(orchID))
					require.NoError(t, err)
					if orchID == string(id)+"_L1" {
						// L1 orchestration should have completed
						if metadata.RuntimeStatus != protos.OrchestrationStatus_ORCHESTRATION_STATUS_COMPLETED {
							return false
						}
					} else {
						// Root and L2 orchestrations should still be running
						if metadata.RuntimeStatus != protos.OrchestrationStatus_ORCHESTRATION_STATUS_RUNNING {
							return false
						}
					}
				}
				return true
			}, 2*time.Second, 100*time.Millisecond)

			// Terminate the root orchestration and mark whether a recursive termination
			output := fmt.Sprintf("Recursive termination = %v", recurse)
			opts := []api.TerminateOptions{api.WithOutput(output), api.WithRecursiveTerminate(recurse)}
			require.NoError(t, client.TerminateOrchestration(ctx, id, opts...))

			// Wait for the root orchestration to complete and verify its terminated status
			metadata, err := client.WaitForOrchestrationCompletion(ctx, id)
			require.NoError(t, err)
			require.Equal(t, protos.OrchestrationStatus_ORCHESTRATION_STATUS_TERMINATED, metadata.RuntimeStatus)
			require.Equal(t, fmt.Sprintf("\"%s\"", output), metadata.SerializedOutput)

			// Verify that the L1 and L2 orchestrations have completed with the appropriate status
			L1_OrchestrationStatus := protos.OrchestrationStatus_ORCHESTRATION_STATUS_COMPLETED
			L2_OrchestrationStatus := protos.OrchestrationStatus_ORCHESTRATION_STATUS_COMPLETED
			L2_Output := ""
			L1_Output := ""
			if recurse {
				L2_OrchestrationStatus = protos.OrchestrationStatus_ORCHESTRATION_STATUS_TERMINATED
				L2_Output = fmt.Sprintf("\"%s\"", output)
			}
			// In recursive case, L1 orchestration is not terminated because it was already completed when the root orchestration was terminated
			metadata, err = client.WaitForOrchestrationCompletion(ctx, id+"_L1")
			require.NoError(t, err)
			require.Equal(t, L1_OrchestrationStatus, metadata.RuntimeStatus)
			require.Equal(t, L1_Output, metadata.SerializedOutput)

			// In recursive case, L2 is terminated because it was still running when the root orchestration was terminated
			metadata, err = client.WaitForOrchestrationCompletion(ctx, id+"_L1_L2")
			require.NoError(t, err)
			require.Equal(t, L2_OrchestrationStatus, metadata.RuntimeStatus)
			require.Equal(t, L2_Output, metadata.SerializedOutput)
		})
	}
}

func Test_PurgeCompletedOrchestration(t *testing.T) {
	// Registration
	r := task.NewTaskRegistry()
	r.AddOrchestratorN("ExternalEventOrchestration", func(ctx *task.OrchestrationContext) (any, error) {
		if err := ctx.WaitForSingleEvent("MyEvent", 30*time.Second).Await(nil); err != nil {
			return nil, err
		}
		return nil, nil
	})

	// Initialization
	ctx := context.Background()
	client, worker := initTaskHubWorker(ctx, r)
	defer worker.Shutdown(ctx)

	// Run the orchestration
	id, err := client.ScheduleNewOrchestration(ctx, "ExternalEventOrchestration")
	if !assert.NoError(t, err) {
		return
	}
	if _, err = client.WaitForOrchestrationStart(ctx, id); !assert.NoError(t, err) {
		return
	}

	// Try to purge the orchestration state before it completes and verify that it fails with ErrNotCompleted
	if err = client.PurgeOrchestrationState(ctx, id); !assert.ErrorIs(t, err, api.ErrNotCompleted) {
		return
	}

	// Raise an event to the orchestration so that it can complete
	if err = client.RaiseEvent(ctx, id, "MyEvent"); !assert.NoError(t, err) {
		return
	}
	if _, err = client.WaitForOrchestrationCompletion(ctx, id); !assert.NoError(t, err) {
		return
	}

	// Try to purge the orchestration state again and verify that it succeeds
	if err = client.PurgeOrchestrationState(ctx, id); !assert.NoError(t, err) {
		return
	}

	// Try to fetch the orchestration metadata and verify that it fails with ErrInstanceNotFound
	if _, err = client.FetchOrchestrationMetadata(ctx, id); !assert.ErrorIs(t, err, api.ErrInstanceNotFound) {
		return
	}

	// Try to purge again and verify that it also fails with ErrInstanceNotFound
	if err = client.PurgeOrchestrationState(ctx, id); !assert.ErrorIs(t, err, api.ErrInstanceNotFound) {
		return
	}
}

func Test_PurgeOrchestration_Recursive(t *testing.T) {
	delayTime := 4 * time.Second
	r := task.NewTaskRegistry()
	r.AddOrchestratorN("Root", func(ctx *task.OrchestrationContext) (any, error) {
		ctx.CallSubOrchestrator("L1", task.WithSubOrchestrationInstanceID(string(ctx.ID)+"_L1")).Await(nil)
		return nil, nil
	})
	r.AddOrchestratorN("L1", func(ctx *task.OrchestrationContext) (any, error) {
		ctx.CallSubOrchestrator("L2", task.WithSubOrchestrationInstanceID(string(ctx.ID)+"_L2")).Await(nil)
		return nil, nil
	})
	r.AddOrchestratorN("L2", func(ctx *task.OrchestrationContext) (any, error) {
		ctx.CreateTimer(delayTime).Await(nil)
		return nil, nil
	})

	// Initialization
	ctx := context.Background()
	client, worker := initTaskHubWorker(ctx, r)
	defer worker.Shutdown(ctx)

	// Test terminating with and without recursion
	for _, recurse := range []bool{true, false} {
		t.Run(fmt.Sprintf("Recurse = %v", recurse), func(t *testing.T) {
			// Run the orchestration, which will block waiting for external events
			id, err := client.ScheduleNewOrchestration(ctx, "Root")
			require.NoError(t, err)

			metadata, err := client.WaitForOrchestrationCompletion(ctx, id)
			require.NoError(t, err)
			require.Equal(t, protos.OrchestrationStatus_ORCHESTRATION_STATUS_COMPLETED, metadata.RuntimeStatus)

			// Purge the root orchestration
			opts := []api.PurgeOptions{api.WithRecursivePurge(recurse)}
			err = client.PurgeOrchestrationState(ctx, id, opts...)
			assert.NoError(t, err)

			// Verify that root Orchestration has been purged
			_, err = client.FetchOrchestrationMetadata(ctx, id)
			assert.ErrorIs(t, err, api.ErrInstanceNotFound)

			if recurse {
				// Verify that L1 and L2 orchestrations have been purged
				_, err = client.FetchOrchestrationMetadata(ctx, id+"_L1")
				assert.ErrorIs(t, err, api.ErrInstanceNotFound)

				_, err = client.FetchOrchestrationMetadata(ctx, id+"_L1_L2")
				assert.ErrorIs(t, err, api.ErrInstanceNotFound)
			} else {
				// Verify that L1 and L2 orchestrations are not purged
				metadata, err = client.FetchOrchestrationMetadata(ctx, id+"_L1")
				assert.NoError(t, err)
				assert.Equal(t, protos.OrchestrationStatus_ORCHESTRATION_STATUS_COMPLETED, metadata.RuntimeStatus)

				_, err = client.FetchOrchestrationMetadata(ctx, id+"_L1_L2")
				assert.NoError(t, err)
				assert.Equal(t, protos.OrchestrationStatus_ORCHESTRATION_STATUS_COMPLETED, metadata.RuntimeStatus)
			}
		})
	}
}

func Test_RecreateCompletedOrchestration(t *testing.T) {
	t.Skip("Not yet supported. Needs https://github.com/microsoft/durabletask-go/issues/42")

	// Registration
	r := task.NewTaskRegistry()
	r.AddOrchestratorN("HelloOrchestration", func(ctx *task.OrchestrationContext) (any, error) {
		var input string
		if err := ctx.GetInput(&input); err != nil {
			return nil, err
		}
		var output string
		err := ctx.CallActivity("SayHello", task.WithActivityInput(input)).Await(&output)
		return output, err
	})
	r.AddActivityN("SayHello", func(ctx task.ActivityContext) (any, error) {
		var name string
		if err := ctx.GetInput(&name); err != nil {
			return nil, err
		}
		return fmt.Sprintf("Hello, %s!", name), nil
	})

	// Initialization
	ctx := context.Background()
	exporter := initTracing()
	client, worker := initTaskHubWorker(ctx, r)
	defer worker.Shutdown(ctx)

	// Run the first orchestration
	id, err := client.ScheduleNewOrchestration(ctx, "HelloOrchestration", api.WithInput("世界"))
	require.NoError(t, err)
	metadata, err := client.WaitForOrchestrationCompletion(ctx, id)
	require.NoError(t, err)
	assert.Equal(t, protos.OrchestrationStatus_ORCHESTRATION_STATUS_COMPLETED, metadata.RuntimeStatus)
	assert.Equal(t, `"Hello, 世界!"`, metadata.SerializedOutput)

	// Run the second orchestration with the same ID as the first
	var newID api.InstanceID
	newID, err = client.ScheduleNewOrchestration(ctx, "HelloOrchestration", api.WithInstanceID(id), api.WithInput("World"))
	require.NoError(t, err)
	require.Equal(t, id, newID)
	metadata, err = client.WaitForOrchestrationCompletion(ctx, id)
	require.NoError(t, err)
	assert.Equal(t, protos.OrchestrationStatus_ORCHESTRATION_STATUS_COMPLETED, metadata.RuntimeStatus)
	assert.Equal(t, `"Hello, World!"`, metadata.SerializedOutput)

	// Validate the exported OTel traces
	spans := exporter.GetSpans().Snapshots()
	assertSpanSequence(t, spans,
		assertOrchestratorCreated("SingleActivity", id),
		assertActivity("SayHello", id, 0),
		assertOrchestratorExecuted("SingleActivity", id, "COMPLETED"),
		assertOrchestratorCreated("SingleActivity", id),
		assertActivity("SayHello", id, 0),
		assertOrchestratorExecuted("SingleActivity", id, "COMPLETED"),
	)
}

func initTaskHubWorker(ctx context.Context, r *task.TaskRegistry, opts ...backend.NewTaskWorkerOptions) (backend.TaskHubClient, backend.TaskHubWorker) {
	// TODO: Switch to options pattern
	logger := backend.DefaultLogger()
	be := sqlite.NewSqliteBackend(sqlite.NewSqliteOptions(""), logger)
	executor := task.NewTaskExecutor(r)
	orchestrationWorker := backend.NewOrchestrationWorker(be, executor, logger, opts...)
	activityWorker := backend.NewActivityTaskWorker(be, executor, logger, opts...)
	taskHubWorker := backend.NewTaskHubWorker(be, orchestrationWorker, activityWorker, logger)
	if err := taskHubWorker.Start(ctx); err != nil {
		panic(err)
	}
	taskHubClient := backend.NewTaskHubClient(be)
	return taskHubClient, taskHubWorker
}
