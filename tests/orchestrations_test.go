package tests

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
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

type orchestrationTestConfig struct {
	orchestrationWorkerOptions []backend.OrchestrationWorkerOption
	activityWorkerOptions      []backend.ActivityWorkerOption
}

type testOption func(*orchestrationTestConfig)

func withMaxParallelism(maxParallelWorkItems int32) testOption {
	return func(cfg *orchestrationTestConfig) {
		cfg.orchestrationWorkerOptions = append(cfg.orchestrationWorkerOptions, backend.WithMaxConcurrentOrchestratorInvocations(maxParallelWorkItems))
		cfg.activityWorkerOptions = append(cfg.activityWorkerOptions, backend.WithMaxConcurrentActivityInvocations(maxParallelWorkItems))
	}
}

func withChunkingConfig(chunkingConfig backend.ChunkingConfiguration) testOption {
	return func(cfg *orchestrationTestConfig) {
		cfg.orchestrationWorkerOptions = append(cfg.orchestrationWorkerOptions, backend.WithChunkingConfiguration(chunkingConfig))
		cfg.activityWorkerOptions = append(cfg.activityWorkerOptions, backend.WithMaxActivityOutputSizeInKB(chunkingConfig.MaxHistoryEventSizeInKB))
	}
}

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
	client, worker := initTaskHubWorker(ctx, r, withMaxParallelism(10))
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

	// Initialization - configure chunking for this test as a way to increase code coverage
	ctx := context.Background()
	chunkingConfig := backend.ChunkingConfiguration{MaxHistoryEventCount: 1000}
	client, worker := initTaskHubWorker(ctx, r, withChunkingConfig(chunkingConfig))
	defer worker.Shutdown(ctx)

	// Run the orchestration
	eventCount := 100
	id, err := client.ScheduleNewOrchestration(ctx, "ContinueAsNewTest", api.WithInput(0))
	require.NoError(t, err)
	for i := 0; i < eventCount; i++ {
		require.NoError(t, client.RaiseEvent(ctx, id, "MyEvent", api.WithEventPayload(false)))
	}
	require.NoError(t, client.RaiseEvent(ctx, id, "MyEvent", api.WithEventPayload(true)))
	metadata, err := client.WaitForOrchestrationCompletion(ctx, id)
	require.NoError(t, err)
	assert.Equal(t, protos.OrchestrationStatus_ORCHESTRATION_STATUS_COMPLETED, metadata.RuntimeStatus)
	assert.Equal(t, strconv.Itoa(eventCount), metadata.SerializedOutput)
}

func Test_ExternalEventContention(t *testing.T) {
	// Registration
	r := task.NewTaskRegistry()
	r.AddOrchestratorN("ContinueAsNewTest", func(ctx *task.OrchestrationContext) (any, error) {
		var data int32
		if err := ctx.WaitForSingleEvent("MyEventData", 1*time.Second).Await(&data); err != nil && !errors.Is(err, task.ErrTaskCanceled) {
			return nil, err
		}

		var complete bool
		if err := ctx.WaitForSingleEvent("MyEventSignal", -1).Await(&complete); err != nil {
			return nil, err
		}

		if complete {
			return data, nil
		}

		ctx.ContinueAsNew(nil, task.WithKeepUnprocessedEvents())
		return nil, nil
	})

	// Initialization
	ctx := context.Background()
	client, worker := initTaskHubWorker(ctx, r)
	defer worker.Shutdown(ctx)

	// Run the orchestration
	id, err := client.ScheduleNewOrchestration(ctx, "ContinueAsNewTest")
	require.NoError(t, err)

	// Wait for the timer to elapse
	timeoutCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	_, err = client.WaitForOrchestrationCompletion(timeoutCtx, id)
	require.ErrorIs(t, err, timeoutCtx.Err())

	// Now raise the event, which should queue correctly for the next time
	// around
	require.NoError(t, client.RaiseEvent(ctx, id, "MyEventData", api.WithEventPayload(42)))
	require.NoError(t, client.RaiseEvent(ctx, id, "MyEventSignal", api.WithEventPayload(false)))
	require.NoError(t, client.RaiseEvent(ctx, id, "MyEventSignal", api.WithEventPayload(true)))

	metadata, err := client.WaitForOrchestrationCompletion(ctx, id)
	require.NoError(t, err)
	assert.Equal(t, protos.OrchestrationStatus_ORCHESTRATION_STATUS_COMPLETED, metadata.RuntimeStatus)
	assert.Equal(t, `42`, metadata.SerializedOutput)
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
	require.NoError(t, err)
	for i := 0; i < eventCount; i++ {
		opts := api.WithEventPayload(i)
		require.NoError(t, client.RaiseEvent(ctx, id, "MyEvent", opts))
	}
	metadata, err := client.WaitForOrchestrationCompletion(ctx, id)
	require.NoError(t, err)
	require.True(t, metadata.IsComplete())
	require.Equal(t, protos.OrchestrationStatus_ORCHESTRATION_STATUS_COMPLETED, metadata.RuntimeStatus)
	require.Equal(t, strconv.FormatBool(true), metadata.SerializedOutput)

	// Validate the exported OTel traces
	eventSizeInBytes := 1
	require.NoError(t, sharedSpanProcessor.ForceFlush(ctx))
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

func Test_ContinueAsNew_InfiniteLoop(t *testing.T) {
	// Count how many times we run the orchestrator function. We set a hard cap of 1M iterations,
	// but the orchestrator should fail before we reach this number.
	maxTestIterations := 1000000
	iteration := 0

	// Registration
	r := task.NewTaskRegistry()
	r.AddOrchestratorN("InfiniteLoop", func(ctx *task.OrchestrationContext) (any, error) {
		iteration++
		if iteration <= maxTestIterations {
			ctx.ContinueAsNew(nil)
		}
		return nil, nil
	})

	// Initialization
	ctx := context.Background()
	client, worker := initTaskHubWorker(ctx, r)
	defer worker.Shutdown(ctx)

	// Run the orchestration
	id, err := client.ScheduleNewOrchestration(ctx, "InfiniteLoop")
	require.NoError(t, err)

	// Wait for the orchestration to complete
	metadata, err := client.WaitForOrchestrationCompletion(ctx, id)
	require.NoError(t, err)
	assert.True(t, metadata.IsComplete())
	assert.Equal(t, protos.OrchestrationStatus_ORCHESTRATION_STATUS_FAILED, metadata.RuntimeStatus)
	if assert.NotNil(t, metadata.FailureDetails) {
		assert.Contains(t, metadata.FailureDetails.ErrorMessage, "exceeded tight-loop continue-as-new limit")
	}
}

func Test_OrchestrationOutputTooLarge(t *testing.T) {
	// Registration
	r := task.NewTaskRegistry()
	r.AddOrchestratorN("OrchestrationOutputTooLarge", func(ctx *task.OrchestrationContext) (any, error) {
		// Return a payload that's larger than the configured max chunk size
		return strings.Repeat("a", 3*1024), nil
	})

	// Initialization
	ctx := context.Background()
	client, worker := initTaskHubWorker(ctx, r, withChunkingConfig(backend.ChunkingConfiguration{MaxHistoryEventSizeInKB: 2}))
	defer worker.Shutdown(ctx)

	// Run the orchestration with an input that's larger than the max chunk size
	id, err := client.ScheduleNewOrchestration(ctx, "OrchestrationOutputTooLarge")
	require.NoError(t, err)

	// Wait for the orchestration to fail
	metadata, err := client.WaitForOrchestrationCompletion(ctx, id)
	require.NoError(t, err)
	assert.True(t, metadata.IsComplete())
	assert.Equal(t, protos.OrchestrationStatus_ORCHESTRATION_STATUS_FAILED, metadata.RuntimeStatus)
	if assert.NotNil(t, metadata.FailureDetails) {
		assert.Contains(t, metadata.FailureDetails.ErrorMessage, "exceeds")
		assert.Contains(t, metadata.FailureDetails.ErrorMessage, "maximum")
		assert.Contains(t, metadata.FailureDetails.ErrorMessage, "size")
		assert.Contains(t, metadata.FailureDetails.ErrorMessage, "2048")
	}
}

func Test_ActivityInputTooLarge(t *testing.T) {
	// Registration
	r := task.NewTaskRegistry()
	r.AddOrchestratorN("ActivityInputTooLarge", func(ctx *task.OrchestrationContext) (any, error) {
		// Return a payload that's larger than the configured max chunk size
		largeInput := strings.Repeat("a", 3*1024)
		return nil, ctx.CallActivity("SayHello", task.WithActivityInput(largeInput)).Await(nil)
	})
	r.AddActivityN("SayHello", func(ctx task.ActivityContext) (any, error) {
		return nil, nil
	})

	// Initialization
	ctx := context.Background()
	client, worker := initTaskHubWorker(ctx, r, withChunkingConfig(backend.ChunkingConfiguration{MaxHistoryEventSizeInKB: 2}))
	defer worker.Shutdown(ctx)

	// Run the orchestration with an input that's larger than the max chunk size
	id, err := client.ScheduleNewOrchestration(ctx, "ActivityInputTooLarge")
	require.NoError(t, err)

	// Wait for the orchestration to fail
	metadata, err := client.WaitForOrchestrationCompletion(ctx, id)
	require.NoError(t, err)
	assert.True(t, metadata.IsComplete())
	assert.Equal(t, protos.OrchestrationStatus_ORCHESTRATION_STATUS_FAILED, metadata.RuntimeStatus)
	if assert.NotNil(t, metadata.FailureDetails) {
		assert.Contains(t, metadata.FailureDetails.ErrorMessage, "exceeds")
		assert.Contains(t, metadata.FailureDetails.ErrorMessage, "maximum")
		assert.Contains(t, metadata.FailureDetails.ErrorMessage, "size")
		assert.Contains(t, metadata.FailureDetails.ErrorMessage, "2048")
	}
}

func Test_ActivityOutputTooLarge(t *testing.T) {
	// Registration
	r := task.NewTaskRegistry()
	r.AddOrchestratorN("ActivityOutputTooLarge", func(ctx *task.OrchestrationContext) (any, error) {
		if err := ctx.CallActivity("SayHello", task.WithActivityInput("世界")).Await(nil); err != nil {
			return nil, err
		}
		return nil, nil
	})
	r.AddActivityN("SayHello", func(ctx task.ActivityContext) (any, error) {
		// Return a payload that's larger than the configured max chunk size
		return strings.Repeat("a", 3*1024), nil
	})

	// Initialization
	ctx := context.Background()
	client, worker := initTaskHubWorker(ctx, r, withChunkingConfig(backend.ChunkingConfiguration{MaxHistoryEventSizeInKB: 2}))
	defer worker.Shutdown(ctx)

	// Run the orchestration with an input that's larger than the max chunk size
	id, err := client.ScheduleNewOrchestration(ctx, "ActivityOutputTooLarge")
	require.NoError(t, err)

	// Wait for the orchestration to fail
	metadata, err := client.WaitForOrchestrationCompletion(ctx, id)
	require.NoError(t, err)
	assert.True(t, metadata.IsComplete())
	assert.Equal(t, protos.OrchestrationStatus_ORCHESTRATION_STATUS_FAILED, metadata.RuntimeStatus)
	if assert.NotNil(t, metadata.FailureDetails) {
		assert.Contains(t, metadata.FailureDetails.ErrorMessage, "SayHello")
		assert.Contains(t, metadata.FailureDetails.ErrorMessage, "exceeds")
		assert.Contains(t, metadata.FailureDetails.ErrorMessage, "limit")
		assert.Contains(t, metadata.FailureDetails.ErrorMessage, "size")
		assert.Contains(t, metadata.FailureDetails.ErrorMessage, "2048")
	}
}

// Test_ChunkActivityFanOut_MaxEventCount tests the case where the degree of fan out exceeds max chunking configuration.
// The point of this test is to make sure that the orchestration completes successfully. It does not test the chunking behavior itself.
func Test_ChunkActivityFanOut_MaxEventCount(t *testing.T) {
	// Registration
	r := task.NewTaskRegistry()
	r.AddOrchestratorN("ActivityFanOut", func(ctx *task.OrchestrationContext) (any, error) {
		tasks := []task.Task{}
		for i := 0; i < 100; i++ {
			tasks = append(tasks, ctx.CallActivity("PlusOne", task.WithActivityInput(0)))
		}
		results := []int{}
		for _, t := range tasks {
			var result int
			if err := t.Await(&result); err != nil {
				return nil, err
			}
			results = append(results, result)
		}
		sum := 0
		for _, r := range results {
			sum += r
		}
		return sum, nil
	})
	r.AddActivityN("PlusOne", func(ctx task.ActivityContext) (any, error) {
		var input int
		if err := ctx.GetInput(&input); err != nil {
			return nil, err
		}
		return input + 1, nil
	})

	// Force the orchestrator to chunk the history into batches of 10 events
	chunkingConfig := backend.ChunkingConfiguration{MaxHistoryEventCount: 10}

	// Initialization
	ctx := context.Background()
	client, worker := initTaskHubWorker(ctx, r, withChunkingConfig(chunkingConfig))
	defer worker.Shutdown(ctx)

	// Run the orchestration
	id, err := client.ScheduleNewOrchestration(ctx, "ActivityFanOut")
	require.NoError(t, err)
	timeoutCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	metadata, err := client.WaitForOrchestrationCompletion(timeoutCtx, id)
	require.NoError(t, err)
	assert.Equal(t, protos.OrchestrationStatus_ORCHESTRATION_STATUS_COMPLETED, metadata.RuntimeStatus)
	assert.Equal(t, `100`, metadata.SerializedOutput)
}

// Test_ChunkContinueAsNew test the case where an orchestration receives 30 events and then continues as new
// after each one is processed. The chunk size is set to 10, so we expect there are at least 3 chunks.
// We don't measure the chunking behavior here (we can't) but we do want to make sure that the orchestration
// completes successfully with the expected output, which is the number of events processed.
func Test_ChunkContinueAsNew(t *testing.T) {
	targetEventCount := 30

	// Registration
	r := task.NewTaskRegistry()
	r.AddOrchestratorN("ContinueAsNewTest", func(ctx *task.OrchestrationContext) (any, error) {
		var currentValue int
		if err := ctx.GetInput(&currentValue); err != nil {
			return nil, err
		}

		if currentValue == 0 {
			// Wait for 1 second to give the client a chance to raise all the events
			time.Sleep(1 * time.Second)
		}

		// Break out of the loop once we've received all N events
		if currentValue == targetEventCount {
			return currentValue, nil
		}

		// Wait for an event
		if err := ctx.WaitForSingleEvent("MyEvent", 5*time.Second).Await(nil); err != nil {
			return nil, err
		}

		// Loop until we've received N events
		ctx.ContinueAsNew(currentValue+1, task.WithKeepUnprocessedEvents())
		return -1, nil
	})

	// Force the orchestrator to chunk the history into batches of 10 events
	chunkingConfig := backend.ChunkingConfiguration{MaxHistoryEventCount: 10}

	// Initialization
	ctx := context.Background()
	client, worker := initTaskHubWorker(ctx, r, withChunkingConfig(chunkingConfig))
	defer worker.Shutdown(ctx)

	// Run the orchestration
	id, err := client.ScheduleNewOrchestration(ctx, "ContinueAsNewTest", api.WithInput(0))
	require.NoError(t, err)

	// Raise N events to the orchestration
	for i := 0; i < int(targetEventCount); i++ {
		require.NoError(t, client.RaiseEvent(ctx, id, "MyEvent"))
	}

	metadata, err := client.WaitForOrchestrationCompletion(ctx, id)
	require.NoError(t, err)
	assert.Equal(t, protos.OrchestrationStatus_ORCHESTRATION_STATUS_COMPLETED, metadata.RuntimeStatus)
	assert.Equal(t, fmt.Sprintf("%d", targetEventCount), metadata.SerializedOutput)
}

func initTaskHubWorker(ctx context.Context, r *task.TaskRegistry, opts ...testOption) (backend.TaskHubClient, backend.TaskHubWorker) {
	var config orchestrationTestConfig
	for _, configure := range opts {
		configure(&config)
	}
	// TODO: Switch to options pattern
	logger := backend.DefaultLogger()
	be := sqlite.NewSqliteBackend(sqlite.NewSqliteOptions(""), logger)
	executor := task.NewTaskExecutor(r)
	orchestrationWorker := backend.NewOrchestrationWorker(be, executor, logger, config.orchestrationWorkerOptions...)
	activityWorker := backend.NewActivityTaskWorker(be, executor, logger, config.activityWorkerOptions...)
	taskHubWorker := backend.NewTaskHubWorker(be, orchestrationWorker, activityWorker, logger)
	if err := taskHubWorker.Start(ctx); err != nil {
		panic(err)
	}
	taskHubClient := backend.NewTaskHubClient(be)
	return taskHubClient, taskHubWorker
}
