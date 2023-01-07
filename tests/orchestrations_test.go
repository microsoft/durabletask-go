package tests

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

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
	if assert.NoError(t, err) {
		metadata, err := client.WaitForOrchestrationCompletion(ctx, id)
		if assert.NoError(t, err) {
			assert.Equal(t, protos.OrchestrationStatus_ORCHESTRATION_STATUS_COMPLETED, metadata.RuntimeStatus)
		}
	}

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
		err := ctx.CallActivity("SayHello", input).Await(&output)
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
			if err := ctx.CallActivity("PlusOne", val).Await(&val); err != nil {
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
			tasks = append(tasks, ctx.CallActivity("ToString", i))
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

func Test_ContinueAsNewOrchestration(t *testing.T) {
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
			if err := client.RaiseEvent(ctx, id, "MyEvent", i); !assert.NoError(t, err) {
				return
			}
		}

		timeoutCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()

		metadata, err := client.WaitForOrchestrationCompletion(timeoutCtx, id)
		if assert.NoError(t, err) {
			assert.True(t, metadata.IsComplete())
			assert.Equal(t, protos.OrchestrationStatus_ORCHESTRATION_STATUS_COMPLETED, metadata.RuntimeStatus)
		} else {
			return
		}
	}

	// Validate the exported OTel traces
	// TODO: Validate the external event trace events, once those are implemented
	spans := exporter.GetSpans().Snapshots()
	assertSpanSequence(t, spans,
		assertOrchestratorCreated("ExternalEventOrchestration", id),
		assertOrchestratorExecuted("ExternalEventOrchestration", id, "COMPLETED"),
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
			if !assert.NoError(t, err) {
				return
			}
			if raiseEvent {
				if err := client.RaiseEvent(ctx, id, "MyEvent", nil); !assert.NoError(t, err) {
					return
				}
			}

			timeoutCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			defer cancel()

			metadata, err := client.WaitForOrchestrationCompletion(timeoutCtx, id)
			if !assert.NoError(t, err) {
				return
			}

			// Validate the exported OTel traces
			// TODO: Validate the external event trace events, once those are implemented
			spans := exporter.GetSpans().Snapshots()
			if raiseEvent {
				assert.True(t, metadata.IsComplete())
				assert.Equal(t, protos.OrchestrationStatus_ORCHESTRATION_STATUS_COMPLETED, metadata.RuntimeStatus)

				assertSpanSequence(t, spans,
					assertOrchestratorCreated("ExternalEventOrchestrationWithTimeout", id),
					assertOrchestratorExecuted("ExternalEventOrchestrationWithTimeout", id, "COMPLETED"),
				)
			} else {
				assert.True(t, metadata.IsComplete())
				assert.Equal(t, protos.OrchestrationStatus_ORCHESTRATION_STATUS_FAILED, metadata.RuntimeStatus)
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
					assertOrchestratorExecuted("ExternalEventOrchestrationWithTimeout", id, "FAILED"),
				)
			}
		})
		exporter.Reset()
	}
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
