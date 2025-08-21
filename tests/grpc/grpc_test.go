package tests_grpc

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/dapr/durabletask-go/api"
	"github.com/dapr/durabletask-go/api/protos"
	"github.com/dapr/durabletask-go/backend"
	"github.com/dapr/durabletask-go/backend/sqlite"
	"github.com/dapr/durabletask-go/client"
	"github.com/dapr/durabletask-go/task"
	"github.com/dapr/durabletask-go/tests/utils"
	"go.opentelemetry.io/otel"
)

var (
	grpcClient *client.TaskHubGrpcClient
	ctx        = context.Background()
	tracer     = otel.Tracer("grpc-test")
)

// TestMain is the entry point for the test suite. We use this to set up a gRPC server and client instance
// which are used by all tests in the suite.
func TestMain(m *testing.M) {
	sqliteOptions := sqlite.NewSqliteOptions("")
	logger := backend.DefaultLogger()
	be := sqlite.NewSqliteBackend(sqliteOptions, logger)
	grpcServer := grpc.NewServer()
	grpcExecutor, registerFn := backend.NewGrpcExecutor(be, logger)
	registerFn(grpcServer)
	orchestrationWorker := backend.NewOrchestrationWorker(be, grpcExecutor, logger)
	activityWorker := backend.NewActivityTaskWorker(be, grpcExecutor, logger)
	taskHubWorker := backend.NewTaskHubWorker(be, orchestrationWorker, activityWorker, logger)
	if err := taskHubWorker.Start(ctx); err != nil {
		log.Fatalf("failed to start worker: %v", err)
	}

	lis, err := net.Listen("tcp", ":0")
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	go func() {
		if err := grpcServer.Serve(lis); err != nil {
			log.Fatalf("failed to serve: %v", err)
		}
		fmt.Printf("server listening at %v\n", lis.Addr())
	}()

	time.Sleep(1 * time.Second)

	conn, err := grpc.Dial(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("failed to connect to gRPC server: %v", err)
	}
	defer conn.Close()
	grpcClient = client.NewTaskHubGrpcClient(conn, logger)

	// Run the test exitCode
	exitCode := m.Run()

	timeoutCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	err = grpcExecutor.Shutdown(timeoutCtx)
	if err != nil {
		log.Fatalf("failed to shutdown grpc Executor: %v", err)
	}

	timeoutCtx, cancel = context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := taskHubWorker.Shutdown(timeoutCtx); err != nil {
		log.Fatalf("failed to shutdown worker: %v", err)
	}
	grpcServer.Stop()
	os.Exit(exitCode)
}

func startGrpcListener(t *testing.T, r *task.TaskRegistry) context.CancelFunc {
	cancelCtx, cancel := context.WithCancel(ctx)
	require.NoError(t, grpcClient.StartWorkItemListener(cancelCtx, r))
	return cancel
}

func Test_Grpc_WaitForInstanceStart_Timeout(t *testing.T) {
	r := task.NewTaskRegistry()
	r.AddOrchestratorN("WaitForInstanceStartThrowsException", func(ctx *task.OrchestrationContext) (any, error) {
		// sleep 5 seconds
		time.Sleep(5 * time.Second)
		return 42, nil
	})

	cancelListener := startGrpcListener(t, r)
	defer cancelListener()

	go grpcClient.ScheduleNewOrchestration(ctx, "WaitForInstanceStartThrowsException", api.WithInput("世界"), api.WithInstanceID("helloworld"))
	timeoutCtx, cancelTimeout := context.WithTimeout(ctx, time.Second)
	defer cancelTimeout()
	_, err := grpcClient.WaitForOrchestrationStart(timeoutCtx, "helloworld", api.WithFetchPayloads(true))
	if assert.Error(t, err) {
		assert.Contains(t, err.Error(), "context deadline exceeded")
	}
	time.Sleep(1 * time.Second)
}

func Test_Grpc_WaitForInstanceStart_ConnectionResume(t *testing.T) {
	r := task.NewTaskRegistry()
	r.AddOrchestratorN("WaitForInstanceStartThrowsException", func(ctx *task.OrchestrationContext) (any, error) {
		// sleep 5 seconds
		time.Sleep(5 * time.Second)
		return 42, nil
	})

	cancelListener := startGrpcListener(t, r)

	go grpcClient.ScheduleNewOrchestration(ctx, "WaitForInstanceStartThrowsException", api.WithInput("世界"), api.WithInstanceID("worldhello"))
	timeoutCtx, cancelTimeout := context.WithTimeout(ctx, time.Second)
	defer cancelTimeout()
	_, err := grpcClient.WaitForOrchestrationStart(timeoutCtx, "worldhello", api.WithFetchPayloads(true))
	if assert.Error(t, err) {
		assert.Contains(t, err.Error(), "context deadline exceeded")
	}
	cancelListener()
	time.Sleep(2 * time.Second)

	// reconnect
	cancelListener = startGrpcListener(t, r)
	defer cancelListener()

	// workitem should be retried and completed.
	timeoutCtx, cancelTimeout = context.WithTimeout(ctx, 30*time.Second)
	defer cancelTimeout()
	metadata, err := grpcClient.WaitForOrchestrationCompletion(timeoutCtx, "worldhello", api.WithFetchPayloads(true))
	require.NoError(t, err)
	assert.True(t, api.OrchestrationMetadataIsComplete(metadata))
	assert.Equal(t, "42", metadata.Output.Value)
	time.Sleep(1 * time.Second)
}

func Test_Grpc_HelloOrchestration(t *testing.T) {
	r := task.NewTaskRegistry()
	r.AddOrchestratorN("SingleActivity", func(ctx *task.OrchestrationContext) (any, error) {
		var input string
		if err := ctx.GetInput(&input); err != nil {
			return nil, err
		}
		var output string
		err := ctx.CallActivity("SayHello", task.WithActivityInput(input)).Await(&output)
		ctx.SetCustomStatus("hello-test")
		return output, err
	})
	r.AddActivityN("SayHello", func(ctx task.ActivityContext) (any, error) {
		var name string
		if err := ctx.GetInput(&name); err != nil {
			return nil, err
		}
		return fmt.Sprintf("Hello, %s!", name), nil
	})

	cancelListener := startGrpcListener(t, r)
	defer cancelListener()

	id, err := grpcClient.ScheduleNewOrchestration(ctx, "SingleActivity", api.WithInput("世界"))
	require.NoError(t, err)
	timeoutCtx, cancelTimeout := context.WithTimeout(ctx, 30*time.Second)
	defer cancelTimeout()
	metadata, err := grpcClient.WaitForOrchestrationCompletion(timeoutCtx, id, api.WithFetchPayloads(true))
	require.NoError(t, err)
	assert.True(t, api.OrchestrationMetadataIsComplete(metadata))
	assert.Equal(t, `"Hello, 世界!"`, metadata.Output.Value)
	assert.Equal(t, "hello-test", metadata.CustomStatus.Value)
	time.Sleep(1 * time.Second)

	err = grpcClient.PurgeOrchestrationState(ctx, id)
	assert.NoError(t, err)

}

func Test_Grpc_SuspendResume(t *testing.T) {
	const eventCount = 10

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

	cancelListener := startGrpcListener(t, r)
	defer cancelListener()

	// Run the orchestration, which will block waiting for external events
	id, err := grpcClient.ScheduleNewOrchestration(ctx, "SuspendResumeOrchestration", api.WithInput(0))
	require.NoError(t, err)

	// Suspend the orchestration
	require.NoError(t, grpcClient.SuspendOrchestration(ctx, id, ""))

	// Raise a bunch of events to the orchestration (they should get buffered but not consumed)
	for i := 0; i < eventCount; i++ {
		opts := api.WithEventPayload(i)
		require.NoError(t, grpcClient.RaiseEvent(ctx, id, "MyEvent", opts))
	}

	// Make sure the orchestration *doesn't* complete
	timeoutCtx, cancelWait := context.WithTimeout(ctx, 3*time.Second)
	defer cancelWait()
	_, err = grpcClient.WaitForOrchestrationCompletion(timeoutCtx, id)
	require.ErrorIs(t, err, timeoutCtx.Err())

	var metadata *backend.OrchestrationMetadata
	metadata, err = grpcClient.FetchOrchestrationMetadata(ctx, id)
	require.NoError(t, err)
	assert.True(t, api.OrchestrationMetadataIsRunning(metadata))
	require.Equal(t, protos.OrchestrationStatus_ORCHESTRATION_STATUS_SUSPENDED, metadata.RuntimeStatus)

	// Resume the orchestration and wait for it to complete
	require.NoError(t, grpcClient.ResumeOrchestration(ctx, id, ""))
	timeoutCtx, cancelWait = context.WithTimeout(ctx, 3*time.Second)
	defer cancelWait()
	_, err = grpcClient.WaitForOrchestrationCompletion(timeoutCtx, id)
	require.NoError(t, err)
	time.Sleep(1 * time.Second)
}

func Test_Grpc_Terminate_Recursive(t *testing.T) {
	delayTime := 4 * time.Second
	executedActivity := false
	r := task.NewTaskRegistry()
	r.AddOrchestratorN("Root", func(ctx *task.OrchestrationContext) (any, error) {
		tasks := []task.Task{}
		for i := 0; i < 5; i++ {
			task := ctx.CallSubOrchestrator("L1")
			tasks = append(tasks, task)
		}
		for _, task := range tasks {
			task.Await(nil)
		}
		return nil, nil
	})
	r.AddOrchestratorN("L1", func(ctx *task.OrchestrationContext) (any, error) {
		ctx.CallSubOrchestrator("L2").Await(nil)
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

	cancelListener := startGrpcListener(t, r)
	defer cancelListener()

	// Test terminating with and without recursion
	for _, recurse := range []bool{true, false} {
		t.Run(fmt.Sprintf("Recurse = %v", recurse), func(t *testing.T) {
			// Run the orchestration, which will block waiting for external events
			id, err := grpcClient.ScheduleNewOrchestration(ctx, "Root")
			require.NoError(t, err)

			// Wait long enough to ensure all orchestrations have started (but not longer than the timer delay)
			time.Sleep(2 * time.Second)

			// Terminate the root orchestration and mark whether a recursive termination
			output := fmt.Sprintf("Recursive termination = %v", recurse)
			opts := []api.TerminateOptions{api.WithOutput(output), api.WithRecursiveTerminate(recurse)}
			require.NoError(t, grpcClient.TerminateOrchestration(ctx, id, opts...))

			// Wait for the root orchestration to complete and verify its terminated status
			metadata, err := grpcClient.WaitForOrchestrationCompletion(ctx, id)
			require.NoError(t, err)
			require.Equal(t, protos.OrchestrationStatus_ORCHESTRATION_STATUS_TERMINATED, metadata.RuntimeStatus)
			require.Equal(t, fmt.Sprintf("\"%s\"", output), metadata.Output.Value)

			// Wait longer to ensure that none of the sub-orchestrations continued to the next step
			// of executing the activity function.
			time.Sleep(delayTime)
			assert.NotEqual(t, recurse, executedActivity)
		})
	}
}

func Test_Grpc_ReuseInstanceIDIgnore(t *testing.T) {
	delayTime := 2 * time.Second
	r := task.NewTaskRegistry()
	r.AddOrchestratorN("SingleActivity", func(ctx *task.OrchestrationContext) (any, error) {
		var input string
		if err := ctx.GetInput(&input); err != nil {
			return nil, err
		}
		var output string
		ctx.CreateTimer(delayTime).Await(nil)
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

	cancelListener := startGrpcListener(t, r)
	defer cancelListener()
	instanceID := api.InstanceID("SKIP_IF_RUNNING_OR_COMPLETED")
	reuseIdPolicy := &api.OrchestrationIdReusePolicy{
		Action:          api.REUSE_ID_ACTION_IGNORE,
		OperationStatus: []api.OrchestrationStatus{api.RUNTIME_STATUS_RUNNING, api.RUNTIME_STATUS_COMPLETED, api.RUNTIME_STATUS_PENDING},
	}

	id, err := grpcClient.ScheduleNewOrchestration(ctx, "SingleActivity", api.WithInput("世界"), api.WithInstanceID(instanceID))
	require.NoError(t, err)
	// wait orchestration to start
	grpcClient.WaitForOrchestrationStart(ctx, id)
	pivotTime := time.Now()
	// schedule again, it should ignore creating the new orchestration
	id, err = grpcClient.ScheduleNewOrchestration(ctx, "SingleActivity", api.WithInput("World"), api.WithInstanceID(id), api.WithOrchestrationIdReusePolicy(reuseIdPolicy))
	require.NoError(t, err)
	timeoutCtx, cancelTimeout := context.WithTimeout(ctx, 30*time.Second)
	defer cancelTimeout()
	metadata, err := grpcClient.WaitForOrchestrationCompletion(timeoutCtx, id, api.WithFetchPayloads(true))
	require.NoError(t, err)
	assert.True(t, api.OrchestrationMetadataIsComplete(metadata))
	// the first orchestration should complete as the second one is ignored
	assert.Equal(t, `"Hello, 世界!"`, metadata.Output.Value)
	// assert the orchestration created timestamp
	assert.True(t, pivotTime.After(metadata.CreatedAt.AsTime()))
}

func Test_Grpc_ReuseInstanceIDTerminate(t *testing.T) {
	delayTime := 2 * time.Second
	r := task.NewTaskRegistry()
	r.AddOrchestratorN("SingleActivity", func(ctx *task.OrchestrationContext) (any, error) {
		var input string
		if err := ctx.GetInput(&input); err != nil {
			return nil, err
		}
		var output string
		ctx.CreateTimer(delayTime).Await(nil)
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

	cancelListener := startGrpcListener(t, r)
	defer cancelListener()
	instanceID := api.InstanceID("TERMINATE_IF_RUNNING_OR_COMPLETED")
	reuseIdPolicy := &api.OrchestrationIdReusePolicy{
		Action:          api.REUSE_ID_ACTION_TERMINATE,
		OperationStatus: []api.OrchestrationStatus{api.RUNTIME_STATUS_RUNNING, api.RUNTIME_STATUS_COMPLETED, api.RUNTIME_STATUS_PENDING},
	}

	id, err := grpcClient.ScheduleNewOrchestration(ctx, "SingleActivity", api.WithInput("世界"), api.WithInstanceID(instanceID))
	require.NoError(t, err)
	// wait orchestration to start
	grpcClient.WaitForOrchestrationStart(ctx, id)
	pivotTime := time.Now()
	// schedule again, it should terminate the first orchestration and start a new one
	id, err = grpcClient.ScheduleNewOrchestration(ctx, "SingleActivity", api.WithInput("World"), api.WithInstanceID(id), api.WithOrchestrationIdReusePolicy(reuseIdPolicy))
	require.NoError(t, err)
	timeoutCtx, cancelTimeout := context.WithTimeout(ctx, 30*time.Second)
	defer cancelTimeout()
	metadata, err := grpcClient.WaitForOrchestrationCompletion(timeoutCtx, id, api.WithFetchPayloads(true))
	require.NoError(t, err)
	assert.True(t, api.OrchestrationMetadataIsComplete(metadata))
	// the second orchestration should complete.
	assert.Equal(t, `"Hello, World!"`, metadata.Output.Value)
	// assert the orchestration created timestamp
	assert.True(t, pivotTime.Before(metadata.CreatedAt.AsTime()))
}

func Test_Grpc_ReuseInstanceIDError(t *testing.T) {
	delayTime := 4 * time.Second
	r := task.NewTaskRegistry()
	r.AddOrchestratorN("SingleActivity", func(ctx *task.OrchestrationContext) (any, error) {
		var input string
		if err := ctx.GetInput(&input); err != nil {
			return nil, err
		}
		var output string
		ctx.CreateTimer(delayTime).Await(nil)
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

	cancelListener := startGrpcListener(t, r)
	defer cancelListener()
	instanceID := api.InstanceID("THROW_IF_RUNNING_OR_COMPLETED")

	id, err := grpcClient.ScheduleNewOrchestration(ctx, "SingleActivity", api.WithInput("世界"), api.WithInstanceID(instanceID))
	require.NoError(t, err)
	id, err = grpcClient.ScheduleNewOrchestration(ctx, "SingleActivity", api.WithInput("World"), api.WithInstanceID(id))
	if assert.Error(t, err) {
		assert.Contains(t, err.Error(), "orchestration instance already exists")
	}
}

func Test_Grpc_ActivityRetries(t *testing.T) {
	r := task.NewTaskRegistry()
	r.AddOrchestratorN("ActivityRetries", func(ctx *task.OrchestrationContext) (any, error) {
		if err := ctx.CallActivity("FailActivity", task.WithActivityRetryPolicy(&task.RetryPolicy{
			MaxAttempts:          3,
			InitialRetryInterval: 10 * time.Millisecond,
		})).Await(nil); err != nil {
			return nil, err
		}
		return nil, nil
	})
	r.AddActivityN("FailActivity", func(ctx task.ActivityContext) (any, error) {
		return nil, errors.New("activity failure")
	})

	cancelListener := startGrpcListener(t, r)
	defer cancelListener()
	instanceID := api.InstanceID("activity_retries")

	id, err := grpcClient.ScheduleNewOrchestration(ctx, "ActivityRetries", api.WithInstanceID(instanceID))
	require.NoError(t, err)
	timeoutCtx, cancelTimeout := context.WithTimeout(ctx, 30*time.Second)
	defer cancelTimeout()
	metadata, err := grpcClient.WaitForOrchestrationCompletion(timeoutCtx, id, api.WithFetchPayloads(true))
	require.NoError(t, err)
	assert.True(t, api.OrchestrationMetadataIsComplete(metadata))
	assert.Equal(t, protos.OrchestrationStatus_ORCHESTRATION_STATUS_FAILED, metadata.RuntimeStatus)
	// With 3 max attempts there will be two retries with 10 millis delay before each
	require.GreaterOrEqual(t, metadata.LastUpdatedAt.AsTime(), metadata.CreatedAt.AsTime().Add(2*10*time.Millisecond))
}

func Test_Grpc_SubOrchestratorRetries(t *testing.T) {
	r := task.NewTaskRegistry()
	r.AddOrchestratorN("Parent", func(ctx *task.OrchestrationContext) (any, error) {
		err := ctx.CallSubOrchestrator(
			"Child",
			task.WithSubOrchestrationInstanceID(string(ctx.ID)+"_child"),
			task.WithSubOrchestrationRetryPolicy(&task.RetryPolicy{
				MaxAttempts:          3,
				InitialRetryInterval: 10 * time.Millisecond,
				BackoffCoefficient:   2,
			})).Await(nil)
		return nil, err
	})
	r.AddOrchestratorN("Child", func(ctx *task.OrchestrationContext) (any, error) {
		return nil, errors.New("Child failed")
	})

	cancelListener := startGrpcListener(t, r)
	defer cancelListener()
	instanceID := api.InstanceID("orchestrator_retries")

	id, err := grpcClient.ScheduleNewOrchestration(ctx, "Parent", api.WithInstanceID(instanceID))
	require.NoError(t, err)
	timeoutCtx, cancelTimeout := context.WithTimeout(ctx, 30*time.Second)
	defer cancelTimeout()
	metadata, err := grpcClient.WaitForOrchestrationCompletion(timeoutCtx, id, api.WithFetchPayloads(true))
	require.NoError(t, err)
	assert.True(t, api.OrchestrationMetadataIsComplete(metadata))
	assert.Equal(t, protos.OrchestrationStatus_ORCHESTRATION_STATUS_FAILED, metadata.RuntimeStatus)
	// With 3 max attempts there will be two retries with 10 millis delay before each
	require.GreaterOrEqual(t, metadata.LastUpdatedAt.AsTime(), metadata.CreatedAt.AsTime().Add(2*10*time.Millisecond))
}

func Test_SingleActivity_TaskSpan(t *testing.T) {
	// Registration
	r := task.NewTaskRegistry()
	r.AddOrchestratorN("SingleActivity_TestSpan", func(ctx *task.OrchestrationContext) (any, error) {
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
		_, childSpan := tracer.Start(ctx.Context(), "activityChild_TestSpan")
		childSpan.End()
		return fmt.Sprintf("Hello, %s!", name), nil
	})

	exporter := utils.InitTracing()
	cancelListener := startGrpcListener(t, r)
	defer cancelListener()

	// Run the orchestration
	id, err := grpcClient.ScheduleNewOrchestration(ctx, "SingleActivity_TestSpan", api.WithInput("世界"), api.WithStartTime(time.Now()))
	if assert.NoError(t, err) {
		metadata, err := grpcClient.WaitForOrchestrationCompletion(ctx, id)
		if assert.NoError(t, err) {
			assert.Equal(t, protos.OrchestrationStatus_ORCHESTRATION_STATUS_COMPLETED, metadata.RuntimeStatus)
			assert.Equal(t, `"Hello, 世界!"`, metadata.Output.Value)
		}
	}

	// Validate the exported OTel traces
	spans := exporter.GetSpans().Snapshots()
	utils.AssertSpanSequence(t, spans,
		utils.AssertOrchestratorCreated("SingleActivity_TestSpan", id),
		utils.AssertSpan("activityChild_TestSpan"),
		utils.AssertActivity("SayHello", id, 0),
		utils.AssertOrchestratorExecuted("SingleActivity_TestSpan", id, "COMPLETED"),
	)
	// assert child-parent relationship
	assert.Equal(t, spans[1].Parent().SpanID(), spans[2].SpanContext().SpanID())
}
