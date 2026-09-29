package durabletaskscheduler_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/microsoft/durabletask-go/api"
	durabletaskclient "github.com/microsoft/durabletask-go/client"
	"github.com/microsoft/durabletask-go/durabletaskscheduler"
	"github.com/microsoft/durabletask-go/internal/protos"
	"github.com/microsoft/durabletask-go/task"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

func TestDTSTeardown(t *testing.T) {
	options := emulatorOptions(t)
	for _, test := range []struct {
		name   string
		status api.OrchestrationStatus
	}{
		{"status", api.RUNTIME_STATUS_COMPLETED},
		{"completed", api.RUNTIME_STATUS_COMPLETED},
		{"continued", api.RUNTIME_STATUS_COMPLETED},
		{"terminated", api.RUNTIME_STATUS_TERMINATED},
		{"root-failure", api.RUNTIME_STATUS_FAILED},
		{"root-defer", api.RUNTIME_STATUS_COMPLETED},
		{"join", api.RUNTIME_STATUS_COMPLETED},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
			defer cancel() // Release the interceptor before instance-cleanup callbacks run.
			id := uniqueInstanceID("go-teardown-" + test.name)
			name, cleanup := string(id), string(id)+"-cleanup"
			registry := task.NewTaskRegistry()
			require.NoError(t, registry.AddActivityN(cleanup, func(task.ActivityContext) (any, error) {
				return "cleaned", nil
			}), "SETUP: register cleanup")
			require.NoError(t, registry.AddOrchestratorN(name, func(ctx *task.OrchestrationContext) (any, error) {
				if test.name == "continued" {
					var generation int
					if err := ctx.GetInput(&generation); err != nil {
						return nil, err
					}
					if generation == 1 {
						return "next-generation", nil
					}
				}
				switch test.name {
				case "status":
					ctx.SetCustomStatus("waiting")
					defer ctx.SetCustomStatus("finished")
				case "root-defer":
					defer func() { _ = ctx.CallActivity(cleanup).Await(nil) }()
				case "join":
					work, cancel := ctx.WithCancel()
					group := ctx.NewWaitGroup()
					var childErr error
					group.Add(1)
					ctx.Go(func(child *task.OrchestrationContext) {
						defer group.Done()
						defer func() { childErr = errors.Join(childErr, child.CallActivity(cleanup).Await(nil)) }()
						childErr = work.WaitForSingleEvent("work", -1).Await(nil)
						if childErr == task.ErrTaskCanceled { //nolint:errorlint // Only body cancellation is expected, not cleanup failure.
							childErr = nil
						}
					})
					if err := ctx.WaitForSingleEvent("finish", -1).Await(nil); err != nil {
						return nil, err
					}
					cancel()
					group.Wait(ctx)
					return "joined", childErr
				default:
					ctx.Go(func(child *task.OrchestrationContext) {
						defer func() { _ = child.CallActivity(cleanup).Await(nil) }()
						_ = child.WaitForSingleEvent("signal", -1).Await(nil)
					})
				}
				if err := ctx.WaitForSingleEvent("finish", -1).Await(nil); err != nil {
					return nil, err
				}
				if test.name == "root-failure" {
					return nil, errors.New("intentional root failure")
				}
				if test.name == "continued" {
					ctx.ContinueAsNew(1)
				}
				return "done", nil
			}), "SETUP: register orchestrator")

			type completion struct {
				response *protos.OrchestratorResponse
				err      error
			}
			completions := make(chan completion)
			workerOptions := *options
			workerOptions.UnaryInterceptors = []grpc.UnaryClientInterceptor{
				func(callCtx context.Context, method string, request, reply any, connection *grpc.ClientConn, invoke grpc.UnaryInvoker, callOptions ...grpc.CallOption) error {
					err := invoke(callCtx, method, request, reply, connection, callOptions...)
					if response, ok := request.(*protos.OrchestratorResponse); ok &&
						method == protos.TaskHubSidecarService_CompleteOrchestratorTask_FullMethodName && response.InstanceId == string(id) {
						copy := proto.CloneOf(response)
						copy.CompletionToken = ""
						select {
						case completions <- completion{copy, err}:
						case <-ctx.Done():
						}
					}
					return err
				},
			}
			client, _ := startEmulatorWithOptions(t, &workerOptions, registry,
				durabletaskclient.WithAutoWorkItemFilters(),
				durabletaskclient.WithWorkerRPCTimeout(30*time.Second),
				durabletaskclient.WithWorkerTransientRetryPolicy(1, time.Millisecond, time.Millisecond))
			t.Cleanup(func() { cleanupTeardownInstance(t, client, id) })
			nextCompletion := func() *protos.CompleteOrchestrationAction {
				t.Helper()
				select {
				case result := <-completions:
					var terminal *protos.CompleteOrchestrationAction
					for index, action := range result.response.Actions {
						if completed := action.GetCompleteOrchestration(); completed != nil {
							require.Equal(t, len(result.response.Actions)-1, index, "RED:ILLEGAL-ACTION-SEQUENCE")
							terminal = completed
						}
					}
					if test.name == "status" && terminal == nil {
						require.Equal(t, "waiting", result.response.GetCustomStatus().GetValue(), "RED:DURABLE-STATE-LEAK")
					}
					require.NoError(t, result.err, "SERVICE: completion RPC; outcome unknown on failure")
					return terminal
				case <-ctx.Done():
					t.Fatal("HOST-TIMEOUT: waiting for completion", ctx.Err())
					return nil
				}
			}

			_, err := client.ScheduleNewOrchestration(ctx, name, api.WithInstanceID(id), api.WithInput(0))
			require.NoError(t, err, "SETUP: schedule")
			require.Nil(t, nextCompletion(), "RED:PREMATURE-COMPLETION")
			if test.name == "status" {
				require.NoError(t, client.RaiseEvent(ctx, id, "unrelated"), "SETUP: unrelated event")
				require.Nil(t, nextCompletion(), "RED:PREMATURE-COMPLETION")
			}
			if test.name == "terminated" {
				require.NoError(t, client.TerminateOrchestration(ctx, id, api.WithRecursiveTerminate(false)), "SETUP: terminate")
			} else {
				require.NoError(t, client.RaiseEvent(ctx, id, "finish"), "SETUP: finish event")
			}
			for {
				if terminal := nextCompletion(); terminal != nil &&
					terminal.OrchestrationStatus != api.RUNTIME_STATUS_CONTINUED_AS_NEW {
					require.Equal(t, test.status, terminal.OrchestrationStatus, "RED:TERMINAL-STATUS")
					break
				}
			}
			metadata, err := client.WaitForOrchestrationCompletion(ctx, id)
			require.NoError(t, err, "SERVICE: terminal metadata")
			require.Equal(t, test.status, metadata.RuntimeStatus, "RED:TERMINAL-STATUS")
			if test.name == "root-defer" || test.name == "join" {
				history, err := client.GetOrchestrationHistory(ctx, id, api.HistoryQuery{ExecutionID: metadata.ExecutionID})
				require.NoError(t, err, "SERVICE: cleanup history")
				activityID, completed, terminal := int32(-1), false, false
				for _, event := range history.Events {
					switch event.Type {
					case api.HistoryEventTaskScheduled:
						require.Equal(t, cleanup, event.TaskScheduled.Name, "RED:CLEANUP-ORDER")
						require.EqualValues(t, -1, activityID, "RED:CLEANUP-ORDER: duplicate schedule")
						require.False(t, terminal, "RED:CLEANUP-ORDER: schedule after completion")
						activityID = event.EventID
					case api.HistoryEventTaskCompleted:
						require.NotEqualValues(t, -1, activityID, "RED:CLEANUP-ORDER: missing schedule")
						require.Equal(t, activityID, event.TaskCompleted.TaskScheduledID, "RED:CLEANUP-ORDER")
						require.False(t, terminal, "RED:CLEANUP-ORDER: result after completion")
						completed = true
					case api.HistoryEventExecutionCompleted:
						require.True(t, completed, "RED:CLEANUP-ORDER: cleanup was not awaited")
						terminal = true
					}
				}
				require.True(t, terminal, "RED:CLEANUP-ORDER: missing terminal history")
			}
		})
	}
}

func cleanupTeardownInstance(t *testing.T, client *durabletaskscheduler.Client, id api.InstanceID) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	metadata, err := client.FetchOrchestrationMetadata(ctx, id)
	if errors.Is(err, api.ErrInstanceNotFound) {
		return
	}
	require.NoError(t, err, "CLEANUP: fetch %s", id)
	if !metadata.IsComplete() {
		require.NoError(t, client.TerminateOrchestration(ctx, id, api.WithRecursiveTerminate(false)), "CLEANUP: terminate %s", id)
		_, err = client.WaitForOrchestrationCompletion(ctx, id)
		require.NoError(t, err, "CLEANUP: termination %s", id)
	}
	_, err = client.PurgeInstances(ctx, api.PurgeInstancesRequest{InstanceIDs: []api.InstanceID{id}, Recursive: false})
	require.NoError(t, err, "CLEANUP: batch purge %s", id)
	_, err = client.FetchOrchestrationMetadata(ctx, id)
	require.ErrorIs(t, err, api.ErrInstanceNotFound, "CLEANUP: verify absent %s", id)
}
