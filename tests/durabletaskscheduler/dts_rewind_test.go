package durabletaskscheduler_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/microsoft/durabletask-go/api"
	durabletaskclient "github.com/microsoft/durabletask-go/client"
	"github.com/microsoft/durabletask-go/durabletaskscheduler"
	"github.com/microsoft/durabletask-go/task"
	"github.com/stretchr/testify/require"
)

func TestDTSRewindRecovery(t *testing.T) {
	for _, nested := range []bool{false, true} {
		name := "activity"
		if nested {
			name = "child"
		}
		t.Run(name, func(t *testing.T) {
			prefix := "GoRewind_" + uuid.NewString()
			id := api.InstanceID(prefix)
			childID := id + "-child"
			var repaired atomic.Bool
			var goodCalls, parentGoodCalls, failedCalls atomic.Int32
			registry := task.NewTaskRegistry()
			require.NoError(t, registry.AddActivityN(prefix+"Good", func(task.ActivityContext) (any, error) {
				goodCalls.Add(1)
				return "kept", nil
			}))
			require.NoError(t, registry.AddActivityN(prefix+"ParentGood", func(task.ActivityContext) (any, error) {
				parentGoodCalls.Add(1)
				return "parent", nil
			}))
			require.NoError(t, registry.AddActivityN(prefix+"Flaky", func(ctx task.ActivityContext) (any, error) {
				failedCalls.Add(1)
				var input string
				if err := ctx.GetInput(&input); err != nil {
					return nil, err
				}
				if !repaired.Load() {
					return nil, errors.New("rewind dependency unavailable")
				}
				return "recovered:" + input, nil
			}))
			require.NoError(t, registry.AddOrchestratorN(prefix+"Leaf", func(ctx *task.OrchestrationContext) (any, error) {
				var input, good, result string
				if err := ctx.GetInput(&input); err != nil {
					return nil, err
				}
				if err := ctx.CallActivity(prefix + "Good").Await(&good); err != nil {
					return nil, err
				}
				if err := ctx.CallActivity(prefix+"Flaky", task.WithActivityInput(input)).Await(&result); err != nil {
					return nil, err
				}
				return good + ":" + result, nil
			}))
			require.NoError(t, registry.AddOrchestratorN(prefix+"Parent", func(ctx *task.OrchestrationContext) (any, error) {
				var input, good, result string
				if err := ctx.GetInput(&input); err != nil {
					return nil, err
				}
				if err := ctx.CallActivity(prefix + "ParentGood").Await(&good); err != nil {
					return nil, err
				}
				if err := ctx.CallSubOrchestrator(prefix+"Leaf",
					task.WithSubOrchestratorInput(input),
					task.WithSubOrchestrationInstanceID(string(childID)),
				).Await(&result); err != nil {
					return nil, err
				}
				return good + ":" + result, nil
			}))
			client, _, _ := startEmulatorClientAndWorker(t, registry, durabletaskclient.WithAutoWorkItemFilters())
			ids := []api.InstanceID{id}
			if nested {
				ids = append(ids, childID)
			}
			t.Cleanup(func() { cleanupRewindInstances(t, client, ids) })
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
			defer cancel()
			orchestrator := prefix + "Leaf"
			if nested {
				orchestrator = prefix + "Parent"
			}
			_, err := client.ScheduleNewOrchestration(ctx, orchestrator, api.WithInstanceID(id), api.WithInput("payload"))
			require.NoError(t, err)
			failed, err := client.WaitForOrchestrationCompletion(ctx, id, api.WithFetchPayloads(true))
			require.NoError(t, err)
			require.Equal(t, api.RUNTIME_STATUS_FAILED, failed.RuntimeStatus)
			require.Contains(t, failed.FailureDetails.ErrorMessage, "rewind dependency unavailable")
			require.EqualValues(t, 1, goodCalls.Load())
			require.EqualValues(t, 1, failedCalls.Load())
			before := make(map[api.InstanceID]*api.OrchestrationHistory, len(ids))
			for _, instanceID := range ids {
				before[instanceID] = readRewindHistory(t, client, instanceID)
			}

			repaired.Store(true)
			require.NoError(t, client.RewindInstance(ctx, id, api.WithRewindReason("dependency repaired")))
			t.Logf("rewind enqueued: instance=%s previousExecution=%s", id, failed.ExecutionID)
			completed, recoveryErr := waitForRewindRecovery(ctx, client, id, failed.ExecutionID)
			after := make(map[api.InstanceID]*api.OrchestrationHistory, len(ids))
			for _, instanceID := range ids {
				after[instanceID] = readRewindHistory(t, client, instanceID)
			}
			require.NoError(t, recoveryErr)
			want := `"kept:recovered:payload"`
			if nested {
				want = `"parent:kept:recovered:payload"`
				require.EqualValues(t, 1, parentGoodCalls.Load())
			}
			require.Equal(t, want, completed.SerializedOutput)
			require.EqualValues(t, 1, goodCalls.Load(), "successful activity must be replayed, not executed again")
			require.EqualValues(t, 2, failedCalls.Load())
			for _, instanceID := range ids {
				require.NotEmpty(t, before[instanceID].ExecutionID)
				require.NotEqual(t, before[instanceID].ExecutionID, after[instanceID].ExecutionID)
				var successfulResults int
				for _, event := range after[instanceID].Events {
					require.NotEqual(t, api.HistoryEventTaskFailed, event.Type)
					require.NotEqual(t, api.HistoryEventSubOrchestrationInstanceFailed, event.Type)
					if event.TaskCompleted != nil && event.TaskCompleted.SerializedResult == `"kept"` {
						successfulResults++
					}
					if event.ExecutionStarted != nil && instanceID == childID {
						require.Equal(t, after[id].ExecutionID, event.ExecutionStarted.Parent.ExecutionID)
					}
				}
				if !nested || instanceID == childID {
					require.Equal(t, 1, successfulResults)
				}
			}
		})
	}
}

func waitForRewindRecovery(ctx context.Context, client *durabletaskscheduler.Client, id api.InstanceID, oldExecution string) (*api.OrchestrationMetadata, error) {
	ctx, cancel := context.WithTimeout(ctx, 90*time.Second)
	defer cancel()
	var current *api.OrchestrationMetadata
	for {
		var err error
		current, err = client.FetchOrchestrationMetadata(ctx, id, api.WithFetchPayloads(true))
		if err != nil {
			return current, err
		}
		if current.ExecutionID != oldExecution && current.RuntimeStatus == api.RUNTIME_STATUS_COMPLETED {
			return current, nil
		}
		timer := time.NewTimer(250 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return current, fmt.Errorf("rewind recovery not observed: execution=%s status=%s: %w", current.ExecutionID, current.RuntimeStatus, ctx.Err())
		case <-timer.C:
		}
	}
}

func readRewindHistory(t *testing.T, client *durabletaskscheduler.Client, id api.InstanceID) *api.OrchestrationHistory {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	history, err := client.GetOrchestrationHistory(ctx, id, api.HistoryQuery{})
	require.NoError(t, err)
	data, err := json.Marshal(history)
	require.NoError(t, err)
	t.Logf("history: %s", data)
	return history
}

func cleanupRewindInstances(t *testing.T, client *durabletaskscheduler.Client, ids []api.InstanceID) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	for _, id := range ids {
		metadata, err := client.FetchOrchestrationMetadata(ctx, id)
		if errors.Is(err, api.ErrInstanceNotFound) {
			continue
		}
		require.NoError(t, err)
		if !metadata.IsComplete() {
			require.NoError(t, client.TerminateOrchestration(ctx, id))
			_, err = client.WaitForOrchestrationCompletion(ctx, id)
			require.NoError(t, err)
		}
		require.NoError(t, client.PurgeOrchestrationState(ctx, id))
	}
}

func TestDTSRewindRejectedRequests(t *testing.T) {
	name := "GoRewindStates_" + uuid.NewString()
	registry := task.NewTaskRegistry()
	require.NoError(t, registry.AddOrchestratorN(name, func(ctx *task.OrchestrationContext) (any, error) {
		var wait bool
		if err := ctx.GetInput(&wait); err != nil {
			return nil, err
		}
		if wait {
			return nil, ctx.WaitForSingleEvent("release", -1).Await(nil)
		}
		return "done", nil
	}))
	client, _, _ := startEmulatorClientAndWorker(t, registry, durabletaskclient.WithAutoWorkItemFilters())
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	require.ErrorIs(t, client.RewindInstance(ctx, api.InstanceID(name+"-missing")), api.ErrInstanceNotFound)

	for _, state := range []api.OrchestrationStatus{
		api.RUNTIME_STATUS_COMPLETED,
		api.RUNTIME_STATUS_RUNNING,
		api.RUNTIME_STATUS_SUSPENDED,
		api.RUNTIME_STATUS_TERMINATED,
		api.RUNTIME_STATUS_PENDING,
	} {
		t.Run(state.String(), func(t *testing.T) {
			id := api.InstanceID(name + "-" + state.String())
			t.Cleanup(func() { cleanupRewindInstances(t, client, []api.InstanceID{id}) })
			options := []api.NewOrchestrationOptions{
				api.WithInstanceID(id),
				api.WithInput(state != api.RUNTIME_STATUS_COMPLETED),
			}
			if state == api.RUNTIME_STATUS_PENDING {
				options = append(options, api.WithStartTime(time.Now().Add(time.Hour)))
			}
			_, err := client.ScheduleNewOrchestration(ctx, name, options...)
			require.NoError(t, err)
			if state != api.RUNTIME_STATUS_PENDING {
				_, err = client.WaitForOrchestrationStart(ctx, id)
				require.NoError(t, err)
			}
			switch state {
			case api.RUNTIME_STATUS_SUSPENDED:
				require.NoError(t, client.SuspendOrchestration(ctx, id, "rewind state test"))
			case api.RUNTIME_STATUS_TERMINATED:
				require.NoError(t, client.TerminateOrchestration(ctx, id))
			}
			require.Eventually(t, func() bool {
				metadata, err := client.FetchOrchestrationMetadata(ctx, id)
				return err == nil && metadata.RuntimeStatus == state
			}, 10*time.Second, 100*time.Millisecond)
			require.ErrorIs(t, client.RewindInstance(ctx, id), api.ErrInvalidState)
			metadata, err := client.FetchOrchestrationMetadata(ctx, id)
			require.NoError(t, err)
			require.Equal(t, state, metadata.RuntimeStatus)
		})
	}
}
