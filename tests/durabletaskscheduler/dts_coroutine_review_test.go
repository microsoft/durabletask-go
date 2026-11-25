package durabletaskscheduler_test

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/microsoft/durabletask-go/api"
	durabletaskclient "github.com/microsoft/durabletask-go/client"
	"github.com/microsoft/durabletask-go/task"
	"github.com/stretchr/testify/require"
)

func TestDTSCanceledScopeSkipsUnstartedCoroutines(t *testing.T) {
	name := "DTSReviewCanceledCallback-" + uuid.NewString()
	registry := task.NewTaskRegistry()
	require.NoError(t, registry.AddOrchestratorN(name, func(ctx *task.OrchestrationContext) (any, error) {
		var cancelFirst bool
		if err := ctx.GetInput(&cancelFirst); err != nil {
			return nil, err
		}
		child, cancel := ctx.WithCancel()
		if cancelFirst {
			cancel()
		}
		ran, siblingRan := false, false
		child.Go(func(ctx *task.OrchestrationContext) {
			ran = true
			ctx.SetCustomStatus("canceled callback ran")
		})
		cancel()
		group := ctx.NewWaitGroup()
		group.Add(1)
		ctx.Go(func(*task.OrchestrationContext) {
			siblingRan = true
			group.Done()
		})
		group.Wait(ctx)
		if err := ctx.CreateTimer(20 * time.Millisecond).Await(nil); err != nil {
			return nil, err
		}
		return !ran && siblingRan, nil
	}))
	client, _, _ := startEmulatorClientAndWorker(t, registry, durabletaskclient.WithAutoWorkItemFilters())
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	for _, cancelFirst := range []bool{false, true} {
		id := uniqueInstanceID("go-canceled-callback")
		t.Cleanup(func() { cleanupRewindInstances(t, client, []api.InstanceID{id}) })
		_, err := client.ScheduleNewOrchestration(ctx, name, api.WithInstanceID(id), api.WithInput(cancelFirst))
		require.NoError(t, err)
		result, err := client.WaitForOrchestrationCompletion(ctx, id, api.WithFetchPayloads(true))
		require.NoError(t, err)
		require.Equal(t, api.RUNTIME_STATUS_COMPLETED, result.RuntimeStatus, "%+v", result.FailureDetails)
		require.Equal(t, "true", result.SerializedOutput)
		require.Empty(t, result.SerializedCustomStatus)
	}
}

func TestDTSChannelsYieldWorkAndFailLocalDeadlocks(t *testing.T) {
	name := "DTSChannelCoordinator-" + uuid.NewString()
	activityName := "DTSChannelDouble-" + uuid.NewString()
	deadlockName := "DTSChannelDeadlock-" + uuid.NewString()
	registry := task.NewTaskRegistry()
	require.NoError(t, registry.AddActivityN(activityName, func(ctx task.ActivityContext) (any, error) {
		var value int
		if err := ctx.GetInput(&value); err != nil {
			return nil, err
		}
		return value * 2, nil
	}))
	require.NoError(t, registry.AddOrchestratorN(name, func(ctx *task.OrchestrationContext) (any, error) {
		var capacity int
		if err := ctx.GetInput(&capacity); err != nil {
			return nil, err
		}
		results := task.NewChannel[int](ctx, capacity)
		group := ctx.NewWaitGroup()
		group.Add(3)
		for _, value := range []int{2, 4, 6} {
			ctx.Go(func(branch *task.OrchestrationContext) {
				defer group.Done()
				var result int
				if err := branch.CallActivity(activityName, task.WithActivityInput(value)).Await(&result); err != nil {
					panic(err)
				}
				results.Send(branch, result)
			})
		}
		timer := ctx.CreateTimer(15 * time.Second)
		var values []int
		var failure error
		for len(values) < 3 && failure == nil {
			ctx.Select(
				task.OnReceive(results, func(value int) { values = append(values, value) }),
				task.OnTask(timer, func(task.Task) { failure = errors.New("channel coordinator timed out") }),
			)
		}
		if failure != nil {
			return nil, failure
		}
		group.Wait(ctx)
		slices.Sort(values)
		return values, nil
	}))
	require.NoError(t, registry.AddOrchestratorN(deadlockName, func(ctx *task.OrchestrationContext) (any, error) {
		return task.NewChannel[int](ctx, 0).Receive(ctx), nil
	}))
	client, _, _ := startEmulatorClientAndWorker(t, registry,
		durabletaskclient.WithAutoWorkItemFilters(),
		durabletaskclient.WithMaxConcurrentOrchestrationWorkItems(1),
	)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	for _, test := range []struct {
		name     string
		capacity int
		deadlock bool
	}{
		{name: "deadlock", deadlock: true},
		{name: "unbuffered"},
		{name: "buffered", capacity: 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			id := uniqueInstanceID("go-channel")
			t.Cleanup(func() { cleanupRewindInstances(t, client, []api.InstanceID{id}) })
			orchestrator := name
			if test.deadlock {
				orchestrator = deadlockName
			}
			_, err := client.ScheduleNewOrchestration(ctx, orchestrator,
				api.WithInstanceID(id), api.WithInput(test.capacity))
			require.NoError(t, err)
			result, err := client.WaitForOrchestrationCompletion(ctx, id, api.WithFetchPayloads(true))
			require.NoError(t, err)
			if test.deadlock {
				require.Equal(t, api.RUNTIME_STATUS_FAILED, result.RuntimeStatus)
				require.Contains(t, fmt.Sprint(result.FailureDetails), task.ErrOrchestrationDeadlock.Error())
				return
			}
			require.Equal(t, api.RUNTIME_STATUS_COMPLETED, result.RuntimeStatus, "%+v", result.FailureDetails)
			require.Equal(t, "[4,8,12]", result.SerializedOutput)
		})
	}
}
