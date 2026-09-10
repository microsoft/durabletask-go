package durabletaskscheduler_test

import (
	"context"
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
