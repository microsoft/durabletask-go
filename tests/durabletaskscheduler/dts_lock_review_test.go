package durabletaskscheduler_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/microsoft/durabletask-go/api"
	durabletaskclient "github.com/microsoft/durabletask-go/client"
	"github.com/microsoft/durabletask-go/task"
	"github.com/stretchr/testify/require"
)

func TestDTSEmulatorEntityLockCancellationBeforeDispatchReplays(t *testing.T) {
	registry := task.NewTaskRegistry()
	require.NoError(t, registry.AddOrchestratorN("DTSLockReviewChild", func(ctx *task.OrchestrationContext) (any, error) {
		return "child", ctx.CreateTimer(100 * time.Millisecond).Await(nil)
	}))
	require.NoError(t, registry.AddOrchestratorN("DTSLockReviewBeforeDispatch", func(ctx *task.OrchestrationContext) (any, error) {
		child, cancel := ctx.WithCancel()
		ctx.Go(func(*task.OrchestrationContext) { cancel() })
		if release, err := child.LockEntities(api.NewEntityID("lockreview", string(ctx.ID))); !errors.Is(err, task.ErrTaskCanceled) || release != nil {
			return nil, errors.New("expected canceled acquisition without a release callback")
		}
		var output string
		if err := ctx.CallSubOrchestrator("DTSLockReviewChild").Await(&output); err != nil {
			return nil, err
		}
		// Force another replay after the sub-orchestration completes.
		if err := ctx.CreateTimer(100 * time.Millisecond).Await(nil); err != nil {
			return nil, err
		}
		return output, nil
	}))
	client, _, _ := startEmulatorClientAndWorker(t, registry, durabletaskclient.WithAutoWorkItemFilters())
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	id, err := client.ScheduleNewOrchestration(ctx, "DTSLockReviewBeforeDispatch", api.WithInstanceID(uniqueInstanceID("go-lock-cancel-staged")))
	require.NoError(t, err)
	t.Cleanup(func() { cleanupRewindInstances(t, client, []api.InstanceID{id}) })
	result, err := client.WaitForOrchestrationCompletion(ctx, id, api.WithFetchPayloads(true))
	require.NoError(t, err)
	require.Equal(t, api.RUNTIME_STATUS_COMPLETED, result.RuntimeStatus, "%+v", result.FailureDetails)
	require.Equal(t, `"child"`, result.SerializedOutput)
	history := fetchHistory(t, ctx, client, id, "")
	require.Zero(t, countEvents(history, api.HistoryEventEntityLockRequested))
	require.Zero(t, countEvents(history, api.HistoryEventEntityUnlockSent))
	require.Equal(t, 1, countEvents(history, api.HistoryEventSubOrchestrationInstanceCreated))
	require.Equal(t, 1, countEvents(history, api.HistoryEventSubOrchestrationInstanceCompleted))
}

func TestDTSEmulatorEntityLockCanceledRequestReleasesAfterLateGrant(t *testing.T) {
	registry := task.NewTaskRegistry()
	key := uuid.NewString()
	entities := []api.EntityID{api.NewEntityID("lockreview", key+"-a"), api.NewEntityID("lockreview", key+"-b")}
	require.NoError(t, registry.AddEntityN("lockreview", func(*task.EntityContext) (any, error) {
		return "available", nil
	}))
	require.NoError(t, registry.AddOrchestratorN("DTSLockReviewHolder", func(ctx *task.OrchestrationContext) (any, error) {
		release, err := ctx.LockEntities(entities[1])
		if err != nil {
			return nil, err
		}
		defer release()
		ctx.SetCustomStatus(`"holding"`)
		return nil, ctx.WaitForSingleEvent("release", -1).Await(nil)
	}))
	require.NoError(t, registry.AddOrchestratorN("DTSLockReviewCanceled", func(ctx *task.OrchestrationContext) (any, error) {
		child, cancel := ctx.WithCancel()
		ctx.Go(func(ctx *task.OrchestrationContext) {
			if err := ctx.WaitForSingleEvent("cancel", -1).Await(nil); err == nil {
				cancel()
			}
		})
		if release, err := child.LockEntities(entities...); !errors.Is(err, task.ErrTaskCanceled) || release != nil {
			return nil, errors.New("expected canceled acquisition without a release callback")
		}
		if !ctx.IsInCriticalSection() {
			return nil, errors.New("canceled acquisition must retain its critical section until granted")
		}
		ctx.SetCustomStatus(`"canceled"`)
		if err := ctx.WaitForSingleEvent("finish", -1).Await(nil); err != nil {
			return nil, err
		}
		return !ctx.IsInCriticalSection(), nil
	}))
	require.NoError(t, registry.AddOrchestratorN("DTSLockReviewProbe", func(ctx *task.OrchestrationContext) (any, error) {
		release, err := ctx.LockEntities(entities...)
		if err != nil {
			return nil, err
		}
		defer release()
		var output string
		if err := ctx.CallEntity(entities[0], "get").Await(&output); err != nil {
			return nil, err
		}
		return output, nil
	}))
	client, _, _ := startEmulatorClientAndWorker(t, registry, durabletaskclient.WithAutoWorkItemFilters())
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	holderID, err := client.ScheduleNewOrchestration(ctx, "DTSLockReviewHolder", api.WithInstanceID(uniqueInstanceID("go-lock-holder")))
	require.NoError(t, err)
	t.Cleanup(func() { cleanupRewindInstances(t, client, []api.InstanceID{holderID}) })
	require.Eventually(t, func() bool {
		metadata, err := client.FetchOrchestrationMetadata(ctx, holderID, api.WithFetchPayloads(true))
		return err == nil && metadata.SerializedCustomStatus == `"holding"`
	}, 20*time.Second, 100*time.Millisecond)

	waiterID, err := client.ScheduleNewOrchestration(ctx, "DTSLockReviewCanceled", api.WithInstanceID(uniqueInstanceID("go-lock-canceled")))
	require.NoError(t, err)
	t.Cleanup(func() { cleanupRewindInstances(t, client, []api.InstanceID{waiterID}) })
	// Wait until acquisition has reached the first entity but is blocked by
	// the holder at the second. Lock-only entities are transient and cannot be
	// observed through GetEntity.
	require.Eventually(t, func() bool {
		page, err := client.QueryEntities(ctx, api.EntityQuery{
			InstanceIDStartsWith: entities[0].String(),
			IncludeTransient:     true,
		})
		if err != nil || page == nil {
			return false
		}
		for _, metadata := range page.Entities {
			if metadata != nil && metadata.InstanceID == entities[0] && metadata.LockedBy == string(waiterID) {
				return true
			}
		}
		return false
	}, 20*time.Second, 100*time.Millisecond)
	require.NoError(t, client.RaiseEvent(ctx, waiterID, "cancel"))
	require.Eventually(t, func() bool {
		metadata, err := client.FetchOrchestrationMetadata(ctx, waiterID, api.WithFetchPayloads(true))
		return err == nil && metadata.SerializedCustomStatus == `"canceled"`
	}, 20*time.Second, 100*time.Millisecond)
	canceledHistory := fetchHistory(t, ctx, client, waiterID, "")
	require.Equal(t, 1, countEvents(canceledHistory, api.HistoryEventEntityLockRequested))
	require.Zero(t, countEvents(canceledHistory, api.HistoryEventEntityLockGranted))
	require.Zero(t, countEvents(canceledHistory, api.HistoryEventEntityUnlockSent))

	require.NoError(t, client.RaiseEvent(ctx, holderID, "release"))
	holder, err := client.WaitForOrchestrationCompletion(ctx, holderID)
	require.NoError(t, err)
	require.Equal(t, api.RUNTIME_STATUS_COMPLETED, holder.RuntimeStatus)
	require.Eventually(t, func() bool {
		history, err := client.GetOrchestrationHistory(ctx, waiterID, api.HistoryQuery{})
		return err == nil && countEvents(history.Events, api.HistoryEventEntityUnlockSent) == len(entities)
	}, 20*time.Second, 100*time.Millisecond)

	// A separate orchestration must be able to use the locks while the
	// canceled acquisition's parent is still waiting for its finish event.
	probeID, err := client.ScheduleNewOrchestration(ctx, "DTSLockReviewProbe", api.WithInstanceID(uniqueInstanceID("go-lock-probe")))
	require.NoError(t, err)
	t.Cleanup(func() { cleanupRewindInstances(t, client, []api.InstanceID{probeID}) })
	probe, err := client.WaitForOrchestrationCompletion(ctx, probeID, api.WithFetchPayloads(true))
	require.NoError(t, err)
	require.Equal(t, api.RUNTIME_STATUS_COMPLETED, probe.RuntimeStatus, "%+v", probe.FailureDetails)
	require.Equal(t, `"available"`, probe.SerializedOutput)
	waiter, err := client.FetchOrchestrationMetadata(ctx, waiterID)
	require.NoError(t, err)
	require.Equal(t, api.RUNTIME_STATUS_RUNNING, waiter.RuntimeStatus)

	require.NoError(t, client.RaiseEvent(ctx, waiterID, "finish"))
	result, err := client.WaitForOrchestrationCompletion(ctx, waiterID, api.WithFetchPayloads(true))
	require.NoError(t, err)
	require.Equal(t, api.RUNTIME_STATUS_COMPLETED, result.RuntimeStatus, "%+v", result.FailureDetails)
	require.Equal(t, "true", result.SerializedOutput)
	replayedHistory := fetchHistory(t, ctx, client, waiterID, "")
	require.Equal(t, 1, countEvents(replayedHistory, api.HistoryEventEntityLockGranted))
	require.Equal(t, len(entities), countEvents(replayedHistory, api.HistoryEventEntityUnlockSent))
}
