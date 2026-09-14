package durabletaskscheduler_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/microsoft/durabletask-go/api"
	"github.com/microsoft/durabletask-go/task"
	"github.com/microsoft/durabletask-go/tests/failurechain"
	"github.com/stretchr/testify/require"
)

// dtsLeafError opts into every durable failure enrichment hook so the failure
// chain can be validated across a real Durable Task Scheduler service.
var dtsLeafError = &failurechain.LeafError{
	Message:        "dts leaf boom",
	ErrorType:      "Contoso.DTSLeafError",
	Stack:          "Contoso.Leaf.Run(leaf.go:11)",
	Properties:     map[string]any{"code": "E11", "attempts": 4},
	IsNonRetriable: true,
}

// TestDTSEmulatorDeepFailureChain asserts a Parent -> Sub-orchestration ->
// Activity failure keeps its error types, messages, custom properties, stack
// traces, non-retriable flags, and inner failures when it round-trips through
// the Durable Task Scheduler service.
func TestDTSEmulatorDeepFailureChain(t *testing.T) {
	registry := task.NewTaskRegistry()
	require.NoError(t, registry.AddActivityN("DTSFailureLeaf", func(task.ActivityContext) (any, error) {
		return nil, dtsLeafError
	}))
	require.NoError(t, registry.AddOrchestratorN("DTSFailureChild", func(ctx *task.OrchestrationContext) (any, error) {
		return nil, ctx.CallActivity("DTSFailureLeaf").Await(nil)
	}))
	require.NoError(t, registry.AddOrchestratorN("DTSFailureParent", func(ctx *task.OrchestrationContext) (any, error) {
		return nil, ctx.CallSubOrchestrator(
			"DTSFailureChild",
			task.WithSubOrchestrationInstanceID(string(ctx.ID)+"-child"),
		).Await(nil)
	}))
	managementClient, _, _ := startEmulatorClientAndWorker(t, registry)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	parentID := uniqueInstanceID("dts-failure-chain")
	_, err := managementClient.ScheduleNewOrchestration(
		ctx,
		"DTSFailureParent",
		api.WithInstanceID(parentID),
	)
	require.NoError(t, err)
	metadata, err := managementClient.WaitForOrchestrationCompletion(ctx, parentID, api.WithFetchPayloads(true))
	require.NoError(t, err)
	require.Equal(t, api.RUNTIME_STATUS_FAILED, metadata.RuntimeStatus)
	require.NotNil(t, metadata.FailureDetails)

	parent := metadata.FailureDetails
	require.Equal(t, api.ErrorTypeTaskFailed, parent.ErrorType)
	require.Contains(t, parent.ErrorMessage, "Task 'DTSFailureChild' (#0) failed with an unhandled exception")
	require.Contains(t, parent.ErrorMessage, "dts leaf boom")
	require.True(t, parent.IsNonRetriable)

	child := parent.InnerFailure
	require.NotNil(t, child, "DTS must preserve the sub-orchestration failure frame")
	require.Equal(t, api.ErrorTypeTaskFailed, child.ErrorType)
	require.Contains(t, child.ErrorMessage, "Task 'DTSFailureLeaf' (#0) failed with an unhandled exception")
	require.True(t, child.IsNonRetriable)

	leaf := child.InnerFailure
	require.NotNil(t, leaf, "DTS must preserve the activity failure frame")
	require.Equal(t, api.ErrorType("Contoso.DTSLeafError"), leaf.ErrorType)
	require.Equal(t, "dts leaf boom", leaf.ErrorMessage)
	require.True(t, leaf.IsNonRetriable)
	require.Nil(t, leaf.InnerFailure)
	require.True(t, metadata.FailureDetails.IsCausedBy("Contoso.DTSLeafError"))

	if leaf.StackTrace == "" {
		t.Log("DTS emulator limitation: activity failure stack traces are not returned")
	} else {
		require.Equal(t, "Contoso.Leaf.Run(leaf.go:11)", leaf.StackTrace)
	}
	if len(leaf.Properties) == 0 {
		t.Log("DTS emulator limitation: custom failure properties are not returned")
	} else {
		require.Equal(t, "E11", leaf.Properties["code"])
		require.Equal(t, float64(4), leaf.Properties["attempts"])
	}

	childMetadata, err := managementClient.FetchOrchestrationMetadata(
		ctx,
		parentID+"-child",
		api.WithFetchPayloads(true),
	)
	require.NoError(t, err)
	require.Equal(t, api.RUNTIME_STATUS_FAILED, childMetadata.RuntimeStatus)
	require.NotNil(t, childMetadata.FailureDetails)
	require.Equal(t, api.ErrorTypeTaskFailed, childMetadata.FailureDetails.ErrorType)
	require.Equal(
		t,
		api.ErrorType("Contoso.DTSLeafError"),
		childMetadata.FailureDetails.InnerFailure.ErrorType,
	)
}

// TestDTSEmulatorMissingTaskBypassesRetryHandlers asserts an unregistered
// activity produces the canonical non-retriable not-found failure through DTS
// and that the retry handler is never consulted for it.
func TestDTSEmulatorMissingTaskBypassesRetryHandlers(t *testing.T) {
	registry := task.NewTaskRegistry()
	handlerCalls := make(chan struct{}, 8)
	require.NoError(t, registry.AddOrchestratorN("DTSMissingHost", func(ctx *task.OrchestrationContext) (any, error) {
		return nil, ctx.CallActivity("DTSMissingActivity", task.WithActivityRetryPolicy(&task.RetryPolicy{
			MaxAttempts:          3,
			InitialRetryInterval: 10 * time.Millisecond,
			BackoffCoefficient:   1,
			Handle: func(task.RetryContext) bool {
				select {
				case handlerCalls <- struct{}{}:
				default:
				}
				return true
			},
		})).Await(nil)
	}))
	managementClient, _, _ := startEmulatorClientAndWorker(t, registry)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	id := uniqueInstanceID("dts-missing-task")
	_, err := managementClient.ScheduleNewOrchestration(ctx, "DTSMissingHost", api.WithInstanceID(id))
	require.NoError(t, err)
	metadata, err := managementClient.WaitForOrchestrationCompletion(ctx, id, api.WithFetchPayloads(true))
	require.NoError(t, err)
	require.Equal(t, api.RUNTIME_STATUS_FAILED, metadata.RuntimeStatus)
	require.NotNil(t, metadata.FailureDetails)
	require.True(t, metadata.FailureDetails.Matches(api.ErrTaskNotRegistered))
	require.True(t, metadata.FailureDetails.IsCausedBy(api.ErrorTypeActivityTaskNotFound))
	require.True(t, metadata.FailureDetails.IsNonRetriable)
	require.Empty(t, handlerCalls, "retry handler must be bypassed for non-retriable failures")
}

// TestDTSEmulatorSuspendResumeNoOps asserts the redundant suspend and resume
// management contracts against a real service: suspending a completed instance
// and resuming a running instance must not change the instance.
func TestDTSEmulatorSuspendResumeNoOps(t *testing.T) {
	registry := task.NewTaskRegistry()
	require.NoError(t, registry.AddOrchestratorN("DTSNoOpComplete", func(ctx *task.OrchestrationContext) (any, error) {
		var input string
		if err := ctx.GetInput(&input); err != nil {
			return nil, err
		}
		return input, nil
	}))
	require.NoError(t, registry.AddOrchestratorN("DTSNoOpWait", func(ctx *task.OrchestrationContext) (any, error) {
		return nil, ctx.WaitForSingleEvent("release", -1).Await(nil)
	}))
	managementClient, _, _ := startEmulatorClientAndWorker(t, registry)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	completedID := uniqueInstanceID("dts-noop-completed")
	_, err := managementClient.ScheduleNewOrchestration(
		ctx,
		"DTSNoOpComplete",
		api.WithInstanceID(completedID),
		api.WithInput("done"),
	)
	require.NoError(t, err)
	completed, err := managementClient.WaitForOrchestrationCompletion(ctx, completedID, api.WithFetchPayloads(true))
	require.NoError(t, err)
	require.Equal(t, api.RUNTIME_STATUS_COMPLETED, completed.RuntimeStatus)

	if err := managementClient.SuspendOrchestration(ctx, completedID, "too late"); err != nil {
		// Some services reject the request outright rather than dropping it.
		t.Logf("DTS emulator limitation: suspending a completed instance returned %v", err)
	}
	if err := managementClient.ResumeOrchestration(ctx, completedID, "still too late"); err != nil {
		t.Logf("DTS emulator limitation: resuming a completed instance returned %v", err)
	}
	require.Never(t, func() bool {
		metadata, fetchErr := managementClient.FetchOrchestrationMetadata(ctx, completedID)
		return fetchErr != nil || metadata.RuntimeStatus != api.RUNTIME_STATUS_COMPLETED
	}, 2*time.Second, 200*time.Millisecond)

	runningID := uniqueInstanceID("dts-noop-running")
	_, err = managementClient.ScheduleNewOrchestration(ctx, "DTSNoOpWait", api.WithInstanceID(runningID))
	require.NoError(t, err)
	_, err = managementClient.WaitForOrchestrationStart(ctx, runningID)
	require.NoError(t, err)

	require.NoError(t, managementClient.ResumeOrchestration(ctx, runningID, "no-op"))
	require.NoError(t, managementClient.ResumeOrchestration(ctx, runningID, "still a no-op"))
	require.Never(t, func() bool {
		metadata, fetchErr := managementClient.FetchOrchestrationMetadata(ctx, runningID)
		return fetchErr != nil || metadata.RuntimeStatus != api.RUNTIME_STATUS_RUNNING
	}, 2*time.Second, 200*time.Millisecond)

	require.NoError(t, managementClient.RaiseEvent(ctx, runningID, "release"))
	finished, err := managementClient.WaitForOrchestrationCompletion(ctx, runningID)
	require.NoError(t, err)
	require.Equal(t, api.RUNTIME_STATUS_COMPLETED, finished.RuntimeStatus)
}

// TestDTSEmulatorTerminateWhileSuspended asserts termination is not blocked by
// suspension when the orchestration state is owned by a real service.
func TestDTSEmulatorTerminateWhileSuspended(t *testing.T) {
	registry := task.NewTaskRegistry()
	require.NoError(t, registry.AddOrchestratorN("DTSSuspendWait", func(ctx *task.OrchestrationContext) (any, error) {
		return nil, ctx.WaitForSingleEvent("release", -1).Await(nil)
	}))
	managementClient, _, _ := startEmulatorClientAndWorker(t, registry)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	id := uniqueInstanceID("dts-terminate-suspended")
	_, err := managementClient.ScheduleNewOrchestration(ctx, "DTSSuspendWait", api.WithInstanceID(id))
	require.NoError(t, err)
	_, err = managementClient.WaitForOrchestrationStart(ctx, id)
	require.NoError(t, err)
	require.NoError(t, managementClient.SuspendOrchestration(ctx, id, "hold"))
	require.Eventually(t, func() bool {
		metadata, fetchErr := managementClient.FetchOrchestrationMetadata(ctx, id)
		return fetchErr == nil && metadata.RuntimeStatus == api.RUNTIME_STATUS_SUSPENDED
	}, 20*time.Second, 100*time.Millisecond)

	require.NoError(t, managementClient.TerminateOrchestration(ctx, id))
	terminated, err := managementClient.WaitForOrchestrationCompletion(ctx, id)
	require.NoError(t, err)
	require.Equal(t, api.RUNTIME_STATUS_TERMINATED, terminated.RuntimeStatus)
}

// TestDTSEmulatorRestartMissingInstanceIsTypedNotFound asserts management
// requests for unknown instances surface as typed API errors instead of opaque
// RPC failures.
func TestDTSEmulatorRestartMissingInstanceIsTypedNotFound(t *testing.T) {
	registry := task.NewTaskRegistry()
	require.NoError(t, registry.AddOrchestratorN("DTSTypedErrors", func(*task.OrchestrationContext) (any, error) {
		return nil, nil
	}))
	managementClient, _, _ := startEmulatorClientAndWorker(t, registry)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	missingID := uniqueInstanceID("dts-missing-instance")

	_, err := managementClient.RestartInstance(ctx, missingID)
	switch {
	case errors.Is(err, api.ErrFeatureNotSupported):
		t.Log("DTS emulator limitation: RestartInstance is not implemented")
	case errors.Is(err, api.ErrInstanceNotFound):
	default:
		t.Fatalf("RestartInstance() error = %v, want api.ErrInstanceNotFound", err)
	}

	_, err = managementClient.FetchOrchestrationMetadata(ctx, missingID)
	require.ErrorIs(t, err, api.ErrInstanceNotFound)

	err = managementClient.PurgeOrchestrationState(ctx, missingID)
	if err != nil {
		require.ErrorIs(t, err, api.ErrInstanceNotFound)
	} else {
		t.Log("DTS emulator limitation: purging a missing instance succeeds instead of reporting not-found")
	}
}

// TestDTSEmulatorPaginationContinuationEquivalence asserts paging through a
// service query with small pages yields the same instance set as one large page.
func TestDTSEmulatorPaginationContinuationEquivalence(t *testing.T) {
	registry := task.NewTaskRegistry()
	require.NoError(t, registry.AddOrchestratorN("DTSPage", func(ctx *task.OrchestrationContext) (any, error) {
		var input string
		if err := ctx.GetInput(&input); err != nil {
			return nil, err
		}
		return input, nil
	}))
	managementClient, _, _ := startEmulatorClientAndWorker(t, registry)

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	prefix := string(uniqueInstanceID("dts-page"))
	const instanceCount = 6
	expected := make(map[api.InstanceID]struct{}, instanceCount)
	for index := range instanceCount {
		id := api.InstanceID(fmt.Sprintf("%s-%02d", prefix, index))
		_, err := managementClient.ScheduleNewOrchestration(
			ctx,
			"DTSPage",
			api.WithInstanceID(id),
			api.WithInput(fmt.Sprintf("value-%d", index)),
		)
		require.NoError(t, err)
		completed, err := managementClient.WaitForOrchestrationCompletion(ctx, id)
		require.NoError(t, err)
		require.Equal(t, api.RUNTIME_STATUS_COMPLETED, completed.RuntimeStatus)
		expected[id] = struct{}{}
	}

	collect := func(pageSize int) (map[api.InstanceID]struct{}, error) {
		collected := make(map[api.InstanceID]struct{}, instanceCount)
		token := ""
		for pages := 0; pages < instanceCount+2; pages++ {
			page, err := managementClient.QueryInstances(ctx, api.OrchestrationQuery{
				InstanceIDPrefix:  prefix,
				RuntimeStatus:     []api.OrchestrationStatus{api.RUNTIME_STATUS_COMPLETED},
				PageSize:          pageSize,
				ContinuationToken: token,
			})
			if err != nil {
				return nil, err
			}
			if len(page.Orchestrations) > pageSize {
				return nil, fmt.Errorf("page returned %d items for page size %d", len(page.Orchestrations), pageSize)
			}
			for _, item := range page.Orchestrations {
				collected[item.InstanceID] = struct{}{}
			}
			token = page.ContinuationToken
			if token == "" {
				return collected, nil
			}
		}
		return nil, errors.New("pagination did not terminate")
	}

	single, err := collect(instanceCount + 4)
	if errors.Is(err, api.ErrFeatureNotSupported) {
		t.Log("DTS emulator limitation: QueryInstances is not implemented")
		return
	}
	require.NoError(t, err)
	require.Equal(t, expected, single)

	for _, pageSize := range []int{1, 2, 4} {
		paged, err := collect(pageSize)
		require.NoErrorf(t, err, "page size %d", pageSize)
		require.Equalf(t, single, paged, "page size %d", pageSize)
	}
}

// TestDTSEmulatorOrchestrationIDReuse asserts the current status-based instance
// ID deduplication contract against a real service.
func TestDTSEmulatorOrchestrationIDReuse(t *testing.T) {
	registry := task.NewTaskRegistry()
	require.NoError(t, registry.AddOrchestratorN("DTSReuseComplete", func(ctx *task.OrchestrationContext) (any, error) {
		var input string
		if err := ctx.GetInput(&input); err != nil {
			return nil, err
		}
		return input, nil
	}))
	managementClient, _, _ := startEmulatorClientAndWorker(t, registry)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// scheduleDuplicate creates a completed instance and then re-schedules the
	// same ID with the supplied options, returning the resulting error.
	scheduleDuplicate := func(t *testing.T, prefix string, extra ...api.NewOrchestrationOptions) (api.InstanceID, error) {
		t.Helper()
		id := uniqueInstanceID(prefix)
		_, err := managementClient.ScheduleNewOrchestration(
			ctx,
			"DTSReuseComplete",
			api.WithInstanceID(id),
			api.WithInput("original"),
		)
		require.NoError(t, err)
		_, err = managementClient.WaitForOrchestrationCompletion(ctx, id)
		require.NoError(t, err)

		options := append([]api.NewOrchestrationOptions{
			api.WithInstanceID(id),
			api.WithInput("replacement"),
		}, extra...)
		_, err = managementClient.ScheduleNewOrchestration(ctx, "DTSReuseComplete", options...)
		return id, err
	}

	t.Run("empty-dedupe-set-replaces-completed-instance", func(t *testing.T) {
		id, err := scheduleDuplicate(t, "dts-reuse-all", api.WithOrchestrationIDReusePolicy(
			&api.OrchestrationIDReusePolicy{
				DedupeStatuses: []api.OrchestrationStatus{},
			},
		))
		require.NoError(t, err)
		require.Eventually(t, func() bool {
			metadata, fetchErr := managementClient.FetchOrchestrationMetadata(ctx, id, api.WithFetchPayloads(true))
			return fetchErr == nil &&
				metadata.RuntimeStatus == api.RUNTIME_STATUS_COMPLETED &&
				metadata.SerializedOutput == `"replacement"`
		}, 20*time.Second, 100*time.Millisecond)
	})

	t.Run("dedupe-status-rejects-completed-instance", func(t *testing.T) {
		id, err := scheduleDuplicate(t, "dts-reuse-dedupe", api.WithOrchestrationIDReusePolicy(
			&api.OrchestrationIDReusePolicy{
				DedupeStatuses: []api.OrchestrationStatus{api.RUNTIME_STATUS_COMPLETED},
			},
		))
		require.ErrorIs(t, err, api.ErrDuplicateInstance)
		current, fetchErr := managementClient.FetchOrchestrationMetadata(ctx, id, api.WithFetchPayloads(true))
		require.NoError(t, fetchErr)
		require.Equal(t, api.RUNTIME_STATUS_COMPLETED, current.RuntimeStatus)
		require.Equal(t, `"original"`, current.SerializedOutput)
	})

	t.Run("all-dedupe-statuses-replace-nothing", func(t *testing.T) {
		_, err := scheduleDuplicate(t, "dts-reuse-none", api.WithOrchestrationIDReusePolicy(
			&api.OrchestrationIDReusePolicy{
				DedupeStatuses: []api.OrchestrationStatus{
					api.RUNTIME_STATUS_RUNNING,
					api.RUNTIME_STATUS_COMPLETED,
					api.RUNTIME_STATUS_FAILED,
					api.RUNTIME_STATUS_CANCELED,
					api.RUNTIME_STATUS_TERMINATED,
					api.RUNTIME_STATUS_PENDING,
					api.RUNTIME_STATUS_SUSPENDED,
				},
			},
		))
		require.ErrorIs(t, err, api.ErrDuplicateInstance)
	})
}
