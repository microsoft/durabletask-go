package durabletasktest_test

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/microsoft/durabletask-go/api"
	"github.com/microsoft/durabletask-go/durabletasktest"
	"github.com/microsoft/durabletask-go/task"
)

func testContext(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	t.Cleanup(cancel)
	return ctx
}

func run(t *testing.T, host *durabletasktest.Host, name string, opts ...api.NewOrchestrationOptions) *api.OrchestrationMetadata {
	t.Helper()
	ctx := testContext(t)
	id, err := host.Client().ScheduleNewOrchestration(ctx, name, opts...)
	require.NoError(t, err)
	metadata, err := host.Client().WaitForOrchestrationCompletion(ctx, id)
	require.NoError(t, err)
	return metadata
}

func TestActivitySequence(t *testing.T) {
	r := task.NewTaskRegistry()
	require.NoError(t, r.AddActivityN("Hello", func(ctx task.ActivityContext) (any, error) {
		var name string
		if err := ctx.GetInput(&name); err != nil {
			return nil, err
		}
		return "Hello, " + name + "!", nil
	}))
	require.NoError(t, r.AddOrchestratorN("Sequence", func(ctx *task.OrchestrationContext) (any, error) {
		var results []string
		for _, city := range []string{"Tokyo", "London", "Seattle"} {
			var result string
			if err := ctx.CallActivity("Hello", task.WithActivityInput(city)).Await(&result); err != nil {
				return nil, err
			}
			results = append(results, result)
		}
		return results, nil
	}))
	host := durabletasktest.New(t, r)

	metadata := run(t, host, "Sequence")
	require.Equal(t, api.RUNTIME_STATUS_COMPLETED, metadata.RuntimeStatus)
	var output []string
	require.NoError(t, metadata.ReadOutput(&output))
	assert.Equal(t, []string{"Hello, Tokyo!", "Hello, London!", "Hello, Seattle!"}, output)

	history, err := host.Client().GetOrchestrationHistory(testContext(t), metadata.InstanceID, api.HistoryQuery{})
	require.NoError(t, err)
	assert.Equal(t, metadata.ExecutionID, history.ExecutionID)
	scheduled := 0
	for _, event := range history.Events {
		if event.Type == api.HistoryEventTaskScheduled {
			scheduled++
		}
	}
	assert.Equal(t, 3, scheduled)
	assert.Equal(t, api.HistoryEventExecutionCompleted, history.Events[len(history.Events)-1].Type)
}

func TestFanOutFanIn(t *testing.T) {
	r := task.NewTaskRegistry()
	require.NoError(t, r.AddActivityN("Square", func(ctx task.ActivityContext) (any, error) {
		var n int
		if err := ctx.GetInput(&n); err != nil {
			return nil, err
		}
		return n * n, nil
	}))
	require.NoError(t, r.AddOrchestratorN("FanOut", func(ctx *task.OrchestrationContext) (any, error) {
		tasks := make([]task.Task, 10)
		for i := range tasks {
			tasks[i] = ctx.CallActivity("Square", task.WithActivityInput(i))
		}
		sum := 0
		for _, t := range tasks {
			var n int
			if err := t.Await(&n); err != nil {
				return nil, err
			}
			sum += n
		}
		return sum, nil
	}))
	host := durabletasktest.New(t, r)

	metadata := run(t, host, "FanOut")
	require.Equal(t, api.RUNTIME_STATUS_COMPLETED, metadata.RuntimeStatus)
	var sum int
	require.NoError(t, metadata.ReadOutput(&sum))
	assert.Equal(t, 285, sum)
}

func TestActivityFailureFailsOrchestration(t *testing.T) {
	r := task.NewTaskRegistry()
	require.NoError(t, r.AddActivityN("Fail", func(task.ActivityContext) (any, error) {
		return nil, errors.New("boom")
	}))
	require.NoError(t, r.AddOrchestratorN("CallsFail", func(ctx *task.OrchestrationContext) (any, error) {
		return nil, ctx.CallActivity("Fail").Await(nil)
	}))
	host := durabletasktest.New(t, r)

	metadata := run(t, host, "CallsFail")
	require.Equal(t, api.RUNTIME_STATUS_FAILED, metadata.RuntimeStatus)
	require.NotNil(t, metadata.FailureDetails)
	assert.Contains(t, metadata.FailureDetails.ErrorMessage, "boom")
}

func TestSubOrchestrations(t *testing.T) {
	r := task.NewTaskRegistry()
	require.NoError(t, r.AddOrchestratorN("Child", func(ctx *task.OrchestrationContext) (any, error) {
		var n int
		if err := ctx.GetInput(&n); err != nil {
			return nil, err
		}
		if n < 0 {
			return nil, errors.New("negative input")
		}
		return n * 2, nil
	}))
	require.NoError(t, r.AddOrchestratorN("Parent", func(ctx *task.OrchestrationContext) (any, error) {
		var input int
		if err := ctx.GetInput(&input); err != nil {
			return nil, err
		}
		var result int
		err := ctx.CallSubOrchestrator("Child",
			task.WithSubOrchestratorInput(input),
			task.WithSubOrchestrationInstanceID(string(ctx.ID)+"-child"),
		).Await(&result)
		return result, err
	}))
	host := durabletasktest.New(t, r)

	metadata := run(t, host, "Parent", api.WithInput(21))
	require.Equal(t, api.RUNTIME_STATUS_COMPLETED, metadata.RuntimeStatus)
	var result int
	require.NoError(t, metadata.ReadOutput(&result))
	assert.Equal(t, 42, result)
	child, err := host.Client().FetchOrchestrationMetadata(testContext(t), metadata.InstanceID+"-child")
	require.NoError(t, err)
	assert.Equal(t, metadata.InstanceID, child.ParentInstanceID)

	metadata = run(t, host, "Parent", api.WithInput(-1))
	require.Equal(t, api.RUNTIME_STATUS_FAILED, metadata.RuntimeStatus)
	assert.Contains(t, metadata.FailureDetails.ErrorMessage, "negative input")
}

func TestTimersAndExternalEvents(t *testing.T) {
	r := task.NewTaskRegistry()
	require.NoError(t, r.AddOrchestratorN("Approval", func(ctx *task.OrchestrationContext) (any, error) {
		if err := ctx.CreateTimer(10 * time.Millisecond).Await(nil); err != nil {
			return nil, err
		}
		var approver string
		if err := ctx.WaitForSingleEvent("Approve", 5*time.Second).Await(&approver); err != nil {
			return nil, err
		}
		return approver, nil
	}))
	require.NoError(t, r.AddOrchestratorN("TimesOut", func(ctx *task.OrchestrationContext) (any, error) {
		err := ctx.WaitForSingleEvent("Never", 10*time.Millisecond).Await(nil)
		return errors.Is(err, task.ErrTaskCanceled), nil
	}))
	host := durabletasktest.New(t, r)
	ctx := testContext(t)

	id, err := host.Client().ScheduleNewOrchestration(ctx, "Approval")
	require.NoError(t, err)
	require.NoError(t, host.Client().RaiseEvent(ctx, id, "Approve", api.WithEventPayload("alice")))
	metadata, err := host.Client().WaitForOrchestrationCompletion(ctx, id)
	require.NoError(t, err)
	require.Equal(t, api.RUNTIME_STATUS_COMPLETED, metadata.RuntimeStatus)
	var approver string
	require.NoError(t, metadata.ReadOutput(&approver))
	assert.Equal(t, "alice", approver)

	metadata = run(t, host, "TimesOut")
	require.Equal(t, api.RUNTIME_STATUS_COMPLETED, metadata.RuntimeStatus)
	var timedOut bool
	require.NoError(t, metadata.ReadOutput(&timedOut))
	assert.True(t, timedOut)
}

func TestSendEventBetweenOrchestrations(t *testing.T) {
	r := task.NewTaskRegistry()
	require.NoError(t, r.AddOrchestratorN("Receiver", func(ctx *task.OrchestrationContext) (any, error) {
		var message string
		err := ctx.WaitForSingleEvent("Message", 5*time.Second).Await(&message)
		return message, err
	}))
	require.NoError(t, r.AddOrchestratorN("Sender", func(ctx *task.OrchestrationContext) (any, error) {
		var target string
		if err := ctx.GetInput(&target); err != nil {
			return nil, err
		}
		return nil, ctx.SendEvent(api.InstanceID(target), "Message", "ping")
	}))
	host := durabletasktest.New(t, r)
	ctx := testContext(t)

	receiver, err := host.Client().ScheduleNewOrchestration(ctx, "Receiver")
	require.NoError(t, err)
	_, err = host.Client().WaitForOrchestrationStart(ctx, receiver)
	require.NoError(t, err)
	run(t, host, "Sender", api.WithInput(string(receiver)))
	metadata, err := host.Client().WaitForOrchestrationCompletion(ctx, receiver)
	require.NoError(t, err)
	var message string
	require.NoError(t, metadata.ReadOutput(&message))
	assert.Equal(t, "ping", message)
}

func TestContinueAsNew(t *testing.T) {
	r := task.NewTaskRegistry()
	require.NoError(t, r.AddOrchestratorN("Counter", func(ctx *task.OrchestrationContext) (any, error) {
		var n int
		if err := ctx.GetInput(&n); err != nil {
			return nil, err
		}
		if n < 5 {
			ctx.ContinueAsNew(n + 1)
			return nil, nil
		}
		return n, nil
	}))
	host := durabletasktest.New(t, r)

	metadata := run(t, host, "Counter", api.WithInput(0))
	require.Equal(t, api.RUNTIME_STATUS_COMPLETED, metadata.RuntimeStatus)
	var n int
	require.NoError(t, metadata.ReadOutput(&n))
	assert.Equal(t, 5, n)

	history, err := host.Client().GetOrchestrationHistory(testContext(t), metadata.InstanceID, api.HistoryQuery{})
	require.NoError(t, err)
	started := 0
	for _, event := range history.Events {
		if event.Type == api.HistoryEventExecutionStarted {
			started++
		}
	}
	assert.Equal(t, 1, started, "history should contain only the final execution")
	assert.Equal(t, metadata.ExecutionID, history.ExecutionID)
}

func TestTerminateSuspendAndResume(t *testing.T) {
	r := task.NewTaskRegistry()
	require.NoError(t, r.AddOrchestratorN("Waiter", func(ctx *task.OrchestrationContext) (any, error) {
		var value string
		err := ctx.WaitForSingleEvent("Go", time.Minute).Await(&value)
		return value, err
	}))
	require.NoError(t, r.AddOrchestratorN("WaitingParent", func(ctx *task.OrchestrationContext) (any, error) {
		return nil, ctx.CallSubOrchestrator("Waiter",
			task.WithSubOrchestrationInstanceID(string(ctx.ID)+"-child")).Await(nil)
	}))
	host := durabletasktest.New(t, r)
	ctx := testContext(t)
	c := host.Client()

	id, err := c.ScheduleNewOrchestration(ctx, "Waiter")
	require.NoError(t, err)
	_, err = c.WaitForOrchestrationStart(ctx, id)
	require.NoError(t, err)
	require.NoError(t, c.SuspendOrchestration(ctx, id, "pause"))
	metadata, err := c.FetchOrchestrationMetadata(ctx, id)
	require.NoError(t, err)
	assert.Equal(t, api.RUNTIME_STATUS_SUSPENDED, metadata.RuntimeStatus)
	assert.ErrorIs(t, c.PurgeOrchestrationState(ctx, id), api.ErrNotCompleted)
	require.NoError(t, c.RaiseEvent(ctx, id, "Go", api.WithEventPayload("done")))
	require.NoError(t, c.ResumeOrchestration(ctx, id, "continue"))
	metadata, err = c.WaitForOrchestrationCompletion(ctx, id)
	require.NoError(t, err)
	require.Equal(t, api.RUNTIME_STATUS_COMPLETED, metadata.RuntimeStatus)
	assert.ErrorIs(t, c.SuspendOrchestration(ctx, id, "too late"), api.ErrInvalidState)

	parent, err := c.ScheduleNewOrchestration(ctx, "WaitingParent")
	require.NoError(t, err)
	child := parent + "-child"
	require.Eventually(t, func() bool {
		m, err := c.FetchOrchestrationMetadata(ctx, child)
		return err == nil && m.RuntimeStatus == api.RUNTIME_STATUS_RUNNING
	}, 10*time.Second, 10*time.Millisecond)
	require.NoError(t, c.TerminateOrchestration(ctx, parent, api.WithOutput("stopped"), api.WithRecursiveTerminate(true)))
	for _, instance := range []api.InstanceID{parent, child} {
		metadata, err := c.WaitForOrchestrationCompletion(ctx, instance)
		require.NoError(t, err)
		assert.Equal(t, api.RUNTIME_STATUS_TERMINATED, metadata.RuntimeStatus, instance)
	}
}

func TestInstanceManagement(t *testing.T) {
	r := task.NewTaskRegistry()
	require.NoError(t, r.AddOrchestratorN("Echo", func(ctx *task.OrchestrationContext) (any, error) {
		var input string
		err := ctx.GetInput(&input)
		return input, err
	}))
	host := durabletasktest.New(t, r)
	ctx := testContext(t)
	c := host.Client()

	for i := range 3 {
		run(t, host, "Echo", api.WithInstanceID(api.InstanceID(fmt.Sprintf("echo-%d", i))), api.WithInput("hi"))
	}

	_, err := c.ScheduleNewOrchestration(ctx, "Echo", api.WithInstanceID("echo-0"))
	assert.ErrorIs(t, err, api.ErrDuplicateInstance)
	_, err = c.FetchOrchestrationMetadata(ctx, "missing")
	assert.ErrorIs(t, err, api.ErrInstanceNotFound)

	page, err := c.QueryInstances(ctx, api.OrchestrationQuery{InstanceIDPrefix: "echo-", PageSize: 2, FetchInputsAndOutputs: true})
	require.NoError(t, err)
	require.Len(t, page.Orchestrations, 2)
	assert.Equal(t, `"hi"`, page.Orchestrations[0].SerializedOutput)
	require.NotEmpty(t, page.ContinuationToken)
	page, err = c.QueryInstances(ctx, api.OrchestrationQuery{InstanceIDPrefix: "echo-", PageSize: 2, ContinuationToken: page.ContinuationToken})
	require.NoError(t, err)
	require.Len(t, page.Orchestrations, 1)
	assert.Empty(t, page.ContinuationToken)

	ids, err := c.ListInstanceIDs(ctx, api.InstanceIDQuery{RuntimeStatus: []api.OrchestrationStatus{api.RUNTIME_STATUS_COMPLETED}})
	require.NoError(t, err)
	assert.ElementsMatch(t, []api.InstanceID{"echo-0", "echo-1", "echo-2"}, ids.InstanceIDs)

	restarted, err := c.RestartInstance(ctx, "echo-0", api.WithRestartNewInstanceID(true))
	require.NoError(t, err)
	assert.NotEqual(t, api.InstanceID("echo-0"), restarted)
	metadata, err := c.WaitForOrchestrationCompletion(ctx, restarted)
	require.NoError(t, err)
	assert.Equal(t, `"hi"`, metadata.SerializedOutput)

	require.NoError(t, c.PurgeOrchestrationState(ctx, "echo-0"))
	assert.ErrorIs(t, c.PurgeOrchestrationState(ctx, "echo-0"), api.ErrInstanceNotFound)
	result, err := c.PurgeInstances(ctx, api.PurgeInstancesRequest{InstanceIDs: []api.InstanceID{"echo-1", "echo-2"}})
	require.NoError(t, err)
	assert.Equal(t, 2, result.DeletedInstanceCount)
	result, err = c.PurgeInstances(ctx, api.PurgeInstancesRequest{Filter: &api.PurgeInstanceFilter{
		RuntimeStatus: []api.OrchestrationStatus{api.RUNTIME_STATUS_COMPLETED},
	}})
	require.NoError(t, err)
	assert.Equal(t, 1, result.DeletedInstanceCount)
	assert.True(t, result.IsComplete)
}

func TestUnsupportedFeatures(t *testing.T) {
	r := task.NewTaskRegistry()
	require.NoError(t, r.AddOrchestratorN("CallsEntity", func(ctx *task.OrchestrationContext) (any, error) {
		return nil, ctx.CallEntity(api.NewEntityID("counter", "a"), "add").Await(nil)
	}))
	host := durabletasktest.New(t, r)
	ctx := testContext(t)

	metadata := run(t, host, "CallsEntity")
	require.Equal(t, api.RUNTIME_STATUS_FAILED, metadata.RuntimeStatus)
	require.NotNil(t, metadata.FailureDetails)
	assert.Contains(t, metadata.FailureDetails.ErrorMessage, "not supported")

	err := host.Client().SignalEntity(ctx, api.NewEntityID("counter", "a"), "add")
	assert.ErrorIs(t, err, api.ErrFeatureNotSupported)
}

func TestRewindRecovery(t *testing.T) {
	for _, mode := range []string{"activity", "child", "purged-child", "nested-child"} {
		t.Run(mode, func(t *testing.T) {
			var repaired atomic.Bool
			var goodCalls, failedCalls, parentGoodCalls atomic.Int32
			r := task.NewTaskRegistry()
			require.NoError(t, r.AddActivityN("Good", func(task.ActivityContext) (any, error) {
				goodCalls.Add(1)
				return "kept", nil
			}))
			require.NoError(t, r.AddActivityN("ParentGood", func(task.ActivityContext) (any, error) {
				parentGoodCalls.Add(1)
				return "parent", nil
			}))
			require.NoError(t, r.AddActivityN("Flaky", func(ctx task.ActivityContext) (any, error) {
				failedCalls.Add(1)
				var input string
				if err := ctx.GetInput(&input); err != nil {
					return nil, err
				}
				if !repaired.Load() {
					return nil, errors.New("dependency unavailable")
				}
				return "recovered:" + input, nil
			}))
			require.NoError(t, r.AddOrchestratorN("Leaf", func(ctx *task.OrchestrationContext) (any, error) {
				var input, good, result string
				if err := ctx.GetInput(&input); err != nil {
					return nil, err
				}
				if err := ctx.CallActivity("Good").Await(&good); err != nil {
					return nil, err
				}
				err := ctx.CallActivity("Flaky", task.WithActivityInput(input)).Await(&result)
				return good + ":" + result, err
			}))
			require.NoError(t, r.AddOrchestratorN("Parent", func(ctx *task.OrchestrationContext) (any, error) {
				var input, good, result string
				if err := ctx.GetInput(&input); err != nil {
					return nil, err
				}
				if err := ctx.CallActivity("ParentGood").Await(&good); err != nil {
					return nil, err
				}
				childName := "Leaf"
				if mode == "nested-child" && ctx.ID == "root" {
					childName = "Parent"
				}
				err := ctx.CallSubOrchestrator(childName, task.WithSubOrchestratorInput(input),
					task.WithSubOrchestrationInstanceID(string(ctx.ID)+"-child")).Await(&result)
				return good + ":" + result, err
			}))
			host := durabletasktest.New(t, r)
			ctx := testContext(t)
			name := "Leaf"
			ids := []api.InstanceID{"root"}
			want := "kept:recovered:payload"
			if mode != "activity" {
				name = "Parent"
				ids = append(ids, "root-child")
				want = "parent:" + want
			}
			if mode == "nested-child" {
				ids = append(ids, "root-child-child")
				want = "parent:" + want
			}
			failed := run(t, host, name, api.WithInstanceID("root"), api.WithInput("payload"))
			require.Equal(t, api.RUNTIME_STATUS_FAILED, failed.RuntimeStatus)
			before := make(map[api.InstanceID]string)
			for _, id := range ids {
				metadata, err := host.Client().FetchOrchestrationMetadata(ctx, id)
				require.NoError(t, err)
				before[id] = metadata.ExecutionID
			}
			if mode == "purged-child" {
				require.NoError(t, host.Client().PurgeOrchestrationState(ctx, "root-child"))
			}
			repaired.Store(true)
			require.NoError(t, host.Client().RewindInstance(ctx, "root", api.WithRewindReason("dependency repaired")))
			completed, err := host.Client().WaitForOrchestrationCompletion(ctx, "root")
			require.NoError(t, err)
			require.Equal(t, api.RUNTIME_STATUS_COMPLETED, completed.RuntimeStatus)
			var output string
			require.NoError(t, completed.ReadOutput(&output))
			require.Equal(t, want, output)
			require.Nil(t, completed.FailureDetails)
			require.True(t, completed.CompletedAt.After(failed.CompletedAt))
			goodWant := int32(1)
			if mode == "purged-child" {
				goodWant = 2 // A purged child's successful history is no longer available.
			}
			require.Equal(t, goodWant, goodCalls.Load())
			require.EqualValues(t, 2, failedCalls.Load())
			require.EqualValues(t, len(ids)-1, parentGoodCalls.Load())
			for i, id := range ids {
				history, historyErr := host.Client().GetOrchestrationHistory(ctx, id, api.HistoryQuery{})
				require.NoError(t, historyErr)
				require.NotEqual(t, before[id], history.ExecutionID)
				rewinds := 0
				for _, event := range history.Events {
					require.NotEqual(t, api.HistoryEventTaskFailed, event.Type)
					require.NotEqual(t, api.HistoryEventSubOrchestrationInstanceFailed, event.Type)
					if event.ExecutionRewound != nil {
						rewinds++
						require.Equal(t, "dependency repaired", event.ExecutionRewound.Reason)
					}
					if i > 0 && event.ExecutionStarted != nil {
						parent, parentErr := host.Client().FetchOrchestrationMetadata(ctx, ids[i-1])
						require.NoError(t, parentErr)
						require.Equal(t, parent.ExecutionID, event.ExecutionStarted.Parent.ExecutionID)
					}
				}
				wantRewinds := 1
				if mode == "purged-child" && i > 0 {
					wantRewinds = 0
				}
				require.Equal(t, wantRewinds, rewinds)
			}
		})
	}
}

func TestRewindInvalidState(t *testing.T) {
	r := task.NewTaskRegistry()
	require.NoError(t, r.AddOrchestratorN("Done", func(*task.OrchestrationContext) (any, error) {
		return nil, nil
	}))
	require.NoError(t, r.AddOrchestratorN("Waiting", func(ctx *task.OrchestrationContext) (any, error) {
		return nil, ctx.WaitForSingleEvent("finish", -1).Await(nil)
	}))
	host := durabletasktest.New(t, r)
	ctx := testContext(t)
	require.ErrorIs(t, host.Client().RewindInstance(ctx, "missing"), api.ErrInstanceNotFound)
	done := run(t, host, "Done")
	require.ErrorIs(t, host.Client().RewindInstance(ctx, done.InstanceID), api.ErrInvalidState)
	id, err := host.Client().ScheduleNewOrchestration(ctx, "Waiting")
	require.NoError(t, err)
	_, err = host.Client().WaitForOrchestrationStart(ctx, id)
	require.NoError(t, err)
	require.ErrorIs(t, host.Client().RewindInstance(ctx, id), api.ErrInvalidState)
	require.NoError(t, host.Client().SuspendOrchestration(ctx, id, "test"))
	require.ErrorIs(t, host.Client().RewindInstance(ctx, id), api.ErrInvalidState)
	require.NoError(t, host.Client().TerminateOrchestration(ctx, id))
	_, err = host.Client().WaitForOrchestrationCompletion(ctx, id)
	require.NoError(t, err)
	require.ErrorIs(t, host.Client().RewindInstance(ctx, id), api.ErrInvalidState)
}

func TestRewindRejectsUnsafeHistory(t *testing.T) {
	var calls atomic.Int32
	r := task.NewTaskRegistry()
	for _, name := range []string{"handled", "good", "terminal"} {
		require.NoError(t, r.AddActivityN(name, func(task.ActivityContext) (any, error) {
			calls.Add(1)
			if name != "good" {
				return nil, errors.New(name)
			}
			return "kept", nil
		}))
	}
	require.NoError(t, r.AddOrchestratorN("Unsafe", func(ctx *task.OrchestrationContext) (any, error) {
		if err := ctx.CallActivity("handled").Await(nil); err == nil {
			return nil, errors.New("expected a handled failure")
		}
		if err := ctx.CallActivity("good").Await(nil); err != nil {
			return nil, err
		}
		return nil, ctx.CallActivity("terminal").Await(nil)
	}))
	host := durabletasktest.New(t, r)
	ctx := testContext(t)
	failed := run(t, host, "Unsafe")
	require.Equal(t, api.RUNTIME_STATUS_FAILED, failed.RuntimeStatus)
	require.NoError(t, host.Client().RewindInstance(ctx, failed.InstanceID))
	rejected, err := host.Client().WaitForOrchestrationCompletion(ctx, failed.InstanceID)
	require.NoError(t, err)
	require.Equal(t, api.RUNTIME_STATUS_FAILED, rejected.RuntimeStatus)
	require.Contains(t, rejected.FailureDetails.ErrorMessage, "Go SDK cannot safely rewind this history")
	require.Equal(t, failed.ExecutionID, rejected.ExecutionID)
	require.EqualValues(t, 3, calls.Load())
}
