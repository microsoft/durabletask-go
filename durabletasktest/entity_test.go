package durabletasktest_test

import (
	"bytes"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/microsoft/durabletask-go/api"
	"github.com/microsoft/durabletask-go/durabletasktest"
	"github.com/microsoft/durabletask-go/task"
)

// counter is an entity that holds an integer.
func counter(ctx *task.EntityContext) (any, error) {
	var value int
	if ctx.HasState() {
		if err := ctx.GetState(&value); err != nil {
			return nil, err
		}
	}
	switch ctx.Operation {
	case "add":
		var amount int
		if err := ctx.GetInput(&amount); err != nil {
			return nil, err
		}
		value += amount
		return value, ctx.SetState(value)
	case "get":
		return value, nil
	case "fail":
		return nil, errors.New("counter failed")
	case "delete":
		ctx.DeleteState()
		return nil, nil
	case "forward":
		var target string
		if err := ctx.GetInput(&target); err != nil {
			return nil, err
		}
		return nil, ctx.SignalEntity(api.NewEntityID("counter", target), "add", value)
	case "start":
		var instanceID string
		if err := ctx.GetInput(&instanceID); err != nil {
			return nil, err
		}
		return nil, ctx.StartNewOrchestration("Echo",
			task.WithEntityStartOrchestrationInstanceID(instanceID),
			task.WithEntityStartOrchestrationInput(value))
	}
	return nil, errors.New("unknown operation " + ctx.Operation)
}

func newEntityHost(t *testing.T, register func(*task.TaskRegistry)) *durabletasktest.Host {
	t.Helper()
	r := task.NewTaskRegistry()
	require.NoError(t, r.AddEntityN("counter", counter))
	if register != nil {
		register(r)
	}
	return durabletasktest.New(t, r)
}

// counterValue returns the counter's value, or -1 if the entity doesn't exist.
func counterValue(t *testing.T, host *durabletasktest.Host, key string) int {
	t.Helper()
	metadata, err := host.Client().GetEntity(testContext(t), api.NewEntityID("counter", key))
	require.NoError(t, err)
	if metadata == nil {
		return -1
	}
	var value int
	require.NoError(t, metadata.ReadState(&value))
	return value
}

func waitForCounter(t *testing.T, host *durabletasktest.Host, key string, want int) {
	t.Helper()
	require.Eventually(t, func() bool { return counterValue(t, host, key) == want },
		10*time.Second, 10*time.Millisecond, "counter %q never reached %d", key, want)
}

func TestEntityClientSignals(t *testing.T) {
	host := newEntityHost(t, nil)
	c := host.Client()
	ctx := testContext(t)
	id := api.NewEntityID("counter", "a")

	metadata, err := c.GetEntity(ctx, id)
	require.NoError(t, err)
	assert.Nil(t, metadata)

	require.NoError(t, c.SignalEntity(ctx, id, "add", api.WithSignalInput(2)))
	require.NoError(t, c.SignalEntity(ctx, id, "add", api.WithSignalInput(3)))
	waitForCounter(t, host, "a", 5)

	metadata, err = c.GetEntity(ctx, id, api.GetEntityOptions{ExcludeState: true})
	require.NoError(t, err)
	require.NotNil(t, metadata)
	assert.Equal(t, id, metadata.InstanceID)
	assert.False(t, metadata.StateIncluded)
	assert.Empty(t, metadata.LockedBy)

	err = c.SignalEntity(ctx, id, "")
	assert.ErrorIs(t, err, api.ErrInvalidArgument)
}

func TestEntityScheduledSignal(t *testing.T) {
	host := newEntityHost(t, nil)
	ctx := testContext(t)

	start := time.Now()
	require.NoError(t, host.Client().SignalEntity(ctx, api.NewEntityID("counter", "later"), "add",
		api.WithSignalInput(1), api.WithSignalScheduledTime(start.Add(300*time.Millisecond))))
	assert.Equal(t, -1, counterValue(t, host, "later"))
	waitForCounter(t, host, "later", 1)
	assert.GreaterOrEqual(t, time.Since(start), 300*time.Millisecond)
}

func TestOrchestrationCallsAndSignalsEntities(t *testing.T) {
	host := newEntityHost(t, func(r *task.TaskRegistry) {
		require.NoError(t, r.AddOrchestratorN("UseCounter", func(ctx *task.OrchestrationContext) (any, error) {
			id := api.NewEntityID("counter", "orch")
			if err := ctx.SignalEntity(id, "add", task.WithSignalEntityInput(10)); err != nil {
				return nil, err
			}
			var value int
			if err := ctx.CallEntity(id, "add", task.WithEntityInput(5)).Await(&value); err != nil {
				return nil, err
			}
			return value, nil
		}))
	})

	metadata := run(t, host, "UseCounter")
	require.Equal(t, api.RUNTIME_STATUS_COMPLETED, metadata.RuntimeStatus)
	assert.Equal(t, "15", metadata.SerializedOutput)
	assert.Equal(t, 15, counterValue(t, host, "orch"))
}

func TestEntityOperationFailure(t *testing.T) {
	host := newEntityHost(t, func(r *task.TaskRegistry) {
		require.NoError(t, r.AddOrchestratorN("CallFails", func(ctx *task.OrchestrationContext) (any, error) {
			err := ctx.CallEntity(api.NewEntityID("counter", "f"), "fail").Await(nil)
			var failed *task.EntityOperationFailedError
			if !errors.As(err, &failed) {
				return nil, errors.New("expected an entity operation failure")
			}
			return failed.FailureDetails.ErrorMessage, nil
		}))
	})

	metadata := run(t, host, "CallFails")
	require.Equal(t, api.RUNTIME_STATUS_COMPLETED, metadata.RuntimeStatus)
	assert.Equal(t, `"counter failed"`, metadata.SerializedOutput)
}

func TestRewindRetainsLateEntityResult(t *testing.T) {
	var repaired atomic.Bool
	var entityCalls atomic.Int32
	entered := make(chan struct{})
	release := make(chan struct{})
	var enteredOnce, releaseOnce sync.Once
	r := task.NewTaskRegistry()
	require.NoError(t, r.AddEntityN("blocked", func(ctx *task.EntityContext) (any, error) {
		entityCalls.Add(1)
		enteredOnce.Do(func() { close(entered) })
		<-release
		return 42, ctx.SetState(42)
	}))
	require.NoError(t, r.AddActivityN("Flaky", func(ctx task.ActivityContext) (any, error) {
		<-entered
		if !repaired.Load() {
			return nil, errors.New("dependency unavailable")
		}
		return nil, nil
	}))
	require.NoError(t, r.AddOrchestratorN("Caller", func(ctx *task.OrchestrationContext) (any, error) {
		entity := ctx.CallEntity(api.NewEntityID("blocked", "a"), "get")
		if err := ctx.CallActivity("Flaky").Await(nil); err != nil {
			return nil, err
		}
		var result int
		err := entity.Await(&result)
		return result, err
	}))
	host := durabletasktest.New(t, r)
	t.Cleanup(func() { releaseOnce.Do(func() { close(release) }) })
	ctx := testContext(t)
	id, err := host.Client().ScheduleNewOrchestration(ctx, "Caller")
	require.NoError(t, err)
	failed, err := host.Client().WaitForOrchestrationCompletion(ctx, id)
	require.NoError(t, err)
	require.Equal(t, api.RUNTIME_STATUS_FAILED, failed.RuntimeStatus)

	releaseOnce.Do(func() { close(release) })
	require.Eventually(t, func() bool {
		entity, err := host.Client().GetEntity(ctx, api.NewEntityID("blocked", "a"))
		return err == nil && entity != nil
	}, 10*time.Second, 10*time.Millisecond)
	repaired.Store(true)
	require.NoError(t, host.Client().RewindInstance(ctx, id))
	completed, err := host.Client().WaitForOrchestrationCompletion(ctx, id)
	require.NoError(t, err)
	require.Equal(t, api.RUNTIME_STATUS_COMPLETED, completed.RuntimeStatus)
	assert.Equal(t, "42", completed.SerializedOutput)
	assert.EqualValues(t, 1, entityCalls.Load())
}

func TestEntityActions(t *testing.T) {
	host := newEntityHost(t, func(r *task.TaskRegistry) {
		require.NoError(t, r.AddOrchestratorN("Echo", func(ctx *task.OrchestrationContext) (any, error) {
			var input int
			err := ctx.GetInput(&input)
			return input, err
		}))
	})
	c := host.Client()
	ctx := testContext(t)
	source := api.NewEntityID("counter", "source")

	require.NoError(t, c.SignalEntity(ctx, source, "add", api.WithSignalInput(7)))
	require.NoError(t, c.SignalEntity(ctx, source, "forward", api.WithSignalInput("target")))
	waitForCounter(t, host, "target", 7)

	require.NoError(t, c.SignalEntity(ctx, source, "start", api.WithSignalInput("started-by-entity")))
	require.Eventually(t, func() bool {
		_, err := c.FetchOrchestrationMetadata(ctx, "started-by-entity")
		return err == nil
	}, 10*time.Second, 10*time.Millisecond)
	metadata, err := c.WaitForOrchestrationCompletion(ctx, "started-by-entity")
	require.NoError(t, err)
	assert.Equal(t, "7", metadata.SerializedOutput)
}

func TestEntityCriticalSection(t *testing.T) {
	a, b := api.NewEntityID("counter", "a"), api.NewEntityID("counter", "b")
	host := newEntityHost(t, func(r *task.TaskRegistry) {
		require.NoError(t, r.AddOrchestratorN("Transfer", func(ctx *task.OrchestrationContext) (any, error) {
			unlock, err := ctx.LockEntities(b, a)
			if err != nil {
				return nil, err
			}
			defer unlock()
			if err := ctx.CallEntity(a, "add", task.WithEntityInput(1)).Await(nil); err != nil {
				return nil, err
			}
			if err := ctx.CallEntity(b, "add", task.WithEntityInput(1)).Await(nil); err != nil {
				return nil, err
			}
			if err := ctx.WaitForSingleEvent("Continue", time.Minute).Await(nil); err != nil {
				return nil, err
			}
			return nil, ctx.CallEntity(a, "add", task.WithEntityInput(10)).Await(nil)
		}))
	})
	c := host.Client()
	ctx := testContext(t)

	id, err := c.ScheduleNewOrchestration(ctx, "Transfer")
	require.NoError(t, err)
	waitForCounter(t, host, "b", 1)

	// A signal from outside the critical section waits until the lock is released.
	require.NoError(t, c.SignalEntity(ctx, a, "add", api.WithSignalInput(100)))
	time.Sleep(200 * time.Millisecond)
	metadata, err := c.GetEntity(ctx, a)
	require.NoError(t, err)
	require.NotNil(t, metadata)
	assert.Equal(t, string(id), metadata.LockedBy)
	assert.Equal(t, int32(1), metadata.BacklogQueueSize)
	assert.Equal(t, 1, counterValue(t, host, "a"))

	require.NoError(t, c.RaiseEvent(ctx, id, "Continue"))
	orchestration, err := c.WaitForOrchestrationCompletion(ctx, id)
	require.NoError(t, err)
	require.Equal(t, api.RUNTIME_STATUS_COMPLETED, orchestration.RuntimeStatus)
	waitForCounter(t, host, "a", 111)
	metadata, err = c.GetEntity(ctx, a)
	require.NoError(t, err)
	assert.Empty(t, metadata.LockedBy)
	assert.Equal(t, 1, counterValue(t, host, "b"))
}

func TestQueryEntities(t *testing.T) {
	host := newEntityHost(t, nil)
	c := host.Client()
	ctx := testContext(t)
	keys := []string{"q1", "q2", "q3", "q4", "q5"}
	for _, key := range keys {
		require.NoError(t, c.SignalEntity(ctx, api.NewEntityID("counter", key), "add", api.WithSignalInput(1)))
	}
	for _, key := range keys {
		waitForCounter(t, host, key, 1)
	}

	var ids []api.EntityID
	query := api.EntityQuery{InstanceIDStartsWith: "counter@q", PageSize: 2}
	for pages := 0; ; pages++ {
		require.Less(t, pages, len(keys))
		results, err := c.QueryEntities(ctx, query)
		require.NoError(t, err)
		for _, metadata := range results.Entities {
			ids = append(ids, metadata.InstanceID)
			var value int
			require.NoError(t, metadata.ReadState(&value))
			assert.Equal(t, 1, value)
		}
		if results.ContinuationToken == "" {
			break
		}
		query.ContinuationToken = results.ContinuationToken
	}
	var want []api.EntityID
	for _, key := range keys {
		want = append(want, api.NewEntityID("counter", key))
	}
	assert.Equal(t, want, ids)

	results, err := c.QueryEntities(ctx, api.EntityQuery{
		InstanceIDStartsWith: "counter",
		LastModifiedFrom:     time.Now().Add(time.Hour),
	})
	require.NoError(t, err)
	assert.Empty(t, results.Entities)
}

func TestCleanEntityStorage(t *testing.T) {
	locked := api.NewEntityID("counter", "locked")
	host := newEntityHost(t, func(r *task.TaskRegistry) {
		require.NoError(t, r.AddOrchestratorN("HoldLock", func(ctx *task.OrchestrationContext) (any, error) {
			if _, err := ctx.LockEntities(locked); err != nil {
				return nil, err
			}
			return nil, ctx.WaitForSingleEvent("Release", time.Minute).Await(nil)
		}))
	})
	c := host.Client()
	ctx := testContext(t)

	// Deleting an entity's state leaves an empty entity that cleanup removes.
	empty := api.NewEntityID("counter", "empty")
	require.NoError(t, c.SignalEntity(ctx, empty, "add", api.WithSignalInput(1)))
	waitForCounter(t, host, "empty", 1)
	require.NoError(t, c.SignalEntity(ctx, empty, "delete"))
	waitForCounter(t, host, "empty", -1)
	results, err := c.QueryEntities(ctx, api.EntityQuery{InstanceIDStartsWith: "counter@empty", IncludeTransient: true})
	require.NoError(t, err)
	require.Len(t, results.Entities, 1)
	assert.False(t, results.Entities[0].HasState)

	// An orchestration terminated while waiting for a lock releases it once the lock is granted.
	lockedBy := func(owner api.InstanceID) func() bool {
		return func() bool {
			results, err := c.QueryEntities(ctx, api.EntityQuery{InstanceIDStartsWith: "counter@locked", IncludeTransient: true})
			return err == nil && len(results.Entities) == 1 && results.Entities[0].LockedBy == string(owner)
		}
	}
	holder, err := c.ScheduleNewOrchestration(ctx, "HoldLock")
	require.NoError(t, err)
	require.Eventually(t, lockedBy(holder), 10*time.Second, 10*time.Millisecond)
	waiter, err := c.ScheduleNewOrchestration(ctx, "HoldLock")
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		metadata, err := c.FetchOrchestrationMetadata(ctx, waiter)
		return err == nil && metadata.RuntimeStatus == api.RUNTIME_STATUS_RUNNING
	}, 10*time.Second, 10*time.Millisecond)
	require.NoError(t, c.TerminateOrchestration(ctx, waiter))
	_, err = c.WaitForOrchestrationCompletion(ctx, waiter)
	require.NoError(t, err)
	require.NoError(t, c.RaiseEvent(ctx, holder, "Release"))
	_, err = c.WaitForOrchestrationCompletion(ctx, holder)
	require.NoError(t, err)
	require.Eventually(t, lockedBy(""), 10*time.Second, 10*time.Millisecond)
	result, err := c.CleanEntityStorage(ctx)
	require.NoError(t, err)
	assert.Zero(t, result.OrphanedLocksReleased)
	assert.Equal(t, int32(2), result.EmptyEntitiesRemoved)
	results, err = c.QueryEntities(ctx, api.EntityQuery{InstanceIDStartsWith: "counter", IncludeTransient: true})
	require.NoError(t, err)
	assert.Empty(t, results.Entities)
}

func TestRewindKeepsEntityResults(t *testing.T) {
	var repaired atomic.Bool
	host := newEntityHost(t, func(r *task.TaskRegistry) {
		require.NoError(t, r.AddActivityN("Flaky", func(task.ActivityContext) (any, error) {
			if !repaired.Load() {
				return nil, errors.New("dependency unavailable")
			}
			return nil, nil
		}))
		require.NoError(t, r.AddOrchestratorN("CallThenFail", func(ctx *task.OrchestrationContext) (any, error) {
			var value int
			if err := ctx.CallEntity(api.NewEntityID("counter", "rewind"), "add", task.WithEntityInput(1)).Await(&value); err != nil {
				return nil, err
			}
			return value, ctx.CallActivity("Flaky").Await(nil)
		}))
	})
	ctx := testContext(t)

	failed := run(t, host, "CallThenFail")
	require.Equal(t, api.RUNTIME_STATUS_FAILED, failed.RuntimeStatus)
	repaired.Store(true)
	require.NoError(t, host.Client().RewindInstance(ctx, failed.InstanceID))
	completed, err := host.Client().WaitForOrchestrationCompletion(ctx, failed.InstanceID)
	require.NoError(t, err)
	require.Equal(t, api.RUNTIME_STATUS_COMPLETED, completed.RuntimeStatus)
	assert.Equal(t, "1", completed.SerializedOutput)
	assert.Equal(t, 1, counterValue(t, host, "rewind"))
}

// blob is an entity whose state is the input of its last "set" operation. Its
// "send" operation signals the blob entity named by the input to set a 2 MiB value.
func blob(ctx *task.EntityContext) (any, error) {
	var value string
	if err := ctx.GetInput(&value); err != nil {
		return nil, err
	}
	if ctx.Operation == "send" {
		return nil, ctx.SignalEntity(api.NewEntityID("blob", value), "set", strings.Repeat("x", 2<<20))
	}
	return nil, ctx.SetState(value)
}

func newBlobHost(t *testing.T, opts ...durabletasktest.Option) *durabletasktest.Host {
	t.Helper()
	r := task.NewTaskRegistry()
	require.NoError(t, r.AddEntityN("blob", blob))
	return durabletasktest.New(t, r, opts...)
}

func waitForBlob(t *testing.T, host *durabletasktest.Host, id api.EntityID, want string) {
	t.Helper()
	require.Eventually(t, func() bool {
		metadata, err := host.Client().GetEntity(testContext(t), id)
		require.NoError(t, err)
		var value string
		return metadata != nil && metadata.ReadState(&value) == nil && value == want
	}, 10*time.Second, 10*time.Millisecond, "entity %v never reached the expected state", id)
}

func TestSignalEntityRejectsSignalThatDoesNotFitWithState(t *testing.T) {
	host := newBlobHost(t)
	c := host.Client()
	ctx := testContext(t)
	id := api.NewEntityID("blob", "a")

	state := strings.Repeat("s", 3<<20)
	require.NoError(t, c.SignalEntity(ctx, id, "set", api.WithSignalInput(state)))
	waitForBlob(t, host, id, state)

	err := c.SignalEntity(ctx, id, "set", api.WithSignalInput(strings.Repeat("x", 2<<20)))
	require.ErrorIs(t, err, api.ErrInvalidArgument)
	assert.ErrorContains(t, err, "exceeds the 4194304-byte entity work item limit")

	require.NoError(t, c.SignalEntity(ctx, id, "set", api.WithSignalInput("small")))
	waitForBlob(t, host, id, "small")
}

// lockedBuffer is a bytes.Buffer that is safe for concurrent use.
type lockedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func TestDroppedEntitySignalIsLogged(t *testing.T) {
	var logs lockedBuffer
	host := newBlobHost(t, durabletasktest.WithLogger(api.NewSlogLogger(slog.New(slog.NewTextHandler(&logs, nil)))))
	c := host.Client()
	ctx := testContext(t)
	id := api.NewEntityID("blob", "a")

	state := strings.Repeat("s", 3<<20)
	require.NoError(t, c.SignalEntity(ctx, id, "set", api.WithSignalInput(state)))
	waitForBlob(t, host, id, state)

	// A signal from another entity can only be checked when it's dispatched.
	require.NoError(t, c.SignalEntity(ctx, api.NewEntityID("blob", "sender"), "send", api.WithSignalInput("a")))
	require.Eventually(t, func() bool { return strings.Contains(logs.String(), "dropped entity signal") },
		10*time.Second, 10*time.Millisecond)
	assert.Contains(t, logs.String(), `operation \"set\" on entity \"@blob@a\" has a 2097154-byte input`)

	require.NoError(t, c.SignalEntity(ctx, id, "set", api.WithSignalInput("small")))
	waitForBlob(t, host, id, "small")
}

func TestOversizedEntityBatchResultFailsCalls(t *testing.T) {
	var executions atomic.Int32
	r := task.NewTaskRegistry()
	require.NoError(t, r.AddEntityN("large", func(ctx *task.EntityContext) (any, error) {
		executions.Add(1)
		if err := ctx.SetState(executions.Load()); err != nil {
			return nil, err
		}
		return strings.Repeat("x", 1<<20), nil
	}))
	id := api.NewEntityID("large", "a")
	require.NoError(t, r.AddOrchestratorN("CallsLarge", func(ctx *task.OrchestrationContext) (any, error) {
		// The messages are queued together, so the entity runs them in one batch
		// whose combined result exceeds the message limit.
		if err := ctx.SignalEntity(id, "get"); err != nil {
			return nil, err
		}
		var calls []task.Task
		for range 5 {
			calls = append(calls, ctx.CallEntity(id, "get"))
		}
		var failures []*api.FailureDetails
		for _, call := range calls {
			var failed *task.EntityOperationFailedError
			if err := call.Await(nil); !errors.As(err, &failed) {
				return nil, fmt.Errorf("expected an entity operation failure, got %w", err)
			}
			failures = append(failures, failed.FailureDetails)
		}
		return failures, nil
	}))
	var logs lockedBuffer
	host := durabletasktest.New(t, r, durabletasktest.WithLogger(api.NewSlogLogger(slog.New(slog.NewTextHandler(&logs, nil)))))

	metadata := run(t, host, "CallsLarge")
	require.Equal(t, api.RUNTIME_STATUS_COMPLETED, metadata.RuntimeStatus)
	var failures []api.FailureDetails
	require.NoError(t, metadata.ReadOutput(&failures))
	require.Len(t, failures, 5)
	for _, failure := range failures {
		assert.Equal(t, api.ErrorType("EntityResultTooLarge"), failure.ErrorType)
		assert.Contains(t, failure.ErrorMessage, "4194304-byte")
		assert.True(t, failure.NonRetriable())
	}
	assert.Equal(t, int32(6), executions.Load(), "each operation runs once")
	assert.Contains(t, logs.String(), `dropped entity signal`)
	assert.Contains(t, logs.String(), `operation \"get\" on entity \"@large@a\": the `)

	entity, err := host.Client().GetEntity(testContext(t), id)
	require.NoError(t, err)
	assert.Nil(t, entity, "the failed batch doesn't store state")
}
