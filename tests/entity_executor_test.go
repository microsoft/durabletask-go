package tests

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/google/uuid"
	"github.com/microsoft/durabletask-go/api"
	"github.com/microsoft/durabletask-go/internal/protos"
	"github.com/microsoft/durabletask-go/task"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

func newEntityExecutor(r *task.TaskRegistry) task.EntityExecutor {
	return task.NewTaskExecutor(r).(task.EntityExecutor)
}

func Test_Executor_EntityActionsInheritOperationTraceContext(t *testing.T) {
	registry := task.NewTaskRegistry()
	require.NoError(t, registry.AddEntityN("router", func(ctx *task.EntityContext) (any, error) {
		if err := ctx.SignalEntity(api.NewEntityID("target", "one"), "signal", nil); err != nil {
			return nil, err
		}
		return nil, ctx.StartNewOrchestration(
			"child",
			task.WithEntityStartOrchestrationInstanceID("child-instance"),
		)
	}))
	parent := &protos.TraceContext{
		TraceParent: "00-0123456789abcdef0123456789abcdef-0123456789abcdef-01",
		TraceState:  wrapperspb.String("vendor=value"),
	}
	result, err := newEntityExecutor(registry).ExecuteEntity(
		context.Background(),
		&protos.EntityBatchRequest{
			InstanceId: "@router@key",
			Operations: []*protos.OperationRequest{{
				Operation:    "route",
				RequestId:    "request",
				TraceContext: parent,
			}},
		},
	)
	require.NoError(t, err)
	require.Len(t, result.Actions, 2)
	require.Equal(t, parent, result.Actions[0].GetSendSignal().GetParentTraceContext())
	require.Equal(t, parent, result.Actions[1].GetStartNewOrchestration().GetParentTraceContext())
	require.NotSame(t, parent, result.Actions[0].GetSendSignal().GetParentTraceContext())
	require.NotSame(t, parent, result.Actions[1].GetStartNewOrchestration().GetParentTraceContext())
}

func Test_Executor_EntityBasicOperation(t *testing.T) {
	r := task.NewTaskRegistry()
	require.NoError(t, r.AddEntityN("counter", func(ctx *task.EntityContext) (any, error) {
		var count int
		if ctx.HasState() {
			if err := ctx.GetState(&count); err != nil {
				return nil, err
			}
		}

		switch ctx.Operation {
		case "add":
			var amount int
			if err := ctx.GetInput(&amount); err != nil {
				return nil, err
			}
			count += amount
		case "get":
			return count, ctx.SetState(count)
		case "reset":
			count = 0
		}

		if err := ctx.SetState(count); err != nil {
			return nil, err
		}
		return count, nil
	}))

	executor := newEntityExecutor(r)
	entityCtx := context.Background()

	// Test "add" operation with no initial state
	req := &protos.EntityBatchRequest{
		InstanceId: "@counter@myCounter",
		Operations: []*protos.OperationRequest{
			{
				Operation: "add",
				RequestId: "req1",
				Input:     wrapperspb.String("5"),
			},
		},
	}

	result, err := executor.ExecuteEntity(entityCtx, req)
	require.NoError(t, err)
	require.Len(t, result.Results, 1)
	require.NotNil(t, result.Results[0].GetSuccess())
	assert.Equal(t, "5", result.Results[0].GetSuccess().GetResult().GetValue())
	assert.Equal(t, "5", result.EntityState.GetValue())

	// Test "add" again with existing state
	req2 := &protos.EntityBatchRequest{
		InstanceId:  "@counter@myCounter",
		EntityState: result.EntityState,
		Operations: []*protos.OperationRequest{
			{
				Operation: "add",
				RequestId: "req2",
				Input:     wrapperspb.String("3"),
			},
		},
	}

	result2, err := executor.ExecuteEntity(entityCtx, req2)
	require.NoError(t, err)
	require.Len(t, result2.Results, 1)
	require.NotNil(t, result2.Results[0].GetSuccess())
	assert.Equal(t, "8", result2.Results[0].GetSuccess().GetResult().GetValue())
	assert.Equal(t, "8", result2.EntityState.GetValue())

	// Test "get" operation
	req3 := &protos.EntityBatchRequest{
		InstanceId:  "@counter@myCounter",
		EntityState: result2.EntityState,
		Operations: []*protos.OperationRequest{
			{
				Operation: "get",
				RequestId: "req3",
			},
		},
	}

	result3, err := executor.ExecuteEntity(entityCtx, req3)
	require.NoError(t, err)
	require.Len(t, result3.Results, 1)
	require.NotNil(t, result3.Results[0].GetSuccess())
	assert.Equal(t, "8", result3.Results[0].GetSuccess().GetResult().GetValue())
}

func Test_Executor_EntityBatchOperations(t *testing.T) {
	r := task.NewTaskRegistry()
	require.NoError(t, r.AddEntityN("counter", func(ctx *task.EntityContext) (any, error) {
		var count int
		if ctx.HasState() {
			if err := ctx.GetState(&count); err != nil {
				return nil, err
			}
		}

		switch ctx.Operation {
		case "add":
			var amount int
			if err := ctx.GetInput(&amount); err != nil {
				return nil, err
			}
			count += amount
		case "get":
			return count, ctx.SetState(count)
		}

		if err := ctx.SetState(count); err != nil {
			return nil, err
		}
		return count, nil
	}))

	executor := newEntityExecutor(r)
	entityCtx := context.Background()

	// Batch multiple operations
	req := &protos.EntityBatchRequest{
		InstanceId: "@counter@myCounter",
		Operations: []*protos.OperationRequest{
			{Operation: "add", RequestId: "req1", Input: wrapperspb.String("10")},
			{Operation: "add", RequestId: "req2", Input: wrapperspb.String("20")},
			{Operation: "get", RequestId: "req3"},
		},
	}

	result, err := executor.ExecuteEntity(entityCtx, req)
	require.NoError(t, err)
	require.Len(t, result.Results, 3)

	// First add: 0 + 10 = 10
	require.NotNil(t, result.Results[0].GetSuccess())
	assert.Equal(t, "10", result.Results[0].GetSuccess().GetResult().GetValue())

	// Second add: 10 + 20 = 30
	require.NotNil(t, result.Results[1].GetSuccess())
	assert.Equal(t, "30", result.Results[1].GetSuccess().GetResult().GetValue())

	// Get: 30
	require.NotNil(t, result.Results[2].GetSuccess())
	assert.Equal(t, "30", result.Results[2].GetSuccess().GetResult().GetValue())

	// Final state should be 30
	assert.Equal(t, "30", result.EntityState.GetValue())
}

func Test_Executor_EntityOperationError(t *testing.T) {
	r := task.NewTaskRegistry()
	require.NoError(t, r.AddEntityN("faulty", func(ctx *task.EntityContext) (any, error) {
		var count int
		if ctx.HasState() {
			_ = ctx.GetState(&count)
		}

		switch ctx.Operation {
		case "fail":
			return nil, assert.AnError
		case "add":
			count++
			_ = ctx.SetState(count)
			return count, nil
		}
		return nil, nil
	}))

	executor := newEntityExecutor(r)
	entityCtx := context.Background()

	// Batch: add, fail, add — the "fail" operation should not affect state
	req := &protos.EntityBatchRequest{
		InstanceId: "@faulty@key1",
		Operations: []*protos.OperationRequest{
			{Operation: "add", RequestId: "req1"},
			{Operation: "fail", RequestId: "req2"},
			{Operation: "add", RequestId: "req3"},
		},
	}

	result, err := executor.ExecuteEntity(entityCtx, req)
	require.NoError(t, err)
	require.Len(t, result.Results, 3)

	// First add succeeds
	require.NotNil(t, result.Results[0].GetSuccess())
	assert.Equal(t, "1", result.Results[0].GetSuccess().GetResult().GetValue())

	// Second op fails
	require.NotNil(t, result.Results[1].GetFailure())

	// Third add succeeds (state from first op is preserved, failure is rolled back)
	require.NotNil(t, result.Results[2].GetSuccess())
	assert.Equal(t, "2", result.Results[2].GetSuccess().GetResult().GetValue())

	// Final state is 2
	assert.Equal(t, "2", result.EntityState.GetValue())
}

func Test_Executor_EntityPanic(t *testing.T) {
	r := task.NewTaskRegistry()
	require.NoError(t, r.AddEntityN("panicky", func(ctx *task.EntityContext) (any, error) {
		panic("oh no!")
	}))

	executor := newEntityExecutor(r)
	entityCtx := context.Background()

	req := &protos.EntityBatchRequest{
		InstanceId: "@panicky@key1",
		Operations: []*protos.OperationRequest{
			{Operation: "test", RequestId: "req1"},
		},
	}

	result, err := executor.ExecuteEntity(entityCtx, req)
	require.NoError(t, err)
	require.Len(t, result.Results, 1)
	require.NotNil(t, result.Results[0].GetFailure())
	assert.Contains(t, result.Results[0].GetFailure().GetFailureDetails().GetErrorMessage(), "oh no!")
}

type reflectedPanickyEntity struct{}

func (*reflectedPanickyEntity) Explode() {
	panic("reflected boom")
}

func Test_Executor_ReflectedEntityPanicPreservesClassificationAndStack(t *testing.T) {
	registry := task.NewTaskRegistry()
	require.NoError(t, registry.AddEntityN("panicky", task.NewEntityFor[reflectedPanickyEntity]()))

	result, err := newEntityExecutor(registry).ExecuteEntity(context.Background(), &protos.EntityBatchRequest{
		InstanceId: "@panicky@key",
		Operations: []*protos.OperationRequest{{Operation: "Explode"}},
	})
	require.NoError(t, err)
	failure := result.Results[0].GetFailure().GetFailureDetails()
	require.Equal(t, "EntityOperationPanic", failure.ErrorType)
	require.Contains(t, failure.ErrorMessage, "reflected boom")
	require.Contains(t, failure.GetStackTrace().GetValue(), "reflectedPanickyEntity).Explode")
}

func Test_Executor_EntityConversionFailuresStayPerOperation(t *testing.T) {
	t.Run("malformed state", func(t *testing.T) {
		registry := task.NewTaskRegistry()
		require.NoError(t, registry.AddEntityN("counter", func(ctx *task.EntityContext) (any, error) {
			var state int
			if err := ctx.GetState(&state); err != nil {
				return nil, err
			}
			return state, nil
		}))
		result, err := newEntityExecutor(registry).ExecuteEntity(context.Background(), &protos.EntityBatchRequest{
			InstanceId:  "@counter@key",
			EntityState: wrapperspb.String("not-json"),
			Operations:  []*protos.OperationRequest{{Operation: "get"}},
		})
		require.NoError(t, err)
		require.Nil(t, result.FailureDetails)
		require.NotNil(t, result.Results[0].GetFailure())
		require.Equal(t, "not-json", result.EntityState.GetValue())
	})

	t.Run("result serialization", func(t *testing.T) {
		registry := task.NewTaskRegistry()
		require.NoError(t, registry.AddEntityN("counter", func(ctx *task.EntityContext) (any, error) {
			if ctx.Operation == "bad" {
				return make(chan int), nil
			}
			return 1, nil
		}))
		result, err := newEntityExecutor(registry).ExecuteEntity(context.Background(), &protos.EntityBatchRequest{
			InstanceId: "@counter@key",
			Operations: []*protos.OperationRequest{
				{Operation: "bad"},
				{Operation: "good"},
			},
		})
		require.NoError(t, err)
		require.Nil(t, result.FailureDetails)
		require.NotNil(t, result.Results[0].GetFailure())
		require.Equal(t, "1", result.Results[1].GetSuccess().GetResult().GetValue())
	})

	t.Run("typed nil result", func(t *testing.T) {
		registry := task.NewTaskRegistry()
		require.NoError(t, registry.AddEntityN("counter", func(*task.EntityContext) (any, error) {
			var result *int
			return result, nil
		}))
		result, err := newEntityExecutor(registry).ExecuteEntity(context.Background(), &protos.EntityBatchRequest{
			InstanceId: "@counter@key",
			Operations: []*protos.OperationRequest{{Operation: "get"}},
		})
		require.NoError(t, err)
		require.NotNil(t, result.Results[0].GetSuccess())
		require.Nil(t, result.Results[0].GetSuccess().Result)
	})
}

func Test_Executor_EntityNotRegistered(t *testing.T) {
	r := task.NewTaskRegistry()
	executor := newEntityExecutor(r)
	entityCtx := context.Background()

	req := &protos.EntityBatchRequest{
		InstanceId: "@unknown@key1",
		Operations: []*protos.OperationRequest{
			{Operation: "test", RequestId: "req1"},
		},
	}

	result, err := executor.ExecuteEntity(entityCtx, req)
	require.NoError(t, err)
	require.Len(t, result.Results, 1)
	require.NotNil(t, result.Results[0].GetFailure())
	assert.Equal(t, "EntityTaskNotFound", result.Results[0].GetFailure().FailureDetails.ErrorType)
	assert.True(t, result.Results[0].GetFailure().FailureDetails.IsNonRetriable)
}

func Test_Executor_EntitySignalAction(t *testing.T) {
	r := task.NewTaskRegistry()
	require.NoError(t, r.AddEntityN("sender", func(ctx *task.EntityContext) (any, error) {
		return nil, ctx.SignalEntity(api.NewEntityID("receiver", "key2"), "notify", "hello")
	}))

	executor := newEntityExecutor(r)
	entityCtx := context.Background()

	req := &protos.EntityBatchRequest{
		InstanceId: "@sender@key1",
		Operations: []*protos.OperationRequest{
			{Operation: "send", RequestId: "req1"},
		},
	}

	result, err := executor.ExecuteEntity(entityCtx, req)
	require.NoError(t, err)
	require.Len(t, result.Results, 1)
	require.NotNil(t, result.Results[0].GetSuccess())

	// Check that a signal action was emitted
	require.Len(t, result.Actions, 1)
	signal := result.Actions[0].GetSendSignal()
	require.NotNil(t, signal)
	assert.Equal(t, "@receiver@key2", signal.InstanceId)
	assert.Equal(t, "notify", signal.Name)
	assert.Equal(t, `"hello"`, signal.Input.GetValue())
}

func Test_Executor_EntityDeleteState(t *testing.T) {
	r := task.NewTaskRegistry()
	require.NoError(t, r.AddEntityN("deletable", func(ctx *task.EntityContext) (any, error) {
		switch ctx.Operation {
		case "set":
			return nil, ctx.SetState(42)
		case "delete":
			return nil, ctx.SetState(nil)
		}
		return nil, nil
	}))

	executor := newEntityExecutor(r)
	entityCtx := context.Background()

	// First set state
	req := &protos.EntityBatchRequest{
		InstanceId: "@deletable@key1",
		Operations: []*protos.OperationRequest{
			{Operation: "set", RequestId: "req1"},
		},
	}

	result, err := executor.ExecuteEntity(entityCtx, req)
	require.NoError(t, err)
	assert.Equal(t, "42", result.EntityState.GetValue())

	// Then delete state
	req2 := &protos.EntityBatchRequest{
		InstanceId:  "@deletable@key1",
		EntityState: result.EntityState,
		Operations: []*protos.OperationRequest{
			{Operation: "delete", RequestId: "req2"},
		},
	}

	result2, err := executor.ExecuteEntity(entityCtx, req2)
	require.NoError(t, err)
	assert.Nil(t, result2.EntityState)
}

// Tests that state persists correctly across multiple batch requests
func Test_Executor_EntityStatePersistsAcrossBatches(t *testing.T) {
	r := task.NewTaskRegistry()
	require.NoError(t, r.AddEntityN("counter", func(ctx *task.EntityContext) (any, error) {
		var count int
		if ctx.HasState() {
			_ = ctx.GetState(&count)
		}
		switch ctx.Operation {
		case "increment":
			count++
		case "get":
			// no-op
		}
		_ = ctx.SetState(count)
		return count, nil
	}))

	executor := newEntityExecutor(r)
	entityCtx := context.Background()

	// Batch 1: increment 3 times
	req := &protos.EntityBatchRequest{
		InstanceId: "@counter@persist",
		Operations: []*protos.OperationRequest{
			{Operation: "increment", RequestId: "r1"},
			{Operation: "increment", RequestId: "r2"},
			{Operation: "increment", RequestId: "r3"},
		},
	}
	result, err := executor.ExecuteEntity(entityCtx, req)
	require.NoError(t, err)
	require.Len(t, result.Results, 3)
	assert.Equal(t, "3", result.EntityState.GetValue())

	// Batch 2: use state from batch 1, increment 2 more times
	req2 := &protos.EntityBatchRequest{
		InstanceId:  "@counter@persist",
		EntityState: result.EntityState,
		Operations: []*protos.OperationRequest{
			{Operation: "increment", RequestId: "r4"},
			{Operation: "get", RequestId: "r5"},
		},
	}
	result2, err := executor.ExecuteEntity(entityCtx, req2)
	require.NoError(t, err)
	require.Len(t, result2.Results, 2)
	// After 4th increment: 4
	assert.Equal(t, "4", result2.Results[0].GetSuccess().GetResult().GetValue())
	// Get returns 4
	assert.Equal(t, "4", result2.Results[1].GetSuccess().GetResult().GetValue())
	assert.Equal(t, "4", result2.EntityState.GetValue())
}

// When an operation fails, state rolls back to the last successful commit
func Test_Executor_EntityErrorRollbackInBatch(t *testing.T) {
	r := task.NewTaskRegistry()
	require.NoError(t, r.AddEntityN("rollback", func(ctx *task.EntityContext) (any, error) {
		var val int
		if ctx.HasState() {
			_ = ctx.GetState(&val)
		}
		switch ctx.Operation {
		case "set":
			var newVal int
			if err := ctx.GetInput(&newVal); err != nil {
				return nil, err
			}
			val = newVal
			_ = ctx.SetState(val)
			return val, nil
		case "fail_after_set":
			val = 999 // modify state...
			_ = ctx.SetState(val)
			return nil, fmt.Errorf("intentional failure") // ...then fail
		case "get":
			_ = ctx.SetState(val)
			return val, nil
		}
		return nil, fmt.Errorf("unknown op")
	}))

	executor := newEntityExecutor(r)
	entityCtx := context.Background()

	req := &protos.EntityBatchRequest{
		InstanceId: "@rollback@key1",
		Operations: []*protos.OperationRequest{
			{Operation: "set", RequestId: "r1", Input: wrapperspb.String("10")},
			{Operation: "fail_after_set", RequestId: "r2"}, // fails, state should rollback
			{Operation: "get", RequestId: "r3"},
		},
	}

	result, err := executor.ExecuteEntity(entityCtx, req)
	require.NoError(t, err)
	require.Len(t, result.Results, 3)

	// First op succeeds with value 10
	require.NotNil(t, result.Results[0].GetSuccess())
	assert.Equal(t, "10", result.Results[0].GetSuccess().GetResult().GetValue())

	// Second op fails
	require.NotNil(t, result.Results[1].GetFailure())
	assert.Contains(t, result.Results[1].GetFailure().GetFailureDetails().GetErrorMessage(), "intentional failure")

	// Third op sees state 10 (rolled back from 999)
	require.NotNil(t, result.Results[2].GetSuccess())
	assert.Equal(t, "10", result.Results[2].GetSuccess().GetResult().GetValue())

	// Final state is 10 (not 999)
	assert.Equal(t, "10", result.EntityState.GetValue())
}

func Test_Executor_EntityWildcardRegistration(t *testing.T) {
	r := task.NewTaskRegistry()
	// Register a wildcard entity that handles any entity name
	require.NoError(t, r.AddEntityN("*", func(ctx *task.EntityContext) (any, error) {
		return fmt.Sprintf("handled %s on %s", ctx.Operation, ctx.ID.Name), nil
	}))

	executor := newEntityExecutor(r)
	entityCtx := context.Background()

	req := &protos.EntityBatchRequest{
		InstanceId: "@anything@key1",
		Operations: []*protos.OperationRequest{
			{Operation: "test", RequestId: "r1"},
		},
	}

	result, err := executor.ExecuteEntity(entityCtx, req)
	require.NoError(t, err)
	require.Len(t, result.Results, 1)
	require.NotNil(t, result.Results[0].GetSuccess())
	assert.Equal(t, `"handled test on anything"`, result.Results[0].GetSuccess().GetResult().GetValue())
}

func Test_Executor_EntitySignalAndStartOrchestrationActions(t *testing.T) {
	r := task.NewTaskRegistry()
	require.NoError(t, r.AddEntityN("coordinator", func(ctx *task.EntityContext) (any, error) {
		if ctx.Operation == "notify_all" {
			// Signal another entity
			_ = ctx.SignalEntity(api.NewEntityID("worker", "w1"), "process", nil)
			_ = ctx.SignalEntity(api.NewEntityID("worker", "w2"), "process", nil)
			// Start an orchestration
			_ = ctx.StartNewOrchestration("CleanupOrchestrator",
				task.WithEntityStartOrchestrationInstanceID("cleanup-1"),
				task.WithEntityStartOrchestrationInput("batch-42"),
			)
			return "notified", nil
		}
		return nil, nil
	}))

	executor := newEntityExecutor(r)
	entityCtx := context.Background()

	req := &protos.EntityBatchRequest{
		InstanceId: "@coordinator@main",
		Operations: []*protos.OperationRequest{
			{Operation: "notify_all", RequestId: "r1"},
		},
	}

	result, err := executor.ExecuteEntity(entityCtx, req)
	require.NoError(t, err)
	require.Len(t, result.Results, 1)
	require.NotNil(t, result.Results[0].GetSuccess())

	// Should have 3 actions: 2 signals + 1 start orchestration
	require.Len(t, result.Actions, 3)

	signal1 := result.Actions[0].GetSendSignal()
	require.NotNil(t, signal1)
	assert.Equal(t, "@worker@w1", signal1.InstanceId)
	assert.Equal(t, "process", signal1.Name)

	signal2 := result.Actions[1].GetSendSignal()
	require.NotNil(t, signal2)
	assert.Equal(t, "@worker@w2", signal2.InstanceId)

	startOrch := result.Actions[2].GetStartNewOrchestration()
	require.NotNil(t, startOrch)
	assert.Equal(t, "CleanupOrchestrator", startOrch.Name)
	assert.Equal(t, "cleanup-1", startOrch.InstanceId)
	assert.Equal(t, `"batch-42"`, startOrch.Input.GetValue())
}

func Test_Executor_EntityComplexState(t *testing.T) {
	type item struct {
		Name  string `json:"name"`
		Price int    `json:"price"`
	}
	type cart struct {
		Items []item `json:"items"`
		Total int    `json:"total"`
	}

	r := task.NewTaskRegistry()
	require.NoError(t, r.AddEntityN("cart", func(ctx *task.EntityContext) (any, error) {
		var state cart
		if ctx.HasState() {
			_ = ctx.GetState(&state)
		}
		switch ctx.Operation {
		case "add_item":
			var i item
			if err := ctx.GetInput(&i); err != nil {
				return nil, err
			}
			state.Items = append(state.Items, i)
			state.Total += i.Price
			_ = ctx.SetState(state)
			return len(state.Items), nil
		case "get":
			_ = ctx.SetState(state)
			return state, nil
		case "clear":
			_ = ctx.SetState(nil)
			return nil, nil
		}
		return nil, nil
	}))

	executor := newEntityExecutor(r)
	entityCtx := context.Background()

	req := &protos.EntityBatchRequest{
		InstanceId: "@cart@user1",
		Operations: []*protos.OperationRequest{
			{Operation: "add_item", RequestId: "r1", Input: wrapperspb.String(`{"name":"apple","price":3}`)},
			{Operation: "add_item", RequestId: "r2", Input: wrapperspb.String(`{"name":"banana","price":2}`)},
			{Operation: "get", RequestId: "r3"},
		},
	}

	result, err := executor.ExecuteEntity(entityCtx, req)
	require.NoError(t, err)
	require.Len(t, result.Results, 3)

	// 1 item after first add
	assert.Equal(t, "1", result.Results[0].GetSuccess().GetResult().GetValue())
	// 2 items after second add
	assert.Equal(t, "2", result.Results[1].GetSuccess().GetResult().GetValue())
	// Get returns full cart
	getResult := result.Results[2].GetSuccess().GetResult().GetValue()
	assert.Contains(t, getResult, `"apple"`)
	assert.Contains(t, getResult, `"banana"`)
	assert.Contains(t, getResult, `"total":5`)
}

func Test_Executor_EntityEmptyBatch(t *testing.T) {
	r := task.NewTaskRegistry()
	require.NoError(t, r.AddEntityN("noop", func(ctx *task.EntityContext) (any, error) {
		return nil, nil
	}))

	executor := newEntityExecutor(r)
	entityCtx := context.Background()

	req := &protos.EntityBatchRequest{
		InstanceId: "@noop@key1",
		Operations: []*protos.OperationRequest{},
	}

	result, err := executor.ExecuteEntity(entityCtx, req)
	require.NoError(t, err)
	assert.Empty(t, result.Results)
	assert.Empty(t, result.Actions)
}

func Test_Executor_EntityRollbackDiscardsActionsAndReusesDeterministicIDs(t *testing.T) {
	r := task.NewTaskRegistry()
	require.NoError(t, r.AddEntityN("transactional", func(ctx *task.EntityContext) (any, error) {
		target := api.NewEntityID("receiver", ctx.Operation)
		if err := ctx.SignalEntity(target, "notify", nil); err != nil {
			return nil, err
		}
		if ctx.Operation == "fail" {
			return nil, errors.New("rollback")
		}
		return ctx.Operation, nil
	}))
	executor := newEntityExecutor(r)
	request := &protos.EntityBatchRequest{
		InstanceId: "@transactional@key",
		Operations: []*protos.OperationRequest{
			{Operation: "first", RequestId: uuid.NewString()},
			{Operation: "fail", RequestId: uuid.NewString()},
			{Operation: "third", RequestId: uuid.NewString()},
		},
	}
	result, err := executor.ExecuteEntity(context.Background(), request)
	require.NoError(t, err)
	require.Len(t, result.Actions, 2)
	assert.Equal(t, int32(0), result.Actions[0].Id)
	assert.Equal(t, int32(1), result.Actions[1].Id)
	assert.Equal(t, "@receiver@first", result.Actions[0].GetSendSignal().InstanceId)
	assert.Equal(t, "@receiver@third", result.Actions[1].GetSendSignal().InstanceId)
	require.NotNil(t, result.Results[1].GetFailure())
}

func Test_Executor_EntityStartOrchestrationIDIsStableAcrossRetry(t *testing.T) {
	r := task.NewTaskRegistry()
	require.NoError(t, r.AddEntityN("starter", func(ctx *task.EntityContext) (any, error) {
		return nil, ctx.StartNewOrchestration("child")
	}))
	executor := newEntityExecutor(r)
	request := &protos.EntityBatchRequest{
		InstanceId: "@starter@key",
		Operations: []*protos.OperationRequest{{
			Operation: "start",
			RequestId: uuid.NewString(),
		}},
	}
	first, err := executor.ExecuteEntity(context.Background(), request)
	require.NoError(t, err)
	second, err := executor.ExecuteEntity(context.Background(), request)
	require.NoError(t, err)
	require.Len(t, first.Actions, 1)
	require.Len(t, second.Actions, 1)
	assert.Equal(
		t,
		first.Actions[0].GetStartNewOrchestration().InstanceId,
		second.Actions[0].GetStartNewOrchestration().InstanceId,
	)
}

// The scheduler signals an elided entity state with the "IncludeState" property,
// matching Microsoft.DurableTask's GrpcInstanceRunnerUtils.
func Test_Executor_EntityStateElisionRequestsState(t *testing.T) {
	var invoked bool
	r := task.NewTaskRegistry()
	require.NoError(t, r.AddEntityN("cached", func(*task.EntityContext) (any, error) {
		invoked = true
		return nil, nil
	}))
	executor := newEntityExecutor(r)
	result, err := executor.ExecuteEntity(context.Background(), &protos.EntityBatchRequest{
		InstanceId: "@cached@key",
		Operations: []*protos.OperationRequest{{
			Operation: "get",
			RequestId: uuid.NewString(),
		}},
		Properties: map[string]*structpb.Value{
			"IncludeState": structpb.NewBoolValue(false),
		},
	})
	require.NoError(t, err)
	assert.True(t, result.RequiresState)
	assert.False(t, invoked)
	assert.Empty(t, result.Results)
	assert.Nil(t, result.EntityState)
}

// A state request is driven by the property alone, so an attached state does not
// suppress it and an unregistered entity does not mask it.
func Test_Executor_EntityStateElisionIgnoresAttachedStateAndRegistration(t *testing.T) {
	executor := newEntityExecutor(task.NewTaskRegistry())
	result, err := executor.ExecuteEntity(context.Background(), &protos.EntityBatchRequest{
		InstanceId:  "@unregistered@key",
		EntityState: wrapperspb.String("42"),
		Operations: []*protos.OperationRequest{{
			Operation: "get",
			RequestId: uuid.NewString(),
		}},
		Properties: map[string]*structpb.Value{
			"IncludeState": structpb.NewBoolValue(false),
		},
	})
	require.NoError(t, err)
	assert.True(t, result.RequiresState)
	assert.Empty(t, result.Results)
}

// A missing or non-boolean property means the state was included.
func Test_Executor_EntityStateElisionDefaultsToIncluded(t *testing.T) {
	properties := map[string]map[string]*structpb.Value{
		"absent": nil,
		"wrong-type": {
			"IncludeState": structpb.NewStringValue("false"),
		},
		"misspelled": {
			"includestate": structpb.NewBoolValue(false),
		},
		"true": {
			"IncludeState": structpb.NewBoolValue(true),
		},
	}
	for name, property := range properties {
		t.Run(name, func(t *testing.T) {
			var invoked bool
			r := task.NewTaskRegistry()
			require.NoError(t, r.AddEntityN("cached", func(*task.EntityContext) (any, error) {
				invoked = true
				return nil, nil
			}))
			result, err := newEntityExecutor(r).ExecuteEntity(
				context.Background(),
				&protos.EntityBatchRequest{
					InstanceId: "@cached@key",
					Operations: []*protos.OperationRequest{{
						Operation: "get",
						RequestId: uuid.NewString(),
					}},
					Properties: property,
				},
			)
			require.NoError(t, err)
			assert.False(t, result.RequiresState)
			assert.True(t, invoked)
		})
	}
}
