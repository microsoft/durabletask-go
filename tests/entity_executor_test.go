package tests

import (
	"context"
	"fmt"
	"testing"

	"github.com/microsoft/durabletask-go/api"
	"github.com/microsoft/durabletask-go/backend"
	"github.com/microsoft/durabletask-go/internal/protos"
	"github.com/microsoft/durabletask-go/task"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

func newEntityExecutor(r *task.TaskRegistry) backend.EntityExecutor {
	return task.NewTaskExecutor(r).(backend.EntityExecutor)
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
	iid := api.InstanceID("@counter@myCounter")

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

	result, err := executor.ExecuteEntity(entityCtx, iid, req)
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

	result2, err := executor.ExecuteEntity(entityCtx, iid, req2)
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

	result3, err := executor.ExecuteEntity(entityCtx, iid, req3)
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
	iid := api.InstanceID("@counter@myCounter")

	// Batch multiple operations
	req := &protos.EntityBatchRequest{
		InstanceId: "@counter@myCounter",
		Operations: []*protos.OperationRequest{
			{Operation: "add", RequestId: "req1", Input: wrapperspb.String("10")},
			{Operation: "add", RequestId: "req2", Input: wrapperspb.String("20")},
			{Operation: "get", RequestId: "req3"},
		},
	}

	result, err := executor.ExecuteEntity(entityCtx, iid, req)
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
	iid := api.InstanceID("@faulty@key1")

	// Batch: add, fail, add — the "fail" operation should not affect state
	req := &protos.EntityBatchRequest{
		InstanceId: "@faulty@key1",
		Operations: []*protos.OperationRequest{
			{Operation: "add", RequestId: "req1"},
			{Operation: "fail", RequestId: "req2"},
			{Operation: "add", RequestId: "req3"},
		},
	}

	result, err := executor.ExecuteEntity(entityCtx, iid, req)
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
	iid := api.InstanceID("@panicky@key1")

	req := &protos.EntityBatchRequest{
		InstanceId: "@panicky@key1",
		Operations: []*protos.OperationRequest{
			{Operation: "test", RequestId: "req1"},
		},
	}

	result, err := executor.ExecuteEntity(entityCtx, iid, req)
	require.NoError(t, err)
	require.Len(t, result.Results, 1)
	require.NotNil(t, result.Results[0].GetFailure())
	assert.Contains(t, result.Results[0].GetFailure().GetFailureDetails().GetErrorMessage(), "oh no!")
}

func Test_Executor_EntityNotRegistered(t *testing.T) {
	r := task.NewTaskRegistry()
	executor := newEntityExecutor(r)
	entityCtx := context.Background()
	iid := api.InstanceID("@unknown@key1")

	req := &protos.EntityBatchRequest{
		InstanceId: "@unknown@key1",
		Operations: []*protos.OperationRequest{
			{Operation: "test", RequestId: "req1"},
		},
	}

	result, err := executor.ExecuteEntity(entityCtx, iid, req)
	require.NoError(t, err)
	require.NotNil(t, result.FailureDetails)
	assert.Equal(t, "EntityNotRegistered", result.FailureDetails.ErrorType)
}

func Test_Executor_EntitySignalAction(t *testing.T) {
	r := task.NewTaskRegistry()
	require.NoError(t, r.AddEntityN("sender", func(ctx *task.EntityContext) (any, error) {
		return nil, ctx.SignalEntity(api.NewEntityID("receiver", "key2"), "notify", "hello")
	}))

	executor := newEntityExecutor(r)
	entityCtx := context.Background()
	iid := api.InstanceID("@sender@key1")

	req := &protos.EntityBatchRequest{
		InstanceId: "@sender@key1",
		Operations: []*protos.OperationRequest{
			{Operation: "send", RequestId: "req1"},
		},
	}

	result, err := executor.ExecuteEntity(entityCtx, iid, req)
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
	iid := api.InstanceID("@deletable@key1")

	// First set state
	req := &protos.EntityBatchRequest{
		InstanceId: "@deletable@key1",
		Operations: []*protos.OperationRequest{
			{Operation: "set", RequestId: "req1"},
		},
	}

	result, err := executor.ExecuteEntity(entityCtx, iid, req)
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

	result2, err := executor.ExecuteEntity(entityCtx, iid, req2)
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
	iid := api.InstanceID("@counter@persist")

	// Batch 1: increment 3 times
	req := &protos.EntityBatchRequest{
		InstanceId: "@counter@persist",
		Operations: []*protos.OperationRequest{
			{Operation: "increment", RequestId: "r1"},
			{Operation: "increment", RequestId: "r2"},
			{Operation: "increment", RequestId: "r3"},
		},
	}
	result, err := executor.ExecuteEntity(entityCtx, iid, req)
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
	result2, err := executor.ExecuteEntity(entityCtx, iid, req2)
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
	iid := api.InstanceID("@rollback@key1")

	req := &protos.EntityBatchRequest{
		InstanceId: "@rollback@key1",
		Operations: []*protos.OperationRequest{
			{Operation: "set", RequestId: "r1", Input: wrapperspb.String("10")},
			{Operation: "fail_after_set", RequestId: "r2"}, // fails, state should rollback
			{Operation: "get", RequestId: "r3"},
		},
	}

	result, err := executor.ExecuteEntity(entityCtx, iid, req)
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
	iid := api.InstanceID("@anything@key1")

	req := &protos.EntityBatchRequest{
		InstanceId: "@anything@key1",
		Operations: []*protos.OperationRequest{
			{Operation: "test", RequestId: "r1"},
		},
	}

	result, err := executor.ExecuteEntity(entityCtx, iid, req)
	require.NoError(t, err)
	require.Len(t, result.Results, 1)
	require.NotNil(t, result.Results[0].GetSuccess())
	assert.Equal(t, `"handled test on anything"`, result.Results[0].GetSuccess().GetResult().GetValue())
}

func Test_Executor_EntitySignalAndStartOrchestrationActions(t *testing.T) {
	r := task.NewTaskRegistry()
	require.NoError(t, r.AddEntityN("coordinator", func(ctx *task.EntityContext) (any, error) {
		switch ctx.Operation {
		case "notify_all":
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
	iid := api.InstanceID("@coordinator@main")

	req := &protos.EntityBatchRequest{
		InstanceId: "@coordinator@main",
		Operations: []*protos.OperationRequest{
			{Operation: "notify_all", RequestId: "r1"},
		},
	}

	result, err := executor.ExecuteEntity(entityCtx, iid, req)
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
	iid := api.InstanceID("@cart@user1")

	req := &protos.EntityBatchRequest{
		InstanceId: "@cart@user1",
		Operations: []*protos.OperationRequest{
			{Operation: "add_item", RequestId: "r1", Input: wrapperspb.String(`{"name":"apple","price":3}`)},
			{Operation: "add_item", RequestId: "r2", Input: wrapperspb.String(`{"name":"banana","price":2}`)},
			{Operation: "get", RequestId: "r3"},
		},
	}

	result, err := executor.ExecuteEntity(entityCtx, iid, req)
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
	iid := api.InstanceID("@noop@key1")

	req := &protos.EntityBatchRequest{
		InstanceId: "@noop@key1",
		Operations: []*protos.OperationRequest{},
	}

	result, err := executor.ExecuteEntity(entityCtx, iid, req)
	require.NoError(t, err)
	assert.Empty(t, result.Results)
	assert.Empty(t, result.Actions)
}
