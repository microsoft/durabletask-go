package task

import (
	"fmt"
	"sync"
	"testing"

	"github.com/microsoft/durabletask-go/api"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type testCounter struct {
	Value int `json:"value"`
}

func Test_EntityDispatcher_ConcurrentFirstUse(t *testing.T) {
	entity := NewEntityFor[testCounter]()
	var wait sync.WaitGroup
	failures := make(chan error, 32)
	for i := 0; i < 32; i++ {
		wait.Add(1)
		go func() {
			defer wait.Done()
			result, err := entity(&EntityContext{
				ID:        api.NewEntityID("counter", "key"),
				Operation: "Add",
				rawInput:  presentEntityPayload("1"),
			})
			if err != nil {
				failures <- err
			} else if result != 1 {
				failures <- fmt.Errorf("result = %v, want 1", result)
			}
		}()
	}
	wait.Wait()
	close(failures)
	for err := range failures {
		require.NoError(t, err)
	}
}

func (c *testCounter) Add(amount int) (any, error) {
	c.Value += amount
	return c.Value, nil
}

func (c *testCounter) Get() (any, error) {
	return c.Value, nil
}

func (c *testCounter) Reset() (any, error) {
	c.Value = 0
	return nil, nil
}

func Test_EntityDispatcher_BasicOperations(t *testing.T) {
	entity := NewEntityFor[testCounter]()

	// Test "Add" operation
	ctx := &EntityContext{
		ID:        api.NewEntityID("counter", "test"),
		Operation: "Add",
		rawInput:  presentEntityPayload("5"),
	}
	result, err := entity(ctx)
	require.NoError(t, err)
	assert.Equal(t, 5, result)
	assert.True(t, ctx.HasState())

	// Verify state was saved
	var state testCounter
	require.NoError(t, ctx.GetState(&state))
	assert.Equal(t, 5, state.Value)
}

func Test_EntityDispatcher_CaseInsensitive(t *testing.T) {
	entity := NewEntityFor[testCounter]()

	ctx := &EntityContext{
		ID:        api.NewEntityID("counter", "test"),
		Operation: "add", // lowercase
		rawInput:  presentEntityPayload("10"),
	}
	result, err := entity(ctx)
	require.NoError(t, err)
	assert.Equal(t, 10, result)
}

func Test_EntityDispatcher_WithExistingState(t *testing.T) {
	entity := NewEntityFor[testCounter]()

	ctx := &EntityContext{
		ID:        api.NewEntityID("counter", "test"),
		Operation: "Add",
		rawInput:  presentEntityPayload("3"),
		state:     entityState{value: []byte(`{"value":7}`), hasValue: true},
	}
	result, err := entity(ctx)
	require.NoError(t, err)
	assert.Equal(t, 10, result)
}

func Test_EntityDispatcher_Get(t *testing.T) {
	entity := NewEntityFor[testCounter]()

	ctx := &EntityContext{
		ID:        api.NewEntityID("counter", "test"),
		Operation: "Get",
		state:     entityState{value: []byte(`{"value":42}`), hasValue: true},
	}
	result, err := entity(ctx)
	require.NoError(t, err)
	assert.Equal(t, 42, result)
}

func Test_EntityDispatcher_Reset(t *testing.T) {
	entity := NewEntityFor[testCounter]()

	ctx := &EntityContext{
		ID:        api.NewEntityID("counter", "test"),
		Operation: "Reset",
		state:     entityState{value: []byte(`{"value":42}`), hasValue: true},
	}
	_, err := entity(ctx)
	require.NoError(t, err)

	var state testCounter
	require.NoError(t, ctx.GetState(&state))
	assert.Equal(t, 0, state.Value)
}

func Test_EntityDispatcher_ImplicitDelete(t *testing.T) {
	entity := NewEntityFor[testCounter]()

	ctx := &EntityContext{
		ID:        api.NewEntityID("counter", "test"),
		Operation: "delete",
		state:     entityState{value: []byte(`{"value":42}`), hasValue: true},
	}
	_, err := entity(ctx)
	require.NoError(t, err)
	assert.False(t, ctx.HasState())
}

func Test_EntityDispatcher_UnknownOperation(t *testing.T) {
	entity := NewEntityFor[testCounter]()

	ctx := &EntityContext{
		ID:        api.NewEntityID("counter", "test"),
		Operation: "unknown",
	}
	_, err := entity(ctx)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "does not support operation")
}

// Test with EntityContext parameter
type contextAwareEntity struct {
	LastOp string `json:"lastOp"`
}

func (e *contextAwareEntity) Info(ctx *EntityContext) (any, error) {
	e.LastOp = ctx.Operation
	return ctx.ID.String(), nil
}

func Test_EntityDispatcher_WithEntityContext(t *testing.T) {
	entity := NewEntityFor[contextAwareEntity]()

	ctx := &EntityContext{
		ID:        api.NewEntityID("myentity", "key1"),
		Operation: "Info",
	}
	result, err := entity(ctx)
	require.NoError(t, err)
	assert.Equal(t, "@myentity@key1", result)

	var state contextAwareEntity
	require.NoError(t, ctx.GetState(&state))
	assert.Equal(t, "Info", state.LastOp)
}

// OperationNotSupported_Fails: tests rejection of non-existent methods
func Test_EntityDispatcher_OperationNotSupported(t *testing.T) {
	entity := NewEntityFor[testCounter]()

	tests := []struct {
		name string
		op   string
	}{
		{"non-existent method", "doesNotExist"},
		{"special chars", "add!"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := &EntityContext{
				ID:        api.NewEntityID("counter", "k"),
				Operation: tt.op,
			}
			_, err := entity(ctx)
			require.Error(t, err)
			assert.Contains(t, err.Error(), "does not support operation")
		})
	}
}

// Add_Success with case-insensitive matching (lowercase, uppercase, mixed)
func Test_EntityDispatcher_CaseInsensitiveMethodMatching(t *testing.T) {
	entity := NewEntityFor[testCounter]()

	cases := []string{"add", "Add", "ADD", "aDd"}
	for _, op := range cases {
		t.Run(op, func(t *testing.T) {
			ctx := &EntityContext{
				ID:        api.NewEntityID("counter", "k"),
				Operation: op,
				rawInput:  presentEntityPayload("7"),
			}
			result, err := entity(ctx)
			require.NoError(t, err)
			assert.Equal(t, 7, result)
		})
	}
}

// Get_Success: state retrieval from existing state
func Test_EntityDispatcher_GetFromExistingState(t *testing.T) {
	entity := NewEntityFor[testCounter]()

	ctx := &EntityContext{
		ID:        api.NewEntityID("counter", "k"),
		Operation: "Get",
		state:     entityState{value: []byte(`{"value":42}`), hasValue: true},
	}
	result, err := entity(ctx)
	require.NoError(t, err)
	assert.Equal(t, 42, result)
}

func Test_EntityDispatcher_MissingRequiredInputFails(t *testing.T) {
	entity := NewEntityFor[testCounter]()

	ctx := &EntityContext{
		ID:        api.NewEntityID("counter", "k"),
		Operation: "Add",
	}
	_, err := entity(ctx)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "expected an input value")
}

// ImplicitDelete_ClearsState: default delete operation clears state
func Test_EntityDispatcher_ImplicitDeleteClearsState(t *testing.T) {
	entity := NewEntityFor[testCounter]()

	// "delete" and "Delete" both work (case-insensitive)
	for _, op := range []string{"delete", "Delete", "DELETE"} {
		t.Run(op, func(t *testing.T) {
			ctx := &EntityContext{
				ID:        api.NewEntityID("counter", "k"),
				Operation: op,
				state:     entityState{value: []byte(`{"value":42}`), hasValue: true},
			}
			_, err := entity(ctx)
			require.NoError(t, err)
			assert.False(t, ctx.HasState(), "state should be cleared after delete")
		})
	}
}

// ExplicitDelete_Overridden: custom delete method takes precedence
type entityWithDelete struct {
	Value   int  `json:"value"`
	Deleted bool `json:"deleted"`
}

func (e *entityWithDelete) Delete() (any, error) {
	e.Deleted = true
	return "custom delete", nil
}

func (e *entityWithDelete) Get() (any, error) {
	return e.Value, nil
}

func Test_EntityDispatcher_ExplicitDeleteOverridesImplicit(t *testing.T) {
	entity := NewEntityFor[entityWithDelete]()

	ctx := &EntityContext{
		ID:        api.NewEntityID("e", "k"),
		Operation: "delete",
		state:     entityState{value: []byte(`{"value":42,"deleted":false}`), hasValue: true},
	}
	result, err := entity(ctx)
	require.NoError(t, err)
	assert.Equal(t, "custom delete", result)
	// State should still exist (not implicitly cleared) since custom Delete ran
	assert.True(t, ctx.HasState())

	var state entityWithDelete
	require.NoError(t, ctx.GetState(&state))
	assert.True(t, state.Deleted)
	assert.Equal(t, 42, state.Value)
}

type entityWithContextDelete struct {
	Value   int  `json:"value"`
	Deleted bool `json:"deleted"`
}

func (e *entityWithContextDelete) Delete(ctx *EntityContext) (any, error) {
	e.Deleted = true
	return "deleted via ctx", ctx.SetState(nil)
}

func Test_EntityDispatcher_ExplicitContextDeleteCanClearState(t *testing.T) {
	entity := NewEntityFor[entityWithContextDelete]()

	ctx := &EntityContext{
		ID:        api.NewEntityID("e", "k"),
		Operation: "delete",
		state:     entityState{value: []byte(`{"value":42,"deleted":false}`), hasValue: true},
	}
	result, err := entity(ctx)
	require.NoError(t, err)
	assert.Equal(t, "deleted via ctx", result)
	assert.False(t, ctx.HasState())
}

// Throws_ExceptionPreserved: error propagation from entity methods
type errorEntity struct{}

func (e *errorEntity) Fail() (any, error) {
	return nil, fmt.Errorf("entity operation failed: %w", assert.AnError)
}

func (e *errorEntity) Get() (any, error) {
	return "ok", nil
}

func Test_EntityDispatcher_ErrorPreserved(t *testing.T) {
	entity := NewEntityFor[errorEntity]()

	ctx := &EntityContext{
		ID:        api.NewEntityID("e", "k"),
		Operation: "Fail",
	}
	_, err := entity(ctx)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "entity operation failed")
	assert.ErrorIs(t, err, assert.AnError)
}

// State machine pattern: complex state transitions (ported from ExportJobTests)
type jobState struct {
	Status string `json:"status"`
	Name   string `json:"name"`
}

func (j *jobState) Create(name string) (any, error) {
	if j.Status != "" {
		return nil, fmt.Errorf("job already exists with status %s", j.Status)
	}
	j.Status = "active"
	j.Name = name
	return j.Status, nil
}

func (j *jobState) Complete() (any, error) {
	if j.Status != "active" {
		return nil, fmt.Errorf("invalid transition: cannot complete job with status %s", j.Status)
	}
	j.Status = "completed"
	return j.Status, nil
}

func (j *jobState) Fail() (any, error) {
	if j.Status != "active" {
		return nil, fmt.Errorf("invalid transition: cannot fail job with status %s", j.Status)
	}
	j.Status = "failed"
	return j.Status, nil
}

func (j *jobState) Get() (any, error) {
	return *j, nil
}

func Test_EntityDispatcher_StateMachine(t *testing.T) {
	entity := NewEntityFor[jobState]()

	t.Run("create then complete", func(t *testing.T) {
		ctx := &EntityContext{
			ID: api.NewEntityID("job", "1"), Operation: "Create",
			rawInput: presentEntityPayload(`"myJob"`),
		}
		result, err := entity(ctx)
		require.NoError(t, err)
		assert.Equal(t, "active", result)

		ctx2 := &EntityContext{
			ID: api.NewEntityID("job", "1"), Operation: "Complete",
			state: ctx.state,
		}
		result2, err := entity(ctx2)
		require.NoError(t, err)
		assert.Equal(t, "completed", result2)
	})

	t.Run("complete without create fails", func(t *testing.T) {
		ctx := &EntityContext{
			ID: api.NewEntityID("job", "2"), Operation: "Complete",
		}
		_, err := entity(ctx)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "invalid transition")
	})

	t.Run("create twice fails", func(t *testing.T) {
		ctx := &EntityContext{
			ID: api.NewEntityID("job", "3"), Operation: "Create",
			rawInput: presentEntityPayload(`"first"`),
		}
		_, err := entity(ctx)
		require.NoError(t, err)

		ctx2 := &EntityContext{
			ID: api.NewEntityID("job", "3"), Operation: "Create",
			rawInput: presentEntityPayload(`"second"`), state: ctx.state,
		}
		_, err = entity(ctx2)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "already exists")
	})

	t.Run("create then fail then recreate", func(t *testing.T) {
		ctx := &EntityContext{
			ID: api.NewEntityID("job", "4"), Operation: "Create",
			rawInput: presentEntityPayload(`"v1"`),
		}
		_, err := entity(ctx)
		require.NoError(t, err)

		ctx2 := &EntityContext{
			ID: api.NewEntityID("job", "4"), Operation: "Fail",
			state: ctx.state,
		}
		_, err = entity(ctx2)
		require.NoError(t, err)

		// After failure, delete state and re-create
		ctx3 := &EntityContext{
			ID: api.NewEntityID("job", "4"), Operation: "delete",
			state: ctx2.state,
		}
		_, err = entity(ctx3)
		require.NoError(t, err)
		assert.False(t, ctx3.HasState())

		ctx4 := &EntityContext{
			ID: api.NewEntityID("job", "4"), Operation: "Create",
			rawInput: presentEntityPayload(`"v2"`),
		}
		result, err := entity(ctx4)
		require.NoError(t, err)
		assert.Equal(t, "active", result)
	})
}

// Multiple return type support
type multiReturnEntity struct{}

func (e *multiReturnEntity) NoReturn() {
	// void method
}

func (e *multiReturnEntity) ErrorOnly() error {
	return nil
}

func (e *multiReturnEntity) ErrorOnlyFail() error {
	return fmt.Errorf("error only")
}

func (e *multiReturnEntity) ResultOnly() any {
	return 42
}

func Test_EntityDispatcher_ReturnTypeVariations(t *testing.T) {
	entity := NewEntityFor[multiReturnEntity]()

	t.Run("void return", func(t *testing.T) {
		ctx := &EntityContext{ID: api.NewEntityID("e", "k"), Operation: "NoReturn"}
		result, err := entity(ctx)
		require.NoError(t, err)
		assert.Nil(t, result)
	})

	t.Run("error-only success", func(t *testing.T) {
		ctx := &EntityContext{ID: api.NewEntityID("e", "k"), Operation: "ErrorOnly"}
		result, err := entity(ctx)
		require.NoError(t, err)
		assert.Nil(t, result)
	})

	t.Run("error-only failure", func(t *testing.T) {
		ctx := &EntityContext{ID: api.NewEntityID("e", "k"), Operation: "ErrorOnlyFail"}
		_, err := entity(ctx)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "error only")
	})

	t.Run("result-only", func(t *testing.T) {
		ctx := &EntityContext{ID: api.NewEntityID("e", "k"), Operation: "ResultOnly"}
		result, err := entity(ctx)
		require.NoError(t, err)
		assert.Equal(t, 42, result)
	})
}

// Entity with context + input parameter binding
type fullBindingEntity struct {
	Log []string `json:"log"`
}

func (e *fullBindingEntity) Process(ctx *EntityContext, msg string) (any, error) {
	entry := fmt.Sprintf("%s:%s:%s", ctx.ID.String(), ctx.Operation, msg)
	e.Log = append(e.Log, entry)
	return len(e.Log), nil
}

func Test_EntityDispatcher_ContextAndInputBinding(t *testing.T) {
	entity := NewEntityFor[fullBindingEntity]()

	ctx := &EntityContext{
		ID: api.NewEntityID("logger", "main"), Operation: "Process",
		rawInput: presentEntityPayload(`"hello world"`),
	}
	result, err := entity(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, result)

	var state fullBindingEntity
	require.NoError(t, ctx.GetState(&state))
	require.Len(t, state.Log, 1)
	assert.Equal(t, "@logger@main:Process:hello world", state.Log[0])
}

type explicitStateEntity struct {
	Value int    `json:"value"`
	Mode  string `json:"mode,omitempty"`
}

func (e *explicitStateEntity) Replace(ctx *EntityContext, value int) (any, error) {
	e.Value = value
	e.Mode = "receiver"
	return value, ctx.SetState(explicitStateEntity{Value: value * 2, Mode: "context"})
}

func Test_EntityDispatcher_ExplicitContextStateWins(t *testing.T) {
	entity := NewEntityFor[explicitStateEntity]()

	ctx := &EntityContext{
		ID:        api.NewEntityID("explicit", "k"),
		Operation: "Replace",
		rawInput:  presentEntityPayload("5"),
	}
	result, err := entity(ctx)
	require.NoError(t, err)
	assert.Equal(t, 5, result)

	var state explicitStateEntity
	require.NoError(t, ctx.GetState(&state))
	assert.Equal(t, explicitStateEntity{Value: 10, Mode: "context"}, state)
}

func Test_EntityDispatcher_RejectsPointerStateType(t *testing.T) {
	assert.PanicsWithValue(t, "NewEntityFor does not support pointer state types", func() {
		_ = NewEntityFor[*testCounter]()
	})
}

func Test_EntityDispatcher_ZeroValueInitialization(t *testing.T) {
	entity := NewEntityFor[testCounter]()

	// No initial state — should start with zero-value testCounter{Value: 0}
	ctx := &EntityContext{
		ID: api.NewEntityID("counter", "new"), Operation: "Get",
	}
	result, err := entity(ctx)
	require.NoError(t, err)
	assert.Equal(t, 0, result) // zero-value int
}

type invalidSignatureEntity struct{}

func (*invalidSignatureEntity) TooManyInputs(int, string) {}

func (*invalidSignatureEntity) WrongSecondReturn() (int, string) {
	return 0, ""
}

func (*invalidSignatureEntity) Panics() {
	panic("boom")
}

func Test_EntityDispatcher_InvalidSignaturesReturnErrors(t *testing.T) {
	entity := NewEntityFor[invalidSignatureEntity]()
	for _, operation := range []string{"TooManyInputs", "WrongSecondReturn"} {
		t.Run(operation, func(t *testing.T) {
			ctx := &EntityContext{ID: api.NewEntityID("invalid", "key"), Operation: operation}
			assert.NotPanics(t, func() {
				_, err := entity(ctx)
				require.Error(t, err)
			})
		})
	}
}

func Test_EntityDispatcher_UserPanicEscapesReflection(t *testing.T) {
	entity := NewEntityFor[invalidSignatureEntity]()
	ctx := &EntityContext{ID: api.NewEntityID("invalid", "key"), Operation: "Panics"}
	assert.PanicsWithValue(t, "boom", func() {
		_, _ = entity(ctx)
	})
}

type optionalInputEntity struct {
	Value int `json:"value"`
}

func (entity *optionalInputEntity) Add(input OptionalEntityInput[int]) int {
	entity.Value += input.Or(5)
	return entity.Value
}

func Test_EntityDispatcher_OptionalInput(t *testing.T) {
	entity := NewEntityFor[optionalInputEntity]()

	absent := &EntityContext{ID: api.NewEntityID("optional", "key"), Operation: "Add"}
	result, err := entity(absent)
	require.NoError(t, err)
	require.Equal(t, 5, result)

	present := &EntityContext{
		ID:        api.NewEntityID("optional", "key"),
		Operation: "Add",
		rawInput:  presentEntityPayload("3"),
		state:     absent.state,
	}
	result, err = entity(present)
	require.NoError(t, err)
	require.Equal(t, 8, result)
}

type nullableInputEntity struct{}

func (*nullableInputEntity) IsNil(value *string) bool {
	return value == nil
}

func Test_EntityDispatcher_JSONNullIsPresent(t *testing.T) {
	entity := NewEntityFor[nullableInputEntity]()
	ctx := &EntityContext{
		ID:        api.NewEntityID("nullable", "key"),
		Operation: "IsNil",
		rawInput:  presentEntityPayload("null"),
	}
	result, err := entity(ctx)
	require.NoError(t, err)
	require.Equal(t, true, result)
	require.True(t, ctx.HasInput())
}

type caseCollisionEntity struct{}

func (*caseCollisionEntity) Operation() {}
func (*caseCollisionEntity) OPERATION() {}

func Test_EntityDispatcher_RejectsCaseInsensitiveOperationCollisions(t *testing.T) {
	assert.PanicsWithValue(t, "NewEntityFor found case-insensitive operation collision between OPERATION and Operation", func() {
		_ = NewEntityFor[caseCollisionEntity]()
	})
}

type ignoredSetStateErrorEntity struct {
	Value int `json:"value"`
}

func (entity *ignoredSetStateErrorEntity) Mutate(ctx *EntityContext) int {
	entity.Value++
	_ = ctx.SetState(make(chan int))
	return entity.Value
}

func Test_EntityDispatcher_FailedExplicitStateDoesNotSuppressAutoSave(t *testing.T) {
	entity := NewEntityFor[ignoredSetStateErrorEntity]()
	ctx := &EntityContext{ID: api.NewEntityID("state", "key"), Operation: "Mutate"}
	result, err := entity(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, result)
	var state ignoredSetStateErrorEntity
	require.NoError(t, ctx.GetState(&state))
	require.Equal(t, 1, state.Value)
}
