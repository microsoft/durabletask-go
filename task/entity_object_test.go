package task

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/microsoft/durabletask-go/internal/protos"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

type objectCounterState struct {
	Value int `json:"value"`
}

func (state *objectCounterState) Read() int {
	return state.Value
}

type objectCounter struct {
	EntityObjectBase[objectCounterState]
	calls      int
	closeCalls *int
}

func (entity *objectCounter) Add(value int) int {
	entity.calls++
	entity.State().Value += value
	return entity.State().Value
}

func (entity *objectCounter) Fail(value int) error {
	entity.calls++
	entity.State().Value += value
	return errors.New("failed")
}

func (entity *objectCounter) Calls() int {
	return entity.calls
}

func (entity *objectCounter) CloseEntityBatch(context.Context) error {
	(*entity.closeCalls)++
	return nil
}

func Test_EntityObjectFactory_StateLifecycleAndDispatch(t *testing.T) {
	registry := NewTaskRegistry()
	factoryCalls := 0
	initializations := 0
	closeCalls := 0
	factory := NewEntityObjectFactory[objectCounterState, *objectCounter](
		func(EntityFactoryContext) (*objectCounter, error) {
			factoryCalls++
			return &objectCounter{closeCalls: &closeCalls}, nil
		},
		WithEntityStateInitializer(func(*EntityContext) (objectCounterState, error) {
			initializations++
			return objectCounterState{Value: 10}, nil
		}),
		WithEntityStateDispatch[objectCounterState](),
	)
	require.NoError(t, registry.AddEntityFactoryN("counter", factory))

	executor := NewTaskExecutor(registry).(EntityExecutor)
	result, err := executor.ExecuteEntity(context.Background(), &protos.EntityBatchRequest{
		InstanceId: "@counter@key",
		Operations: []*protos.OperationRequest{
			{Operation: "Add", Input: wrapperspb.String("1")},
			{Operation: "Fail", Input: wrapperspb.String("100")},
			{Operation: "Read"},
			{Operation: "Calls"},
			{Operation: "delete"},
			{Operation: "Read"},
		},
	})
	require.NoError(t, err)
	require.Equal(t, 1, factoryCalls)
	require.Equal(t, 1, closeCalls)
	require.Equal(t, 2, initializations)
	require.Len(t, result.Results, 6)
	require.Equal(t, "11", result.Results[0].GetSuccess().GetResult().GetValue())
	require.Contains(t, result.Results[1].GetFailure().GetFailureDetails().GetErrorMessage(), "failed")
	require.Equal(t, "11", result.Results[2].GetSuccess().GetResult().GetValue())
	require.Equal(t, "2", result.Results[3].GetSuccess().GetResult().GetValue())
	require.Nil(t, result.Results[4].GetSuccess().GetResult())
	require.Equal(t, "10", result.Results[5].GetSuccess().GetResult().GetValue())
	require.JSONEq(t, `{"value":10}`, result.EntityState.GetValue())
}

func Test_EntityFactory_RunsAfterStateHandshake(t *testing.T) {
	registry := NewTaskRegistry()
	factoryCalls := 0
	require.NoError(t, registry.AddEntityFactoryN("counter", func(EntityFactoryContext) (EntityBatch, error) {
		factoryCalls++
		return EntityBatch{
			Entity: func(*EntityContext) (any, error) { return nil, nil },
		}, nil
	}))
	executor := NewTaskExecutor(registry).(EntityExecutor)

	result, err := executor.ExecuteEntity(context.Background(), &protos.EntityBatchRequest{
		InstanceId: "@counter@key",
		Properties: map[string]*structpb.Value{
			"IncludeState": structpb.NewBoolValue(false),
		},
	})
	require.NoError(t, err)
	require.True(t, result.RequiresState)
	require.Zero(t, factoryCalls)

	_, err = executor.ExecuteEntity(context.Background(), &protos.EntityBatchRequest{
		InstanceId: "@counter@key",
	})
	require.NoError(t, err)
	require.Equal(t, 1, factoryCalls)
}

func Test_EntityFactory_ErrorIsFrameworkFailure(t *testing.T) {
	registry := NewTaskRegistry()
	expected := errors.New("factory failed")
	require.NoError(t, registry.AddEntityFactoryN("counter", func(EntityFactoryContext) (EntityBatch, error) {
		return EntityBatch{}, expected
	}))

	_, err := NewTaskExecutor(registry).(EntityExecutor).ExecuteEntity(
		context.Background(),
		&protos.EntityBatchRequest{InstanceId: "@counter@key"},
	)
	require.ErrorIs(t, err, expected)
	require.ErrorContains(t, err, fmt.Sprintf("failed to create entity %q", "counter"))
}

func Test_EntityFactory_CloseErrorIsFrameworkFailure(t *testing.T) {
	registry := NewTaskRegistry()
	expected := errors.New("close failed")
	require.NoError(t, registry.AddEntityFactoryN("counter", func(EntityFactoryContext) (EntityBatch, error) {
		return EntityBatch{
			Entity: func(*EntityContext) (any, error) { return nil, nil },
			Close:  func(context.Context) error { return expected },
		}, nil
	}))

	_, err := NewTaskExecutor(registry).(EntityExecutor).ExecuteEntity(
		context.Background(),
		&protos.EntityBatchRequest{InstanceId: "@counter@key"},
	)
	require.ErrorIs(t, err, expected)
	require.ErrorContains(t, err, "failed to close entity batch")
}

type pointerObjectState struct{}

type pointerStateObject struct {
	EntityObjectBase[*pointerObjectState]
}

func Test_EntityObjectFactory_RejectsPointerState(t *testing.T) {
	require.PanicsWithValue(
		t,
		"NewEntityObjectFactory does not support pointer state types",
		func() {
			_ = NewEntityObjectFactory[*pointerObjectState, *pointerStateObject](
				func(EntityFactoryContext) (*pointerStateObject, error) {
					return new(pointerStateObject), nil
				},
			)
		},
	)
}

type setupFailureObject struct {
	EntityObjectBase[objectCounterState]
	closeCalls *int
}

func (*setupFailureObject) Operation() {}
func (*setupFailureObject) OPERATION() {}

func (entity *setupFailureObject) CloseEntityBatch(context.Context) error {
	(*entity.closeCalls)++
	return nil
}

// A colliding operation set is a permanent authoring error, so it must fail when
// the factory is created rather than failing every batch as a retriable fault.
func Test_EntityObjectFactory_RejectsCollisionAtConstruction(t *testing.T) {
	closeCalls := 0
	require.PanicsWithValue(
		t,
		"entity object found case-insensitive operation collision between OPERATION and Operation",
		func() {
			_ = NewEntityObjectFactory[objectCounterState, *setupFailureObject](
				func(EntityFactoryContext) (*setupFailureObject, error) {
					return &setupFailureObject{closeCalls: &closeCalls}, nil
				},
			)
		},
	)
	require.Zero(t, closeCalls)
}

// An interface type parameter only reveals its operations once the object
// exists, so that path still reports setup failure per batch and releases it.
func Test_EntityObjectFactory_ClosesAfterDynamicSetupFailure(t *testing.T) {
	closeCalls := 0
	factory := NewEntityObjectFactory[objectCounterState, EntityObjectBinding[objectCounterState]](
		func(EntityFactoryContext) (EntityObjectBinding[objectCounterState], error) {
			return &setupFailureObject{closeCalls: &closeCalls}, nil
		},
	)
	_, err := factory(EntityFactoryContext{Context: context.Background()})
	require.ErrorContains(t, err, "case-insensitive operation collision")
	require.Equal(t, 1, closeCalls)
}

type panicConverter struct{}

func (panicConverter) Serialize(any) (string, error) {
	panic("converter panic")
}

func (panicConverter) Deserialize(string, any) error {
	return nil
}

func Test_EntityFactory_ClosesWhenExecutionPanics(t *testing.T) {
	registry := NewTaskRegistry()
	closeCalls := 0
	require.NoError(t, registry.AddEntityFactoryN("counter", func(EntityFactoryContext) (EntityBatch, error) {
		return EntityBatch{
			Entity: func(*EntityContext) (any, error) { return 1, nil },
			Close: func(context.Context) error {
				closeCalls++
				return nil
			},
		}, nil
	}))
	executor := NewTaskExecutor(registry, WithDataConverter(panicConverter{})).(EntityExecutor)
	require.PanicsWithValue(t, "converter panic", func() {
		_, _ = executor.ExecuteEntity(context.Background(), &protos.EntityBatchRequest{
			InstanceId: "@counter@key",
			Operations: []*protos.OperationRequest{{Operation: "get"}},
		})
	})
	require.Equal(t, 1, closeCalls)
}

func Test_EntitySingleton_CanRunConcurrentBatches(t *testing.T) {
	registry := NewTaskRegistry()
	var calls atomic.Int32
	require.NoError(t, registry.AddEntityN("counter", func(*EntityContext) (any, error) {
		calls.Add(1)
		return nil, nil
	}))
	executor := NewTaskExecutor(registry).(EntityExecutor)

	var wait sync.WaitGroup
	failures := make(chan error, 16)
	for i := 0; i < 16; i++ {
		wait.Add(1)
		go func() {
			defer wait.Done()
			result, err := executor.ExecuteEntity(context.Background(), &protos.EntityBatchRequest{
				InstanceId: "@counter@key",
				Operations: []*protos.OperationRequest{{Operation: "run"}},
			})
			if err != nil {
				failures <- err
			} else if len(result.Results) != 1 {
				failures <- fmt.Errorf("result count = %d, want 1", len(result.Results))
			}
		}()
	}
	wait.Wait()
	close(failures)
	for err := range failures {
		require.NoError(t, err)
	}
	require.EqualValues(t, 16, calls.Load())
}
