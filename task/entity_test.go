package task

import (
	"regexp"
	"testing"

	"github.com/microsoft/durabletask-go/api"
	"github.com/microsoft/durabletask-go/internal/protos"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

func presentEntityPayload(value string) entityPayload {
	return entityPayload{value: []byte(value), present: true}
}

func Test_EntityContext_State(t *testing.T) {
	t.Run("no state initially", func(t *testing.T) {
		ctx := &EntityContext{
			ID:        api.NewEntityID("test", "key1"),
			Operation: "op",
		}
		assert.False(t, ctx.HasState())
		assert.Error(t, ctx.GetState(new(int)))
	})

	t.Run("set and get state", func(t *testing.T) {
		ctx := &EntityContext{
			ID:        api.NewEntityID("test", "key1"),
			Operation: "op",
		}
		require.NoError(t, ctx.SetState(42))
		assert.True(t, ctx.HasState())

		var val int
		require.NoError(t, ctx.GetState(&val))
		assert.Equal(t, 42, val)
	})

	t.Run("delete state with nil", func(t *testing.T) {
		ctx := &EntityContext{
			ID:        api.NewEntityID("test", "key1"),
			Operation: "op",
			state:     entityState{value: []byte("42"), hasValue: true},
		}
		assert.True(t, ctx.HasState())
		require.NoError(t, ctx.SetState(nil))
		assert.False(t, ctx.HasState())
	})

	t.Run("delete state with typed nil", func(t *testing.T) {
		ctx := &EntityContext{
			state: entityState{value: []byte("42"), hasValue: true},
		}
		var state *int
		require.NoError(t, ctx.SetState(state))
		assert.False(t, ctx.HasState())
	})

	t.Run("set struct state", func(t *testing.T) {
		type MyState struct {
			Count int    `json:"count"`
			Name  string `json:"name"`
		}
		ctx := &EntityContext{
			ID:        api.NewEntityID("test", "key1"),
			Operation: "op",
		}
		require.NoError(t, ctx.SetState(MyState{Count: 5, Name: "hello"}))
		assert.True(t, ctx.HasState())

		var result MyState
		require.NoError(t, ctx.GetState(&result))
		assert.Equal(t, 5, result.Count)
		assert.Equal(t, "hello", result.Name)
	})
}

func Test_EntityContext_GetInput(t *testing.T) {
	ctx := &EntityContext{
		ID:        api.NewEntityID("test", "key1"),
		Operation: "op",
		rawInput:  presentEntityPayload(`"hello"`),
	}

	var input string
	require.NoError(t, ctx.GetInput(&input))
	assert.Equal(t, "hello", input)

	require.Error(t, (&EntityContext{}).GetInput(&input))
	require.Error(t, (&EntityContext{
		rawInput: presentEntityPayload(""),
	}).GetInput(&input))
}

func Test_EntityContext_RawState(t *testing.T) {
	ctx := &EntityContext{}
	ctx.SetRawState(`{"version":1}`)
	state, ok := ctx.GetRawState()
	require.True(t, ok)
	assert.Equal(t, `{"version":1}`, state)

	ctx.SetRawState("")
	_, ok = ctx.GetRawState()
	assert.False(t, ok)
}

func Test_EntityContext_SignalEntity(t *testing.T) {
	parentTrace := &protos.TraceContext{
		TraceParent: "00-0123456789abcdef0123456789abcdef-0123456789abcdef-01",
		TraceState:  wrapperspb.String("vendor=value"),
	}
	ctx := &EntityContext{
		ID:          api.NewEntityID("test", "key1"),
		Operation:   "op",
		parentTrace: parentTrace,
	}

	err := ctx.SignalEntity(api.NewEntityID("other", "key2"), "increment", 5)
	require.NoError(t, err)
	require.Len(t, ctx.actions, 1)

	action := ctx.actions[0]
	signal := action.GetSendSignal()
	require.NotNil(t, signal)
	assert.Equal(t, "@other@key2", signal.InstanceId)
	assert.Equal(t, "increment", signal.Name)
	assert.Equal(t, "5", signal.Input.GetValue())
	assert.Equal(t, parentTrace, signal.ParentTraceContext)
	assert.NotSame(t, parentTrace, signal.ParentTraceContext)
}

func Test_EntityContext_TypedNilSignalInputIsAbsent(t *testing.T) {
	ctx := &EntityContext{}
	var input *int
	require.NoError(t, ctx.SignalEntity(api.NewEntityID("other", "key"), "operation", input))
	require.Len(t, ctx.actions, 1)
	require.Nil(t, ctx.actions[0].GetSendSignal().Input)
}

func Test_EntityTypedNilOptionsAreAbsent(t *testing.T) {
	var input *int

	callOptions := new(callEntityOptions)
	require.NoError(t, WithEntityInput(input)(callOptions, api.DefaultDataConverter()))
	require.Nil(t, callOptions.rawInput)

	signalOptions := new(signalEntityOptions)
	require.NoError(t, WithSignalEntityInput(input)(signalOptions, api.DefaultDataConverter()))
	require.Nil(t, signalOptions.rawInput)

	startOptions := new(entityStartOrchestrationOptions)
	require.NoError(t, WithEntityStartOrchestrationInput(input)(startOptions, api.DefaultDataConverter()))
	require.Nil(t, startOptions.rawInput)
}

func Test_EntityContext_SignalEntity_RejectsInvalidEntityID(t *testing.T) {
	ctx := &EntityContext{
		ID:        api.NewEntityID("test", "key1"),
		Operation: "op",
	}

	err := ctx.SignalEntity(api.EntityID{Name: "bad@name", Key: "key2"}, "increment", 5)
	require.Error(t, err)
	require.Empty(t, ctx.actions)
}

func Test_EntityContext_StartNewOrchestration(t *testing.T) {
	parentTrace := &protos.TraceContext{
		TraceParent: "00-0123456789abcdef0123456789abcdef-0123456789abcdef-01",
	}
	ctx := &EntityContext{
		ID:          api.NewEntityID("test", "key1"),
		Operation:   "op",
		parentTrace: parentTrace,
	}

	err := ctx.StartNewOrchestration("MyOrchestrator",
		WithEntityStartOrchestrationInput("hello"),
		WithEntityStartOrchestrationInstanceID("my-instance"),
	)
	require.NoError(t, err)
	require.Len(t, ctx.actions, 1)

	action := ctx.actions[0]
	startOrch := action.GetStartNewOrchestration()
	require.NotNil(t, startOrch)
	assert.Equal(t, "MyOrchestrator", startOrch.Name)
	assert.Equal(t, "my-instance", startOrch.InstanceId)
	assert.Equal(t, `"hello"`, startOrch.Input.GetValue())
	assert.Equal(t, parentTrace, startOrch.ParentTraceContext)
	assert.NotSame(t, parentTrace, startOrch.ParentTraceContext)
}

func Test_EntityContext_StartNewOrchestration_RawInput(t *testing.T) {
	ctx := &EntityContext{
		ID:        api.NewEntityID("test", "key1"),
		Operation: "op",
	}

	err := ctx.StartNewOrchestration("MyOrchestrator",
		WithRawEntityStartOrchestrationInput(`{"hello":"world"}`),
		WithEntityStartOrchestrationInstanceID("my-instance"),
	)
	require.NoError(t, err)
	require.Len(t, ctx.actions, 1)
	assert.Equal(t, `{"hello":"world"}`, ctx.actions[0].GetStartNewOrchestration().Input.GetValue())
}

func Test_EntityContext_StartNewOrchestration_DefaultInstanceID(t *testing.T) {
	ctx := &EntityContext{
		ID:        api.NewEntityID("test", "key1"),
		Operation: "op",
	}

	err := ctx.StartNewOrchestration("MyOrchestrator")
	require.NoError(t, err)
	require.Len(t, ctx.actions, 1)

	startOrch := ctx.actions[0].GetStartNewOrchestration()
	require.NotNil(t, startOrch)
	assert.Regexp(t, regexp.MustCompile("^[a-f0-9]{32}$"), startOrch.InstanceId)
}

func Test_EntityContext_StartNewOrchestration_RejectsEntityInstanceID(t *testing.T) {
	ctx := &EntityContext{
		ID:        api.NewEntityID("test", "key1"),
		Operation: "op",
	}

	err := ctx.StartNewOrchestration("MyOrchestrator", WithEntityStartOrchestrationInstanceID("@counter@key1"))
	require.Error(t, err)
	require.Empty(t, ctx.actions)
}

func Test_EntityRegistry(t *testing.T) {
	r := NewTaskRegistry()

	myEntity := func(ctx *EntityContext) (any, error) { return nil, nil }
	require.NoError(t, r.AddEntityN("counter", myEntity))

	// Duplicate registration should fail
	err := r.AddEntityN("counter", myEntity)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "already registered")

	err = r.AddEntityN("", myEntity)
	require.Error(t, err)

	err = r.AddEntityN("bad@name", myEntity)
	require.Error(t, err)
}
