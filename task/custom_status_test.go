package task

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/microsoft/durabletask-go/api"
	"github.com/microsoft/durabletask-go/internal/helpers"
	"github.com/microsoft/durabletask-go/internal/protos"
	"github.com/stretchr/testify/require"
)

// SetCustomStatus stores an already-serialized status verbatim. The DTS worker
// forwards OrchestratorResponse.CustomStatus straight to the scheduler, so a
// caller that pre-serializes must not get a second round of serialization.
func TestSetCustomStatusStoresRawStringVerbatim(t *testing.T) {
	registry := NewTaskRegistry()
	require.NoError(t, registry.AddOrchestratorN(
		"raw-status",
		func(ctx *OrchestrationContext) (any, error) {
			ctx.SetCustomStatus(`{"stage":"halfway"}`)
			return nil, nil
		},
	))

	results, err := NewTaskExecutor(registry).ExecuteOrchestrator(
		context.Background(),
		"raw-status-instance",
		nil,
		[]*protos.HistoryEvent{
			helpers.NewOrchestratorStartedEvent(),
			helpers.NewExecutionStartedEvent("raw-status", "raw-status-instance", nil, nil, nil, nil),
		}, nil)

	require.NoError(t, err)
	require.Equal(t, `{"stage":"halfway"}`, results.Response.GetCustomStatus().GetValue())
}

// SetCustomStatusValue applies the configured converter, unlike SetCustomStatus.
func TestSetCustomStatusValueSerializesWithConverter(t *testing.T) {
	registry := NewTaskRegistry()
	require.NoError(t, registry.AddOrchestratorN(
		"typed-status",
		func(ctx *OrchestrationContext) (any, error) {
			return nil, ctx.SetCustomStatusValue(map[string]string{"stage": "halfway"})
		},
	))

	results, err := NewTaskExecutor(registry).ExecuteOrchestrator(
		context.Background(),
		"typed-status-instance",
		nil,
		[]*protos.HistoryEvent{
			helpers.NewOrchestratorStartedEvent(),
			helpers.NewExecutionStartedEvent("typed-status", "typed-status-instance", nil, nil, nil, nil),
		}, nil)

	require.NoError(t, err)
	require.JSONEq(t, `{"stage":"halfway"}`, results.Response.GetCustomStatus().GetValue())
}

// WithSubOrchestratorInput serializes through the configured converter, while
// WithRawSubOrchestratorInput passes the string through untouched. Both end up
// on the CreateSubOrchestration action the worker sends to DTS.
func TestSubOrchestratorInputOptionsProduceExpectedWireInput(t *testing.T) {
	type payload struct {
		Value int `json:"value"`
	}

	for _, test := range []struct {
		name      string
		configure func(*OrchestrationContext)
		wantInput string
	}{
		{
			name: "typed input is serialized",
			configure: func(ctx *OrchestrationContext) {
				ctx.CallSubOrchestrator("child", WithSubOrchestratorInput(payload{Value: 42}))
			},
			wantInput: `{"value":42}`,
		},
		{
			name: "raw input is passed through",
			configure: func(ctx *OrchestrationContext) {
				ctx.CallSubOrchestrator("child", WithRawSubOrchestratorInput(`{"value":7}`))
			},
			wantInput: `{"value":7}`,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			registry := NewTaskRegistry()
			require.NoError(t, registry.AddOrchestratorN(
				"parent",
				func(ctx *OrchestrationContext) (any, error) {
					test.configure(ctx)
					return nil, nil
				},
			))

			results, err := NewTaskExecutor(registry).ExecuteOrchestrator(
				context.Background(),
				"parent-instance",
				nil,
				[]*protos.HistoryEvent{
					helpers.NewOrchestratorStartedEvent(),
					helpers.NewExecutionStartedEvent("parent", "parent-instance", nil, nil, nil, nil),
				}, nil)

			require.NoError(t, err)
			created := createSubOrchestrationAction(results.Response.Actions)
			require.NotNil(t, created)
			require.Equal(t, "child", created.GetName())
			require.JSONEq(t, test.wantInput, created.GetInput().GetValue())
		})
	}
}

// A sub-orchestrator input that cannot be serialized must surface as an error on
// the returned task rather than silently scheduling an empty child.
func TestWithSubOrchestratorInputReportsSerializationFailure(t *testing.T) {
	registry := NewTaskRegistry()
	var awaitErr error
	require.NoError(t, registry.AddOrchestratorN(
		"parent",
		func(ctx *OrchestrationContext) (any, error) {
			awaitErr = ctx.CallSubOrchestrator("child", WithSubOrchestratorInput(make(chan int))).Await(nil)
			return nil, nil
		},
	))

	results, err := NewTaskExecutor(registry).ExecuteOrchestrator(
		context.Background(),
		"parent-instance",
		nil,
		[]*protos.HistoryEvent{
			helpers.NewOrchestratorStartedEvent(),
			helpers.NewExecutionStartedEvent("parent", "parent-instance", nil, nil, nil, nil),
		}, nil)

	require.NoError(t, err)
	require.Error(t, awaitErr)
	require.ErrorContains(t, awaitErr, "failed to serialize input")
	require.Nil(t, createSubOrchestrationAction(results.Response.Actions))
}

func createSubOrchestrationAction(
	actions []*protos.OrchestratorAction,
) *protos.CreateSubOrchestrationAction {
	for _, action := range actions {
		if created := action.GetCreateSubOrchestration(); created != nil {
			return created
		}
	}
	return nil
}

// Guards the documented difference between the two custom status setters: the
// raw setter must not double-encode a value the typed setter would encode once.
func TestRawAndTypedCustomStatusDifferForStringValues(t *testing.T) {
	registry := NewTaskRegistry()
	require.NoError(t, registry.AddOrchestratorN(
		"status",
		func(ctx *OrchestrationContext) (any, error) {
			ctx.SetCustomStatus("plain")
			return nil, nil
		},
	))
	require.NoError(t, registry.AddOrchestratorN(
		"typed",
		func(ctx *OrchestrationContext) (any, error) {
			return nil, ctx.SetCustomStatusValue("plain")
		},
	))

	executor := NewTaskExecutor(registry)
	statusOf := func(name string) string {
		results, err := executor.ExecuteOrchestrator(
			context.Background(),
			api.InstanceID(name+"-instance"),
			nil,
			[]*protos.HistoryEvent{
				helpers.NewOrchestratorStartedEvent(),
				helpers.NewExecutionStartedEvent(name, name+"-instance", nil, nil, nil, nil),
			}, nil)

		require.NoError(t, err)
		return results.Response.GetCustomStatus().GetValue()
	}

	require.Equal(t, "plain", statusOf("status"))
	encoded, err := json.Marshal("plain")
	require.NoError(t, err)
	require.Equal(t, string(encoded), statusOf("typed"))
}
