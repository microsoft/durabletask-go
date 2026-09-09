package task

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/microsoft/durabletask-go/api"
	"github.com/microsoft/durabletask-go/internal/helpers"
	"github.com/microsoft/durabletask-go/internal/protos"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/durationpb"
)

func Test_ValidateEntityParameters(t *testing.T) {
	supported, err := validateEntityParameters(nil)
	require.NoError(t, err)
	require.False(t, supported)

	supported, err = validateEntityParameters(&protos.OrchestratorEntityParameters{
		EntityMessageReorderWindow: durationpb.New(0),
	})
	require.NoError(t, err)
	require.True(t, supported)

	for name, parameters := range map[string]*protos.OrchestratorEntityParameters{
		"missing-window": {},
		"negative-window": {
			EntityMessageReorderWindow: durationpb.New(-time.Second),
		},
		"positive-window": {
			EntityMessageReorderWindow: durationpb.New(time.Second),
		},
		"malformed-window": {
			EntityMessageReorderWindow: &durationpb.Duration{Seconds: 1, Nanos: -1},
		},
	} {
		t.Run(name, func(t *testing.T) {
			supported, err := validateEntityParameters(parameters)
			require.Error(t, err)
			require.False(t, supported)
			var delayed interface{ WorkItemAbandonDelay() time.Duration }
			require.True(t, errors.As(err, &delayed))
		})
	}
}

func Test_OrchestrationEntitySupportGating(t *testing.T) {
	entityID := api.NewEntityID("counter", "key")
	for _, test := range []struct {
		name         string
		orchestrator Orchestrator
	}{
		{
			name: "call",
			orchestrator: func(ctx *OrchestrationContext) (any, error) {
				return nil, ctx.CallEntity(entityID, "get").Await(nil)
			},
		},
		{
			name: "signal",
			orchestrator: func(ctx *OrchestrationContext) (any, error) {
				return nil, ctx.SignalEntity(entityID, "add")
			},
		},
		{
			name: "lock",
			orchestrator: func(ctx *OrchestrationContext) (any, error) {
				_, err := ctx.LockEntities(entityID)
				return nil, err
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			registry := NewTaskRegistry()
			require.NoError(t, registry.AddOrchestratorN(test.name, test.orchestrator))
			result, err := NewTaskExecutor(registry).ExecuteOrchestrator(
				context.Background(),
				"instance",
				nil,
				[]*protos.HistoryEvent{
					helpers.NewOrchestratorStartedEvent(),
					helpers.NewExecutionStartedEvent(test.name, "instance", nil, nil, nil, nil),
				},
				nil,
			)
			require.NoError(t, err)
			for _, action := range result.Response.Actions {
				require.Nil(t, action.GetSendEntityMessage())
			}
			require.NotNil(t, completionAction(t, result.Response).FailureDetails)
		})
	}
}

func Test_OrchestrationEntitySupportWithZeroWindow(t *testing.T) {
	registry := NewTaskRegistry()
	require.NoError(t, registry.AddOrchestratorN("signal", func(ctx *OrchestrationContext) (any, error) {
		return nil, ctx.SignalEntity(api.NewEntityID("counter", "key"), "add")
	}))
	result, err := NewTaskExecutor(registry).ExecuteOrchestrator(
		context.Background(),
		"instance",
		nil,
		[]*protos.HistoryEvent{
			helpers.NewOrchestratorStartedEvent(),
			helpers.NewExecutionStartedEvent("signal", "instance", nil, nil, nil, nil),
		},
		supportedEntityParameters(),
	)
	require.NoError(t, err)
	require.Len(t, result.Response.Actions, 2)
	require.NotNil(t, result.Response.Actions[0].GetSendEntityMessage())
}
