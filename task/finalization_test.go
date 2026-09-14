package task

import (
	"context"
	"errors"
	"testing"

	"github.com/microsoft/durabletask-go/api"
	"github.com/microsoft/durabletask-go/internal/helpers"
	"github.com/microsoft/durabletask-go/internal/protos"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

type panickingOrchestrationResult struct{}

func (panickingOrchestrationResult) MarshalJSON() ([]byte, error) {
	panic("result serialization panic")
}

func TestOrchestrationTurnContainsFinalizationPanics(t *testing.T) {
	tests := []struct {
		name          string
		orchestrator  Orchestrator
		options       []TaskExecutorOption
		message       string
		providerCalls int
		suspended     bool
	}{
		{
			name: "result marshaler",
			orchestrator: func(*OrchestrationContext) (any, error) {
				return panickingOrchestrationResult{}, nil
			},
			message: "result serialization panic",
		},
		{
			name: "continue as new marshaler",
			orchestrator: func(ctx *OrchestrationContext) (any, error) {
				ctx.ContinueAsNew(panickingOrchestrationResult{}, WithKeepUnprocessedEvents())
				return nil, nil
			},
			message: "result serialization panic",
		},
		{
			name: "converter",
			orchestrator: func(*OrchestrationContext) (any, error) {
				return 1, nil
			},
			options: []TaskExecutorOption{WithDataConverter(panicConverter{})},
			message: "converter panic",
		},
		{
			name: "history limit finalization",
			orchestrator: func(ctx *OrchestrationContext) (any, error) {
				return nil, ctx.WaitForSingleEvent("never", -1).Await(nil)
			},
			options: []TaskExecutorOption{WithOrchestrationOptions(OrchestrationOptions{
				MaxHistoryEvents: 3,
				OnHistoryLimitExceeded: func(HistoryLimitInfo) (any, error) {
					return panickingOrchestrationResult{}, nil
				},
			})},
			message: "result serialization panic",
		},
		{
			name: "suspended history limit finalization",
			orchestrator: func(ctx *OrchestrationContext) (any, error) {
				return nil, ctx.WaitForSingleEvent("never", -1).Await(nil)
			},
			options: []TaskExecutorOption{WithOrchestrationOptions(OrchestrationOptions{
				MaxHistoryEvents: 3,
				OnHistoryLimitExceeded: func(HistoryLimitInfo) (any, error) {
					return panickingOrchestrationResult{}, nil
				},
			})},
			message:   "result serialization panic",
			suspended: true,
		},
		{
			name: "failure properties provider",
			orchestrator: func(ctx *OrchestrationContext) (any, error) {
				ctx.ContinueAsNew("unused", WithKeepUnprocessedEvents())
				return nil, errors.New("handler failed")
			},
			message:       "failure provider panic",
			providerCalls: 1,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			registry := NewTaskRegistry()
			var captured *OrchestrationContext
			require.NoError(t, registry.AddOrchestratorN("finalization", func(ctx *OrchestrationContext) (any, error) {
				captured = ctx
				ctx.CallActivity("pending")
				return test.orchestrator(ctx)
			}))
			providerCalls := 0
			options := append([]TaskExecutorOption(nil), test.options...)
			options = append(options, WithErrorPropertiesProvider(api.ErrorPropertiesProviderFunc(func(error) map[string]any {
				providerCalls++
				panic("failure provider panic")
			})))
			executor := NewTaskExecutor(registry, options...)
			events := []*protos.HistoryEvent{
				helpers.NewOrchestratorStartedEvent(),
				helpers.NewExecutionStartedEvent("finalization", "instance", nil, nil, nil, nil),
			}
			if test.suspended {
				events = append(events, helpers.NewSuspendOrchestrationEvent("pause"))
			}
			events = append(events,
				helpers.NewEventRaisedEvent("unconsumed", wrapperspb.String("1")),
				helpers.NewOrchestratorStartedEvent(),
			)
			var result *ExecutionResults
			require.NotPanics(t, func() {
				var err error
				result, err = executor.ExecuteOrchestrator(context.Background(), "instance", nil, events, supportedEntityParameters())
				require.NoError(t, err)
			})
			require.Equal(t, test.providerCalls, providerCalls)
			require.Nil(t, captured.scheduler)
			require.False(t, captured.continuedAsNew)
			require.Len(t, result.Response.Actions, 2)
			require.NotNil(t, result.Response.Actions[0].GetScheduleTask())
			completed := result.Response.Actions[1].GetCompleteOrchestration()
			require.NotNil(t, completed)
			require.Equal(t, api.RUNTIME_STATUS_FAILED, completed.OrchestrationStatus)
			require.Empty(t, completed.CarryoverEvents)
			details := completed.FailureDetails
			require.NotNil(t, details)
			require.Equal(t, string(api.ErrorTypeOrchestratorPanic), details.ErrorType)
			require.Contains(t, details.ErrorMessage, test.message)
			require.NotEmpty(t, details.GetStackTrace().GetValue())
			require.LessOrEqual(t, len(details.GetStackTrace().GetValue()), 16*1024)
			require.Nil(t, details.InnerFailure)
			require.Empty(t, details.Properties)
		})
	}
}

func TestOrchestrationFinalizationPanicReleasesEntityLocks(t *testing.T) {
	registry := NewTaskRegistry()
	entityID := api.NewEntityID("counter", "locked")
	require.NoError(t, registry.AddOrchestratorN("locked-finalization", func(ctx *OrchestrationContext) (any, error) {
		if _, err := ctx.LockEntities(entityID); err != nil {
			return nil, err
		}
		ctx.CallActivity("pending")
		ctx.ContinueAsNew(panickingOrchestrationResult{}, WithKeepUnprocessedEvents())
		return nil, nil
	}))
	started := helpers.NewOrchestratorStartedEvent()
	executionStarted := helpers.NewExecutionStartedEvent("locked-finalization", "instance", nil, nil, nil, nil)
	first := executeOrchestrationTurn(t, registry, "instance", nil, []*protos.HistoryEvent{started, executionStarted})
	require.Len(t, first.Actions, 1)
	lock := first.Actions[0].GetSendEntityMessage().GetEntityLockRequested()
	require.NotNil(t, lock)

	var response *protos.OrchestratorResponse
	require.NotPanics(t, func() {
		response = executeOrchestrationTurn(t, registry, "instance", []*protos.HistoryEvent{
			started,
			executionStarted,
			{
				EventId:   first.Actions[0].Id,
				Timestamp: started.Timestamp,
				EventType: &protos.HistoryEvent_EntityLockRequested{EntityLockRequested: lock},
			},
		}, []*protos.HistoryEvent{
			{
				EventId:   -1,
				Timestamp: started.Timestamp,
				EventType: &protos.HistoryEvent_EntityLockGranted{
					EntityLockGranted: &protos.EntityLockGrantedEvent{CriticalSectionId: lock.CriticalSectionId},
				},
			},
			helpers.NewEventRaisedEvent("unconsumed", wrapperspb.String("1")),
		})
	})
	require.Len(t, response.Actions, 3)
	require.NotNil(t, response.Actions[0].GetScheduleTask())
	unlock := response.Actions[1].GetSendEntityMessage().GetEntityUnlockSent()
	require.NotNil(t, unlock)
	require.Equal(t, lock.CriticalSectionId, unlock.CriticalSectionId)
	require.Equal(t, entityID.String(), unlock.TargetInstanceId.GetValue())
	completed := response.Actions[2].GetCompleteOrchestration()
	require.NotNil(t, completed)
	require.Equal(t, api.RUNTIME_STATUS_FAILED, completed.OrchestrationStatus)
	require.Empty(t, completed.CarryoverEvents)
}
