package task

import (
	"math/rand"
	"slices"
	"testing"

	"github.com/microsoft/durabletask-go/api"
	"github.com/microsoft/durabletask-go/internal/helpers"
	"github.com/microsoft/durabletask-go/internal/protos"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestLockEntitiesSortsAndDeduplicatesEveryPermutation(t *testing.T) {
	source := []api.EntityID{
		api.NewEntityID("cart", "z"),
		api.NewEntityID("account", "b"),
		api.NewEntityID("account", "a"),
		api.NewEntityID("cart", "z"),
	}
	expected := []string{"@account@a", "@account@b", "@cart@z"}
	random := rand.New(rand.NewSource(42))

	for iteration := 0; iteration < 100; iteration++ {
		entities := append([]api.EntityID(nil), source...)
		random.Shuffle(len(entities), func(left, right int) {
			entities[left], entities[right] = entities[right], entities[left]
		})
		registry := NewTaskRegistry()
		require.NoError(t, registry.AddOrchestratorN("lock-order", func(ctx *OrchestrationContext) (any, error) {
			_, err := ctx.LockEntities(entities...)
			return nil, err
		}))
		instanceID := api.InstanceID("lock-order-instance")
		result := executeOrchestrationTurn(
			t,
			registry,
			instanceID,
			nil,
			[]*protos.HistoryEvent{
				helpers.NewOrchestratorStartedEvent(),
				helpers.NewExecutionStartedEvent("lock-order", string(instanceID), nil, nil, nil, nil),
			},
		)
		require.Len(t, result.Actions, 1)
		lock := result.Actions[0].GetSendEntityMessage().GetEntityLockRequested()
		require.NotNil(t, lock)
		require.True(t, slices.Equal(expected, lock.LockSet))
		require.Equal(t, int32(0), lock.Position)
	}
}

func TestLockEntitiesCancellationBeforeDispatchDoesNotEmitLock(t *testing.T) {
	entityID := api.NewEntityID("counter", "cancel")
	registry := NewTaskRegistry()
	require.NoError(t, registry.AddOrchestratorN("cancel-lock", func(ctx *OrchestrationContext) (any, error) {
		child, cancel := ctx.WithCancel()
		ctx.Go(func(*OrchestrationContext) {
			cancel()
		})
		_, err := child.LockEntities(entityID)
		return err != nil, nil
	}))
	instanceID := api.InstanceID("cancel-lock-instance")
	result := executeOrchestrationTurn(
		t,
		registry,
		instanceID,
		nil,
		[]*protos.HistoryEvent{
			helpers.NewOrchestratorStartedEvent(),
			helpers.NewExecutionStartedEvent("cancel-lock", string(instanceID), nil, nil, nil, nil),
		},
	)
	for _, action := range result.Actions {
		require.Nil(t, action.GetSendEntityMessage())
	}
}

func TestCallEntityCompletionAfterUnlockDoesNotPanic(t *testing.T) {
	entityID := api.NewEntityID("counter", "unlock-before-await")
	registry := NewTaskRegistry()
	require.NoError(t, registry.AddOrchestratorN("unlock-before-await", func(ctx *OrchestrationContext) (any, error) {
		unlock, err := ctx.LockEntities(entityID)
		if err != nil {
			return nil, err
		}
		call := ctx.CallEntity(entityID, "get")
		unlock()
		return "done", call.Await(nil)
	}))
	instanceID := api.InstanceID("unlock-before-await-instance")
	started := helpers.NewOrchestratorStartedEvent()
	executionStarted := helpers.NewExecutionStartedEvent("unlock-before-await", string(instanceID), nil, nil, nil, nil)
	first := executeOrchestrationTurn(
		t,
		registry,
		instanceID,
		nil,
		[]*protos.HistoryEvent{started, executionStarted},
	)
	require.Len(t, first.Actions, 1)
	lockRequest := first.Actions[0].GetSendEntityMessage().GetEntityLockRequested()
	require.NotNil(t, lockRequest)

	lockHistory := &protos.HistoryEvent{
		EventId:   first.Actions[0].Id,
		Timestamp: timestamppb.Now(),
		EventType: &protos.HistoryEvent_EntityLockRequested{
			EntityLockRequested: lockRequest,
		},
	}
	lockGranted := &protos.HistoryEvent{
		EventId:   -1,
		Timestamp: timestamppb.Now(),
		EventType: &protos.HistoryEvent_EntityLockGranted{
			EntityLockGranted: &protos.EntityLockGrantedEvent{CriticalSectionId: lockRequest.CriticalSectionId},
		},
	}
	secondHistory := []*protos.HistoryEvent{started, executionStarted, lockHistory, lockGranted}
	second := executeOrchestrationTurn(t, registry, instanceID, secondHistory, nil)
	require.Len(t, second.Actions, 2)
	callAction := second.Actions[0].GetSendEntityMessage().GetEntityOperationCalled()
	unlockAction := second.Actions[1].GetSendEntityMessage().GetEntityUnlockSent()
	require.NotNil(t, callAction)
	require.NotNil(t, unlockAction)

	callHistory := &protos.HistoryEvent{
		EventId:   second.Actions[0].Id,
		Timestamp: timestamppb.Now(),
		EventType: &protos.HistoryEvent_EntityOperationCalled{
			EntityOperationCalled: callAction,
		},
	}
	unlockHistory := &protos.HistoryEvent{
		EventId:   second.Actions[1].Id,
		Timestamp: timestamppb.Now(),
		EventType: &protos.HistoryEvent_EntityUnlockSent{
			EntityUnlockSent: unlockAction,
		},
	}
	completed := &protos.HistoryEvent{
		EventId:   -1,
		Timestamp: timestamppb.Now(),
		EventType: &protos.HistoryEvent_EntityOperationCompleted{
			EntityOperationCompleted: &protos.EntityOperationCompletedEvent{RequestId: callAction.RequestId},
		},
	}
	thirdHistory := append(append([]*protos.HistoryEvent(nil), secondHistory...), callHistory, unlockHistory)
	var third *protos.OrchestratorResponse
	require.NotPanics(t, func() {
		third = executeOrchestrationTurn(
			t,
			registry,
			instanceID,
			thirdHistory,
			[]*protos.HistoryEvent{helpers.NewOrchestratorStartedEvent(), completed},
		)
	})
	require.Len(t, third.Actions, 1)
	require.NotNil(t, third.Actions[0].GetCompleteOrchestration())
}
