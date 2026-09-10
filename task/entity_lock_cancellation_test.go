package task

import (
	"errors"
	"testing"

	"github.com/microsoft/durabletask-go/api"
	"github.com/microsoft/durabletask-go/internal/helpers"
	"github.com/microsoft/durabletask-go/internal/protos"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestLockEntitiesCancellationBeforeDispatchReplaysSubOrchestration(t *testing.T) {
	registry := NewTaskRegistry()
	var captured *OrchestrationContext
	var nextGUID string
	require.NoError(t, registry.AddOrchestratorN("cancel-lock-sub", func(ctx *OrchestrationContext) (any, error) {
		captured = ctx
		child, cancel := ctx.WithCancel()
		ctx.Go(func(*OrchestrationContext) { cancel() })
		if release, err := child.LockEntities(api.NewEntityID("counter", "cancel")); !errors.Is(err, ErrTaskCanceled) || release != nil {
			return nil, errors.New("expected canceled acquisition without a release callback")
		}
		nextGUID = ctx.NewGuid()
		if err := ctx.CallSubOrchestrator("child").Await(nil); err != nil {
			return nil, err
		}
		return "done", ctx.WaitForSingleEvent("finish", -1).Await(nil)
	}))
	history := []*protos.HistoryEvent{
		helpers.NewOrchestratorStartedEvent(),
		helpers.NewExecutionStartedEvent("cancel-lock-sub", "instance", nil, nil, nil, nil),
	}
	first := executeOrchestrationTurn(t, registry, "instance", nil, history)
	require.Len(t, first.Actions, 1)
	require.Equal(t, int32(1), first.Actions[0].Id, "cancellation must not recycle the lock action ID")
	sub := first.Actions[0].GetCreateSubOrchestration()
	require.NotNil(t, sub)
	require.Empty(t, captured.pendingEntityTasks)
	require.False(t, captured.IsInCriticalSection())
	require.Equal(t, uint64(2), captured.newGuidCounter)
	expectedGUID := nextGUID

	history = append(history, helpers.NewSubOrchestrationCreatedEvent(1, sub.Name, sub.Version, sub.Input, sub.InstanceId, nil))
	replay := executeOrchestrationTurn(t, registry, "instance", history, nil)
	require.Empty(t, replay.Actions, "the canceled lock must not block the recorded sub-orchestration")
	require.Empty(t, captured.pendingEntityTasks)
	require.False(t, captured.IsInCriticalSection())
	require.Equal(t, expectedGUID, nextGUID)
	require.Equal(t, uint64(2), captured.newGuidCounter, "cancellation must not recycle the lock GUID")

	completed := &protos.HistoryEvent{
		EventId:   -1,
		Timestamp: timestamppb.Now(),
		EventType: &protos.HistoryEvent_SubOrchestrationInstanceCompleted{
			SubOrchestrationInstanceCompleted: &protos.SubOrchestrationInstanceCompletedEvent{TaskScheduledId: 1},
		},
	}
	second := executeOrchestrationTurn(t, registry, "instance", history, []*protos.HistoryEvent{completed})
	require.Empty(t, second.Actions)
	history = append(history, completed)
	third := executeOrchestrationTurn(t, registry, "instance", history, []*protos.HistoryEvent{helpers.NewEventRaisedEvent("finish", nil)})
	require.Len(t, third.Actions, 1)
	require.Equal(t, int32(2), third.Actions[0].Id)
	require.Equal(t, `"done"`, completionResult(t, third))
	require.Equal(t, expectedGUID, nextGUID)
}

func TestLockEntitiesCanceledCommittedRequestReleasesOnLateGrant(t *testing.T) {
	for _, activityWait := range []bool{false, true} {
		name := "event"
		if activityWait {
			name = "activity"
		}
		t.Run(name, func(t *testing.T) {
			registry := NewTaskRegistry()
			entities := []api.EntityID{api.NewEntityID("counter", "a"), api.NewEntityID("counter", "b")}
			var captured *OrchestrationContext
			require.NoError(t, registry.AddOrchestratorN("cancel-committed", func(ctx *OrchestrationContext) (any, error) {
				captured = ctx
				child, cancel := ctx.WithCancel()
				ctx.Go(func(ctx *OrchestrationContext) {
					if err := ctx.WaitForSingleEvent("cancel", -1).Await(nil); err == nil {
						cancel()
					}
				})
				if release, err := child.LockEntities(entities...); !errors.Is(err, ErrTaskCanceled) || release != nil {
					return nil, errors.New("expected canceled acquisition without a release callback")
				}
				if !ctx.IsInCriticalSection() {
					return nil, errors.New("abandoned acquisition must retain its critical section until granted")
				}
				if _, err := ctx.LockEntities(entities...); err == nil {
					return nil, errors.New("a draining request must not allow another acquisition")
				}
				if err := ctx.CallSubOrchestrator("forbidden").Await(nil); err == nil {
					return nil, errors.New("a draining request must not allow sub-orchestrations")
				}
				if err := ctx.CallEntity(entities[0], "get").Await(nil); err == nil {
					return nil, errors.New("a draining request must not allow entity calls")
				}
				var err error
				if activityWait {
					err = ctx.CallActivity("finish").Await(nil)
				} else {
					err = ctx.WaitForSingleEvent("finish", -1).Await(nil)
				}
				if err != nil {
					return nil, err
				}
				return !ctx.IsInCriticalSection(), nil
			}))
			history := []*protos.HistoryEvent{
				helpers.NewOrchestratorStartedEvent(),
				helpers.NewExecutionStartedEvent("cancel-committed", "instance", nil, nil, nil, nil),
			}
			first := executeOrchestrationTurn(t, registry, "instance", nil, history)
			require.Len(t, first.Actions, 1)
			request := first.Actions[0].GetSendEntityMessage().GetEntityLockRequested()
			require.NotNil(t, request)
			history = append(history, lockRequestHistory(first.Actions[0]))
			cancelEvent := helpers.NewEventRaisedEvent("cancel", nil)
			second := executeOrchestrationTurn(t, registry, "instance", history, []*protos.HistoryEvent{cancelEvent})
			require.True(t, captured.criticalSectionAbandoned)
			require.True(t, captured.IsInCriticalSection())
			require.Empty(t, captured.pendingEntityTasks, "cleanup must not rely on the canceled task")
			history = append(history, cancelEvent)
			nextActionID := int32(1)
			finishEvent := helpers.NewEventRaisedEvent("finish", nil)
			if activityWait {
				require.Len(t, second.Actions, 1)
				require.NotNil(t, second.Actions[0].GetScheduleTask())
				history = append(history, helpers.NewTaskScheduledEvent(1, "finish", nil, nil, nil))
				nextActionID++
				finishEvent = helpers.NewTaskCompletedEvent(1, nil)
			} else {
				require.Empty(t, second.Actions, "cancellation must not release before the full grant")
			}

			grant := lockGrantedHistory(request.CriticalSectionId)
			for _, replayGrant := range []bool{false, true} {
				var oldEvents, newEvents []*protos.HistoryEvent
				if replayGrant {
					oldEvents = append(append([]*protos.HistoryEvent(nil), history...), grant, grant)
				} else {
					oldEvents, newEvents = history, []*protos.HistoryEvent{grant, grant}
				}
				third := executeOrchestrationTurn(t, registry, "instance", oldEvents, newEvents)
				require.Len(t, third.Actions, len(entities), "late grant must release once while the parent is still running")
				require.False(t, captured.IsInCriticalSection())
				releasedHistory := append(append([]*protos.HistoryEvent(nil), history...), grant)
				for index, action := range third.Actions {
					require.Equal(t, nextActionID+int32(index), action.Id)
					unlock := action.GetSendEntityMessage().GetEntityUnlockSent()
					require.NotNil(t, unlock)
					require.Equal(t, request.CriticalSectionId, unlock.CriticalSectionId)
					require.Equal(t, entities[index].String(), unlock.TargetInstanceId.GetValue())
					releasedHistory = append(releasedHistory, lockUnlockHistory(action))
				}
				replay := executeOrchestrationTurn(t, registry, "instance", releasedHistory, []*protos.HistoryEvent{grant})
				require.Empty(t, replay.Actions, "recorded unlocks and duplicate grants must replay without new actions")
				last := executeOrchestrationTurn(t, registry, "instance", releasedHistory, []*protos.HistoryEvent{finishEvent})
				require.Len(t, last.Actions, 1)
				require.Equal(t, nextActionID+int32(len(entities)), last.Actions[0].Id)
				require.Equal(t, "true", completionResult(t, last))
			}
		})
	}
}

func TestLockEntitiesCancellationRecognizesUnprocessedRequestMarker(t *testing.T) {
	registry := NewTaskRegistry()
	var captured *OrchestrationContext
	require.NoError(t, registry.AddOrchestratorN("marker-lookahead", func(ctx *OrchestrationContext) (any, error) {
		captured = ctx
		child, cancel := ctx.WithCancel()
		ctx.Go(func(ctx *OrchestrationContext) {
			if err := ctx.WaitForSingleEvent("cancel", -1).Await(nil); err == nil {
				cancel()
			}
		})
		if _, err := child.LockEntities(api.NewEntityID("counter", "marker")); !errors.Is(err, ErrTaskCanceled) {
			return nil, errors.New("expected cancellation")
		}
		return nil, ctx.WaitForSingleEvent("finish", -1).Await(nil)
	}))
	start := []*protos.HistoryEvent{
		helpers.NewOrchestratorStartedEvent(),
		helpers.NewExecutionStartedEvent("marker-lookahead", "instance", nil, nil, nil, nil),
	}
	first := executeOrchestrationTurn(t, registry, "instance", nil, start)
	require.Len(t, first.Actions, 1)
	marker := lockRequestHistory(first.Actions[0])
	cancelEvent := helpers.NewEventRaisedEvent("cancel", nil)
	history := append(append([]*protos.HistoryEvent(nil), start...), cancelEvent, marker)
	for _, split := range []int{0, len(start), len(history) - 1, len(history)} {
		replay := executeOrchestrationTurn(t, registry, "instance", history[:split], history[split:])
		require.Empty(t, replay.Actions, "request marker after cancellation must still match, split=%d", split)
		require.True(t, captured.criticalSectionRequestCommitted)
		require.True(t, captured.criticalSectionAbandoned)
		require.Empty(t, captured.pendingEntityTasks)
	}
}

func TestHistoricalEntityLockRequestMatchesAcquisition(t *testing.T) {
	for _, test := range []struct {
		name      string
		actionID  int32
		sectionID string
		matches   bool
	}{
		{"matching", 3, "current", true},
		{"different action", 4, "current", false},
		{"different section", 3, "previous", false},
	} {
		t.Run(test.name, func(t *testing.T) {
			marker := lockRequestHistory(helpers.NewEntityLockRequestedAction(test.actionID, test.sectionID, "instance", []string{"@counter@a"}))
			for _, old := range []bool{false, true} {
				ctx := newTestOrchestrationContext(NewTaskRegistry(), "instance", nil, nil)
				if old {
					ctx.oldEvents = []*protos.HistoryEvent{marker}
				} else {
					ctx.newEvents = []*protos.HistoryEvent{marker}
				}
				require.Equal(t, test.matches, ctx.hasHistoricalEntityLockRequest(3, "current"))
			}
		})
	}
}

func TestLockEntitiesGrantBeforeAppliedCancellationWins(t *testing.T) {
	registry := NewTaskRegistry()
	require.NoError(t, registry.AddOrchestratorN("grant-before-cancel", func(ctx *OrchestrationContext) (any, error) {
		child, cancel := ctx.WithCancel()
		ctx.Go(func(*OrchestrationContext) {
			cancel()
			if child.scope.isCanceled() {
				panic("cancellation applied before the scheduler boundary")
			}
			// Complete the pending acquisition in the same scheduler step as
			// the cancellation request, before the queued cancellation applies.
			if err := ctx.onEntityLockGranted(&protos.EntityLockGrantedEvent{CriticalSectionId: ctx.criticalSectionID}); err != nil {
				panic(err)
			}
		})
		release, err := child.LockEntities(api.NewEntityID("counter", "granted"))
		if err != nil {
			return nil, err
		}
		if !child.scope.isCanceled() || !ctx.IsInCriticalSection() || ctx.criticalSectionAbandoned {
			return nil, errors.New("completed acquisition must win over applied cancellation")
		}
		release()
		release()
		return "granted", nil
	}))
	response := executeOrchestrationTurn(t, registry, "instance", nil, []*protos.HistoryEvent{
		helpers.NewOrchestratorStartedEvent(),
		helpers.NewExecutionStartedEvent("grant-before-cancel", "instance", nil, nil, nil, nil),
	})
	require.Len(t, response.Actions, 3)
	require.NotNil(t, response.Actions[0].GetSendEntityMessage().GetEntityLockRequested())
	require.NotNil(t, response.Actions[1].GetSendEntityMessage().GetEntityUnlockSent())
	require.Equal(t, `"granted"`, completionResult(t, response))
}

func TestLockEntitiesStaleGrantAndReleaseDoNotAffectNewSection(t *testing.T) {
	registry := NewTaskRegistry()
	var captured *OrchestrationContext
	require.NoError(t, registry.AddOrchestratorN("successive-locks", func(ctx *OrchestrationContext) (any, error) {
		captured = ctx
		entity := api.NewEntityID("counter", "successive")
		oldRelease, err := ctx.LockEntities(entity)
		if err != nil {
			return nil, err
		}
		// Simulate engine-owned cleanup without consuming the user's callback.
		ctx.releaseCriticalSection(ctx.criticalSectionID)
		release, err := ctx.LockEntities(entity)
		if err != nil {
			return nil, err
		}
		defer release()
		oldRelease()
		oldRelease()
		if !ctx.IsInCriticalSection() {
			return nil, errors.New("stale callback released the new critical section")
		}
		if err := ctx.WaitForSingleEvent("finish", -1).Await(nil); err != nil {
			return nil, err
		}
		release()
		release()
		return "done", nil
	}))
	history := []*protos.HistoryEvent{
		helpers.NewOrchestratorStartedEvent(),
		helpers.NewExecutionStartedEvent("successive-locks", "instance", nil, nil, nil, nil),
	}
	first := executeOrchestrationTurn(t, registry, "instance", nil, history)
	require.Len(t, first.Actions, 1)
	oldSectionID := first.Actions[0].GetSendEntityMessage().GetEntityLockRequested().CriticalSectionId
	history = append(history, lockRequestHistory(first.Actions[0]), lockGrantedHistory(oldSectionID))
	second := executeOrchestrationTurn(t, registry, "instance", history, nil)
	require.Len(t, second.Actions, 2)
	require.NotNil(t, second.Actions[0].GetSendEntityMessage().GetEntityUnlockSent())
	nextRequest := second.Actions[1].GetSendEntityMessage().GetEntityLockRequested()
	require.NotNil(t, nextRequest)
	require.NotEqual(t, oldSectionID, nextRequest.CriticalSectionId)
	history = append(history, lockUnlockHistory(second.Actions[0]), lockRequestHistory(second.Actions[1]))

	stale := executeOrchestrationTurn(t, registry, "instance", history, []*protos.HistoryEvent{lockGrantedHistory(oldSectionID)})
	require.Empty(t, stale.Actions)
	require.Equal(t, nextRequest.CriticalSectionId, captured.criticalSectionID)
	require.Nil(t, captured.criticalSectionAvailable, "stale grant must not complete the new acquisition")
	require.Len(t, captured.pendingEntityTasks, 1)

	history = append(history, lockGrantedHistory(nextRequest.CriticalSectionId))
	replay := executeOrchestrationTurn(t, registry, "instance", history, []*protos.HistoryEvent{lockGrantedHistory(oldSectionID)})
	require.Empty(t, replay.Actions, "stale releases and replay teardown must not unlock the new section")
	require.Equal(t, nextRequest.CriticalSectionId, captured.criticalSectionID)
	require.NotNil(t, captured.criticalSectionAvailable)
	last := executeOrchestrationTurn(t, registry, "instance", history, []*protos.HistoryEvent{helpers.NewEventRaisedEvent("finish", nil)})
	require.Len(t, last.Actions, 2)
	require.Equal(t, nextRequest.CriticalSectionId, last.Actions[0].GetSendEntityMessage().GetEntityUnlockSent().CriticalSectionId)
	require.Equal(t, `"done"`, completionResult(t, last))
}

func lockRequestHistory(action *protos.OrchestratorAction) *protos.HistoryEvent {
	return &protos.HistoryEvent{
		EventId:   action.Id,
		Timestamp: timestamppb.Now(),
		EventType: &protos.HistoryEvent_EntityLockRequested{
			EntityLockRequested: action.GetSendEntityMessage().GetEntityLockRequested(),
		},
	}
}

func lockGrantedHistory(criticalSectionID string) *protos.HistoryEvent {
	return &protos.HistoryEvent{
		EventId:   -1,
		Timestamp: timestamppb.Now(),
		EventType: &protos.HistoryEvent_EntityLockGranted{
			EntityLockGranted: &protos.EntityLockGrantedEvent{CriticalSectionId: criticalSectionID},
		},
	}
}

func lockUnlockHistory(action *protos.OrchestratorAction) *protos.HistoryEvent {
	return &protos.HistoryEvent{
		EventId:   action.Id,
		Timestamp: timestamppb.Now(),
		EventType: &protos.HistoryEvent_EntityUnlockSent{
			EntityUnlockSent: action.GetSendEntityMessage().GetEntityUnlockSent(),
		},
	}
}
