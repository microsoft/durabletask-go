package inmemory

import (
	"bytes"
	"context"
	"log/slog"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/microsoft/durabletask-go/api"
	"github.com/microsoft/durabletask-go/internal/helpers"
	"github.com/microsoft/durabletask-go/internal/protos"
)

func signalEvent(operation string) *protos.HistoryEvent {
	return &protos.HistoryEvent{EventType: &protos.HistoryEvent_EntityOperationSignaled{
		EntityOperationSignaled: &protos.EntityOperationSignaledEvent{RequestId: newID(), Operation: operation},
	}}
}

func lockRequestEvent(s *Server, owner *instance, criticalSectionID string, lockSet ...string) *protos.HistoryEvent {
	s.lockRequests[criticalSectionID] = owner.executionID
	return &protos.HistoryEvent{EventType: &protos.HistoryEvent_EntityLockRequested{
		EntityLockRequested: &protos.EntityLockRequestedEvent{
			CriticalSectionId: criticalSectionID,
			LockSet:           lockSet,
			ParentInstanceId:  wrapperspb.String(owner.id),
		},
	}}
}

func unlockEvent(owner *instance, criticalSectionID string) *protos.HistoryEvent {
	return &protos.HistoryEvent{EventType: &protos.HistoryEvent_EntityUnlockSent{
		EntityUnlockSent: &protos.EntityUnlockSentEvent{
			CriticalSectionId: criticalSectionID,
			ParentInstanceId:  wrapperspb.String(owner.id),
		},
	}}
}

func TestLockedEntityDispatchesOnlyOwnerCalls(t *testing.T) {
	s := NewServer()
	defer s.Close()
	s.mu.Lock()
	defer s.mu.Unlock()

	owner := s.newInstanceLocked("owner", "orchestrator", nil, nil, nil, nil, nil, nil)
	s.deliverEntityLocked("@counter@a", lockRequestEvent(s, owner, "cs", "@counter@a"))
	s.deliverEntityLocked("@counter@a", signalEvent("outside"))
	assert.Nil(t, s.nextEntityWorkItemLocked(nil, 1), "a signal from outside the critical section must wait")

	s.deliverEntityLocked("@counter@a", &protos.HistoryEvent{EventType: &protos.HistoryEvent_EntityOperationCalled{
		EntityOperationCalled: &protos.EntityOperationCalledEvent{
			RequestId:         newID(),
			Operation:         "inside",
			ParentInstanceId:  wrapperspb.String("owner"),
			ParentExecutionId: wrapperspb.String(owner.executionID),
		},
	}})
	item := s.nextEntityWorkItemLocked(nil, 1)
	require.NotNil(t, item)
	requests := item.GetEntityRequestV2().GetOperationRequests()
	require.Len(t, requests, 1)
	assert.Equal(t, "inside", requests[0].GetEntityOperationCalled().GetOperation())
}

func callEvent(owner *instance, requestID string) *protos.HistoryEvent {
	return &protos.HistoryEvent{EventType: &protos.HistoryEvent_EntityOperationCalled{
		EntityOperationCalled: &protos.EntityOperationCalledEvent{
			RequestId:         requestID,
			Operation:         "get",
			ParentInstanceId:  wrapperspb.String(owner.id),
			ParentExecutionId: wrapperspb.String(owner.executionID),
		},
	}}
}

func TestEntityWorkItemFilters(t *testing.T) {
	tests := []struct {
		name    string
		filters *protos.WorkItemFilters
		want    bool
	}{
		{"nil", nil, true},
		{"other work kinds", &protos.WorkItemFilters{
			Orchestrations: []*protos.OrchestrationFilter{{Name: "orchestration"}},
			Activities:     []*protos.ActivityFilter{{Name: "activity"}},
		}, true},
		{"matching", &protos.WorkItemFilters{Entities: []*protos.EntityFilter{{Name: "Counter"}}}, true},
		{"different", &protos.WorkItemFilters{Entities: []*protos.EntityFilter{{Name: "other"}}}, false},
		{"reject all", &protos.WorkItemFilters{Entities: []*protos.EntityFilter{{Name: helpers.RejectAllWorkItemFilterName}}}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := NewServer()
			defer s.Close()
			s.deliverEntityLocked("@counter@a", signalEvent("add"))
			item := s.nextEntityWorkItemLocked(tt.filters, 1)
			assert.Equal(t, tt.want, item != nil)
		})
	}
}

func TestRewindRetainsEntityResults(t *testing.T) {
	for _, failed := range []bool{false, true} {
		for _, beforeFailure := range []bool{false, true} {
			name := "completed"
			if failed {
				name = "failed"
			}
			if beforeFailure {
				name += "-before-failure"
			} else {
				name += "-after-failure"
			}
			t.Run(name, func(t *testing.T) {
				s := NewServer()
				defer s.Close()
				inst := s.newInstanceLocked("caller", "orchestrator", nil, nil, nil, nil, nil, nil)
				started := inst.pending[0]
				called := callEvent(inst, "request")
				inst.history = []*protos.HistoryEvent{started, called}
				inst.pending = nil
				result := &protos.HistoryEvent{}
				if failed {
					result.EventType = &protos.HistoryEvent_EntityOperationFailed{
						EntityOperationFailed: &protos.EntityOperationFailedEvent{RequestId: "request"},
					}
				} else {
					result.EventType = &protos.HistoryEvent_EntityOperationCompleted{
						EntityOperationCompleted: &protos.EntityOperationCompletedEvent{RequestId: "request"},
					}
				}
				if beforeFailure {
					s.respondToCallerLocked(called.GetEntityOperationCalled(), result)
				}
				s.completeLocked(inst, protos.OrchestrationStatus_ORCHESTRATION_STATUS_FAILED, nil, failureDetails("failure", "boom"))
				if !beforeFailure {
					s.respondToCallerLocked(called.GetEntityOperationCalled(), result)
				}
				require.Equal(t, []*protos.HistoryEvent{result}, inst.pending)
				s.prepareRewindLocked(inst, newRewindEvent(nil, nil))
				renewed := proto.CloneOf(started)
				renewed.GetExecutionStarted().OrchestrationInstance.ExecutionId = wrapperspb.String("renewed")
				s.applyRewindLocked(inst, &protos.RewindOrchestrationAction{NewHistory: []*protos.HistoryEvent{renewed, called}})
				item := s.nextWorkItemLocked(nil, 1, workItemLimits{})
				require.NotNil(t, item)
				require.Contains(t, item.GetOrchestratorRequest().GetNewEvents(), result)

				inst.pending = nil
				s.respondToCallerLocked(called.GetEntityOperationCalled(), result)
				assert.Empty(t, inst.pending, "a stale execution must not receive results")
				inst.status = protos.OrchestrationStatus_ORCHESTRATION_STATUS_COMPLETED
				called.GetEntityOperationCalled().ParentExecutionId = wrapperspb.String("renewed")
				s.respondToCallerLocked(called.GetEntityOperationCalled(), result)
				assert.Empty(t, inst.pending, "a completed instance must not receive results")
			})
		}
	}
}

func TestReplacedOwnerLocksAreOrphaned(t *testing.T) {
	s := NewServer()
	defer s.Close()
	s.mu.Lock()
	old := s.newInstanceLocked("owner", "orchestrator", nil, nil, nil, nil, nil, nil)
	s.deliverEntityLocked("@counter@a", lockRequestEvent(s, old, "old-cs", "@counter@a"))
	require.NotNil(t, s.entities["@counter@a"].lock)

	replacement := s.newInstanceLocked("owner", "orchestrator", nil, nil, nil, nil, nil, nil)
	s.deliverEntityLocked("@counter@a", callEvent(replacement, "call-1"))
	assert.Nil(t, s.nextEntityWorkItemLocked(nil, 1), "the replacement doesn't own the old execution's lock")
	s.mu.Unlock()

	resp, err := s.CleanEntityStorage(context.Background(), &protos.CleanEntityStorageRequest{ReleaseOrphanedLocks: true})
	require.NoError(t, err)
	assert.Equal(t, int32(1), resp.GetOrphanedLocksReleased())

	s.mu.Lock()
	defer s.mu.Unlock()
	assert.Nil(t, s.entities["@counter@a"].lock)
	assert.NotNil(t, s.nextEntityWorkItemLocked(nil, 1), "the replacement's call runs once the old lock is released")
}

func TestRewindRebindsOutstandingEntityCalls(t *testing.T) {
	s := NewServer()
	defer s.Close()
	s.mu.Lock()
	inst := s.newInstanceLocked("caller", "orchestrator", nil, nil, nil, nil, nil, nil)
	s.deliverEntityLocked("@counter@a", lockRequestEvent(s, inst, "cs", "@counter@a", "@counter@b"))
	s.deliverEntityLocked("@counter@a", callEvent(inst, "leased"))
	item := s.nextEntityWorkItemLocked(nil, 1)
	require.NotNil(t, item)
	s.deliverEntityLocked("@counter@a", callEvent(inst, "queued"))

	started := proto.CloneOf(inst.pending[0])
	started.GetExecutionStarted().OrchestrationInstance.ExecutionId = wrapperspb.String("renewed")
	s.applyRewindLocked(inst, &protos.RewindOrchestrationAction{NewHistory: []*protos.HistoryEvent{started}})
	inst.pending = nil
	assert.Equal(t, "renewed", s.entities["@counter@a"].lock.ownerExecutionID)
	assert.Equal(t, "renewed", s.entities["@counter@b"].lock.ownerExecutionID)
	assert.Equal(t, "leased", item.GetEntityRequestV2().GetOperationRequests()[0].GetEntityOperationCalled().GetRequestId())
	assert.NotEqual(t, "renewed", item.GetEntityRequestV2().GetOperationRequests()[0].GetEntityOperationCalled().GetParentExecutionId().GetValue(),
		"do not mutate the dispatched request")
	s.mu.Unlock()

	_, err := s.CompleteEntityTask(context.Background(), &protos.EntityBatchResult{
		CompletionToken: item.GetCompletionToken(),
		Results:         []*protos.OperationResult{{ResultType: &protos.OperationResult_Success{Success: &protos.OperationResultSuccess{}}}},
	})
	require.NoError(t, err)

	s.mu.Lock()
	next := s.nextEntityWorkItemLocked(nil, 2)
	require.NotNil(t, next, "the renewed execution still owns the lock")
	s.mu.Unlock()
	_, err = s.CompleteEntityTask(context.Background(), &protos.EntityBatchResult{
		CompletionToken: next.GetCompletionToken(),
		Results:         []*protos.OperationResult{{ResultType: &protos.OperationResult_Success{Success: &protos.OperationResultSuccess{}}}},
	})
	require.NoError(t, err)

	s.mu.Lock()
	defer s.mu.Unlock()
	var completed []string
	for _, event := range inst.pending {
		if c := event.GetEntityOperationCompleted(); c != nil {
			completed = append(completed, c.GetRequestId())
		}
	}
	assert.Equal(t, []string{"leased", "queued"}, completed, "results reach the renewed execution")
}

func TestQueryEntitiesLastModifiedToIsExclusive(t *testing.T) {
	s := NewServer()
	defer s.Close()
	s.mu.Lock()
	e := s.getOrCreateEntityLocked("@counter@a", "counter")
	e.state = wrapperspb.String("1")
	s.mu.Unlock()

	query := func(to time.Time) int {
		resp, err := s.QueryEntities(context.Background(), &protos.QueryEntitiesRequest{Query: &protos.EntityQuery{
			LastModifiedFrom: timestamppb.New(e.lastModified),
			LastModifiedTo:   timestamppb.New(to),
		}})
		require.NoError(t, err)
		return len(resp.GetEntities())
	}
	assert.Zero(t, query(e.lastModified))
	assert.Equal(t, 1, query(e.lastModified.Add(time.Nanosecond)))
}

func TestCleanEntityStorageReleasesOrphanedLocks(t *testing.T) {
	s := NewServer()
	defer s.Close()
	s.mu.Lock()
	e := s.getOrCreateEntityLocked("@counter@a", "counter")
	e.lock = &entityLock{criticalSectionID: "cs", ownerInstanceID: "gone"}
	e.queue = append(e.queue, signalEvent("add"))
	s.mu.Unlock()

	resp, err := s.CleanEntityStorage(context.Background(), &protos.CleanEntityStorageRequest{
		ReleaseOrphanedLocks: true,
		RemoveEmptyEntities:  true,
	})
	require.NoError(t, err)
	assert.Equal(t, int32(1), resp.GetOrphanedLocksReleased())
	assert.Zero(t, resp.GetEmptyEntitiesRemoved(), "an entity with queued messages isn't empty")

	s.mu.Lock()
	defer s.mu.Unlock()
	assert.Nil(t, e.lock)
	assert.NotNil(t, s.nextEntityWorkItemLocked(nil, 1), "the queued signal is dispatched once the lock is released")
}

func TestLockAcquisitionStopsWhenRequesterEnds(t *testing.T) {
	s := NewServer()
	defer s.Close()
	s.mu.Lock()
	defer s.mu.Unlock()
	lockSet := []string{"@counter@a", "@counter@b", "@counter@c"}

	holder := s.newInstanceLocked("holder", "orchestrator", nil, nil, nil, nil, nil, nil)
	s.deliverEntityLocked("@counter@b", lockRequestEvent(s, holder, "holder-cs", "@counter@b"))
	waiter := s.newInstanceLocked("waiter", "orchestrator", nil, nil, nil, nil, nil, nil)
	s.deliverEntityLocked(lockSet[0], lockRequestEvent(s, waiter, "waiter-cs", lockSet...))
	require.Equal(t, "waiter", s.entities["@counter@a"].lock.ownerInstanceID)
	require.Equal(t, "holder", s.entities["@counter@b"].lock.ownerInstanceID)

	// Terminating the waiter releases its lock set while it still waits for B,
	// so its unlock reaches C before the acquisition does.
	for _, id := range lockSet {
		s.deliverEntityLocked(id, unlockEvent(waiter, "waiter-cs"))
	}
	s.completeLocked(waiter, protos.OrchestrationStatus_ORCHESTRATION_STATUS_TERMINATED, nil, nil)
	s.deliverEntityLocked("@counter@b", unlockEvent(holder, "holder-cs"))

	for _, id := range lockSet {
		e := s.entities[id]
		assert.Nil(t, e.lock, "%s must not stay locked by the terminated execution", id)
		assert.Empty(t, e.queue, "%s has no pending messages", id)
	}
	assert.Empty(t, s.lockRequests)
}

func TestEntityWorkItemsAreBoundedBySize(t *testing.T) {
	s := NewServer()
	defer s.Close()
	s.mu.Lock()
	defer s.mu.Unlock()
	s.maxEntityWorkItemBytes = 1024

	const count = 20
	for i := range count {
		event := signalEvent(strconv.Itoa(i))
		event.GetEntityOperationSignaled().Input = wrapperspb.String(strings.Repeat("x", 200))
		s.deliverEntityLocked("@counter@a", event)
	}
	var operations []string
	for streamID := uint64(1); len(operations) < count; streamID++ {
		item := s.nextEntityWorkItemLocked(nil, streamID)
		require.NotNil(t, item)
		requests := item.GetEntityRequestV2().GetOperationRequests()
		require.NotEmpty(t, requests)
		assert.Less(t, len(requests), count, "a large backlog is split across work items")
		assert.LessOrEqual(t, proto.Size(item), s.maxEntityWorkItemBytes)
		for _, event := range requests {
			operations = append(operations, event.GetEntityOperationSignaled().GetOperation())
		}
		s.mu.Unlock()
		_, err := s.CompleteEntityTask(context.Background(), &protos.EntityBatchResult{CompletionToken: item.GetCompletionToken()})
		s.mu.Lock()
		require.NoError(t, err)
	}
	for i, operation := range operations {
		assert.Equal(t, strconv.Itoa(i), operation, "operations are dispatched in order")
	}
}

func TestOversizedEntityOperationsAreRejected(t *testing.T) {
	var logs bytes.Buffer
	s := NewServer(WithLogger(api.NewSlogLogger(slog.New(slog.NewTextHandler(&logs, nil)))))
	defer s.Close()
	s.mu.Lock()
	defer s.mu.Unlock()
	s.maxEntityWorkItemBytes = 1024

	// The stored state and the signal input each fit within the limit, but not together.
	e := s.getOrCreateEntityLocked("@counter@a", "counter")
	e.state = wrapperspb.String(strings.Repeat("s", 600))
	oversized := signalEvent("oversized")
	oversized.GetEntityOperationSignaled().Input = wrapperspb.String(strings.Repeat("x", 600))
	require.Less(t, operationRequestSize(oversized), s.maxEntityWorkItemBytes)

	caller := s.newInstanceLocked("caller", "orchestrator", nil, nil, nil, nil, nil, nil)
	caller.pending = nil
	call := &protos.HistoryEvent{EventType: &protos.HistoryEvent_EntityOperationCalled{
		EntityOperationCalled: &protos.EntityOperationCalledEvent{
			RequestId:        "call-1",
			Operation:        "oversizedCall",
			Input:            wrapperspb.String(strings.Repeat("x", s.maxEntityWorkItemBytes)),
			ParentInstanceId: wrapperspb.String("caller"),
		},
	}}

	s.deliverEntityLocked("@counter@a", oversized)
	s.deliverEntityLocked("@counter@a", call)
	s.deliverEntityLocked("@counter@a", signalEvent("next"))

	item := s.nextEntityWorkItemLocked(nil, 1)
	require.NotNil(t, item, "the entity keeps making progress after rejecting oversized operations")
	assert.LessOrEqual(t, proto.Size(item), s.maxEntityWorkItemBytes)
	requests := item.GetEntityRequestV2().GetOperationRequests()
	require.Len(t, requests, 1)
	assert.Equal(t, "next", requests[0].GetEntityOperationSignaled().GetOperation())
	assert.Empty(t, s.entities["@counter@a"].queue)

	require.Len(t, caller.pending, 1)
	failed := caller.pending[0].GetEntityOperationFailed()
	require.NotNil(t, failed, "the oversized call fails instead of being dispatched")
	assert.Equal(t, "call-1", failed.GetRequestId())
	assert.Equal(t, "EntityOperationTooLarge", failed.GetFailureDetails().GetErrorType())

	log := logs.String()
	assert.Contains(t, log, "level=WARN", "the dropped signal is logged")
	assert.Contains(t, log, "dropped entity signal "+oversized.GetEntityOperationSignaled().GetRequestId())
	assert.Contains(t, log, `operation \"oversized\" on entity \"@counter@a\" has a 600-byte input and the entity has 600 bytes of state`)
	assert.Contains(t, log, "exceeds the 1024-byte entity work item limit")
	assert.NotContains(t, log, "oversizedCall", "calls fail instead of being logged")
}

func TestReleaseStreamRequeuesEntityWorkItems(t *testing.T) {
	s := NewServer()
	defer s.Close()
	s.mu.Lock()
	s.deliverEntityLocked("@counter@a", signalEvent("first"))
	s.deliverEntityLocked("@counter@a", signalEvent("second"))
	item := s.nextEntityWorkItemLocked(nil, 7)
	require.NotNil(t, item)
	require.Len(t, item.GetEntityRequestV2().GetOperationRequests(), 2)
	s.deliverEntityLocked("@counter@a", signalEvent("third"))
	assert.Nil(t, s.nextEntityWorkItemLocked(nil, 7), "a leased entity isn't dispatched again")
	s.mu.Unlock()

	s.releaseStream(7)

	s.mu.Lock()
	defer s.mu.Unlock()
	item = s.nextEntityWorkItemLocked(nil, 8)
	require.NotNil(t, item)
	var operations []string
	for _, event := range item.GetEntityRequestV2().GetOperationRequests() {
		operations = append(operations, event.GetEntityOperationSignaled().GetOperation())
	}
	assert.Equal(t, []string{"first", "second", "third"}, operations)
}

func TestSignalEntityValidatesRequestID(t *testing.T) {
	s := NewServer()
	defer s.Close()
	signal := func(requestID string) error {
		_, err := s.SignalEntity(context.Background(), &protos.SignalEntityRequest{
			InstanceId: "@counter@a",
			Name:       "add",
			RequestId:  requestID,
		})
		return err
	}
	assert.Equal(t, codes.InvalidArgument, status.Code(signal("bad-id")))
	s.mu.Lock()
	assert.NotContains(t, s.entities, "@counter@a", "a rejected signal isn't enqueued")
	s.mu.Unlock()

	require.NoError(t, signal(""))
	require.NoError(t, signal(newID()))
	s.mu.Lock()
	defer s.mu.Unlock()
	queue := s.entities["@counter@a"].queue
	require.Len(t, queue, 2)
	_, err := uuid.Parse(queue[0].GetEntityOperationSignaled().GetRequestId())
	assert.NoError(t, err, "an omitted request ID is generated")
}

func TestInvalidEntityMessageFailsOrchestration(t *testing.T) {
	tests := map[string]*protos.SendEntityMessageAction{
		"call request ID": {EntityMessageType: &protos.SendEntityMessageAction_EntityOperationCalled{
			EntityOperationCalled: &protos.EntityOperationCalledEvent{
				RequestId: "bad-id", Operation: "get", TargetInstanceId: wrapperspb.String("@counter@a"),
			},
		}},
		"signal request ID": {EntityMessageType: &protos.SendEntityMessageAction_EntityOperationSignaled{
			EntityOperationSignaled: &protos.EntityOperationSignaledEvent{
				Operation: "add", TargetInstanceId: wrapperspb.String("@counter@a"),
			},
		}},
		"call target": {EntityMessageType: &protos.SendEntityMessageAction_EntityOperationCalled{
			EntityOperationCalled: &protos.EntityOperationCalledEvent{
				RequestId: newID(), Operation: "get", TargetInstanceId: wrapperspb.String("counter"),
			},
		}},
		"lock set": {EntityMessageType: &protos.SendEntityMessageAction_EntityLockRequested{
			EntityLockRequested: &protos.EntityLockRequestedEvent{
				CriticalSectionId: newID(), LockSet: []string{"@counter@a", "not-an-entity"},
			},
		}},
	}
	for name, message := range tests {
		t.Run(name, func(t *testing.T) {
			s := NewServer()
			defer s.Close()
			s.mu.Lock()
			defer s.mu.Unlock()
			inst := s.newInstanceLocked("caller", "orchestrator", nil, nil, nil, nil, nil, nil)
			s.applyActionsLocked(inst, []*protos.OrchestratorAction{{
				Id:                     1,
				OrchestratorActionType: &protos.OrchestratorAction_SendEntityMessage{SendEntityMessage: message},
			}})
			assert.Equal(t, protos.OrchestrationStatus_ORCHESTRATION_STATUS_FAILED, inst.status)
			assert.Equal(t, "InvalidEntityMessage", inst.failure.GetErrorType())
			assert.Empty(t, s.entities, "an invalid message isn't delivered")
		})
	}
}

func TestFailedSignalBatchNotifiesWorkItemStreams(t *testing.T) {
	s := NewServer()
	defer s.Close()
	s.mu.Lock()
	s.deliverEntityLocked("@counter@a", signalEvent("add"))
	item := s.nextEntityWorkItemLocked(nil, 1)
	require.NotNil(t, item)
	changed := s.changed
	s.mu.Unlock()

	_, err := s.CompleteEntityTask(context.Background(), &protos.EntityBatchResult{
		CompletionToken: item.GetCompletionToken(),
		FailureDetails:  failureDetails("failure", "boom"),
	})
	require.NoError(t, err)
	select {
	case <-changed:
	default:
		t.Fatal("completing a failed batch must wake streams waiting for a free entity slot")
	}
}

func TestQueryEntitiesResumesAfterLastReturnedEntity(t *testing.T) {
	s := NewServer()
	defer s.Close()
	s.mu.Lock()
	for _, key := range []string{"a", "b", "c"} {
		s.getOrCreateEntityLocked("@counter@"+key, "counter").state = wrapperspb.String("1")
	}
	s.mu.Unlock()

	query := func(token *wrapperspb.StringValue) *protos.QueryEntitiesResponse {
		resp, err := s.QueryEntities(context.Background(), &protos.QueryEntitiesRequest{Query: &protos.EntityQuery{
			PageSize:          wrapperspb.Int32(1),
			ContinuationToken: token,
		}})
		require.NoError(t, err)
		return resp
	}
	ids := func(resp *protos.QueryEntitiesResponse) []string {
		var ids []string
		for _, e := range resp.GetEntities() {
			ids = append(ids, e.GetInstanceId())
		}
		return ids
	}
	first := query(nil)
	require.Equal(t, []string{"@counter@a"}, ids(first))
	s.mu.Lock()
	delete(s.entities, "@counter@a")
	s.mu.Unlock()

	second := query(first.GetContinuationToken())
	assert.Equal(t, []string{"@counter@b"}, ids(second), "removing a returned entity doesn't skip the next one")
	third := query(second.GetContinuationToken())
	assert.Equal(t, []string{"@counter@c"}, ids(third))
	assert.Nil(t, third.GetContinuationToken())

	for _, token := range []string{"1", "not base64!", encodeEntityCursor("not-an-entity")} {
		_, err := s.QueryEntities(context.Background(), &protos.QueryEntitiesRequest{Query: &protos.EntityQuery{
			ContinuationToken: wrapperspb.String(token),
		}})
		assert.Equal(t, codes.InvalidArgument, status.Code(err), "token %q", token)
	}
}
