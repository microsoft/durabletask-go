package inmemory

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/microsoft/durabletask-go/internal/protos"
)

func signalEvent(operation string) *protos.HistoryEvent {
	return &protos.HistoryEvent{EventType: &protos.HistoryEvent_EntityOperationSignaled{
		EntityOperationSignaled: &protos.EntityOperationSignaledEvent{RequestId: newID(), Operation: operation},
	}}
}

func TestLockedEntityDispatchesOnlyOwnerCalls(t *testing.T) {
	s := NewServer()
	defer s.Close()
	s.mu.Lock()
	defer s.mu.Unlock()

	s.deliverEntityLocked("@counter@a", &protos.HistoryEvent{EventType: &protos.HistoryEvent_EntityLockRequested{
		EntityLockRequested: &protos.EntityLockRequestedEvent{
			CriticalSectionId: "cs",
			LockSet:           []string{"@counter@a"},
			ParentInstanceId:  wrapperspb.String("owner"),
		},
	}})
	s.deliverEntityLocked("@counter@a", signalEvent("outside"))
	assert.Nil(t, s.nextEntityWorkItemLocked(nil, 1), "a signal from outside the critical section must wait")

	s.deliverEntityLocked("@counter@a", &protos.HistoryEvent{EventType: &protos.HistoryEvent_EntityOperationCalled{
		EntityOperationCalled: &protos.EntityOperationCalledEvent{
			RequestId:        newID(),
			Operation:        "inside",
			ParentInstanceId: wrapperspb.String("owner"),
		},
	}})
	item := s.nextEntityWorkItemLocked(nil, 1)
	require.NotNil(t, item)
	requests := item.GetEntityRequestV2().GetOperationRequests()
	require.Len(t, requests, 1)
	assert.Equal(t, "inside", requests[0].GetEntityOperationCalled().GetOperation())
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
