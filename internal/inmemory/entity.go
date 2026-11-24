package inmemory

import (
	"cmp"
	"context"
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/microsoft/durabletask-go/internal/helpers"
	"github.com/microsoft/durabletask-go/internal/protos"
)

// entity is the stored state of a durable entity.
//
// Messages are processed in arrival order. Lock requests and unlocks are
// handled by the service; signals and calls are dispatched to a worker in
// batches. While an entity is locked by a critical section, only calls from
// the lock owner and the owner's unlock are processed; other messages wait.
type entity struct {
	id           string
	name         string
	state        *wrapperspb.StringValue
	lastModified time.Time
	queue        []*protos.HistoryEvent
	lock         *entityLock
	leased       bool
	queued       bool
}

type entityLock struct {
	criticalSectionID string
	ownerInstanceID   string
	ownerExecutionID  string
}

type entityLease struct {
	entity   *entity
	events   []*protos.HistoryEvent
	streamID uint64
}

// normalizeEntityID validates an "@name@key" entity instance ID and lowercases its name.
func normalizeEntityID(id string) (string, string, bool) {
	name, key, err := helpers.ParseEntityInstanceID(id)
	if err != nil {
		return "", "", false
	}
	return "@" + name + "@" + key, name, true
}

func invalidEntityIDError(id string) error {
	return status.Errorf(codes.InvalidArgument, "invalid entity instance ID %q", id)
}

func (s *Server) getOrCreateEntityLocked(id, name string) *entity {
	if e, ok := s.entities[id]; ok {
		return e
	}
	e := &entity{id: id, name: name, lastModified: time.Now()}
	s.entities[id] = e
	return e
}

// deliverEntityLocked adds a message to an entity's queue, creating the entity if needed.
func (s *Server) deliverEntityLocked(targetID string, event *protos.HistoryEvent) {
	id, name, ok := normalizeEntityID(targetID)
	if !ok {
		return
	}
	e := s.getOrCreateEntityLocked(id, name)
	e.queue = append(e.queue, event)
	s.pumpEntityLocked(e)
}

// newSignalEvent returns the message that delivers a signal to an entity.
func newSignalEvent(signal *protos.EntityOperationSignaledEvent) *protos.HistoryEvent {
	return &protos.HistoryEvent{
		EventId:   -1,
		Timestamp: timestamppb.Now(),
		EventType: &protos.HistoryEvent_EntityOperationSignaled{EntityOperationSignaled: signal},
	}
}

// signalEntityLocked delivers a signal message now or, if it is scheduled for
// later, when it is due.
func (s *Server) signalEntityLocked(targetID string, event *protos.HistoryEvent) {
	if at := event.GetEntityOperationSignaled().GetScheduledTime(); at != nil && at.AsTime().After(time.Now()) {
		s.startServerTimerLocked(at.AsTime(), func() { s.deliverEntityLocked(targetID, event) })
		return
	}
	s.deliverEntityLocked(targetID, event)
}

// startServerTimerLocked runs fn under the server lock at the given time, unless
// the server has been closed.
func (s *Server) startServerTimerLocked(at time.Time, fn func()) {
	var timer *time.Timer
	timer = time.AfterFunc(time.Until(at), func() {
		s.mu.Lock()
		defer s.mu.Unlock()
		s.timers = slices.DeleteFunc(s.timers, func(t *time.Timer) bool { return t == timer })
		if s.closed {
			return
		}
		fn()
		s.notifyLocked()
	})
	s.timers = append(s.timers, timer)
}

// nextMessage returns the index of the next message the entity can
// process, or -1 if none can be processed now.
func (e *entity) nextMessage() int {
	return slices.IndexFunc(e.queue, e.canProcess)
}

// canProcess reports whether the entity can process a message now. While the
// entity is locked, only the owning execution's calls and unlock can be processed.
func (e *entity) canProcess(event *protos.HistoryEvent) bool {
	if e.lock == nil {
		return true
	}
	if called := event.GetEntityOperationCalled(); called != nil {
		return called.GetParentInstanceId().GetValue() == e.lock.ownerInstanceID &&
			called.GetParentExecutionId().GetValue() == e.lock.ownerExecutionID
	}
	if unlock := event.GetEntityUnlockSent(); unlock != nil {
		return unlock.GetCriticalSectionId() == e.lock.criticalSectionID
	}
	return false
}

func isEntityControlMessage(event *protos.HistoryEvent) bool {
	return event.GetEntityLockRequested() != nil || event.GetEntityUnlockSent() != nil
}

// pumpEntityLocked processes lock and unlock messages that are ready and queues
// the entity for dispatch if a signal or call is ready.
func (s *Server) pumpEntityLocked(e *entity) {
	if e.leased || s.entities[e.id] != e {
		return
	}
	for {
		i := e.nextMessage()
		if i < 0 {
			return
		}
		event := e.queue[i]
		if !isEntityControlMessage(event) {
			break
		}
		e.queue = slices.Delete(e.queue, i, i+1)
		s.processEntityControlMessageLocked(e, event)
	}
	if !e.queued {
		e.queued = true
		s.entityQueue = append(s.entityQueue, e)
	}
	s.notifyLocked()
}

func (s *Server) processEntityControlMessageLocked(e *entity, event *protos.HistoryEvent) {
	if unlock := event.GetEntityUnlockSent(); unlock != nil {
		if e.lock != nil && e.lock.criticalSectionID == unlock.GetCriticalSectionId() {
			e.lock = nil
		}
		return
	}
	request := event.GetEntityLockRequested()
	if !s.lockRequestActiveLocked(request) {
		// The requesting execution has ended. Its unlocks may already have
		// passed the entities later in the lock set, so the acquisition must
		// stop here rather than lock entities that will never be released.
		delete(s.lockRequests, request.GetCriticalSectionId())
		return
	}
	e.lock = &entityLock{
		criticalSectionID: request.GetCriticalSectionId(),
		ownerInstanceID:   request.GetParentInstanceId().GetValue(),
		ownerExecutionID:  s.lockRequests[request.GetCriticalSectionId()],
	}
	// Locks are acquired one entity at a time in lock-set order. Because lock
	// sets are sorted, critical sections can't deadlock.
	next := request.GetPosition() + 1
	if int(next) < len(request.GetLockSet()) {
		forwarded := proto.CloneOf(request)
		forwarded.Position = next
		s.deliverEntityLocked(request.GetLockSet()[next], &protos.HistoryEvent{
			EventId:   -1,
			Timestamp: timestamppb.Now(),
			EventType: &protos.HistoryEvent_EntityLockRequested{EntityLockRequested: forwarded},
		})
		return
	}
	delete(s.lockRequests, request.GetCriticalSectionId())
	if owner, ok := s.instances[e.lock.ownerInstanceID]; ok {
		s.deliverLocked(owner, &protos.HistoryEvent{
			EventId:   -1,
			Timestamp: timestamppb.Now(),
			EventType: &protos.HistoryEvent_EntityLockGranted{
				EntityLockGranted: &protos.EntityLockGrantedEvent{CriticalSectionId: request.GetCriticalSectionId()},
			},
		})
	}
}

// lockRequestActiveLocked reports whether the execution that sent a lock
// request is still running.
func (s *Server) lockRequestActiveLocked(request *protos.EntityLockRequestedEvent) bool {
	executionID, ok := s.lockRequests[request.GetCriticalSectionId()]
	if !ok {
		return false
	}
	owner, ok := s.instances[request.GetParentInstanceId().GetValue()]
	return ok && !isTerminal(owner.status) && owner.executionID == executionID
}

// takeBatch removes the signals and calls that are ready to run, stopping at
// the next lock or unlock message or at the first operation that would make
// the work item larger than maxBytes; that operation stays queued for the next
// batch. An operation that can't fit even in a work item with no other
// operations is removed and returned in rejected so that it can't stall the
// entity. baseSize is the work item's size before operations are added, as
// returned by entityWorkItemBaseSize.
func (e *entity) takeBatch(baseSize, maxBytes int) (batch, rejected []*protos.HistoryEvent) {
	var kept []*protos.HistoryEvent
	size := baseSize
	stop := len(e.queue)
	for i, event := range e.queue {
		if !e.canProcess(event) {
			kept = append(kept, event)
			continue
		}
		if isEntityControlMessage(event) {
			stop = i
			break
		}
		opSize := operationRequestSize(event)
		if size+opSize > maxBytes {
			if len(batch) > 0 {
				stop = i
				break
			}
			rejected = append(rejected, event)
			continue
		}
		size += opSize
		batch = append(batch, event)
	}
	if len(batch) > 0 || len(rejected) > 0 {
		e.queue = append(kept, e.queue[stop:]...)
	}
	return batch, rejected
}

// rejectOversizedOperationsLocked fails calls whose request can't fit in an
// entity work item with the entity's state, and logs a warning for each such
// signal, which has no caller to notify. baseSize is the size of the entity's
// work item before operations are added.
func (s *Server) rejectOversizedOperationsLocked(e *entity, events []*protos.HistoryEvent, baseSize int) {
	for _, event := range events {
		message := s.oversizedOperationMessage(e.id, e.state, event, baseSize)
		called := event.GetEntityOperationCalled()
		if called == nil {
			s.reportDroppedSignalLocked(event.GetEntityOperationSignaled(), message)
			continue
		}
		s.respondToCallerLocked(called, &protos.HistoryEvent{EventId: -1, Timestamp: timestamppb.Now(), EventType: &protos.HistoryEvent_EntityOperationFailed{
			EntityOperationFailed: &protos.EntityOperationFailedEvent{
				RequestId:      called.GetRequestId(),
				FailureDetails: failureDetails("EntityOperationTooLarge", "%s", message),
			},
		}})
	}
}

// reportDroppedSignalLocked logs a warning for an entity signal that the
// service drops, which has no caller to notify.
func (s *Server) reportDroppedSignalLocked(signal *protos.EntityOperationSignaledEvent, reason string) {
	s.logger.Warnf("in-memory durable task service dropped entity signal %s: %s", signal.GetRequestId(), reason)
}

// newEntityWorkItem returns a work item for an entity with no operations.
func newEntityWorkItem(entityID string, state *wrapperspb.StringValue, token string) *protos.WorkItem {
	return &protos.WorkItem{
		Request: &protos.WorkItem_EntityRequestV2{EntityRequestV2: &protos.EntityRequest{
			InstanceId:  entityID,
			EntityState: state,
		}},
		CompletionToken: token,
	}
}

// entityWorkItemBaseSize returns the size of an entity work item with no
// operations, plus room for its EntityRequest and WorkItem length prefixes to
// lengthen as operations are added.
func entityWorkItemBaseSize(item *protos.WorkItem) int {
	return proto.Size(item) + 2*binary.MaxVarintLen32
}

// oversizedOperationMessage describes an operation that can't fit in an entity
// work item with the entity's state.
func (s *Server) oversizedOperationMessage(entityID string, state *wrapperspb.StringValue, event *protos.HistoryEvent, baseSize int) string {
	operation, input := event.GetEntityOperationSignaled().GetOperation(), event.GetEntityOperationSignaled().GetInput()
	if called := event.GetEntityOperationCalled(); called != nil {
		operation, input = called.GetOperation(), called.GetInput()
	}
	return fmt.Sprintf("operation %q on entity %q has a %d-byte input and the entity has %d bytes of state; "+
		"together they need a %d-byte work item, which exceeds the %d-byte entity work item limit",
		operation, entityID, len(input.GetValue()), len(state.GetValue()),
		baseSize+operationRequestSize(event), s.maxEntityWorkItemBytes)
}

// operationRequestSize returns the bytes an operation adds to an EntityRequest.
func operationRequestSize(event *protos.HistoryEvent) int {
	const field = 4 // EntityRequest.operationRequests
	return protowire.SizeTag(field) + protowire.SizeBytes(proto.Size(event))
}

func (s *Server) nextEntityWorkItemLocked(filters *protos.WorkItemFilters, streamID uint64) *protos.WorkItem {
	for i, e := range s.entityQueue {
		if !matchesEntityFilter(filters, e.name) {
			continue
		}
		s.entityQueue = slices.Delete(s.entityQueue, i, i+1)
		e.queued = false
		if s.entities[e.id] != e || e.leased {
			return s.nextEntityWorkItemLocked(filters, streamID)
		}
		token := newID()
		item := newEntityWorkItem(e.id, e.state, token)
		baseSize := entityWorkItemBaseSize(item)
		events, rejected := e.takeBatch(baseSize, s.maxEntityWorkItemBytes)
		s.rejectOversizedOperationsLocked(e, rejected, baseSize)
		if len(events) == 0 {
			// Rejecting operations can leave a lock or unlock message ready.
			s.pumpEntityLocked(e)
			return s.nextEntityWorkItemLocked(filters, streamID)
		}
		e.leased = true
		s.entityLeases[token] = &entityLease{entity: e, events: events, streamID: streamID}
		item.GetEntityRequestV2().OperationRequests = events
		return item
	}
	return nil
}

func matchesEntityFilter(filters *protos.WorkItemFilters, name string) bool {
	if filters == nil || len(filters.GetEntities()) == 0 {
		return true
	}
	return slices.ContainsFunc(filters.GetEntities(), func(f *protos.EntityFilter) bool {
		return helpers.ToLowerInvariant(f.GetName()) == name
	})
}

func (s *Server) abandonEntityLocked(token string, lease *entityLease) {
	delete(s.entityLeases, token)
	e := lease.entity
	e.leased = false
	e.queue = append(slices.Clone(lease.events), e.queue...)
	s.pumpEntityLocked(e)
}

// AbandonTaskEntityWorkItem returns an entity work item's operations to the entity's queue.
func (s *Server) AbandonTaskEntityWorkItem(
	_ context.Context,
	req *protos.AbandonEntityTaskRequest,
) (*protos.AbandonEntityTaskResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	lease, ok := s.entityLeases[req.GetCompletionToken()]
	if !ok {
		return nil, status.Error(codes.NotFound, "entity work item not found")
	}
	s.abandonEntityLocked(req.GetCompletionToken(), lease)
	return &protos.AbandonEntityTaskResponse{}, nil
}

// CompleteEntityTask commits an entity batch: it stores the new state, sends
// results to calling orchestrations, and applies the batch's actions. A batch
// result larger than the message limit isn't applied; its calls fail with
// EntityResultTooLarge instead.
func (s *Server) CompleteEntityTask(_ context.Context, result *protos.EntityBatchResult) (*protos.CompleteTaskResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	lease, ok := s.entityLeases[result.GetCompletionToken()]
	if !ok {
		return nil, status.Error(codes.NotFound, "entity work item not found")
	}
	delete(s.entityLeases, result.GetCompletionToken())
	e := lease.entity
	e.leased = false

	batchFailure := result.GetFailureDetails()
	if failure := oversizedCompletionFailure(errorTypeEntityResultTooLarge, "entity batch result", result); failure != nil {
		// Fail the batch rather than store a result over the receive limit.
		batchFailure = failure
		for _, event := range lease.events {
			if signal := event.GetEntityOperationSignaled(); signal != nil {
				s.reportDroppedSignalLocked(signal, fmt.Sprintf("operation %q on entity %q: %s",
					signal.GetOperation(), e.id, failure.GetErrorMessage()))
			}
		}
	}
	if batchFailure != nil {
		// The batch had no effect. Fail its calls rather than retrying the batch
		// so that a persistent failure can't loop forever in a test.
		for _, event := range lease.events {
			if called := event.GetEntityOperationCalled(); called != nil {
				s.respondToCallerLocked(called, &protos.HistoryEvent{EventId: -1, Timestamp: timestamppb.Now(), EventType: &protos.HistoryEvent_EntityOperationFailed{
					EntityOperationFailed: &protos.EntityOperationFailedEvent{
						RequestId:      called.GetRequestId(),
						FailureDetails: batchFailure,
					},
				}})
			}
		}
		s.pumpEntityLocked(e)
		// The completed work item frees a concurrency slot even if this entity
		// has nothing more to dispatch.
		s.notifyLocked()
		return &protos.CompleteTaskResponse{}, nil
	}

	e.state = result.GetEntityState()
	e.lastModified = time.Now()
	for i, opResult := range result.GetResults() {
		if i >= len(lease.events) {
			break
		}
		called := lease.events[i].GetEntityOperationCalled()
		if called == nil {
			continue
		}
		if failure := opResult.GetFailure(); failure != nil {
			s.respondToCallerLocked(called, &protos.HistoryEvent{EventId: -1, Timestamp: timestamppb.Now(), EventType: &protos.HistoryEvent_EntityOperationFailed{
				EntityOperationFailed: &protos.EntityOperationFailedEvent{
					RequestId:      called.GetRequestId(),
					FailureDetails: failure.GetFailureDetails(),
				},
			}})
		} else {
			s.respondToCallerLocked(called, &protos.HistoryEvent{EventId: -1, Timestamp: timestamppb.Now(), EventType: &protos.HistoryEvent_EntityOperationCompleted{
				EntityOperationCompleted: &protos.EntityOperationCompletedEvent{
					RequestId: called.GetRequestId(),
					Output:    opResult.GetSuccess().GetResult(),
				},
			}})
		}
	}
	for _, action := range result.GetActions() {
		switch a := action.GetOperationActionType().(type) {
		case *protos.OperationAction_SendSignal:
			signal := a.SendSignal
			s.signalEntityLocked(signal.GetInstanceId(), newSignalEvent(&protos.EntityOperationSignaledEvent{
				RequestId:     newID(),
				Operation:     signal.GetName(),
				Input:         signal.GetInput(),
				ScheduledTime: signal.GetScheduledTime(),
			}))
		case *protos.OperationAction_StartNewOrchestration:
			start := a.StartNewOrchestration
			id := start.GetInstanceId()
			if id == "" {
				id = newID()
			}
			if existing, ok := s.instances[id]; ok && !isTerminal(existing.status) {
				continue
			}
			s.newInstanceLocked(id, start.GetName(), start.GetVersion(), start.GetInput(), start.GetTags(),
				nil, start.GetParentTraceContext(), start.GetScheduledTime())
		}
	}
	s.pumpEntityLocked(e)
	s.notifyLocked()
	return &protos.CompleteTaskResponse{}, nil
}

// rebindEntityExecutionLocked moves an instance's outstanding entity calls,
// lock requests, and locks from one execution to another. Rewind renews the
// execution ID but keeps the history that sent them, so replay waits for their
// results rather than sending them again.
func (s *Server) rebindEntityExecutionLocked(instanceID, oldExecutionID, newExecutionID string) {
	if oldExecutionID == newExecutionID {
		return
	}
	rebind := func(events []*protos.HistoryEvent) []*protos.HistoryEvent {
		var rebound []*protos.HistoryEvent
		for i, event := range events {
			called := event.GetEntityOperationCalled()
			if called.GetParentInstanceId().GetValue() != instanceID ||
				called.GetParentExecutionId().GetValue() != oldExecutionID {
				continue
			}
			if rebound == nil {
				// Leased events may be in use outside the server lock; don't modify them.
				rebound = slices.Clone(events)
			}
			cloned := proto.CloneOf(event)
			cloned.GetEntityOperationCalled().ParentExecutionId = wrapperspb.String(newExecutionID)
			rebound[i] = cloned
		}
		if rebound == nil {
			return events
		}
		return rebound
	}
	for criticalSectionID, executionID := range s.lockRequests {
		if executionID == oldExecutionID {
			s.lockRequests[criticalSectionID] = newExecutionID
		}
	}
	for _, lease := range s.entityLeases {
		lease.events = rebind(lease.events)
	}
	for _, e := range s.entities {
		e.queue = rebind(e.queue)
		if e.lock != nil && e.lock.ownerInstanceID == instanceID && e.lock.ownerExecutionID == oldExecutionID {
			e.lock.ownerExecutionID = newExecutionID
		}
		s.pumpEntityLocked(e)
	}
}

// respondToCallerLocked delivers an operation result to the calling execution,
// retaining it for a failed orchestration in case it is rewound.
func (s *Server) respondToCallerLocked(called *protos.EntityOperationCalledEvent, event *protos.HistoryEvent) {
	inst, ok := s.instances[called.GetParentInstanceId().GetValue()]
	if !ok {
		return
	}
	if executionID := called.GetParentExecutionId(); executionID != nil && executionID.GetValue() != inst.executionID {
		return
	}
	if inst.status == protos.OrchestrationStatus_ORCHESTRATION_STATUS_FAILED {
		inst.pending = append(inst.pending, event)
	} else {
		s.deliverLocked(inst, event)
	}
}

// validateEntityMessage reports an entity message that the service couldn't
// deliver or that a worker would refuse to process.
func validateEntityMessage(action *protos.SendEntityMessageAction) error {
	var requestID string
	var hasRequestID bool
	var targets []string
	switch m := action.GetEntityMessageType().(type) {
	case *protos.SendEntityMessageAction_EntityOperationSignaled:
		requestID, hasRequestID = m.EntityOperationSignaled.GetRequestId(), true
		targets = []string{m.EntityOperationSignaled.GetTargetInstanceId().GetValue()}
	case *protos.SendEntityMessageAction_EntityOperationCalled:
		requestID, hasRequestID = m.EntityOperationCalled.GetRequestId(), true
		targets = []string{m.EntityOperationCalled.GetTargetInstanceId().GetValue()}
	case *protos.SendEntityMessageAction_EntityLockRequested:
		targets = m.EntityLockRequested.GetLockSet()
	case *protos.SendEntityMessageAction_EntityUnlockSent:
		targets = []string{m.EntityUnlockSent.GetTargetInstanceId().GetValue()}
	}
	for _, target := range targets {
		if _, _, ok := normalizeEntityID(target); !ok {
			return fmt.Errorf("invalid entity instance ID %q", target)
		}
	}
	if hasRequestID {
		// Workers reject entity work items whose request IDs aren't UUIDs.
		if _, err := uuid.Parse(requestID); err != nil {
			return fmt.Errorf("invalid entity request ID %q: must be a UUID", requestID)
		}
	}
	return nil
}

// sendEntityMessageLocked records an orchestration's entity message in its
// history and delivers the message to the target entity.
func (s *Server) sendEntityMessageLocked(inst *instance, id int32, action *protos.SendEntityMessageAction) {
	event := &protos.HistoryEvent{EventId: id, Timestamp: timestamppb.Now()}
	message := &protos.HistoryEvent{EventId: -1, Timestamp: event.GetTimestamp()}
	switch m := action.GetEntityMessageType().(type) {
	case *protos.SendEntityMessageAction_EntityOperationSignaled:
		event.EventType = &protos.HistoryEvent_EntityOperationSignaled{EntityOperationSignaled: m.EntityOperationSignaled}
		inst.history = append(inst.history, event)
		signal := proto.CloneOf(m.EntityOperationSignaled)
		signal.TargetInstanceId = nil
		s.signalEntityLocked(m.EntityOperationSignaled.GetTargetInstanceId().GetValue(), newSignalEvent(signal))
	case *protos.SendEntityMessageAction_EntityOperationCalled:
		event.EventType = &protos.HistoryEvent_EntityOperationCalled{EntityOperationCalled: m.EntityOperationCalled}
		inst.history = append(inst.history, event)
		called := proto.CloneOf(m.EntityOperationCalled)
		called.TargetInstanceId = nil
		called.ParentInstanceId = wrapperspb.String(inst.id)
		called.ParentExecutionId = wrapperspb.String(inst.executionID)
		message.EventType = &protos.HistoryEvent_EntityOperationCalled{EntityOperationCalled: called}
		s.deliverEntityLocked(m.EntityOperationCalled.GetTargetInstanceId().GetValue(), message)
	case *protos.SendEntityMessageAction_EntityLockRequested:
		event.EventType = &protos.HistoryEvent_EntityLockRequested{EntityLockRequested: m.EntityLockRequested}
		inst.history = append(inst.history, event)
		request := proto.CloneOf(m.EntityLockRequested)
		if len(request.GetLockSet()) == 0 {
			return
		}
		request.Position = 0
		request.ParentInstanceId = wrapperspb.String(inst.id)
		s.lockRequests[request.GetCriticalSectionId()] = inst.executionID
		message.EventType = &protos.HistoryEvent_EntityLockRequested{EntityLockRequested: request}
		s.deliverEntityLocked(request.GetLockSet()[0], message)
	case *protos.SendEntityMessageAction_EntityUnlockSent:
		event.EventType = &protos.HistoryEvent_EntityUnlockSent{EntityUnlockSent: m.EntityUnlockSent}
		inst.history = append(inst.history, event)
		unlock := proto.CloneOf(m.EntityUnlockSent)
		unlock.TargetInstanceId = nil
		unlock.ParentInstanceId = wrapperspb.String(inst.id)
		message.EventType = &protos.HistoryEvent_EntityUnlockSent{EntityUnlockSent: unlock}
		s.deliverEntityLocked(m.EntityUnlockSent.GetTargetInstanceId().GetValue(), message)
	}
}

func (e *entity) metadata(includeState bool) *protos.EntityMetadata {
	metadata := &protos.EntityMetadata{
		InstanceId:       e.id,
		LastModifiedTime: timestamppb.New(e.lastModified),
		BacklogQueueSize: int32(len(e.queue)),
	}
	if e.lock != nil {
		metadata.LockedBy = wrapperspb.String(e.lock.ownerInstanceID)
	}
	if includeState {
		metadata.SerializedState = e.state
	}
	return metadata
}

// SignalEntity sends a one-way operation to an entity.
func (s *Server) SignalEntity(_ context.Context, req *protos.SignalEntityRequest) (*protos.SignalEntityResponse, error) {
	id, _, ok := normalizeEntityID(req.GetInstanceId())
	if !ok {
		return nil, invalidEntityIDError(req.GetInstanceId())
	}
	if req.GetName() == "" {
		return nil, status.Error(codes.InvalidArgument, "entity operation name is required")
	}
	requestID := req.GetRequestId()
	if requestID == "" {
		requestID = newID()
	} else if _, err := uuid.Parse(requestID); err != nil {
		// Workers reject entity work items whose request IDs aren't UUIDs.
		return nil, status.Errorf(codes.InvalidArgument, "invalid entity request ID %q: must be a UUID", requestID)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	event := newSignalEvent(&protos.EntityOperationSignaledEvent{
		RequestId:     requestID,
		Operation:     req.GetName(),
		Input:         req.GetInput(),
		ScheduledTime: req.GetScheduledTime(),
	})
	// Reject a signal that can't be dispatched with the entity's current state.
	// The work item's completion token is a UUID, like every other token.
	var state *wrapperspb.StringValue
	if e, ok := s.entities[id]; ok {
		state = e.state
	}
	baseSize := entityWorkItemBaseSize(newEntityWorkItem(id, state, newID()))
	if baseSize+operationRequestSize(event) > s.maxEntityWorkItemBytes {
		return nil, status.Error(codes.InvalidArgument, s.oversizedOperationMessage(id, state, event, baseSize))
	}
	s.signalEntityLocked(id, event)
	return &protos.SignalEntityResponse{}, nil
}

// GetEntity returns an entity's metadata. An entity exists while it has state.
func (s *Server) GetEntity(_ context.Context, req *protos.GetEntityRequest) (*protos.GetEntityResponse, error) {
	id, _, ok := normalizeEntityID(req.GetInstanceId())
	if !ok {
		return nil, invalidEntityIDError(req.GetInstanceId())
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.entities[id]
	if !ok || e.state == nil {
		return &protos.GetEntityResponse{Exists: false}, nil
	}
	return &protos.GetEntityResponse{Exists: true, Entity: e.metadata(req.GetIncludeState())}, nil
}

// QueryEntities returns entities that match a query, ordered by instance ID.
// The continuation token encodes the last returned instance ID, so a query
// resumes after it even if entities are added or removed between pages.
func (s *Server) QueryEntities(_ context.Context, req *protos.QueryEntitiesRequest) (*protos.QueryEntitiesResponse, error) {
	query := req.GetQuery()
	var cursor string
	if token := query.GetContinuationToken().GetValue(); token != "" {
		var ok bool
		if cursor, ok = decodeEntityCursor(token); !ok {
			return nil, status.Errorf(codes.InvalidArgument, "invalid continuation token %q", token)
		}
	}
	pageSize := int(query.GetPageSize().GetValue())
	if pageSize <= 0 {
		pageSize = defaultQueryPageSize
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	var matches []*entity
	for _, e := range s.entities {
		if cursor != "" && e.id <= cursor {
			continue
		}
		if e.state == nil && !query.GetIncludeTransient() {
			continue
		}
		if prefix := query.GetInstanceIdStartsWith().GetValue(); prefix != "" && !strings.HasPrefix(e.id, prefix) {
			continue
		}
		// LastModifiedFrom is inclusive and LastModifiedTo is exclusive.
		if from := query.GetLastModifiedFrom(); from != nil && e.lastModified.Before(from.AsTime()) {
			continue
		}
		if to := query.GetLastModifiedTo(); to != nil && !e.lastModified.Before(to.AsTime()) {
			continue
		}
		matches = append(matches, e)
	}
	slices.SortFunc(matches, func(a, b *entity) int { return cmp.Compare(a.id, b.id) })

	resp := &protos.QueryEntitiesResponse{}
	if len(matches) > pageSize {
		matches = matches[:pageSize]
		resp.ContinuationToken = wrapperspb.String(encodeEntityCursor(matches[pageSize-1].id))
	}
	for _, e := range matches {
		resp.Entities = append(resp.Entities, e.metadata(query.GetIncludeState()))
	}
	return resp, nil
}

func encodeEntityCursor(instanceID string) string {
	return base64.RawURLEncoding.EncodeToString([]byte(instanceID))
}

func decodeEntityCursor(token string) (string, bool) {
	decoded, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil {
		return "", false
	}
	id, _, ok := normalizeEntityID(string(decoded))
	return id, ok && id == string(decoded)
}

// CleanEntityStorage releases locks held by orchestrations that are no longer
// running and removes entities that have no state, messages, or lock.
func (s *Server) CleanEntityStorage(_ context.Context, req *protos.CleanEntityStorageRequest) (*protos.CleanEntityStorageResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	resp := &protos.CleanEntityStorageResponse{}
	if req.GetReleaseOrphanedLocks() {
		for _, e := range s.entities {
			if e.lock == nil {
				continue
			}
			if owner, ok := s.instances[e.lock.ownerInstanceID]; ok && !isTerminal(owner.status) &&
				owner.executionID == e.lock.ownerExecutionID {
				continue
			}
			e.lock = nil
			resp.OrphanedLocksReleased++
			s.pumpEntityLocked(e)
		}
	}
	if req.GetRemoveEmptyEntities() {
		for id, e := range s.entities {
			if e.state == nil && len(e.queue) == 0 && e.lock == nil && !e.leased {
				delete(s.entities, id)
				resp.EmptyEntitiesRemoved++
			}
		}
	}
	return resp, nil
}
