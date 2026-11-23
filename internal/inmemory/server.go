// Package inmemory implements an in-memory Durable Task sidecar service for tests.
//
// The service keeps all orchestration state in process memory and serves the
// same gRPC protocol as the Durable Task Scheduler, so the regular client and
// worker can run against it. Timers fire in real time. Entities and rewind are
// not supported.
package inmemory

import (
	"context"
	"fmt"
	"slices"
	"sync"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/microsoft/durabletask-go/api"
	"github.com/microsoft/durabletask-go/internal/grpcerrors"
	"github.com/microsoft/durabletask-go/internal/helpers"
	"github.com/microsoft/durabletask-go/internal/protos"
)

const (
	defaultHealthPingInterval    = 30 * time.Second
	defaultQueryPageSize         = 100
	maxOrchestrationWorkItemSize = 4 << 20
	maxHistoryChunkSize          = 4 << 20
)

// Server is an in-memory implementation of the TaskHubSidecarService.
type Server struct {
	protos.UnimplementedTaskHubSidecarServiceServer

	healthPingInterval time.Duration

	mu           sync.Mutex
	instances    map[string]*instance
	orchQueue    []*instance
	actQueue     []*activityItem
	orchLeases   map[string]*orchestrationLease
	actLeases    map[string]*activityLease
	changed      chan struct{}
	closed       bool
	nextStreamID uint64
}

type instance struct {
	id             string
	name           string
	version        *wrapperspb.StringValue
	status         protos.OrchestrationStatus
	input          *wrapperspb.StringValue
	output         *wrapperspb.StringValue
	customStatus   *wrapperspb.StringValue
	failure        *protos.TaskFailureDetails
	tags           map[string]string
	parent         *protos.ParentInstanceInfo
	traceContext   *protos.TraceContext
	createdAt      time.Time
	updatedAt      time.Time
	completedAt    time.Time
	scheduledStart *timestamppb.Timestamp
	executionID    string

	history []*protos.HistoryEvent
	pending []*protos.HistoryEvent

	leased   bool
	queued   bool
	children []string
	timers   []*time.Timer
}

type activityItem struct {
	inst        *instance
	executionID string
	request     *protos.ActivityRequest
}

type orchestrationLease struct {
	inst     *instance
	events   []*protos.HistoryEvent
	streamID uint64
}

type activityLease struct {
	item     *activityItem
	streamID uint64
}

// NewServer creates an empty in-memory service.
func NewServer() *Server {
	return &Server{
		healthPingInterval: defaultHealthPingInterval,
		instances:          make(map[string]*instance),
		orchLeases:         make(map[string]*orchestrationLease),
		actLeases:          make(map[string]*activityLease),
		changed:            make(chan struct{}),
	}
}

// Register registers the service with a gRPC server.
func (s *Server) Register(registrar grpc.ServiceRegistrar) {
	protos.RegisterTaskHubSidecarServiceServer(registrar, s)
}

// Close stops timers and ends all open work item streams and waiters.
func (s *Server) Close() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return
	}
	s.closed = true
	for _, inst := range s.instances {
		inst.stopTimers()
	}
	s.notifyLocked()
}

func (s *Server) notifyLocked() {
	close(s.changed)
	s.changed = make(chan struct{})
}

func isTerminal(status protos.OrchestrationStatus) bool {
	switch status {
	case protos.OrchestrationStatus_ORCHESTRATION_STATUS_COMPLETED,
		protos.OrchestrationStatus_ORCHESTRATION_STATUS_FAILED,
		protos.OrchestrationStatus_ORCHESTRATION_STATUS_TERMINATED,
		protos.OrchestrationStatus_ORCHESTRATION_STATUS_CANCELED:
		return true
	default:
		return false
	}
}

func newID() string {
	if u, err := uuid.NewV7(); err == nil {
		return u.String()
	}
	return uuid.NewString()
}

func (inst *instance) stopTimers() {
	for _, t := range inst.timers {
		t.Stop()
	}
	inst.timers = nil
}

func (inst *instance) state(includePayloads bool) *protos.OrchestrationState {
	state := &protos.OrchestrationState{
		InstanceId:              inst.id,
		Name:                    inst.name,
		Version:                 inst.version,
		OrchestrationStatus:     inst.status,
		ScheduledStartTimestamp: inst.scheduledStart,
		CreatedTimestamp:        timestamppb.New(inst.createdAt),
		LastUpdatedTimestamp:    timestamppb.New(inst.updatedAt),
		CustomStatus:            inst.customStatus,
		FailureDetails:          inst.failure,
		ExecutionId:             wrapperspb.String(inst.executionID),
		Tags:                    inst.tags,
	}
	if includePayloads {
		state.Input = inst.input
		state.Output = inst.output
	}
	if !inst.completedAt.IsZero() {
		state.CompletedTimestamp = timestamppb.New(inst.completedAt)
	}
	if parentID := inst.parent.GetOrchestrationInstance().GetInstanceId(); parentID != "" {
		state.ParentInstanceId = wrapperspb.String(parentID)
	}
	return state
}

// newInstanceLocked creates a pending instance, replacing any existing instance
// with the same ID, and queues its first work item.
func (s *Server) newInstanceLocked(
	id, name string,
	version, input *wrapperspb.StringValue,
	tags map[string]string,
	parent *protos.ParentInstanceInfo,
	traceContext *protos.TraceContext,
	scheduledStart *timestamppb.Timestamp,
) *instance {
	now := time.Now()
	inst := &instance{
		id:             id,
		name:           name,
		version:        version,
		status:         protos.OrchestrationStatus_ORCHESTRATION_STATUS_PENDING,
		input:          input,
		tags:           tags,
		parent:         parent,
		traceContext:   traceContext,
		createdAt:      now,
		updatedAt:      now,
		scheduledStart: scheduledStart,
	}
	if old, ok := s.instances[id]; ok {
		old.stopTimers()
	}
	s.instances[id] = inst
	inst.pending = []*protos.HistoryEvent{inst.newExecutionStartedEvent()}
	if scheduledStart != nil && scheduledStart.AsTime().After(now) {
		s.startTimerLocked(inst, scheduledStart.AsTime(), func() { s.enqueueLocked(inst) })
	} else {
		s.enqueueLocked(inst)
	}
	s.notifyLocked()
	return inst
}

// newExecutionStartedEvent assigns a new execution ID and returns its start event.
func (inst *instance) newExecutionStartedEvent() *protos.HistoryEvent {
	event := helpers.NewExecutionStartedEvent(
		inst.name, inst.id, inst.input, inst.parent, inst.traceContext, inst.scheduledStart, inst.version)
	started := event.GetExecutionStarted()
	started.Tags = inst.tags
	inst.executionID = started.GetOrchestrationInstance().GetExecutionId().GetValue()
	return event
}

// startTimerLocked runs fn under the server lock at the given time, unless the
// instance has been replaced or purged, its execution has changed, or the
// server has been closed.
func (s *Server) startTimerLocked(inst *instance, at time.Time, fn func()) {
	executionID := inst.executionID
	timer := time.AfterFunc(time.Until(at), func() {
		s.mu.Lock()
		defer s.mu.Unlock()
		if s.closed || s.instances[inst.id] != inst || inst.executionID != executionID {
			return
		}
		fn()
		s.notifyLocked()
	})
	inst.timers = append(inst.timers, timer)
}

func (s *Server) enqueueLocked(inst *instance) {
	if inst.queued || inst.leased || len(inst.pending) == 0 || s.instances[inst.id] != inst {
		return
	}
	if inst.scheduledStart != nil && len(inst.history) == 0 && inst.scheduledStart.AsTime().After(time.Now()) {
		return
	}
	inst.queued = true
	s.orchQueue = append(s.orchQueue, inst)
	s.notifyLocked()
}

// deliverLocked adds a new event to an active instance's inbox.
func (s *Server) deliverLocked(inst *instance, event *protos.HistoryEvent) {
	if isTerminal(inst.status) {
		return
	}
	inst.pending = append(inst.pending, event)
	inst.updatedAt = time.Now()
	s.enqueueLocked(inst)
}

// Hello implements the service handshake.
func (s *Server) Hello(context.Context, *emptypb.Empty) (*emptypb.Empty, error) {
	return &emptypb.Empty{}, nil
}

// StartInstance creates a new orchestration instance.
func (s *Server) StartInstance(_ context.Context, req *protos.CreateInstanceRequest) (*protos.CreateInstanceResponse, error) {
	if req.GetName() == "" {
		return nil, status.Error(codes.InvalidArgument, "orchestration name is required")
	}
	id := req.GetInstanceId()
	if id == "" {
		id = newID()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if existing, ok := s.instances[id]; ok {
		replaceable := req.GetOrchestrationIdReusePolicy().GetReplaceableStatus()
		if !slices.Contains(replaceable, existing.status) {
			return nil, status.Errorf(codes.AlreadyExists, "orchestration instance %q already exists", id)
		}
	}
	s.newInstanceLocked(id, req.GetName(), req.GetVersion(), req.GetInput(), req.GetTags(),
		nil, req.GetParentTraceContext(), req.GetScheduledStartTimestamp())
	return &protos.CreateInstanceResponse{InstanceId: id}, nil
}

// GetInstance returns an instance's current state.
func (s *Server) GetInstance(_ context.Context, req *protos.GetInstanceRequest) (*protos.GetInstanceResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.getInstanceLocked(req), nil
}

func (s *Server) getInstanceLocked(req *protos.GetInstanceRequest) *protos.GetInstanceResponse {
	inst, ok := s.instances[req.GetInstanceId()]
	if !ok {
		return &protos.GetInstanceResponse{Exists: false}
	}
	return &protos.GetInstanceResponse{Exists: true, OrchestrationState: inst.state(req.GetGetInputsAndOutputs())}
}

// WaitForInstanceStart blocks until the instance leaves the pending state.
func (s *Server) WaitForInstanceStart(ctx context.Context, req *protos.GetInstanceRequest) (*protos.GetInstanceResponse, error) {
	return s.waitForInstance(ctx, req, func(st protos.OrchestrationStatus) bool {
		return st != protos.OrchestrationStatus_ORCHESTRATION_STATUS_PENDING
	})
}

// WaitForInstanceCompletion blocks until the instance reaches a terminal state.
func (s *Server) WaitForInstanceCompletion(ctx context.Context, req *protos.GetInstanceRequest) (*protos.GetInstanceResponse, error) {
	return s.waitForInstance(ctx, req, isTerminal)
}

func (s *Server) waitForInstance(
	ctx context.Context,
	req *protos.GetInstanceRequest,
	done func(protos.OrchestrationStatus) bool,
) (*protos.GetInstanceResponse, error) {
	for {
		s.mu.Lock()
		inst, ok := s.instances[req.GetInstanceId()]
		if !ok || done(inst.status) {
			resp := s.getInstanceLocked(req)
			s.mu.Unlock()
			return resp, nil
		}
		changed, closed := s.changed, s.closed
		s.mu.Unlock()
		if closed {
			return nil, status.Error(codes.Unavailable, "in-memory service is closed")
		}
		select {
		case <-ctx.Done():
			return nil, status.FromContextError(ctx.Err()).Err()
		case <-changed:
		}
	}
}

// RaiseEvent delivers an external event to an instance.
func (s *Server) RaiseEvent(_ context.Context, req *protos.RaiseEventRequest) (*protos.RaiseEventResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	inst, err := s.lookupLocked(req.GetInstanceId())
	if err != nil {
		return nil, err
	}
	s.deliverLocked(inst, helpers.NewEventRaisedEvent(req.GetName(), req.GetInput()))
	return &protos.RaiseEventResponse{}, nil
}

// TerminateInstance terminates an instance and, optionally, its sub-orchestrations.
func (s *Server) TerminateInstance(_ context.Context, req *protos.TerminateRequest) (*protos.TerminateResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	inst, err := s.lookupLocked(req.GetInstanceId())
	if err != nil {
		return nil, err
	}
	s.terminateLocked(inst, req.GetOutput(), req.GetRecursive())
	return &protos.TerminateResponse{}, nil
}

func (s *Server) terminateLocked(inst *instance, reason *wrapperspb.StringValue, recursive bool) {
	if isTerminal(inst.status) {
		return
	}
	if recursive {
		for _, childID := range inst.children {
			if child, ok := s.instances[childID]; ok &&
				child.parent.GetOrchestrationInstance().GetInstanceId() == inst.id {
				s.terminateLocked(child, reason, true)
			}
		}
	}
	terminated := helpers.NewExecutionTerminatedEvent(reason, recursive)
	if len(inst.history) == 0 && !inst.leased && len(inst.pending) > 0 {
		// The orchestrator never ran, so complete the termination without a worker.
		inst.history = append(inst.history, inst.pending[0], terminated)
		s.completeLocked(inst, protos.OrchestrationStatus_ORCHESTRATION_STATUS_TERMINATED, reason, nil)
		return
	}
	s.deliverLocked(inst, terminated)
}

// SuspendInstance suspends a running instance.
func (s *Server) SuspendInstance(_ context.Context, req *protos.SuspendRequest) (*protos.SuspendResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	inst, err := s.lookupLocked(req.GetInstanceId())
	if err != nil {
		return nil, err
	}
	if isTerminal(inst.status) {
		return nil, grpcerrors.New(codes.FailedPrecondition, fmt.Sprintf("orchestration instance %q has already completed", inst.id), grpcerrors.ReasonInvalidState)
	}
	if inst.status != protos.OrchestrationStatus_ORCHESTRATION_STATUS_SUSPENDED {
		inst.status = protos.OrchestrationStatus_ORCHESTRATION_STATUS_SUSPENDED
		s.deliverLocked(inst, helpers.NewSuspendOrchestrationEvent(req.GetReason().GetValue()))
		s.notifyLocked()
	}
	return &protos.SuspendResponse{}, nil
}

// ResumeInstance resumes a suspended instance.
func (s *Server) ResumeInstance(_ context.Context, req *protos.ResumeRequest) (*protos.ResumeResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	inst, err := s.lookupLocked(req.GetInstanceId())
	if err != nil {
		return nil, err
	}
	if isTerminal(inst.status) {
		return nil, grpcerrors.New(codes.FailedPrecondition, fmt.Sprintf("orchestration instance %q has already completed", inst.id), grpcerrors.ReasonInvalidState)
	}
	if inst.status == protos.OrchestrationStatus_ORCHESTRATION_STATUS_SUSPENDED {
		inst.status = protos.OrchestrationStatus_ORCHESTRATION_STATUS_RUNNING
		s.deliverLocked(inst, helpers.NewResumeOrchestrationEvent(req.GetReason().GetValue()))
		s.notifyLocked()
	}
	return &protos.ResumeResponse{}, nil
}

// RestartInstance starts a terminal instance again with its original input.
func (s *Server) RestartInstance(_ context.Context, req *protos.RestartInstanceRequest) (*protos.RestartInstanceResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	inst, err := s.lookupLocked(req.GetInstanceId())
	if err != nil {
		return nil, err
	}
	if !isTerminal(inst.status) {
		return nil, notCompletedError(inst.id)
	}
	id := inst.id
	if req.GetRestartWithNewInstanceId() {
		id = newID()
	}
	s.newInstanceLocked(id, inst.name, inst.version, inst.input, inst.tags, nil, inst.traceContext, nil)
	return &protos.RestartInstanceResponse{InstanceId: id}, nil
}

func (s *Server) lookupLocked(id string) (*instance, error) {
	inst, ok := s.instances[id]
	if !ok {
		return nil, status.Errorf(codes.NotFound, "orchestration instance %q not found", id)
	}
	return inst, nil
}

// StreamInstanceHistory streams an instance's committed history.
func (s *Server) StreamInstanceHistory(
	req *protos.StreamInstanceHistoryRequest,
	stream grpc.ServerStreamingServer[protos.HistoryChunk],
) error {
	s.mu.Lock()
	inst, ok := s.instances[req.GetInstanceId()]
	if !ok || (req.GetExecutionId() != nil && req.GetExecutionId().GetValue() != inst.executionID) {
		s.mu.Unlock()
		return status.Errorf(codes.NotFound, "orchestration instance %q not found", req.GetInstanceId())
	}
	history := slices.Clone(inst.history)
	s.mu.Unlock()

	chunk := &protos.HistoryChunk{}
	chunkSize := 0
	for _, event := range history {
		eventSize := proto.Size(event)
		encodedSize := protowire.SizeTag(1) + protowire.SizeBytes(eventSize)
		if encodedSize > maxHistoryChunkSize {
			return status.Errorf(codes.ResourceExhausted, "history event exceeds the %d-byte stream chunk limit", maxHistoryChunkSize)
		}
		if len(chunk.Events) > 0 && chunkSize+encodedSize > maxHistoryChunkSize {
			if err := stream.Send(chunk); err != nil {
				return err
			}
			chunk = &protos.HistoryChunk{}
			chunkSize = 0
		}
		chunk.Events = append(chunk.Events, event)
		chunkSize += encodedSize
	}
	if len(chunk.Events) > 0 {
		return stream.Send(chunk)
	}
	return nil
}

func failureDetails(errorType, format string, args ...any) *protos.TaskFailureDetails {
	return &protos.TaskFailureDetails{ErrorType: errorType, ErrorMessage: fmt.Sprintf(format, args...)}
}

// notCompletedError maps to api.ErrNotCompleted in the client.
func notCompletedError(id string) error {
	return grpcerrors.New(codes.FailedPrecondition, fmt.Sprintf("%s: instance %q", api.ErrNotCompleted, id), grpcerrors.ReasonNotCompleted)
}
