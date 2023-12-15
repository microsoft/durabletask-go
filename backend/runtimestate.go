package backend

import (
	"errors"
	"fmt"
	"time"

	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/microsoft/durabletask-go/api"
	"github.com/microsoft/durabletask-go/internal/helpers"
	"github.com/microsoft/durabletask-go/internal/protos"
)

var (
	ErrDuplicateEvent = errors.New("duplicate event")
	ErrContinuedAsNew = errors.New("orchestration continued-as-new")
)

type OrchestrationRuntimeState struct {
	instanceID      api.InstanceID
	committedEvents []*protos.HistoryEvent
	pendingEvents   []*protos.HistoryEvent
	newEvents       []*protos.HistoryEvent
	newActions      []*protos.OrchestratorAction

	startEvent       *protos.ExecutionStartedEvent
	completedEvent   *protos.ExecutionCompletedEvent
	createdTime      time.Time
	lastUpdatedTime  time.Time
	completedTime    time.Time
	continuedAsNew   bool
	isSuspended      bool
	startedTaskIDs   map[int32]bool
	completedTaskIDs map[int32]bool

	CustomStatus *wrapperspb.StringValue
}

type OrchestratorMessage struct {
	HistoryEvent     *HistoryEvent
	TargetInstanceID string
}

// ChunkingConfiguration specifies the maximum size of a single chunk of history events.
// See https://github.com/microsoft/durabletask-go/issues/44 for more details.
type ChunkingConfiguration struct {
	// MaxHistoryEventCount is the maximum number of history events that can be stored in a single chunk.
	// A value of 0 or less means that there is no limit.
	MaxHistoryEventCount int
	// MaxHistoryEventSizeInKB is the maximum size of a single chunk in kilobytes.
	// For example, a max size of 2MB would be specified as 2048.
	// A value of 0 or less means that there is no limit.
	MaxHistoryEventSizeInKB int
}

func NewOrchestrationRuntimeState(instanceID api.InstanceID, existingHistory []*HistoryEvent) *OrchestrationRuntimeState {
	s := &OrchestrationRuntimeState{
		instanceID:       instanceID,
		committedEvents:  make([]*HistoryEvent, 0, len(existingHistory)+10),
		startedTaskIDs:   make(map[int32]bool),
		completedTaskIDs: make(map[int32]bool),
	}

	for _, e := range existingHistory {
		s.addEvent(e, false)
	}

	return s
}

// AddEvent appends a new history event to the orchestration history
//
// Returns [ErrDuplicateEvent] if the event is known to be a duplicate.
func (s *OrchestrationRuntimeState) AddEvent(e *HistoryEvent) error {
	return s.addEvent(e, true)
}

func (s *OrchestrationRuntimeState) addEvent(e *HistoryEvent, isNew bool) error {
	if startEvent := e.GetExecutionStarted(); startEvent != nil {
		if s.startEvent != nil {
			return ErrDuplicateEvent
		}
		s.startEvent = startEvent
		s.createdTime = e.Timestamp.AsTime()
	} else if completedEvent := e.GetExecutionCompleted(); completedEvent != nil {
		if s.completedEvent != nil {
			return ErrDuplicateEvent
		}
		s.completedEvent = completedEvent
		s.completedTime = e.Timestamp.AsTime()
	} else if e.GetExecutionSuspended() != nil {
		s.isSuspended = true
	} else if e.GetExecutionResumed() != nil {
		s.isSuspended = false
	} else if e.GetTaskScheduled() != nil || e.GetSubOrchestrationInstanceCreated() != nil || e.GetTimerCreated() != nil {
		// Filter out duplicate task started events. This is never expected unless there is a bug
		// in a Durable Task SDK library. We filter here to prevent state store insert problems.
		if _, exists := s.startedTaskIDs[e.EventId]; exists {
			return ErrDuplicateEvent
		}
		s.startedTaskIDs[e.EventId] = true
	} else if e.GetTaskCompleted() != nil || e.GetTaskFailed() != nil ||
		e.GetSubOrchestrationInstanceCompleted() != nil || e.GetSubOrchestrationInstanceFailed() != nil ||
		e.GetTimerFired() != nil {
		// Filter out duplicate task completed events. This can happen in failure recovery cases where
		// completion events get played into the history multiple times.
		taskID := helpers.GetTaskId(e)
		if _, exists := s.completedTaskIDs[taskID]; exists {
			return ErrDuplicateEvent
		}
		s.completedTaskIDs[taskID] = true
	}

	if isNew {
		s.pendingEvents = append(s.pendingEvents, e)
		s.newEvents = append(s.newEvents, e)
	} else {
		s.committedEvents = append(s.committedEvents, e)
	}

	s.lastUpdatedTime = e.Timestamp.AsTime()
	return nil
}

func (s *OrchestrationRuntimeState) IsValid() bool {
	if len(s.committedEvents) == 0 && len(s.pendingEvents) == 0 {
		// empty orchestration state
		return true
	} else if s.startEvent != nil {
		// orchestration history has a start event
		return true
	}
	return false
}

func (s *OrchestrationRuntimeState) AddActions(actions []*protos.OrchestratorAction) {
	s.newActions = append(s.newActions, actions...)
}

// ProcessChanges processes all the changes that were added to the orchestration state and returns the changes that were applied
// based on the specified chunking configuration. ProcessChanges should be called continuously until it returns a non-partial
// result. If the result is partial, the caller should commit the changes and then call ProcessChanges again with the same
// state object to process any remaining changes.
//
// Returns [ErrContinuedAsNew] if the orchestration has continued-as-new.
func (s *OrchestrationRuntimeState) ProcessChanges(c ChunkingConfiguration, currentTraceContext *protos.TraceContext, log Logger) (OrchestrationStateChanges, error) {
	var currentChunkLength int
	var currentChunkSizeInBytes int
	var changes OrchestrationStateChanges

	resetChanges := func(continuedAsNew bool) {
		currentChunkLength = 0
		currentChunkSizeInBytes = 0
		changes = OrchestrationStateChanges{
			IsPartial:      true,
			ContinuedAsNew: s.continuedAsNew || continuedAsNew,
			CustomStatus:   s.CustomStatus,
			RuntimeStatus:  s.RuntimeStatus(),
		}
	}
	resetChanges(false)

	// verifyAndAddPayloadSize is a helper function that processes a single history event and returns true if it should be
	// included in the current chunk, false if it should be excluded, or an error if the item cannot be processed. We need
	// to run this function for each item in the list of new events, new messages, new timers, and new tasks.
	verifyAndAddPayloadSize := func(e *HistoryEvent) (bool, error) {
		if c.MaxHistoryEventSizeInKB > 0 {
			eventSize := helpers.GetProtoSize(e)
			maxChunkSizeInBytes := c.MaxHistoryEventSizeInKB * 1024
			if eventSize > maxChunkSizeInBytes {
				// This is a fatal error; we can't split a single event into multiple chunks
				return false, fmt.Errorf("orchestration event size of %d bytes exceeds the maximum allowable size of %d bytes", eventSize, maxChunkSizeInBytes)
			}

			currentChunkSizeInBytes += eventSize
			if currentChunkSizeInBytes > maxChunkSizeInBytes {
				// Can't fit this event into the current chunk; return the changes that were applied so far
				return false, nil
			}
		}

		if c.MaxHistoryEventCount > 0 {
			currentChunkLength += 1
			if currentChunkLength > c.MaxHistoryEventCount {
				// Can't fit any more events into the current chunk; return the changes that were applied so far
				return false, nil
			}
		}

		return true, nil
	}

	// Process all the inbox events that were added to the state.
	for len(s.pendingEvents) > 0 {
		e := s.pendingEvents[0]
		if ok, err := verifyAndAddPayloadSize(e); err != nil {
			return OrchestrationStateChanges{}, err
		} else if !ok {
			return changes, nil
		}
		changes.NewEvents = append(changes.NewEvents, e)
		s.pendingEvents = s.pendingEvents[1:]
	}

	// Process all the orchestrator actions that were added to the state.
	for len(s.newActions) > 0 {
		action := s.newActions[0]
		if completedAction := action.GetCompleteOrchestration(); completedAction != nil {
			// Continue-as-new requires us to reset all changes and start over with a new state object.
			if completedAction.OrchestrationStatus == protos.OrchestrationStatus_ORCHESTRATION_STATUS_CONTINUED_AS_NEW {
				resetChanges(true)

				orchestratorStartedEvent := helpers.NewOrchestratorStartedEvent()
				verifyAndAddPayloadSize(orchestratorStartedEvent)

				// Create a new start event based on the old start event, updating just the input
				executionStartedEvent := helpers.NewExecutionStartedEvent(
					s.startEvent.Name,
					string(s.instanceID),
					completedAction.Result,
					s.startEvent.ParentInstance,
					s.startEvent.ParentTraceContext,
				)
				if ok, err := verifyAndAddPayloadSize(executionStartedEvent); err != nil {
					return OrchestrationStateChanges{}, err
				} else if !ok {
					return OrchestrationStateChanges{}, fmt.Errorf("unable to fit both orchestratorStarted and executionStarted events into a single chunk")
				}

				// Replace the current state with a new state. All unprocessed actions that came after this continue-as-new will be lost.
				newState := NewOrchestrationRuntimeState(s.instanceID, []*protos.HistoryEvent{})
				newState.AddEvent(orchestratorStartedEvent)
				newState.AddEvent(executionStartedEvent)
				newState.continuedAsNew = true
				for _, e := range completedAction.CarryoverEvents {
					if err := newState.AddEvent(e); err != nil {
						return OrchestrationStateChanges{}, err
					}
				}
				if len(s.newActions) > 1 {
					log.Warnf("%v: Discarding %d orchestrator action(s) because they were scheduled after the orchestration continued-as-new", s.instanceID, len(s.newActions)-1)
				}
				*s = *newState

				// Return ErrContinuedAsNew to indicate that the caller should start a new orchestrator invocation
				// with the updated state.
				return OrchestrationStateChanges{}, ErrContinuedAsNew
			} else {
				changes.RuntimeStatus = completedAction.OrchestrationStatus
				changes.IsPartial = false

				completedEvent := helpers.NewExecutionCompletedEvent(action.Id, completedAction.OrchestrationStatus, completedAction.Result, completedAction.FailureDetails)
				if ok, err := verifyAndAddPayloadSize(completedEvent); err != nil {
					return OrchestrationStateChanges{}, err
				} else if !ok {
					return changes, nil
				}
				changes.NewEvents = append(changes.NewEvents, completedEvent)
				if s.startEvent.GetParentInstance() != nil {
					msg := OrchestratorMessage{
						TargetInstanceID: s.startEvent.GetParentInstance().OrchestrationInstance.GetInstanceId(),
					}
					if completedAction.OrchestrationStatus == protos.OrchestrationStatus_ORCHESTRATION_STATUS_COMPLETED {
						msg.HistoryEvent = helpers.NewSubOrchestrationCompletedEvent(
							s.startEvent.ParentInstance.TaskScheduledId,
							completedAction.Result,
						)
					} else {
						msg.HistoryEvent = helpers.NewSubOrchestrationFailedEvent(
							s.startEvent.ParentInstance.TaskScheduledId,
							completedAction.FailureDetails,
						)
					}
					changes.NewMessages = append(changes.NewMessages, msg)
				}

				s.newEvents = append(s.newEvents, completedEvent)
				s.completedEvent = completedEvent.GetExecutionCompleted()
				s.completedTime = completedEvent.Timestamp.AsTime()
			}
		} else if createtimer := action.GetCreateTimer(); createtimer != nil {
			timerCreatedEvent := helpers.NewTimerCreatedEvent(action.Id, createtimer.FireAt)
			if _, ok := s.startedTaskIDs[timerCreatedEvent.EventId]; ok {
				log.Debugf("%v: Ignoring duplicate timer created event: %v", s.instanceID, timerCreatedEvent)
			} else if s.IsCompleted() {
				log.Warnf("%v: Dropping timer creation action because the orchestration is %s", s.instanceID, helpers.ToRuntimeStatusString(s.RuntimeStatus()))
			} else {
				if ok, err := verifyAndAddPayloadSize(timerCreatedEvent); err != nil {
					return OrchestrationStateChanges{}, err
				} else if !ok {
					return changes, nil
				}

				s.newEvents = append(s.newEvents, timerCreatedEvent)
				changes.NewEvents = append(changes.NewEvents, timerCreatedEvent)
				changes.NewTimers = append(changes.NewTimers, helpers.NewTimerFiredEvent(action.Id, createtimer.FireAt))
			}
		} else if scheduleTask := action.GetScheduleTask(); scheduleTask != nil {
			scheduledEvent := helpers.NewTaskScheduledEvent(
				action.Id,
				scheduleTask.Name,
				scheduleTask.Version,
				scheduleTask.Input,
				currentTraceContext,
			)
			if _, ok := s.startedTaskIDs[scheduledEvent.EventId]; ok {
				log.Debugf("%v: Ignoring duplicate task scheduled event: %v", s.instanceID, scheduledEvent)
			} else if s.IsCompleted() {
				log.Warnf("%v: Dropping task schedule action because the orchestration is %s", s.instanceID, helpers.ToRuntimeStatusString(s.RuntimeStatus()))
			} else {
				if ok, err := verifyAndAddPayloadSize(scheduledEvent); err != nil {
					return OrchestrationStateChanges{}, err
				} else if !ok {
					return changes, nil
				}

				s.newEvents = append(s.newEvents, scheduledEvent)
				changes.NewEvents = append(changes.NewEvents, scheduledEvent)
				changes.NewTasks = append(changes.NewTasks, scheduledEvent)
			}
		} else if createSO := action.GetCreateSubOrchestration(); createSO != nil {
			// Autogenerate an instance ID for the sub-orchestration if none is provided, using a
			// deterministic algorithm based on the parent instance ID to help enable de-duplication.
			if createSO.InstanceId == "" {
				createSO.InstanceId = fmt.Sprintf("%s:%04x", s.instanceID, action.Id)
			}
			createdEvent := helpers.NewSubOrchestrationCreatedEvent(
				action.Id,
				createSO.Name,
				createSO.Version,
				createSO.Input,
				createSO.InstanceId,
				currentTraceContext,
			)
			if _, ok := s.startedTaskIDs[createdEvent.EventId]; ok {
				log.Debugf("%v: Ignoring duplicate sub-orchestration created event: %v", s.instanceID, createdEvent)
			} else if s.IsCompleted() {
				log.Warnf("%v: Dropping sub-orchestration creation action because the orchestration is %s", s.instanceID, helpers.ToRuntimeStatusString(s.RuntimeStatus()))
			} else {
				if ok, err := verifyAndAddPayloadSize(createdEvent); err != nil {
					return OrchestrationStateChanges{}, err
				} else if !ok {
					return changes, nil
				}

				s.newEvents = append(s.newEvents, createdEvent)
				changes.NewEvents = append(changes.NewEvents, createdEvent)
				startEvent := helpers.NewExecutionStartedEvent(
					createSO.Name,
					createSO.InstanceId,
					createSO.Input,
					helpers.NewParentInfo(action.Id, s.startEvent.Name, string(s.instanceID)),
					currentTraceContext,
				)
				changes.NewMessages = append(changes.NewMessages, OrchestratorMessage{HistoryEvent: startEvent, TargetInstanceID: createSO.InstanceId})
			}
		} else if sendEvent := action.GetSendEvent(); sendEvent != nil {
			e := helpers.NewSendEventEvent(action.Id, sendEvent.Instance.InstanceId, sendEvent.Name, sendEvent.Data)
			if ok, err := verifyAndAddPayloadSize(e); err != nil {
				return OrchestrationStateChanges{}, err
			} else if !ok {
				return changes, nil
			}

			s.newEvents = append(s.newEvents, e)
			changes.NewEvents = append(changes.NewEvents, e)
			changes.NewMessages = append(changes.NewMessages, OrchestratorMessage{HistoryEvent: e, TargetInstanceID: sendEvent.Instance.InstanceId})
		} else if terminate := action.GetTerminateOrchestration(); terminate != nil {
			// Send a message to terminate the target orchestration
			e := helpers.NewExecutionTerminatedEvent(terminate.Reason, terminate.Recurse)
			msg := OrchestratorMessage{TargetInstanceID: terminate.InstanceId, HistoryEvent: e}
			s.newEvents = append(s.newEvents, e)
			changes.NewMessages = append(changes.NewMessages, msg)
		} else {
			return OrchestrationStateChanges{}, fmt.Errorf("unknown action type: %v", action)
		}
		s.newActions = s.newActions[1:]
	}

	// All changes were applied, so we set the IsPartial flag to false.
	changes.IsPartial = false
	return changes, nil
}

// SetFailed adds a failure action to the orchestration state and removes all other pending actions.
func (s *OrchestrationRuntimeState) SetFailed(err error) {
	// Clear the list of pending events since we don't care about these anymore.
	s.pendingEvents = nil

	// Apply an "orchestration failed" action to the current state.
	s.newActions = []*protos.OrchestratorAction{helpers.NewCompleteOrchestrationAction(
		-1,
		protos.OrchestrationStatus_ORCHESTRATION_STATUS_FAILED,
		nil,
		nil,
		helpers.NewTaskFailureDetails(err),
	)}
}

func (s *OrchestrationRuntimeState) InstanceID() api.InstanceID {
	return s.instanceID
}

func (s *OrchestrationRuntimeState) Name() (string, error) {
	if s.startEvent == nil {
		return "", api.ErrNotStarted
	}

	return s.startEvent.Name, nil
}

func (s *OrchestrationRuntimeState) Input() (string, error) {
	if s.startEvent == nil {
		return "", api.ErrNotStarted
	}

	// REVIEW: Should we distinguish between no input and the empty string?
	return s.startEvent.Input.GetValue(), nil
}

func (s *OrchestrationRuntimeState) Output() (string, error) {
	if s.completedEvent == nil {
		return "", api.ErrNotCompleted
	}

	// REVIEW: Should we distinguish between no output and the empty string?
	return s.completedEvent.Result.GetValue(), nil
}

func (s *OrchestrationRuntimeState) RuntimeStatus() protos.OrchestrationStatus {
	if s.startEvent == nil {
		return protos.OrchestrationStatus_ORCHESTRATION_STATUS_PENDING
	} else if s.isSuspended {
		return protos.OrchestrationStatus_ORCHESTRATION_STATUS_SUSPENDED
	} else if s.completedEvent != nil {
		return s.completedEvent.GetOrchestrationStatus()
	}

	return protos.OrchestrationStatus_ORCHESTRATION_STATUS_RUNNING
}

func (s *OrchestrationRuntimeState) CreatedTime() (time.Time, error) {
	if s.startEvent == nil {
		return time.Time{}, api.ErrNotStarted
	}

	return s.createdTime, nil
}

func (s *OrchestrationRuntimeState) LastUpdatedTime() (time.Time, error) {
	if s.startEvent == nil {
		return time.Time{}, api.ErrNotStarted
	}

	return s.lastUpdatedTime, nil
}

func (s *OrchestrationRuntimeState) CompletedTime() (time.Time, error) {
	if s.completedEvent == nil {
		return time.Time{}, api.ErrNotCompleted
	}

	return s.completedTime, nil
}

func (s *OrchestrationRuntimeState) IsCompleted() bool {
	return s.completedEvent != nil
}

func (s *OrchestrationRuntimeState) OldEvents() []*HistoryEvent {
	return s.committedEvents
}

func (s *OrchestrationRuntimeState) NewEvents() []*HistoryEvent {
	return s.newEvents
}

func (s *OrchestrationRuntimeState) FailureDetails() (*TaskFailureDetails, error) {
	if s.completedEvent == nil {
		return nil, api.ErrNotCompleted
	} else if s.completedEvent.FailureDetails == nil {
		return nil, api.ErrNoFailures
	}

	return s.completedEvent.FailureDetails, nil
}

func (s *OrchestrationRuntimeState) ContinuedAsNew() bool {
	return s.continuedAsNew
}

func (s *OrchestrationRuntimeState) String() string {
	return fmt.Sprintf("%v:%v", s.instanceID, helpers.ToRuntimeStatusString(s.RuntimeStatus()))
}

func (s *OrchestrationRuntimeState) getStartedTime() time.Time {
	var startTime time.Time
	if len(s.committedEvents) > 0 {
		startTime = s.committedEvents[0].Timestamp.AsTime()
	} else if len(s.committedEvents) > 0 {
		startTime = s.committedEvents[0].Timestamp.AsTime()
	}
	return startTime
}
