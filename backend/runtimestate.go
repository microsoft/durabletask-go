package backend

import (
	"errors"
	"fmt"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/microsoft/durabletask-go/api"
	"github.com/microsoft/durabletask-go/internal/helpers"
	"github.com/microsoft/durabletask-go/internal/protos"
)

var ErrDuplicateEvent = errors.New("duplicate event")

type OrchestrationRuntimeState struct {
	instanceID      api.InstanceID
	newEvents       []*protos.HistoryEvent
	oldEvents       []*protos.HistoryEvent
	pendingTasks    []*protos.HistoryEvent
	pendingTimers   []*protos.HistoryEvent
	pendingMessages []OrchestratorMessage

	startEvent      *protos.ExecutionStartedEvent
	completedEvent  *protos.ExecutionCompletedEvent
	createdTime     time.Time
	lastUpdatedTime time.Time
	completedTime   time.Time
	continuedAsNew  bool
	isSuspended     bool

	CustomStatus *wrapperspb.StringValue
}

type OrchestratorMessage struct {
	HistoryEvent     *HistoryEvent
	TargetInstanceID string
}

func NewOrchestrationRuntimeState(instanceID api.InstanceID, existingHistory []*HistoryEvent) *OrchestrationRuntimeState {
	s := &OrchestrationRuntimeState{
		instanceID: instanceID,
		oldEvents:  make([]*HistoryEvent, 0, len(existingHistory)),
		newEvents:  make([]*HistoryEvent, 0, 10),
	}

	for _, e := range existingHistory {
		s.addEvent(e, false)
	}

	return s
}

// AddEvent appends a new history event to the orchestration history
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
	} else {
		// TODO: Check for other possible duplicates using task IDs
	}

	if isNew {
		s.newEvents = append(s.newEvents, e)
	} else {
		s.oldEvents = append(s.oldEvents, e)
	}

	s.lastUpdatedTime = e.Timestamp.AsTime()
	return nil
}

func (s *OrchestrationRuntimeState) IsValid() bool {
	if len(s.oldEvents) == 0 && len(s.newEvents) == 0 {
		// empty orchestration state
		return true
	} else if s.startEvent != nil {
		// orchestration history has a start event
		return true
	}
	return false
}

// ApplyActions takes a set of actions and updates its internal state, including populating the outbox.
func (s *OrchestrationRuntimeState) ApplyActions(actions []*protos.OrchestratorAction, currentTraceContext *protos.TraceContext) (bool, error) {
	for _, action := range actions {
		if completedAction := action.GetCompleteOrchestration(); completedAction != nil {
			if completedAction.OrchestrationStatus == protos.OrchestrationStatus_ORCHESTRATION_STATUS_CONTINUED_AS_NEW {
				newState := NewOrchestrationRuntimeState(s.instanceID, []*protos.HistoryEvent{})
				newState.continuedAsNew = true
				newState.AddEvent(helpers.NewOrchestratorStartedEvent())

				// Duplicate the start event info, updating just the input
				newState.AddEvent(
					helpers.NewExecutionStartedEvent(
						s.startEvent.Name,
						string(s.instanceID),
						completedAction.Result,
						s.startEvent.ParentInstance,
						s.startEvent.ParentTraceContext,
						nil,
					),
				)

				// Unprocessed "carryover" events
				for _, e := range completedAction.CarryoverEvents {
					newState.AddEvent(e)
				}

				// Overwrite the current state object with a new one
				*s = *newState

				// ignore all remaining actions
				return true, nil
			} else {
				s.AddEvent(helpers.NewExecutionCompletedEvent(action.Id, completedAction.OrchestrationStatus, completedAction.Result, completedAction.FailureDetails))
				if s.startEvent.GetParentInstance() != nil {
					msg := OrchestratorMessage{
						HistoryEvent:     &protos.HistoryEvent{EventId: -1, Timestamp: timestamppb.Now()},
						TargetInstanceID: s.startEvent.GetParentInstance().OrchestrationInstance.GetInstanceId(),
					}
					if completedAction.OrchestrationStatus == protos.OrchestrationStatus_ORCHESTRATION_STATUS_COMPLETED {
						msg.HistoryEvent.EventType = &protos.HistoryEvent_SubOrchestrationInstanceCompleted{
							SubOrchestrationInstanceCompleted: &protos.SubOrchestrationInstanceCompletedEvent{
								TaskScheduledId: s.startEvent.ParentInstance.TaskScheduledId,
								Result:          completedAction.Result,
							},
						}
					} else {
						// TODO: What is the expected result for termination?
						msg.HistoryEvent.EventType = &protos.HistoryEvent_SubOrchestrationInstanceFailed{
							SubOrchestrationInstanceFailed: &protos.SubOrchestrationInstanceFailedEvent{
								TaskScheduledId: s.startEvent.ParentInstance.TaskScheduledId,
								FailureDetails:  completedAction.FailureDetails,
							},
						}
					}
					s.pendingMessages = append(s.pendingMessages, msg)
				}
			}
		} else if createtimer := action.GetCreateTimer(); createtimer != nil {
			s.AddEvent(helpers.NewTimerCreatedEvent(action.Id, createtimer.FireAt))
			s.pendingTimers = append(s.pendingTimers, helpers.NewTimerFiredEvent(action.Id, createtimer.FireAt, currentTraceContext))
		} else if scheduleTask := action.GetScheduleTask(); scheduleTask != nil {
			scheduledEvent := helpers.NewTaskScheduledEvent(
				action.Id,
				scheduleTask.Name,
				scheduleTask.Version,
				scheduleTask.Input,
				currentTraceContext,
			)
			s.AddEvent(scheduledEvent)
			s.pendingTasks = append(s.pendingTasks, scheduledEvent)
		} else if createSO := action.GetCreateSubOrchestration(); createSO != nil {
			// Autogenerate an instance ID for the sub-orchestration if none is provided, using a
			// deterministic algorithm based on the parent instance ID to help enable de-duplication.
			if createSO.InstanceId == "" {
				createSO.InstanceId = fmt.Sprintf("%s:%04x", s.instanceID, action.Id)
			}
			s.AddEvent(helpers.NewSubOrchestrationCreatedEvent(
				action.Id,
				createSO.Name,
				createSO.Version,
				createSO.Input,
				createSO.InstanceId,
				currentTraceContext))
			startEvent := helpers.NewExecutionStartedEvent(
				createSO.Name,
				createSO.InstanceId,
				createSO.Input,
				helpers.NewParentInfo(action.Id, s.startEvent.Name, string(s.instanceID)),
				currentTraceContext,
				nil,
			)
			s.pendingMessages = append(s.pendingMessages, OrchestratorMessage{HistoryEvent: startEvent, TargetInstanceID: createSO.InstanceId})
		} else if sendEvent := action.GetSendEvent(); sendEvent != nil {
			e := helpers.NewSendEventEvent(action.Id, sendEvent.Instance.InstanceId, sendEvent.Name, sendEvent.Data)
			s.AddEvent(e)
			s.pendingMessages = append(s.pendingMessages, OrchestratorMessage{HistoryEvent: e, TargetInstanceID: sendEvent.Instance.InstanceId})
		} else if terminate := action.GetTerminateOrchestration(); terminate != nil {
			// Send a message to terminate the target orchestration
			msg := OrchestratorMessage{
				TargetInstanceID: terminate.InstanceId,
				HistoryEvent:     helpers.NewExecutionTerminatedEvent(terminate.Reason, terminate.Recurse),
			}
			s.pendingMessages = append(s.pendingMessages, msg)
		} else {
			return false, fmt.Errorf("unknown action type: %v", action)
		}
	}

	return false, nil
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
	return s.oldEvents
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

func (s *OrchestrationRuntimeState) PendingTimers() []*HistoryEvent {
	return s.pendingTimers
}

func (s *OrchestrationRuntimeState) PendingTasks() []*HistoryEvent {
	return s.pendingTasks
}

func (s *OrchestrationRuntimeState) PendingMessages() []OrchestratorMessage {
	return s.pendingMessages
}

func (s *OrchestrationRuntimeState) ContinuedAsNew() bool {
	return s.continuedAsNew
}

func (s *OrchestrationRuntimeState) String() string {
	return fmt.Sprintf("%v:%v", s.instanceID, helpers.ToRuntimeStatusString(s.RuntimeStatus()))
}

func (s *OrchestrationRuntimeState) getStartedTime() time.Time {
	var startTime time.Time
	if len(s.oldEvents) > 0 {
		startTime = s.oldEvents[0].Timestamp.AsTime()
	} else if len(s.newEvents) > 0 {
		startTime = s.newEvents[0].Timestamp.AsTime()
	}
	return startTime
}
