package backend

import (
	"errors"
	"fmt"
	"time"

	"github.com/cgillum/durabletask-go/api"
	"github.com/cgillum/durabletask-go/internal/helpers"
	"github.com/cgillum/durabletask-go/internal/protos"
	"google.golang.org/protobuf/types/known/timestamppb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

var ErrDuplicateEvent = errors.New("duplicate event")

type OrchestrationRuntimeState struct {
	instanceID      api.InstanceID
	newEvents       []*protos.HistoryEvent
	oldEvents       []*protos.HistoryEvent
	pendingTasks    []*protos.HistoryEvent
	pendingTimers   []*protos.HistoryEvent
	pendingMessages []OrchestratorMessage

	startEvent     *protos.ExecutionStartedEvent
	completedEvent *protos.ExecutionCompletedEvent
	createdTime    time.Time
	completedTime  time.Time
	continuedAsNew bool

	CustomStatus *wrapperspb.StringValue
}

type OrchestratorMessage struct {
	HistoryEvent     *protos.HistoryEvent
	TargetInstanceID string
}

func NewOrchestrationRuntimeState(instanceID api.InstanceID, existingHistory []*protos.HistoryEvent) *OrchestrationRuntimeState {
	s := &OrchestrationRuntimeState{
		instanceID: instanceID,
		oldEvents:  make([]*protos.HistoryEvent, 0, len(existingHistory)),
		newEvents:  make([]*protos.HistoryEvent, 0, 10),
	}

	for _, e := range existingHistory {
		s.addEvent(e, false)
	}

	return s
}

// AddEvent appends a new history event to the orchestration history
func (s *OrchestrationRuntimeState) AddEvent(e *protos.HistoryEvent) error {
	return s.addEvent(e, true)
}

func (s *OrchestrationRuntimeState) addEvent(e *protos.HistoryEvent, isNew bool) error {
	if startEvent, ok := e.GetEventType().(*protos.HistoryEvent_ExecutionStarted); ok {
		if s.startEvent != nil {
			return ErrDuplicateEvent
		}
		s.startEvent = startEvent.ExecutionStarted
		s.createdTime = e.Timestamp.AsTime()
	} else if completedEvent, ok := e.GetEventType().(*protos.HistoryEvent_ExecutionCompleted); ok {
		if s.completedEvent != nil {
			return ErrDuplicateEvent
		}
		s.completedEvent = completedEvent.ExecutionCompleted
		s.completedTime = e.Timestamp.AsTime()
	} else {
		// TODO: Check for other possible duplicates using task IDs
	}

	if isNew {
		s.newEvents = append(s.newEvents, e)
	} else {
		s.oldEvents = append(s.oldEvents, e)
	}

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
func (s *OrchestrationRuntimeState) ApplyActions(actions []*protos.OrchestratorAction) (bool, error) {
	for _, action := range actions {
		if completedAction := action.GetCompleteOrchestration(); completedAction != nil {
			if completedAction.OrchestrationStatus == protos.OrchestrationStatus_ORCHESTRATION_STATUS_CONTINUED_AS_NEW {
				newState := NewOrchestrationRuntimeState(s.instanceID, []*protos.HistoryEvent{})
				newState.continuedAsNew = true

				// Duplicate the start event info, updating just the input
				newState.AddEvent(
					helpers.NewExecutionStartedEvent(-1, s.startEvent.Name, string(s.instanceID), completedAction.Result, s.startEvent.ParentInstance),
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
			s.pendingTimers = append(s.pendingTasks, helpers.NewTimerFiredEvent(action.Id, createtimer.FireAt))
		} else if scheduleTask := action.GetScheduleTask(); scheduleTask != nil {
			scheduledEvent := helpers.NewTaskScheduledEvent(action.Id, scheduleTask.Name, scheduleTask.Version, scheduleTask.Input)
			s.AddEvent(scheduledEvent)
			s.pendingTasks = append(s.pendingTasks, scheduledEvent)
		} else if createSO := action.GetCreateSubOrchestration(); createSO != nil {
			s.AddEvent(helpers.NewSubOrchestrationCreatedEvent(action.Id, createSO.Name, createSO.Version, createSO.Input, createSO.InstanceId))
			startEvent := helpers.NewExecutionStartedEvent(-1, createSO.Name, createSO.InstanceId, createSO.Input, helpers.NewParentInfo(action.Id, s.startEvent.Name, string(s.instanceID)))
			s.pendingMessages = append(s.pendingMessages, OrchestratorMessage{HistoryEvent: startEvent, TargetInstanceID: createSO.InstanceId})
		} else if sendEvent := action.GetSendEvent(); sendEvent != nil {
			e := helpers.NewSendEventEvent(action.Id, sendEvent.Instance.InstanceId, sendEvent.Name, sendEvent.Data)
			s.AddEvent(e)
			s.pendingMessages = append(s.pendingMessages, OrchestratorMessage{HistoryEvent: e, TargetInstanceID: sendEvent.Instance.InstanceId})
		} else {
			return false, fmt.Errorf("Unknown action type: %v", action)
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

func (s *OrchestrationRuntimeState) CompletedTime() (time.Time, error) {
	if s.completedEvent == nil {
		return time.Time{}, api.ErrNotCompleted
	}

	return s.completedTime, nil
}

func (s *OrchestrationRuntimeState) IsCompleted() bool {
	return s.completedEvent != nil
}

func (s *OrchestrationRuntimeState) OldEvents() []*protos.HistoryEvent {
	return s.oldEvents
}

func (s *OrchestrationRuntimeState) NewEvents() []*protos.HistoryEvent {
	return s.newEvents
}

func (s *OrchestrationRuntimeState) FailureDetails() (*protos.TaskFailureDetails, error) {
	if s.completedEvent == nil {
		return nil, api.ErrNotCompleted
	} else if s.completedEvent.FailureDetails == nil {
		return nil, api.ErrNoFailures
	}

	return s.completedEvent.FailureDetails, nil
}

func (s *OrchestrationRuntimeState) PendingTimers() []*protos.HistoryEvent {
	return s.pendingTimers
}

func (s *OrchestrationRuntimeState) PendingTasks() []*protos.HistoryEvent {
	return s.pendingTasks
}

func (s *OrchestrationRuntimeState) PendingMessages() []OrchestratorMessage {
	return s.pendingMessages
}

func (s *OrchestrationRuntimeState) ContinuedAsNew() bool {
	return s.continuedAsNew
}

func (s *OrchestrationRuntimeState) String() string {
	return fmt.Sprintf("%v:%v", s.instanceID, ToRuntimeStatusString(s.RuntimeStatus()))
}

func ToRuntimeStatusString(status protos.OrchestrationStatus) string {
	name := protos.OrchestrationStatus_name[int32(status)]
	return name[len("ORCHESTRATION_STATUS_"):]
}

func FromRuntimeStatusString(status string) protos.OrchestrationStatus {
	runtimeStatus := "ORCHESTRATION_STATUS_" + status
	return protos.OrchestrationStatus(protos.OrchestrationStatus_value[runtimeStatus])
}
