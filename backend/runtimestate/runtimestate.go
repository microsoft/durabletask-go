package runtimestate

import (
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"google.golang.org/protobuf/types/known/timestamppb"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/dapr/durabletask-go/api"
	"github.com/dapr/durabletask-go/api/helpers"
	"github.com/dapr/durabletask-go/api/protos"
	"github.com/dapr/kit/ptr"
)

var ErrDuplicateEvent = errors.New("duplicate event")

func NewOrchestrationRuntimeState(instanceID string, customStatus *wrapperspb.StringValue, existingHistory []*protos.HistoryEvent) *protos.OrchestrationRuntimeState {
	s := &protos.OrchestrationRuntimeState{
		InstanceId:   instanceID,
		OldEvents:    make([]*protos.HistoryEvent, 0, len(existingHistory)),
		NewEvents:    make([]*protos.HistoryEvent, 0, 10),
		CustomStatus: customStatus,
	}

	for _, e := range existingHistory {
		addEvent(s, e, false)
	}

	return s
}

// AddEvent appends a new history event to the orchestration history
func AddEvent(s *protos.OrchestrationRuntimeState, e *protos.HistoryEvent) error {
	return addEvent(s, e, true)
}

func addEvent(s *protos.OrchestrationRuntimeState, e *protos.HistoryEvent, isNew bool) error {
	if startEvent := e.GetExecutionStarted(); startEvent != nil {
		if s.StartEvent != nil {
			return ErrDuplicateEvent
		}
		s.StartEvent = startEvent
		s.CreatedTime = timestamppb.New(e.Timestamp.AsTime())
	} else if completedEvent := e.GetExecutionCompleted(); completedEvent != nil {
		if s.CompletedEvent != nil {
			return ErrDuplicateEvent
		}
		s.CompletedEvent = completedEvent
		s.CompletedTime = timestamppb.New(e.Timestamp.AsTime())
	} else if e.GetExecutionSuspended() != nil {
		s.IsSuspended = true
	} else if e.GetExecutionResumed() != nil {
		s.IsSuspended = false
	} else {
		// TODO: Check for other possible duplicates using task IDs
	}

	if isNew {
		s.NewEvents = append(s.NewEvents, e)
	} else {
		s.OldEvents = append(s.OldEvents, e)
	}

	s.LastUpdatedTime = timestamppb.New(e.Timestamp.AsTime())
	return nil
}

func IsValid(s *protos.OrchestrationRuntimeState) bool {
	if len(s.OldEvents) == 0 && len(s.NewEvents) == 0 {
		// empty orchestration state
		return true
	} else if s.StartEvent != nil {
		// orchestration history has a start event
		return true
	}
	return false
}

// ApplyActions takes a set of actions and updates its internal state, including populating the outbox.
func ApplyActions(s *protos.OrchestrationRuntimeState, customStatus *wrapperspb.StringValue, actions []*protos.OrchestratorAction, currentTraceContext *protos.TraceContext) (bool, error) {
	s.CustomStatus = customStatus
	for _, action := range actions {
		if completedAction := action.GetCompleteOrchestration(); completedAction != nil {
			if completedAction.OrchestrationStatus == protos.OrchestrationStatus_ORCHESTRATION_STATUS_CONTINUED_AS_NEW {
				newState := NewOrchestrationRuntimeState(s.InstanceId, customStatus, []*protos.HistoryEvent{})
				newState.ContinuedAsNew = true
				AddEvent(newState, &protos.HistoryEvent{
					EventId:   -1,
					Timestamp: timestamppb.Now(),
					EventType: &protos.HistoryEvent_OrchestratorStarted{
						OrchestratorStarted: &protos.OrchestratorStartedEvent{},
					},
					Router: action.Router,
				})

				// Duplicate the start event info, updating just the input
				AddEvent(newState,
					&protos.HistoryEvent{
						EventId:   -1,
						Timestamp: timestamppb.New(time.Now()),
						EventType: &protos.HistoryEvent_ExecutionStarted{
							ExecutionStarted: &protos.ExecutionStartedEvent{
								Name:           s.StartEvent.Name,
								ParentInstance: s.StartEvent.ParentInstance,
								Input:          completedAction.Result,
								OrchestrationInstance: &protos.OrchestrationInstance{
									InstanceId:  s.InstanceId,
									ExecutionId: wrapperspb.String(uuid.New().String()),
								},
								ParentTraceContext: s.StartEvent.ParentTraceContext,
							},
						},
						Router: action.Router,
					},
				)

				// Unprocessed "carryover" events
				for _, e := range completedAction.CarryoverEvents {
					AddEvent(newState, e)
				}

				// Overwrite the current state object with a new one
				*s = *newState

				// ignore all remaining actions
				return true, nil
			} else {
				AddEvent(s, &protos.HistoryEvent{
					EventId:   action.Id,
					Timestamp: timestamppb.Now(),
					EventType: &protos.HistoryEvent_ExecutionCompleted{
						ExecutionCompleted: &protos.ExecutionCompletedEvent{
							OrchestrationStatus: completedAction.OrchestrationStatus,
							Result:              completedAction.Result,
							FailureDetails:      completedAction.FailureDetails,
						},
					},
					Router: action.Router,
				})
				if s.StartEvent.GetParentInstance() != nil {
					// Create a router for the completion event that routes back to the parent
					var completionRouter *protos.TaskRouter
					if action.Router != nil {
						var parentAppID *string

						allEvents := append(s.OldEvents, s.NewEvents...)
						for _, event := range allEvents {
							if es := event.GetExecutionStarted(); es != nil && event.GetRouter() != nil {
								parentAppID = ptr.Of(event.GetRouter().GetSourceAppID())
								break
							}
						}

						if parentAppID != nil {
							completionRouter = &protos.TaskRouter{
								SourceAppID: action.Router.SourceAppID,
								TargetAppID: parentAppID,
							}
						} else {
							completionRouter = action.Router
						}
					}

					msg := &protos.OrchestrationRuntimeStateMessage{
						HistoryEvent: &protos.HistoryEvent{
							EventId:   -1,
							Timestamp: timestamppb.Now(),
							Router:    completionRouter,
						},
						TargetInstanceID: s.StartEvent.GetParentInstance().OrchestrationInstance.InstanceId,
					}
					if completedAction.OrchestrationStatus == protos.OrchestrationStatus_ORCHESTRATION_STATUS_COMPLETED {
						msg.HistoryEvent.EventType = &protos.HistoryEvent_SubOrchestrationInstanceCompleted{
							SubOrchestrationInstanceCompleted: &protos.SubOrchestrationInstanceCompletedEvent{
								TaskScheduledId: s.StartEvent.ParentInstance.TaskScheduledId,
								Result:          completedAction.Result,
							},
						}
					} else {
						// TODO: What is the expected result for termination?
						msg.HistoryEvent.EventType = &protos.HistoryEvent_SubOrchestrationInstanceFailed{
							SubOrchestrationInstanceFailed: &protos.SubOrchestrationInstanceFailedEvent{
								TaskScheduledId: s.StartEvent.ParentInstance.TaskScheduledId,
								FailureDetails:  completedAction.FailureDetails,
							},
						}
					}
					s.PendingMessages = append(s.PendingMessages, msg)
				}
			}
		} else if createtimer := action.GetCreateTimer(); createtimer != nil {
			AddEvent(s, &protos.HistoryEvent{
				EventId:   action.Id,
				Timestamp: timestamppb.New(time.Now()),
				EventType: &protos.HistoryEvent_TimerCreated{
					TimerCreated: &protos.TimerCreatedEvent{
						FireAt: createtimer.FireAt,
						Name:   createtimer.Name,
					},
				},
				Router: action.Router,
			})
			// TODO cant pass trace context
			s.PendingTimers = append(s.PendingTimers, &protos.HistoryEvent{
				EventId:   -1,
				Timestamp: timestamppb.New(time.Now()),
				EventType: &protos.HistoryEvent_TimerFired{
					TimerFired: &protos.TimerFiredEvent{
						TimerId: action.Id,
						FireAt:  createtimer.FireAt,
					},
				},
			})
		} else if scheduleTask := action.GetScheduleTask(); scheduleTask != nil {
			scheduledEvent := &protos.HistoryEvent{
				EventId:   action.Id,
				Timestamp: timestamppb.New(time.Now()),
				EventType: &protos.HistoryEvent_TaskScheduled{
					TaskScheduled: &protos.TaskScheduledEvent{
						Name:               scheduleTask.Name,
						TaskExecutionId:    scheduleTask.TaskExecutionId,
						Version:            scheduleTask.Version,
						Input:              scheduleTask.Input,
						ParentTraceContext: currentTraceContext,
					},
				},
				Router: action.Router,
			}
			AddEvent(s, scheduledEvent)
			s.PendingTasks = append(s.PendingTasks, scheduledEvent)
		} else if createSO := action.GetCreateSubOrchestration(); createSO != nil {
			// Autogenerate an instance ID for the sub-orchestration if none is provided, using a
			// deterministic algorithm based on the parent instance ID to help enable de-duplication.
			if createSO.InstanceId == "" {
				createSO.InstanceId = fmt.Sprintf("%s:%04x", s.InstanceId, action.Id)
			}
			AddEvent(s, &protos.HistoryEvent{
				EventId:   action.Id,
				Timestamp: timestamppb.New(time.Now()),
				EventType: &protos.HistoryEvent_SubOrchestrationInstanceCreated{
					SubOrchestrationInstanceCreated: &protos.SubOrchestrationInstanceCreatedEvent{
						Name:               createSO.Name,
						Version:            createSO.Version,
						Input:              createSO.Input,
						InstanceId:         createSO.InstanceId,
						ParentTraceContext: currentTraceContext,
					},
				},
				Router: action.Router,
			})
			startEvent := &protos.HistoryEvent{
				EventId:   -1,
				Timestamp: timestamppb.New(time.Now()),
				EventType: &protos.HistoryEvent_ExecutionStarted{
					ExecutionStarted: &protos.ExecutionStartedEvent{
						Name: createSO.Name,
						ParentInstance: &protos.ParentInstanceInfo{
							TaskScheduledId:       action.Id,
							Name:                  wrapperspb.String(s.StartEvent.Name),
							OrchestrationInstance: &protos.OrchestrationInstance{InstanceId: string(s.InstanceId)},
						},
						Input: createSO.Input,
						OrchestrationInstance: &protos.OrchestrationInstance{
							InstanceId:  createSO.InstanceId,
							ExecutionId: wrapperspb.String(uuid.New().String()),
						},
						ParentTraceContext: currentTraceContext,
					},
				},
				Router: action.Router,
			}

			s.PendingMessages = append(s.PendingMessages, &protos.OrchestrationRuntimeStateMessage{HistoryEvent: startEvent, TargetInstanceID: createSO.InstanceId})
		} else if sendEvent := action.GetSendEvent(); sendEvent != nil {
			e := &protos.HistoryEvent{
				EventId:   action.Id,
				Timestamp: timestamppb.New(time.Now()),
				EventType: &protos.HistoryEvent_EventSent{
					EventSent: &protos.EventSentEvent{
						InstanceId: sendEvent.Instance.InstanceId,
						Name:       sendEvent.Name,
						Input:      sendEvent.Data,
					},
				},
				Router: action.Router,
			}
			AddEvent(s, e)
			s.PendingMessages = append(s.PendingMessages, &protos.OrchestrationRuntimeStateMessage{HistoryEvent: e, TargetInstanceID: sendEvent.Instance.InstanceId})
		} else if terminate := action.GetTerminateOrchestration(); terminate != nil {
			// Send a message to terminate the target orchestration
			msg := &protos.OrchestrationRuntimeStateMessage{
				TargetInstanceID: terminate.InstanceId,
				HistoryEvent: &protos.HistoryEvent{
					EventId:   -1,
					Timestamp: timestamppb.Now(),
					EventType: &protos.HistoryEvent_ExecutionTerminated{
						ExecutionTerminated: &protos.ExecutionTerminatedEvent{
							Input:   terminate.Reason,
							Recurse: terminate.Recurse,
						},
					},
					Router: action.Router,
				},
			}
			s.PendingMessages = append(s.PendingMessages, msg)
		} else {
			return false, fmt.Errorf("unknown action type: %v", action)
		}
	}

	return false, nil
}

func Name(s *protos.OrchestrationRuntimeState) (string, error) {
	if s.StartEvent == nil {
		return "", api.ErrNotStarted
	}

	return s.StartEvent.Name, nil
}

func Input(s *protos.OrchestrationRuntimeState) (*wrapperspb.StringValue, error) {
	if s.StartEvent == nil {
		return nil, api.ErrNotStarted
	}

	// REVIEW: Should we distinguish between no input and the empty string?
	return s.StartEvent.Input, nil
}

func Output(s *protos.OrchestrationRuntimeState) (*wrapperspb.StringValue, error) {
	if s.CompletedEvent == nil {
		return nil, api.ErrNotCompleted
	}

	// REVIEW: Should we distinguish between no output and the empty string?
	return s.CompletedEvent.Result, nil
}

func RuntimeStatus(s *protos.OrchestrationRuntimeState) protos.OrchestrationStatus {
	if s.StartEvent == nil {
		return protos.OrchestrationStatus_ORCHESTRATION_STATUS_PENDING
	} else if s.IsSuspended {
		return protos.OrchestrationStatus_ORCHESTRATION_STATUS_SUSPENDED
	} else if s.CompletedEvent != nil {
		return s.CompletedEvent.GetOrchestrationStatus()
	}

	return protos.OrchestrationStatus_ORCHESTRATION_STATUS_RUNNING
}

func CreatedTime(s *protos.OrchestrationRuntimeState) (time.Time, error) {
	if s.StartEvent == nil {
		return time.Time{}, api.ErrNotStarted
	}

	return s.CreatedTime.AsTime(), nil
}

func LastUpdatedTime(s *protos.OrchestrationRuntimeState) (time.Time, error) {
	if s.StartEvent == nil {
		return time.Time{}, api.ErrNotStarted
	}

	return s.LastUpdatedTime.AsTime(), nil
}

func CompletedTime(s *protos.OrchestrationRuntimeState) (time.Time, error) {
	if s.CompletedEvent == nil {
		return time.Time{}, api.ErrNotCompleted
	}

	return s.CompletedTime.AsTime(), nil
}

func FailureDetails(s *protos.OrchestrationRuntimeState) (*protos.TaskFailureDetails, error) {
	if s.CompletedEvent == nil {
		return nil, api.ErrNotCompleted
	} else if s.CompletedEvent.FailureDetails == nil {
		return nil, api.ErrNoFailures
	}

	return s.CompletedEvent.FailureDetails, nil
}

// useful for abruptly stopping any execution of an orchestration from the backend
func CancelPending(s *protos.OrchestrationRuntimeState) {
	s.NewEvents = []*protos.HistoryEvent{}
	s.PendingMessages = []*protos.OrchestrationRuntimeStateMessage{}
	s.PendingTasks = []*protos.HistoryEvent{}
	s.PendingTimers = []*protos.HistoryEvent{}
}

func String(s *protos.OrchestrationRuntimeState) string {
	return fmt.Sprintf("%v:%v", s.InstanceId, helpers.ToRuntimeStatusString(RuntimeStatus(s)))
}

func GetStartedTime(s *protos.OrchestrationRuntimeState) time.Time {
	var startTime time.Time
	if len(s.OldEvents) > 0 {
		startTime = s.OldEvents[0].Timestamp.AsTime()
	} else if len(s.NewEvents) > 0 {
		startTime = s.NewEvents[0].Timestamp.AsTime()
	}
	return startTime
}

func IsCompleted(s *protos.OrchestrationRuntimeState) bool {
	return s.CompletedEvent != nil
}
