package helpers

import (
	"time"

	"github.com/google/uuid"
	"google.golang.org/protobuf/types/known/timestamppb"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/microsoft/durabletask-go/internal/protos"
)

func NewExecutionStartedEvent(
	name string,
	instanceId string,
	input *wrapperspb.StringValue,
	parent *protos.ParentInstanceInfo,
	parentTraceContext *protos.TraceContext,
	scheduledStartTimeStamp *timestamppb.Timestamp,
	version ...*wrapperspb.StringValue,
) *protos.HistoryEvent {
	u, err := uuid.NewV7()
	if err != nil {
		u = uuid.New()
	}
	event := &protos.HistoryEvent{
		EventId:   -1,
		Timestamp: timestamppb.New(time.Now()),
		EventType: &protos.HistoryEvent_ExecutionStarted{
			ExecutionStarted: &protos.ExecutionStartedEvent{
				Name:           name,
				ParentInstance: parent,
				Input:          input,
				OrchestrationInstance: &protos.OrchestrationInstance{
					InstanceId:  instanceId,
					ExecutionId: wrapperspb.String(u.String()),
				},
				ParentTraceContext:      parentTraceContext,
				ScheduledStartTimestamp: scheduledStartTimeStamp,
			},
		},
	}
	if len(version) > 0 {
		event.GetExecutionStarted().Version = version[0]
	}
	return event
}

func NewExecutionTerminatedEvent(rawReason *wrapperspb.StringValue, recurse bool) *protos.HistoryEvent {
	return &protos.HistoryEvent{
		EventId:   -1,
		Timestamp: timestamppb.Now(),
		EventType: &protos.HistoryEvent_ExecutionTerminated{
			ExecutionTerminated: &protos.ExecutionTerminatedEvent{
				Input:   rawReason,
				Recurse: recurse,
			},
		},
	}
}

func NewOrchestratorStartedEvent() *protos.HistoryEvent {
	return &protos.HistoryEvent{
		EventId:   -1,
		Timestamp: timestamppb.Now(),
		EventType: &protos.HistoryEvent_OrchestratorStarted{
			OrchestratorStarted: &protos.OrchestratorStartedEvent{},
		},
	}
}

func NewEventRaisedEvent(name string, rawInput *wrapperspb.StringValue) *protos.HistoryEvent {
	return &protos.HistoryEvent{
		EventId:   -1,
		Timestamp: timestamppb.New(time.Now()),
		EventType: &protos.HistoryEvent_EventRaised{
			EventRaised: &protos.EventRaisedEvent{Name: name, Input: rawInput},
		},
	}
}

func NewTaskScheduledEvent(
	taskID int32,
	name string,
	version *wrapperspb.StringValue,
	rawInput *wrapperspb.StringValue,
	tc *protos.TraceContext,
) *protos.HistoryEvent {
	return &protos.HistoryEvent{
		EventId:   taskID,
		Timestamp: timestamppb.New(time.Now()),
		EventType: &protos.HistoryEvent_TaskScheduled{
			TaskScheduled: &protos.TaskScheduledEvent{
				Name:               name,
				Version:            version,
				Input:              rawInput,
				ParentTraceContext: tc,
			},
		},
	}
}

func NewTaskCompletedEvent(taskID int32, result *wrapperspb.StringValue) *protos.HistoryEvent {
	return &protos.HistoryEvent{
		EventId:   -1,
		Timestamp: timestamppb.New(time.Now()),
		EventType: &protos.HistoryEvent_TaskCompleted{
			TaskCompleted: &protos.TaskCompletedEvent{
				TaskScheduledId: taskID,
				Result:          result,
			},
		},
	}
}

func NewTaskFailedEvent(taskID int32, failureDetails *protos.TaskFailureDetails) *protos.HistoryEvent {
	return &protos.HistoryEvent{
		EventId:   -1,
		Timestamp: timestamppb.Now(),
		EventType: &protos.HistoryEvent_TaskFailed{
			TaskFailed: &protos.TaskFailedEvent{
				TaskScheduledId: taskID,
				FailureDetails:  failureDetails,
			},
		},
	}
}

func NewTimerCreatedEvent(eventID int32, fireAt *timestamppb.Timestamp) *protos.HistoryEvent {
	return &protos.HistoryEvent{
		EventId:   eventID,
		Timestamp: timestamppb.New(time.Now()),
		EventType: &protos.HistoryEvent_TimerCreated{
			TimerCreated: &protos.TimerCreatedEvent{FireAt: fireAt},
		},
	}
}

func NewTimerFiredEvent(
	timerID int32,
	fireAt *timestamppb.Timestamp,
	parentTraceContext *protos.TraceContext,
) *protos.HistoryEvent {
	return &protos.HistoryEvent{
		EventId:   -1,
		Timestamp: timestamppb.New(time.Now()),
		EventType: &protos.HistoryEvent_TimerFired{
			TimerFired: &protos.TimerFiredEvent{
				TimerId: timerID,
				FireAt:  fireAt,
			},
		},
	}
}

func NewSubOrchestrationCreatedEvent(
	eventID int32,
	name string,
	version *wrapperspb.StringValue,
	rawInput *wrapperspb.StringValue,
	instanceID string,
	parentTraceContext *protos.TraceContext,
) *protos.HistoryEvent {
	return &protos.HistoryEvent{
		EventId:   eventID,
		Timestamp: timestamppb.New(time.Now()),
		EventType: &protos.HistoryEvent_SubOrchestrationInstanceCreated{
			SubOrchestrationInstanceCreated: &protos.SubOrchestrationInstanceCreatedEvent{
				Name:               name,
				Version:            version,
				Input:              rawInput,
				InstanceId:         instanceID,
				ParentTraceContext: parentTraceContext,
			},
		},
	}
}

func NewSendEventEvent(eventID int32, instanceID string, name string, rawInput *wrapperspb.StringValue) *protos.HistoryEvent {
	return &protos.HistoryEvent{
		EventId:   eventID,
		Timestamp: timestamppb.New(time.Now()),
		EventType: &protos.HistoryEvent_EventSent{
			EventSent: &protos.EventSentEvent{
				InstanceId: instanceID,
				Name:       name,
				Input:      rawInput,
			},
		},
	}
}

func NewSuspendOrchestrationEvent(reason string) *protos.HistoryEvent {
	var input *wrapperspb.StringValue
	if reason != "" {
		input = wrapperspb.String(reason)
	}
	return &protos.HistoryEvent{
		EventId:   -1,
		Timestamp: timestamppb.New(time.Now()),
		EventType: &protos.HistoryEvent_ExecutionSuspended{
			ExecutionSuspended: &protos.ExecutionSuspendedEvent{
				Input: input,
			},
		},
	}
}

func NewResumeOrchestrationEvent(reason string) *protos.HistoryEvent {
	var input *wrapperspb.StringValue
	if reason != "" {
		input = wrapperspb.String(reason)
	}
	return &protos.HistoryEvent{
		EventId:   -1,
		Timestamp: timestamppb.New(time.Now()),
		EventType: &protos.HistoryEvent_ExecutionResumed{
			ExecutionResumed: &protos.ExecutionResumedEvent{
				Input: input,
			},
		},
	}
}

func NewParentInfo(taskID int32, name string, iid string) *protos.ParentInstanceInfo {
	return &protos.ParentInstanceInfo{
		TaskScheduledId:       taskID,
		Name:                  wrapperspb.String(name),
		OrchestrationInstance: &protos.OrchestrationInstance{InstanceId: iid},
	}
}

func NewScheduleTaskAction(
	taskID int32,
	name string,
	input *wrapperspb.StringValue,
	version ...*wrapperspb.StringValue,
) *protos.OrchestratorAction {
	action := &protos.OrchestratorAction{
		Id: taskID,
		OrchestratorActionType: &protos.OrchestratorAction_ScheduleTask{
			ScheduleTask: &protos.ScheduleTaskAction{Name: name, Input: input},
		},
	}
	if len(version) > 0 {
		action.GetScheduleTask().Version = version[0]
	}
	return action
}

func NewCreateTimerAction(taskID int32, fireAt time.Time) *protos.OrchestratorAction {
	return &protos.OrchestratorAction{
		Id: taskID,
		OrchestratorActionType: &protos.OrchestratorAction_CreateTimer{
			CreateTimer: &protos.CreateTimerAction{FireAt: timestamppb.New(fireAt)},
		},
	}
}

func NewSendEventAction(iid string, name string, data *wrapperspb.StringValue) *protos.OrchestratorAction {
	return &protos.OrchestratorAction{
		Id: -1,
		OrchestratorActionType: &protos.OrchestratorAction_SendEvent{
			SendEvent: &protos.SendEventAction{
				Instance: &protos.OrchestrationInstance{InstanceId: iid},
				Name:     name,
				Data:     data,
			},
		},
	}
}

func NewEntityOperationSignaledAction(
	id int32,
	requestID string,
	entityID string,
	operation string,
	input *wrapperspb.StringValue,
	scheduledTime *timestamppb.Timestamp,
) *protos.OrchestratorAction {
	return &protos.OrchestratorAction{
		Id: id,
		OrchestratorActionType: &protos.OrchestratorAction_SendEntityMessage{
			SendEntityMessage: &protos.SendEntityMessageAction{
				EntityMessageType: &protos.SendEntityMessageAction_EntityOperationSignaled{
					EntityOperationSignaled: &protos.EntityOperationSignaledEvent{
						RequestId:        requestID,
						Operation:        operation,
						ScheduledTime:    scheduledTime,
						Input:            input,
						TargetInstanceId: wrapperspb.String(entityID),
					},
				},
			},
		},
	}
}

func NewEntityOperationCalledAction(
	id int32,
	requestID string,
	entityID string,
	parentInstanceID string,
	parentExecutionID string,
	operation string,
	input *wrapperspb.StringValue,
) *protos.OrchestratorAction {
	return &protos.OrchestratorAction{
		Id: id,
		OrchestratorActionType: &protos.OrchestratorAction_SendEntityMessage{
			SendEntityMessage: &protos.SendEntityMessageAction{
				EntityMessageType: &protos.SendEntityMessageAction_EntityOperationCalled{
					EntityOperationCalled: &protos.EntityOperationCalledEvent{
						RequestId:         requestID,
						Operation:         operation,
						Input:             input,
						ParentInstanceId:  wrapperspb.String(parentInstanceID),
						ParentExecutionId: wrapperspb.String(parentExecutionID),
						TargetInstanceId:  wrapperspb.String(entityID),
					},
				},
			},
		},
	}
}

func NewEntityLockRequestedAction(
	id int32,
	criticalSectionID string,
	parentInstanceID string,
	lockSet []string,
) *protos.OrchestratorAction {
	return &protos.OrchestratorAction{
		Id: id,
		OrchestratorActionType: &protos.OrchestratorAction_SendEntityMessage{
			SendEntityMessage: &protos.SendEntityMessageAction{
				EntityMessageType: &protos.SendEntityMessageAction_EntityLockRequested{
					EntityLockRequested: &protos.EntityLockRequestedEvent{
						CriticalSectionId: criticalSectionID,
						LockSet:           append([]string(nil), lockSet...),
						ParentInstanceId:  wrapperspb.String(parentInstanceID),
					},
				},
			},
		},
	}
}

func NewEntityUnlockSentAction(
	id int32,
	criticalSectionID string,
	parentInstanceID string,
	entityID string,
) *protos.OrchestratorAction {
	return &protos.OrchestratorAction{
		Id: id,
		OrchestratorActionType: &protos.OrchestratorAction_SendEntityMessage{
			SendEntityMessage: &protos.SendEntityMessageAction{
				EntityMessageType: &protos.SendEntityMessageAction_EntityUnlockSent{
					EntityUnlockSent: &protos.EntityUnlockSentEvent{
						CriticalSectionId: criticalSectionID,
						ParentInstanceId:  wrapperspb.String(parentInstanceID),
						TargetInstanceId:  wrapperspb.String(entityID),
					},
				},
			},
		},
	}
}

func NewCreateSubOrchestrationAction(
	taskID int32,
	name string,
	iid string,
	input *wrapperspb.StringValue,
	version ...*wrapperspb.StringValue,
) *protos.OrchestratorAction {
	action := &protos.OrchestratorAction{
		Id: taskID,
		OrchestratorActionType: &protos.OrchestratorAction_CreateSubOrchestration{
			CreateSubOrchestration: &protos.CreateSubOrchestrationAction{
				Name:       name,
				Input:      input,
				InstanceId: iid,
			},
		},
	}
	if len(version) > 0 {
		action.GetCreateSubOrchestration().Version = version[0]
	}
	return action
}

func NewCompleteOrchestrationAction(
	taskID int32,
	status protos.OrchestrationStatus,
	rawResult *wrapperspb.StringValue,
	carryoverEvents []*protos.HistoryEvent,
	failureDetails *protos.TaskFailureDetails,
) *protos.OrchestratorAction {
	return &protos.OrchestratorAction{
		Id: taskID,
		OrchestratorActionType: &protos.OrchestratorAction_CompleteOrchestration{
			CompleteOrchestration: &protos.CompleteOrchestrationAction{
				OrchestrationStatus: status,
				Result:              rawResult,
				CarryoverEvents:     carryoverEvents,
				FailureDetails:      failureDetails,
			},
		},
	}
}

func ToRuntimeStatusString(status protos.OrchestrationStatus) string {
	name := protos.OrchestrationStatus_name[int32(status)]
	return name[len("ORCHESTRATION_STATUS_"):]
}

func FromRuntimeStatusString(status string) protos.OrchestrationStatus {
	runtimeStatus := "ORCHESTRATION_STATUS_" + status
	return protos.OrchestrationStatus(protos.OrchestrationStatus_value[runtimeStatus])
}
