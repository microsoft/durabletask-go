package helpers

import (
	"reflect"
	"strconv"
	"strings"
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
) *protos.HistoryEvent {
	return &protos.HistoryEvent{
		EventId:   -1,
		Timestamp: timestamppb.New(time.Now()),
		EventType: &protos.HistoryEvent_ExecutionStarted{
			ExecutionStarted: &protos.ExecutionStartedEvent{
				Name:           name,
				ParentInstance: parent,
				Input:          input,
				OrchestrationInstance: &protos.OrchestrationInstance{
					InstanceId:  instanceId,
					ExecutionId: wrapperspb.String(uuid.New().String()),
				},
				ParentTraceContext: parentTraceContext,
			},
		},
	}
}

func NewExecutionCompletedEvent(eventID int32, status protos.OrchestrationStatus, result *wrapperspb.StringValue, failureDetails *protos.TaskFailureDetails) *protos.HistoryEvent {
	return &protos.HistoryEvent{
		EventId:   eventID,
		Timestamp: timestamppb.Now(),
		EventType: &protos.HistoryEvent_ExecutionCompleted{
			ExecutionCompleted: &protos.ExecutionCompletedEvent{
				OrchestrationStatus: status,
				Result:              result,
				FailureDetails:      failureDetails,
			},
		},
	}
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
				TimerId:            timerID,
				FireAt:             fireAt,
				ParentTraceContext: parentTraceContext,
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

func NewScheduleTaskAction(taskID int32, name string, input *wrapperspb.StringValue) *protos.OrchestratorAction {
	return &protos.OrchestratorAction{
		Id: taskID,
		OrchestratorActionType: &protos.OrchestratorAction_ScheduleTask{
			ScheduleTask: &protos.ScheduleTaskAction{Name: name, Input: input},
		},
	}
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

func NewCreateSubOrchestrationAction(
	taskID int32,
	name string,
	iid string,
	input *wrapperspb.StringValue,
) *protos.OrchestratorAction {
	return &protos.OrchestratorAction{
		Id: taskID,
		OrchestratorActionType: &protos.OrchestratorAction_CreateSubOrchestration{
			CreateSubOrchestration: &protos.CreateSubOrchestrationAction{
				Name:       name,
				Input:      input,
				InstanceId: iid,
			},
		},
	}
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

func NewTerminateOrchestrationAction(taskID int32, iid string, recurse bool, rawReason *wrapperspb.StringValue) *protos.OrchestratorAction {
	return &protos.OrchestratorAction{
		Id: taskID,
		OrchestratorActionType: &protos.OrchestratorAction_TerminateOrchestration{
			TerminateOrchestration: &protos.TerminateOrchestrationAction{
				InstanceId: iid,
				Recurse:    recurse,
				Reason:     rawReason,
			},
		},
	}
}

func NewTaskFailureDetails(err error) *protos.TaskFailureDetails {
	if err == nil {
		return nil
	}
	return &protos.TaskFailureDetails{
		ErrorType:    reflect.TypeOf(err).String(),
		ErrorMessage: err.Error(),
	}
}

func HistoryListSummary(list []*protos.HistoryEvent) string {
	var sb strings.Builder
	sb.WriteString("[")
	for i, e := range list {
		if i > 0 {
			sb.WriteString(", ")
		}
		if i >= 10 {
			sb.WriteString("...")
			break
		}
		name := getHistoryEventTypeName(e)
		sb.WriteString(name)
		taskID := GetTaskId(e)
		if taskID > -0 {
			sb.WriteRune('#')
			sb.WriteString(strconv.FormatInt(int64(taskID), 10))
		}
	}
	sb.WriteString("]")
	return sb.String()
}

func ActionListSummary(actions []*protos.OrchestratorAction) string {
	var sb strings.Builder
	sb.WriteString("[")
	for i, a := range actions {
		if i > 0 {
			sb.WriteString(", ")
		}
		if i >= 10 {
			sb.WriteString("...")
			break
		}
		name := getActionTypeName(a)
		sb.WriteString(name)
		if a.Id >= 0 {
			sb.WriteRune('#')
			sb.WriteString(strconv.FormatInt(int64(a.Id), 10))
		}
	}
	sb.WriteString("]")
	return sb.String()
}

func GetTaskId(e *protos.HistoryEvent) int32 {
	if e.EventId >= 0 {
		return e.EventId
	} else if x := e.GetTaskCompleted(); x != nil {
		return x.TaskScheduledId
	} else if x := e.GetTaskFailed(); x != nil {
		return x.TaskScheduledId
	} else if x := e.GetSubOrchestrationInstanceCompleted(); x != nil {
		return x.TaskScheduledId
	} else if x := e.GetSubOrchestrationInstanceFailed(); x != nil {
		return x.TaskScheduledId
	} else if x := e.GetTimerFired(); x != nil {
		return x.TimerId
	} else if x := e.GetExecutionStarted().GetParentInstance(); x != nil {
		return x.TaskScheduledId
	} else {
		return -1
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

func getHistoryEventTypeName(e *protos.HistoryEvent) string {
	// PERFORMANCE: Replace this with a switch statement or a map lookup to avoid this use of reflection
	return reflect.TypeOf(e.EventType).Elem().Name()[len("HistoryEvent_"):]
}

func getActionTypeName(a *protos.OrchestratorAction) string {
	// PERFORMANCE: Replace this with a switch statement or a map lookup to avoid this use of reflection
	return reflect.TypeOf(a.OrchestratorActionType).Elem().Name()[len("OrchestratorAction_"):]
}
