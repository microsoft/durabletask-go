package historyconv

import (
	"fmt"
	"reflect"
	"slices"
	"strings"

	"github.com/microsoft/durabletask-go/api"
	"github.com/microsoft/durabletask-go/internal/failure"
	"github.com/microsoft/durabletask-go/internal/protos"
	"github.com/microsoft/durabletask-go/internal/tagcodec"
)

// Converter converts one sequential history stream into API-owned records while
// preserving the current orchestration identity for legacy entity events that
// omit it. A Converter is not safe for concurrent use or reuse across streams.
type Converter struct {
	converter          api.DataConverter
	currentInstanceID  string
	currentExecutionID string
}

func New(converter api.DataConverter) *Converter {
	return &Converter{converter: api.NormalizeDataConverter(converter)}
}

func (c *Converter) Convert(event *protos.HistoryEvent) (*api.HistoryEvent, error) {
	if event == nil {
		return nil, fmt.Errorf("history event must not be nil")
	}
	result := &api.HistoryEvent{
		Type:      api.HistoryEventUnknown,
		EventID:   event.GetEventId(),
		Converter: c.converter,
	}
	if event.GetTimestamp() != nil {
		result.Timestamp = event.GetTimestamp().AsTime()
	}

	switch value := event.GetEventType().(type) {
	case *protos.HistoryEvent_ExecutionStarted:
		started := value.ExecutionStarted
		instance := started.GetOrchestrationInstance()
		c.currentInstanceID = instance.GetInstanceId()
		c.currentExecutionID = instance.GetExecutionId().GetValue()
		result.Type = api.HistoryEventExecutionStarted
		result.ExecutionStarted = &api.HistoryExecutionStartedEvent{
			Name:                started.GetName(),
			Version:             started.GetVersion().GetValue(),
			InstanceID:          api.InstanceID(c.currentInstanceID),
			ExecutionID:         c.currentExecutionID,
			SerializedInput:     started.GetInput().GetValue(),
			Parent:              parentInfo(started.GetParentInstance()),
			ParentTraceContext:  traceContext(started.GetParentTraceContext()),
			OrchestrationSpanID: started.GetOrchestrationSpanID().GetValue(),
			Tags:                tagcodec.DecodeUserTagsOrPlain(started.GetTags()),
			ContextFields:       api.ContextFields(tagcodec.DecodeContextFields(started.GetTags())),
		}
		if started.GetScheduledStartTimestamp() != nil {
			result.ExecutionStarted.ScheduledStartAt = started.GetScheduledStartTimestamp().AsTime()
		}
	case *protos.HistoryEvent_ExecutionCompleted:
		result.Type = api.HistoryEventExecutionCompleted
		result.ExecutionCompleted = &api.HistoryExecutionCompletedEvent{
			RuntimeStatus:    value.ExecutionCompleted.GetOrchestrationStatus(),
			SerializedResult: value.ExecutionCompleted.GetResult().GetValue(),
			FailureDetails:   failure.FromProto(value.ExecutionCompleted.GetFailureDetails()),
		}
	case *protos.HistoryEvent_ExecutionTerminated:
		result.Type = api.HistoryEventExecutionTerminated
		result.ExecutionTerminated = &api.HistoryExecutionTerminatedEvent{
			SerializedInput: value.ExecutionTerminated.GetInput().GetValue(),
			Recursive:       value.ExecutionTerminated.GetRecurse(),
		}
	case *protos.HistoryEvent_TaskScheduled:
		result.Type = api.HistoryEventTaskScheduled
		result.TaskScheduled = &api.HistoryTaskScheduledEvent{
			Name:               value.TaskScheduled.GetName(),
			Version:            value.TaskScheduled.GetVersion().GetValue(),
			SerializedInput:    value.TaskScheduled.GetInput().GetValue(),
			ParentTraceContext: traceContext(value.TaskScheduled.GetParentTraceContext()),
			Tags:               tagcodec.DecodeUserTagsOrPlain(value.TaskScheduled.GetTags()),
			ContextFields:      api.ContextFields(tagcodec.DecodeContextFields(value.TaskScheduled.GetTags())),
		}
	case *protos.HistoryEvent_TaskCompleted:
		result.Type = api.HistoryEventTaskCompleted
		result.TaskCompleted = taskResult(value.TaskCompleted.GetTaskScheduledId(), value.TaskCompleted.GetResult().GetValue())
	case *protos.HistoryEvent_TaskFailed:
		result.Type = api.HistoryEventTaskFailed
		result.TaskFailed = taskFailure(value.TaskFailed.GetTaskScheduledId(), value.TaskFailed.GetFailureDetails())
	case *protos.HistoryEvent_SubOrchestrationInstanceCreated:
		created := value.SubOrchestrationInstanceCreated
		result.Type = api.HistoryEventSubOrchestrationInstanceCreated
		result.SubOrchestrationInstanceCreated = &api.HistorySubOrchestrationInstanceCreatedEvent{
			InstanceID:         api.InstanceID(created.GetInstanceId()),
			Name:               created.GetName(),
			Version:            created.GetVersion().GetValue(),
			SerializedInput:    created.GetInput().GetValue(),
			ParentTraceContext: traceContext(created.GetParentTraceContext()),
			Tags:               tagcodec.DecodeUserTagsOrPlain(created.GetTags()),
			ContextFields:      api.ContextFields(tagcodec.DecodeContextFields(created.GetTags())),
		}
	case *protos.HistoryEvent_SubOrchestrationInstanceCompleted:
		result.Type = api.HistoryEventSubOrchestrationInstanceCompleted
		result.SubOrchestrationInstanceCompleted = taskResult(
			value.SubOrchestrationInstanceCompleted.GetTaskScheduledId(),
			value.SubOrchestrationInstanceCompleted.GetResult().GetValue(),
		)
	case *protos.HistoryEvent_SubOrchestrationInstanceFailed:
		result.Type = api.HistoryEventSubOrchestrationInstanceFailed
		result.SubOrchestrationInstanceFailed = taskFailure(
			value.SubOrchestrationInstanceFailed.GetTaskScheduledId(),
			value.SubOrchestrationInstanceFailed.GetFailureDetails(),
		)
	case *protos.HistoryEvent_TimerCreated:
		result.Type = api.HistoryEventTimerCreated
		result.TimerCreated = &api.HistoryTimerEvent{}
		if value.TimerCreated.GetFireAt() != nil {
			result.TimerCreated.FireAt = value.TimerCreated.GetFireAt().AsTime()
		}
	case *protos.HistoryEvent_TimerFired:
		result.Type = api.HistoryEventTimerFired
		result.TimerFired = &api.HistoryTimerFiredEvent{TimerID: value.TimerFired.GetTimerId()}
		if value.TimerFired.GetFireAt() != nil {
			result.TimerFired.FireAt = value.TimerFired.GetFireAt().AsTime()
		}
	case *protos.HistoryEvent_OrchestratorStarted:
		result.Type = api.HistoryEventOrchestratorStarted
	case *protos.HistoryEvent_OrchestratorCompleted:
		result.Type = api.HistoryEventOrchestratorCompleted
	case *protos.HistoryEvent_EventSent:
		result.Type = api.HistoryEventEventSent
		result.EventSent = &api.HistoryExternalEvent{
			InstanceID:      api.InstanceID(value.EventSent.GetInstanceId()),
			Name:            value.EventSent.GetName(),
			SerializedInput: value.EventSent.GetInput().GetValue(),
		}
	case *protos.HistoryEvent_EventRaised:
		result.Type = api.HistoryEventEventRaised
		result.EventRaised = &api.HistoryExternalEvent{
			Name:            value.EventRaised.GetName(),
			SerializedInput: value.EventRaised.GetInput().GetValue(),
		}
	case *protos.HistoryEvent_GenericEvent:
		result.Type = api.HistoryEventGeneric
		result.Generic = payloadEvent(value.GenericEvent.GetData().GetValue())
	case *protos.HistoryEvent_HistoryState:
		result.Type = api.HistoryEventHistoryState
		result.HistoryState = &api.HistoryStateEvent{
			State: orchestrationMetadata(value.HistoryState.GetOrchestrationState(), c.converter),
		}
	case *protos.HistoryEvent_ContinueAsNew:
		result.Type = api.HistoryEventContinueAsNew
		result.ContinueAsNew = payloadEvent(value.ContinueAsNew.GetInput().GetValue())
	case *protos.HistoryEvent_ExecutionSuspended:
		result.Type = api.HistoryEventExecutionSuspended
		result.ExecutionSuspended = payloadEvent(value.ExecutionSuspended.GetInput().GetValue())
	case *protos.HistoryEvent_ExecutionResumed:
		result.Type = api.HistoryEventExecutionResumed
		result.ExecutionResumed = payloadEvent(value.ExecutionResumed.GetInput().GetValue())
	case *protos.HistoryEvent_EntityOperationSignaled:
		result.Type = api.HistoryEventEntityOperationSignaled
		result.Entity = c.entityOperationSignaled(value.EntityOperationSignaled)
	case *protos.HistoryEvent_EntityOperationCalled:
		result.Type = api.HistoryEventEntityOperationCalled
		result.Entity = c.entityOperationCalled(value.EntityOperationCalled)
	case *protos.HistoryEvent_EntityOperationCompleted:
		result.Type = api.HistoryEventEntityOperationCompleted
		result.Entity = &api.HistoryEntityEvent{
			RequestID:        value.EntityOperationCompleted.GetRequestId(),
			SerializedOutput: value.EntityOperationCompleted.GetOutput().GetValue(),
		}
	case *protos.HistoryEvent_EntityOperationFailed:
		result.Type = api.HistoryEventEntityOperationFailed
		result.Entity = &api.HistoryEntityEvent{
			RequestID:      value.EntityOperationFailed.GetRequestId(),
			FailureDetails: failure.FromProto(value.EntityOperationFailed.GetFailureDetails()),
		}
	case *protos.HistoryEvent_EntityLockRequested:
		result.Type = api.HistoryEventEntityLockRequested
		lock := value.EntityLockRequested
		result.Entity = &api.HistoryEntityEvent{
			RequestID:         lock.GetCriticalSectionId(),
			CriticalSectionID: lock.GetCriticalSectionId(),
			ParentInstanceID:  fallback(lock.GetParentInstanceId().GetValue(), c.currentInstanceID),
			LockSet:           slices.Clone(lock.GetLockSet()),
			Position:          lock.GetPosition(),
		}
		if position := int(lock.GetPosition()); position >= 0 && position < len(lock.GetLockSet()) {
			result.Entity.TargetInstanceID = lock.GetLockSet()[position]
		}
	case *protos.HistoryEvent_EntityLockGranted:
		result.Type = api.HistoryEventEntityLockGranted
		result.Entity = &api.HistoryEntityEvent{
			RequestID:         value.EntityLockGranted.GetCriticalSectionId(),
			CriticalSectionID: value.EntityLockGranted.GetCriticalSectionId(),
		}
	case *protos.HistoryEvent_EntityUnlockSent:
		result.Type = api.HistoryEventEntityUnlockSent
		unlock := value.EntityUnlockSent
		result.Entity = &api.HistoryEntityEvent{
			RequestID:         unlock.GetCriticalSectionId(),
			CriticalSectionID: unlock.GetCriticalSectionId(),
			ParentInstanceID:  fallback(unlock.GetParentInstanceId().GetValue(), c.currentInstanceID),
			TargetInstanceID:  unlock.GetTargetInstanceId().GetValue(),
		}
	case *protos.HistoryEvent_ExecutionRewound:
		rewound := value.ExecutionRewound
		result.Type = api.HistoryEventExecutionRewound
		result.ExecutionRewound = &api.HistoryExecutionRewoundEvent{
			Reason:             rewound.GetReason().GetValue(),
			Name:               rewound.GetName().GetValue(),
			Version:            rewound.GetVersion().GetValue(),
			InstanceID:         api.InstanceID(rewound.GetInstanceId().GetValue()),
			ParentExecutionID:  rewound.GetParentExecutionId().GetValue(),
			SerializedInput:    rewound.GetInput().GetValue(),
			Parent:             parentInfo(rewound.GetParentInstance()),
			ParentTraceContext: traceContext(rewound.GetParentTraceContext()),
			Tags:               tagcodec.DecodeUserTagsOrPlain(rewound.GetTags()),
			ContextFields:      api.ContextFields(tagcodec.DecodeContextFields(rewound.GetTags())),
		}
	case nil:
		// Preserve the envelope as an unknown event for forward compatibility.
	default:
		eventType := reflect.TypeOf(value)
		if eventType.Kind() == reflect.Pointer {
			eventType = eventType.Elem()
		}
		result.UnknownType = strings.TrimPrefix(eventType.Name(), "HistoryEvent_")
	}
	return result, nil
}

func (c *Converter) entityOperationSignaled(value *protos.EntityOperationSignaledEvent) *api.HistoryEntityEvent {
	result := &api.HistoryEntityEvent{
		RequestID:        value.GetRequestId(),
		Operation:        value.GetOperation(),
		TargetInstanceID: value.GetTargetInstanceId().GetValue(),
		SerializedInput:  value.GetInput().GetValue(),
	}
	if value.GetScheduledTime() != nil {
		result.ScheduledAt = value.GetScheduledTime().AsTime()
	}
	return result
}

func (c *Converter) entityOperationCalled(value *protos.EntityOperationCalledEvent) *api.HistoryEntityEvent {
	result := &api.HistoryEntityEvent{
		RequestID:         value.GetRequestId(),
		Operation:         value.GetOperation(),
		TargetInstanceID:  value.GetTargetInstanceId().GetValue(),
		ParentInstanceID:  fallback(value.GetParentInstanceId().GetValue(), c.currentInstanceID),
		ParentExecutionID: fallback(value.GetParentExecutionId().GetValue(), c.currentExecutionID),
		SerializedInput:   value.GetInput().GetValue(),
	}
	if value.GetScheduledTime() != nil {
		result.ScheduledAt = value.GetScheduledTime().AsTime()
	}
	return result
}

func taskResult(taskScheduledID int32, result string) *api.HistoryTaskResultEvent {
	return &api.HistoryTaskResultEvent{TaskScheduledID: taskScheduledID, SerializedResult: result}
}

func taskFailure(taskScheduledID int32, details *protos.TaskFailureDetails) *api.HistoryTaskFailureEvent {
	return &api.HistoryTaskFailureEvent{
		TaskScheduledID: taskScheduledID,
		FailureDetails:  failure.FromProto(details),
	}
}

func payloadEvent(payload string) *api.HistoryPayloadEvent {
	return &api.HistoryPayloadEvent{SerializedInput: payload}
}

func parentInfo(parent *protos.ParentInstanceInfo) *api.HistoryParentInstanceInfo {
	if parent == nil {
		return nil
	}
	instance := parent.GetOrchestrationInstance()
	return &api.HistoryParentInstanceInfo{
		Name:            parent.GetName().GetValue(),
		Version:         parent.GetVersion().GetValue(),
		InstanceID:      api.InstanceID(instance.GetInstanceId()),
		ExecutionID:     instance.GetExecutionId().GetValue(),
		TaskScheduledID: parent.GetTaskScheduledId(),
	}
}

func traceContext(value *protos.TraceContext) *api.HistoryTraceContext {
	if value == nil {
		return nil
	}
	return &api.HistoryTraceContext{
		TraceParent: value.GetTraceParent(),
		TraceState:  value.GetTraceState().GetValue(),
		SpanID:      value.GetSpanID(), //nolint:staticcheck // preserve the deprecated wire field.
	}
}

func orchestrationMetadata(
	state *protos.OrchestrationState,
	converter api.DataConverter,
) *api.OrchestrationMetadata {
	if state == nil {
		return nil
	}
	result := &api.OrchestrationMetadata{
		InstanceID:             api.InstanceID(state.GetInstanceId()),
		Name:                   state.GetName(),
		Version:                state.GetVersion().GetValue(),
		ExecutionID:            state.GetExecutionId().GetValue(),
		ParentInstanceID:       api.InstanceID(state.GetParentInstanceId().GetValue()),
		RuntimeStatus:          state.GetOrchestrationStatus(),
		SerializedInput:        state.GetInput().GetValue(),
		SerializedOutput:       state.GetOutput().GetValue(),
		SerializedCustomStatus: state.GetCustomStatus().GetValue(),
		FailureDetails:         failure.FromProto(state.GetFailureDetails()),
		Tags:                   tagcodec.DecodeUserTagsOrPlain(state.GetTags()),
		Converter:              converter,
	}
	if state.GetScheduledStartTimestamp() != nil {
		result.ScheduledStartAt = state.GetScheduledStartTimestamp().AsTime()
	}
	if state.GetCreatedTimestamp() != nil {
		result.CreatedAt = state.GetCreatedTimestamp().AsTime()
	}
	if state.GetLastUpdatedTimestamp() != nil {
		result.LastUpdatedAt = state.GetLastUpdatedTimestamp().AsTime()
	}
	if state.GetCompletedTimestamp() != nil {
		result.CompletedAt = state.GetCompletedTimestamp().AsTime()
	}
	return result
}

func fallback(value, fallbackValue string) string {
	if value != "" {
		return value
	}
	return fallbackValue
}
