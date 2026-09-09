package api

import (
	"errors"
	"fmt"
	"time"
)

const (
	DefaultHistoryMaxEvents = 100_000
	MaxHistoryMaxEvents     = 1_000_000
	DefaultHistoryMaxBytes  = 64 * 1024 * 1024
	MaxHistoryMaxBytes      = 1024 * 1024 * 1024
)

var ErrHistoryLimitExceeded = errors.New("orchestration history exceeds the configured event limit")

// HistoryEventType identifies the durable event represented by a [HistoryEvent].
type HistoryEventType string

const (
	HistoryEventUnknown                           HistoryEventType = "Unknown"
	HistoryEventExecutionStarted                  HistoryEventType = "ExecutionStarted"
	HistoryEventExecutionCompleted                HistoryEventType = "ExecutionCompleted"
	HistoryEventExecutionTerminated               HistoryEventType = "ExecutionTerminated"
	HistoryEventTaskScheduled                     HistoryEventType = "TaskScheduled"
	HistoryEventTaskCompleted                     HistoryEventType = "TaskCompleted"
	HistoryEventTaskFailed                        HistoryEventType = "TaskFailed"
	HistoryEventSubOrchestrationInstanceCreated   HistoryEventType = "SubOrchestrationInstanceCreated"
	HistoryEventSubOrchestrationInstanceCompleted HistoryEventType = "SubOrchestrationInstanceCompleted"
	HistoryEventSubOrchestrationInstanceFailed    HistoryEventType = "SubOrchestrationInstanceFailed"
	HistoryEventTimerCreated                      HistoryEventType = "TimerCreated"
	HistoryEventTimerFired                        HistoryEventType = "TimerFired"
	HistoryEventOrchestratorStarted               HistoryEventType = "OrchestratorStarted"
	HistoryEventOrchestratorCompleted             HistoryEventType = "OrchestratorCompleted"
	HistoryEventEventSent                         HistoryEventType = "EventSent"
	HistoryEventEventRaised                       HistoryEventType = "EventRaised"
	HistoryEventGeneric                           HistoryEventType = "Generic"
	HistoryEventHistoryState                      HistoryEventType = "HistoryState"
	HistoryEventContinueAsNew                     HistoryEventType = "ContinueAsNew"
	HistoryEventExecutionSuspended                HistoryEventType = "ExecutionSuspended"
	HistoryEventExecutionResumed                  HistoryEventType = "ExecutionResumed"
	HistoryEventEntityOperationSignaled           HistoryEventType = "EntityOperationSignaled"
	HistoryEventEntityOperationCalled             HistoryEventType = "EntityOperationCalled"
	HistoryEventEntityOperationCompleted          HistoryEventType = "EntityOperationCompleted"
	HistoryEventEntityOperationFailed             HistoryEventType = "EntityOperationFailed"
	HistoryEventEntityLockRequested               HistoryEventType = "EntityLockRequested"
	HistoryEventEntityLockGranted                 HistoryEventType = "EntityLockGranted"
	HistoryEventEntityUnlockSent                  HistoryEventType = "EntityUnlockSent"
	HistoryEventExecutionRewound                  HistoryEventType = "ExecutionRewound"
)

// HistoryQuery configures an orchestration history read.
type HistoryQuery struct {
	// ExecutionID selects a specific execution. Empty selects the service's
	// current execution. GetOrchestrationHistory verifies a nonempty selection
	// against ExecutionStarted events and rejects missing or conflicting IDs.
	ExecutionID string
	// MaxEvents bounds the buffered GetOrchestrationHistory result. It is
	// validated but ignored by StreamOrchestrationHistory, which does not retain
	// events. Zero uses DefaultHistoryMaxEvents.
	MaxEvents int
	// MaxBytes bounds the approximate in-memory size of the buffered
	// GetOrchestrationHistory result. It is ignored by
	// StreamOrchestrationHistory. Zero uses DefaultHistoryMaxBytes.
	MaxBytes int
}

// NormalizeHistoryQuery validates a history query and applies defaults.
func NormalizeHistoryQuery(query HistoryQuery) (HistoryQuery, error) {
	switch {
	case query.MaxEvents < 0:
		return HistoryQuery{}, WrapInvalidArgument(errors.New("history event limit cannot be negative"))
	case query.MaxEvents == 0:
		query.MaxEvents = DefaultHistoryMaxEvents
	case query.MaxEvents > MaxHistoryMaxEvents:
		return HistoryQuery{}, WrapInvalidArgument(
			fmt.Errorf("history event limit cannot exceed %d", MaxHistoryMaxEvents),
		)
	}
	switch {
	case query.MaxBytes < 0:
		return HistoryQuery{}, WrapInvalidArgument(errors.New("history byte limit cannot be negative"))
	case query.MaxBytes == 0:
		query.MaxBytes = DefaultHistoryMaxBytes
	case query.MaxBytes > MaxHistoryMaxBytes:
		return HistoryQuery{}, WrapInvalidArgument(
			fmt.Errorf("history byte limit cannot exceed %d", MaxHistoryMaxBytes),
		)
	}
	return query, nil
}

// OrchestrationHistory is a bounded snapshot of an orchestration's durable history.
type OrchestrationHistory struct {
	InstanceID InstanceID `json:"instanceId"`
	// ExecutionID is observed in ExecutionStarted events, never copied from the
	// query. It may be empty only for an unpinned history without such events.
	ExecutionID string          `json:"executionId,omitempty"`
	Events      []*HistoryEvent `json:"events"`
}

// HistoryEventHandler receives events in storage/service order.
type HistoryEventHandler func(*HistoryEvent) error

// HistoryEvent is one API-owned durable history record. Exactly one detail
// field is populated for event types that carry details.
type HistoryEvent struct {
	Type      HistoryEventType `json:"type"`
	EventID   int32            `json:"eventId"`
	Timestamp time.Time        `json:"timestamp"`
	// UnknownType identifies a wire event type that this SDK does not yet
	// understand. It is diagnostic and populated only for HistoryEventUnknown.
	UnknownType string `json:"unknownType,omitempty"`

	ExecutionStarted                  *HistoryExecutionStartedEvent                `json:"executionStarted,omitempty"`
	ExecutionCompleted                *HistoryExecutionCompletedEvent              `json:"executionCompleted,omitempty"`
	ExecutionTerminated               *HistoryExecutionTerminatedEvent             `json:"executionTerminated,omitempty"`
	TaskScheduled                     *HistoryTaskScheduledEvent                   `json:"taskScheduled,omitempty"`
	TaskCompleted                     *HistoryTaskResultEvent                      `json:"taskCompleted,omitempty"`
	TaskFailed                        *HistoryTaskFailureEvent                     `json:"taskFailed,omitempty"`
	SubOrchestrationInstanceCreated   *HistorySubOrchestrationInstanceCreatedEvent `json:"subOrchestrationInstanceCreated,omitempty"`
	SubOrchestrationInstanceCompleted *HistoryTaskResultEvent                      `json:"subOrchestrationInstanceCompleted,omitempty"`
	SubOrchestrationInstanceFailed    *HistoryTaskFailureEvent                     `json:"subOrchestrationInstanceFailed,omitempty"`
	TimerCreated                      *HistoryTimerEvent                           `json:"timerCreated,omitempty"`
	TimerFired                        *HistoryTimerFiredEvent                      `json:"timerFired,omitempty"`
	EventSent                         *HistoryExternalEvent                        `json:"eventSent,omitempty"`
	EventRaised                       *HistoryExternalEvent                        `json:"eventRaised,omitempty"`
	Generic                           *HistoryPayloadEvent                         `json:"generic,omitempty"`
	HistoryState                      *HistoryStateEvent                           `json:"historyState,omitempty"`
	ContinueAsNew                     *HistoryPayloadEvent                         `json:"continueAsNew,omitempty"`
	ExecutionSuspended                *HistoryPayloadEvent                         `json:"executionSuspended,omitempty"`
	ExecutionResumed                  *HistoryPayloadEvent                         `json:"executionResumed,omitempty"`
	Entity                            *HistoryEntityEvent                          `json:"entity,omitempty"`
	ExecutionRewound                  *HistoryExecutionRewoundEvent                `json:"executionRewound,omitempty"`

	Converter DataConverter `json:"-"`
}

type HistoryExecutionStartedEvent struct {
	Name                string                     `json:"name"`
	Version             string                     `json:"version,omitempty"`
	InstanceID          InstanceID                 `json:"instanceId"`
	ExecutionID         string                     `json:"executionId,omitempty"`
	SerializedInput     string                     `json:"serializedInput,omitempty"`
	ScheduledStartAt    time.Time                  `json:"scheduledStartAt,omitempty"`
	Parent              *HistoryParentInstanceInfo `json:"parent,omitempty"`
	ParentTraceContext  *HistoryTraceContext       `json:"parentTraceContext,omitempty"`
	OrchestrationSpanID string                     `json:"orchestrationSpanId,omitempty"`
	Tags                map[string]string          `json:"tags,omitempty"`
	ContextFields       ContextFields              `json:"contextFields,omitempty"`
}

type HistoryParentInstanceInfo struct {
	Name            string     `json:"name,omitempty"`
	Version         string     `json:"version,omitempty"`
	InstanceID      InstanceID `json:"instanceId,omitempty"`
	ExecutionID     string     `json:"executionId,omitempty"`
	TaskScheduledID int32      `json:"taskScheduledId"`
}

type HistoryTraceContext struct {
	TraceParent string `json:"traceParent,omitempty"`
	TraceState  string `json:"traceState,omitempty"`
	SpanID      string `json:"spanId,omitempty"`
}

type HistoryExecutionCompletedEvent struct {
	RuntimeStatus    OrchestrationStatus `json:"runtimeStatus"`
	SerializedResult string              `json:"serializedResult,omitempty"`
	FailureDetails   *FailureDetails     `json:"failureDetails,omitempty"`
}

type HistoryExecutionTerminatedEvent struct {
	SerializedInput string `json:"serializedInput,omitempty"`
	Recursive       bool   `json:"recursive"`
}

type HistoryTaskScheduledEvent struct {
	Name               string               `json:"name"`
	Version            string               `json:"version,omitempty"`
	SerializedInput    string               `json:"serializedInput,omitempty"`
	ParentTraceContext *HistoryTraceContext `json:"parentTraceContext,omitempty"`
	Tags               map[string]string    `json:"tags,omitempty"`
	ContextFields      ContextFields        `json:"contextFields,omitempty"`
}

type HistoryTaskResultEvent struct {
	TaskScheduledID  int32  `json:"taskScheduledId"`
	SerializedResult string `json:"serializedResult,omitempty"`
}

type HistoryTaskFailureEvent struct {
	TaskScheduledID int32           `json:"taskScheduledId"`
	FailureDetails  *FailureDetails `json:"failureDetails,omitempty"`
}

type HistorySubOrchestrationInstanceCreatedEvent struct {
	InstanceID         InstanceID           `json:"instanceId"`
	Name               string               `json:"name"`
	Version            string               `json:"version,omitempty"`
	SerializedInput    string               `json:"serializedInput,omitempty"`
	ParentTraceContext *HistoryTraceContext `json:"parentTraceContext,omitempty"`
	Tags               map[string]string    `json:"tags,omitempty"`
	ContextFields      ContextFields        `json:"contextFields,omitempty"`
}

type HistoryTimerEvent struct {
	FireAt time.Time `json:"fireAt"`
}

type HistoryTimerFiredEvent struct {
	FireAt  time.Time `json:"fireAt"`
	TimerID int32     `json:"timerId"`
}

type HistoryExternalEvent struct {
	InstanceID      InstanceID `json:"instanceId,omitempty"`
	Name            string     `json:"name"`
	SerializedInput string     `json:"serializedInput,omitempty"`
}

type HistoryPayloadEvent struct {
	SerializedInput string `json:"serializedInput,omitempty"`
}

type HistoryStateEvent struct {
	State *OrchestrationMetadata `json:"state,omitempty"`
}

type HistoryEntityEvent struct {
	RequestID         string          `json:"requestId,omitempty"`
	Operation         string          `json:"operation,omitempty"`
	TargetInstanceID  string          `json:"targetInstanceId,omitempty"`
	ParentInstanceID  string          `json:"parentInstanceId,omitempty"`
	ParentExecutionID string          `json:"parentExecutionId,omitempty"`
	ScheduledAt       time.Time       `json:"scheduledAt,omitempty"`
	CriticalSectionID string          `json:"criticalSectionId,omitempty"`
	LockSet           []string        `json:"lockSet,omitempty"`
	Position          int32           `json:"position,omitempty"`
	SerializedInput   string          `json:"serializedInput,omitempty"`
	SerializedOutput  string          `json:"serializedOutput,omitempty"`
	FailureDetails    *FailureDetails `json:"failureDetails,omitempty"`
}

type HistoryExecutionRewoundEvent struct {
	Reason             string                     `json:"reason,omitempty"`
	Name               string                     `json:"name,omitempty"`
	Version            string                     `json:"version,omitempty"`
	InstanceID         InstanceID                 `json:"instanceId,omitempty"`
	ParentExecutionID  string                     `json:"parentExecutionId,omitempty"`
	SerializedInput    string                     `json:"serializedInput,omitempty"`
	Parent             *HistoryParentInstanceInfo `json:"parent,omitempty"`
	ParentTraceContext *HistoryTraceContext       `json:"parentTraceContext,omitempty"`
	Tags               map[string]string          `json:"tags,omitempty"`
	ContextFields      ContextFields              `json:"contextFields,omitempty"`
}

// ReadInput deserializes the input-like payload carried by this event.
// Events without an input payload leave target unchanged and return nil.
func (e *HistoryEvent) ReadInput(target any) error {
	if e == nil {
		return nil
	}
	var payload string
	switch e.Type {
	case HistoryEventExecutionStarted:
		if e.ExecutionStarted != nil {
			payload = e.ExecutionStarted.SerializedInput
		}
	case HistoryEventExecutionTerminated:
		if e.ExecutionTerminated != nil {
			payload = e.ExecutionTerminated.SerializedInput
		}
	case HistoryEventTaskScheduled:
		if e.TaskScheduled != nil {
			payload = e.TaskScheduled.SerializedInput
		}
	case HistoryEventSubOrchestrationInstanceCreated:
		if e.SubOrchestrationInstanceCreated != nil {
			payload = e.SubOrchestrationInstanceCreated.SerializedInput
		}
	case HistoryEventEventSent:
		if e.EventSent != nil {
			payload = e.EventSent.SerializedInput
		}
	case HistoryEventEventRaised:
		if e.EventRaised != nil {
			payload = e.EventRaised.SerializedInput
		}
	case HistoryEventContinueAsNew:
		if e.ContinueAsNew != nil {
			payload = e.ContinueAsNew.SerializedInput
		}
	case HistoryEventExecutionSuspended:
		if e.ExecutionSuspended != nil {
			payload = e.ExecutionSuspended.SerializedInput
		}
	case HistoryEventExecutionResumed:
		if e.ExecutionResumed != nil {
			payload = e.ExecutionResumed.SerializedInput
		}
	case HistoryEventExecutionRewound:
		if e.ExecutionRewound != nil {
			payload = e.ExecutionRewound.SerializedInput
		}
	case HistoryEventEntityOperationSignaled, HistoryEventEntityOperationCalled:
		if e.Entity != nil {
			payload = e.Entity.SerializedInput
		}
	}
	return deserializePayload(e.Converter, payload, target)
}

// ReadResult deserializes the result/output payload carried by this event.
// Events without a result payload leave target unchanged and return nil.
func (e *HistoryEvent) ReadResult(target any) error {
	if e == nil {
		return nil
	}
	var payload string
	switch e.Type {
	case HistoryEventExecutionCompleted:
		if e.ExecutionCompleted != nil {
			payload = e.ExecutionCompleted.SerializedResult
		}
	case HistoryEventTaskCompleted:
		if e.TaskCompleted != nil {
			payload = e.TaskCompleted.SerializedResult
		}
	case HistoryEventSubOrchestrationInstanceCompleted:
		if e.SubOrchestrationInstanceCompleted != nil {
			payload = e.SubOrchestrationInstanceCompleted.SerializedResult
		}
	case HistoryEventEntityOperationCompleted:
		if e.Entity != nil {
			payload = e.Entity.SerializedOutput
		}
	}
	return deserializePayload(e.Converter, payload, target)
}

// ReadData deserializes a generic event payload.
func (e *HistoryEvent) ReadData(target any) error {
	if e == nil || e.Type != HistoryEventGeneric || e.Generic == nil {
		return nil
	}
	return deserializePayload(e.Converter, e.Generic.SerializedInput, target)
}
