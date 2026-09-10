package historyconv

import (
	"testing"
	"time"

	"github.com/microsoft/durabletask-go/api"
	"github.com/microsoft/durabletask-go/internal/protos"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

func TestConvertPreservesExecutionAndEntityDetails(t *testing.T) {
	timestamp := time.Date(2026, 8, 27, 12, 30, 0, 0, time.UTC)
	converter := New(api.DefaultDataConverter())
	started, err := converter.Convert(&protos.HistoryEvent{
		EventId:   4,
		Timestamp: timestamppb.New(timestamp),
		EventType: &protos.HistoryEvent_ExecutionStarted{
			ExecutionStarted: &protos.ExecutionStartedEvent{
				Name:    "orchestrator",
				Version: wrapperspb.String("2.0"),
				Input:   wrapperspb.String(`{"value":1}`),
				OrchestrationInstance: &protos.OrchestrationInstance{
					InstanceId:  "instance",
					ExecutionId: wrapperspb.String("execution"),
				},
				Tags: map[string]string{
					"tag":                                      "value",
					"__durabletask.context.encoding":           "1",
					"__durabletask.context.field.tenant":       "north",
					"__durabletask.context.instance_id":        "instance",
					"__durabletask.context.orchestration_name": "orchestrator",
				},
			},
		},
	})
	require.NoError(t, err)
	require.Equal(t, api.HistoryEventExecutionStarted, started.Type)
	require.EqualValues(t, 4, started.EventID)
	require.Equal(t, timestamp, started.Timestamp)
	require.Equal(t, api.InstanceID("instance"), started.ExecutionStarted.InstanceID)
	require.Equal(t, "execution", started.ExecutionStarted.ExecutionID)
	require.Equal(t, map[string]string{"tag": "value"}, started.ExecutionStarted.Tags)
	require.Equal(t, api.ContextFields{"tenant": "north"}, started.ExecutionStarted.ContextFields)

	entity, err := converter.Convert(&protos.HistoryEvent{
		EventType: &protos.HistoryEvent_EntityOperationCalled{
			EntityOperationCalled: &protos.EntityOperationCalledEvent{
				RequestId:        "request",
				Operation:        "add",
				TargetInstanceId: wrapperspb.String("@counter@one"),
				Input:            wrapperspb.String("1"),
			},
		},
	})
	require.NoError(t, err)
	require.Equal(t, api.HistoryEventEntityOperationCalled, entity.Type)
	require.Equal(t, "instance", entity.Entity.ParentInstanceID)
	require.Equal(t, "execution", entity.Entity.ParentExecutionID)
	require.Equal(t, "@counter@one", entity.Entity.TargetInstanceID)
}

func TestConvertEntityWithoutExecutionStarted(t *testing.T) {
	event, err := New(nil).Convert(&protos.HistoryEvent{
		EventType: &protos.HistoryEvent_EntityOperationCalled{
			EntityOperationCalled: &protos.EntityOperationCalledEvent{
				RequestId:         "request",
				ParentInstanceId:  wrapperspb.String("parent"),
				ParentExecutionId: wrapperspb.String("execution"),
			},
		},
	})
	require.NoError(t, err)
	require.Equal(t, "parent", event.Entity.ParentInstanceID)
	require.Equal(t, "execution", event.Entity.ParentExecutionID)
}

func TestConvertPreservesRewindAndHistoryState(t *testing.T) {
	converter := New(nil)
	rewound, err := converter.Convert(&protos.HistoryEvent{
		EventId: 9,
		EventType: &protos.HistoryEvent_ExecutionRewound{
			ExecutionRewound: &protos.ExecutionRewoundEvent{
				Reason:            wrapperspb.String("retry"),
				Name:              wrapperspb.String("orchestrator"),
				Version:           wrapperspb.String("3.0"),
				InstanceId:        wrapperspb.String("instance"),
				ParentExecutionId: wrapperspb.String("parent-execution"),
				Input:             wrapperspb.String(`"input"`),
				Tags:              map[string]string{"tag": "value"},
			},
		},
	})
	require.NoError(t, err)
	require.Equal(t, api.HistoryEventExecutionRewound, rewound.Type)
	require.Equal(t, "retry", rewound.ExecutionRewound.Reason)
	require.Equal(t, api.InstanceID("instance"), rewound.ExecutionRewound.InstanceID)
	require.Equal(t, map[string]string{"tag": "value"}, rewound.ExecutionRewound.Tags)

	completedAt := time.Date(2026, 8, 27, 13, 0, 0, 0, time.UTC)
	state, err := converter.Convert(&protos.HistoryEvent{
		EventType: &protos.HistoryEvent_HistoryState{
			HistoryState: &protos.HistoryStateEvent{
				OrchestrationState: &protos.OrchestrationState{
					InstanceId:          "instance",
					ExecutionId:         wrapperspb.String("execution"),
					Name:                "orchestrator",
					OrchestrationStatus: api.RUNTIME_STATUS_COMPLETED,
					Output:              wrapperspb.String(`"done"`),
					CompletedTimestamp:  timestamppb.New(completedAt),
				},
			},
		},
	})
	require.NoError(t, err)
	require.Equal(t, "execution", state.HistoryState.State.ExecutionID)
	require.Equal(t, completedAt, state.HistoryState.State.CompletedAt)
	require.Equal(t, `"done"`, state.HistoryState.State.SerializedOutput)
}

func TestConvertPreservesTimerEnvelopeEventID(t *testing.T) {
	event, err := New(nil).Convert(&protos.HistoryEvent{
		EventId: 17,
		EventType: &protos.HistoryEvent_TimerFired{
			TimerFired: &protos.TimerFiredEvent{TimerId: 5},
		},
	})
	require.NoError(t, err)
	require.EqualValues(t, 17, event.EventID)
	require.EqualValues(t, 5, event.TimerFired.TimerID)
}
