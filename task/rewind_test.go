package task

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/microsoft/durabletask-go/api"
	"github.com/microsoft/durabletask-go/internal/helpers"
	"github.com/microsoft/durabletask-go/internal/protos"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

func rewindEvent(parentExecutionID string) *protos.HistoryEvent {
	rewound := &protos.ExecutionRewoundEvent{Reason: wrapperspb.String("dependency repaired")}
	if parentExecutionID != "" {
		rewound.ParentExecutionId = wrapperspb.String(parentExecutionID)
	}
	return &protos.HistoryEvent{EventId: -1, EventType: &protos.HistoryEvent_ExecutionRewound{ExecutionRewound: rewound}}
}

func failedCompletionEvent() *protos.HistoryEvent {
	return &protos.HistoryEvent{EventId: 10, EventType: &protos.HistoryEvent_ExecutionCompleted{
		ExecutionCompleted: &protos.ExecutionCompletedEvent{
			OrchestrationStatus: api.RUNTIME_STATUS_FAILED,
			FailureDetails:      &protos.TaskFailureDetails{ErrorType: "Failure", ErrorMessage: "unavailable"},
		},
	}}
}

func TestRewindHistoryPreservesSuccessfulWork(t *testing.T) {
	start := helpers.NewExecutionStartedEvent("workflow", "instance", wrapperspb.String(`"input"`), nil, nil, nil)
	start.GetExecutionStarted().Tags = map[string]string{"tag": "value"}
	start.ProtoReflect().SetUnknown([]byte{0xa0, 0x06, 0x01})
	old := []*protos.HistoryEvent{
		helpers.NewOrchestratorStartedEvent(),
		start,
		helpers.NewTaskScheduledEvent(0, "good", nil, nil, nil),
		helpers.NewTaskCompletedEvent(0, wrapperspb.String(`"kept"`)),
		helpers.NewTaskScheduledEvent(1, "bad", nil, nil, nil),
		helpers.NewTaskFailedEvent(1, &protos.TaskFailureDetails{ErrorMessage: "failed"}),
		{EventId: 2, EventType: &protos.HistoryEvent_SubOrchestrationInstanceCreated{
			SubOrchestrationInstanceCreated: &protos.SubOrchestrationInstanceCreatedEvent{InstanceId: "failed-child", Name: "child"},
		}},
		{EventType: &protos.HistoryEvent_SubOrchestrationInstanceFailed{
			SubOrchestrationInstanceFailed: &protos.SubOrchestrationInstanceFailedEvent{TaskScheduledId: 2},
		}},
		{EventId: 3, EventType: &protos.HistoryEvent_SubOrchestrationInstanceCreated{
			SubOrchestrationInstanceCreated: &protos.SubOrchestrationInstanceCreatedEvent{InstanceId: "successful-child", Name: "child"},
		}},
		{EventType: &protos.HistoryEvent_SubOrchestrationInstanceCompleted{
			SubOrchestrationInstanceCompleted: &protos.SubOrchestrationInstanceCompletedEvent{TaskScheduledId: 3, Result: wrapperspb.String(`"child-kept"`)},
		}},
		helpers.NewTimerCreatedEvent(4, start.Timestamp),
		{EventType: &protos.HistoryEvent_TimerFired{TimerFired: &protos.TimerFiredEvent{TimerId: 4}}},
		{EventType: &protos.HistoryEvent_GenericEvent{GenericEvent: &protos.GenericEvent{Data: wrapperspb.String("audit")}}},
		{EventType: &protos.HistoryEvent_OrchestratorCompleted{OrchestratorCompleted: &protos.OrchestratorCompletedEvent{}}},
		rewindEvent(""),
		failedCompletionEvent(),
	}
	newEvents := []*protos.HistoryEvent{helpers.NewOrchestratorStartedEvent(), rewindEvent("")}
	original := proto.CloneOf(&protos.OrchestratorRequest{PastEvents: old, NewEvents: newEvents})
	// Rewriting is a protocol operation, not application replay or version dispatch.
	executor := NewTaskExecutor(NewTaskRegistry(),
		WithOrchestratorNotFoundStrategy(OrchestratorNotFoundReject),
		WithVersioning(VersioningOptions{DefaultVersion: "2.0", MatchStrategy: VersionMatchStrict, FailureStrategy: VersionFailureReject}),
		WithOrchestrationOptions(OrchestrationOptions{MaxEventsPerTurn: 1, MaxHistoryEvents: 1}),
	)
	result, err := executor.ExecuteOrchestrator(context.Background(), "instance", old, newEvents, nil)
	require.NoError(t, err)
	require.Equal(t, "instance", result.Response.InstanceId)
	require.Nil(t, result.Response.CustomStatus)
	require.Nil(t, result.Response.NumEventsProcessed)
	require.Len(t, result.Response.Actions, 1)
	action := result.Response.Actions[0]
	require.EqualValues(t, -1, action.Id)
	history := action.GetRewindOrchestration().GetNewHistory()
	want := []*protos.HistoryEvent{old[0], proto.CloneOf(start), old[2], old[3], old[6], old[8], old[9], old[10], old[11], old[12], old[13], old[14], newEvents[0], newEvents[1]}
	require.Len(t, history, len(want))
	newID := history[1].GetExecutionStarted().GetOrchestrationInstance().GetExecutionId().GetValue()
	require.Len(t, newID, 32)
	parsed, err := uuid.Parse(newID)
	require.NoError(t, err)
	require.Equal(t, uuid.Version(4), parsed.Version())
	require.NotEqual(t, start.GetExecutionStarted().GetOrchestrationInstance().GetExecutionId().GetValue(), newID)
	want[1].GetExecutionStarted().OrchestrationInstance.ExecutionId = wrapperspb.String(newID)
	for i := range want {
		require.Truef(t, proto.Equal(want[i], history[i]), "history event %d changed", i)
	}
	require.True(t, proto.Equal(original, &protos.OrchestratorRequest{PastEvents: old, NewEvents: newEvents}))

	redelivered, err := executor.ExecuteOrchestrator(context.Background(), "instance", old, newEvents, nil)
	require.NoError(t, err)
	require.NotEqual(t, newID, redelivered.Response.Actions[0].GetRewindOrchestration().NewHistory[1].GetExecutionStarted().GetOrchestrationInstance().GetExecutionId().GetValue())
}

func TestRewindParentExecutionIdentity(t *testing.T) {
	for _, test := range []struct {
		name      string
		parent    *protos.ParentInstanceInfo
		requestID string
		wantID    string
	}{
		{"update child", &protos.ParentInstanceInfo{OrchestrationInstance: &protos.OrchestrationInstance{InstanceId: "parent", ExecutionId: wrapperspb.String("old")}}, "new-parent", "new-parent"},
		{"preserve parent", &protos.ParentInstanceInfo{OrchestrationInstance: &protos.OrchestrationInstance{ExecutionId: wrapperspb.String("old")}}, "", "old"},
		{"missing nested identity", &protos.ParentInstanceInfo{}, "new-parent", "new-parent"},
		{"no synthetic parent", nil, "new-parent", ""},
	} {
		t.Run(test.name, func(t *testing.T) {
			start := helpers.NewExecutionStartedEvent("child", "child-id", nil, test.parent, nil, nil)
			original := proto.CloneOf(start)
			result, err := buildRewindResult("child-id",
				[]*protos.HistoryEvent{start, failedCompletionEvent()},
				[]*protos.HistoryEvent{helpers.NewOrchestratorStartedEvent(), rewindEvent(test.requestID)})
			require.NoError(t, err)
			rewritten := result.Response.Actions[0].GetRewindOrchestration().NewHistory[0].GetExecutionStarted()
			require.Equal(t, test.wantID, rewritten.GetParentInstance().GetOrchestrationInstance().GetExecutionId().GetValue())
			if test.parent == nil {
				require.Nil(t, rewritten.ParentInstance)
			}
			require.True(t, proto.Equal(original, start))
		})
	}
}

func TestRewindJumpStartReplaysWithoutRewriting(t *testing.T) {
	registry := NewTaskRegistry()
	require.NoError(t, registry.AddOrchestratorN("workflow", func(ctx *OrchestrationContext) (any, error) {
		var good string
		if err := ctx.CallActivity("good").Await(&good); err != nil {
			return nil, err
		}
		return nil, ctx.CallActivity("bad", WithActivityInput(good)).Await(nil)
	}))
	start := helpers.NewExecutionStartedEvent("workflow", "instance", nil, nil, nil, nil)
	old := []*protos.HistoryEvent{
		helpers.NewOrchestratorStartedEvent(), start,
		helpers.NewTaskScheduledEvent(0, "good", nil, nil, nil),
		helpers.NewTaskCompletedEvent(0, wrapperspb.String(`"kept"`)),
		rewindEvent(""),
	}
	result, err := NewTaskExecutor(registry, WithOrchestrationOptions(OrchestrationOptions{MaxEventsPerTurn: 1})).
		ExecuteOrchestrator(context.Background(), "instance", old,
			[]*protos.HistoryEvent{helpers.NewOrchestratorStartedEvent(), rewindEvent("")}, nil)
	require.NoError(t, err)
	require.Len(t, result.Response.Actions, 1)
	require.EqualValues(t, 1, result.Response.Actions[0].Id)
	scheduled := result.Response.Actions[0].GetScheduleTask()
	require.NotNil(t, scheduled)
	require.Equal(t, "bad", scheduled.Name)
	require.Equal(t, `"kept"`, scheduled.GetInput().GetValue())
}

func TestRewindMalformedRequests(t *testing.T) {
	for _, events := range [][]*protos.HistoryEvent{
		{rewindEvent("")},
		{rewindEvent(""), helpers.NewOrchestratorStartedEvent()},
		{helpers.NewOrchestratorStartedEvent(), rewindEvent(""), helpers.NewEventRaisedEvent("extra", nil)},
	} {
		_, err := NewTaskExecutor(NewTaskRegistry()).ExecuteOrchestrator(context.Background(), "instance",
			[]*protos.HistoryEvent{failedCompletionEvent()}, events, nil)
		require.ErrorContains(t, err, "rewind requires exactly two new events")
	}
	require.False(t, isRewindRequest([]*protos.HistoryEvent{rewindEvent(""), failedCompletionEvent()}, nil))
	require.False(t, isRewindRequest(nil, []*protos.HistoryEvent{rewindEvent("")}))
}
