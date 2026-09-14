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
		{EventId: 1, EventType: &protos.HistoryEvent_SubOrchestrationInstanceCreated{
			SubOrchestrationInstanceCreated: &protos.SubOrchestrationInstanceCreatedEvent{InstanceId: "successful-child", Name: "child"},
		}},
		{EventType: &protos.HistoryEvent_SubOrchestrationInstanceCompleted{
			SubOrchestrationInstanceCompleted: &protos.SubOrchestrationInstanceCompletedEvent{TaskScheduledId: 1, Result: wrapperspb.String(`"child-kept"`)},
		}},
		helpers.NewTimerCreatedEvent(2, start.Timestamp),
		{EventType: &protos.HistoryEvent_TimerFired{TimerFired: &protos.TimerFiredEvent{TimerId: 2}}},
		{EventType: &protos.HistoryEvent_GenericEvent{GenericEvent: &protos.GenericEvent{Data: wrapperspb.String("audit")}}},
		rewindEvent(""),
		helpers.NewTaskScheduledEvent(3, "bad", nil, nil, nil),
		helpers.NewTaskFailedEvent(3, &protos.TaskFailureDetails{ErrorMessage: "failed"}),
		{EventType: &protos.HistoryEvent_OrchestratorCompleted{OrchestratorCompleted: &protos.OrchestratorCompletedEvent{}}},
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
	want := append([]*protos.HistoryEvent(nil), old[:10]...)
	want[1] = proto.CloneOf(start)
	want = append(want, old[12], newEvents[0], newEvents[1])
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
		{nil, rewindEvent("")},
		{new(protos.HistoryEvent), rewindEvent("")},
		{helpers.NewTaskCompletedEvent(0, nil), rewindEvent("")},
		{helpers.NewOrchestratorStartedEvent(), rewindEvent(""), helpers.NewEventRaisedEvent("extra", nil)},
	} {
		result, err := NewTaskExecutor(NewTaskRegistry()).ExecuteOrchestrator(context.Background(), "instance",
			[]*protos.HistoryEvent{failedCompletionEvent()}, events, nil)
		require.ErrorContains(t, err, "rewind requires exactly two new events")
		require.Nil(t, result)
	}
	require.False(t, isRewindRequest([]*protos.HistoryEvent{rewindEvent(""), failedCompletionEvent()}, nil))
	require.False(t, isRewindRequest(nil, []*protos.HistoryEvent{rewindEvent("")}))
}

func TestRewindReplacementReplaysSuccessfulPrefix(t *testing.T) {
	registry := NewTaskRegistry()
	require.NoError(t, registry.AddOrchestratorN("workflow", func(ctx *OrchestrationContext) (any, error) {
		var good, recovered string
		if err := ctx.CallActivity("good").Await(&good); err != nil {
			return nil, err
		}
		if err := ctx.CallActivity("bad", WithActivityInput(good)).Await(&recovered); err != nil {
			return nil, err
		}
		return good + ":" + recovered, nil
	}))
	executor := NewTaskExecutor(registry)
	history := []*protos.HistoryEvent{
		helpers.NewOrchestratorStartedEvent(),
		helpers.NewExecutionStartedEvent("workflow", "instance", nil, nil, nil, nil),
		helpers.NewTaskScheduledEvent(0, "good", nil, nil, nil),
		helpers.NewTaskCompletedEvent(0, wrapperspb.String(`"kept"`)),
		helpers.NewTaskScheduledEvent(1, "bad", nil, wrapperspb.String(`"kept"`), nil),
		helpers.NewTaskFailedEvent(1, nil), failedCompletionEvent(),
	}
	rewrite, err := executor.ExecuteOrchestrator(context.Background(), "instance", history,
		[]*protos.HistoryEvent{helpers.NewOrchestratorStartedEvent(), rewindEvent("")}, nil)
	require.NoError(t, err)
	replacement := rewrite.Response.Actions[0].GetRewindOrchestration().GetNewHistory()
	newEvents := []*protos.HistoryEvent{helpers.NewOrchestratorStartedEvent(), rewindEvent("")}
	replay, err := executor.ExecuteOrchestrator(context.Background(), "instance", replacement, newEvents, nil)
	require.NoError(t, err)
	require.Len(t, replay.Response.Actions, 1)
	retried := replay.Response.Actions[0]
	require.EqualValues(t, 1, retried.Id)
	require.Equal(t, "bad", retried.GetScheduleTask().GetName())
	require.Equal(t, `"kept"`, retried.GetScheduleTask().GetInput().GetValue())
	replacement = append(replacement, newEvents...)
	replacement = append(replacement, helpers.NewTaskScheduledEvent(retried.Id, "bad", nil, retried.GetScheduleTask().Input, nil))
	completed, err := executor.ExecuteOrchestrator(context.Background(), "instance", replacement,
		[]*protos.HistoryEvent{helpers.NewOrchestratorStartedEvent(), helpers.NewTaskCompletedEvent(retried.Id, wrapperspb.String(`"recovered"`))}, nil)
	require.NoError(t, err)
	require.Len(t, completed.Response.Actions, 1)
	require.Equal(t, `"kept:recovered"`, completed.Response.Actions[0].GetCompleteOrchestration().GetResult().GetValue())
}

func TestRewindRejectsHistoryAfterAFailure(t *testing.T) {
	firstFailure := helpers.NewTaskFailedEvent(0, &protos.TaskFailureDetails{ErrorMessage: "handled"})
	for _, test := range []struct {
		name   string
		events []*protos.HistoryEvent
	}{
		{"handled activity", []*protos.HistoryEvent{
			helpers.NewTaskScheduledEvent(0, "handled", nil, nil, nil), firstFailure,
			helpers.NewTaskScheduledEvent(1, "good", nil, nil, nil), helpers.NewTaskCompletedEvent(1, wrapperspb.String(`"kept"`)),
			helpers.NewTaskScheduledEvent(2, "terminal", nil, nil, nil), helpers.NewTaskFailedEvent(2, nil),
		}},
		{"handled child", []*protos.HistoryEvent{
			{EventId: 0, EventType: &protos.HistoryEvent_SubOrchestrationInstanceCreated{
				SubOrchestrationInstanceCreated: &protos.SubOrchestrationInstanceCreatedEvent{Name: "child", InstanceId: "child"},
			}},
			{EventType: &protos.HistoryEvent_SubOrchestrationInstanceFailed{
				SubOrchestrationInstanceFailed: &protos.SubOrchestrationInstanceFailedEvent{TaskScheduledId: 0},
			}},
			helpers.NewTaskScheduledEvent(1, "good", nil, nil, nil), helpers.NewTaskCompletedEvent(1, wrapperspb.String(`"kept"`)),
		}},
		{"handled activity then local error", []*protos.HistoryEvent{
			helpers.NewTaskScheduledEvent(0, "handled", nil, nil, nil), firstFailure,
			helpers.NewTaskScheduledEvent(1, "good", nil, nil, nil), helpers.NewTaskCompletedEvent(1, wrapperspb.String(`"kept"`)),
		}},
		{"concurrent success after failure", []*protos.HistoryEvent{
			helpers.NewTaskScheduledEvent(0, "bad", nil, nil, nil), helpers.NewTaskScheduledEvent(1, "good", nil, nil, nil),
			firstFailure, helpers.NewTaskCompletedEvent(1, wrapperspb.String(`"kept"`)),
		}},
		{"multiple failures", []*protos.HistoryEvent{
			helpers.NewTaskScheduledEvent(0, "bad", nil, nil, nil), helpers.NewTaskScheduledEvent(1, "also-bad", nil, nil, nil),
			firstFailure, helpers.NewTaskFailedEvent(1, nil),
		}},
		{"retry timer", []*protos.HistoryEvent{
			helpers.NewTaskScheduledEvent(0, "bad", nil, nil, nil), firstFailure,
			helpers.NewTimerCreatedEvent(1, helpers.NewOrchestratorStartedEvent().Timestamp),
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			history := append([]*protos.HistoryEvent{
				helpers.NewOrchestratorStartedEvent(),
				helpers.NewExecutionStartedEvent("workflow", "instance", nil, nil, nil, nil),
			}, test.events...)
			history = append(history, failedCompletionEvent())
			original := proto.CloneOf(&protos.OrchestratorRequest{PastEvents: history})
			result, err := NewTaskExecutor(NewTaskRegistry()).ExecuteOrchestrator(context.Background(), "instance",
				history, []*protos.HistoryEvent{helpers.NewOrchestratorStartedEvent(), rewindEvent("")}, nil)
			require.ErrorIs(t, err, api.ErrFeatureNotSupported)
			require.Nil(t, result)
			require.True(t, proto.Equal(original, &protos.OrchestratorRequest{PastEvents: history}))
		})
	}
}
