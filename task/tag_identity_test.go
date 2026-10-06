package task

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/microsoft/durabletask-go/api"
	"github.com/microsoft/durabletask-go/internal/contextprop"
	"github.com/microsoft/durabletask-go/internal/helpers"
	"github.com/microsoft/durabletask-go/internal/protos"
	"github.com/microsoft/durabletask-go/internal/tagcodec"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

func scheduledActivityAction(t testing.TB, response *protos.OrchestratorResponse) *protos.OrchestratorAction {
	t.Helper()
	for _, action := range response.Actions {
		if action.GetScheduleTask() != nil {
			return action
		}
	}
	t.Fatal("missing scheduled activity")
	return nil
}

func TestScheduledActivityIdentityIsOptIn(t *testing.T) {
	for _, version := range []string{"", "v1"} {
		for _, includeIdentity := range []bool{false, true} {
			for _, withCallerData := range []bool{false, true} {
				t.Run(version+"/identity="+boolName(includeIdentity)+"/caller="+boolName(withCallerData), func(t *testing.T) {
					registry := NewTaskRegistry()
					options := []CallActivityOption{
						WithActivityVersion("a2"),
						WithRawActivityInput(`"input"`),
					}
					if includeIdentity {
						options = append(options, WithActivityOrchestrationIdentity())
					}
					var fields api.ContextFields
					var tags map[string]string
					if withCallerData {
						fields = api.ContextFields{"tenant": "persisted", "empty": ""}
						tags = map[string]string{"tenant": "user", "scope": "parent", "empty": ""}
						options = append(options, WithActivityTags(map[string]string{"scope": "activity"}))
					}
					require.NoError(t, registry.AddOrchestratorNVersion("parent", version, func(ctx *OrchestrationContext) (any, error) {
						ctx.CallActivity("inspect", options...)
						return nil, nil
					}))
					started := helpers.NewExecutionStartedEvent("parent", "instance", nil,
						helpers.NewParentInfo(7, "root", "root-instance"), nil, nil, wrapperspb.String(version))
					started.GetExecutionStarted().Tags = contextprop.Encode(nil, fields, tags)
					response := executeOrchestrationTurn(t, registry, "instance", nil, []*protos.HistoryEvent{started})
					action := scheduledActivityAction(t, response)
					scheduled := action.GetScheduleTask()
					require.EqualValues(t, 0, action.Id)
					require.Equal(t, `"input"`, scheduled.GetInput().GetValue())
					require.Equal(t, "a2", scheduled.GetVersion().GetValue())
					info, persistedFields := contextprop.Decode(scheduled.Tags)
					wantInfo := api.OrchestrationContextInfo{InstanceID: "instance"}
					if includeIdentity {
						wantInfo.Name, wantInfo.Version, wantInfo.ParentInstanceID = "parent", version, "root-instance"
						require.Equal(t, wantInfo, info)
					} else {
						require.Equal(t, api.OrchestrationContextInfo{}, info)
						if !withCallerData {
							require.Nil(t, scheduled.Tags)
						}
					}
					require.Equal(t, fields, persistedFields)
					if withCallerData {
						require.Equal(t, map[string]string{
							"tenant": "user", "scope": "activity", "empty": "",
						}, tagcodec.DecodeUserTagsOrPlain(scheduled.Tags))
					}
					require.NoError(t, registry.AddActivityNVersion("inspect", "a2", func(ctx ActivityContext) (any, error) {
						orchestration, ok := api.OrchestrationContextInfoFromContext(ctx.Context())
						require.True(t, ok)
						require.Equal(t, wantInfo, orchestration)
						activity, ok := api.ActivityContextInfoFromContext(ctx.Context())
						require.True(t, ok)
						require.Equal(t, api.ActivityContextInfo{
							InstanceID: "instance", Name: "inspect", Version: "a2", TaskID: 0,
						}, activity)
						require.Equal(t, fields, api.ContextFieldsFromContext(ctx.Context()))
						var input string
						require.NoError(t, ctx.GetInput(&input))
						return input, nil
					}))
					event := helpers.NewTaskScheduledEvent(action.Id, scheduled.Name, scheduled.Version, scheduled.Input, nil)
					event.GetTaskScheduled().Tags = contextprop.Clone(scheduled.Tags)
					result, err := NewTaskExecutor(registry).ExecuteActivity(context.Background(), "instance", event)
					require.NoError(t, err)
					require.Equal(t, `"input"`, result.GetTaskCompleted().GetResult().GetValue())
				})
			}
		}
	}
}

func boolName(value bool) string {
	if value {
		return "yes"
	}
	return "no"
}

func legacyIdentityTags() map[string]string {
	return map[string]string{
		"__durabletask.context.encoding":              "1",
		"__durabletask.context.instance_id":           "instance",
		"__durabletask.context.orchestration_name":    "parent",
		"__durabletask.context.orchestration_version": "v1",
		"__durabletask.context.parent_instance_id":    "root-instance",
	}
}

func TestLegacyActivityTagsReplayWithNewWriter(t *testing.T) {
	registry := NewTaskRegistry()
	require.NoError(t, registry.AddOrchestratorNVersion("parent", "v1", func(ctx *OrchestrationContext) (any, error) {
		var output string
		if err := ctx.CallActivity("first", WithRawActivityInput(`"original"`)).Await(&output); err != nil {
			return nil, err
		}
		canceled, cancel := ctx.WithCancel()
		cancel()
		require.ErrorIs(t, canceled.WaitForSingleEvent("never", -1).Await(nil), ErrTaskCanceled)
		canceled.CallActivity("never", WithActivityOrchestrationIdentity())
		return nil, ctx.CallActivity("next", WithActivityInput(output)).Await(nil)
	}))
	started := helpers.NewExecutionStartedEvent("parent", "instance", nil, nil, nil, nil, wrapperspb.String("v1"))
	started.GetExecutionStarted().Tags = legacyIdentityTags()
	started.GetExecutionStarted().Tags["user"] = "retained"
	started.GetExecutionStarted().Tags[tagcodec.ContextFieldPrefix+"tenant"] = "retained"
	scheduled := helpers.NewTaskScheduledEvent(0, "first", wrapperspb.String("v1"), wrapperspb.String(`"original"`), nil)
	scheduled.GetTaskScheduled().Tags = legacyIdentityTags()
	old := []*protos.HistoryEvent{helpers.NewOrchestratorStartedEvent(), started, scheduled}
	delivered := []*protos.HistoryEvent{helpers.NewTaskCompletedEvent(0, wrapperspb.String(`"output"`))}
	first := executeOrchestrationTurn(t, registry, "instance", old, delivered)
	replayed := executeOrchestrationTurn(t, registry, "instance", append(old, delivered...), nil)
	require.True(t, proto.Equal(first, replayed), "redelivery changed the actions")
	next := scheduledActivityAction(t, first)
	require.EqualValues(t, 1, next.Id, "canceled work must not consume a sequence number")
	require.Equal(t, "next", next.GetScheduleTask().Name)
	require.Equal(t, "v1", next.GetScheduleTask().GetVersion().GetValue())
	require.Equal(t, `"output"`, next.GetScheduleTask().GetInput().GetValue())
	require.Equal(t, map[string]string{
		tagcodec.ContextEncodingTag:            "1",
		tagcodec.ContextFieldPrefix + "tenant": "retained",
		"user":                                 "retained",
	}, next.GetScheduleTask().Tags)
}

func TestActivityIdentityChoiceSurvivesRetryAndRedelivery(t *testing.T) {
	for _, includeIdentity := range []bool{false, true} {
		t.Run(boolName(includeIdentity), func(t *testing.T) {
			registry := NewTaskRegistry()
			require.NoError(t, registry.AddOrchestratorNVersion("parent", "v1", func(ctx *OrchestrationContext) (any, error) {
				options := []CallActivityOption{
					WithActivityRetryPolicy(&RetryPolicy{MaxAttempts: 2, InitialRetryInterval: time.Second}),
					WithRawActivityInput(`"retry"`),
					WithActivityTags(map[string]string{"scope": "activity"}),
				}
				if includeIdentity {
					options = append(options, WithActivityOrchestrationIdentity())
				}
				return nil, ctx.CallActivity("flaky", options...).Await(nil)
			}))
			now := time.Unix(1_700_000_000, 0).UTC()
			turn := helpers.NewOrchestratorStartedEvent()
			turn.Timestamp = timestamppb.New(now)
			started := helpers.NewExecutionStartedEvent("parent", "instance", nil,
				helpers.NewParentInfo(7, "root", "root-instance"), nil, nil, wrapperspb.String("v1"))
			started.GetExecutionStarted().Tags = contextprop.Encode(nil, api.ContextFields{"tenant": "persisted"},
				map[string]string{"scope": "parent", "user": "retained"})
			initial := executeOrchestrationTurn(t, registry, "instance", nil, []*protos.HistoryEvent{turn, started})
			firstAction := scheduledActivityAction(t, initial)
			scheduled := helpers.NewTaskScheduledEvent(0, "flaky", wrapperspb.String("v1"), wrapperspb.String(`"retry"`), nil)
			scheduled.GetTaskScheduled().Tags = contextprop.Clone(firstAction.GetScheduleTask().Tags)
			failure := helpers.NewTaskFailedEvent(0, &protos.TaskFailureDetails{ErrorType: "Transient", ErrorMessage: "retry"})
			history := []*protos.HistoryEvent{turn, started, scheduled, failure}
			timerResponse := executeOrchestrationTurn(t, registry, "instance", history, nil)
			require.Len(t, timerResponse.Actions, 1)
			timer := timerResponse.Actions[0]
			require.EqualValues(t, 1, timer.Id)
			require.NotNil(t, timer.GetCreateTimer())
			history = append(history, helpers.NewTimerCreatedEvent(timer.Id, timer.GetCreateTimer().FireAt))
			retryTurn := helpers.NewOrchestratorStartedEvent()
			retryTurn.Timestamp = timer.GetCreateTimer().FireAt
			delivered := []*protos.HistoryEvent{retryTurn, helpers.NewTimerFiredEvent(timer.Id, timer.GetCreateTimer().FireAt, nil)}
			retryResponse := executeOrchestrationTurn(t, registry, "instance", history, delivered,
				WithContextFields(api.ContextFields{"host": "first"}))
			replayed := executeOrchestrationTurn(t, registry, "instance", append(history, delivered...), nil,
				WithContextFields(api.ContextFields{"host": "second"}))
			require.True(t, proto.Equal(retryResponse, replayed))
			retry := scheduledActivityAction(t, retryResponse)
			require.EqualValues(t, 2, retry.Id)
			require.True(t, proto.Equal(firstAction.GetScheduleTask(), retry.GetScheduleTask()))
			retryEvent := helpers.NewTaskScheduledEvent(retry.Id, "flaky", wrapperspb.String("v1"), wrapperspb.String(`"retry"`), nil)
			retryEvent.GetTaskScheduled().Tags = contextprop.Clone(retry.GetScheduleTask().Tags)
			require.NoError(t, registry.AddActivityNVersion("flaky", "v1", func(ctx ActivityContext) (any, error) {
				info, _ := api.OrchestrationContextInfoFromContext(ctx.Context())
				want := api.OrchestrationContextInfo{InstanceID: "instance"}
				if includeIdentity {
					want.Name, want.Version, want.ParentInstanceID = "parent", "v1", "root-instance"
				}
				require.Equal(t, want, info)
				require.Equal(t, api.ContextFields{"tenant": "persisted"}, api.ContextFieldsFromContext(ctx.Context()))
				return "recovered", nil
			}))
			completed, err := NewTaskExecutor(registry).ExecuteActivity(context.Background(), "instance", retryEvent)
			require.NoError(t, err)
			require.Equal(t, `"recovered"`, completed.GetTaskCompleted().GetResult().GetValue())
			finished := executeOrchestrationTurn(t, registry, "instance",
				append(append(history, delivered...), retryEvent), []*protos.HistoryEvent{completed})
			require.Equal(t, protos.OrchestrationStatus_ORCHESTRATION_STATUS_COMPLETED,
				completionAction(t, finished).OrchestrationStatus)
		})
	}
}

func TestSubOrchestrationAndContinueAsNewUseNativeIdentity(t *testing.T) {
	registry := NewTaskRegistry()
	require.NoError(t, registry.AddOrchestratorNVersion("parent", "v1", func(ctx *OrchestrationContext) (any, error) {
		ctx.CallSubOrchestrator("child", WithSubOrchestrationVersion("c1"),
			WithSubOrchestrationTags(map[string]string{"scope": "child"}),
			WithSubOrchestrationContextFields(api.ContextFields{"tenant": "child"}))
		return nil, nil
	}))
	type state struct {
		Info   api.OrchestrationContextInfo
		Fields api.ContextFields
	}
	require.NoError(t, registry.AddOrchestratorNVersion("child", "c1", func(ctx *OrchestrationContext) (any, error) {
		info, _ := api.OrchestrationContextInfoFromContext(ctx.Context())
		require.Equal(t, api.OrchestrationContextInfo{
			InstanceID: ctx.ID, Name: "child", Version: "c1", ParentInstanceID: "instance",
		}, info)
		ctx.ContinueAsNew("next", WithContinueAsNewVersion("c2"))
		return nil, nil
	}))
	require.NoError(t, registry.AddOrchestratorNVersion("child", "c2", func(ctx *OrchestrationContext) (any, error) {
		info, _ := api.OrchestrationContextInfoFromContext(ctx.Context())
		return state{info, api.ContextFieldsFromContext(ctx.Context())}, nil
	}))
	started := helpers.NewExecutionStartedEvent("parent", "instance", nil, nil, nil, nil, wrapperspb.String("v1"))
	started.GetExecutionStarted().Tags = legacyIdentityTags()
	started.GetExecutionStarted().Tags["scope"] = "parent"
	started.GetExecutionStarted().Tags["user"] = "retained"
	started.GetExecutionStarted().Tags[tagcodec.ContextFieldPrefix+"tenant"] = "parent"
	response := executeOrchestrationTurn(t, registry, "instance", nil, []*protos.HistoryEvent{started})
	var child *protos.CreateSubOrchestrationAction
	for _, action := range response.Actions {
		if action.GetCreateSubOrchestration() != nil {
			child = action.GetCreateSubOrchestration()
		}
	}
	require.NotNil(t, child)
	wantTags := map[string]string{
		tagcodec.ContextEncodingTag:            "1",
		tagcodec.ContextFieldPrefix + "tenant": "child",
		"scope":                                "child",
		"user":                                 "retained",
	}
	require.Equal(t, wantTags, child.Tags)
	require.Equal(t, "instance:0000", child.InstanceId)
	parent := helpers.NewParentInfo(0, "parent", "instance")
	childStarted := helpers.NewExecutionStartedEvent(child.Name, child.InstanceId, child.Input,
		parent, nil, nil, child.Version)
	childStarted.GetExecutionStarted().Tags = contextprop.Clone(child.Tags)
	childResponse := executeOrchestrationTurn(t, registry, api.InstanceID(child.InstanceId), nil,
		[]*protos.HistoryEvent{childStarted})
	continued := completionAction(t, childResponse)
	require.Equal(t, protos.OrchestrationStatus_ORCHESTRATION_STATUS_CONTINUED_AS_NEW, continued.OrchestrationStatus)
	require.Equal(t, wantTags, continued.Tags)
	require.Equal(t, "c2", continued.GetNewVersion().GetValue())
	nextStarted := helpers.NewExecutionStartedEvent(child.Name, child.InstanceId, continued.Result,
		parent, nil, nil, continued.NewVersion)
	nextStarted.GetExecutionStarted().Tags = contextprop.Clone(continued.Tags)
	continued.Tags["scope"] = "mutated"
	next := executeOrchestrationTurn(t, registry, api.InstanceID(child.InstanceId), nil, []*protos.HistoryEvent{nextStarted})
	var output state
	require.NoError(t, json.Unmarshal([]byte(completionResult(t, next)), &output))
	require.Equal(t, api.OrchestrationContextInfo{
		InstanceID: api.InstanceID(child.InstanceId), Name: "child", Version: "c2", ParentInstanceID: "instance",
	}, output.Info)
	require.Equal(t, api.ContextFields{"tenant": "child"}, output.Fields)
	require.Equal(t, map[string]string{
		tagcodec.ContextEncodingTag: "1", "scope": "child", "user": "retained",
	}, completionAction(t, next).Tags)
}

func TestLegacySubOrchestrationTagsReplayAndRetry(t *testing.T) {
	registry := NewTaskRegistry()
	require.NoError(t, registry.AddOrchestratorNVersion("parent", "v1", func(ctx *OrchestrationContext) (any, error) {
		var output string
		err := ctx.CallSubOrchestrator("child", WithSubOrchestrationVersion("c1"),
			WithRawSubOrchestratorInput(`"input"`),
			WithSubOrchestrationTags(map[string]string{"scope": "child"}),
			WithSubOrchestrationContextFields(api.ContextFields{"tenant": "child"}),
			WithSubOrchestrationRetryPolicy(&RetryPolicy{
				MaxAttempts: 2, InitialRetryInterval: time.Second,
			})).Await(&output)
		return output, err
	}))
	turn := helpers.NewOrchestratorStartedEvent()
	turn.Timestamp = timestamppb.New(time.Unix(1_700_000_000, 0).UTC())
	started := helpers.NewExecutionStartedEvent("parent", "instance", nil, nil, nil, nil, wrapperspb.String("v1"))
	started.GetExecutionStarted().Tags = contextprop.Encode(nil, api.ContextFields{"tenant": "parent"},
		map[string]string{"scope": "parent", "user": ""})
	created := helpers.NewSubOrchestrationCreatedEvent(0, "child", wrapperspb.String("c1"),
		wrapperspb.String(`"input"`), "instance:0000", nil)
	created.GetSubOrchestrationInstanceCreated().Tags = legacyIdentityTags()
	failed := &protos.HistoryEvent{EventType: &protos.HistoryEvent_SubOrchestrationInstanceFailed{
		SubOrchestrationInstanceFailed: &protos.SubOrchestrationInstanceFailedEvent{
			TaskScheduledId: 0, FailureDetails: &protos.TaskFailureDetails{ErrorType: "Transient", ErrorMessage: "retry"},
		},
	}}
	history := []*protos.HistoryEvent{turn, started, created, failed}
	timer := singleRetryTimer(t, executeOrchestrationTurn(t, registry, "instance", history, nil))
	history = append(history, helpers.NewTimerCreatedEvent(1, timer.FireAt))
	retryTurn := helpers.NewOrchestratorStartedEvent()
	retryTurn.Timestamp = timer.FireAt
	delivered := []*protos.HistoryEvent{retryTurn, helpers.NewTimerFiredEvent(1, timer.FireAt, nil)}
	response := executeOrchestrationTurn(t, registry, "instance", history, delivered)
	replayed := executeOrchestrationTurn(t, registry, "instance", append(history, delivered...), nil)
	require.True(t, proto.Equal(response, replayed))
	require.Len(t, response.Actions, 1)
	retryAction := response.Actions[0]
	require.EqualValues(t, 2, retryAction.Id)
	retry := retryAction.GetCreateSubOrchestration()
	require.NotNil(t, retry)
	require.Equal(t, "instance:0002", retry.InstanceId)
	require.Equal(t, "child", retry.Name)
	require.Equal(t, "c1", retry.GetVersion().GetValue())
	require.Equal(t, `"input"`, retry.GetInput().GetValue())
	require.Equal(t, map[string]string{
		tagcodec.ContextEncodingTag:            "1",
		tagcodec.ContextFieldPrefix + "tenant": "child",
		"scope":                                "child",
		"user":                                 "",
	}, retry.Tags)
	retryEvent := helpers.NewSubOrchestrationCreatedEvent(retryAction.Id, retry.Name, retry.Version,
		retry.Input, retry.InstanceId, nil)
	retryEvent.GetSubOrchestrationInstanceCreated().Tags = contextprop.Clone(retry.Tags)
	completed := &protos.HistoryEvent{EventType: &protos.HistoryEvent_SubOrchestrationInstanceCompleted{
		SubOrchestrationInstanceCompleted: &protos.SubOrchestrationInstanceCompletedEvent{
			TaskScheduledId: retryAction.Id, Result: wrapperspb.String(`"completed"`),
		},
	}}
	finished := executeOrchestrationTurn(t, registry, "instance",
		append(append(history, delivered...), retryEvent), []*protos.HistoryEvent{completed})
	require.Equal(t, `"completed"`, completionResult(t, finished))
}

func TestUntaggedLifecycleActionsHaveNoIdentityTags(t *testing.T) {
	registry := NewTaskRegistry()
	require.NoError(t, registry.AddOrchestratorN("parent", func(ctx *OrchestrationContext) (any, error) {
		ctx.CallActivity("activity")
		ctx.CallSubOrchestrator("child")
		ctx.ContinueAsNew(nil)
		return nil, nil
	}))
	started := helpers.NewExecutionStartedEvent("parent", "instance", nil, nil, nil, nil)
	for _, tags := range []map[string]string{nil, {}, legacyIdentityTags()} {
		started.GetExecutionStarted().Tags = tags
		response := executeOrchestrationTurn(t, registry, "instance", nil, []*protos.HistoryEvent{started})
		require.Len(t, response.Actions, 3)
		for _, action := range response.Actions {
			switch {
			case action.GetScheduleTask() != nil:
				require.Nil(t, action.GetScheduleTask().Tags)
			case action.GetCreateSubOrchestration() != nil:
				require.Nil(t, action.GetCreateSubOrchestration().Tags)
			case action.GetCompleteOrchestration() != nil:
				require.Nil(t, action.GetCompleteOrchestration().Tags)
			default:
				t.Fatalf("unexpected action: %v", action)
			}
		}
	}
}

func TestActivityExplicitContextOverridesHistoricalIdentity(t *testing.T) {
	registry := NewTaskRegistry()
	require.NoError(t, registry.AddActivityN("inspect", func(ctx ActivityContext) (any, error) {
		info, _ := api.OrchestrationContextInfoFromContext(ctx.Context())
		require.Equal(t, api.OrchestrationContextInfo{
			InstanceID: "explicit-instance", Name: "explicit-name", Version: "v1", ParentInstanceID: "root-instance",
		}, info)
		require.Equal(t, api.ContextFields{"tenant": "durable", "worker": "local"},
			api.ContextFieldsFromContext(ctx.Context()))
		return "retained", nil
	}))
	base := api.WithOrchestrationContextInfo(context.Background(), api.OrchestrationContextInfo{
		InstanceID: "explicit-instance", Name: "explicit-name",
	})
	event := helpers.NewTaskScheduledEvent(4, "inspect", nil, nil, nil)
	event.GetTaskScheduled().Tags = legacyIdentityTags()
	event.GetTaskScheduled().Tags[tagcodec.ContextFieldPrefix+"tenant"] = "durable"
	event.GetTaskScheduled().Tags["user"] = "not a context field"
	response, err := NewTaskExecutor(registry, WithContextFields(api.ContextFields{
		"tenant": "worker", "worker": "local",
	})).ExecuteActivity(base, "instance", event)
	require.NoError(t, err)
	require.Equal(t, `"retained"`, response.GetTaskCompleted().GetResult().GetValue())
}
