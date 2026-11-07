package task

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/microsoft/durabletask-go/api"
	"github.com/microsoft/durabletask-go/internal/helpers"
	"github.com/microsoft/durabletask-go/internal/protos"
)

var sessionTestStart = time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)

// sessionScenario describes a multi-turn orchestration run against a fake
// backend. After each turn the backend stores the batch plus the history
// events created from the sent actions, then sends a new batch.
type sessionScenario struct {
	orchestrator Orchestrator
	input        *wrapperspb.StringValue
	options      []TaskExecutorOption
	// perTurn limits how many outstanding responses are delivered in one
	// batch. Zero delivers all of them.
	perTurn int
	// extra adds events to the batch of the given resumed turn (1-based).
	extra map[int][]*protos.HistoryEvent
	// respond overrides the default response for an action. Return nil to
	// use the default.
	respond func(action *protos.OrchestratorAction) *protos.HistoryEvent
	// step is the time between turns. Zero means one minute.
	step time.Duration
}

type sessionRun struct {
	responses []*protos.OrchestratorResponse
	// historyLengths are the HistoryLength values reported by the session path.
	historyLengths []int
	sessionEnded   bool
}

const sessionTestInstance = api.InstanceID("session-instance")

func sessionOrchestratorStarted(at time.Time) *protos.HistoryEvent {
	return &protos.HistoryEvent{
		EventId:   -1,
		Timestamp: timestamppb.New(at),
		EventType: &protos.HistoryEvent_OrchestratorStarted{OrchestratorStarted: &protos.OrchestratorStartedEvent{}},
	}
}

func sessionExecutionStarted(name string, input *wrapperspb.StringValue) *protos.HistoryEvent {
	event := helpers.NewExecutionStartedEvent(name, string(sessionTestInstance), input, nil, nil, nil)
	event.Timestamp = timestamppb.New(sessionTestStart)
	event.GetExecutionStarted().OrchestrationInstance.ExecutionId = wrapperspb.String("execution-1")
	return event
}

func newSessionTestExecutor(
	registry *TaskRegistry,
	lengths *[]int,
	options ...TaskExecutorOption,
) *taskExecutor {
	hooks := MetricsHooks{History: func(metric HistoryMetric) {
		*lengths = append(*lengths, metric.HistoryLength)
	}}
	all := append([]TaskExecutorOption{WithMetricsHooks(hooks)}, options...)
	return NewTaskExecutor(registry, all...).(*taskExecutor)
}

// historyFromActions builds the history events that the backend stores for
// the actions sent in one turn.
func historyFromActions(t *testing.T, actions []*protos.OrchestratorAction) []*protos.HistoryEvent {
	t.Helper()
	events := make([]*protos.HistoryEvent, 0, len(actions))
	for _, action := range actions {
		action = proto.Clone(action).(*protos.OrchestratorAction)
		event := &protos.HistoryEvent{EventId: action.Id, Timestamp: timestamppb.New(sessionTestStart)}
		switch {
		case action.GetScheduleTask() != nil:
			scheduled := action.GetScheduleTask()
			event.EventType = &protos.HistoryEvent_TaskScheduled{TaskScheduled: &protos.TaskScheduledEvent{
				Name: scheduled.Name, Version: scheduled.Version, Input: scheduled.Input, Tags: scheduled.Tags,
			}}
		case action.GetCreateTimer() != nil:
			event.EventType = &protos.HistoryEvent_TimerCreated{TimerCreated: &protos.TimerCreatedEvent{
				FireAt: action.GetCreateTimer().FireAt,
			}}
		case action.GetCreateSubOrchestration() != nil:
			created := action.GetCreateSubOrchestration()
			event.EventType = &protos.HistoryEvent_SubOrchestrationInstanceCreated{
				SubOrchestrationInstanceCreated: &protos.SubOrchestrationInstanceCreatedEvent{
					InstanceId: created.InstanceId, Name: created.Name, Version: created.Version, Input: created.Input,
				},
			}
		case action.GetSendEvent() != nil:
			sent := action.GetSendEvent()
			event.EventType = &protos.HistoryEvent_EventSent{EventSent: &protos.EventSentEvent{
				InstanceId: sent.GetInstance().GetInstanceId(), Name: sent.Name, Input: sent.Data,
			}}
		case action.GetSendEntityMessage() != nil:
			message := action.GetSendEntityMessage()
			switch {
			case message.GetEntityOperationCalled() != nil:
				event.EventType = &protos.HistoryEvent_EntityOperationCalled{EntityOperationCalled: message.GetEntityOperationCalled()}
			case message.GetEntityOperationSignaled() != nil:
				event.EventType = &protos.HistoryEvent_EntityOperationSignaled{EntityOperationSignaled: message.GetEntityOperationSignaled()}
			case message.GetEntityLockRequested() != nil:
				event.EventType = &protos.HistoryEvent_EntityLockRequested{EntityLockRequested: message.GetEntityLockRequested()}
			case message.GetEntityUnlockSent() != nil:
				event.EventType = &protos.HistoryEvent_EntityUnlockSent{EntityUnlockSent: message.GetEntityUnlockSent()}
			default:
				t.Fatalf("unsupported entity message: %v", message)
			}
		case action.GetCompleteOrchestration() != nil:
			continue
		default:
			t.Fatalf("unsupported action: %v", action)
		}
		events = append(events, event)
	}
	return events
}

// defaultResponse returns the inbox event that answers an action, or nil if
// the action has no response.
func defaultResponse(action *protos.OrchestratorAction) *protos.HistoryEvent {
	event := &protos.HistoryEvent{EventId: -1, Timestamp: timestamppb.New(sessionTestStart)}
	switch {
	case action.GetScheduleTask() != nil:
		result := wrapperspb.String(fmt.Sprintf("%q", fmt.Sprintf("%s-%d", action.GetScheduleTask().Name, action.Id)))
		event.EventType = &protos.HistoryEvent_TaskCompleted{TaskCompleted: &protos.TaskCompletedEvent{
			TaskScheduledId: action.Id, Result: result,
		}}
	case action.GetCreateTimer() != nil:
		event.EventType = &protos.HistoryEvent_TimerFired{TimerFired: &protos.TimerFiredEvent{
			TimerId: action.Id, FireAt: action.GetCreateTimer().FireAt,
		}}
	case action.GetCreateSubOrchestration() != nil:
		event.EventType = &protos.HistoryEvent_SubOrchestrationInstanceCompleted{
			SubOrchestrationInstanceCompleted: &protos.SubOrchestrationInstanceCompletedEvent{
				TaskScheduledId: action.Id, Result: wrapperspb.String(`"child-done"`),
			},
		}
	case action.GetSendEntityMessage().GetEntityOperationCalled() != nil:
		called := action.GetSendEntityMessage().GetEntityOperationCalled()
		event.EventType = &protos.HistoryEvent_EntityOperationCompleted{
			EntityOperationCompleted: &protos.EntityOperationCompletedEvent{
				RequestId: called.RequestId, Output: wrapperspb.String(fmt.Sprintf("%q", called.Operation+"-result")),
			},
		}
	case action.GetSendEntityMessage().GetEntityLockRequested() != nil:
		requested := action.GetSendEntityMessage().GetEntityLockRequested()
		event.EventType = &protos.HistoryEvent_EntityLockGranted{
			EntityLockGranted: &protos.EntityLockGrantedEvent{CriticalSectionId: requested.CriticalSectionId},
		}
	default:
		return nil
	}
	return event
}

// runSessionEquivalence runs the scenario through one session and, at every
// turn, through a fresh full replay of the same stored history. Both paths
// must produce the same response and the same history length.
func runSessionEquivalence(t *testing.T, scenario sessionScenario) sessionRun {
	t.Helper()
	baseline := runtime.NumGoroutine()
	registry := NewTaskRegistry()
	require.NoError(t, registry.AddOrchestratorN("session", scenario.orchestrator))
	var sessionLengths, replayLengths []int
	sessionExecutor := newSessionTestExecutor(registry, &sessionLengths, scenario.options...)
	replayExecutor := newSessionTestExecutor(registry, &replayLengths, scenario.options...)
	step := scenario.step
	if step == 0 {
		step = time.Minute
	}
	respond := func(action *protos.OrchestratorAction) *protos.HistoryEvent {
		if scenario.respond != nil {
			if event := scenario.respond(action); event != nil {
				return event
			}
		}
		return defaultResponse(action)
	}
	lastExtraTurn := 0
	for turn := range scenario.extra {
		lastExtraTurn = max(lastExtraTurn, turn)
	}

	ctx := context.Background()
	params := supportedEntityParameters()
	var stored, outstanding []*protos.HistoryEvent
	batch := []*protos.HistoryEvent{
		sessionOrchestratorStarted(sessionTestStart),
		sessionExecutionStarted("session", scenario.input),
	}
	result, session, err := sessionExecutor.StartOrchestratorSession(ctx, sessionTestInstance, nil, batch, params)
	require.NoError(t, err)
	run := sessionRun{}
	for turn := 0; ; turn++ {
		replay, replayErr := replayExecutor.ExecuteOrchestrator(ctx, sessionTestInstance, stored, batch, params)
		require.NoError(t, replayErr)
		requireSameResponse(t, turn, replay.Response, result.Response)
		require.Equal(t, replayLengths[len(replayLengths)-1], sessionLengths[len(sessionLengths)-1], "turn %d history length", turn)
		require.Equal(t, len(stored)+len(batch), sessionLengths[len(sessionLengths)-1], "turn %d history length", turn)
		run.responses = append(run.responses, result.Response)
		run.historyLengths = append(run.historyLengths, sessionLengths[len(sessionLengths)-1])

		if session == nil {
			run.sessionEnded = true
			break
		}
		require.Nil(t, result.Response.NumEventsProcessed, "turn %d: a partial turn must end the session", turn)
		require.Nil(t, completionOf(result.Response), "turn %d: a completed turn must end the session", turn)

		// The backend applies the turn and acknowledges it.
		stored = append(stored, batch...)
		stored = append(stored, historyFromActions(t, result.Response.Actions)...)
		session.Commit(result.Response.Actions)
		for _, action := range result.Response.Actions {
			if response := respond(action); response != nil {
				outstanding = append(outstanding, response)
			}
		}

		next := turn + 1
		if len(outstanding) == 0 && next > lastExtraTurn {
			session.Close()
			break
		}
		require.Less(t, next, 50, "scenario does not finish")
		deliver := len(outstanding)
		if scenario.perTurn > 0 {
			deliver = min(deliver, scenario.perTurn)
		}
		batch = []*protos.HistoryEvent{sessionOrchestratorStarted(sessionTestStart.Add(time.Duration(next) * step))}
		batch = append(batch, outstanding[:deliver]...)
		outstanding = outstanding[deliver:]
		batch = append(batch, scenario.extra[next]...)

		result, session, err = session.Resume(ctx, batch, len(stored))
		require.NoError(t, err)
	}
	requireNoGoroutineLeak(t, baseline)
	return run
}

func requireSameResponse(t *testing.T, turn int, want, got *protos.OrchestratorResponse) {
	t.Helper()
	if !proto.Equal(want, got) {
		t.Fatalf("turn %d: session response differs from full replay\nreplay:  %v\nsession: %v", turn, want, got)
	}
}

func completionOf(response *protos.OrchestratorResponse) *protos.CompleteOrchestrationAction {
	for _, action := range response.GetActions() {
		if completed := action.GetCompleteOrchestration(); completed != nil {
			return completed
		}
	}
	return nil
}

func requireNoGoroutineLeak(t *testing.T, baseline int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for runtime.NumGoroutine() > baseline && time.Now().Before(deadline) {
		runtime.Gosched()
		time.Sleep(5 * time.Millisecond)
	}
	require.LessOrEqual(t, runtime.NumGoroutine(), baseline, "orchestration goroutines leaked")
}

func requireCompletedWith(t *testing.T, run sessionRun, output string) {
	t.Helper()
	require.True(t, run.sessionEnded, "session should end with the orchestration")
	completed := completionOf(run.responses[len(run.responses)-1])
	require.NotNil(t, completed)
	require.Equal(t, protos.OrchestrationStatus_ORCHESTRATION_STATUS_COMPLETED, completed.OrchestrationStatus,
		"failure: %v", completed.GetFailureDetails())
	require.Equal(t, output, completed.GetResult().GetValue())
}

func eventRaised(name, rawInput string) *protos.HistoryEvent {
	event := helpers.NewEventRaisedEvent(name, wrapperspb.String(rawInput))
	event.Timestamp = timestamppb.New(sessionTestStart)
	return event
}

func suspended() *protos.HistoryEvent {
	event := helpers.NewSuspendOrchestrationEvent("pause")
	event.Timestamp = timestamppb.New(sessionTestStart)
	return event
}

func resumed() *protos.HistoryEvent {
	event := helpers.NewResumeOrchestrationEvent("continue")
	event.Timestamp = timestamppb.New(sessionTestStart)
	return event
}

func TestSessionReplayEquivalence(t *testing.T) {
	counter := api.NewEntityID("counter", "a")
	other := api.NewEntityID("counter", "b")

	tests := []struct {
		name     string
		scenario sessionScenario
		output   string
		check    func(t *testing.T, run sessionRun)
	}{
		{
			name:   "activities",
			output: `["first-0","second-1","third-2"]`,
			scenario: sessionScenario{orchestrator: func(ctx *OrchestrationContext) (any, error) {
				results := make([]string, 0, 3)
				for _, name := range []string{"first", "second", "third"} {
					var result string
					if err := ctx.CallActivity(name, WithActivityInput(len(results))).Await(&result); err != nil {
						return nil, err
					}
					results = append(results, result)
				}
				return results, nil
			}},
		},
		{
			name:   "activity retry",
			output: `"flaky-2"`,
			scenario: func() sessionScenario {
				failed := false
				return sessionScenario{
					orchestrator: func(ctx *OrchestrationContext) (any, error) {
						var result string
						err := ctx.CallActivity("flaky", WithActivityRetryPolicy(&RetryPolicy{
							MaxAttempts:          3,
							InitialRetryInterval: 10 * time.Second,
						})).Await(&result)
						return result, err
					},
					respond: func(action *protos.OrchestratorAction) *protos.HistoryEvent {
						if action.GetScheduleTask() == nil || failed {
							return nil
						}
						failed = true
						event := helpers.NewTaskFailedEvent(action.Id, &protos.TaskFailureDetails{
							ErrorType: "Transient", ErrorMessage: "try again",
						})
						event.Timestamp = timestamppb.New(sessionTestStart)
						return event
					},
				}
			}(),
		},
		{
			name:   "timers and long timer chunks",
			output: `"done"`,
			scenario: sessionScenario{
				options: []TaskExecutorOption{WithMaximumTimerInterval(time.Minute)},
				orchestrator: func(ctx *OrchestrationContext) (any, error) {
					if err := ctx.CreateTimer(30 * time.Second).Await(nil); err != nil {
						return nil, err
					}
					if err := ctx.CreateTimer(150 * time.Second).Await(nil); err != nil {
						return nil, err
					}
					return "done", nil
				},
			},
		},
		{
			name:   "sub-orchestrations and send event",
			output: `["child-done","child-done"]`,
			scenario: sessionScenario{perTurn: 1, orchestrator: func(ctx *OrchestrationContext) (any, error) {
				first := ctx.CallSubOrchestrator("child", WithSubOrchestratorInput(1))
				second := ctx.CallSubOrchestrator("child", WithSubOrchestrationInstanceID("fixed-child"))
				if err := ctx.SendEvent("other", "hello", "payload"); err != nil {
					return nil, err
				}
				results := make([]string, 2)
				if err := first.Await(&results[0]); err != nil {
					return nil, err
				}
				if err := second.Await(&results[1]); err != nil {
					return nil, err
				}
				return results, nil
			}},
		},
		{
			name:   "external events and event channels",
			output: `["a","b","c","late"]`,
			scenario: sessionScenario{
				extra: map[int][]*protos.HistoryEvent{
					1: {eventRaised("ch", `"a"`)},
					2: {eventRaised("ch", `"b"`), eventRaised("ch", `"c"`), eventRaised("single", `"late"`)},
				},
				orchestrator: func(ctx *OrchestrationContext) (any, error) {
					channel := NewEventChannel[string](ctx, "ch")
					values := []string{}
					for range 3 {
						values = append(values, channel.Receive(ctx))
					}
					var single string
					if err := ctx.WaitForSingleEvent("single", -1).Await(&single); err != nil {
						return nil, err
					}
					return append(values, single), nil
				},
			},
		},
		{
			name:   "Go, wait group, WhenAll and select",
			output: `"left-0|right-1|all-2,all-3|picked:timer"`,
			scenario: sessionScenario{
				perTurn: 1,
				extra:   map[int][]*protos.HistoryEvent{9: {eventRaised("late", `"x"`)}},
				orchestrator: func(ctx *OrchestrationContext) (any, error) {
					results := make([]string, 2)
					wg := ctx.NewWaitGroup()
					wg.Add(2)
					for index, name := range []string{"left", "right"} {
						ctx.Go(func(child *OrchestrationContext) {
							defer wg.Done()
							_ = child.CallActivity(name).Await(&results[index])
						})
					}
					wg.Wait(ctx)
					all := []Task{ctx.CallActivity("all"), ctx.CallActivity("all")}
					if err := ctx.WhenAll(all...); err != nil {
						return nil, err
					}
					allResults := make([]string, 2)
					for i, task := range all {
						_ = task.Await(&allResults[i])
					}
					picked := ""
					channel := NewEventChannel[string](ctx, "late")
					ctx.Select(
						OnTask(ctx.CreateTimer(time.Second), func(Task) { picked = "timer" }),
						OnEvent(channel, func(value string) { picked = "event:" + value }),
					)
					return strings.Join([]string{results[0], results[1], strings.Join(allResults, ","), "picked:" + picked}, "|"), nil
				},
			},
		},
		{
			name:   "cancellation scopes",
			output: `"canceled:after-2"`,
			scenario: sessionScenario{perTurn: 1, orchestrator: func(ctx *OrchestrationContext) (any, error) {
				timer := ctx.CreateTimer(time.Second)
				child, cancel := ctx.WithCancel()
				work := child.CallActivity("slow")
				winner := ctx.WhenAny(timer, work)
				state := "finished"
				if winner == timer {
					cancel()
					if err := work.Await(nil); errors.Is(err, ErrTaskCanceled) {
						state = "canceled"
					}
				}
				var after string
				if err := ctx.CallActivity("after").Await(&after); err != nil {
					return nil, err
				}
				return state + ":" + after, nil
			}},
		},
		{
			name:   "entities and critical sections",
			output: `"get-result|add-result"`,
			scenario: sessionScenario{perTurn: 1, orchestrator: func(ctx *OrchestrationContext) (any, error) {
				if err := ctx.SignalEntity(other, "reset"); err != nil {
					return nil, err
				}
				var first string
				if err := ctx.CallEntity(counter, "get").Await(&first); err != nil {
					return nil, err
				}
				release, err := ctx.LockEntities(counter, other)
				if err != nil {
					return nil, err
				}
				var second string
				if err := ctx.CallEntity(counter, "add", WithEntityInput(1)).Await(&second); err != nil {
					return nil, err
				}
				release()
				return first + "|" + second, nil
			}},
		},
		{
			name:   "canceled lock request after commit",
			output: `"lock canceled"`,
			check: func(t *testing.T, run sessionRun) {
				unlocks := 0
				for _, response := range run.responses {
					for _, action := range response.Actions {
						if action.GetSendEntityMessage().GetEntityUnlockSent() != nil {
							unlocks++
						}
					}
				}
				require.Equal(t, 1, unlocks, "the late grant must be released once")
			},
			scenario: sessionScenario{perTurn: 1, orchestrator: func(ctx *OrchestrationContext) (any, error) {
				timer := ctx.CreateTimer(time.Second)
				child, cancel := ctx.WithCancel()
				lockErr := make([]error, 1)
				done := ctx.NewWaitGroup()
				done.Add(1)
				ctx.Go(func(*OrchestrationContext) {
					defer done.Done()
					_, lockErr[0] = child.LockEntities(counter)
				})
				if err := timer.Await(nil); err != nil {
					return nil, err
				}
				cancel()
				done.Wait(ctx)
				if !errors.Is(lockErr[0], ErrTaskCanceled) {
					return nil, fmt.Errorf("unexpected lock result: %w", lockErr[0])
				}
				// Wait long enough for the late grant to be released.
				if err := ctx.CreateTimer(time.Second).Await(nil); err != nil {
					return nil, err
				}
				return "lock canceled", nil
			}},
		},
		{
			name: "custom status and NewGuid",
			scenario: sessionScenario{orchestrator: func(ctx *OrchestrationContext) (any, error) {
				ids := []string{}
				for range 3 {
					ids = append(ids, ctx.NewGuid())
					ctx.SetCustomStatus(fmt.Sprintf("step-%d", len(ids)))
					if err := ctx.CallActivity("work").Await(nil); err != nil {
						return nil, err
					}
				}
				ids = append(ids, ctx.NewGuid())
				return ids, nil
			}},
		},
		{
			name:   "suspend with actions created before suspension",
			output: `"work-0"`,
			check: func(t *testing.T, run sessionRun) {
				require.Empty(t, run.responses[1].Actions, "a suspended turn sends no actions")
				require.Len(t, run.responses[2].Actions, 1)
				require.Equal(t, "work", run.responses[2].Actions[0].GetScheduleTask().GetName())
			},
			scenario: sessionScenario{
				extra: map[int][]*protos.HistoryEvent{
					1: {eventRaised("go", `""`), suspended()},
					2: {resumed()},
				},
				orchestrator: func(ctx *OrchestrationContext) (any, error) {
					if err := ctx.WaitForSingleEvent("go", -1).Await(nil); err != nil {
						return nil, err
					}
					var result string
					err := ctx.CallActivity("work").Await(&result)
					return result, err
				},
			},
		},
		{
			name:   "events buffered while suspended",
			output: `"payload"`,
			scenario: sessionScenario{
				extra: map[int][]*protos.HistoryEvent{
					1: {suspended(), eventRaised("go", `"payload"`), suspended()},
					2: {resumed()},
				},
				orchestrator: func(ctx *OrchestrationContext) (any, error) {
					var value string
					err := ctx.WaitForSingleEvent("go", -1).Await(&value)
					return value, err
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			run := runSessionEquivalence(t, tt.scenario)
			require.Greater(t, len(run.responses), 2, "scenario should run several turns")
			if tt.output != "" {
				requireCompletedWith(t, run, tt.output)
			} else {
				require.True(t, run.sessionEnded)
				require.NotNil(t, completionOf(run.responses[len(run.responses)-1]))
			}
			if tt.check != nil {
				tt.check(t, run)
			}
		})
	}
}

func TestSessionCustomStatusIsCarriedAcrossTurns(t *testing.T) {
	run := runSessionEquivalence(t, sessionScenario{orchestrator: func(ctx *OrchestrationContext) (any, error) {
		ctx.SetCustomStatus("waiting")
		if err := ctx.CallActivity("a").Await(nil); err != nil {
			return nil, err
		}
		return nil, ctx.CallActivity("b").Await(nil)
	}})
	for _, response := range run.responses {
		require.Equal(t, "waiting", response.GetCustomStatus().GetValue())
	}
}

func TestSessionEndsWithOrchestration(t *testing.T) {
	waitThen := func(end func(ctx *OrchestrationContext) (any, error)) Orchestrator {
		return func(ctx *OrchestrationContext) (any, error) {
			if err := ctx.CallActivity("work").Await(nil); err != nil {
				return nil, err
			}
			return end(ctx)
		}
	}
	tests := []struct {
		name     string
		scenario sessionScenario
		status   protos.OrchestrationStatus
	}{
		{
			name:   "failure",
			status: protos.OrchestrationStatus_ORCHESTRATION_STATUS_FAILED,
			scenario: sessionScenario{orchestrator: waitThen(func(*OrchestrationContext) (any, error) {
				return nil, errors.New("boom")
			})},
		},
		{
			name:   "continue as new",
			status: protos.OrchestrationStatus_ORCHESTRATION_STATUS_CONTINUED_AS_NEW,
			scenario: sessionScenario{
				extra: map[int][]*protos.HistoryEvent{1: {eventRaised("kept", `"x"`)}},
				orchestrator: waitThen(func(ctx *OrchestrationContext) (any, error) {
					ctx.ContinueAsNew("next", WithKeepUnprocessedEvents())
					return nil, nil
				}),
			},
		},
		{
			name:   "termination",
			status: protos.OrchestrationStatus_ORCHESTRATION_STATUS_TERMINATED,
			scenario: sessionScenario{
				extra: map[int][]*protos.HistoryEvent{1: {func() *protos.HistoryEvent {
					event := helpers.NewExecutionTerminatedEvent(wrapperspb.String(`"stop"`), false)
					event.Timestamp = timestamppb.New(sessionTestStart)
					return event
				}()}},
				orchestrator: func(ctx *OrchestrationContext) (any, error) {
					return nil, ctx.WaitForSingleEvent("never", -1).Await(nil)
				},
			},
		},
		{
			name:   "history limit",
			status: protos.OrchestrationStatus_ORCHESTRATION_STATUS_FAILED,
			scenario: sessionScenario{
				options: []TaskExecutorOption{WithOrchestrationOptions(OrchestrationOptions{MaxHistoryEvents: 6})},
				orchestrator: func(ctx *OrchestrationContext) (any, error) {
					for {
						if err := ctx.CallActivity("work").Await(nil); err != nil {
							return nil, err
						}
					}
				},
			},
		},
		{
			name:   "history limit continue as new",
			status: protos.OrchestrationStatus_ORCHESTRATION_STATUS_CONTINUED_AS_NEW,
			scenario: sessionScenario{
				options: []TaskExecutorOption{WithOrchestrationOptions(OrchestrationOptions{
					MaxHistoryEvents: 6,
					OnHistoryLimitExceeded: func(info HistoryLimitInfo) (any, error) {
						return info.HistoryLength, nil
					},
				})},
				orchestrator: func(ctx *OrchestrationContext) (any, error) {
					for {
						if err := ctx.CallActivity("work").Await(nil); err != nil {
							return nil, err
						}
					}
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			run := runSessionEquivalence(t, tt.scenario)
			require.True(t, run.sessionEnded)
			require.Greater(t, len(run.responses), 1)
			completed := completionOf(run.responses[len(run.responses)-1])
			require.NotNil(t, completed)
			require.Equal(t, tt.status, completed.OrchestrationStatus)
		})
	}
}

func TestSessionEndsOnPartialTurn(t *testing.T) {
	run := runSessionEquivalence(t, sessionScenario{
		options: []TaskExecutorOption{WithOrchestrationOptions(OrchestrationOptions{MaxEventsPerTurn: 2})},
		extra: map[int][]*protos.HistoryEvent{
			1: {eventRaised("e", `1`), eventRaised("e", `2`), eventRaised("e", `3`)},
		},
		orchestrator: func(ctx *OrchestrationContext) (any, error) {
			channel := NewEventChannel[int](ctx, "e")
			sum := 0
			for range 3 {
				sum += channel.Receive(ctx)
			}
			return sum, nil
		},
	})
	require.True(t, run.sessionEnded)
	last := run.responses[len(run.responses)-1]
	require.NotNil(t, last.NumEventsProcessed)
	require.Equal(t, int32(2), last.NumEventsProcessed.GetValue())
	require.Nil(t, completionOf(last))
}

func TestSessionEndsOnPanicDuringResume(t *testing.T) {
	registry := NewTaskRegistry()
	require.NoError(t, registry.AddOrchestratorN("session", func(ctx *OrchestrationContext) (any, error) {
		if err := ctx.CallActivity("work").Await(nil); err != nil {
			return nil, err
		}
		panic("resume panic")
	}))
	baseline := runtime.NumGoroutine()
	executor := NewTaskExecutor(registry).(*taskExecutor)
	first := []*protos.HistoryEvent{sessionOrchestratorStarted(sessionTestStart), sessionExecutionStarted("session", nil)}
	result, session, err := executor.StartOrchestratorSession(context.Background(), sessionTestInstance, nil, first, nil)
	require.NoError(t, err)
	require.NotNil(t, session)
	session.Commit(result.Response.Actions)
	batch := []*protos.HistoryEvent{
		sessionOrchestratorStarted(sessionTestStart.Add(time.Minute)),
		defaultResponse(result.Response.Actions[0]),
	}
	result, next, err := session.Resume(context.Background(), batch, len(first)+1)
	require.NoError(t, err)
	require.Nil(t, next)
	completed := completionOf(result.Response)
	require.NotNil(t, completed)
	require.Equal(t, protos.OrchestrationStatus_ORCHESTRATION_STATUS_FAILED, completed.OrchestrationStatus)
	require.Contains(t, completed.GetFailureDetails().GetErrorMessage(), "resume panic")
	requireNoGoroutineLeak(t, baseline)
}

// TestStartOrchestratorSessionMatchesExecuteOrchestrator checks first turns
// that cannot keep a session. They must return the same result or error as
// ExecuteOrchestrator and no session.
func TestStartOrchestratorSessionMatchesExecuteOrchestrator(t *testing.T) {
	started := func(name string, version string) *protos.HistoryEvent {
		event := sessionExecutionStarted(name, nil)
		if version != "" {
			event.GetExecutionStarted().Version = wrapperspb.String(version)
		}
		return event
	}
	tests := []struct {
		name      string
		options   []TaskExecutorOption
		old       []*protos.HistoryEvent
		new       []*protos.HistoryEvent
		params    *protos.OrchestratorEntityParameters
		wantError bool
	}{
		{name: "completes", new: []*protos.HistoryEvent{sessionOrchestratorStarted(sessionTestStart), started("done", "")}},
		{name: "fails", new: []*protos.HistoryEvent{sessionOrchestratorStarted(sessionTestStart), started("fail", "")}},
		{name: "panics", new: []*protos.HistoryEvent{sessionOrchestratorStarted(sessionTestStart), started("panic", "")}},
		{name: "continues as new", new: []*protos.HistoryEvent{sessionOrchestratorStarted(sessionTestStart), started("again", "")}},
		{name: "not registered", new: []*protos.HistoryEvent{sessionOrchestratorStarted(sessionTestStart), started("missing", "")}},
		{
			name: "not registered reject",
			options: []TaskExecutorOption{
				WithOrchestratorNotFoundStrategy(OrchestratorNotFoundReject),
			},
			new:       []*protos.HistoryEvent{sessionOrchestratorStarted(sessionTestStart), started("missing", "")},
			wantError: true,
		},
		{
			name:    "version fail",
			options: []TaskExecutorOption{WithVersioning(VersioningOptions{Version: "2.0", MatchStrategy: VersionMatchStrict, FailureStrategy: VersionFailureFail})},
			new:     []*protos.HistoryEvent{sessionOrchestratorStarted(sessionTestStart), started("wait", "1.0")},
		},
		{
			name:      "version reject",
			options:   []TaskExecutorOption{WithVersioning(VersioningOptions{Version: "2.0", MatchStrategy: VersionMatchStrict, FailureStrategy: VersionFailureReject})},
			new:       []*protos.HistoryEvent{sessionOrchestratorStarted(sessionTestStart), started("wait", "1.0")},
			wantError: true,
		},
		{
			name:      "unsupported entity parameters",
			new:       []*protos.HistoryEvent{sessionOrchestratorStarted(sessionTestStart), started("wait", "")},
			params:    &protos.OrchestratorEntityParameters{},
			wantError: true,
		},
		{
			name: "non-deterministic replay",
			old: []*protos.HistoryEvent{
				sessionOrchestratorStarted(sessionTestStart),
				started("wait", ""),
				helpers.NewTimerCreatedEvent(0, timestamppb.New(sessionTestStart)),
			},
			new: []*protos.HistoryEvent{sessionOrchestratorStarted(sessionTestStart.Add(time.Minute))},
		},
		{
			name: "partial turn",
			options: []TaskExecutorOption{
				WithOrchestrationOptions(OrchestrationOptions{MaxEventsPerTurn: 1}),
			},
			new: []*protos.HistoryEvent{
				sessionOrchestratorStarted(sessionTestStart),
				started("wait", ""),
				eventRaised("x", `1`),
			},
		},
		{
			name: "rewind",
			old: []*protos.HistoryEvent{
				sessionOrchestratorStarted(sessionTestStart),
				started("fail", ""),
				{
					EventId:   -1,
					Timestamp: timestamppb.New(sessionTestStart),
					EventType: &protos.HistoryEvent_ExecutionCompleted{ExecutionCompleted: &protos.ExecutionCompletedEvent{
						OrchestrationStatus: protos.OrchestrationStatus_ORCHESTRATION_STATUS_FAILED,
						FailureDetails:      &protos.TaskFailureDetails{ErrorType: "x", ErrorMessage: "y"},
					}},
				},
			},
			new: []*protos.HistoryEvent{
				sessionOrchestratorStarted(sessionTestStart.Add(time.Minute)),
				{
					EventId:   -1,
					Timestamp: timestamppb.New(sessionTestStart),
					EventType: &protos.HistoryEvent_ExecutionRewound{ExecutionRewound: &protos.ExecutionRewoundEvent{}},
				},
			},
		},
	}
	registry := NewTaskRegistry()
	require.NoError(t, registry.AddOrchestratorN("done", func(*OrchestrationContext) (any, error) { return "ok", nil }))
	require.NoError(t, registry.AddOrchestratorN("fail", func(*OrchestrationContext) (any, error) { return nil, errors.New("failed") }))
	require.NoError(t, registry.AddOrchestratorN("panic", func(*OrchestrationContext) (any, error) { panic("first turn") }))
	require.NoError(t, registry.AddOrchestratorN("again", func(ctx *OrchestrationContext) (any, error) {
		ctx.ContinueAsNew(1)
		return nil, nil
	}))
	require.NoError(t, registry.AddOrchestratorN("wait", func(ctx *OrchestrationContext) (any, error) {
		return nil, ctx.WaitForSingleEvent("never", -1).Await(nil)
	}))
	require.NoError(t, registry.AddOrchestratorNVersion("wait", "1.0", func(ctx *OrchestrationContext) (any, error) {
		return nil, ctx.WaitForSingleEvent("never", -1).Await(nil)
	}))
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			params := tt.params
			if params == nil {
				params = supportedEntityParameters()
			}
			baseline := runtime.NumGoroutine()
			executor := NewTaskExecutor(registry, tt.options...).(*taskExecutor)
			want, wantErr := executor.ExecuteOrchestrator(context.Background(), sessionTestInstance, tt.old, tt.new, params)
			got, session, gotErr := executor.StartOrchestratorSession(context.Background(), sessionTestInstance, tt.old, tt.new, params)
			require.Nil(t, session)
			if tt.wantError {
				require.Error(t, wantErr)
				require.EqualError(t, gotErr, wantErr.Error())
				require.Nil(t, got)
			} else {
				require.NoError(t, wantErr)
				require.NoError(t, gotErr)
				switch tt.name {
				case "panics":
					// Stack traces differ between the two calls.
					require.Equal(t, completionOf(want.Response).OrchestrationStatus, completionOf(got.Response).OrchestrationStatus)
				case "rewind":
					// Rewind generates a new execution ID on every call.
					require.Len(t, got.Response.Actions, len(want.Response.Actions))
				default:
					requireSameResponse(t, 0, want.Response, got.Response)
				}
			}
			requireNoGoroutineLeak(t, baseline)
		})
	}
}

func startWaitingSession(t *testing.T, orchestrator Orchestrator) (*OrchestrationContext, OrchestratorSession, *protos.OrchestratorResponse) {
	t.Helper()
	registry := NewTaskRegistry()
	require.NoError(t, registry.AddOrchestratorN("session", orchestrator))
	executor := NewTaskExecutor(registry).(*taskExecutor)
	result, session, err := executor.StartOrchestratorSession(
		context.Background(),
		sessionTestInstance,
		nil,
		[]*protos.HistoryEvent{sessionOrchestratorStarted(sessionTestStart), sessionExecutionStarted("session", nil)},
		nil,
	)
	require.NoError(t, err)
	require.NotNil(t, session)
	return session.(*orchestratorSession).ctx, session, result.Response
}

func TestSessionStateMachine(t *testing.T) {
	waiting := func(ctx *OrchestrationContext) (any, error) {
		return nil, ctx.CallActivity("work").Await(nil)
	}
	validBatch := []*protos.HistoryEvent{sessionOrchestratorStarted(sessionTestStart.Add(time.Minute))}

	t.Run("resume before commit", func(t *testing.T) {
		_, session, _ := startWaitingSession(t, waiting)
		_, next, err := session.Resume(context.Background(), validBatch, 3)
		require.ErrorContains(t, err, "before the last turn was committed")
		require.Nil(t, next)
		_, _, err = session.Resume(context.Background(), validBatch, 3)
		require.Error(t, err)
	})
	t.Run("commit twice", func(t *testing.T) {
		_, session, response := startWaitingSession(t, waiting)
		session.Commit(response.Actions)
		session.Commit(response.Actions)
		_, _, err := session.Resume(context.Background(), validBatch, 3)
		require.ErrorContains(t, err, "commit is out of order")
	})
	t.Run("resume after close", func(t *testing.T) {
		_, session, response := startWaitingSession(t, waiting)
		session.Commit(response.Actions)
		session.Close()
		session.Close()
		_, _, err := session.Resume(context.Background(), validBatch, 3)
		require.ErrorIs(t, err, errOrchestratorSessionClosed)
	})
	invalid := []struct {
		name    string
		batch   []*protos.HistoryEvent
		base    int
		message string
	}{
		{name: "empty", batch: nil, base: 3, message: "must start with an OrchestratorStarted"},
		{name: "no orchestrator started", batch: []*protos.HistoryEvent{eventRaised("x", `1`)}, base: 3, message: "must start with an OrchestratorStarted"},
		{name: "negative base", batch: validBatch, base: -1, message: "must not be negative"},
		{name: "new execution", batch: []*protos.HistoryEvent{validBatch[0], sessionExecutionStarted("session", nil)}, base: 3, message: "must not start a new execution"},
		{name: "rewind", batch: []*protos.HistoryEvent{validBatch[0], {EventType: &protos.HistoryEvent_ExecutionRewound{ExecutionRewound: &protos.ExecutionRewoundEvent{}}}}, base: 3, message: "rewind"},
		{name: "nil event", batch: []*protos.HistoryEvent{validBatch[0], nil}, base: 3, message: "nil events"},
	}
	for _, tt := range invalid {
		t.Run("invalid batch "+tt.name, func(t *testing.T) {
			ctx, session, response := startWaitingSession(t, waiting)
			session.Commit(response.Actions)
			_, next, err := session.Resume(context.Background(), tt.batch, tt.base)
			require.ErrorContains(t, err, tt.message)
			require.Nil(t, next)
			require.Nil(t, ctx.scheduler, "an invalid batch must unload the session")
			_, _, err = session.Resume(context.Background(), validBatch, 3)
			require.Error(t, err)
		})
	}
}

func TestSessionCommitRetiresOnlySentActions(t *testing.T) {
	ctx, session, response := startWaitingSession(t, func(ctx *OrchestrationContext) (any, error) {
		return nil, ctx.WhenAll(ctx.CallActivity("a"), ctx.CallActivity("b"))
	})
	require.Len(t, response.Actions, 2)
	session.Commit(response.Actions[:1])
	require.Len(t, ctx.pendingActions, 1)
	result, next, err := session.Resume(context.Background(), []*protos.HistoryEvent{
		sessionOrchestratorStarted(sessionTestStart.Add(time.Minute)),
	}, 4)
	require.NoError(t, err)
	require.NotNil(t, next)
	require.Len(t, result.Response.Actions, 1)
	require.True(t, proto.Equal(response.Actions[1], result.Response.Actions[0]), "an unsent action must be sent again")
	next.Close()
}

func TestSessionHistoryLengthUsesHistoryBase(t *testing.T) {
	var lengths []int
	registry := NewTaskRegistry()
	require.NoError(t, registry.AddOrchestratorN("session", func(ctx *OrchestrationContext) (any, error) {
		for {
			if err := ctx.WaitForSingleEvent("e", -1).Await(nil); err != nil {
				return nil, err
			}
		}
	}))
	executor := newSessionTestExecutor(registry, &lengths,
		WithOrchestrationOptions(OrchestrationOptions{MaxHistoryEvents: 100}))
	result, session, err := executor.StartOrchestratorSession(context.Background(), sessionTestInstance, nil,
		[]*protos.HistoryEvent{sessionOrchestratorStarted(sessionTestStart), sessionExecutionStarted("session", nil)}, nil)
	require.NoError(t, err)
	require.Equal(t, []int{2}, lengths)
	session.Commit(result.Response.Actions)

	batch := []*protos.HistoryEvent{sessionOrchestratorStarted(sessionTestStart.Add(time.Minute)), eventRaised("e", `1`)}
	result, session, err = session.Resume(context.Background(), batch, 98)
	require.NoError(t, err)
	require.NotNil(t, session)
	require.Equal(t, []int{2, 100}, lengths)
	session.Commit(result.Response.Actions)

	result, session, err = session.Resume(context.Background(), batch, 99)
	require.NoError(t, err)
	require.Nil(t, session, "the history limit must end the session")
	completed := completionOf(result.Response)
	require.NotNil(t, completed)
	require.Equal(t, protos.OrchestrationStatus_ORCHESTRATION_STATUS_FAILED, completed.OrchestrationStatus)
	require.Equal(t, []int{2, 100, 101}, lengths)
}

func TestSessionDropsProcessedHistoryBetweenTurns(t *testing.T) {
	ctx, session, response := startWaitingSession(t, func(ctx *OrchestrationContext) (any, error) {
		var value string
		if err := ctx.WaitForSingleEvent("go", -1).Await(&value); err != nil {
			return nil, err
		}
		return value, ctx.CallActivity("work").Await(nil)
	})
	require.Nil(t, ctx.oldEvents)
	require.Nil(t, ctx.newEvents)
	require.Equal(t, 2, ctx.historyBase)
	session.Commit(response.Actions)

	// The event is buffered while suspended, and it must survive the cleanup.
	batch := []*protos.HistoryEvent{
		sessionOrchestratorStarted(sessionTestStart.Add(time.Minute)),
		suspended(),
		eventRaised("go", `"payload"`),
	}
	result, next, err := session.Resume(context.Background(), batch, 2)
	require.NoError(t, err)
	require.NotNil(t, next)
	require.Empty(t, result.Response.Actions)
	require.Nil(t, ctx.newEvents)
	require.Len(t, ctx.suspendedEvents, 1)
	next.Commit(result.Response.Actions)

	result, next, err = next.Resume(context.Background(),
		[]*protos.HistoryEvent{sessionOrchestratorStarted(sessionTestStart.Add(2 * time.Minute)), resumed()}, 5)
	require.NoError(t, err)
	require.NotNil(t, next)
	require.Len(t, result.Response.Actions, 1)
	require.Equal(t, "work", result.Response.Actions[0].GetScheduleTask().GetName())
	require.Nil(t, ctx.newEvents)
	next.Close()
}

func TestSessionCloseEmitsNoDurableWork(t *testing.T) {
	baseline := runtime.NumGoroutine()
	deferredBlocked := 0
	ctx, session, response := startWaitingSession(t, func(ctx *OrchestrationContext) (any, error) {
		for range 3 {
			ctx.Go(func(child *OrchestrationContext) {
				defer func() {
					if recovered := recover(); recovered != nil {
						if errors.Is(recovered.(error), ErrTaskBlocked) {
							deferredBlocked++
						}
						panic(recovered)
					}
				}()
				defer func() {
					_ = child.CallActivity("cleanup")
				}()
				_ = child.CreateTimer(time.Hour).Await(nil)
			})
		}
		defer func() {
			ctx.SetCustomStatus("changed")
			_ = ctx.SignalEntity(api.NewEntityID("counter", "x"), "op")
		}()
		return nil, ctx.WaitForSingleEvent("never", -1).Await(nil)
	})
	require.Len(t, response.Actions, 3)
	session.Commit(response.Actions)
	sequence := ctx.sequenceNumber
	status := ctx.customStatus
	require.Greater(t, runtime.NumGoroutine(), baseline)

	session.Close()
	require.Equal(t, sequence, ctx.sequenceNumber, "Close must not schedule durable work")
	require.Empty(t, ctx.pendingActions)
	require.Equal(t, status, ctx.customStatus)
	require.Equal(t, 3, deferredBlocked)
	requireNoGoroutineLeak(t, baseline)
}

func TestSessionDropsFinishedCoroutinesBetweenTurns(t *testing.T) {
	ctx, session, response := startWaitingSession(t, func(ctx *OrchestrationContext) (any, error) {
		childDone := false
		ctx.Go(func(child *OrchestrationContext) {
			if child.WaitForSingleEvent("child", -1).Await(nil) == nil {
				childDone = true
			}
		})
		for range 3 {
			if err := ctx.WaitForSingleEvent("e", -1).Await(nil); err != nil {
				return nil, err
			}
			ctx.Go(func(*OrchestrationContext) {})
		}
		if err := ctx.WaitForSingleEvent("end", -1).Await(nil); err != nil {
			return nil, err
		}
		return childDone, nil
	})
	live := func(want int) {
		t.Helper()
		require.Len(t, ctx.scheduler.all, want)
		for _, c := range ctx.scheduler.all {
			if c.scope != nil {
				for _, scoped := range c.scope.coroutines {
					require.NotEqual(t, coroutineCompleted, scoped.state, "a finished coroutine is still in its scope")
				}
			}
		}
	}
	live(2)
	actions := response.Actions
	historyBase := 2
	turn := func(event string) (*ExecutionResults, OrchestratorSession) {
		t.Helper()
		session.Commit(actions)
		historyBase += 2
		result, next, err := session.Resume(context.Background(), []*protos.HistoryEvent{
			sessionOrchestratorStarted(sessionTestStart.Add(time.Duration(historyBase) * time.Minute)),
			eventRaised(event, `1`),
		}, historyBase)
		require.NoError(t, err)
		actions = result.Response.Actions
		return result, next
	}
	for range 3 {
		_, next := turn("e")
		require.NotNil(t, next)
		live(2)
	}
	_, next := turn("child")
	require.NotNil(t, next)
	live(1)
	result, next := turn("end")
	require.Nil(t, next)
	requireCompletedWith(t, sessionRun{responses: []*protos.OrchestratorResponse{result.Response}, sessionEnded: true}, `true`)
}

// A full replay reads HistoryLength again with the latest turn's value. The
// session must end once orchestrator code reads it, or a later turn could
// decide differently from the replay.
func TestSessionEndsAfterTurnHistoryRead(t *testing.T) {
	for _, tt := range []struct {
		name string
		read func(*OrchestrationContext) bool
	}{
		{name: "HistoryLength", read: func(ctx *OrchestrationContext) bool { return ctx.HistoryLength() > 9 }},
		{name: "HistoryLimitExceeded", read: func(ctx *OrchestrationContext) bool { return ctx.HistoryLimitExceeded() }},
	} {
		t.Run(tt.name, func(t *testing.T) {
			run := runSessionEquivalence(t, sessionScenario{orchestrator: func(ctx *OrchestrationContext) (any, error) {
				for i := 0; ; i++ {
					// The first reads happen on turn 2, so the session must
					// survive the earlier turns.
					if i >= 2 && tt.read(ctx) {
						return i, nil
					}
					if i == 6 {
						return i, nil
					}
					if err := ctx.CallActivity("work").Await(nil); err != nil {
						return nil, err
					}
				}
			}})
			require.True(t, run.sessionEnded)
			require.Len(t, run.responses, 3, "the session must end on the turn that read the value")
			require.Nil(t, completionOf(run.responses[2]))
		})
	}
}
