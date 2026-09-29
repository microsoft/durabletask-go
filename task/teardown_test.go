package task

import (
	"bytes"
	"errors"
	"log/slog"
	"maps"
	"slices"
	"testing"
	"time"

	"github.com/microsoft/durabletask-go/api"
	"github.com/microsoft/durabletask-go/internal/helpers"
	"github.com/microsoft/durabletask-go/internal/protos"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

var teardownClock = time.Date(2026, 9, 28, 0, 0, 0, 0, time.UTC)

func teardownStarted() []*protos.HistoryEvent {
	started := helpers.NewOrchestratorStartedEvent()
	started.Timestamp = timestamppb.New(teardownClock)
	execution := helpers.NewExecutionStartedEvent("teardown", "teardown-instance", nil, nil, nil, nil)
	execution.Timestamp = started.Timestamp
	execution.GetExecutionStarted().OrchestrationInstance.ExecutionId = wrapperspb.String("teardown-execution")
	return []*protos.HistoryEvent{started, execution}
}

func teardownRegistry(t *testing.T, fn Orchestrator) *TaskRegistry {
	t.Helper()
	registry := NewTaskRegistry()
	require.NoError(t, registry.AddOrchestratorN("teardown", fn))
	return registry
}

func TestTeardownTerminalPaths(t *testing.T) {
	for _, outcome := range []string{"completed", "continued", "terminated", "error", "panic", "child-panic"} {
		t.Run(outcome, func(t *testing.T) {
			deferred := 0
			registry := teardownRegistry(t, func(ctx *OrchestrationContext) (any, error) {
				ctx.SetCustomStatus("live")
				ctx.Go(func(child *OrchestrationContext) {
					defer child.SetCustomStatus("teardown")
					defer func() {
						deferred++
						_ = child.CallActivity("cleanup").Await(nil)
					}()
					_ = child.WaitForSingleEvent("child-signal", -1).Await(nil)
				})
				if outcome == "child-panic" {
					ctx.Go(func(child *OrchestrationContext) {
						_ = child.WaitForSingleEvent("finish", -1).Await(nil)
						panic("child failure")
					})
					return nil, ctx.WaitForSingleEvent("never", -1).Await(nil)
				}
				if err := ctx.WaitForSingleEvent("finish", -1).Await(nil); err != nil {
					return nil, err
				}
				switch outcome {
				case "continued":
					ctx.ContinueAsNew("next", WithKeepUnprocessedEvents())
				case "error":
					return nil, errors.New("root failure")
				case "panic":
					panic("root failure")
				}
				return "done", nil
			})
			trigger := helpers.NewEventRaisedEvent("finish", nil)
			expected := api.RUNTIME_STATUS_COMPLETED
			switch outcome {
			case "continued":
				expected = api.RUNTIME_STATUS_CONTINUED_AS_NEW
			case "terminated":
				trigger = helpers.NewExecutionTerminatedEvent(nil, false)
				expected = api.RUNTIME_STATUS_TERMINATED
			case "error", "panic", "child-panic":
				expected = api.RUNTIME_STATUS_FAILED
			}
			response := executeOrchestrationTurn(t, registry, "teardown-instance", nil, append(teardownStarted(), trigger))
			require.Equal(t, 1, deferred)
			require.Len(t, response.Actions, 1, "teardown must not append actions: %v", response.Actions)
			require.Equal(t, expected, completionAction(t, response).OrchestrationStatus)
			require.Equal(t, "live", response.GetCustomStatus().GetValue())
		})
	}
}

func TestTeardownUnloadPreservesStatusAndTurnPolicy(t *testing.T) {
	for _, mode := range []string{"ordinary", "partial", "suspended", "history-continue", "history-failure"} {
		t.Run(mode, func(t *testing.T) {
			registry := teardownRegistry(t, func(ctx *OrchestrationContext) (any, error) {
				ctx.SetCustomStatus("waiting")
				defer ctx.SetCustomStatus("finished")
				return nil, ctx.WaitForSingleEvent("finish", -1).Await(nil)
			})
			events := teardownStarted()
			var options OrchestrationOptions
			switch mode {
			case "partial":
				options.MaxEventsPerTurn = 1
				events = append(events, helpers.NewEventRaisedEvent("extra", nil))
			case "suspended":
				events = append(events, helpers.NewSuspendOrchestrationEvent("pause"))
			case "history-continue", "history-failure":
				options.MaxHistoryEvents = 3
				events = append(events, events[0], events[0])
				if mode == "history-continue" {
					options.OnHistoryLimitExceeded = func(HistoryLimitInfo) (any, error) { return "next", nil }
				}
			}
			response := executeOrchestrationTurn(t, registry, "teardown-instance", nil, events, WithOrchestrationOptions(options))
			require.Equal(t, "waiting", response.GetCustomStatus().GetValue())
			switch mode {
			case "history-continue":
				require.Equal(t, api.RUNTIME_STATUS_CONTINUED_AS_NEW, completionAction(t, response).OrchestrationStatus)
			case "history-failure":
				require.Equal(t, api.RUNTIME_STATUS_FAILED, completionAction(t, response).OrchestrationStatus)
			default:
				require.Empty(t, response.Actions)
			}
			if mode == "partial" {
				require.NotNil(t, response.NumEventsProcessed)
				require.EqualValues(t, 1, response.NumEventsProcessed.Value)
			}
		})
	}
}

type teardownHandles struct {
	pending, complete Task
	channel           *EventChannel[int]
	group, emptyGroup WaitGroup
	value, handlers   int
}

type teardownConverter struct {
	serializations, deserializations int
}

func (c *teardownConverter) Serialize(value any) (string, error) {
	c.serializations++
	return api.DefaultDataConverter().Serialize(value)
}

func (c *teardownConverter) Deserialize(value string, target any) error {
	c.deserializations++
	return api.DefaultDataConverter().Deserialize(value, target)
}

type teardownState struct {
	sequence              int32
	guids                 uint64
	actions, tasks        []int32
	entityTasks, channels []string
	waits, eventWaiters   map[string]int
	buffered              map[string][]string
	continued, keep       bool
	input                 any
	version, status       string
}

func teardownSnapshot(ctx *OrchestrationContext) teardownState {
	state := teardownState{
		sequence: ctx.sequenceNumber, guids: ctx.newGuidCounter,
		waits: make(map[string]int), eventWaiters: make(map[string]int), buffered: make(map[string][]string),
		continued: ctx.continuedAsNew, keep: ctx.saveBufferedExternalEvents, input: ctx.continuedAsNewInput,
		version: ctx.continuedAsNewVersion.GetValue(), status: ctx.customStatus,
	}
	state.actions = slices.Sorted(maps.Keys(ctx.pendingActions))
	state.tasks = slices.Sorted(maps.Keys(ctx.pendingTasks))
	state.entityTasks = slices.Sorted(maps.Keys(ctx.pendingEntityTasks))
	state.channels = slices.Sorted(maps.Keys(ctx.eventChannels))
	for name, list := range ctx.pendingExternalEventTasks {
		state.waits[name] = list.Len()
	}
	for name, waiters := range ctx.eventWaiters {
		state.eventWaiters[name] = len(waiters)
	}
	for name, list := range ctx.bufferedExternalEvents {
		for item := list.Front(); item != nil; item = item.Next() {
			state.buffered[name] = append(state.buffered[name], item.Value.(*bufferedEvent).event.GetEventRaised().GetInput().GetValue())
		}
	}
	return state
}

func TestTeardownAPIsDoNotMutateState(t *testing.T) {
	entity := api.NewEntityID("counter", "teardown")
	cases := []struct {
		name string
		call func(*OrchestrationContext, *teardownHandles)
	}{
		{"activity", func(ctx *OrchestrationContext, _ *teardownHandles) {
			ctx.CallActivity("cleanup", WithActivityInput("payload"))
		}},
		{"activity-retry", func(ctx *OrchestrationContext, _ *teardownHandles) {
			ctx.CallActivity("cleanup", WithActivityRetryPolicy(&RetryPolicy{MaxAttempts: 2, InitialRetryInterval: time.Second}))
		}},
		{"sub-orchestration", func(ctx *OrchestrationContext, _ *teardownHandles) {
			ctx.CallSubOrchestrator("cleanup", WithSubOrchestratorInput("payload"))
		}},
		{"timer", func(ctx *OrchestrationContext, _ *teardownHandles) { ctx.CreateTimer(time.Hour) }},
		{"event-wait", func(ctx *OrchestrationContext, _ *teardownHandles) { ctx.WaitForSingleEvent("new-wait", -1) }},
		{"event-timeout", func(ctx *OrchestrationContext, _ *teardownHandles) { ctx.WaitForSingleEvent("new-wait", time.Hour) }},
		{"event-ready", func(ctx *OrchestrationContext, _ *teardownHandles) { ctx.WaitForSingleEvent("buffered", 0) }},
		{"channel-new", func(ctx *OrchestrationContext, _ *teardownHandles) { NewEventChannel[int](ctx, "new-channel") }},
		{"channel-cached", func(ctx *OrchestrationContext, _ *teardownHandles) { NewEventChannel[int](ctx, "buffered") }},
		{"channel-try", func(_ *OrchestrationContext, h *teardownHandles) { h.channel.TryReceive() }},
		{"channel-receive", func(ctx *OrchestrationContext, h *teardownHandles) { _, _ = h.channel.ReceiveErr(ctx) }},
		{"task-pending", func(_ *OrchestrationContext, h *teardownHandles) { _ = h.pending.Await(nil) }},
		{"task-completed", func(_ *OrchestrationContext, h *teardownHandles) { _ = h.complete.Await(&h.value) }},
		{"select", func(ctx *OrchestrationContext, h *teardownHandles) {
			ctx.Select(OnTask(h.complete, func(Task) { h.handlers++ }))
		}},
		{"when-any", func(ctx *OrchestrationContext, h *teardownHandles) { ctx.WhenAny(h.complete) }},
		{"when-all", func(ctx *OrchestrationContext, h *teardownHandles) { _ = ctx.WhenAll(h.complete) }},
		{"group-wait", func(ctx *OrchestrationContext, h *teardownHandles) { h.group.Wait(ctx) }},
		{"group-ready", func(ctx *OrchestrationContext, h *teardownHandles) { h.emptyGroup.Wait(ctx) }},
		{"entity-call", func(ctx *OrchestrationContext, _ *teardownHandles) { ctx.CallEntity(entity, "cleanup") }},
		{"entity-signal", func(ctx *OrchestrationContext, _ *teardownHandles) { _ = ctx.SignalEntity(entity, "cleanup") }},
		{"entity-lock", func(ctx *OrchestrationContext, _ *teardownHandles) { _, _ = ctx.LockEntities(entity) }},
		{"continue", func(ctx *OrchestrationContext, _ *teardownHandles) {
			ctx.ContinueAsNew("next", WithKeepUnprocessedEvents())
		}},
		{"keep-option", func(ctx *OrchestrationContext, _ *teardownHandles) { WithKeepUnprocessedEvents()(ctx) }},
		{"version-option", func(ctx *OrchestrationContext, _ *teardownHandles) { WithContinueAsNewVersion("next")(ctx) }},
		{"send-event", func(ctx *OrchestrationContext, _ *teardownHandles) { _ = ctx.SendEvent("target", "cleanup", "payload") }},
		{"status", func(ctx *OrchestrationContext, _ *teardownHandles) { ctx.SetCustomStatus("finished") }},
		{"raw-status", func(ctx *OrchestrationContext, _ *teardownHandles) { ctx.SetRawCustomStatus("finished") }},
		{"typed-status", func(ctx *OrchestrationContext, _ *teardownHandles) { _ = ctx.SetCustomStatusValue("finished") }},
		{"guid", func(ctx *OrchestrationContext, _ *teardownHandles) { ctx.NewGuid() }},
		{"scope", func(ctx *OrchestrationContext, _ *teardownHandles) { ctx.WithCancel() }},
		{"task-factory", func(ctx *OrchestrationContext, _ *teardownHandles) { newTaskInScope(ctx, ctx.scope) }},
		{"sequence", func(ctx *OrchestrationContext, _ *teardownHandles) { ctx.getNextSequenceNumber() }},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			var before, after teardownState
			var recovered any
			returned, observed := false, false
			converter := new(teardownConverter)
			handles := new(teardownHandles)
			registry := teardownRegistry(t, func(ctx *OrchestrationContext) (any, error) {
				ctx.SetCustomStatus("live")
				handles.channel = NewEventChannel[int](ctx, "buffered")
				handles.pending = ctx.WaitForSingleEvent("pending", -1)
				complete := newTaskInScope(ctx, ctx.scope)
				complete.complete([]byte("42"))
				handles.complete = complete
				handles.group, handles.emptyGroup = ctx.NewWaitGroup(), ctx.NewWaitGroup()
				handles.group.Add(1)
				defer func() {
					before = teardownSnapshot(ctx)
					defer func() {
						recovered = recover()
						after = teardownSnapshot(ctx)
						observed = true
						if recovered != nil {
							panic(recovered)
						}
					}()
					test.call(ctx, handles)
					returned = true
				}()
				return nil, ctx.WaitForSingleEvent("never", -1).Await(nil)
			})
			events := append(teardownStarted(), helpers.NewEventRaisedEvent("buffered", wrapperspb.String("7")))
			response := executeOrchestrationTurn(t, registry, "teardown-instance", nil, events, WithDataConverter(converter))
			require.True(t, observed)
			require.False(t, returned)
			require.Equal(t, ErrTaskBlocked, recovered)
			require.Equal(t, before, after)
			require.Zero(t, handles.value)
			require.Zero(t, handles.handlers)
			require.Zero(t, converter.serializations)
			require.Zero(t, converter.deserializations)
			require.Empty(t, response.Actions)
			require.Equal(t, "live", response.GetCustomStatus().GetValue())
		})
	}
}

func TestTeardownAllowsLocalCoordination(t *testing.T) {
	finished := false
	registry := teardownRegistry(t, func(ctx *OrchestrationContext) (any, error) {
		group := ctx.NewWaitGroup()
		group.Add(1)
		defer func() {
			group.Done()
			group.Add(1)
			group.Done()
			finished = ctx.WhenAll() == nil
		}()
		return nil, ctx.WaitForSingleEvent("never", -1).Await(nil)
	})
	response := executeOrchestrationTurn(t, registry, "teardown-instance", nil, teardownStarted())
	require.True(t, finished, "local coordination must not interrupt the rest of a defer")
	require.Empty(t, response.Actions)
}

func TestTeardownCannotConsumeContinueAsNewCarryover(t *testing.T) {
	registry := teardownRegistry(t, func(ctx *OrchestrationContext) (any, error) {
		channel := NewEventChannel[int](ctx, "carry")
		ctx.Go(func(child *OrchestrationContext) {
			defer func() { channel.TryReceive() }()
			_ = child.WaitForSingleEvent("child-signal", -1).Await(nil)
		})
		if err := ctx.WaitForSingleEvent("finish", -1).Await(nil); err != nil {
			return nil, err
		}
		ctx.ContinueAsNew("next", WithKeepUnprocessedEvents())
		return nil, nil
	})
	events := append(teardownStarted(),
		helpers.NewEventRaisedEvent("carry", wrapperspb.String("7")),
		helpers.NewEventRaisedEvent("finish", nil))
	response := executeOrchestrationTurn(t, registry, "teardown-instance", nil, events)
	require.Len(t, response.Actions, 1)
	completed := completionAction(t, response)
	require.Equal(t, api.RUNTIME_STATUS_CONTINUED_AS_NEW, completed.OrchestrationStatus)
	require.Len(t, completed.CarryoverEvents, 1)
	require.Equal(t, "7", completed.CarryoverEvents[0].GetEventRaised().GetInput().GetValue())
}

func TestTeardownInsideDeferredAwaitPreservesNormalReplay(t *testing.T) {
	registry := teardownRegistry(t, func(ctx *OrchestrationContext) (any, error) {
		defer func() { _ = ctx.CallActivity("cleanup-A").Await(nil) }()
		defer func() { _ = ctx.CallActivity("cleanup-B").Await(nil) }()
		return "done", nil
	})
	start := teardownStarted()
	first := executeOrchestrationTurn(t, registry, "teardown-instance", nil, start)
	require.Len(t, first.Actions, 1)
	require.Equal(t, "cleanup-B", first.Actions[0].GetScheduleTask().GetName())

	old := append(slices.Clone(start), helpers.NewTaskScheduledEvent(0, "cleanup-B", nil, nil, nil))
	terminated := executeOrchestrationTurn(t, registry, "teardown-instance", old, []*protos.HistoryEvent{helpers.NewExecutionTerminatedEvent(nil, false)})
	require.Len(t, terminated.Actions, 1, "remaining defer A must not schedule on termination")
	require.Equal(t, api.RUNTIME_STATUS_TERMINATED, completionAction(t, terminated).OrchestrationStatus)

	completedB := helpers.NewTaskCompletedEvent(0, nil)
	second := executeOrchestrationTurn(t, registry, "teardown-instance", old, []*protos.HistoryEvent{completedB})
	require.Len(t, second.Actions, 1)
	require.Equal(t, "cleanup-A", second.Actions[0].GetScheduleTask().GetName())
	old = append(old, completedB, helpers.NewTaskScheduledEvent(1, "cleanup-A", nil, nil, nil))
	third := executeOrchestrationTurn(t, registry, "teardown-instance", old, []*protos.HistoryEvent{helpers.NewTaskCompletedEvent(1, nil)})
	require.Len(t, third.Actions, 1)
	require.Equal(t, api.RUNTIME_STATUS_COMPLETED, completionAction(t, third).OrchestrationStatus)
}

func TestTeardownNormalDefersRemainAwaitable(t *testing.T) {
	for _, mode := range []string{"return", "error", "panic", "joined-child"} {
		t.Run(mode, func(t *testing.T) {
			registry := teardownRegistry(t, func(ctx *OrchestrationContext) (any, error) {
				if mode == "joined-child" {
					group := ctx.NewWaitGroup()
					group.Add(1)
					ctx.Go(func(child *OrchestrationContext) {
						defer group.Done()
						defer func() { _ = child.CallActivity("cleanup").Await(nil) }()
					})
					group.Wait(ctx)
					return "done", nil
				}
				defer func() { _ = ctx.CallActivity("cleanup").Await(nil) }()
				if mode == "error" {
					return nil, errors.New("root failure")
				}
				if mode == "panic" {
					panic("root failure")
				}
				return "done", nil
			})
			start := teardownStarted()
			first := executeOrchestrationTurn(t, registry, "teardown-instance", nil, start)
			require.Len(t, first.Actions, 1)
			require.Equal(t, "cleanup", first.Actions[0].GetScheduleTask().GetName())
			old := append(slices.Clone(start), helpers.NewTaskScheduledEvent(0, "cleanup", nil, nil, nil))
			second := executeOrchestrationTurn(t, registry, "teardown-instance", old, []*protos.HistoryEvent{helpers.NewTaskCompletedEvent(0, nil)})
			require.Len(t, second.Actions, 1)
			expected := api.RUNTIME_STATUS_COMPLETED
			if mode == "error" || mode == "panic" {
				expected = api.RUNTIME_STATUS_FAILED
			}
			require.Equal(t, expected, completionAction(t, second).OrchestrationStatus)
		})
	}
}

func TestTeardownStopsLateTimerChunks(t *testing.T) {
	registry := teardownRegistry(t, func(ctx *OrchestrationContext) (any, error) {
		ctx.CreateTimer(10 * 24 * time.Hour)
		return "done", ctx.WaitForSingleEvent("finish", -1).Await(nil)
	})
	first := executeOrchestrationTurn(t, registry, "teardown-instance", nil, teardownStarted())
	require.Len(t, first.Actions, 1)
	fireAt := first.Actions[0].GetCreateTimer().FireAt
	old := append(teardownStarted(), helpers.NewTimerCreatedEvent(0, fireAt))
	fired := helpers.NewTimerFiredEvent(0, fireAt, nil)

	active := executeOrchestrationTurn(t, registry, "teardown-instance", old, []*protos.HistoryEvent{fired})
	require.Len(t, active.Actions, 1)
	require.NotNil(t, active.Actions[0].GetCreateTimer())

	terminal := executeOrchestrationTurn(t, registry, "teardown-instance", old, []*protos.HistoryEvent{
		helpers.NewEventRaisedEvent("finish", nil), fired,
	})
	require.Len(t, terminal.Actions, 1)
	require.Equal(t, api.RUNTIME_STATUS_COMPLETED, completionAction(t, terminal).OrchestrationStatus)
}

func TestTeardownLoggerSuppressesUnwindNotLiveOrHostLogs(t *testing.T) {
	var output bytes.Buffer
	var root *OrchestrationContext
	var replaying bool
	registry := teardownRegistry(t, func(ctx *OrchestrationContext) (any, error) {
		root = ctx
		child, cancel := ctx.WithCancel()
		cached := child.Logger().With("scope", "child")
		cancel()
		ctx.Logger().Info("live-message")
		defer func() {
			replaying = ctx.IsReplaying
			ctx.Logger().Info("unwind-message")
			child.Logger().Info("unwind-child")
			cached.Info("unwind-cached")
		}()
		return nil, ctx.WaitForSingleEvent("never", -1).Await(nil)
	})
	executeOrchestrationTurn(t, registry, "teardown-instance", nil, append(teardownStarted(), helpers.NewEventRaisedEvent("other", nil)),
		WithLogger(slog.New(slog.NewTextHandler(&output, nil))),
		WithMetricsHooks(MetricsHooks{History: func(HistoryMetric) { root.Logger().Info("host-message") }}))
	require.False(t, replaying, "forced unload must not change IsReplaying")
	require.Contains(t, output.String(), "live-message")
	require.Contains(t, output.String(), "host-message")
	require.NotContains(t, output.String(), "unwind-")
}

func TestTeardownGuardUsesRootOfCanceledContexts(t *testing.T) {
	localDefers := 0
	var captured *OrchestrationContext
	registry := teardownRegistry(t, func(ctx *OrchestrationContext) (any, error) {
		captured = ctx
		child, cancel := ctx.WithCancel()
		cancel()
		defer func() { localDefers++ }()
		defer func() {
			child.SetCustomStatus("finished")
			localDefers += 100
		}()
		ctx.SetCustomStatus("live")
		return nil, ctx.WaitForSingleEvent("never", -1).Await(nil)
	})
	response := executeOrchestrationTurn(t, registry, "teardown-instance", nil, append(teardownStarted(), helpers.NewEventRaisedEvent("other", nil)))
	require.Equal(t, 1, localDefers)
	require.Empty(t, captured.derived, "canceled child should have been pruned")
	require.Equal(t, "live", response.GetCustomStatus().GetValue())
}

func TestTeardownStillAllowsLaterEngineTermination(t *testing.T) {
	registry := teardownRegistry(t, func(ctx *OrchestrationContext) (any, error) {
		ctx.Go(func(child *OrchestrationContext) {
			defer child.SetCustomStatus("teardown")
			_ = child.WaitForSingleEvent("never", -1).Await(nil)
		})
		return "done", ctx.WaitForSingleEvent("finish", -1).Await(nil)
	})
	response := executeOrchestrationTurn(t, registry, "teardown-instance", nil, append(teardownStarted(),
		helpers.NewEventRaisedEvent("finish", nil),
		helpers.NewExecutionTerminatedEvent(nil, false)))
	require.Len(t, response.Actions, 1)
	require.Equal(t, api.RUNTIME_STATUS_TERMINATED, completionAction(t, response).OrchestrationStatus)
	require.Empty(t, response.GetCustomStatus().GetValue())
}

func teardownCancelableBody(body func() error) (err error) {
	defer func() {
		value := recover()
		if value == ErrTaskCanceled { //nolint:errorlint // Do not swallow wrapped or joined application errors.
			err = ErrTaskCanceled
		} else if value != nil {
			panic(value)
		}
	}()
	return body()
}

func TestTeardownExplicitCleanupJoinsBeforeCompletion(t *testing.T) {
	for _, mode := range []string{"await", "select", "cancel-before-start", "cleanup-fails"} {
		t.Run(mode, func(t *testing.T) {
			registry := teardownRegistry(t, func(root *OrchestrationContext) (any, error) {
				work, cancel := root.WithCancel()
				group := root.NewWaitGroup()
				var childErr error
				group.Add(1)
				root.Go(func(cleanup *OrchestrationContext) {
					defer group.Done()
					defer func() {
						childErr = errors.Join(childErr, cleanup.CallActivity("cleanup").Await(nil))
					}()
					childErr = teardownCancelableBody(func() error {
						if mode == "select" {
							work.Select(OnEvent(NewEventChannel[string](work, "work"), nil))
							return nil
						}
						return work.WaitForSingleEvent("work", -1).Await(nil)
					})
					if childErr == ErrTaskCanceled { //nolint:errorlint // Only the body's bare cancellation signal is expected.
						childErr = nil
					}
				})
				if mode != "cancel-before-start" {
					if err := root.WaitForSingleEvent("stop", -1).Await(nil); err != nil {
						return nil, err
					}
				}
				cancel()
				group.Wait(root)
				return "joined", childErr
			})
			history := teardownStarted()
			if mode != "cancel-before-start" {
				history = append(history, helpers.NewEventRaisedEvent("stop", nil))
			}
			first := executeOrchestrationTurn(t, registry, "teardown-instance", nil, history)
			require.Len(t, first.Actions, 1)
			require.Equal(t, "cleanup", first.Actions[0].GetScheduleTask().GetName())
			history = append(history, helpers.NewTaskScheduledEvent(0, "cleanup", nil, nil, nil))
			cleanupResult := helpers.NewTaskCompletedEvent(0, nil)
			expected := api.RUNTIME_STATUS_COMPLETED
			if mode == "cleanup-fails" {
				cleanupResult = helpers.NewTaskFailedEvent(0, &protos.TaskFailureDetails{
					ErrorType: "CleanupFailure", ErrorMessage: "cleanup failed",
				})
				expected = api.RUNTIME_STATUS_FAILED
			}
			second := executeOrchestrationTurn(t, registry, "teardown-instance", history, []*protos.HistoryEvent{cleanupResult})
			require.Len(t, second.Actions, 1)
			completed := completionAction(t, second)
			require.Equal(t, expected, completed.OrchestrationStatus)
			if mode == "cleanup-fails" {
				require.Contains(t, completed.GetFailureDetails().GetErrorMessage(), "cleanup failed")
			}
		})
	}
}

func TestTeardownFinalizationReleasesLocksExactlyOnce(t *testing.T) {
	entities := []api.EntityID{api.NewEntityID("counter", "a"), api.NewEntityID("counter", "b")}
	registry := teardownRegistry(t, func(ctx *OrchestrationContext) (any, error) {
		ctx.Go(func(child *OrchestrationContext) {
			unlock, err := child.LockEntities(entities...)
			if err != nil {
				panic(err)
			}
			defer unlock()
			_ = child.WaitForSingleEvent("never", -1).Await(nil)
		})
		if err := ctx.WaitForSingleEvent("finish", -1).Await(nil); err != nil {
			return nil, err
		}
		return nil, errors.New("root failure")
	})
	first := executeOrchestrationTurn(t, registry, "teardown-instance", nil, teardownStarted())
	require.Len(t, first.Actions, 1)
	request := first.Actions[0].GetSendEntityMessage().GetEntityLockRequested()
	require.NotNil(t, request)
	old := append(teardownStarted(), lockRequestHistory(first.Actions[0]))
	response := executeOrchestrationTurn(t, registry, "teardown-instance", old, []*protos.HistoryEvent{
		lockGrantedHistory(request.CriticalSectionId),
		helpers.NewEventRaisedEvent("finish", nil),
	})
	require.Len(t, response.Actions, 3)
	for index, entity := range entities {
		unlocked := response.Actions[index].GetSendEntityMessage().GetEntityUnlockSent()
		require.NotNil(t, unlocked)
		require.Equal(t, request.CriticalSectionId, unlocked.CriticalSectionId)
		require.Equal(t, entity.String(), unlocked.GetTargetInstanceId().GetValue())
	}
	require.Equal(t, api.RUNTIME_STATUS_FAILED, response.Actions[2].GetCompleteOrchestration().GetOrchestrationStatus())
}
