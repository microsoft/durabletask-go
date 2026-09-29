package task

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"testing"
	"time"

	"github.com/microsoft/durabletask-go/api"
	"github.com/microsoft/durabletask-go/internal/helpers"
	"github.com/microsoft/durabletask-go/internal/protos"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

const (
	continueAsNewEventsName     = "continue-as-new-events"
	continueAsNewEventsInstance = api.InstanceID("continue-as-new-events-instance")
)

// timerWinsThenContinuesAsNew returns an orchestrator whose first generation
// leaves the given number of "work" waits pending, continues as new when its
// timer wins, and whose next generation returns the first "work" value.
func timerWinsThenContinuesAsNew(waits int, keep bool) Orchestrator {
	return func(ctx *OrchestrationContext) (any, error) {
		var generation int
		if err := ctx.GetInput(&generation); err != nil {
			return nil, err
		}
		if generation > 0 {
			var value int
			err := ctx.WaitForSingleEvent("work", -1).Await(&value)
			return value, err
		}
		tasks := make([]Task, 0, waits+1)
		for range waits {
			tasks = append(tasks, ctx.WaitForSingleEvent("work", -1))
		}
		timer := ctx.CreateTimer(time.Hour)
		if ctx.WhenAny(append(tasks, timer)...) != timer {
			return nil, errors.New("an event won before the timer")
		}
		var options []ContinueAsNewOption
		if keep {
			options = append(options, WithKeepUnprocessedEvents())
		}
		ctx.ContinueAsNew(generation+1, options...)
		return nil, nil
	}
}

func continueAsNewEventsRegistry(t *testing.T, orchestrator Orchestrator) *TaskRegistry {
	t.Helper()
	registry := NewTaskRegistry()
	require.NoError(t, registry.AddOrchestratorN(continueAsNewEventsName, orchestrator))
	return registry
}

func continueAsNewEventsStarted(input *wrapperspb.StringValue) []*protos.HistoryEvent {
	return []*protos.HistoryEvent{
		helpers.NewOrchestratorStartedEvent(),
		helpers.NewExecutionStartedEvent(
			continueAsNewEventsName,
			string(continueAsNewEventsInstance),
			input,
			nil,
			nil,
			nil,
		),
	}
}

// afterFirstTimer runs a first turn that must only schedule one timer. It
// returns the committed history and the event that fires that timer.
func afterFirstTimer(t *testing.T, registry *TaskRegistry) ([]*protos.HistoryEvent, *protos.HistoryEvent) {
	t.Helper()
	history := continueAsNewEventsStarted(nil)
	response := executeContinueAsNewTurn(t, registry, OrchestrationOptions{}, nil, history...)
	require.Len(t, response.Actions, 1)
	timer := response.Actions[0].GetCreateTimer()
	require.NotNil(t, timer, "the first turn must only schedule the timer")
	timerID := response.Actions[0].Id
	return append(history, helpers.NewTimerCreatedEvent(timerID, timer.GetFireAt())),
		helpers.NewTimerFiredEvent(timerID, timer.GetFireAt(), nil)
}

func executeContinueAsNewTurn(
	t *testing.T,
	registry *TaskRegistry,
	options OrchestrationOptions,
	oldEvents []*protos.HistoryEvent,
	newEvents ...*protos.HistoryEvent,
) *protos.OrchestratorResponse {
	t.Helper()
	result, err := NewTaskExecutor(registry, WithOrchestrationOptions(options)).ExecuteOrchestrator(
		context.Background(),
		continueAsNewEventsInstance,
		oldEvents,
		newEvents,
		supportedEntityParameters(),
	)
	require.NoError(t, err)
	return result.Response
}

func raised(name string, value int) *protos.HistoryEvent {
	return helpers.NewEventRaisedEvent(name, wrapperspb.String(strconv.Itoa(value)))
}

// requireContinuedAsNewCarryover asserts a CONTINUED_AS_NEW completion whose
// carryover holds exactly want, formatted as name=payload, in order.
func requireContinuedAsNewCarryover(
	t *testing.T,
	response *protos.OrchestratorResponse,
	want ...string,
) *protos.CompleteOrchestrationAction {
	t.Helper()
	completed := completionAction(t, response)
	require.Equal(
		t,
		protos.OrchestrationStatus_ORCHESTRATION_STATUS_CONTINUED_AS_NEW,
		completed.GetOrchestrationStatus(),
	)
	var carryover []string
	for _, event := range completed.GetCarryoverEvents() {
		eventRaised := event.GetEventRaised()
		carryover = append(carryover, eventRaised.GetName()+"="+eventRaised.GetInput().GetValue())
	}
	require.Equal(t, want, carryover)
	return completed
}

// TestContinueAsNewRetainsEventRaisedAfterTimerWins covers an event that is in
// the same work item as the winning timer while a WaitForSingleEvent loser is
// still pending. The finalized generation can never run that wait again.
func TestContinueAsNewRetainsEventRaisedAfterTimerWins(t *testing.T) {
	for _, keep := range []bool{true, false} {
		t.Run(fmt.Sprintf("keep=%t", keep), func(t *testing.T) {
			registry := continueAsNewEventsRegistry(t, timerWinsThenContinuesAsNew(1, keep))
			oldEvents, timerFired := afterFirstTimer(t, registry)
			response := executeContinueAsNewTurn(t, registry, OrchestrationOptions{}, oldEvents,
				helpers.NewOrchestratorStartedEvent(),
				timerFired,
				raised("work", 1),
			)
			if !keep {
				requireContinuedAsNewCarryover(t, response)
				return
			}
			completed := requireContinuedAsNewCarryover(t, response, "work=1")

			nextGeneration := append(
				continueAsNewEventsStarted(completed.GetResult()),
				completed.GetCarryoverEvents()...,
			)
			next := executeContinueAsNewTurn(t, registry, OrchestrationOptions{}, nil, nextGeneration...)
			require.Equal(t, "1", completionResult(t, next))
		})
	}
}

func TestContinueAsNewCarryoverKeepsArrivalOrderAfterTimerWins(t *testing.T) {
	registry := continueAsNewEventsRegistry(t, timerWinsThenContinuesAsNew(2, true))
	oldEvents, timerFired := afterFirstTimer(t, registry)
	response := executeContinueAsNewTurn(t, registry, OrchestrationOptions{}, oldEvents,
		helpers.NewOrchestratorStartedEvent(),
		raised("other", 0),
		timerFired,
		raised("work", 1),
		raised("other", 2),
		raised("WORK", 3),
		raised("work", 4),
	)
	requireContinuedAsNewCarryover(t, response, "other=0", "work=1", "other=2", "WORK=3", "work=4")
}

// TestContinueAsNewFinalizationBoundary verifies that events keep their normal
// delivery until a continue-as-new completion is finalized.
func TestContinueAsNewFinalizationBoundary(t *testing.T) {
	t.Run("event consumed before the timer is not carried over", func(t *testing.T) {
		registry := continueAsNewEventsRegistry(t, func(ctx *OrchestrationContext) (any, error) {
			pending := ctx.WaitForSingleEvent("work", -1)
			if ctx.WhenAny(pending, ctx.CreateTimer(time.Hour)) != pending {
				return nil, errors.New("the timer won")
			}
			var value int
			if err := pending.Await(&value); err != nil {
				return nil, err
			}
			ctx.ContinueAsNew(value, WithKeepUnprocessedEvents())
			return nil, nil
		})
		oldEvents, timerFired := afterFirstTimer(t, registry)
		response := executeContinueAsNewTurn(t, registry, OrchestrationOptions{}, oldEvents,
			helpers.NewOrchestratorStartedEvent(),
			raised("work", 1),
			raised("work", 2),
			timerFired,
		)
		completed := requireContinuedAsNewCarryover(t, response, "work=2")
		require.Equal(t, "1", completed.GetResult().GetValue())
	})

	t.Run("continue-as-new intent does not stop delivery", func(t *testing.T) {
		registry := continueAsNewEventsRegistry(t, func(ctx *OrchestrationContext) (any, error) {
			ctx.ContinueAsNew(0, WithKeepUnprocessedEvents())
			var value int
			if err := ctx.WaitForSingleEvent("work", -1).Await(&value); err != nil {
				return nil, err
			}
			ctx.ContinueAsNew(value, WithKeepUnprocessedEvents())
			return nil, nil
		})
		newEvents := append(continueAsNewEventsStarted(nil), raised("work", 1), raised("work", 2))
		response := executeContinueAsNewTurn(t, registry, OrchestrationOptions{}, nil, newEvents...)
		completed := requireContinuedAsNewCarryover(t, response, "work=2")
		require.Equal(t, "1", completed.GetResult().GetValue())
	})
}

// TestContinueAsNewCarryoverForEventWaitPatterns covers the ways an event
// receive can still be outstanding when a timer wins.
func TestContinueAsNewCarryoverForEventWaitPatterns(t *testing.T) {
	tests := []struct {
		name         string
		orchestrator Orchestrator
	}{
		{
			name: "blocked event channel receive",
			orchestrator: func(ctx *OrchestrationContext) (any, error) {
				channel := NewEventChannel[int](ctx, "work")
				ctx.Go(func(ctx *OrchestrationContext) {
					_, _ = channel.ReceiveErr(ctx)
				})
				if err := ctx.CreateTimer(time.Hour).Await(nil); err != nil {
					return nil, err
				}
				ctx.ContinueAsNew(1, WithKeepUnprocessedEvents())
				return nil, nil
			},
		},
		{
			name: "canceled wait scope",
			orchestrator: func(ctx *OrchestrationContext) (any, error) {
				waitCtx, cancel := ctx.WithCancel()
				pending := waitCtx.WaitForSingleEvent("work", -1)
				timer := ctx.CreateTimer(time.Hour)
				if ctx.WhenAny(pending, timer) != timer {
					return nil, errors.New("an event won before the timer")
				}
				cancel()
				ctx.ContinueAsNew(1, WithKeepUnprocessedEvents())
				return nil, nil
			},
		},
		{
			name: "event channel select",
			orchestrator: func(ctx *OrchestrationContext) (any, error) {
				timerWon := false
				ctx.Select(
					OnTask(ctx.CreateTimer(time.Hour), func(Task) { timerWon = true }),
					OnEvent(NewEventChannel[int](ctx, "work"), func(int) {}),
				)
				if !timerWon {
					return nil, errors.New("an event won before the timer")
				}
				ctx.ContinueAsNew(1, WithKeepUnprocessedEvents())
				return nil, nil
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			registry := continueAsNewEventsRegistry(t, test.orchestrator)
			oldEvents, timerFired := afterFirstTimer(t, registry)
			response := executeContinueAsNewTurn(t, registry, OrchestrationOptions{}, oldEvents,
				helpers.NewOrchestratorStartedEvent(),
				timerFired,
				raised("work", 1),
			)
			requireContinuedAsNewCarryover(t, response, "work=1")
		})
	}
}

// TestContinueAsNewIntentDoesNotAddCarryoverToOtherOutcomes verifies that only
// an actual CONTINUED_AS_NEW completion carries events forward.
func TestContinueAsNewIntentDoesNotAddCarryoverToOtherOutcomes(t *testing.T) {
	tests := []struct {
		name         string
		orchestrator Orchestrator
		trailing     []*protos.HistoryEvent
		status       protos.OrchestrationStatus
	}{
		{
			name: "orchestrator error",
			orchestrator: func(ctx *OrchestrationContext) (any, error) {
				ctx.ContinueAsNew(1, WithKeepUnprocessedEvents())
				return nil, errors.New("orchestrator failed")
			},
			status: protos.OrchestrationStatus_ORCHESTRATION_STATUS_FAILED,
		},
		{
			name: "input serialization error",
			orchestrator: func(ctx *OrchestrationContext) (any, error) {
				ctx.ContinueAsNew(make(chan int), WithKeepUnprocessedEvents())
				return nil, nil
			},
			status: protos.OrchestrationStatus_ORCHESTRATION_STATUS_FAILED,
		},
		{
			name: "termination",
			orchestrator: func(ctx *OrchestrationContext) (any, error) {
				ctx.ContinueAsNew(1, WithKeepUnprocessedEvents())
				return nil, ctx.WaitForSingleEvent("never", -1).Await(nil)
			},
			trailing: []*protos.HistoryEvent{
				helpers.NewExecutionTerminatedEvent(wrapperspb.String(`"stopped"`), false),
			},
			status: protos.OrchestrationStatus_ORCHESTRATION_STATUS_TERMINATED,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			registry := continueAsNewEventsRegistry(t, test.orchestrator)
			newEvents := append(continueAsNewEventsStarted(nil), raised("other", 1))
			newEvents = append(newEvents, test.trailing...)
			response := executeContinueAsNewTurn(t, registry, OrchestrationOptions{}, nil, newEvents...)
			completed := completionAction(t, response)
			require.Equal(t, test.status, completed.GetOrchestrationStatus())
			require.Empty(t, completed.GetCarryoverEvents())
		})
	}
}

// TestContinueAsNewRetainsTrailingEventsAcrossTurnCapsAndSuspension covers
// trailing events left unread by MaxEventsPerTurn and events buffered while
// suspended that are drained after the timer when the orchestration resumes.
func TestContinueAsNewRetainsTrailingEventsAcrossTurnCapsAndSuspension(t *testing.T) {
	registry := continueAsNewEventsRegistry(t, timerWinsThenContinuesAsNew(1, true))
	oldEvents, timerFired := afterFirstTimer(t, registry)
	trailing := []*protos.HistoryEvent{raised("work", 1), raised("other", 2), raised("work", 3)}
	tests := []struct {
		name      string
		options   OrchestrationOptions
		oldEvents []*protos.HistoryEvent
		newEvents []*protos.HistoryEvent
	}{
		{
			name:      "turn cap after a trailing event",
			options:   OrchestrationOptions{MaxEventsPerTurn: 2},
			oldEvents: oldEvents,
			newEvents: append([]*protos.HistoryEvent{helpers.NewOrchestratorStartedEvent(), timerFired}, trailing...),
		},
		{
			name:      "turn cap before trailing events",
			options:   OrchestrationOptions{MaxEventsPerTurn: 1},
			oldEvents: oldEvents,
			newEvents: append([]*protos.HistoryEvent{helpers.NewOrchestratorStartedEvent(), timerFired}, trailing...),
		},
		{
			name:      "suspended and resumed in one work item",
			oldEvents: oldEvents,
			newEvents: []*protos.HistoryEvent{
				helpers.NewOrchestratorStartedEvent(),
				helpers.NewSuspendOrchestrationEvent("hold"),
				timerFired,
				trailing[0],
				trailing[1],
				helpers.NewResumeOrchestrationEvent("go"),
				trailing[2],
			},
		},
		{
			name: "suspended history drained on resume",
			oldEvents: append(slices.Clone(oldEvents),
				helpers.NewOrchestratorStartedEvent(),
				helpers.NewSuspendOrchestrationEvent("hold"),
				timerFired,
				trailing[0],
				trailing[1],
			),
			newEvents: []*protos.HistoryEvent{
				helpers.NewOrchestratorStartedEvent(),
				helpers.NewResumeOrchestrationEvent("go"),
				trailing[2],
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			response := executeContinueAsNewTurn(t, registry, test.options, test.oldEvents, test.newEvents...)
			requireContinuedAsNewCarryover(t, response, "work=1", "other=2", "work=3")
			require.Nil(t, response.NumEventsProcessed)
		})
	}
}
