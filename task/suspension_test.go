package task

import (
	"fmt"
	"testing"

	"github.com/microsoft/durabletask-go/api"
	"github.com/microsoft/durabletask-go/internal/helpers"
	"github.com/microsoft/durabletask-go/internal/protos"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

func suspensionRegistry(t *testing.T) *TaskRegistry {
	t.Helper()
	registry := NewTaskRegistry()
	require.NoError(t, registry.AddOrchestratorN("suspendable", func(ctx *OrchestrationContext) (any, error) {
		var payload string
		if err := ctx.WaitForSingleEvent("release", -1).Await(&payload); err != nil {
			return nil, err
		}
		return payload, nil
	}))
	return registry
}

func suspensionHistory(events ...*protos.HistoryEvent) []*protos.HistoryEvent {
	return append([]*protos.HistoryEvent{
		helpers.NewOrchestratorStartedEvent(),
		helpers.NewExecutionStartedEvent("suspendable", "instance", nil, nil, nil, nil),
	}, events...)
}

// TestRedundantSuspendIsDroppedNotBuffered asserts a suspend received while
// already suspended is dropped instead of buffered. Buffering it would
// re-suspend the orchestration as soon as the matching resume drained the
// buffer, so N suspends would require N resumes to make progress again.
func TestRedundantSuspendIsDroppedNotBuffered(t *testing.T) {
	for _, suspendCount := range []int{1, 2, 3} {
		t.Run(fmt.Sprintf("suspends=%d", suspendCount), func(t *testing.T) {
			events := make([]*protos.HistoryEvent, 0, suspendCount+2)
			for range suspendCount {
				events = append(events, helpers.NewSuspendOrchestrationEvent("hold"))
			}

			events = append(
				events,
				helpers.NewEventRaisedEvent("release", wrapperspb.String(`"payload"`)),
				helpers.NewResumeOrchestrationEvent("go"),
			)

			response := executeOrchestrationTurn(
				t,
				suspensionRegistry(t),
				"instance",
				nil,
				suspensionHistory(events...),
			)
			completed := completionAction(t, response)
			require.Equal(
				t,
				protos.OrchestrationStatus_ORCHESTRATION_STATUS_COMPLETED,
				completed.GetOrchestrationStatus(),
			)
			require.Equal(t, `"payload"`, completed.GetResult().GetValue())
		})
	}
}

func TestTerminationReplacesNaturalCompletionInTheSameTurn(t *testing.T) {
	registry := NewTaskRegistry()
	if err := registry.AddOrchestratorN("complete-on-event", func(ctx *OrchestrationContext) (any, error) {
		if err := ctx.WaitForSingleEvent("finish", -1).Await(nil); err != nil {
			return nil, err
		}
		return "natural", nil
	}); err != nil {
		t.Fatal(err)
	}
	instanceID := api.InstanceID("terminate-completion-race")
	result := executeOrchestrationTurn(
		t,
		registry,
		instanceID,
		[]*protos.HistoryEvent{
			helpers.NewOrchestratorStartedEvent(),
			helpers.NewExecutionStartedEvent(
				"complete-on-event",
				string(instanceID),
				nil,
				nil,
				nil,
				nil,
			),
		},
		[]*protos.HistoryEvent{
			helpers.NewOrchestratorStartedEvent(),
			helpers.NewEventRaisedEvent("finish", nil),
			helpers.NewExecutionTerminatedEvent(wrapperspb.String(`"terminated"`), true),
		},
	)
	var completions []*protos.CompleteOrchestrationAction
	for _, action := range result.Actions {
		if completion := action.GetCompleteOrchestration(); completion != nil {
			completions = append(completions, completion)
		}
	}
	if len(completions) != 1 {
		t.Fatalf("completion actions = %d, want 1: %v", len(completions), result.Actions)
	}
	if got, want := completions[0].GetOrchestrationStatus(),
		protos.OrchestrationStatus_ORCHESTRATION_STATUS_TERMINATED; got != want {
		t.Fatalf("completion status = %v, want %v", got, want)
	}
	if got, want := completions[0].GetResult().GetValue(), `"terminated"`; got != want {
		t.Fatalf("completion result = %q, want %q", got, want)
	}
}

// TestSuspendedOrchestrationEmitsNoActions asserts a still-suspended
// orchestration withholds its actions, which is what keeps buffered events from
// being acted upon while suspended.
func TestSuspendedOrchestrationEmitsNoActions(t *testing.T) {
	response := executeOrchestrationTurn(t, suspensionRegistry(t), "instance", nil, suspensionHistory(
		helpers.NewSuspendOrchestrationEvent("hold"),
		helpers.NewEventRaisedEvent("release", wrapperspb.String(`"payload"`)),
	))
	require.Empty(t, response.Actions)
}

// TestTerminationOverridesSuspension asserts termination is processed even
// while suspended and still emits the terminal completion action.
func TestTerminationOverridesSuspension(t *testing.T) {
	for _, test := range []struct {
		name   string
		events []*protos.HistoryEvent
	}{
		{
			name: "terminate-while-suspended",
			events: []*protos.HistoryEvent{
				helpers.NewSuspendOrchestrationEvent("hold"),
				helpers.NewExecutionTerminatedEvent(wrapperspb.String(`"stopped"`), false),
			},
		},
		{
			name: "terminate-after-repeated-suspends",
			events: []*protos.HistoryEvent{
				helpers.NewSuspendOrchestrationEvent("hold"),
				helpers.NewSuspendOrchestrationEvent("hold again"),
				helpers.NewExecutionTerminatedEvent(wrapperspb.String(`"stopped"`), false),
			},
		},
		{
			name: "terminate-with-buffered-events",
			events: []*protos.HistoryEvent{
				helpers.NewSuspendOrchestrationEvent("hold"),
				helpers.NewEventRaisedEvent("release", wrapperspb.String(`"payload"`)),
				helpers.NewExecutionTerminatedEvent(wrapperspb.String(`"stopped"`), false),
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			response := executeOrchestrationTurn(
				t,
				suspensionRegistry(t),
				"instance",
				nil,
				suspensionHistory(test.events...),
			)
			completed := completionAction(t, response)
			require.Equal(
				t,
				protos.OrchestrationStatus_ORCHESTRATION_STATUS_TERMINATED,
				completed.GetOrchestrationStatus(),
			)
			require.Equal(t, `"stopped"`, completed.GetResult().GetValue())
		})
	}
}

// TestResumeWithoutSuspendIsANoOp asserts resuming an orchestration that was
// never suspended does not disturb normal event processing.
func TestResumeWithoutSuspendIsANoOp(t *testing.T) {
	response := executeOrchestrationTurn(t, suspensionRegistry(t), "instance", nil, suspensionHistory(
		helpers.NewResumeOrchestrationEvent("no-op"),
		helpers.NewResumeOrchestrationEvent("still a no-op"),
		helpers.NewEventRaisedEvent("release", wrapperspb.String(`"payload"`)),
	))
	completed := completionAction(t, response)
	require.Equal(
		t,
		protos.OrchestrationStatus_ORCHESTRATION_STATUS_COMPLETED,
		completed.GetOrchestrationStatus(),
	)
	require.Equal(t, `"payload"`, completed.GetResult().GetValue())
}
