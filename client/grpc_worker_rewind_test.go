package client

import (
	"context"
	"strings"
	"testing"

	"github.com/microsoft/durabletask-go/api"
	"github.com/microsoft/durabletask-go/internal/helpers"
	"github.com/microsoft/durabletask-go/internal/largepayload"
	"github.com/microsoft/durabletask-go/internal/protos"
	"github.com/microsoft/durabletask-go/payload"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

func TestWorkerRewindHistoryTransport(t *testing.T) {
	for _, streamed := range []bool{false, true} {
		name := "inline"
		if streamed {
			name = "streamed"
		}
		t.Run(name, func(t *testing.T) {
			store := payload.NewMemoryStore()
			options := &api.LargePayloadOptions{Store: store, Resolver: store, ThresholdBytes: 8}
			raw := wrapperspb.String(`"preserved large input"`)
			token, err := largepayload.Externalize(context.Background(), options, raw)
			require.NoError(t, err)
			require.NotEqual(t, raw.Value, token.Value)
			start := helpers.NewExecutionStartedEvent("workflow", "instance", token, nil, nil, nil)
			oldID := start.GetExecutionStarted().GetOrchestrationInstance().GetExecutionId().GetValue()
			history := []*protos.HistoryEvent{
				helpers.NewOrchestratorStartedEvent(), start,
				helpers.NewTaskScheduledEvent(0, "bad", nil, token, nil),
				helpers.NewTaskFailedEvent(0, &protos.TaskFailureDetails{ErrorMessage: "failed"}),
				{EventType: &protos.HistoryEvent_ExecutionCompleted{
					ExecutionCompleted: &protos.ExecutionCompletedEvent{OrchestrationStatus: api.RUNTIME_STATUS_FAILED},
				}},
			}
			client := &fakeSchedulerClient{history: []*protos.HistoryChunk{
				{Events: history[:2]}, {Events: history[2:]},
			}}
			worker := newFakeWorker(t, client, WithWorkerLargePayloads(options))
			request := &protos.OrchestratorRequest{
				InstanceId: "instance", ExecutionId: wrapperspb.String(oldID), PastEvents: history,
				NewEvents: []*protos.HistoryEvent{
					helpers.NewOrchestratorStartedEvent(),
					{EventType: &protos.HistoryEvent_ExecutionRewound{ExecutionRewound: &protos.ExecutionRewoundEvent{}}},
				},
				RequiresHistoryStreaming: streamed,
			}
			if streamed {
				request.PastEvents = nil
			}
			worker.processOrchestration(context.Background(), client, "completion-token", request)
			require.Zero(t, client.orchestrationAbandons)
			require.Len(t, client.orchestrationCompletions, 1)
			response := client.orchestrationCompletions[0]
			require.Equal(t, "completion-token", response.CompletionToken)
			require.Nil(t, response.CustomStatus)
			require.Len(t, response.Actions, 1)
			clean := response.Actions[0].GetRewindOrchestration().GetNewHistory()
			require.Len(t, clean, 4)
			rewritten := clean[1].GetExecutionStarted()
			require.NotEqual(t, oldID, rewritten.GetOrchestrationInstance().GetExecutionId().GetValue())
			require.NotEqual(t, raw.Value, rewritten.GetInput().GetValue(), "replacement history must be externalized before send")
			hydrated, err := largepayload.Hydrate(context.Background(), options, rewritten.Input)
			require.NoError(t, err)
			require.Equal(t, raw.Value, hydrated.Value)
			require.NotNil(t, clean[3].GetExecutionRewound())
		})
	}
}

func TestWorkerRewindOversizedHistoryFailsExplicitly(t *testing.T) {
	client := new(fakeSchedulerClient)
	worker := newFakeWorker(t, client, WithMaxOrchestratorCompletionBytes(minOrchestratorCompletionBytes))
	worker.processOrchestration(context.Background(), client, "token", &protos.OrchestratorRequest{
		InstanceId: "instance",
		PastEvents: []*protos.HistoryEvent{
			helpers.NewExecutionStartedEvent("workflow", "instance", nil, nil, nil, nil),
			{EventType: &protos.HistoryEvent_GenericEvent{GenericEvent: &protos.GenericEvent{
				Data: wrapperspb.String(strings.Repeat("x", 2*minOrchestratorCompletionBytes)),
			}}},
			{EventType: &protos.HistoryEvent_ExecutionCompleted{
				ExecutionCompleted: &protos.ExecutionCompletedEvent{OrchestrationStatus: api.RUNTIME_STATUS_FAILED},
			}},
		},
		NewEvents: []*protos.HistoryEvent{
			helpers.NewOrchestratorStartedEvent(),
			{EventType: &protos.HistoryEvent_ExecutionRewound{ExecutionRewound: &protos.ExecutionRewoundEvent{}}},
		},
	})
	require.Len(t, client.orchestrationCompletions, 1)
	response := client.orchestrationCompletions[0]
	require.Len(t, response.Actions, 1)
	completion := response.Actions[0].GetCompleteOrchestration()
	require.Equal(t, api.RUNTIME_STATUS_FAILED, completion.GetOrchestrationStatus())
	require.Equal(t, string(api.ErrorTypeOrchestratorResponseTooLarge), completion.GetFailureDetails().GetErrorType())
	require.True(t, completion.GetFailureDetails().GetIsNonRetriable())
}
