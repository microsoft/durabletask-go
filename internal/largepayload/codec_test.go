package largepayload

import (
	"context"
	"errors"
	"testing"

	"github.com/microsoft/durabletask-go/api"
	"github.com/microsoft/durabletask-go/internal/protos"
	"github.com/microsoft/durabletask-go/payload"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

func TestExternalizeAndHydrate(t *testing.T) {
	store := payload.NewMemoryStore()
	options := &api.LargePayloadOptions{
		Store:           store,
		Resolver:        store,
		ThresholdBytes:  4,
		MaxPayloadBytes: 1024,
	}
	externalized, err := Externalize(context.Background(), options, wrapperspb.String("large payload"))
	require.NoError(t, err)
	require.NotEqual(t, "large payload", externalized.GetValue())

	hydrated, err := Hydrate(context.Background(), options, externalized)
	require.NoError(t, err)
	require.Equal(t, "large payload", hydrated.GetValue())
}

func TestLargePayloadIntegrityFailure(t *testing.T) {
	store := payload.NewMemoryStore()
	options := &api.LargePayloadOptions{
		Store:           store,
		Resolver:        store,
		ThresholdBytes:  1,
		MaxPayloadBytes: 1024,
	}
	externalized, err := Externalize(context.Background(), options, wrapperspb.String("payload"))
	require.NoError(t, err)

	options.Resolver = staticResolver{payload: []byte("tampered")}
	_, err = Hydrate(context.Background(), options, externalized)
	require.ErrorIs(t, err, api.ErrLargePayloadIntegrity)
}

func TestLargePayloadReferenceRequiresConfiguration(t *testing.T) {
	store := payload.NewMemoryStore()
	options := &api.LargePayloadOptions{
		Store:           store,
		Resolver:        store,
		ThresholdBytes:  1,
		MaxPayloadBytes: 1024,
	}
	externalized, err := Externalize(context.Background(), options, wrapperspb.String("payload"))
	require.NoError(t, err)
	_, err = Hydrate(context.Background(), nil, externalized)
	require.ErrorIs(t, err, api.ErrFeatureNotSupported)
	_, err = Externalize(context.Background(), nil, externalized)
	require.ErrorIs(t, err, api.ErrFeatureNotSupported)
	err = TransformActivityRequest(context.Background(), nil, &protos.ActivityRequest{Input: externalized})
	require.ErrorIs(t, err, api.ErrFeatureNotSupported)
	err = TransformOrchestrationState(context.Background(), nil, &protos.OrchestrationState{Output: externalized})
	require.ErrorIs(t, err, api.ErrFeatureNotSupported)
}

func TestLargePayloadLimitAndMalformedReference(t *testing.T) {
	store := payload.NewMemoryStore()
	options := &api.LargePayloadOptions{
		Store:           store,
		Resolver:        store,
		ThresholdBytes:  1,
		MaxPayloadBytes: 4,
	}

	_, err := Externalize(context.Background(), options, wrapperspb.String("oversized"))
	require.ErrorIs(t, err, api.ErrLargePayloadTooLarge)

	_, err = Hydrate(context.Background(), options, wrapperspb.String(referencePrefix+"not-base64"))
	require.ErrorIs(t, err, api.ErrLargePayloadReference)
}

func TestNativeLargePayloadTokens(t *testing.T) {
	store := nativeStore{}
	options := &api.LargePayloadOptions{
		Store:           store,
		Resolver:        store,
		ThresholdBytes:  4,
		MaxPayloadBytes: 1024,
	}
	externalized, err := Externalize(context.Background(), options, wrapperspb.String("payload"))
	require.NoError(t, err)
	require.Equal(t, "blob:v2:https://account.example/payload", externalized.GetValue())

	hydrated, err := Hydrate(context.Background(), options, externalized)
	require.NoError(t, err)
	require.Equal(t, "payload", hydrated.GetValue())
	_, err = Hydrate(context.Background(), options, wrapperspb.String("blob:v2:malformed"))
	require.ErrorIs(t, err, api.ErrLargePayloadReference)
}

func TestNativeLargePayloadThresholdIsInclusive(t *testing.T) {
	store := inclusiveNativeStore{}
	externalized, err := Externalize(context.Background(), &api.LargePayloadOptions{
		Store: store, Resolver: store, ThresholdBytes: len("payload"), MaxPayloadBytes: 1024,
	}, wrapperspb.String("payload"))
	require.NoError(t, err)
	require.Equal(t, "blob:v2:https://account.example/payload", externalized.GetValue())
}

func TestTransformOrchestratorResponsePayloadFields(t *testing.T) {
	store := payload.NewMemoryStore()
	options := &api.LargePayloadOptions{
		Store:           store,
		Resolver:        store,
		ThresholdBytes:  1,
		MaxPayloadBytes: 1024,
	}
	response := &protos.OrchestratorResponse{
		CustomStatus: wrapperspb.String("status"),
		Actions: []*protos.OrchestratorAction{
			{
				OrchestratorActionType: &protos.OrchestratorAction_ScheduleTask{
					ScheduleTask: &protos.ScheduleTaskAction{Input: wrapperspb.String("activity")},
				},
			},
			{
				OrchestratorActionType: &protos.OrchestratorAction_CreateSubOrchestration{
					CreateSubOrchestration: &protos.CreateSubOrchestrationAction{Input: wrapperspb.String("child")},
				},
			},
			{
				OrchestratorActionType: &protos.OrchestratorAction_CompleteOrchestration{
					CompleteOrchestration: &protos.CompleteOrchestrationAction{Result: wrapperspb.String("result")},
				},
			},
			{
				OrchestratorActionType: &protos.OrchestratorAction_SendEntityMessage{
					SendEntityMessage: &protos.SendEntityMessageAction{
						EntityMessageType: &protos.SendEntityMessageAction_EntityOperationCalled{
							EntityOperationCalled: &protos.EntityOperationCalledEvent{Input: wrapperspb.String("entity")},
						},
					},
				},
			},
		},
	}
	require.NoError(t, TransformOrchestratorResponse(context.Background(), options, response))

	for _, value := range []*wrapperspb.StringValue{
		response.CustomStatus,
		response.Actions[0].GetScheduleTask().Input,
		response.Actions[1].GetCreateSubOrchestration().Input,
		response.Actions[2].GetCompleteOrchestration().Result,
		response.Actions[3].GetSendEntityMessage().GetEntityOperationCalled().Input,
	} {
		hydrated, err := Hydrate(context.Background(), options, value)
		require.NoError(t, err)
		require.NotEqual(t, value.GetValue(), hydrated.GetValue())
	}
}

func BenchmarkTransformOrchestratorResponseDisabled(b *testing.B) {
	response := &protos.OrchestratorResponse{
		CustomStatus: wrapperspb.String("status"),
		Actions: []*protos.OrchestratorAction{
			{
				OrchestratorActionType: &protos.OrchestratorAction_ScheduleTask{
					ScheduleTask: &protos.ScheduleTaskAction{Input: wrapperspb.String("activity")},
				},
			},
			{
				OrchestratorActionType: &protos.OrchestratorAction_CreateSubOrchestration{
					CreateSubOrchestration: &protos.CreateSubOrchestrationAction{Input: wrapperspb.String("child")},
				},
			},
			{
				OrchestratorActionType: &protos.OrchestratorAction_CompleteOrchestration{
					CompleteOrchestration: &protos.CompleteOrchestrationAction{Result: wrapperspb.String("result")},
				},
			},
		},
	}
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if err := TransformOrchestratorResponse(ctx, nil, response); err != nil {
			b.Fatal(err)
		}
	}
}

func TestTransformOrchestratorResponseDisabledFastPath(t *testing.T) {
	t.Run("ordinary payload ignores cancellation because no work is required", func(t *testing.T) {
		value := wrapperspb.String("ordinary")
		response := &protos.OrchestratorResponse{CustomStatus: value}
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		require.NoError(t, TransformOrchestratorResponse(ctx, nil, response))
		require.Same(t, value, response.CustomStatus)
	})

	t.Run("reserved reference still requires configuration", func(t *testing.T) {
		response := &protos.OrchestratorResponse{
			CustomStatus: wrapperspb.String(api.AzureBlobPayloadReferencePrefixV2 + "https://account.example/payload"),
		}

		err := TransformOrchestratorResponse(context.Background(), nil, response)
		require.ErrorIs(t, err, api.ErrFeatureNotSupported)
	})
}

func TestTransformHistoryEventPayloadFields(t *testing.T) {
	store := payload.NewMemoryStore()
	options := &api.LargePayloadOptions{
		Store:           store,
		Resolver:        store,
		ThresholdBytes:  1,
		MaxPayloadBytes: 1024,
	}
	tests := []struct {
		name  string
		event *protos.HistoryEvent
		value func(*protos.HistoryEvent) *wrapperspb.StringValue
	}{
		{
			name: "event raised",
			event: &protos.HistoryEvent{EventType: &protos.HistoryEvent_EventRaised{
				EventRaised: &protos.EventRaisedEvent{Input: wrapperspb.String("raised")},
			}},
			value: func(event *protos.HistoryEvent) *wrapperspb.StringValue {
				return event.GetEventRaised().Input
			},
		},
		{
			name: "generic data",
			event: &protos.HistoryEvent{EventType: &protos.HistoryEvent_GenericEvent{
				GenericEvent: &protos.GenericEvent{Data: wrapperspb.String("generic")},
			}},
			value: func(event *protos.HistoryEvent) *wrapperspb.StringValue {
				return event.GetGenericEvent().Data
			},
		},
		{
			name: "execution suspended",
			event: &protos.HistoryEvent{EventType: &protos.HistoryEvent_ExecutionSuspended{
				ExecutionSuspended: &protos.ExecutionSuspendedEvent{Input: wrapperspb.String("suspended")},
			}},
			value: func(event *protos.HistoryEvent) *wrapperspb.StringValue {
				return event.GetExecutionSuspended().Input
			},
		},
		{
			name: "execution resumed",
			event: &protos.HistoryEvent{EventType: &protos.HistoryEvent_ExecutionResumed{
				ExecutionResumed: &protos.ExecutionResumedEvent{Input: wrapperspb.String("resumed")},
			}},
			value: func(event *protos.HistoryEvent) *wrapperspb.StringValue {
				return event.GetExecutionResumed().Input
			},
		},
		{
			name: "event sent",
			event: &protos.HistoryEvent{EventType: &protos.HistoryEvent_EventSent{
				EventSent: &protos.EventSentEvent{Input: wrapperspb.String("sent")},
			}},
			value: func(event *protos.HistoryEvent) *wrapperspb.StringValue {
				return event.GetEventSent().Input
			},
		},
		{
			name: "termination",
			event: &protos.HistoryEvent{EventType: &protos.HistoryEvent_ExecutionTerminated{
				ExecutionTerminated: &protos.ExecutionTerminatedEvent{Input: wrapperspb.String("terminated")},
			}},
			value: func(event *protos.HistoryEvent) *wrapperspb.StringValue {
				return event.GetExecutionTerminated().Input
			},
		},
		{
			name: "continue as new",
			event: &protos.HistoryEvent{EventType: &protos.HistoryEvent_ContinueAsNew{
				ContinueAsNew: &protos.ContinueAsNewEvent{Input: wrapperspb.String("continued")},
			}},
			value: func(event *protos.HistoryEvent) *wrapperspb.StringValue {
				return event.GetContinueAsNew().Input
			},
		},
		{
			name: "rewind",
			event: &protos.HistoryEvent{EventType: &protos.HistoryEvent_ExecutionRewound{
				ExecutionRewound: &protos.ExecutionRewoundEvent{Input: wrapperspb.String("rewound")},
			}},
			value: func(event *protos.HistoryEvent) *wrapperspb.StringValue {
				return event.GetExecutionRewound().Input
			},
		},
		{
			name: "entity signal",
			event: &protos.HistoryEvent{EventType: &protos.HistoryEvent_EntityOperationSignaled{
				EntityOperationSignaled: &protos.EntityOperationSignaledEvent{Input: wrapperspb.String("signal")},
			}},
			value: func(event *protos.HistoryEvent) *wrapperspb.StringValue {
				return event.GetEntityOperationSignaled().Input
			},
		},
		{
			name: "entity call",
			event: &protos.HistoryEvent{EventType: &protos.HistoryEvent_EntityOperationCalled{
				EntityOperationCalled: &protos.EntityOperationCalledEvent{Input: wrapperspb.String("call")},
			}},
			value: func(event *protos.HistoryEvent) *wrapperspb.StringValue {
				return event.GetEntityOperationCalled().Input
			},
		},
		{
			name: "entity result",
			event: &protos.HistoryEvent{EventType: &protos.HistoryEvent_EntityOperationCompleted{
				EntityOperationCompleted: &protos.EntityOperationCompletedEvent{Output: wrapperspb.String("result")},
			}},
			value: func(event *protos.HistoryEvent) *wrapperspb.StringValue {
				return event.GetEntityOperationCompleted().Output
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			original := test.value(test.event).GetValue()
			require.NoError(t, TransformHistoryEvent(context.Background(), options, test.event, true))
			require.NotEqual(t, original, test.value(test.event).GetValue())
			require.NoError(t, TransformHistoryEvent(context.Background(), options, test.event, false))
			require.Equal(t, original, test.value(test.event).GetValue())
		})
	}
}

func TestTransformHistoryStateEventPayloadFields(t *testing.T) {
	store := payload.NewMemoryStore()
	options := &api.LargePayloadOptions{
		Store:           store,
		Resolver:        store,
		ThresholdBytes:  1,
		MaxPayloadBytes: 1024,
	}
	state := &protos.OrchestrationState{
		Input:        wrapperspb.String("input"),
		Output:       wrapperspb.String("output"),
		CustomStatus: wrapperspb.String("status"),
	}
	event := &protos.HistoryEvent{EventType: &protos.HistoryEvent_HistoryState{
		HistoryState: &protos.HistoryStateEvent{OrchestrationState: state},
	}}
	require.NoError(t, TransformHistoryEvent(context.Background(), options, event, true))
	require.NoError(t, TransformHistoryEvent(context.Background(), options, event, false))
	require.Equal(t, "input", state.Input.GetValue())
	require.Equal(t, "output", state.Output.GetValue())
	require.Equal(t, "status", state.CustomStatus.GetValue())
}

func TestTransformEntityBatchPayloadFields(t *testing.T) {
	store := payload.NewMemoryStore()
	options := &api.LargePayloadOptions{
		Store:           store,
		Resolver:        store,
		ThresholdBytes:  1,
		MaxPayloadBytes: 1024,
	}
	request := &protos.EntityBatchRequest{
		EntityState: wrapperspb.String("state"),
		Operations: []*protos.OperationRequest{{
			Input: wrapperspb.String("input"),
		}},
	}
	var err error
	request.EntityState, err = Externalize(context.Background(), options, request.EntityState)
	require.NoError(t, err)
	request.Operations[0].Input, err = Externalize(context.Background(), options, request.Operations[0].Input)
	require.NoError(t, err)
	require.ErrorIs(t, TransformEntityBatchRequest(context.Background(), nil, &protos.EntityBatchRequest{
		EntityState: request.EntityState,
		Operations:  []*protos.OperationRequest{{Input: request.Operations[0].Input}},
	}), api.ErrFeatureNotSupported)
	require.ErrorIs(t, TransformEntityBatchResult(context.Background(), nil, &protos.EntityBatchResult{
		EntityState: request.EntityState,
	}), api.ErrFeatureNotSupported)
	require.NoError(t, TransformEntityBatchRequest(context.Background(), options, request))
	require.Equal(t, "state", request.EntityState.GetValue())
	require.Equal(t, "input", request.Operations[0].Input.GetValue())

	result := &protos.EntityBatchResult{
		EntityState: wrapperspb.String("next-state"),
		Results: []*protos.OperationResult{{
			ResultType: &protos.OperationResult_Success{
				Success: &protos.OperationResultSuccess{Result: wrapperspb.String("output")},
			},
		}},
		Actions: []*protos.OperationAction{
			{
				OperationActionType: &protos.OperationAction_SendSignal{
					SendSignal: &protos.SendSignalAction{Input: wrapperspb.String("signal")},
				},
			},
			{
				OperationActionType: &protos.OperationAction_StartNewOrchestration{
					StartNewOrchestration: &protos.StartNewOrchestrationAction{Input: wrapperspb.String("start")},
				},
			},
		},
	}
	require.NoError(t, TransformEntityBatchResult(context.Background(), options, result))
	for _, value := range []*wrapperspb.StringValue{
		result.EntityState,
		result.Results[0].GetSuccess().Result,
		result.Actions[0].GetSendSignal().Input,
		result.Actions[1].GetStartNewOrchestration().Input,
	} {
		hydrated, err := Hydrate(context.Background(), options, value)
		require.NoError(t, err)
		require.NotEqual(t, value.GetValue(), hydrated.GetValue())
	}
}

type staticResolver struct {
	payload []byte
	err     error
}

func (r staticResolver) Resolve(context.Context, string) ([]byte, error) {
	if r.err != nil {
		return nil, r.err
	}
	return append([]byte(nil), r.payload...), nil
}

func (staticResolver) Store(context.Context, []byte) (string, error) {
	return "", errors.New("not implemented")
}

type nativeStore struct{}

func (nativeStore) Store(context.Context, []byte) (string, error)   { return "", nil }
func (nativeStore) Resolve(context.Context, string) ([]byte, error) { return []byte("payload"), nil }
func (nativeStore) StoreToken(context.Context, []byte) (string, error) {
	return "blob:v2:https://account.example/payload", nil
}
func (nativeStore) ResolveToken(context.Context, string) ([]byte, error) {
	return []byte("payload"), nil
}
func (nativeStore) IsLargePayloadToken(value string) bool {
	return len(value) >= len("blob:v2:") && value[:len("blob:v2:")] == "blob:v2:"
}
func (nativeStore) ValidateLargePayloadToken(value string) error {
	if value != "blob:v2:https://account.example/payload" {
		return api.ErrLargePayloadReference
	}
	return nil
}

type inclusiveNativeStore struct{ nativeStore }

func (inclusiveNativeStore) UsesInclusiveLargePayloadThreshold() bool { return true }
