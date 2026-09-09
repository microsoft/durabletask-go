package client

import (
	"testing"

	"github.com/google/uuid"
	"github.com/microsoft/durabletask-go/internal/helpers"
	"github.com/microsoft/durabletask-go/internal/protos"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

func TestEntityBatchFromRequestV2RejectsMissingParentDestination(t *testing.T) {
	requestID := uuid.NewString()
	_, _, err := entityBatchFromRequestV2(&protos.EntityRequest{
		InstanceId: "@counter@key",
		OperationRequests: []*protos.HistoryEvent{{
			EventType: &protos.HistoryEvent_EntityOperationCalled{
				EntityOperationCalled: &protos.EntityOperationCalledEvent{
					RequestId: requestID,
					Operation: "get",
				},
			},
		}},
	})
	require.ErrorContains(t, err, "missing its response destination")
}

func TestEntityBatchFromRequestV2Validation(t *testing.T) {
	t.Run("nil-request", func(t *testing.T) {
		_, _, err := entityBatchFromRequestV2(nil)
		require.ErrorContains(t, err, "entity request must not be nil")
	})

	t.Run("invalid-instance-id", func(t *testing.T) {
		_, _, err := entityBatchFromRequestV2(&protos.EntityRequest{InstanceId: "not-an-entity"})
		require.ErrorContains(t, err, "invalid entity instance ID")
	})

	t.Run("nil-operation-event", func(t *testing.T) {
		_, _, err := entityBatchFromRequestV2(&protos.EntityRequest{
			InstanceId:        "@counter@key",
			OperationRequests: []*protos.HistoryEvent{nil},
		})
		require.ErrorContains(t, err, "entity operation history event must not be nil")
	})

	t.Run("invalid-signal-request-id", func(t *testing.T) {
		_, _, err := entityBatchFromRequestV2(&protos.EntityRequest{
			InstanceId: "@counter@key",
			OperationRequests: []*protos.HistoryEvent{{
				EventType: &protos.HistoryEvent_EntityOperationSignaled{
					EntityOperationSignaled: &protos.EntityOperationSignaledEvent{
						RequestId: "not-a-guid",
						Operation: "add",
					},
				},
			}},
		})
		require.ErrorContains(t, err, "invalid entity signal request ID")
	})

	t.Run("invalid-call-request-id", func(t *testing.T) {
		_, _, err := entityBatchFromRequestV2(&protos.EntityRequest{
			InstanceId: "@counter@key",
			OperationRequests: []*protos.HistoryEvent{{
				EventType: &protos.HistoryEvent_EntityOperationCalled{
					EntityOperationCalled: &protos.EntityOperationCalledEvent{
						RequestId: "not-a-guid",
						Operation: "get",
					},
				},
			}},
		})
		require.ErrorContains(t, err, "invalid entity call request ID")
	})

	t.Run("unsupported-history-event", func(t *testing.T) {
		_, _, err := entityBatchFromRequestV2(&protos.EntityRequest{
			InstanceId: "@counter@key",
			OperationRequests: []*protos.HistoryEvent{{
				EventType: &protos.HistoryEvent_TimerCreated{
					TimerCreated: &protos.TimerCreatedEvent{},
				},
			}},
		})
		require.ErrorContains(t, err, "unsupported entity operation history event")
	})
}

// Signals are marked in batch properties so the executor can skip building a
// response for them, and calls carry a routable response destination.
func TestEntityBatchFromRequestV2RoutesSignalsAndCalls(t *testing.T) {
	signalID := uuid.NewString()
	callID := uuid.NewString()
	batch, infos, err := entityBatchFromRequestV2(&protos.EntityRequest{
		InstanceId:  "@counter@key",
		EntityState: wrapperspb.String("7"),
		OperationRequests: []*protos.HistoryEvent{
			{
				EventType: &protos.HistoryEvent_EntityOperationSignaled{
					EntityOperationSignaled: &protos.EntityOperationSignaledEvent{
						RequestId: signalID,
						Operation: "add",
					},
				},
			},
			{
				EventType: &protos.HistoryEvent_EntityOperationCalled{
					EntityOperationCalled: &protos.EntityOperationCalledEvent{
						RequestId:         callID,
						Operation:         "get",
						ParentInstanceId:  wrapperspb.String("parent-instance"),
						ParentExecutionId: wrapperspb.String("parent-execution"),
					},
				},
			},
		},
	})
	require.NoError(t, err)
	require.Equal(t, "7", batch.EntityState.GetValue())
	require.Len(t, batch.Operations, 2)
	require.True(t, batch.Properties[helpers.EntitySignalProperty(signalID)].GetBoolValue())
	require.NotContains(t, batch.Properties, helpers.EntitySignalProperty(callID))

	require.Len(t, infos, 2)
	require.Nil(t, infos[0].ResponseDestination)
	require.Equal(t, "parent-instance", infos[1].ResponseDestination.GetInstanceId())
	require.Equal(t, "parent-execution", infos[1].ResponseDestination.GetExecutionId().GetValue())
}
