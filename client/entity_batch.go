package client

import (
	"fmt"

	"github.com/google/uuid"
	"github.com/microsoft/durabletask-go/api"
	"github.com/microsoft/durabletask-go/internal/helpers"
	"github.com/microsoft/durabletask-go/internal/protos"
	"google.golang.org/protobuf/types/known/structpb"
)

// entityBatchFromRequestV2 converts a scheduler-dispatched V2 entity request
// into the executor-facing batch model and response routing metadata.
func entityBatchFromRequestV2(request *protos.EntityRequest) (*protos.EntityBatchRequest, []*protos.OperationInfo, error) {
	if request == nil {
		return nil, nil, fmt.Errorf("entity request must not be nil")
	}
	if _, err := api.EntityIDFromString(request.InstanceId); err != nil {
		return nil, nil, fmt.Errorf("invalid entity instance ID: %w", err)
	}
	batch := &protos.EntityBatchRequest{
		InstanceId:  request.InstanceId,
		EntityState: request.EntityState,
		Operations:  make([]*protos.OperationRequest, 0, len(request.OperationRequests)),
		Properties:  make(map[string]*structpb.Value),
	}
	operationInfos := make([]*protos.OperationInfo, 0, len(request.OperationRequests))
	for _, historyEvent := range request.OperationRequests {
		if historyEvent == nil {
			return nil, nil, fmt.Errorf("entity operation history event must not be nil")
		}
		switch {
		case historyEvent.GetEntityOperationSignaled() != nil:
			event := historyEvent.GetEntityOperationSignaled()
			if _, err := uuid.Parse(event.RequestId); err != nil {
				return nil, nil, fmt.Errorf("invalid entity signal request ID %q: %w", event.RequestId, err)
			}
			batch.Operations = append(batch.Operations, &protos.OperationRequest{
				Operation: event.Operation,
				RequestId: event.RequestId,
				Input:     event.Input,
			})
			batch.Properties[helpers.EntitySignalProperty(event.RequestId)] = structpb.NewBoolValue(true)
			operationInfos = append(operationInfos, &protos.OperationInfo{RequestId: event.RequestId})
		case historyEvent.GetEntityOperationCalled() != nil:
			event := historyEvent.GetEntityOperationCalled()
			if _, err := uuid.Parse(event.RequestId); err != nil {
				return nil, nil, fmt.Errorf("invalid entity call request ID %q: %w", event.RequestId, err)
			}
			if event.ParentInstanceId.GetValue() == "" {
				return nil, nil, fmt.Errorf("entity call %q is missing its response destination", event.RequestId)
			}
			batch.Operations = append(batch.Operations, &protos.OperationRequest{
				Operation: event.Operation,
				RequestId: event.RequestId,
				Input:     event.Input,
			})
			info := &protos.OperationInfo{
				RequestId: event.RequestId,
				ResponseDestination: &protos.OrchestrationInstance{
					InstanceId:  event.ParentInstanceId.GetValue(),
					ExecutionId: event.ParentExecutionId,
				},
			}
			operationInfos = append(operationInfos, info)
		default:
			return nil, nil, fmt.Errorf("unsupported entity operation history event")
		}
	}
	if len(batch.Properties) == 0 {
		batch.Properties = nil
	}
	return batch, operationInfos, nil
}
