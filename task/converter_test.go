package task

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/gob"
	"fmt"
	"testing"

	"github.com/microsoft/durabletask-go/api"
	"github.com/microsoft/durabletask-go/internal/helpers"
	"github.com/microsoft/durabletask-go/internal/protos"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

type gobDataConverter struct{}

func (gobDataConverter) Serialize(value any) (string, error) {
	var buffer bytes.Buffer
	if err := gob.NewEncoder(&buffer).Encode(value); err != nil {
		return "", err
	}
	return base64.RawStdEncoding.EncodeToString(buffer.Bytes()), nil
}

func (gobDataConverter) Deserialize(payload string, target any) error {
	data, err := base64.RawStdEncoding.DecodeString(payload)
	if err != nil {
		return err
	}
	return gob.NewDecoder(bytes.NewReader(data)).Decode(target)
}

type converterPayload struct {
	Value int
}

func TestDataConverterCoversOrchestrationActivityEventsAndContinueAsNew(t *testing.T) {
	converter := gobDataConverter{}
	registry := NewTaskRegistry()
	require.NoError(t, registry.AddOrchestratorN("converter", func(ctx *OrchestrationContext) (any, error) {
		var input converterPayload
		if err := ctx.GetInput(&input); err != nil {
			return nil, err
		}
		if input.Value == 1 {
			if err := ctx.SetCustomStatusValue(converterPayload{Value: 2}); err != nil {
				return nil, err
			}
			ctx.CallActivity("activity", WithActivityInput(converterPayload{Value: 3}))
			return converterPayload{Value: 4}, nil
		}
		ctx.ContinueAsNew(converterPayload{Value: input.Value + 1})
		return nil, nil
	}))
	require.NoError(t, registry.AddOrchestratorN("event", func(ctx *OrchestrationContext) (any, error) {
		var payload converterPayload
		if err := ctx.WaitForSingleEvent("signal", -1).Await(&payload); err != nil {
			return nil, err
		}
		return payload, nil
	}))

	executor := NewTaskExecutor(registry, WithDataConverter(converter))
	input := mustConvert(t, converter, converterPayload{Value: 1})
	result, err := executor.ExecuteOrchestrator(
		context.Background(),
		"converter-instance",
		nil,
		[]*protos.HistoryEvent{helpers.NewExecutionStartedEvent(
			"converter",
			"converter-instance",
			wrapperspb.String(input),
			nil,
			nil,
			nil,
		)}, nil)

	require.NoError(t, err)
	requireConvertedValue(t, converter, result.Response.CustomStatus.GetValue(), 2)
	for _, action := range result.Response.Actions {
		switch {
		case action.GetScheduleTask() != nil:
			requireConvertedValue(t, converter, action.GetScheduleTask().GetInput().GetValue(), 3)
		case action.GetCompleteOrchestration() != nil:
			requireConvertedValue(t, converter, action.GetCompleteOrchestration().GetResult().GetValue(), 4)
		}
	}

	continueInput := mustConvert(t, converter, converterPayload{Value: 10})
	result, err = executor.ExecuteOrchestrator(
		context.Background(),
		"continue-instance",
		nil,
		[]*protos.HistoryEvent{helpers.NewExecutionStartedEvent(
			"converter",
			"continue-instance",
			wrapperspb.String(continueInput),
			nil,
			nil,
			nil,
		)}, nil)

	require.NoError(t, err)
	requireConvertedValue(t, converter, completionAction(t, result.Response).GetResult().GetValue(), 11)

	eventPayload := mustConvert(t, converter, converterPayload{Value: 20})
	result, err = executor.ExecuteOrchestrator(
		context.Background(),
		"event-instance",
		nil,
		[]*protos.HistoryEvent{
			helpers.NewExecutionStartedEvent("event", "event-instance", nil, nil, nil, nil),
			helpers.NewEventRaisedEvent("signal", wrapperspb.String(eventPayload)),
		}, nil)

	require.NoError(t, err)
	requireConvertedValue(t, converter, completionAction(t, result.Response).GetResult().GetValue(), 20)
}

func TestDataConverterCoversActivityAndEntityExecution(t *testing.T) {
	converter := gobDataConverter{}
	registry := NewTaskRegistry()
	require.NoError(t, registry.AddActivityN("activity", func(ctx ActivityContext) (any, error) {
		var input converterPayload
		if err := ctx.GetInput(&input); err != nil {
			return nil, err
		}
		return converterPayload{Value: input.Value + 1}, nil
	}))
	require.NoError(t, registry.AddEntityN("counter", func(ctx *EntityContext) (any, error) {
		var state converterPayload
		if ctx.HasState() {
			if err := ctx.GetState(&state); err != nil {
				return nil, err
			}
		}
		var input converterPayload
		if err := ctx.GetInput(&input); err != nil {
			return nil, err
		}
		state.Value += input.Value
		if err := ctx.SetState(state); err != nil {
			return nil, err
		}
		return state, nil
	}))
	executor := NewTaskExecutor(registry, WithDataConverter(converter))

	activity, err := executor.ExecuteActivity(
		context.Background(),
		"instance",
		helpers.NewTaskScheduledEvent(
			1,
			"activity",
			nil,
			wrapperspb.String(mustConvert(t, converter, converterPayload{Value: 4})),
			nil,
		),
	)
	require.NoError(t, err)
	requireConvertedValue(t, converter, activity.GetTaskCompleted().GetResult().GetValue(), 5)

	entity, err := executor.(backendEntityExecutor).ExecuteEntity(context.Background(), &protos.EntityBatchRequest{
		InstanceId:  api.NewEntityID("counter", "one").String(),
		EntityState: wrapperspb.String(mustConvert(t, converter, converterPayload{Value: 5})),
		Operations: []*protos.OperationRequest{{
			Operation: "add",
			Input:     wrapperspb.String(mustConvert(t, converter, converterPayload{Value: 7})),
		}},
	})
	require.NoError(t, err)
	requireConvertedValue(t, converter, entity.EntityState.GetValue(), 12)
	requireConvertedValue(t, converter, entity.Results[0].GetSuccess().GetResult().GetValue(), 12)
}

type backendEntityExecutor interface {
	ExecuteEntity(context.Context, *protos.EntityBatchRequest) (*protos.EntityBatchResult, error)
}

func mustConvert(t *testing.T, converter api.DataConverter, value any) string {
	t.Helper()
	payload, err := converter.Serialize(value)
	require.NoError(t, err)
	return payload
}

func requireConvertedValue(t *testing.T, converter api.DataConverter, payload string, expected int) {
	t.Helper()
	var value converterPayload
	require.NoError(t, converter.Deserialize(payload, &value), fmt.Sprintf("payload %q", payload))
	require.Equal(t, expected, value.Value)
}
