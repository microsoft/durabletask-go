package api

import (
	"errors"
	"testing"

	"github.com/microsoft/durabletask-go/internal/protos"
	"github.com/stretchr/testify/require"
)

type recordingConverter struct {
	serialized   []any
	deserialized []string
	err          error
}

func (c *recordingConverter) Serialize(value any) (string, error) {
	c.serialized = append(c.serialized, value)
	if c.err != nil {
		return "", c.err
	}
	return "converted", nil
}

func (c *recordingConverter) Deserialize(payload string, target any) error {
	c.deserialized = append(c.deserialized, payload)
	if c.err != nil {
		return c.err
	}
	if value, ok := target.(*string); ok {
		*value = payload
	}
	return nil
}

func TestJSONDataConverterMatchesExistingPayloadFormat(t *testing.T) {
	converter := DefaultDataConverter()
	payload, err := converter.Serialize(struct {
		Value string `json:"value"`
	}{Value: "test"})
	require.NoError(t, err)
	require.Equal(t, `{"value":"test"}`, payload)

	var result struct {
		Value string `json:"value"`
	}
	require.NoError(t, converter.Deserialize(payload, &result))
	require.Equal(t, "test", result.Value)
}

func TestPayloadOptionsUseConfiguredConverterAndRawOptionsBypassIt(t *testing.T) {
	converter := new(recordingConverter)

	create := new(protos.CreateInstanceRequest)
	require.NoError(t, WithInput("value")(create, converter))
	require.Equal(t, "converted", create.Input.GetValue())

	event := new(protos.RaiseEventRequest)
	require.NoError(t, WithEventPayload("value")(event, converter))
	require.Equal(t, "converted", event.Input.GetValue())

	terminate := new(protos.TerminateRequest)
	require.NoError(t, WithOutput("value")(terminate, converter))
	require.Equal(t, "converted", terminate.Output.GetValue())

	signal := new(protos.SignalEntityRequest)
	require.NoError(t, WithSignalInput("value")(signal, converter))
	require.Equal(t, "converted", signal.Input.GetValue())
	require.Len(t, converter.serialized, 4)

	failing := &recordingConverter{err: errors.New("must not be called")}
	require.NoError(t, WithRawInput("raw")(create, failing))
	require.NoError(t, WithRawEventData("raw")(event, failing))
	require.NoError(t, WithRawOutput("raw")(terminate, failing))
	require.NoError(t, WithRawSignalInput("raw")(signal, failing))
	require.Empty(t, failing.serialized)
}

func TestEntityTypedNilInputIsAbsent(t *testing.T) {
	var input *int
	request := new(protos.SignalEntityRequest)
	require.NoError(t, WithSignalInput(input)(request, DefaultDataConverter()))
	require.Nil(t, request.Input)
}

func TestMetadataUsesConfiguredConverter(t *testing.T) {
	converter := new(recordingConverter)
	metadata := &OrchestrationMetadata{
		SerializedInput:        "input",
		SerializedOutput:       "output",
		SerializedCustomStatus: "status",
		Converter:              converter,
	}
	var value string
	require.NoError(t, metadata.ReadInput(&value))
	require.Equal(t, "input", value)
	require.NoError(t, metadata.ReadOutput(&value))
	require.Equal(t, "output", value)
	require.NoError(t, metadata.ReadCustomStatus(&value))
	require.Equal(t, "status", value)

	entity := &EntityMetadata{
		StateIncluded:   true,
		HasState:        true,
		SerializedState: "state",
		Converter:       converter,
	}
	require.NoError(t, entity.ReadState(&value))
	require.Equal(t, "state", value)
	require.Equal(t, []string{"input", "output", "status", "state"}, converter.deserialized)
}

func TestConverterErrorsAreReturnedWithoutJSONFallback(t *testing.T) {
	expected := errors.New("converter failed")
	converter := &recordingConverter{err: expected}
	req := new(protos.CreateInstanceRequest)
	require.ErrorIs(t, WithInput(make(chan int))(req, converter), expected)
	require.Nil(t, req.Input)
}

type emptyDataConverter struct{}

func (emptyDataConverter) Serialize(any) (string, error) {
	return "", nil
}

func (emptyDataConverter) Deserialize(string, any) error {
	return nil
}

func TestEmptyTypedEncodingIsRejected(t *testing.T) {
	req := new(protos.CreateInstanceRequest)
	require.ErrorContains(t, WithInput("value")(req, emptyDataConverter{}), "empty payload")
	require.Nil(t, req.Input)
}
