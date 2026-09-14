package api

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNormalizeHistoryQuery(t *testing.T) {
	normalized, err := NormalizeHistoryQuery(HistoryQuery{})
	require.NoError(t, err)
	require.Equal(t, DefaultHistoryMaxEvents, normalized.MaxEvents)

	_, err = NormalizeHistoryQuery(HistoryQuery{MaxEvents: -1})
	require.ErrorIs(t, err, ErrInvalidArgument)

	_, err = NormalizeHistoryQuery(HistoryQuery{MaxEvents: MaxHistoryMaxEvents + 1})
	require.ErrorIs(t, err, ErrInvalidArgument)

	_, err = NormalizeHistoryQuery(HistoryQuery{MaxBytes: MaxHistoryMaxBytes + 1})
	require.ErrorIs(t, err, ErrInvalidArgument)
}

func TestHistoryEventTypedPayloadReaders(t *testing.T) {
	converter := historyTestConverter{}
	input := &HistoryEvent{
		Type:             HistoryEventExecutionStarted,
		ExecutionStarted: &HistoryExecutionStartedEvent{SerializedInput: "input"},
		Converter:        converter,
	}
	var decoded string
	require.NoError(t, input.ReadInput(&decoded))
	require.Equal(t, "decoded:input", decoded)

	result := &HistoryEvent{
		Type:      HistoryEventEntityOperationCompleted,
		Entity:    &HistoryEntityEvent{SerializedOutput: "output"},
		Converter: converter,
	}
	require.NoError(t, result.ReadResult(&decoded))
	require.Equal(t, "decoded:output", decoded)

	generic := &HistoryEvent{
		Type:      HistoryEventGeneric,
		Generic:   &HistoryPayloadEvent{SerializedInput: "data"},
		Converter: converter,
	}
	require.NoError(t, generic.ReadData(&decoded))
	require.Equal(t, "decoded:data", decoded)

	require.NoError(t, (&HistoryEvent{}).ReadInput(&decoded))
	require.NoError(t, (*HistoryEvent)(nil).ReadResult(&decoded))
}

type historyTestConverter struct{}

func (historyTestConverter) Serialize(any) (string, error) {
	return "", errors.New("not implemented")
}

func (historyTestConverter) Deserialize(payload string, target any) error {
	value, ok := target.(*string)
	if !ok {
		return errors.New("expected *string")
	}
	*value = "decoded:" + payload
	return nil
}
