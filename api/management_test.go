package api

import (
	"context"
	"testing"

	"github.com/microsoft/durabletask-go/internal/protos"
	"github.com/microsoft/durabletask-go/internal/tagcodec"
	"github.com/stretchr/testify/require"
)

func TestWithTagsMergesAndRejectsReservedKeys(t *testing.T) {
	req := &protos.CreateInstanceRequest{Tags: map[string]string{"existing": "value"}}
	require.NoError(t, WithTags(map[string]string{"team": "durable"})(req, DefaultDataConverter()))
	require.Equal(t, "value", req.Tags["existing"])
	require.Equal(t, "durable", req.Tags["team"])
	require.Equal(t, "1", req.Tags[tagcodec.ContextEncodingTag])
	require.Error(t, WithTags(map[string]string{ReservedContextFieldPrefix + "name": "invalid"})(req, DefaultDataConverter()))
}

func TestNormalizeLargePayloadOptions(t *testing.T) {
	store := testPayloadStore{}
	normalized, err := NormalizeLargePayloadOptions(&LargePayloadOptions{
		Store:    store,
		Resolver: store,
	})
	require.NoError(t, err)
	require.Equal(t, DefaultLargePayloadThresholdBytes, normalized.ThresholdBytes)
	require.Equal(t, DefaultLargePayloadMaxBytes, normalized.MaxPayloadBytes)

	_, err = NormalizeLargePayloadOptions(&LargePayloadOptions{})
	require.ErrorIs(t, err, ErrInvalidArgument)
}

type testPayloadStore struct{}

func (testPayloadStore) Store(context.Context, []byte) (string, error) {
	return "test", nil
}

func (testPayloadStore) Resolve(context.Context, string) ([]byte, error) {
	return nil, nil
}
