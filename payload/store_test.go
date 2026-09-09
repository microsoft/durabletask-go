package payload

import (
	"context"
	"testing"

	"github.com/microsoft/durabletask-go/api"
	"github.com/stretchr/testify/require"
)

func TestMemoryStoreRoundTripCopiesPayload(t *testing.T) {
	store := NewMemoryStore()
	original := []byte("payload")
	location, err := store.Store(context.Background(), original)
	require.NoError(t, err)
	original[0] = 'X'

	resolved, err := store.Resolve(context.Background(), location)
	require.NoError(t, err)
	require.Equal(t, []byte("payload"), resolved)
	resolved[0] = 'Y'

	again, err := store.Resolve(context.Background(), location)
	require.NoError(t, err)
	require.Equal(t, []byte("payload"), again)
}

func TestFileStoreRoundTripAndLimit(t *testing.T) {
	store, err := NewFileStore(t.TempDir(), 8)
	require.NoError(t, err)
	location, err := store.Store(context.Background(), []byte("payload"))
	require.NoError(t, err)

	resolved, err := store.Resolve(context.Background(), location)
	require.NoError(t, err)
	require.Equal(t, []byte("payload"), resolved)

	_, err = store.Store(context.Background(), []byte("too-large"))
	require.ErrorIs(t, err, api.ErrLargePayloadTooLarge)
	_, err = store.Resolve(context.Background(), "file://sha256/not-a-hash")
	require.Error(t, err)
}
