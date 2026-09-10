package payload

import (
	"context"
	"os"
	"path/filepath"
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

func TestFileStoreRejectsCorruptedContent(t *testing.T) {
	for _, corrupted := range []string{"changed", "short", "", "payload with appended bytes"} {
		t.Run(corrupted, func(t *testing.T) {
			store, err := NewFileStore(t.TempDir(), 64)
			require.NoError(t, err)
			original := []byte("payload")
			location, err := store.Store(context.Background(), original)
			require.NoError(t, err)
			hash, err := parseFileLocation(location)
			require.NoError(t, err)
			require.NoError(t, os.WriteFile(filepath.Join(store.root, hash+".payload"), []byte(corrupted), 0o600))

			resolved, err := store.Resolve(context.Background(), location)
			require.ErrorIs(t, err, api.ErrLargePayloadIntegrity)
			require.Nil(t, resolved)
			reused, err := store.Store(context.Background(), original)
			require.ErrorIs(t, err, api.ErrLargePayloadIntegrity)
			require.Empty(t, reused)
		})
	}
}

func TestFileStoreVerifiedReuseAndCancellation(t *testing.T) {
	store, err := NewFileStore(t.TempDir())
	require.NoError(t, err)
	for _, content := range []string{"", "payload"} {
		location, err := store.Store(context.Background(), []byte(content))
		require.NoError(t, err)
		reused, err := store.Store(context.Background(), []byte(content))
		require.NoError(t, err)
		require.Equal(t, location, reused)
		resolved, err := store.Resolve(context.Background(), location)
		require.NoError(t, err)
		require.Equal(t, content, string(resolved))

		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		resolved, err = store.Resolve(ctx, location)
		require.ErrorIs(t, err, context.Canceled)
		require.Nil(t, resolved)
	}
}
