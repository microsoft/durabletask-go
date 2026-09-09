package payload

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// newAzuriteStore builds a store against a throwaway container so Azurite runs
// exercise the real container-initialization and upload paths.
func newAzuriteStore(t *testing.T, compression *bool) *AzureBlobStore {
	t.Helper()
	connectionString := os.Getenv("AZURITE_CONNECTION_STRING")
	if connectionString == "" {
		t.Skip("set AZURITE_CONNECTION_STRING to run against Azurite")
	}
	var suffix [8]byte
	_, err := rand.Read(suffix[:])
	require.NoError(t, err)
	container := "dtgo" + hex.EncodeToString(suffix[:])
	store, err := NewAzureBlobStore(AzureBlobStoreOptions{
		ConnectionString:   connectionString,
		Container:          container,
		AllowInsecureHTTP:  true,
		CompressionEnabled: compression,
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = store.client.DeleteContainer(context.Background(), container, nil)
	})
	return store
}

func TestAzureBlobStoreAzuriteCompressionModes(t *testing.T) {
	enabled, disabled := true, false
	payload := []byte(strings.Repeat("Azure Blob payload ", 512))
	tests := []struct {
		name        string
		compression *bool
	}{
		{name: "gzip enabled", compression: &enabled},
		{name: "gzip disabled", compression: &disabled},
		{name: "default"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store := newAzuriteStore(t, test.compression)
			token, err := store.StoreToken(context.Background(), payload)
			require.NoError(t, err)
			require.True(t, strings.HasPrefix(token, azureBlobTokenV2))

			resolved, err := store.ResolveToken(context.Background(), token)
			require.NoError(t, err)
			require.Equal(t, payload, resolved)
		})
	}
}

// TestAzureBlobStoreAzuriteResolvesLegacyV1Token pins .NET blob:v1 read
// interoperability against a blob written with the current blob:v2 writer.
func TestAzureBlobStoreAzuriteResolvesLegacyV1Token(t *testing.T) {
	store := newAzuriteStore(t, nil)
	token, err := store.StoreToken(context.Background(), []byte("legacy token payload"))
	require.NoError(t, err)

	name := token[strings.LastIndex(token, "/")+1:]
	legacy := azureBlobTokenV1 + store.container + ":" + name
	resolved, err := store.ResolveToken(context.Background(), legacy)
	require.NoError(t, err)
	require.Equal(t, []byte("legacy token payload"), resolved)
}

// TestAzureBlobStoreAzuriteConcurrentUploadsShareInitialization exercises the
// single-flight container gate against real storage.
func TestAzureBlobStoreAzuriteConcurrentUploadsShareInitialization(t *testing.T) {
	store := newAzuriteStore(t, nil)
	const uploaders = 16
	// Assertions run on the test goroutine: require calls FailNow, which is
	// undefined behavior from a goroutine other than the one running the test,
	// so mismatches are reported as errors on the channel instead.
	results := make(chan error, uploaders)
	for i := 0; i < uploaders; i++ {
		go func(i int) {
			want := strings.Repeat("p", i+1)
			token, err := store.StoreToken(context.Background(), []byte(want))
			if err != nil {
				results <- err
				return
			}
			resolved, err := store.ResolveToken(context.Background(), token)
			if err == nil && string(resolved) != want {
				err = fmt.Errorf("uploader %d resolved %q, want %q", i, resolved, want)
			}
			results <- err
		}(i)
	}
	for i := 0; i < uploaders; i++ {
		require.NoError(t, <-results)
	}
}

// TestAzureBlobStoreAzuriteRecreatesDeletedContainer verifies that an
// out-of-band container deletion is recovered from within a single upload.
func TestAzureBlobStoreAzuriteRecreatesDeletedContainer(t *testing.T) {
	store := newAzuriteStore(t, nil)
	_, err := store.StoreToken(context.Background(), []byte("first"))
	require.NoError(t, err)

	_, err = store.client.DeleteContainer(context.Background(), store.container, nil)
	require.NoError(t, err)

	token, err := store.StoreToken(context.Background(), []byte("second"))
	require.NoError(t, err)
	resolved, err := store.ResolveToken(context.Background(), token)
	require.NoError(t, err)
	require.Equal(t, []byte("second"), resolved)
}

// TestAzureBlobStoreAzuriteMissingBlobIsAReferenceError pins the error surface
// for a token whose blob no longer exists.
func TestAzureBlobStoreAzuriteMissingBlobIsAReferenceError(t *testing.T) {
	store := newAzuriteStore(t, nil)
	token, err := store.StoreToken(context.Background(), []byte("payload"))
	require.NoError(t, err)
	_, err = store.client.DeleteBlob(
		context.Background(),
		store.container,
		token[strings.LastIndex(token, "/")+1:],
		nil,
	)
	require.NoError(t, err)

	_, err = store.ResolveToken(context.Background(), token)
	require.Error(t, err)
}
