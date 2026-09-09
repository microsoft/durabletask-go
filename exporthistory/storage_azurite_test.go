package exporthistory

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"io"
	"os"
	"strings"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/microsoft/durabletask-go/api"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newAzuriteExportStore builds a store against a throwaway container so Azurite
// runs exercise the real container-initialization and upload paths.
func newAzuriteExportStore(t *testing.T) (*AzureBlobHistoryStore, string) {
	t.Helper()
	connectionString := os.Getenv("AZURITE_CONNECTION_STRING")
	if connectionString == "" {
		t.Skip("set AZURITE_CONNECTION_STRING to run against Azurite")
	}
	container := randomContainerName(t)
	store, err := NewAzureBlobHistoryStore(AzureBlobHistoryStoreOptions{
		ConnectionString:  connectionString,
		ContainerName:     container,
		AllowInsecureHTTP: true,
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = store.client.DeleteContainer(context.Background(), container, nil)
	})
	return store, container
}

func randomContainerName(t *testing.T) string {
	t.Helper()
	var suffix [8]byte
	_, err := rand.Read(suffix[:])
	require.NoError(t, err)
	return "dtgoexport" + hex.EncodeToString(suffix[:])
}

// TestAzureBlobHistoryStoreAzuriteWritesCompressedObjects covers the production write
// path against real storage, including container creation, headers, metadata,
// and overwrite-on-reexport.
func TestAzureBlobHistoryStoreAzuriteWritesCompressedObjects(t *testing.T) {
	store, container := newAzuriteExportStore(t)
	ctx := context.Background()

	events := []*api.HistoryEvent{
		{Type: api.HistoryEventExecutionStarted},
		{Type: api.HistoryEventExecutionCompleted},
	}
	content, contentType, err := serializeHistory(events, DefaultExportFormat())
	require.NoError(t, err)
	object := ExportObject{
		Container:   container,
		Name:        "batch-job/" + strings.Repeat("a", 64) + ".jsonl.gz",
		Content:     content,
		ContentType: contentType,
		Metadata:    map[string]string{"instanceId": "instance-1", "schemaVersion": DefaultSchemaVersion},
	}
	require.NoError(t, store.Write(ctx, object))

	properties := azuriteBlobProperties(t, store, container, object.Name)
	// The object is stored as an opaque gzip file, so no content coding is set
	// and no client transparently decompresses it.
	assert.Equal(t, "application/gzip", derefString(properties.ContentType))
	assert.Empty(t, derefString(properties.ContentEncoding))
	assert.Equal(t, "instance-1", metadataValue(properties.Metadata, "instanceId"))
	assert.Equal(t, DefaultSchemaVersion, metadataValue(properties.Metadata, "schemaVersion"))

	// Downloading always yields exactly the gzip bytes that were uploaded.
	downloaded := downloadAzuriteBlob(t, store, container, object.Name)
	assert.Equal(t, content, downloaded)

	decompressed := decompressGzip(t, downloaded)
	lines := strings.Split(strings.TrimRight(string(decompressed), "\n"), "\n")
	require.Len(t, lines, 2)
	assert.Contains(t, lines[0], string(api.HistoryEventExecutionStarted))
	assert.Contains(t, lines[1], string(api.HistoryEventExecutionCompleted))

	// Re-exporting the same instance overwrites its object rather than failing.
	object.Content = []byte("second write")
	object.ContentType = "text/plain"
	require.NoError(t, store.Write(ctx, object))
	assert.Equal(t, []byte("second write"), downloadAzuriteBlob(t, store, container, object.Name))
}

// TestAzureBlobHistoryStoreAzuriteRejectsDisallowedContainer keeps a job from writing
// outside the containers the worker was configured for, against real storage.
func TestAzureBlobHistoryStoreAzuriteRejectsDisallowedContainer(t *testing.T) {
	store, _ := newAzuriteExportStore(t)
	err := store.Write(context.Background(), ExportObject{
		Container: randomContainerName(t),
		Name:      "object.jsonl.gz",
		Content:   []byte("payload"),
	})
	require.ErrorIs(t, err, ErrValidation)
	assert.Contains(t, err.Error(), "is not allowed by this worker")
}

// TestAzureBlobHistoryStoreAzuriteWritesAcrossAllowedContainers covers the allow-list
// and per-container initialization against real storage.
func TestAzureBlobHistoryStoreAzuriteWritesAcrossAllowedContainers(t *testing.T) {
	connectionString := os.Getenv("AZURITE_CONNECTION_STRING")
	if connectionString == "" {
		t.Skip("set AZURITE_CONNECTION_STRING to run against Azurite")
	}
	primary := randomContainerName(t)
	secondary := randomContainerName(t)
	store, err := NewAzureBlobHistoryStore(AzureBlobHistoryStoreOptions{
		ConnectionString:  connectionString,
		ContainerName:     primary,
		AllowedContainers: []string{secondary},
		AllowInsecureHTTP: true,
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = store.client.DeleteContainer(context.Background(), primary, nil)
		_, _ = store.client.DeleteContainer(context.Background(), secondary, nil)
	})

	ctx := context.Background()
	for _, container := range []string{primary, secondary} {
		require.NoError(t, store.Write(ctx, ExportObject{
			Container: container,
			Name:      "prefix/object.json",
			Content:   []byte(`[]`),
			Metadata:  map[string]string{"instanceId": "instance-1"},
		}), container)
		assert.Equal(t, []byte(`[]`), downloadAzuriteBlob(t, store, container, "prefix/object.json"))
	}
}

func downloadAzuriteBlob(t *testing.T, store *AzureBlobHistoryStore, container, name string) []byte {
	t.Helper()
	response, err := store.client.DownloadStream(context.Background(), container, name, nil)
	require.NoError(t, err)
	defer func() { require.NoError(t, response.Body.Close()) }()
	payload, err := io.ReadAll(response.Body)
	require.NoError(t, err)
	return payload
}

func azuriteBlobProperties(
	t *testing.T,
	store *AzureBlobHistoryStore,
	container string,
	name string,
) blob.GetPropertiesResponse {
	t.Helper()
	blobClient := store.client.ServiceClient().NewContainerClient(container).NewBlobClient(name)
	properties, err := blobClient.GetProperties(context.Background(), nil)
	require.NoError(t, err)
	return properties
}

// metadataValue reads a blob metadata entry case-insensitively, since Azure
// normalizes metadata key casing.
func metadataValue(metadata map[string]*string, key string) string {
	for name, value := range metadata {
		if strings.EqualFold(name, key) {
			return derefString(value)
		}
	}
	return ""
}

func derefString(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}
