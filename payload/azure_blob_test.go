package payload

import (
	"context"
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/microsoft/durabletask-go/api"
	"github.com/stretchr/testify/require"
)

func newTestAzureBlobStore(t *testing.T) *AzureBlobStore {
	t.Helper()
	store, err := NewAzureBlobStore(AzureBlobStoreOptions{
		AccountURL:        "http://127.0.0.1:10000/devstoreaccount1",
		Credential:        staticCredential{},
		Container:         "mycontainer",
		AllowInsecureHTTP: true,
		AllowedHosts:      []string{"myaccount.blob.core.windows.net"},
	})
	require.NoError(t, err)
	return store
}

func TestAzureBlobStoreGoldenTokenVectors(t *testing.T) {
	data, err := os.ReadFile("testdata/blob_tokens.json")
	require.NoError(t, err)
	var vectors []struct {
		Token     string `json:"token"`
		Container string `json:"container"`
		Name      string `json:"name"`
	}
	require.NoError(t, json.Unmarshal(data, &vectors))

	store := newTestAzureBlobStore(t)
	for _, vector := range vectors {
		t.Run(vector.Token[:7], func(t *testing.T) {
			ref, err := store.parseToken(vector.Token)
			require.NoError(t, err)
			require.Equal(t, vector.Container, ref.container)
			require.Equal(t, vector.Name, ref.name)
		})
	}
}

func TestAzureBlobStoreRejectsUntrustedTokens(t *testing.T) {
	store := newTestAzureBlobStore(t)
	for _, token := range []string{
		"blob:v1:othercontainer:0f8fad5bd9cb469fa16570867728950e",
		"blob:v2:https://user@myaccount.blob.core.windows.net/mycontainer/0f8fad5bd9cb469fa16570867728950e",
		"blob:v2:https://myaccount.blob.core.windows.net/mycontainer/0f8fad5bd9cb469fa16570867728950e?sig=x",
		"blob:v2:https://myaccount.blob.core.windows.net/mycontainer/0f8fad5bd9cb469fa16570867728950e#fragment",
		"blob:v2:https://untrusted.blob.core.windows.net/mycontainer/0f8fad5bd9cb469fa16570867728950e",
	} {
		require.ErrorIs(t, store.ValidateLargePayloadToken(token), api.ErrLargePayloadReference)
	}
}

func TestAzureBlobStoreAllowsExplicitCrossAccountHost(t *testing.T) {
	store, err := NewAzureBlobStore(AzureBlobStoreOptions{
		AccountURL:   "https://account.blob.core.windows.net",
		Credential:   staticCredential{},
		Container:    "mycontainer",
		AllowedHosts: []string{"other.blob.core.windows.net"},
	})
	require.NoError(t, err)
	require.NoError(t, store.ValidateLargePayloadToken(
		"blob:v2:https://other.blob.core.windows.net/othercontainer/0f8fad5bd9cb469fa16570867728950e",
	))
	require.ErrorIs(t, store.ValidateLargePayloadToken(
		"blob:v2:https://untrusted.blob.core.windows.net/othercontainer/0f8fad5bd9cb469fa16570867728950e",
	), api.ErrLargePayloadReference)
}

func TestAzureBlobStoreAcceptsConfiguredHostAndSASConnectionString(t *testing.T) {
	store, err := NewAzureBlobStore(AzureBlobStoreOptions{
		AccountURL: "https://account.blob.core.windows.net",
		Credential: staticCredential{},
		Container:  "mycontainer",
	})
	require.NoError(t, err)
	require.NoError(t, store.ValidateLargePayloadToken(
		"blob:v2:https://account.blob.core.windows.net/mycontainer/0f8fad5bd9cb469fa16570867728950e",
	))
	require.NoError(t, store.ValidateLargePayloadToken(
		"blob:v2:https://account.blob.core.windows.net:443/mycontainer/0f8fad5bd9cb469fa16570867728950e",
	))

	_, err = NewAzureBlobStore(AzureBlobStoreOptions{
		ConnectionString: "BlobEndpoint=https://account.blob.core.windows.net;SharedAccessSignature=sv=2024-01-01&sig=example",
		Container:        "mycontainer",
	})
	require.NoError(t, err)
}

func TestAzureBlobStoreDefaults(t *testing.T) {
	store := newTestAzureBlobStore(t)
	threshold, max := store.LargePayloadDefaults()
	require.Equal(t, api.DefaultAzureBlobPayloadThresholdBytes, threshold)
	require.Equal(t, api.DefaultAzureBlobPayloadMaxBytes, max)
	require.True(t, store.UsesInclusiveLargePayloadThreshold())
	require.True(t, store.compressionEnabled)
}

func TestAzureBlobStoreIntegrityMetadata(t *testing.T) {
	payload := []byte("payload")
	size, hash := "7", "239f59ed55e737c77147cf55ad0c1b030b6d7ee748a7426952f9b852d5a935e5"
	require.NoError(t, verifyMetadata(payload, map[string]*string{
		"Durabletask_Size": &size, "DURABLETASK_SHA256": &hash,
	}))
	require.NoError(t, verifyMetadata(payload, nil), ".NET blobs have no Go metadata")
	hash = "not-a-hash"
	require.ErrorIs(t, verifyMetadata(payload, map[string]*string{
		"durabletask_size": &size, "durabletask_sha256": &hash,
	}), api.ErrLargePayloadIntegrity)
}

func TestAzureBlobStoreAzuriteRoundTrip(t *testing.T) {
	connectionString := os.Getenv("AZURITE_CONNECTION_STRING")
	if connectionString == "" {
		t.Skip("set AZURITE_CONNECTION_STRING to run against Azurite")
	}
	store, err := NewAzureBlobStore(AzureBlobStoreOptions{
		ConnectionString: connectionString, Container: "durabletaskgotest", AllowInsecureHTTP: true,
	})
	require.NoError(t, err)
	token, err := store.StoreToken(context.Background(), []byte("Azure Blob payload"))
	require.NoError(t, err)
	require.Regexp(t, `^blob:v2:.*/[0-9a-f]{32}$`, token)
	resolved, err := store.ResolveToken(context.Background(), token)
	require.NoError(t, err)
	require.Equal(t, []byte("Azure Blob payload"), resolved)
}

func TestAzureBlobStoreAcceptsDotNetGoldenBlobNames(t *testing.T) {
	store := newTestAzureBlobStore(t)
	for _, token := range []string{
		"blob:v2:https://myaccount.blob.core.windows.net/mycontainer/abc123def456",
		"blob:v2:http://127.0.0.1:10000/devstoreaccount1/mycontainer/abc123def456",
		"blob:v1:mycontainer:abc123def456",
		"blob:v2:https://myaccount.blob.core.windows.net/mycontainer/nested/abc123def456",
	} {
		require.NoError(t, store.ValidateLargePayloadToken(token))
	}
}

type staticCredential struct{}

func (staticCredential) GetToken(context.Context, policy.TokenRequestOptions) (azcore.AccessToken, error) {
	return azcore.AccessToken{Token: "test", ExpiresOn: time.Now().Add(time.Hour)}, nil
}
