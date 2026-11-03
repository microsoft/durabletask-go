package payload

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/microsoft/durabletask-go/api"
	"github.com/stretchr/testify/require"
)

type blobAccessRecorder struct {
	tokenCalls int
	requests   []*http.Request
}

func (r *blobAccessRecorder) GetToken(context.Context, policy.TokenRequestOptions) (azcore.AccessToken, error) {
	r.tokenCalls++
	return azcore.AccessToken{Token: "offline-test-token", ExpiresOn: time.Now().Add(time.Hour)}, nil
}

func (r *blobAccessRecorder) Do(req *http.Request) (*http.Response, error) {
	r.requests = append(r.requests, req.Clone(req.Context()))
	return &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Length": {"7"}},
		Body:       io.NopCloser(strings.NewReader("payload")),
		Request:    req,
	}, nil
}

func newAccessTestAzureBlobStore(
	t *testing.T, accountURL, auth string, allowedHosts []string,
) (*AzureBlobStore, *blobAccessRecorder) {
	t.Helper()
	recorder := &blobAccessRecorder{}
	options := AzureBlobStoreOptions{
		AccountURL:        accountURL,
		Credential:        recorder,
		Container:         "mycontainer",
		AllowedHosts:      allowedHosts,
		AllowInsecureHTTP: true,
	}
	if auth != "identity" {
		options.AccountURL, options.Credential = "", nil
		options.ConnectionString = "BlobEndpoint=" + accountURL + ";"
		switch auth {
		case "shared-key":
			options.ConnectionString += "AccountName=account;AccountKey=dGVzdA=="
		case "sas":
			options.ConnectionString += "SharedAccessSignature=sv=2024-01-01&sig=test"
		default:
			t.Fatalf("unknown authentication mode %q", auth)
		}
	}
	store, err := NewAzureBlobStore(options)
	require.NoError(t, err)
	// Use the real Azure pipelines with an offline transport for both the
	// configured client and any client created for an explicitly allowed host.
	store.clientOptions = &azblob.ClientOptions{ClientOptions: azcore.ClientOptions{
		Transport: recorder,
		Retry:     policy.RetryOptions{MaxRetries: -1},
	}}
	if auth == "identity" {
		store.client, err = azblob.NewClient(accountURL, recorder, store.clientOptions)
	} else {
		store.client, err = azblob.NewClientFromConnectionString(options.ConnectionString, store.clientOptions)
	}
	require.NoError(t, err)
	require.Zero(t, recorder.tokenCalls)
	require.Empty(t, recorder.requests)
	return store, recorder
}

func TestAzureBlobStoreDefaultReadBoundary(t *testing.T) {
	for _, accountURL := range []string{
		"https://account.blob.core.windows.net",
		"https://127.0.0.1:10000/account1",
	} {
		for _, auth := range []string{"identity", "shared-key", "sas"} {
			for _, hosts := range []struct {
				name  string
				value []string
			}{
				{name: "nil"},
				{name: "empty", value: []string{}},
			} {
				t.Run(accountURL+"/"+auth+"/"+hosts.name, func(t *testing.T) {
					store, recorder := newAccessTestAzureBlobStore(t, accountURL, auth, hosts.value)
					location := accountURL + "/mycontainer/blob"
					for _, token := range []string{
						azureBlobTokenV2 + accountURL + "/othercontainer/blob",
						azureBlobTokenV2 + "https://other.blob.core.windows.net/mycontainer/blob",
						azureBlobTokenV2 + "https://other.blob.core.windows.net/othercontainer/blob",
						azureBlobTokenV2 + strings.Replace(location, "https:", "http:", 1),
						azureBlobTokenV2 + strings.Replace(location, store.containerURL.Host, store.containerURL.Hostname()+":444", 1),
						azureBlobTokenV2 + "https://127.0.0.1:10000/account2/mycontainer/blob",
						azureBlobTokenV1 + "othercontainer:blob",
					} {
						t.Run(token, func(t *testing.T) {
							require.ErrorIs(t, store.ValidateLargePayloadToken(token), api.ErrLargePayloadReference)
							_, err := store.ResolveToken(context.Background(), token)
							require.ErrorIs(t, err, api.ErrLargePayloadReference)
							if strings.HasPrefix(token, azureBlobTokenV2) {
								_, err = store.Resolve(context.Background(), strings.TrimPrefix(token, azureBlobTokenV2))
								require.ErrorIs(t, err, api.ErrLargePayloadReference)
							}
							require.Zero(t, recorder.tokenCalls)
							require.Empty(t, recorder.requests)
						})
					}

					for _, token := range []string{
						azureBlobTokenV2 + location,
						azureBlobTokenV1 + "mycontainer:blob",
					} {
						resolved, err := store.ResolveToken(context.Background(), token)
						require.NoError(t, err)
						require.Equal(t, "payload", string(resolved))
					}
					resolved, err := store.Resolve(context.Background(), location)
					require.NoError(t, err)
					require.Equal(t, "payload", string(resolved))
					require.Len(t, recorder.requests, 3)
					for _, req := range recorder.requests {
						require.Equal(t, store.containerURL.Host, req.URL.Host)
						require.Equal(t, store.containerURL.Path+"/blob", req.URL.Path)
					}
					if auth == "identity" {
						require.Positive(t, recorder.tokenCalls)
					}
				})
			}
		}
	}
}

func TestAzureBlobStoreAllowedHostsReads(t *testing.T) {
	for _, test := range []struct {
		name       string
		accountURL string
		host       string
		location   string
	}{
		{
			name: "same account other container", accountURL: "https://account.blob.core.windows.net",
			host: "account.blob.core.windows.net", location: "https://account.blob.core.windows.net/othercontainer/blob",
		},
		{
			name: "other account same container", accountURL: "https://account.blob.core.windows.net",
			host: "other.blob.core.windows.net", location: "https://other.blob.core.windows.net/mycontainer/blob",
		},
		{
			name: "other account other container", accountURL: "https://account.blob.core.windows.net",
			host: "other.blob.core.windows.net", location: "https://other.blob.core.windows.net/othercontainer/blob",
		},
		{
			name: "hostname match ignores port", accountURL: "https://account.blob.core.windows.net",
			host: "account.blob.core.windows.net", location: "https://account.blob.core.windows.net:444/othercontainer/blob",
		},
		{
			name: "path style other account", accountURL: "https://127.0.0.1:10000/account1",
			host: "127.0.0.1", location: "https://127.0.0.1:10000/account2/othercontainer/blob",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			store, recorder := newAccessTestAzureBlobStore(t, test.accountURL, "identity", []string{test.host})
			_, err := store.ResolveToken(context.Background(), azureBlobTokenV2+"https://unlisted.blob.core.windows.net/mycontainer/blob")
			require.ErrorIs(t, err, api.ErrLargePayloadReference)
			_, err = store.Resolve(context.Background(), test.location)
			require.ErrorIs(t, err, api.ErrLargePayloadReference)
			require.NoError(t, store.ValidateLargePayloadToken(azureBlobTokenV2+test.location))
			require.Zero(t, recorder.tokenCalls)
			require.Empty(t, recorder.requests)

			resolved, err := store.ResolveToken(context.Background(), azureBlobTokenV2+test.location)
			require.NoError(t, err)
			require.Equal(t, "payload", string(resolved))
			require.Positive(t, recorder.tokenCalls)
			require.Len(t, recorder.requests, 1)
			require.Equal(t, test.location, recorder.requests[0].URL.String())
			require.Equal(t, "Bearer offline-test-token", recorder.requests[0].Header.Get("Authorization"))
		})
	}
}

func TestAzureBlobStoreConnectionStringCannotUseAllowedHosts(t *testing.T) {
	for _, auth := range []string{"shared-key", "sas"} {
		t.Run(auth, func(t *testing.T) {
			store, recorder := newAccessTestAzureBlobStore(t, "https://account.blob.core.windows.net", auth, []string{
				"account.blob.core.windows.net", "other.blob.core.windows.net",
			})
			for _, location := range []string{
				"https://account.blob.core.windows.net/othercontainer/blob",
				"https://other.blob.core.windows.net/mycontainer/blob",
				"https://other.blob.core.windows.net/othercontainer/blob",
			} {
				require.ErrorIs(t, store.ValidateLargePayloadToken(azureBlobTokenV2+location), api.ErrLargePayloadReference)
				_, err := store.ResolveToken(context.Background(), azureBlobTokenV2+location)
				require.ErrorIs(t, err, api.ErrLargePayloadReference)
				require.Zero(t, recorder.tokenCalls)
				require.Empty(t, recorder.requests)
			}
			resolved, err := store.ResolveToken(context.Background(), azureBlobTokenV2+"https://account.blob.core.windows.net/mycontainer/blob")
			require.NoError(t, err)
			require.Equal(t, "payload", string(resolved))
			require.Len(t, recorder.requests, 1)
			require.Zero(t, recorder.tokenCalls)
		})
	}
}
