package payload

import (
	"context"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/microsoft/durabletask-go/api"
	"github.com/microsoft/durabletask-go/internal/largepayload"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

type blobAccessRecorder struct {
	tokenCalls atomic.Int32
	mu         sync.Mutex
	requests   []*http.Request
	respond    func(*http.Request) *http.Response
}

func (r *blobAccessRecorder) GetToken(context.Context, policy.TokenRequestOptions) (azcore.AccessToken, error) {
	r.tokenCalls.Add(1)
	return azcore.AccessToken{Token: "offline-test-token", ExpiresOn: time.Now().Add(time.Hour)}, nil
}

func (r *blobAccessRecorder) recorded() []*http.Request {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]*http.Request(nil), r.requests...)
}

func (r *blobAccessRecorder) Do(req *http.Request) (*http.Response, error) {
	r.mu.Lock()
	r.requests = append(r.requests, req.Clone(req.Context()))
	r.mu.Unlock()
	response := &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Length": {"7"}},
		Body:       io.NopCloser(strings.NewReader("payload")),
		Request:    req,
	}
	if req.Method == http.MethodPut {
		response.StatusCode, response.Body = http.StatusCreated, http.NoBody
		response.Header = http.Header{}
	}
	if r.respond != nil {
		response = r.respond(req)
	}
	return response, nil
}

func (r *blobAccessRecorder) RoundTrip(req *http.Request) (*http.Response, error) {
	return r.Do(req)
}

func blobAccessOptions(accountURL, auth string) AzureBlobStoreOptions {
	options := AzureBlobStoreOptions{AccountURL: accountURL, Container: "mycontainer", AllowInsecureHTTP: true}
	if auth != "identity" {
		options.AccountURL = ""
		options.ConnectionString = "BlobEndpoint=" + accountURL + ";"
		if auth == "sas" {
			options.ConnectionString += "SharedAccessSignature=sv=2024-01-01&sig=configured"
		} else {
			options.ConnectionString += "AccountName=account;AccountKey=dGVzdA=="
		}
	}
	return options
}

func newAccessTestAzureBlobStore(t *testing.T, options AzureBlobStoreOptions) (*AzureBlobStore, *blobAccessRecorder) {
	t.Helper()
	recorder := &blobAccessRecorder{}
	if options.ConnectionString == "" {
		options.Credential = recorder
	}
	clientOptions := newAzureBlobClientOptions()
	clientOptions.Transport = recorder
	clientOptions.Retry.MaxRetries = -1
	store, err := newAzureBlobStore(options, clientOptions)
	require.NoError(t, err)
	require.Zero(t, recorder.tokenCalls.Load())
	require.Empty(t, recorder.recorded())
	return store, recorder
}

func assertBlobTokenAccess(t *testing.T, store *AzureBlobStore, recorder *blobAccessRecorder, token string, allowed bool) {
	t.Helper()
	calls, requests := recorder.tokenCalls.Load(), len(recorder.recorded())
	err := store.ValidateLargePayloadToken(token)
	require.Equal(t, calls, recorder.tokenCalls.Load())
	require.Len(t, recorder.recorded(), requests)
	if allowed {
		require.NoError(t, err)
	} else {
		require.ErrorIs(t, err, api.ErrLargePayloadReference)
	}
	resolve := func(read func() ([]byte, error)) {
		t.Helper()
		result, err := read()
		if allowed {
			require.NoError(t, err)
			require.Equal(t, "payload", string(result))
		} else {
			require.ErrorIs(t, err, api.ErrLargePayloadReference)
			require.Nil(t, result)
			require.Equal(t, calls, recorder.tokenCalls.Load())
			require.Len(t, recorder.recorded(), requests)
		}
	}
	resolve(func() ([]byte, error) { return store.ResolveToken(context.Background(), token) })
	if strings.HasPrefix(token, azureBlobTokenV2) {
		resolve(func() ([]byte, error) {
			return store.Resolve(context.Background(), strings.TrimPrefix(token, azureBlobTokenV2))
		})
	}
}

func TestAzureBlobStoreAccessPolicyMatrix(t *testing.T) {
	for _, primary := range []string{"https://account.blob.core.windows.net", "https://127.0.0.1:10000/account1"} {
		for _, auth := range []string{"identity", "shared-key", "sas"} {
			for _, all := range []bool{false, true} {
				for _, additional := range []bool{false, true} {
					if additional && auth != "identity" {
						continue
					}
					name := primary + "/" + auth
					if all {
						name += "/all"
					}
					if additional {
						name += "/additional"
					}
					t.Run(name, func(t *testing.T) {
						options := blobAccessOptions(primary, auth)
						options.AllowAllContainersInConfiguredAccount = all
						if additional {
							options.AdditionalAccountURLs = []string{"https://other.blob.core.windows.net"}
						}
						store, recorder := newAccessTestAzureBlobStore(t, options)
						for _, test := range []struct {
							token   string
							allowed bool
						}{
							{azureBlobTokenV2 + primary + "/mycontainer/blob", true},
							{azureBlobTokenV2 + primary + "/othercontainer/blob", all},
							{azureBlobTokenV1 + "mycontainer:blob", true},
							{azureBlobTokenV1 + "othercontainer:blob", all},
							{azureBlobTokenV2 + "https://other.blob.core.windows.net/mycontainer/blob", additional},
							{azureBlobTokenV2 + "https://other.blob.core.windows.net/othercontainer/blob", additional},
							{azureBlobTokenV2 + "https://unlisted.blob.core.windows.net/mycontainer/blob", false},
						} {
							assertBlobTokenAccess(t, store, recorder, test.token, test.allowed)
						}
						for _, req := range recorder.recorded() {
							switch auth {
							case "identity":
								require.Equal(t, "Bearer "+"offline-test-token", req.Header.Get("Authorization"))
							case "sas":
								require.Equal(t, "configured", req.URL.Query().Get("sig"))
							default:
								require.True(t, strings.HasPrefix(req.Header.Get("Authorization"), "SharedKey account:"))
							}
						}
					})
				}
			}
		}
	}
}

func TestAzureBlobStoreExactEndpoints(t *testing.T) {
	for _, listed := range []bool{false, true} {
		options := blobAccessOptions("https://127.0.0.1:10000/account1", "identity")
		options.AllowAllContainersInConfiguredAccount = true
		if listed {
			options.AdditionalAccountURLs = []string{
				"https://127.0.0.1:10001/account1", "https://127.0.0.1:10000/account2",
			}
		}
		store, recorder := newAccessTestAzureBlobStore(t, options)
		assertBlobTokenAccess(t, store, recorder, azureBlobTokenV2+"https://127.0.0.1:10001/account1/othercontainer/blob", listed)
		assertBlobTokenAccess(t, store, recorder, azureBlobTokenV2+"https://127.0.0.1:10000/account2/othercontainer/blob", listed)
		assertBlobTokenAccess(t, store, recorder, azureBlobTokenV2+"https://127.0.0.1:10000/Account1/mycontainer/blob", false)
		assertBlobTokenAccess(t, store, recorder, azureBlobTokenV2+"http://127.0.0.1:10000/account1/mycontainer/blob", false)
	}
}

func TestAzureBlobStoreAdditionalAccountConfiguration(t *testing.T) {
	for _, endpoint := range []string{
		"", "other.blob.core.windows.net", "//other.blob.core.windows.net",
		"http://other.blob.core.windows.net", "https://*.blob.core.windows.net",
		"https://user@other.blob.core.windows.net", "https://other.blob.core.windows.net?sig=x",
		"https://other.blob.core.windows.net?", "https://other.blob.core.windows.net#fragment", "https://other.blob.core.windows.net#",
		"https://.blob.core.windows.net", "https://other..blob.core.windows.net",
		"https://other.blob.core.windows.net/mycontainer", "https://other.blob.core.windows.net/mycontainer/blob",
		"https://other.blob.core.windows.net:0", "https://other.blob.core.windows.net:65536",
		"https://localhost/account1", "https://127.0.0.1:10000", "https://127.0.0.1:10000/account/container",
		"https://127.0.0.1:10000/account%2Fcontainer", "https://127.0.0.1:10000/../",
		"https://ACCOUNT.blob.core.windows.net:443/",
	} {
		t.Run(endpoint, func(t *testing.T) {
			recorder := &blobAccessRecorder{}
			options := blobAccessOptions("https://account.blob.core.windows.net", "identity")
			options.Credential, options.AdditionalAccountURLs = recorder, []string{endpoint}
			_, err := newAzureBlobStore(options, &azblob.ClientOptions{ClientOptions: azcore.ClientOptions{Transport: recorder}})
			require.Error(t, err)
			require.Zero(t, recorder.tokenCalls.Load())
			require.Empty(t, recorder.recorded())
		})
	}
	for _, auth := range []string{"sas", "shared-key"} {
		options := blobAccessOptions("https://account.blob.core.windows.net", auth)
		options.AdditionalAccountURLs = []string{"https://other.blob.core.windows.net"}
		_, err := NewAzureBlobStore(options)
		require.ErrorContains(t, err, "token credential")
	}
	options := blobAccessOptions("https://account.blob.core.windows.net", "identity")
	options.AdditionalAccountURLs = []string{"HTTPS://OTHER.blob.core.windows.net:443/", "https://other.blob.core.windows.net"}
	store, recorder := newAccessTestAzureBlobStore(t, options)
	require.Len(t, store.additionalAccounts, 1)
	options.AdditionalAccountURLs[0] = "https://new.blob.core.windows.net"
	options.AdditionalAccountURLs = append(options.AdditionalAccountURLs, "https://another.blob.core.windows.net")
	options.AllowAllContainersInConfiguredAccount = true
	options.AccountURL, options.Container, options.Credential = "https://new.blob.core.windows.net", "othercontainer", staticCredential{}
	assertBlobTokenAccess(t, store, recorder, azureBlobTokenV2+"https://new.blob.core.windows.net/mycontainer/blob", false)
	assertBlobTokenAccess(t, store, recorder, azureBlobTokenV2+"https://another.blob.core.windows.net/mycontainer/blob", false)
	assertBlobTokenAccess(t, store, recorder, azureBlobTokenV1+"othercontainer:blob", false)
	assertBlobTokenAccess(t, store, recorder, azureBlobTokenV2+"https://OTHER.blob.core.windows.net:443/othercontainer/blob", true)
}

func TestAzureBlobStoreTokenCompatibility(t *testing.T) {
	for _, auth := range []string{"identity", "sas", "shared-key"} {
		options := blobAccessOptions("https://account.blob.core.windows.net", auth)
		store, recorder := newAccessTestAzureBlobStore(t, options)
		for _, name := range []string{"nested/a b%?#", "nested/%2e%2e/literal", "nested/\u65e5\u672c\u8a9e", "nested//blob"} {
			u, err := url.Parse("https://account.blob.core.windows.net/mycontainer/")
			require.NoError(t, err)
			u.Path += name
			assertBlobTokenAccess(t, store, recorder, azureBlobTokenV2+u.String()+"?sig=untrusted&snapshot=old&versionid=old#ignored", true)
			assertBlobTokenAccess(t, store, recorder, azureBlobTokenV1+"mycontainer:"+name, true)
			for _, req := range recorder.recorded() {
				require.Equal(t, "https://account.blob.core.windows.net/mycontainer/"+url.PathEscape(name), req.URL.Scheme+"://"+req.URL.Host+req.URL.EscapedPath())
				require.Empty(t, req.URL.Fragment)
				require.Empty(t, req.URL.Query().Get("snapshot"))
				require.Empty(t, req.URL.Query().Get("versionid"))
				if auth == "sas" {
					require.Equal(t, "configured", req.URL.Query().Get("sig"))
				} else {
					require.Empty(t, req.URL.RawQuery)
				}
			}
			recorder.mu.Lock()
			recorder.requests = nil
			recorder.mu.Unlock()
		}
	}
}

func TestAzureBlobStoreEmptyAdditionalAccounts(t *testing.T) {
	for _, accounts := range [][]string{nil, {}} {
		options := blobAccessOptions("https://account.blob.core.windows.net", "identity")
		options.AdditionalAccountURLs = accounts
		store, recorder := newAccessTestAzureBlobStore(t, options)
		assertBlobTokenAccess(t, store, recorder, azureBlobTokenV2+"https://account.blob.core.windows.net/othercontainer/blob", false)
		assertBlobTokenAccess(t, store, recorder, azureBlobTokenV2+"https://other.blob.core.windows.net/mycontainer/blob", false)
	}
}

func TestAzureBlobStoreChecksConstructedDestination(t *testing.T) {
	for _, primary := range []bool{false, true} {
		options := blobAccessOptions("https://account.blob.core.windows.net", "identity")
		options.AdditionalAccountURLs = []string{"https://other.blob.core.windows.net"}
		store, recorder := newAccessTestAzureBlobStore(t, options)
		clientOptions := newAzureBlobClientOptions()
		clientOptions.Transport = recorder
		wrongClient, err := azblob.NewClient("https://unlisted.blob.core.windows.net", recorder, clientOptions)
		require.NoError(t, err)
		host := "other.blob.core.windows.net"
		if primary {
			store.client, host = wrongClient, "account.blob.core.windows.net"
		} else {
			store.additionalAccounts["https://other.blob.core.windows.net"] = wrongClient
		}
		assertBlobTokenAccess(t, store, recorder, azureBlobTokenV2+"https://"+host+"/mycontainer/blob", false)
	}
}

func TestAzureBlobStoreRejectsDestinationEscapes(t *testing.T) {
	options := blobAccessOptions("https://account.blob.core.windows.net", "identity")
	options.AdditionalAccountURLs = []string{"https://other.blob.core.windows.net"}
	store, recorder := newAccessTestAzureBlobStore(t, options)
	for _, token := range []string{
		azureBlobTokenV2 + "https://account.blob.core.windows.net.evil.example/mycontainer/blob",
		azureBlobTokenV2 + "https://other.blob.core.windows.net.evil.example/mycontainer/blob",
		azureBlobTokenV2 + "https://user@account.blob.core.windows.net/mycontainer/blob",
		azureBlobTokenV2 + "https://account.blob.core.windows.net/mycontainer/../othercontainer/blob",
		azureBlobTokenV2 + "https://account.blob.core.windows.net/mycontainer/%2e%2e%2fothercontainer/blob",
		azureBlobTokenV2 + "https://account.blob.core.windows.net/mycontainer%2fblob/name",
		azureBlobTokenV2 + "https://account.blob.core.windows.net/mycontainer/..%5cothercontainer/blob",
		azureBlobTokenV1 + "mycontainer:../othercontainer/blob",
		azureBlobTokenV1 + "mycontainer:..",
	} {
		assertBlobTokenAccess(t, store, recorder, token, false)
		_, err := largepayload.Hydrate(context.Background(), &api.LargePayloadOptions{Store: store, Resolver: store}, wrapperspb.String(token))
		require.ErrorIs(t, err, api.ErrLargePayloadReference)
	}
	require.Zero(t, recorder.tokenCalls.Load())
	require.Empty(t, recorder.recorded())
}

func TestAzureBlobStoreUploadsStayPrimary(t *testing.T) {
	for _, auth := range []string{"identity", "sas", "shared-key"} {
		options := blobAccessOptions("https://account.blob.core.windows.net", auth)
		options.AllowAllContainersInConfiguredAccount = true
		if auth == "identity" {
			options.AdditionalAccountURLs = []string{"https://other.blob.core.windows.net"}
		}
		store, recorder := newAccessTestAzureBlobStore(t, options)
		token, err := store.StoreToken(context.Background(), []byte("payload"))
		require.NoError(t, err)
		require.Regexp(t, `^blob:v2:https://account\.blob\.core\.windows\.net/mycontainer/[0-9a-f]{32}$`, token)
		for _, req := range recorder.recorded() {
			require.Equal(t, "account.blob.core.windows.net", req.URL.Host)
			require.True(t, req.URL.Path == "/mycontainer" || strings.HasPrefix(req.URL.Path, "/mycontainer/"))
			if auth == "sas" {
				require.Equal(t, "configured", req.URL.Query().Get("sig"))
			}
		}
		require.Len(t, recorder.recorded(), 2)
	}
}

func TestAzureBlobStoreDoesNotFallBackAfterStorageDenial(t *testing.T) {
	options := blobAccessOptions("https://account.blob.core.windows.net", "identity")
	options.AdditionalAccountURLs = []string{"https://other.blob.core.windows.net"}
	store, recorder := newAccessTestAzureBlobStore(t, options)
	recorder.respond = func(req *http.Request) *http.Response {
		return &http.Response{StatusCode: http.StatusForbidden, Header: http.Header{"X-Ms-Error-Code": {"AuthorizationPermissionMismatch"}}, Body: http.NoBody, Request: req}
	}
	_, err := store.ResolveToken(context.Background(), azureBlobTokenV2+"https://other.blob.core.windows.net/othercontainer/blob")
	var responseErr *azcore.ResponseError
	require.ErrorAs(t, err, &responseErr)
	require.Equal(t, http.StatusForbidden, responseErr.StatusCode)
	require.Len(t, recorder.recorded(), 1)
	require.Equal(t, "other.blob.core.windows.net", recorder.recorded()[0].URL.Host)
}

func TestAzureBlobStoreDoesNotFollowRedirects(t *testing.T) {
	recorder := &blobAccessRecorder{respond: func(req *http.Request) *http.Response {
		return &http.Response{StatusCode: http.StatusTemporaryRedirect, Header: http.Header{"Location": {"https://unlisted.blob.core.windows.net/othercontainer/blob"}}, Body: http.NoBody, Request: req}
	}}
	options := blobAccessOptions("https://account.blob.core.windows.net", "identity")
	options.Credential, options.AdditionalAccountURLs = recorder, []string{"https://other.blob.core.windows.net"}
	clientOptions := newAzureBlobClientOptions()
	clientOptions.Transport.(*http.Client).Transport = recorder
	clientOptions.Retry.MaxRetries = -1
	store, err := newAzureBlobStore(options, clientOptions)
	require.NoError(t, err)
	for _, host := range []string{"account.blob.core.windows.net", "other.blob.core.windows.net"} {
		_, err = store.ResolveToken(context.Background(), azureBlobTokenV2+"https://"+host+"/mycontainer/blob")
		require.Error(t, err)
	}
	require.Len(t, recorder.recorded(), 2)
}
