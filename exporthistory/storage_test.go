package exporthistory

import (
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"io"
	"maps"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/bloberror"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// placeholderAccountKey is a syntactically valid base64 value, not a credential.
// These tests only construct clients to exercise URL and transport policy; they
// never authenticate against a storage service. Its decoded text says as much.
const placeholderAccountKey = "ZHVyYWJsZXRhc2stZ28gZXhwb3J0aGlzdG9yeSB1bml0IHRlc3RzIHVzZSB0aGlzIHBsYWNlaG9sZGVyIGFjY291bnQga2V5OyBpdCBpcyBub3QgYSByZWFsIGNyZWRlbnRpYWwgYW5kIG5ldmVyIGF1dGhlbnRpY2F0ZXMu"

// testConnectionString points at an HTTPS endpoint that is never contacted:
// every test that uses it installs client hooks.
const testConnectionString = "DefaultEndpointsProtocol=https;AccountName=devstoreaccount1;" +
	"AccountKey=" + placeholderAccountKey + ";EndpointSuffix=core.windows.net"

// azuriteConnectionString mirrors the shape of the Azurite development endpoint,
// which is plaintext and therefore requires AllowInsecureHTTP. Live Azurite tests
// read a real connection string from AZURITE_CONNECTION_STRING instead.
const azuriteConnectionString = "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;" +
	"AccountKey=" + placeholderAccountKey + ";BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;"

// memoryStore is an in-memory [Store] used by tests that exercise the export
// pipeline without Azure Storage.
type memoryStore struct {
	mu      sync.Mutex
	objects map[string]ExportObject
	writes  int
	failure error
	// failFor fails the write for a specific instance ID, so a test can drive a
	// partially failing batch.
	failFor map[string]error
}

func newMemoryStore() *memoryStore {
	return &memoryStore{objects: make(map[string]ExportObject), failFor: make(map[string]error)}
}

func (s *memoryStore) Write(_ context.Context, object ExportObject) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.writes++
	if s.failure != nil {
		return s.failure
	}
	if err, ok := s.failFor[object.Metadata["instanceId"]]; ok {
		return err
	}
	s.objects[object.Container+"/"+object.Name] = object
	return nil
}

func (s *memoryStore) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.objects)
}

func (s *memoryStore) writeCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.writes
}

func (s *memoryStore) snapshot() map[string]ExportObject {
	s.mu.Lock()
	defer s.mu.Unlock()
	return maps.Clone(s.objects)
}

func (s *memoryStore) failInstance(instanceID string, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.failFor[instanceID] = err
}

var _ Store = (*memoryStore)(nil)

func TestValidBlobContainerName(t *testing.T) {
	valid := []string{"abc", "a-b-c", "container1", strings.Repeat("a", 63)}
	for _, name := range valid {
		assert.True(t, validBlobContainerName(name), name)
	}
	invalid := []string{
		"", "ab", strings.Repeat("a", 64), "-abc", "abc-", "a--b",
		"ABC", "a_b", "a.b", "a b", "contaìner",
	}
	for _, name := range invalid {
		assert.False(t, validBlobContainerName(name), name)
	}
}

func TestValidateBlobName(t *testing.T) {
	require.NoError(t, validateBlobName("a/b/c.jsonl.gz"))
	for _, name := range []string{
		"", "/leading", "trailing/", strings.Repeat("a", 1025), "a\x00b", "a\nb", "a\\b",
	} {
		require.ErrorIs(t, validateBlobName(name), ErrValidation, name)
	}
}

func TestValidateBlobPrefix(t *testing.T) {
	require.NoError(t, validateBlobPrefix(""))
	require.NoError(t, validateBlobPrefix("a/b/"))
	for _, prefix := range []string{
		"/a", "a//b", "../a", "a/../b", "a/./b", strings.Repeat("a", 901), "a\tb", "a\\b",
	} {
		require.ErrorIs(t, validateBlobPrefix(prefix), ErrValidation, prefix)
	}
}

func TestIsSafeBlobURL(t *testing.T) {
	parse := func(raw string) *url.URL {
		parsed, err := url.Parse(raw)
		require.NoError(t, err)
		return parsed
	}
	assert.True(t, isSafeBlobURL(parse("https://account.blob.core.windows.net"), false))
	assert.False(t, isSafeBlobURL(parse("http://account.blob.core.windows.net"), false))
	assert.False(t, isSafeBlobURL(parse("http://account.blob.core.windows.net"), true))
	assert.True(t, isSafeBlobURL(parse("http://127.0.0.1:10000"), true))
	assert.True(t, isSafeBlobURL(parse("http://localhost:10000"), true))
	assert.True(t, isSafeBlobURL(parse("http://[::1]:10000"), true))
	assert.False(t, isSafeBlobURL(parse("http://127.0.0.1:10000"), false))
	assert.False(t, isSafeBlobURL(parse("ftp://127.0.0.1"), true))
	assert.False(t, isSafeBlobURL(nil, true))
}

// TestIsSafeBlobURLRejectsCredentialCarryingURLs keeps the export store's
// endpoint check at least as strict as the large-payload store's: userinfo, a
// query string, and a fragment are all rejected, so a configured endpoint cannot
// smuggle a SAS token or credentials past the scheme and host checks.
func TestIsSafeBlobURLRejectsCredentialCarryingURLs(t *testing.T) {
	parse := func(raw string) *url.URL {
		parsed, err := url.Parse(raw)
		require.NoError(t, err)
		return parsed
	}
	rejected := []string{
		"https://user@account.blob.core.windows.net",
		"https://user:password@account.blob.core.windows.net",
		"https://account.blob.core.windows.net/?sig=redacted&se=2030-01-01",
		"https://account.blob.core.windows.net/?",
		"https://account.blob.core.windows.net/#fragment",
		"https://account.blob.core.windows.net/container?sv=2021#frag",
		// A relative or opaque reference has no host to validate at all.
		"account.blob.core.windows.net",
		"//account.blob.core.windows.net",
		"mailto:someone@example.com",
	}
	for _, raw := range rejected {
		assert.False(t, isSafeBlobURL(parse(raw), true), raw)
		assert.False(t, isSafeBlobURL(parse(raw), false), raw)
	}
	// A loopback endpoint stays usable, but only without credentials in the URL.
	assert.True(t, isSafeBlobURL(parse("http://127.0.0.1:10000/devstoreaccount1"), true))
	assert.False(t, isSafeBlobURL(parse("http://user:pass@127.0.0.1:10000"), true))
}

// TestNewAzureBlobHistoryStoreRejectsCredentialCarryingAccountURL covers the same rule
// at the constructor boundary, which is where an operator-supplied endpoint
// actually enters the process.
func TestNewAzureBlobHistoryStoreRejectsCredentialCarryingAccountURL(t *testing.T) {
	for _, accountURL := range []string{
		"https://user:password@account.blob.core.windows.net",
		"https://account.blob.core.windows.net/?sig=redacted",
		"https://account.blob.core.windows.net/#fragment",
	} {
		_, err := NewAzureBlobHistoryStore(AzureBlobHistoryStoreOptions{
			ContainerName: "container",
			AccountURL:    accountURL,
			Credential:    fakeCredential{},
		})
		require.ErrorIs(t, err, ErrValidation, accountURL)
		assert.Contains(t, err.Error(), "invalid Azure Blob account URL", accountURL)
	}
}

func TestNewAzureBlobHistoryStoreValidation(t *testing.T) {
	tests := []struct {
		name    string
		options AzureBlobHistoryStoreOptions
		message string
	}{
		{"missing container", AzureBlobHistoryStoreOptions{ConnectionString: testConnectionString}, "container name is required"},
		{
			name:    "invalid container",
			options: AzureBlobHistoryStoreOptions{ConnectionString: testConnectionString, ContainerName: "BAD"},
			message: "is not valid",
		},
		{
			name:    "no credentials",
			options: AzureBlobHistoryStoreOptions{ContainerName: "container"},
			message: "exactly one of connection string or account URL is required",
		},
		{
			name: "both credential modes",
			options: AzureBlobHistoryStoreOptions{
				ContainerName:    "container",
				ConnectionString: testConnectionString,
				AccountURL:       "https://account.blob.core.windows.net",
			},
			message: "exactly one of connection string or account URL is required",
		},
		{
			name:    "account URL without a credential",
			options: AzureBlobHistoryStoreOptions{ContainerName: "container", AccountURL: "https://account.blob.core.windows.net"},
			message: "requires a token credential",
		},
		{
			name: "connection string with a credential",
			options: AzureBlobHistoryStoreOptions{
				ContainerName:    "container",
				ConnectionString: testConnectionString,
				Credential:       fakeCredential{},
			},
			message: "cannot be combined",
		},
		{
			name: "insecure account URL",
			options: AzureBlobHistoryStoreOptions{
				ContainerName: "container",
				AccountURL:    "http://account.blob.core.windows.net",
				Credential:    fakeCredential{},
			},
			message: "invalid Azure Blob account URL",
		},
		{
			name: "invalid allow-listed container",
			options: AzureBlobHistoryStoreOptions{
				ContainerName:     "container",
				ConnectionString:  testConnectionString,
				AllowedContainers: []string{"BAD"},
			},
			message: "allowed azure blob container name",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := NewAzureBlobHistoryStore(test.options)
			require.ErrorIs(t, err, ErrValidation)
			assert.Contains(t, err.Error(), test.message)
		})
	}

	store, err := NewAzureBlobHistoryStore(AzureBlobHistoryStoreOptions{
		ConnectionString: testConnectionString,
		ContainerName:    "container",
	})
	require.NoError(t, err)
	assert.Equal(t, "container", store.DefaultContainer())

	// The well-known Azurite endpoint is plaintext, so it needs an explicit opt-in.
	_, err = NewAzureBlobHistoryStore(AzureBlobHistoryStoreOptions{
		ConnectionString: azuriteConnectionString,
		ContainerName:    "container",
	})
	require.ErrorIs(t, err, ErrValidation)
	assert.Contains(t, err.Error(), "service URL is not allowed")

	_, err = NewAzureBlobHistoryStore(AzureBlobHistoryStoreOptions{
		ConnectionString:  azuriteConnectionString,
		ContainerName:     "container",
		AllowInsecureHTTP: true,
	})
	require.NoError(t, err)
}

func TestAzureBlobHistoryStoreRejectsDisallowedContainers(t *testing.T) {
	store, err := NewAzureBlobHistoryStore(AzureBlobHistoryStoreOptions{
		ConnectionString:  testConnectionString,
		ContainerName:     "primary",
		AllowedContainers: []string{"secondary"},
	})
	require.NoError(t, err)
	store.createContainerHook = func(context.Context, string) error { return nil }
	store.uploadBlobHook = func(context.Context, string, string, []byte, *azblob.UploadBufferOptions) error {
		return nil
	}

	ctx := context.Background()
	require.NoError(t, store.Write(ctx, ExportObject{Container: "primary", Name: "a"}))
	require.NoError(t, store.Write(ctx, ExportObject{Container: "secondary", Name: "a"}))

	err = store.Write(ctx, ExportObject{Container: "tertiary", Name: "a"})
	require.ErrorIs(t, err, ErrValidation)
	assert.Contains(t, err.Error(), "is not allowed by this worker")

	err = store.Write(ctx, ExportObject{Container: "BAD", Name: "a"})
	require.ErrorIs(t, err, ErrValidation)
	err = store.Write(ctx, ExportObject{Name: "a"})
	require.ErrorIs(t, err, ErrValidation)
	err = store.Write(ctx, ExportObject{Container: "primary", Name: "/bad"})
	require.ErrorIs(t, err, ErrValidation)
}

func TestAzureBlobHistoryStoreAllowAnyContainer(t *testing.T) {
	store, err := NewAzureBlobHistoryStore(AzureBlobHistoryStoreOptions{
		ConnectionString:  testConnectionString,
		ContainerName:     "primary",
		AllowAnyContainer: true,
	})
	require.NoError(t, err)
	store.createContainerHook = func(context.Context, string) error { return nil }
	store.uploadBlobHook = func(context.Context, string, string, []byte, *azblob.UploadBufferOptions) error {
		return nil
	}
	require.NoError(t, store.Write(context.Background(), ExportObject{Container: "anything", Name: "a"}))
}

// TestAzureBlobHistoryStoreCreatesEachContainerOnce keeps a large export from issuing
// one container create per object.
func TestAzureBlobHistoryStoreCreatesEachContainerOnce(t *testing.T) {
	store, err := NewAzureBlobHistoryStore(AzureBlobHistoryStoreOptions{
		ConnectionString:  testConnectionString,
		ContainerName:     "primary",
		AllowAnyContainer: true,
	})
	require.NoError(t, err)

	var mu sync.Mutex
	creates := map[string]int{}
	store.createContainerHook = func(_ context.Context, container string) error {
		mu.Lock()
		defer mu.Unlock()
		creates[container]++
		return nil
	}
	store.uploadBlobHook = func(context.Context, string, string, []byte, *azblob.UploadBufferOptions) error {
		return nil
	}

	var group sync.WaitGroup
	for i := 0; i < 16; i++ {
		group.Add(1)
		go func(i int) {
			defer group.Done()
			container := "primary"
			if i%2 == 0 {
				container = "secondary"
			}
			assert.NoError(t, store.Write(context.Background(), ExportObject{Container: container, Name: "object"}))
		}(i)
	}
	group.Wait()

	mu.Lock()
	defer mu.Unlock()
	assert.Equal(t, 1, creates["primary"])
	assert.Equal(t, 1, creates["secondary"])
}

// TestAzureBlobHistoryStoreRetriesContainerBeingDeleted covers Azure keeping a deleted
// container's name reserved for a short window.
func TestAzureBlobHistoryStoreRetriesContainerBeingDeleted(t *testing.T) {
	store, err := NewAzureBlobHistoryStore(AzureBlobHistoryStoreOptions{
		ConnectionString: testConnectionString,
		ContainerName:    "primary",
	})
	require.NoError(t, err)

	attempts := 0
	store.createContainerHook = func(context.Context, string) error {
		attempts++
		if attempts < 3 {
			return &azcore.ResponseError{ErrorCode: string(bloberror.ContainerBeingDeleted)}
		}
		return nil
	}
	waits := []time.Duration{}
	store.waitHook = func(_ context.Context, d time.Duration) error {
		waits = append(waits, d)
		return nil
	}
	store.uploadBlobHook = func(context.Context, string, string, []byte, *azblob.UploadBufferOptions) error {
		return nil
	}

	require.NoError(t, store.Write(context.Background(), ExportObject{Container: "primary", Name: "object"}))
	assert.Equal(t, 3, attempts)
	assert.Equal(t, []time.Duration{
		containerBeingDeletedInitialBackoff,
		2 * containerBeingDeletedInitialBackoff,
	}, waits)
}

func TestAzureBlobHistoryStoreTreatsExistingContainerAsSuccess(t *testing.T) {
	store, err := NewAzureBlobHistoryStore(AzureBlobHistoryStoreOptions{
		ConnectionString: testConnectionString,
		ContainerName:    "primary",
	})
	require.NoError(t, err)
	store.createContainerHook = func(context.Context, string) error {
		return &azcore.ResponseError{ErrorCode: string(bloberror.ContainerAlreadyExists)}
	}
	uploaded := 0
	store.uploadBlobHook = func(context.Context, string, string, []byte, *azblob.UploadBufferOptions) error {
		uploaded++
		return nil
	}
	require.NoError(t, store.Write(context.Background(), ExportObject{Container: "primary", Name: "object"}))
	assert.Equal(t, 1, uploaded)
}

// TestAzureBlobHistoryStoreRetriesAfterContainerCreationFailure keeps a transient
// creation failure from permanently wedging the store.
func TestAzureBlobHistoryStoreRetriesAfterContainerCreationFailure(t *testing.T) {
	store, err := NewAzureBlobHistoryStore(AzureBlobHistoryStoreOptions{
		ConnectionString: testConnectionString,
		ContainerName:    "primary",
	})
	require.NoError(t, err)

	attempts := 0
	store.createContainerHook = func(context.Context, string) error {
		attempts++
		if attempts == 1 {
			return errors.New("transient")
		}
		return nil
	}
	store.uploadBlobHook = func(context.Context, string, string, []byte, *azblob.UploadBufferOptions) error {
		return nil
	}

	require.Error(t, store.Write(context.Background(), ExportObject{Container: "primary", Name: "object"}))
	require.NoError(t, store.Write(context.Background(), ExportObject{Container: "primary", Name: "object"}))
	assert.Equal(t, 2, attempts)
}

// TestAzureBlobHistoryStoreForgetsDeletedContainer covers a container removed between
// initialization and a later upload.
func TestAzureBlobHistoryStoreForgetsDeletedContainer(t *testing.T) {
	store, err := NewAzureBlobHistoryStore(AzureBlobHistoryStoreOptions{
		ConnectionString: testConnectionString,
		ContainerName:    "primary",
	})
	require.NoError(t, err)

	creates := 0
	store.createContainerHook = func(context.Context, string) error {
		creates++
		return nil
	}
	uploads := 0
	store.uploadBlobHook = func(context.Context, string, string, []byte, *azblob.UploadBufferOptions) error {
		uploads++
		if uploads == 2 {
			return &azcore.ResponseError{ErrorCode: string(bloberror.ContainerNotFound)}
		}
		return nil
	}

	ctx := context.Background()
	require.NoError(t, store.Write(ctx, ExportObject{Container: "primary", Name: "object"}))
	require.Error(t, store.Write(ctx, ExportObject{Container: "primary", Name: "object"}))
	require.NoError(t, store.Write(ctx, ExportObject{Container: "primary", Name: "object"}))
	assert.Equal(t, 2, creates)
}

// TestAzureBlobHistoryStoreReportsCreationFailureToWaiters keeps a caller that joined a
// failing container initialization from proceeding to upload as if the container
// existed.
func TestAzureBlobHistoryStoreReportsCreationFailureToWaiters(t *testing.T) {
	store, err := NewAzureBlobHistoryStore(AzureBlobHistoryStoreOptions{
		ConnectionString: testConnectionString,
		ContainerName:    "primary",
	})
	require.NoError(t, err)

	release := make(chan struct{})
	creationFailure := errors.New("creation denied")
	store.createContainerHook = func(context.Context, string) error {
		<-release
		return creationFailure
	}
	uploads := 0
	store.uploadBlobHook = func(context.Context, string, string, []byte, *azblob.UploadBufferOptions) error {
		uploads++
		return nil
	}

	const writers = 8
	errs := make(chan error, writers)
	for i := 0; i < writers; i++ {
		go func() {
			errs <- store.Write(context.Background(), ExportObject{Container: "primary", Name: "object"})
		}()
	}
	// Give every writer a chance to join the single in-flight initialization.
	time.Sleep(50 * time.Millisecond)
	close(release)

	for i := 0; i < writers; i++ {
		require.ErrorIs(t, <-errs, creationFailure, "writer %d", i)
	}
	assert.Zero(t, uploads, "no upload may run against a container that was never created")

	// The failed initialization is not cached, so a later write retries it.
	store.createContainerHook = func(context.Context, string) error { return nil }
	require.NoError(t, store.Write(context.Background(), ExportObject{Container: "primary", Name: "object"}))
	assert.Equal(t, 1, uploads)
}

// TestAzureBlobHistoryStoreInvalidationKeepsNewerInitialization keeps a stale
// ContainerNotFound from discarding a container initialization that started
// after the failing write began.
func TestAzureBlobHistoryStoreInvalidationKeepsNewerInitialization(t *testing.T) {
	store, err := NewAzureBlobHistoryStore(AzureBlobHistoryStoreOptions{
		ConnectionString: testConnectionString,
		ContainerName:    "primary",
	})
	require.NoError(t, err)
	creates := 0
	store.createContainerHook = func(context.Context, string) error {
		creates++
		return nil
	}
	store.uploadBlobHook = func(context.Context, string, string, []byte, *azblob.UploadBufferOptions) error {
		return nil
	}

	ctx := context.Background()
	stale, err := store.ensureContainer(ctx, "primary")
	require.NoError(t, err)
	store.forgetContainer("primary", stale)

	current, err := store.ensureContainer(ctx, "primary")
	require.NoError(t, err)
	require.NotSame(t, stale, current)

	// Retiring the already-retired initialization must not evict the current one.
	store.forgetContainer("primary", stale)
	again, err := store.ensureContainer(ctx, "primary")
	require.NoError(t, err)
	assert.Same(t, current, again)
	assert.Equal(t, 2, creates)
}

func TestAzureBlobHistoryStoreSendsHeadersAndMetadata(t *testing.T) {
	store, err := NewAzureBlobHistoryStore(AzureBlobHistoryStoreOptions{
		ConnectionString: testConnectionString,
		ContainerName:    "primary",
	})
	require.NoError(t, err)
	store.createContainerHook = func(context.Context, string) error { return nil }

	var captured *azblob.UploadBufferOptions
	var capturedBody []byte
	var capturedName string
	store.uploadBlobHook = func(
		_ context.Context,
		_ string,
		name string,
		body []byte,
		options *azblob.UploadBufferOptions,
	) error {
		capturedName, capturedBody, captured = name, body, options
		return nil
	}

	require.NoError(t, store.Write(context.Background(), ExportObject{
		Container:   "primary",
		Name:        "prefix/object.jsonl.gz",
		Content:     []byte("body"),
		ContentType: "application/gzip",
		Metadata:    map[string]string{"instanceId": "abc"},
	}))
	assert.Equal(t, "prefix/object.jsonl.gz", capturedName)
	assert.Equal(t, []byte("body"), capturedBody)
	require.NotNil(t, captured)
	require.NotNil(t, captured.HTTPHeaders.BlobContentType)
	assert.Equal(t, "application/gzip", *captured.HTTPHeaders.BlobContentType)
	// The store never declares a content coding, so no reader transparently
	// decompresses an object whose name promises gzip bytes.
	assert.Nil(t, captured.HTTPHeaders.BlobContentEncoding)
	require.NotNil(t, captured.Metadata["instanceId"])
	assert.Equal(t, "abc", *captured.Metadata["instanceId"])
}

func TestGzipContentRoundTrip(t *testing.T) {
	payload := []byte(strings.Repeat("history event\n", 512))
	compressed, err := gzipContent(payload)
	require.NoError(t, err)
	assert.Less(t, len(compressed), len(payload))
	assert.Equal(t, payload, decompressGzip(t, compressed))

	empty, err := gzipContent(nil)
	require.NoError(t, err)
	assert.Empty(t, decompressGzip(t, empty))
}

func decompressGzip(t *testing.T, content []byte) []byte {
	t.Helper()
	reader, err := gzip.NewReader(bytes.NewReader(content))
	require.NoError(t, err)
	decompressed, err := io.ReadAll(reader)
	require.NoError(t, err)
	require.NoError(t, reader.Close())
	return decompressed
}

// fakeCredential satisfies azcore.TokenCredential for construction-time tests
// that never issue a request.
type fakeCredential struct{}

func (fakeCredential) GetToken(context.Context, policy.TokenRequestOptions) (azcore.AccessToken, error) {
	return azcore.AccessToken{}, errors.New("not implemented")
}
