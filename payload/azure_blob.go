package payload

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/md5" //nolint:gosec // Azure Blob uses MD5 as a transport checksum.
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/bloberror"
	"github.com/microsoft/durabletask-go/api"
)

const (
	azureBlobTokenV1 = api.AzureBlobPayloadReferencePrefixV1
	azureBlobTokenV2 = api.AzureBlobPayloadReferencePrefixV2
)

// AzureBlobStoreOptions configures AzureBlobStore. Exactly one authentication
// mode must be supplied: ConnectionString, or AccountURL with Credential.
type AzureBlobStoreOptions struct {
	ConnectionString string
	AccountURL       string
	Credential       azcore.TokenCredential
	Container        string
	// CompressionEnabled defaults to true when nil.
	CompressionEnabled *bool
	// AllowedHosts explicitly permits cross-account blob:v2 reads with Credential.
	// .NET-authored blobs on these hosts may not include Go integrity metadata;
	// they remain bounded by MaxPayloadBytes.
	AllowedHosts      []string
	AllowInsecureHTTP bool
	MaxPayloadBytes   int
}

// AzureBlobStore stores payloads in Azure Blob Storage using .NET-compatible
// blob:v2 tokens. It also resolves .NET's legacy blob:v1 tokens.
type AzureBlobStore struct {
	client             *azblob.Client
	credential         azcore.TokenCredential
	clientOptions      *azblob.ClientOptions
	container          string
	containerURL       *url.URL
	allowHosts         map[string]struct{}
	allowInsecureHTTP  bool
	compressionEnabled bool
	maxPayloadBytes    int

	containerMu sync.Mutex
	// containerGeneration is the initialization currently believed valid, or
	// zero when the container still needs to be created. Generations are never
	// reused, so a stale upload cannot invalidate a newer initialization.
	containerGeneration     uint64
	nextContainerGeneration uint64
	containerInitDone       chan struct{}

	// Narrow hooks stand in for the individual *azblob.Client calls and the
	// backoff wait rather than for the client as a whole: azblob.Client is a
	// concrete struct, so the only alternatives are an HTTP-level fake, which
	// would test the SDK instead of this file, or a wrapper interface far wider
	// than the three operations whose ordering and retry behavior matter here.
	// Every hook is nil in production, where the real client is used.
	createContainerHook func(ctx context.Context) error
	uploadBlobHook      func(ctx context.Context, name string, body []byte, options *azblob.UploadBufferOptions) error
	waitHook            func(ctx context.Context, d time.Duration) error
}

// Azure keeps a deleted container's name reserved until the delete finishes,
// rejecting recreation with ContainerBeingDeleted for anywhere up to about
// half a minute. Recreation therefore retries with a capped exponential
// backoff instead of failing the payload write.
const (
	containerBeingDeletedAttempts       = 9
	containerBeingDeletedInitialBackoff = 250 * time.Millisecond
	containerBeingDeletedMaxBackoff     = 8 * time.Second
)

var (
	_ api.LargePayloadStore              = (*AzureBlobStore)(nil)
	_ api.LargePayloadResolver           = (*AzureBlobStore)(nil)
	_ api.LargePayloadTokenStore         = (*AzureBlobStore)(nil)
	_ api.LargePayloadTokenValidator     = (*AzureBlobStore)(nil)
	_ api.LargePayloadDefaults           = (*AzureBlobStore)(nil)
	_ api.InclusiveLargePayloadThreshold = (*AzureBlobStore)(nil)
	_ api.LargePayloadOptionsValidator   = (*AzureBlobStore)(nil)
)

// NewAzureBlobStore constructs a production Azure Blob Storage payload store.
func NewAzureBlobStore(options AzureBlobStoreOptions) (*AzureBlobStore, error) {
	container := options.Container
	if container == "" {
		container = "durabletask-payloads"
	}
	if !validContainer(container) {
		return nil, fmt.Errorf("%w: invalid Azure Blob container name", api.ErrLargePayloadReference)
	}
	if (options.ConnectionString == "") == (options.AccountURL == "") {
		return nil, errors.New("exactly one of connection string or account URL is required")
	}
	if options.ConnectionString == "" && options.Credential == nil {
		return nil, errors.New("azure blob account URL requires a token credential")
	}
	if options.ConnectionString != "" && options.Credential != nil {
		return nil, errors.New("connection string and token credential cannot be combined")
	}
	compressionEnabled := true
	if options.CompressionEnabled != nil {
		compressionEnabled = *options.CompressionEnabled
	}
	maxPayloadBytes := api.DefaultAzureBlobPayloadMaxBytes
	if options.MaxPayloadBytes != 0 {
		maxPayloadBytes = options.MaxPayloadBytes
	}
	if maxPayloadBytes <= 0 {
		return nil, errors.New("azure blob maximum payload size must be greater than zero")
	}
	clientOptions := &azblob.ClientOptions{ClientOptions: azcore.ClientOptions{Retry: policy.RetryOptions{
		MaxRetries:    8,
		RetryDelay:    250 * time.Millisecond,
		MaxRetryDelay: 10 * time.Second,
		TryTimeout:    2 * time.Minute,
	}}}
	var (
		client *azblob.Client
		err    error
	)
	if options.ConnectionString != "" {
		client, err = azblob.NewClientFromConnectionString(options.ConnectionString, clientOptions)
	} else {
		accountURL, parseErr := url.Parse(options.AccountURL)
		if parseErr != nil || !isSafeBlobURL(accountURL, options.AllowInsecureHTTP) {
			return nil, fmt.Errorf("%w: invalid Azure Blob account URL", api.ErrLargePayloadReference)
		}
		client, err = azblob.NewClient(options.AccountURL, options.Credential, clientOptions)
	}
	if err != nil {
		return nil, fmt.Errorf("create Azure Blob client: %w", err)
	}
	serviceURL, err := url.Parse(client.URL())
	if err != nil {
		return nil, fmt.Errorf("%w: Azure Blob service URL is not allowed", api.ErrLargePayloadReference)
	}
	publicURL := *serviceURL
	publicURL.RawQuery, publicURL.Fragment, publicURL.User = "", "", nil
	publicURL.ForceQuery = false
	if !isSafeBlobURL(&publicURL, options.AllowInsecureHTTP) {
		return nil, fmt.Errorf("%w: Azure Blob service URL is not allowed", api.ErrLargePayloadReference)
	}
	containerURL := publicURL
	containerURL.Path = strings.TrimRight(publicURL.Path, "/") + "/" + container
	containerURL.RawPath = ""
	allowedHosts := make(map[string]struct{}, len(options.AllowedHosts))
	for _, host := range options.AllowedHosts {
		host = normalizedHost(host)
		if host == "" {
			return nil, errors.New("azure blob allowed host cannot be empty")
		}
		allowedHosts[host] = struct{}{}
	}
	return &AzureBlobStore{
		client:             client,
		credential:         options.Credential,
		clientOptions:      clientOptions,
		container:          container,
		containerURL:       &containerURL,
		allowHosts:         allowedHosts,
		allowInsecureHTTP:  options.AllowInsecureHTTP,
		compressionEnabled: compressionEnabled,
		maxPayloadBytes:    maxPayloadBytes,
	}, nil
}

// LargePayloadDefaults supplies Azure Blob's .NET-compatible defaults.
func (s *AzureBlobStore) LargePayloadDefaults() (thresholdBytes, maxPayloadBytes int) {
	return api.DefaultAzureBlobPayloadThresholdBytes, s.maxPayloadBytes
}

// UsesInclusiveLargePayloadThreshold matches the .NET Azure Blob extension.
func (*AzureBlobStore) UsesInclusiveLargePayloadThreshold() bool { return true }

// ValidateLargePayloadOptions enforces the .NET Azure Blob extension's
// threshold limit and the store's configured payload cap.
func (s *AzureBlobStore) ValidateLargePayloadOptions(thresholdBytes, maxPayloadBytes int) error {
	const maxThreshold = 1024 * 1024
	if thresholdBytes > maxThreshold {
		return fmt.Errorf("azure blob payload threshold cannot exceed %d bytes", maxThreshold)
	}
	if maxPayloadBytes > s.maxPayloadBytes {
		return fmt.Errorf(
			"large payload maximum %d exceeds Azure Blob store maximum %d",
			maxPayloadBytes,
			s.maxPayloadBytes,
		)
	}
	return nil
}

func (s *AzureBlobStore) Store(ctx context.Context, payload []byte) (string, error) {
	if len(payload) > s.maxPayloadBytes {
		return "", fmt.Errorf("%w: %d bytes exceeds %d", api.ErrLargePayloadTooLarge, len(payload), s.maxPayloadBytes)
	}
	name, err := randomBlobName()
	if err != nil {
		return "", err
	}
	body := payload
	headers := &blob.HTTPHeaders{}
	if s.compressionEnabled {
		body, err = gzipPayload(payload)
		if err != nil {
			return "", err
		}
		gzipEncoding := "gzip"
		headers.BlobContentEncoding = &gzipEncoding
	}
	bodyDigest := md5.Sum(body)
	headers.BlobContentMD5 = bodyDigest[:]
	digest := sha256.Sum256(payload)
	size := strconv.Itoa(len(payload))
	hash := hex.EncodeToString(digest[:])
	metadata := map[string]*string{"durabletask_size": &size, "durabletask_sha256": &hash}
	// A concurrently deleted container is recreated once before giving up.
	for attempt := 0; ; attempt++ {
		generation, err := s.ensureContainer(ctx)
		if err != nil {
			return "", err
		}
		err = s.uploadBlob(ctx, name, body, &azblob.UploadBufferOptions{
			HTTPHeaders: headers,
			Metadata:    metadata,
		})
		if err == nil {
			return s.blobURL(name).String(), nil
		}
		if attempt > 0 || !bloberror.HasCode(err, bloberror.ContainerNotFound) {
			return "", fmt.Errorf("upload Azure Blob payload: %w", err)
		}
		s.invalidateContainer(generation)
	}
}

func (s *AzureBlobStore) Resolve(ctx context.Context, location string) ([]byte, error) {
	ref, err := s.parseToken(azureBlobTokenV2 + location)
	if err != nil || !ref.same || ref.container != s.container {
		return nil, fmt.Errorf("%w: invalid Azure Blob location", api.ErrLargePayloadReference)
	}
	return s.download(ctx, ref)
}

// StoreToken stores a payload and returns the unwrapped .NET blob:v2 token.
func (s *AzureBlobStore) StoreToken(ctx context.Context, payload []byte) (string, error) {
	location, err := s.Store(ctx, payload)
	if err != nil {
		return "", err
	}
	return azureBlobTokenV2 + location, nil
}

// IsLargePayloadToken recognizes both .NET Azure Blob token versions.
func (*AzureBlobStore) IsLargePayloadToken(value string) bool {
	return strings.HasPrefix(value, azureBlobTokenV1) || strings.HasPrefix(value, azureBlobTokenV2)
}

// ValidateLargePayloadToken validates a recognized token without accessing the network.
func (s *AzureBlobStore) ValidateLargePayloadToken(token string) error {
	_, err := s.parseToken(token)
	return err
}

// ResolveToken resolves a .NET blob:v1 or blob:v2 token.
func (s *AzureBlobStore) ResolveToken(ctx context.Context, token string) ([]byte, error) {
	parsed, err := s.parseToken(token)
	if err != nil {
		return nil, err
	}
	return s.download(ctx, parsed)
}

type azureBlobReference struct {
	container string
	name      string
	blobURL   *url.URL
	same      bool
}

func (s *AzureBlobStore) parseToken(token string) (azureBlobReference, error) {
	if strings.HasPrefix(token, azureBlobTokenV1) {
		parts := strings.SplitN(strings.TrimPrefix(token, azureBlobTokenV1), ":", 2)
		if len(parts) != 2 || parts[0] != s.container || parts[1] == "" {
			return azureBlobReference{}, fmt.Errorf("%w: invalid blob:v1 token", api.ErrLargePayloadReference)
		}
		return azureBlobReference{container: s.container, name: parts[1], same: true}, nil
	}
	if !strings.HasPrefix(token, azureBlobTokenV2) {
		return azureBlobReference{}, fmt.Errorf("%w: invalid Azure Blob token prefix", api.ErrLargePayloadReference)
	}
	blobURL, err := url.Parse(strings.TrimPrefix(token, azureBlobTokenV2))
	if err != nil || !isSafeBlobURL(blobURL, s.allowInsecureHTTP) {
		return azureBlobReference{}, fmt.Errorf("%w: invalid blob:v2 URL", api.ErrLargePayloadReference)
	}
	container, name, err := parseAzureBlobPath(blobURL)
	if err != nil || !validContainer(container) || name == "" {
		return azureBlobReference{}, fmt.Errorf("%w: invalid blob:v2 path", api.ErrLargePayloadReference)
	}
	same := sameURLContainer(blobURL, s.containerURL)
	if !same {
		if s.credential == nil {
			return azureBlobReference{}, fmt.Errorf("%w: cross-account Azure Blob tokens require a token credential", api.ErrLargePayloadReference)
		}
		if _, allowed := s.allowHosts[normalizedHost(blobURL.Host)]; !allowed {
			return azureBlobReference{}, fmt.Errorf("%w: Azure Blob token host is not allowed", api.ErrLargePayloadReference)
		}
	}
	return azureBlobReference{container: container, name: name, blobURL: blobURL, same: same}, nil
}

func (s *AzureBlobStore) download(ctx context.Context, ref azureBlobReference) ([]byte, error) {
	var (
		response azblob.DownloadStreamResponse
		err      error
	)
	if ref.same {
		response, err = s.client.DownloadStream(ctx, s.container, ref.name, nil)
	} else {
		client, clientErr := azblob.NewClient(blobAccountURL(ref.blobURL).String(), s.credential, s.clientOptions)
		if clientErr != nil {
			return nil, fmt.Errorf("create cross-account Azure Blob client: %w", clientErr)
		}
		response, err = client.DownloadStream(ctx, ref.container, ref.name, nil)
	}
	if err != nil {
		if bloberror.HasCode(err, bloberror.BlobNotFound, bloberror.ContainerNotFound) {
			return nil, fmt.Errorf(
				"%w: blob %q was not found in container %q",
				api.ErrLargePayloadReference,
				ref.name,
				ref.container,
			)
		}
		return nil, fmt.Errorf("download Azure Blob payload: %w", err)
	}
	defer response.Body.Close() //nolint:errcheck // read-only cleanup
	if response.ContentLength != nil && *response.ContentLength > int64(s.maxPayloadBytes)*2 {
		return nil, fmt.Errorf("%w: compressed payload exceeds configured bound", api.ErrLargePayloadTooLarge)
	}
	reader := io.Reader(response.Body)
	if response.ContentEncoding != nil && strings.EqualFold(*response.ContentEncoding, "gzip") {
		gzipReader, gzipErr := gzip.NewReader(reader)
		if gzipErr != nil {
			return nil, fmt.Errorf("%w: invalid gzip payload", api.ErrLargePayloadIntegrity)
		}
		defer gzipReader.Close() //nolint:errcheck // read-only cleanup
		reader = gzipReader
	}
	payload, err := readBounded(reader, s.maxPayloadBytes)
	if err != nil {
		return nil, err
	}
	if err := verifyMetadata(payload, response.Metadata); err != nil {
		return nil, err
	}
	return payload, nil
}

// ensureContainer creates the payload container once and caches the result.
// The returned generation identifies the initialization the caller relied on so
// a later ContainerNotFound failure can invalidate exactly that generation.
func (s *AzureBlobStore) ensureContainer(ctx context.Context) (uint64, error) {
	for {
		s.containerMu.Lock()
		if s.containerGeneration != 0 {
			generation := s.containerGeneration
			s.containerMu.Unlock()
			return generation, nil
		}
		if done := s.containerInitDone; done != nil {
			s.containerMu.Unlock()
			// Checking ctx before the select is load bearing: when done is
			// already closed, select would otherwise pick pseudo-randomly
			// between the two ready cases and an already cancelled caller
			// could still be reported as successful.
			if err := ctx.Err(); err != nil {
				return 0, err
			}
			// Waiting is cancellable, but a waiter's cancellation must never
			// cancel the in-flight initializer or the work it is creating for.
			select {
			case <-ctx.Done():
				return 0, ctx.Err()
			case <-done:
			}
			continue
		}
		done := make(chan struct{})
		s.containerInitDone = done
		s.containerMu.Unlock()

		err := s.createContainerWithRetry(ctx)

		s.containerMu.Lock()
		s.containerInitDone = nil
		if err != nil && !bloberror.HasCode(err, bloberror.ContainerAlreadyExists) {
			// A failed or cancelled initializer publishes no generation, so the
			// next caller retries initialization with its own context.
			s.containerMu.Unlock()
			close(done)
			return 0, fmt.Errorf("create Azure Blob payload container: %w", err)
		}
		s.nextContainerGeneration++
		generation := s.nextContainerGeneration
		s.containerGeneration = generation
		s.containerMu.Unlock()
		close(done)
		return generation, nil
	}
}

func (s *AzureBlobStore) invalidateContainer(generation uint64) {
	s.containerMu.Lock()
	defer s.containerMu.Unlock()
	if s.containerGeneration == generation {
		s.containerGeneration = 0
	}
}

// createContainerWithRetry creates the container, waiting out an in-progress
// delete of a container with the same name. Any other outcome, including
// cancellation and ContainerAlreadyExists, is returned to the caller
// unchanged on the first attempt.
func (s *AzureBlobStore) createContainerWithRetry(ctx context.Context) error {
	backoff := containerBeingDeletedInitialBackoff
	for attempt := 1; ; attempt++ {
		err := s.createContainer(ctx)
		if attempt == containerBeingDeletedAttempts ||
			!bloberror.HasCode(err, bloberror.ContainerBeingDeleted) {
			return err
		}
		if waitErr := s.waitForRetry(ctx, backoff); waitErr != nil {
			return waitErr
		}
		backoff = min(backoff*2, containerBeingDeletedMaxBackoff)
	}
}

func (s *AzureBlobStore) createContainer(ctx context.Context) error {
	if s.createContainerHook != nil {
		return s.createContainerHook(ctx)
	}
	_, err := s.client.CreateContainer(ctx, s.container, nil)
	return err
}

func (s *AzureBlobStore) uploadBlob(
	ctx context.Context,
	name string,
	body []byte,
	options *azblob.UploadBufferOptions,
) error {
	if s.uploadBlobHook != nil {
		return s.uploadBlobHook(ctx, name, body, options)
	}
	_, err := s.client.UploadBuffer(ctx, s.container, name, body, options)
	return err
}

// waitForRetry waits for d, reporting cancellation instead of blocking through
// it.
func (s *AzureBlobStore) waitForRetry(ctx context.Context, d time.Duration) error {
	if s.waitHook != nil {
		return s.waitHook(ctx, d)
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func (s *AzureBlobStore) blobURL(name string) *url.URL {
	u := *s.containerURL
	u.Path = strings.TrimRight(u.Path, "/") + "/" + name
	return &u
}

func parseAzureBlobPath(u *url.URL) (string, string, error) {
	segments := strings.Split(strings.Trim(u.EscapedPath(), "/"), "/")
	pathStyle := net.ParseIP(u.Hostname()) != nil || strings.EqualFold(u.Hostname(), "localhost")
	if pathStyle {
		if len(segments) < 3 || segments[0] == "" {
			return "", "", errors.New("invalid path-style Azure Blob URL")
		}
		segments = segments[1:]
	}
	if len(segments) < 2 {
		return "", "", errors.New("invalid Azure Blob URL path")
	}
	container, err := url.PathUnescape(segments[0])
	if err != nil {
		return "", "", err
	}
	name, err := url.PathUnescape(strings.Join(segments[1:], "/"))
	if err != nil || name == "" {
		return "", "", errors.New("invalid Azure Blob name")
	}
	return container, name, nil
}

func blobAccountURL(blobURL *url.URL) *url.URL {
	u := *blobURL
	segments := strings.Split(strings.Trim(blobURL.EscapedPath(), "/"), "/")
	if net.ParseIP(blobURL.Hostname()) != nil || strings.EqualFold(blobURL.Hostname(), "localhost") {
		u.Path = "/" + segments[0] + "/"
	} else {
		u.Path = "/"
	}
	u.RawPath, u.RawQuery, u.Fragment, u.User = "", "", "", nil
	return &u
}

func sameURLContainer(blobURL, containerURL *url.URL) bool {
	if blobURL == nil || containerURL == nil {
		return false
	}
	tokenContainer := *blobURL
	container, _, err := parseAzureBlobPath(&tokenContainer)
	if err != nil {
		return false
	}
	expected := *containerURL
	expected.Path = strings.TrimRight(expected.Path, "/")
	tokenContainer.Path = strings.TrimRight(blobAccountURL(&tokenContainer).Path, "/") + "/" + container
	tokenContainer.RawPath, tokenContainer.RawQuery, tokenContainer.Fragment, tokenContainer.User = "", "", "", nil
	return strings.EqualFold(tokenContainer.Scheme, expected.Scheme) &&
		strings.EqualFold(tokenContainer.Hostname(), expected.Hostname()) &&
		effectivePort(&tokenContainer) == effectivePort(&expected) &&
		tokenContainer.Path == expected.Path
}

func effectivePort(u *url.URL) string {
	if port := u.Port(); port != "" {
		return port
	}
	if strings.EqualFold(u.Scheme, "https") {
		return "443"
	}
	if strings.EqualFold(u.Scheme, "http") {
		return "80"
	}
	return ""
}

func isSafeBlobURL(u *url.URL, allowHTTP bool) bool {
	return u != nil && u.IsAbs() && u.Host != "" && u.User == nil && u.RawQuery == "" && u.Fragment == "" &&
		(u.Scheme == "https" || (allowHTTP && u.Scheme == "http"))
}

func validContainer(container string) bool {
	if len(container) < 3 || len(container) > 63 || container[0] == '-' || container[len(container)-1] == '-' {
		return false
	}
	for _, r := range container {
		if (r < 'a' || r > 'z') && (r < '0' || r > '9') && r != '-' {
			return false
		}
	}
	return !strings.Contains(container, "--")
}

func normalizedHost(host string) string {
	host = strings.TrimSpace(strings.ToLower(host))
	if parsedHost, _, err := net.SplitHostPort(host); err == nil {
		return parsedHost
	}
	return strings.Trim(host, "[]")
}

func randomBlobName() (string, error) {
	var bytes [16]byte
	if _, err := rand.Read(bytes[:]); err != nil {
		return "", fmt.Errorf("generate Azure Blob payload name: %w", err)
	}
	return hex.EncodeToString(bytes[:]), nil
}

func gzipPayload(payload []byte) ([]byte, error) {
	var buffer bytes.Buffer
	writer := gzip.NewWriter(&buffer)
	if _, err := writer.Write(payload); err != nil {
		return nil, err
	}
	if err := writer.Close(); err != nil {
		return nil, err
	}
	return buffer.Bytes(), nil
}

func readBounded(reader io.Reader, max int) ([]byte, error) {
	payload, err := io.ReadAll(io.LimitReader(reader, int64(max)+1))
	if err != nil {
		return nil, fmt.Errorf("read Azure Blob payload: %w", err)
	}
	if len(payload) > max {
		return nil, fmt.Errorf("%w: payload exceeds %d bytes", api.ErrLargePayloadTooLarge, max)
	}
	return payload, nil
}

func verifyMetadata(payload []byte, metadata map[string]*string) error {
	size, hasSize := metadataValue(metadata, "durabletask_size")
	hash, hasHash := metadataValue(metadata, "durabletask_sha256")
	if !hasSize && !hasHash {
		return nil
	}
	if !hasSize || !hasHash || size == nil || hash == nil {
		return fmt.Errorf("%w: incomplete Azure Blob payload metadata", api.ErrLargePayloadIntegrity)
	}
	expectedSize, err := strconv.Atoi(*size)
	if err != nil || expectedSize != len(payload) {
		return fmt.Errorf("%w: Azure Blob payload size mismatch", api.ErrLargePayloadIntegrity)
	}
	expectedHash, err := hex.DecodeString(*hash)
	digest := sha256.Sum256(payload)
	if err != nil || len(expectedHash) != len(digest) || subtle.ConstantTimeCompare(digest[:], expectedHash) != 1 {
		return fmt.Errorf("%w: Azure Blob payload hash mismatch", api.ErrLargePayloadIntegrity)
	}
	return nil
}

func metadataValue(metadata map[string]*string, name string) (*string, bool) {
	for key, value := range metadata {
		if strings.EqualFold(key, name) {
			return value, true
		}
	}
	return nil, false
}
