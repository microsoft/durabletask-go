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
	"net/http"
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
	// AllowAllContainersInConfiguredAccount permits reads in every container
	// at the primary account endpoint, using its configured authentication.
	AllowAllContainersInConfiguredAccount bool
	// AdditionalAccountURLs lists absolute HTTPS Blob account service endpoints
	// whose containers may be read using Credential. Requires AccountURL plus
	// Credential; connection-string authentication cannot use additional accounts.
	// This list does not widen the primary account's container scope.
	AdditionalAccountURLs []string
	AllowInsecureHTTP     bool
	MaxPayloadBytes       int
}

// AzureBlobStore stores payloads in Azure Blob Storage using .NET-compatible
// blob:v2 tokens. It also resolves .NET's legacy blob:v1 tokens.
type AzureBlobStore struct {
	client             *azblob.Client
	container          string
	containerURL       *url.URL
	primaryAccount     string
	additionalAccounts map[string]*azblob.Client
	allowAllContainers bool
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
	return newAzureBlobStore(options, newAzureBlobClientOptions())
}

func newAzureBlobClientOptions() *azblob.ClientOptions {
	return &azblob.ClientOptions{ClientOptions: azcore.ClientOptions{
		Retry: policy.RetryOptions{
			MaxRetries:    8,
			RetryDelay:    250 * time.Millisecond,
			MaxRetryDelay: 10 * time.Second,
			TryTimeout:    2 * time.Minute,
		},
		Transport: &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		}},
	}}
}

func newAzureBlobStore(options AzureBlobStoreOptions, clientOptions *azblob.ClientOptions) (*AzureBlobStore, error) {
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
	if len(options.AdditionalAccountURLs) != 0 && options.ConnectionString != "" {
		return nil, errors.New("additional Azure Blob accounts require AccountURL and a token credential")
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
	var (
		client *azblob.Client
		err    error
	)
	if options.ConnectionString != "" {
		client, err = azblob.NewClientFromConnectionString(options.ConnectionString, clientOptions)
	} else {
		accountURL, parseErr := url.Parse(options.AccountURL)
		if parseErr != nil || strings.ContainsAny(options.AccountURL, "?#") {
			return nil, fmt.Errorf("%w: invalid Azure Blob account URL", api.ErrLargePayloadReference)
		}
		accountURL, err = canonicalBlobAccountURL(accountURL, options.AllowInsecureHTTP)
		if err != nil {
			return nil, err
		}
		client, err = azblob.NewClient(accountURL.String(), options.Credential, clientOptions)
	}
	if err != nil {
		return nil, fmt.Errorf("create Azure Blob client: %w", err)
	}
	serviceURL, err := url.Parse(client.URL())
	if err != nil {
		return nil, fmt.Errorf("%w: Azure Blob service URL is not allowed", api.ErrLargePayloadReference)
	}
	publicURL := credentialFreeBlobURL(serviceURL)
	accountURL, err := canonicalBlobAccountURL(publicURL, options.AllowInsecureHTTP)
	if err != nil || serviceURL.User != nil || strings.Contains(client.URL(), "#") {
		return nil, fmt.Errorf("%w: invalid Azure Blob service endpoint", api.ErrLargePayloadReference)
	}
	containerURL := *accountURL
	containerURL.Path += "/" + container
	containerURL.RawPath = ""
	additional := make(map[string]*azblob.Client, len(options.AdditionalAccountURLs))
	for _, raw := range options.AdditionalAccountURLs {
		endpoint, parseErr := url.Parse(raw)
		if parseErr != nil || strings.ContainsAny(raw, "?#") {
			return nil, fmt.Errorf("%w: invalid additional Azure Blob account URL", api.ErrLargePayloadReference)
		}
		endpoint, parseErr = canonicalBlobAccountURL(endpoint, false)
		if parseErr != nil {
			return nil, parseErr
		}
		key := endpoint.String()
		if key == accountURL.String() {
			return nil, fmt.Errorf("%w: additional account repeats the primary endpoint; use AllowAllContainersInConfiguredAccount", api.ErrLargePayloadReference)
		}
		if _, exists := additional[key]; exists {
			continue
		}
		additionalClient, clientErr := azblob.NewClient(key, options.Credential, clientOptions)
		if clientErr != nil {
			return nil, fmt.Errorf("create additional Azure Blob client: %w", clientErr)
		}
		additional[key] = additionalClient
	}
	return &AzureBlobStore{
		client:             client,
		container:          container,
		containerURL:       &containerURL,
		primaryAccount:     accountURL.String(),
		additionalAccounts: additional,
		allowAllContainers: options.AllowAllContainersInConfiguredAccount,
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
	return s.ResolveToken(ctx, azureBlobTokenV2+location)
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
	client    *azblob.Client
}

func (s *AzureBlobStore) parseToken(token string) (azureBlobReference, error) {
	account := s.primaryAccount
	var container, name string
	if strings.HasPrefix(token, azureBlobTokenV1) {
		parts := strings.SplitN(strings.TrimPrefix(token, azureBlobTokenV1), ":", 2)
		if len(parts) != 2 {
			return azureBlobReference{}, fmt.Errorf("%w: invalid blob:v1 token", api.ErrLargePayloadReference)
		}
		container, name = parts[0], parts[1]
	} else if strings.HasPrefix(token, azureBlobTokenV2) {
		raw := strings.TrimPrefix(token, azureBlobTokenV2)
		// Older tokens can contain SAS, snapshot, version or fragment fields.
		// They describe neither the destination policy nor its authentication.
		if i := strings.IndexAny(raw, "?#"); i >= 0 {
			raw = raw[:i]
		}
		blobURL, err := url.Parse(raw)
		if err != nil || !isSafeBlobURL(blobURL, s.allowInsecureHTTP) {
			return azureBlobReference{}, fmt.Errorf("%w: invalid blob:v2 URL", api.ErrLargePayloadReference)
		}
		account, container, name, err = parseAzureBlobDestination(blobURL)
		if err != nil {
			return azureBlobReference{}, err
		}
	} else {
		return azureBlobReference{}, fmt.Errorf("%w: invalid Azure Blob token prefix", api.ErrLargePayloadReference)
	}
	if !validContainer(container) || name == "" || !validBlobPath(name) {
		return azureBlobReference{}, fmt.Errorf("%w: invalid Azure Blob path", api.ErrLargePayloadReference)
	}
	client := s.client
	if account == s.primaryAccount {
		if container != s.container && !s.allowAllContainers {
			return azureBlobReference{}, fmt.Errorf("%w: Azure Blob container is not allowed", api.ErrLargePayloadReference)
		}
	} else {
		client = s.additionalAccounts[account]
		if client == nil {
			return azureBlobReference{}, fmt.Errorf("%w: Azure Blob account endpoint is not allowed", api.ErrLargePayloadReference)
		}
	}
	// Check the SDK's actual joined/escaped destination before its pipeline
	// can acquire credentials or send a request, including literal v1 names.
	finalURL, err := url.Parse(client.ServiceClient().NewContainerClient(container).NewBlobClient(name).URL())
	if err != nil {
		return azureBlobReference{}, fmt.Errorf("%w: invalid constructed Azure Blob URL", api.ErrLargePayloadReference)
	}
	finalAccount, finalContainer, finalName, err := parseAzureBlobDestination(credentialFreeBlobURL(finalURL))
	if err != nil || finalAccount != account || finalContainer != container || finalName != name {
		return azureBlobReference{}, fmt.Errorf("%w: constructed Azure Blob destination does not match the authorized location", api.ErrLargePayloadReference)
	}
	return azureBlobReference{container: container, name: name, client: client}, nil
}

func (s *AzureBlobStore) download(ctx context.Context, ref azureBlobReference) ([]byte, error) {
	response, err := ref.client.DownloadStream(ctx, ref.container, ref.name, nil)
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

func credentialFreeBlobURL(u *url.URL) *url.URL {
	clean := *u
	clean.RawQuery, clean.Fragment, clean.RawFragment = "", "", ""
	clean.ForceQuery = false
	return &clean
}

func canonicalBlobAccountURL(u *url.URL, allowHTTP bool) (*url.URL, error) {
	invalid := fmt.Errorf("%w: invalid Azure Blob account service endpoint", api.ErrLargePayloadReference)
	if !isSafeBlobURL(u, allowHTTP) || !validBlobHostname(u.Hostname()) {
		return nil, invalid
	}
	canonical := *u
	canonical.Path = strings.TrimRight(u.Path, "/")
	canonical.RawPath = ""
	parts, err := blob.ParseURL(canonical.String())
	if err != nil || parts.ContainerName != "" || parts.BlobName != "" {
		return nil, invalid
	}
	if net.ParseIP(u.Hostname()) != nil {
		account := parts.IPEndpointStyleInfo.AccountName
		if account == "" || strings.ContainsAny(account, "/\\% ") || canonical.Path != "/"+account {
			return nil, invalid
		}
	} else if canonical.Path != "" {
		return nil, invalid
	}
	port, err := strconv.Atoi(effectivePort(u))
	if err != nil || port < 1 || port > 65535 {
		return nil, invalid
	}
	host := strings.ToLower(u.Hostname())
	canonical.Host = host
	if strings.Contains(host, ":") {
		canonical.Host = "[" + host + "]"
	}
	if (u.Scheme != "https" || port != 443) && (u.Scheme != "http" || port != 80) {
		canonical.Host = net.JoinHostPort(host, strconv.Itoa(port))
	}
	return &canonical, nil
}

func validBlobHostname(host string) bool {
	if net.ParseIP(host) != nil {
		return true
	}
	if len(host) > 253 {
		return false
	}
	for _, label := range strings.Split(strings.TrimSuffix(host, "."), ".") {
		if label == "" || len(label) > 63 || label[0] == '-' || label[len(label)-1] == '-' {
			return false
		}
		for _, r := range label {
			if (r < 'a' || r > 'z') && (r < 'A' || r > 'Z') && (r < '0' || r > '9') && r != '-' {
				return false
			}
		}
	}
	return true
}

func parseAzureBlobDestination(u *url.URL) (account, container, name string, err error) {
	invalid := fmt.Errorf("%w: invalid Azure Blob destination path", api.ErrLargePayloadReference)
	if !isSafeBlobURL(u, true) {
		return "", "", "", invalid
	}
	serviceURL := *u
	serviceURL.Path, serviceURL.RawPath = "", ""
	path := strings.TrimPrefix(u.EscapedPath(), "/")
	if net.ParseIP(u.Hostname()) != nil {
		var rawAccount string
		var found bool
		rawAccount, path, found = strings.Cut(path, "/")
		decoded, decodeErr := url.PathUnescape(rawAccount)
		if !found || decodeErr != nil || strings.ContainsAny(decoded, "/\\") {
			return "", "", "", invalid
		}
		serviceURL.Path = "/" + decoded
	}
	rawContainer, rawName, found := strings.Cut(path, "/")
	if !found {
		return "", "", "", invalid
	}
	container, err = url.PathUnescape(rawContainer)
	if err != nil || !validContainer(container) {
		return "", "", "", invalid
	}
	name, err = url.PathUnescape(rawName)
	if err != nil || name == "" || !validBlobPath(name) {
		return "", "", "", invalid
	}
	endpoint, err := canonicalBlobAccountURL(&serviceURL, true)
	if err != nil {
		return "", "", "", err
	}
	// Match the SDK's own once-decoded interpretation, not a URL-prefix guess.
	parts, err := blob.ParseURL(u.String())
	if err != nil || parts.ContainerName != container || parts.BlobName != name {
		return "", "", "", invalid
	}
	return endpoint.String(), container, name, nil
}

func validBlobPath(path string) bool {
	for _, r := range path {
		if r == '\\' || r < 32 || r == 127 {
			return false
		}
	}
	for _, segment := range strings.Split(path, "/") {
		if segment == "." || segment == ".." {
			return false
		}
	}
	return true
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
	return u != nil && u.IsAbs() && u.Opaque == "" && u.Hostname() != "" && u.User == nil &&
		u.RawQuery == "" && !u.ForceQuery && u.Fragment == "" && u.RawFragment == "" && validBlobPath(u.Path) &&
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
