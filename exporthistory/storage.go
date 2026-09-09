package exporthistory

import (
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"net"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/bloberror"
)

// ExportObject is one serialized orchestration history ready to be persisted.
type ExportObject struct {
	// Container is the destination container name.
	Container string
	// Name is the full object path inside the container, prefix included.
	Name string
	// Content is the object body, already compressed when the format requires it.
	Content []byte
	// ContentType is the MIME type of Content. A compressed format reports the
	// compressed type, such as application/gzip, rather than declaring the
	// compression as a separate content coding, so a reader always receives the
	// exact bytes the object name describes.
	ContentType string
	// Metadata is attached to the stored object.
	Metadata map[string]string
}

// Store persists exported history objects. It is deliberately narrower than
// [github.com/microsoft/durabletask-go/payload.AzureBlobStore], whose
// large-payload contract assigns random object names and owns a single
// container, because export jobs choose deterministic names in a caller-chosen
// container.
//
// Implementations must be safe for concurrent use and should treat a repeated
// write of the same object name as an overwrite, since an activity retry can
// re-export an instance whose object already exists.
type Store interface {
	Write(ctx context.Context, object ExportObject) error
}

// AzureBlobHistoryStoreOptions configures [AzureBlobHistoryStore]. Exactly one authentication
// mode must be supplied: ConnectionString, or AccountURL with Credential.
type AzureBlobHistoryStoreOptions struct {
	ConnectionString string
	AccountURL       string
	Credential       azcore.TokenCredential
	// ContainerName is the default destination container. It is required so a
	// misconfigured worker fails at construction instead of at export time.
	ContainerName string
	// AllowInsecureHTTP permits plaintext endpoints such as Azurite.
	AllowInsecureHTTP bool
	// AllowedContainers optionally restricts the containers an export job may
	// write to. When empty only ContainerName is allowed.
	AllowedContainers []string
	// AllowAnyContainer disables container allow-listing entirely.
	AllowAnyContainer bool
}

// AzureBlobHistoryStore writes exported history objects to Azure Blob Storage.
type AzureBlobHistoryStore struct {
	client            *azblob.Client
	defaultContainer  string
	allowedContainers map[string]struct{}
	allowAnyContainer bool

	containersMu sync.Mutex
	// containers records the container initializations this process already
	// started, so a job exporting thousands of instances issues one create call
	// per container. An entry is removed before its initialization is reported
	// as failed, so a waiter never mistakes a failure for a success.
	containers map[string]*containerInit

	// Narrow hooks stand in for the two *azblob.Client calls whose ordering and
	// retry behavior matter. Both are nil in production.
	createContainerHook func(ctx context.Context, container string) error
	uploadBlobHook      func(ctx context.Context, container, name string, body []byte, options *azblob.UploadBufferOptions) error
	waitHook            func(ctx context.Context, d time.Duration) error
}

// containerInit is one in-flight or completed container initialization. err is
// written before done is closed, so a waiter that observes the close also
// observes the result.
type containerInit struct {
	done chan struct{}
	err  error
}

var _ Store = (*AzureBlobHistoryStore)(nil)

// Azure keeps a deleted container's name reserved until the delete finishes,
// rejecting recreation with ContainerBeingDeleted for up to about half a
// minute. Creation therefore retries with a capped exponential backoff.
const (
	containerBeingDeletedAttempts       = 9
	containerBeingDeletedInitialBackoff = 250 * time.Millisecond
	containerBeingDeletedMaxBackoff     = 8 * time.Second
)

// NewAzureBlobHistoryStore constructs a production Azure Blob Storage export store.
func NewAzureBlobHistoryStore(options AzureBlobHistoryStoreOptions) (*AzureBlobHistoryStore, error) {
	if strings.TrimSpace(options.ContainerName) == "" {
		return nil, &ValidationError{Message: "azure blob container name is required"}
	}
	if !validBlobContainerName(options.ContainerName) {
		return nil, &ValidationError{
			Message: fmt.Sprintf("azure blob container name %q is not valid", options.ContainerName),
		}
	}
	if (options.ConnectionString == "") == (options.AccountURL == "") {
		return nil, &ValidationError{Message: "exactly one of connection string or account URL is required"}
	}
	if options.ConnectionString == "" && options.Credential == nil {
		return nil, &ValidationError{Message: "azure blob account URL requires a token credential"}
	}
	if options.ConnectionString != "" && options.Credential != nil {
		return nil, &ValidationError{Message: "connection string and token credential cannot be combined"}
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
			return nil, &ValidationError{Message: "invalid Azure Blob account URL"}
		}
		client, err = azblob.NewClient(options.AccountURL, options.Credential, clientOptions)
	}
	if err != nil {
		return nil, fmt.Errorf("create Azure Blob client: %w", err)
	}
	serviceURL, err := url.Parse(client.URL())
	if err != nil {
		return nil, &ValidationError{Message: "Azure Blob service URL is not allowed"}
	}
	publicURL := *serviceURL
	publicURL.RawQuery, publicURL.Fragment, publicURL.User = "", "", nil
	publicURL.ForceQuery = false
	if !isSafeBlobURL(&publicURL, options.AllowInsecureHTTP) {
		return nil, &ValidationError{Message: "Azure Blob service URL is not allowed"}
	}

	allowed := make(map[string]struct{}, len(options.AllowedContainers)+1)
	allowed[options.ContainerName] = struct{}{}
	for _, container := range options.AllowedContainers {
		container = strings.TrimSpace(container)
		if !validBlobContainerName(container) {
			return nil, &ValidationError{
				Message: fmt.Sprintf("allowed azure blob container name %q is not valid", container),
			}
		}
		allowed[container] = struct{}{}
	}

	return &AzureBlobHistoryStore{
		client:            client,
		defaultContainer:  options.ContainerName,
		allowedContainers: allowed,
		allowAnyContainer: options.AllowAnyContainer,
		containers:        make(map[string]*containerInit),
	}, nil
}

// DefaultContainer returns the container configured for this store.
func (s *AzureBlobHistoryStore) DefaultContainer() string { return s.defaultContainer }

// Write uploads object, creating its container on first use.
func (s *AzureBlobHistoryStore) Write(ctx context.Context, object ExportObject) error {
	if err := s.validateObject(object); err != nil {
		return err
	}
	init, err := s.ensureContainer(ctx, object.Container)
	if err != nil {
		return err
	}
	metadata := make(map[string]*string, len(object.Metadata))
	for key, value := range object.Metadata {
		metadata[key] = to.Ptr(value)
	}
	headers := &blob.HTTPHeaders{}
	if object.ContentType != "" {
		headers.BlobContentType = to.Ptr(object.ContentType)
	}
	options := &azblob.UploadBufferOptions{HTTPHeaders: headers, Metadata: metadata}
	if err := s.uploadBlob(ctx, object.Container, object.Name, object.Content, options); err != nil {
		// A container deleted between initialization and upload must not wedge
		// the store: forget the exact initialization this write relied on so the
		// next write recreates the container, without discarding a newer one.
		if bloberror.HasCode(err, bloberror.ContainerNotFound) {
			s.forgetContainer(object.Container, init)
		}
		return fmt.Errorf("upload export blob %q: %w", object.Name, err)
	}
	return nil
}

func (s *AzureBlobHistoryStore) validateObject(object ExportObject) error {
	container := object.Container
	if container == "" {
		return &ValidationError{Message: "export object container is required"}
	}
	if !validBlobContainerName(container) {
		return &ValidationError{Message: fmt.Sprintf("export object container %q is not valid", container)}
	}
	if !s.allowAnyContainer {
		if _, ok := s.allowedContainers[container]; !ok {
			return &ValidationError{
				Message: fmt.Sprintf("export destination container %q is not allowed by this worker", container),
			}
		}
	}
	if err := validateBlobName(object.Name); err != nil {
		return err
	}
	return nil
}

// ensureContainer creates container once per process and returns the
// initialization the caller joined, so a later failure can invalidate exactly
// that initialization.
func (s *AzureBlobHistoryStore) ensureContainer(ctx context.Context, container string) (*containerInit, error) {
	s.containersMu.Lock()
	if existing, ok := s.containers[container]; ok {
		s.containersMu.Unlock()
		select {
		case <-existing.done:
			return existing, existing.err
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	init := &containerInit{done: make(chan struct{})}
	s.containers[container] = init
	s.containersMu.Unlock()

	init.err = s.createContainerWithRetry(ctx, container)
	if init.err != nil {
		// Retire the entry before publishing the result so a waiter that wakes
		// on the close cannot observe this failed initialization as cached.
		s.forgetContainer(container, init)
	}
	close(init.done)
	return init, init.err
}

// forgetContainer drops a cached initialization, but only when it is still the
// current one, so it never discards a newer initialization started after this
// one was retired.
func (s *AzureBlobHistoryStore) forgetContainer(container string, init *containerInit) {
	s.containersMu.Lock()
	defer s.containersMu.Unlock()
	if current, ok := s.containers[container]; ok && current == init {
		delete(s.containers, container)
	}
}

func (s *AzureBlobHistoryStore) createContainerWithRetry(ctx context.Context, container string) error {
	backoff := containerBeingDeletedInitialBackoff
	for attempt := 0; ; attempt++ {
		err := s.createContainer(ctx, container)
		switch {
		case err == nil,
			bloberror.HasCode(err, bloberror.ContainerAlreadyExists):
			return nil
		case bloberror.HasCode(err, bloberror.ContainerBeingDeleted) && attempt < containerBeingDeletedAttempts-1:
			if waitErr := s.waitForRetry(ctx, backoff); waitErr != nil {
				return waitErr
			}
			backoff = min(backoff*2, containerBeingDeletedMaxBackoff)
		default:
			return fmt.Errorf("create export container %q: %w", container, err)
		}
	}
}

func (s *AzureBlobHistoryStore) createContainer(ctx context.Context, container string) error {
	if s.createContainerHook != nil {
		return s.createContainerHook(ctx, container)
	}
	_, err := s.client.CreateContainer(ctx, container, nil)
	return err
}

func (s *AzureBlobHistoryStore) uploadBlob(
	ctx context.Context,
	container string,
	name string,
	body []byte,
	options *azblob.UploadBufferOptions,
) error {
	if s.uploadBlobHook != nil {
		return s.uploadBlobHook(ctx, container, name, body, options)
	}
	_, err := s.client.UploadBuffer(ctx, container, name, body, options)
	return err
}

func (s *AzureBlobHistoryStore) waitForRetry(ctx context.Context, d time.Duration) error {
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

// gzipContent compresses content with the deterministic settings the JSONL
// export format expects.
func gzipContent(content []byte) ([]byte, error) {
	var buffer bytes.Buffer
	writer, err := gzip.NewWriterLevel(&buffer, gzip.BestCompression)
	if err != nil {
		return nil, fmt.Errorf("create gzip writer: %w", err)
	}
	if _, err := writer.Write(content); err != nil {
		return nil, fmt.Errorf("compress export content: %w", err)
	}
	if err := writer.Close(); err != nil {
		return nil, fmt.Errorf("finish export compression: %w", err)
	}
	return buffer.Bytes(), nil
}

// validBlobContainerName mirrors the Azure Blob container naming rules.
func validBlobContainerName(name string) bool {
	if len(name) < 3 || len(name) > 63 {
		return false
	}
	if strings.HasPrefix(name, "-") || strings.HasSuffix(name, "-") {
		return false
	}
	previousDash := false
	for _, r := range name {
		switch {
		case r >= 'a' && r <= 'z', r >= '0' && r <= '9':
			previousDash = false
		case r == '-':
			if previousDash {
				return false
			}
			previousDash = true
		default:
			return false
		}
	}
	return true
}

// validateBlobPrefix rejects prefixes that would escape the destination or
// produce an unusable blob path.
func validateBlobPrefix(prefix string) error {
	if prefix == "" {
		return nil
	}
	if len(prefix) > 900 {
		return &ValidationError{Message: "export destination prefix is too long"}
	}
	if strings.HasPrefix(prefix, "/") {
		return &ValidationError{Message: "export destination prefix must not start with '/'"}
	}
	if strings.Contains(prefix, "//") {
		return &ValidationError{Message: "export destination prefix must not contain '//'"}
	}
	for _, segment := range strings.Split(prefix, "/") {
		if segment == "." || segment == ".." {
			return &ValidationError{Message: "export destination prefix must not contain relative path segments"}
		}
	}
	if strings.ContainsAny(prefix, "\x00\r\n\t\\") {
		return &ValidationError{Message: "export destination prefix contains an unsupported character"}
	}
	return nil
}

func validateBlobName(name string) error {
	switch {
	case name == "":
		return &ValidationError{Message: "export object name is required"}
	case len(name) > 1024:
		return &ValidationError{Message: "export object name is too long"}
	case strings.HasPrefix(name, "/"), strings.HasSuffix(name, "/"):
		return &ValidationError{Message: "export object name must not start or end with '/'"}
	case strings.ContainsAny(name, "\x00\r\n\t\\"):
		return &ValidationError{Message: "export object name contains an unsupported character"}
	default:
		return nil
	}
}

// isSafeBlobURL rejects endpoints that carry credentials or extra URL parts, and
// endpoints that are neither HTTPS nor an explicitly permitted loopback HTTP
// endpoint.
//
// It is at least as strict as the equivalent check in
// [github.com/microsoft/durabletask-go/payload]: userinfo, a query string, and a
// fragment are all rejected outright so a configured account URL cannot smuggle
// a SAS token or credentials, and plaintext HTTP is additionally confined to
// loopback.
func isSafeBlobURL(u *url.URL, allowHTTP bool) bool {
	if u == nil || !u.IsAbs() || u.Host == "" || u.Opaque != "" {
		return false
	}
	if u.User != nil || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" || u.RawFragment != "" {
		return false
	}
	switch strings.ToLower(u.Scheme) {
	case "https":
		return true
	case "http":
		if !allowHTTP {
			return false
		}
		host := u.Hostname()
		if strings.EqualFold(host, "localhost") {
			return true
		}
		ip := net.ParseIP(host)
		return ip != nil && ip.IsLoopback()
	default:
		return false
	}
}
