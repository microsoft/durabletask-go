package payload

import (
	"context"
	"errors"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/bloberror"
	"github.com/stretchr/testify/require"
)

func blobCodeError(status int, code bloberror.Code) error {
	return &azcore.ResponseError{StatusCode: status, ErrorCode: string(code)}
}

// blobBackend replaces the Azure Blob client with scripted, deterministic
// container and upload behavior. Backoff waits are recorded rather than slept
// so retry tests stay fast and deterministic.
type blobBackend struct {
	createResult func(ctx context.Context, call int) error
	uploadResult func(ctx context.Context, call int) error

	createCalls atomic.Int64
	uploadCalls atomic.Int64

	sleepMu sync.Mutex
	sleeps  []time.Duration
}

func (b *blobBackend) createContainer(ctx context.Context) error {
	call := int(b.createCalls.Add(1))
	if b.createResult == nil {
		return nil
	}
	return b.createResult(ctx, call)
}

func (b *blobBackend) uploadBlob(
	ctx context.Context,
	_ string,
	_ []byte,
	_ *azblob.UploadBufferOptions,
) error {
	call := int(b.uploadCalls.Add(1))
	if b.uploadResult == nil {
		return nil
	}
	return b.uploadResult(ctx, call)
}

func (b *blobBackend) waitForRetry(ctx context.Context, delay time.Duration) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	b.sleepMu.Lock()
	b.sleeps = append(b.sleeps, delay)
	b.sleepMu.Unlock()
	return nil
}

func (b *blobBackend) recordedSleeps() []time.Duration {
	b.sleepMu.Lock()
	defer b.sleepMu.Unlock()
	return append([]time.Duration(nil), b.sleeps...)
}

func newScriptedAzureBlobStore(t *testing.T, backend *blobBackend) *AzureBlobStore {
	t.Helper()
	store := newTestAzureBlobStore(t)
	store.createContainerHook = backend.createContainer
	store.uploadBlobHook = backend.uploadBlob
	store.waitHook = backend.waitForRetry
	return store
}

// TestAzureBlobStoreScriptedContainerLifecycle covers the container
// initialization outcomes that can be expressed as a fixed script of create and
// upload results.
func TestAzureBlobStoreScriptedContainerLifecycle(t *testing.T) {
	transient := errors.New("service unavailable")
	unrelated := blobCodeError(http.StatusInternalServerError, bloberror.InternalError)
	missing := blobCodeError(http.StatusNotFound, bloberror.ContainerNotFound)
	beingDeleted := blobCodeError(http.StatusConflict, bloberror.ContainerBeingDeleted)

	tests := []struct {
		name string
		// createResults and uploadResults are indexed by call number, and the
		// final entry repeats for any further calls.
		createResults []error
		uploadResults []error
		// uploads describes the expected error of each sequential upload.
		uploads             []error
		expectedCreateCalls int64
		expectedUploadCalls int64
		// expectedBackoffs are the waits between container creation attempts.
		expectedBackoffs []time.Duration
	}{
		{
			name:                "cached initialization is reused",
			createResults:       []error{nil},
			uploadResults:       []error{nil},
			uploads:             []error{nil, nil, nil},
			expectedCreateCalls: 1,
			expectedUploadCalls: 3,
		},
		{
			name:                "failed initialization is retried on the next call",
			createResults:       []error{transient, nil},
			uploadResults:       []error{nil},
			uploads:             []error{transient, nil},
			expectedCreateCalls: 2,
			expectedUploadCalls: 1,
		},
		{
			name:                "already existing container completes initialization",
			createResults:       []error{blobCodeError(http.StatusConflict, bloberror.ContainerAlreadyExists)},
			uploadResults:       []error{nil},
			uploads:             []error{nil, nil},
			expectedCreateCalls: 1,
			expectedUploadCalls: 2,
		},
		{
			name:                "deleted container is recreated within the same upload",
			createResults:       []error{nil},
			uploadResults:       []error{missing, nil},
			uploads:             []error{nil},
			expectedCreateCalls: 2,
			expectedUploadCalls: 2,
		},
		{
			name:                "second missing container is not retried again",
			createResults:       []error{nil},
			uploadResults:       []error{missing},
			uploads:             []error{missing},
			expectedCreateCalls: 2,
			expectedUploadCalls: 2,
		},
		{
			name:                "unrelated storage error propagates without invalidating the cache",
			createResults:       []error{nil},
			uploadResults:       []error{unrelated, nil},
			uploads:             []error{unrelated, nil},
			expectedCreateCalls: 1,
			expectedUploadCalls: 2,
		},
		{
			name:                "container being deleted is waited out with capped backoff",
			createResults:       []error{beingDeleted, beingDeleted, beingDeleted, nil},
			uploadResults:       []error{nil},
			uploads:             []error{nil, nil},
			expectedCreateCalls: 4,
			expectedUploadCalls: 2,
			expectedBackoffs:    []time.Duration{250 * time.Millisecond, 500 * time.Millisecond, time.Second},
		},
		{
			name:                "container being deleted gives up after the attempt bound",
			createResults:       []error{beingDeleted},
			uploadResults:       []error{nil},
			uploads:             []error{beingDeleted},
			expectedCreateCalls: containerBeingDeletedAttempts,
			expectedUploadCalls: 0,
			expectedBackoffs: []time.Duration{
				250 * time.Millisecond,
				500 * time.Millisecond,
				time.Second,
				2 * time.Second,
				4 * time.Second,
				containerBeingDeletedMaxBackoff,
				containerBeingDeletedMaxBackoff,
				containerBeingDeletedMaxBackoff,
			},
		},
		{
			name:                "a container being deleted that resolves as already existing succeeds",
			createResults:       []error{beingDeleted, blobCodeError(http.StatusConflict, bloberror.ContainerAlreadyExists)},
			uploadResults:       []error{nil},
			uploads:             []error{nil},
			expectedCreateCalls: 2,
			expectedUploadCalls: 1,
			expectedBackoffs:    []time.Duration{250 * time.Millisecond},
		},
	}

	scripted := func(results []error) func(context.Context, int) error {
		return func(_ context.Context, call int) error {
			if call > len(results) {
				call = len(results)
			}
			return results[call-1]
		}
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			backend := &blobBackend{
				createResult: scripted(test.createResults),
				uploadResult: scripted(test.uploadResults),
			}
			store := newScriptedAzureBlobStore(t, backend)
			for i, expected := range test.uploads {
				token, err := store.StoreToken(context.Background(), []byte("payload"))
				if expected == nil {
					require.NoErrorf(t, err, "upload %d", i)
					require.Regexpf(t, `^blob:v2:.*/[0-9a-f]{32}$`, token, "upload %d", i)
					continue
				}
				require.ErrorIsf(t, err, expected, "upload %d", i)
			}
			require.Equal(t, test.expectedCreateCalls, backend.createCalls.Load(), "container creations")
			require.Equal(t, test.expectedUploadCalls, backend.uploadCalls.Load(), "blob uploads")
			require.Equal(t, test.expectedBackoffs, backend.recordedSleeps(), "recreation backoff")
		})
	}
}

// TestAzureBlobStoreContainerBeingDeletedStopsOnCancellation verifies that a
// caller cancelled while the recreation backoff is waiting reports cancellation
// instead of exhausting the attempt bound.
func TestAzureBlobStoreContainerBeingDeletedStopsOnCancellation(t *testing.T) {
	beingDeleted := blobCodeError(http.StatusConflict, bloberror.ContainerBeingDeleted)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	backend := &blobBackend{createResult: func(context.Context, int) error { return beingDeleted }}
	store := newScriptedAzureBlobStore(t, backend)
	store.waitHook = func(ctx context.Context, _ time.Duration) error {
		cancel()
		return ctx.Err()
	}

	_, err := store.StoreToken(ctx, []byte("payload"))
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, int64(1), backend.createCalls.Load(), "cancellation must stop the retry loop")
	require.Zero(t, backend.uploadCalls.Load())
}

// TestAzureBlobStoreConcurrentUploadsInitializeContainerOnce keeps container
// creation in flight until every uploader has reached the store so the
// single-flight gate is genuinely contended.
func TestAzureBlobStoreConcurrentUploadsInitializeContainerOnce(t *testing.T) {
	const uploaders = 16
	var ready sync.WaitGroup
	ready.Add(uploaders)
	backend := &blobBackend{createResult: func(context.Context, int) error {
		ready.Wait()
		return nil
	}}
	store := newScriptedAzureBlobStore(t, backend)

	errs := make(chan error, uploaders)
	for i := 0; i < uploaders; i++ {
		go func() {
			ready.Done()
			_, err := store.StoreToken(context.Background(), []byte("payload"))
			errs <- err
		}()
	}
	for i := 0; i < uploaders; i++ {
		require.NoError(t, <-errs)
	}
	require.Equal(t, int64(1), backend.createCalls.Load())
	require.Equal(t, int64(uploaders), backend.uploadCalls.Load())
}

// TestAzureBlobStoreCancelledInitializerReleasesWaiters verifies that a
// cancelled initializer publishes no generation, so a waiting caller retries
// initialization with its own context.
func TestAzureBlobStoreCancelledInitializerReleasesWaiters(t *testing.T) {
	createStarted := make(chan struct{})
	waiterReady := make(chan struct{})
	backend := &blobBackend{createResult: func(ctx context.Context, call int) error {
		if call > 1 {
			return nil
		}
		close(createStarted)
		<-waiterReady
		<-ctx.Done()
		return ctx.Err()
	}}
	store := newScriptedAzureBlobStore(t, backend)

	cancellable, cancel := context.WithCancel(context.Background())
	defer cancel()
	cancelled := make(chan error, 1)
	go func() {
		_, err := store.StoreToken(cancellable, []byte("payload"))
		cancelled <- err
	}()
	<-createStarted

	waiting := make(chan error, 1)
	go func() {
		close(waiterReady)
		_, err := store.StoreToken(context.Background(), []byte("payload"))
		waiting <- err
	}()

	cancel()
	require.ErrorIs(t, <-cancelled, context.Canceled)
	require.NoError(t, <-waiting)
	require.Equal(t, int64(2), backend.createCalls.Load())
	require.Equal(t, int64(1), backend.uploadCalls.Load(), "the cancelled caller must not upload")
}

// TestAzureBlobStoreCancelledWaiterDoesNotCancelInitializer verifies that one
// caller abandoning the wait leaves the in-flight initialization untouched.
func TestAzureBlobStoreCancelledWaiterDoesNotCancelInitializer(t *testing.T) {
	createStarted := make(chan struct{})
	releaseCreate := make(chan struct{})
	backend := &blobBackend{createResult: func(ctx context.Context, _ int) error {
		close(createStarted)
		<-releaseCreate
		return ctx.Err()
	}}
	store := newScriptedAzureBlobStore(t, backend)

	initializing := make(chan error, 1)
	go func() {
		_, err := store.StoreToken(context.Background(), []byte("payload"))
		initializing <- err
	}()
	<-createStarted

	cancellable, cancel := context.WithCancel(context.Background())
	defer cancel()
	waiting := make(chan error, 1)
	waiterReady := make(chan struct{})
	go func() {
		close(waiterReady)
		_, err := store.StoreToken(cancellable, []byte("payload"))
		waiting <- err
	}()
	<-waiterReady
	cancel()

	require.ErrorIs(t, <-waiting, context.Canceled)
	close(releaseCreate)
	require.NoError(t, <-initializing)
	require.Equal(t, int64(1), backend.createCalls.Load())
	require.Equal(t, int64(1), backend.uploadCalls.Load())
}

// TestAzureBlobStoreStaleContainerNotFoundKeepsNewerInitialization pins the
// generation identity contract: a slow upload that only discovers the container
// is gone after a newer initialization was published must not invalidate it.
func TestAzureBlobStoreStaleContainerNotFoundKeepsNewerInitialization(t *testing.T) {
	type uploadTag struct{}
	missing := blobCodeError(http.StatusNotFound, bloberror.ContainerNotFound)
	slowStarted := make(chan struct{})
	releaseSlow := make(chan struct{})

	var slowUpload sync.Once
	backend := &blobBackend{uploadResult: func(ctx context.Context, _ int) error {
		switch tag, _ := ctx.Value(uploadTag{}).(string); tag {
		case "slow":
			// Only the first slow attempt parks; the retry is already ordered
			// behind it on the same goroutine.
			slowUpload.Do(func() {
				close(slowStarted)
				<-releaseSlow
			})
			return missing
		case "missing":
			return missing
		default:
			return nil
		}
	}}
	store := newScriptedAzureBlobStore(t, backend)

	tagged := func(tag string) context.Context {
		return context.WithValue(context.Background(), uploadTag{}, tag)
	}

	slow := make(chan error, 1)
	go func() {
		_, err := store.StoreToken(tagged("slow"), []byte("payload"))
		slow <- err
	}()
	<-slowStarted

	// Discovers the deletion first, invalidating the generation the slow upload
	// is still holding, then fails again on its single permitted retry.
	_, err := store.StoreToken(tagged("missing"), []byte("payload"))
	require.ErrorIs(t, err, missing)
	require.Equal(t, int64(2), backend.createCalls.Load())

	_, err = store.StoreToken(tagged("recreated"), []byte("payload"))
	require.NoError(t, err)

	close(releaseSlow)
	require.ErrorIs(t, <-slow, missing)

	_, err = store.StoreToken(tagged("final"), []byte("payload"))
	require.NoError(t, err)
	require.Equal(t, int64(2), backend.createCalls.Load(),
		"the stale failure must not invalidate the newer initialization")
}
