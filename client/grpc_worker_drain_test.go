package client

import (
	"context"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/microsoft/durabletask-go/api"
	"github.com/microsoft/durabletask-go/internal/protos"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type leaseAwareSchedulerClient struct {
	*fakeSchedulerClient
	acknowledged   atomic.Int32
	expired        atomic.Int32
	ackStarted     chan struct{}
	releaseAck     chan struct{}
	abandoned      atomic.Int32
	abandonStarted chan struct{}
	releaseAbandon chan struct{}
}

func (c *leaseAwareSchedulerClient) CompleteActivityTask(
	ctx context.Context,
	response *protos.ActivityResponse,
	options ...grpc.CallOption,
) (*protos.CompleteTaskResponse, error) {
	if c.ackStarted != nil {
		close(c.ackStarted)
		select {
		case <-c.releaseAck:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	if c.stream.ctx.Err() != nil {
		c.expired.Add(1)
		return nil, status.Error(codes.NotFound, "intake ended and released the work item lease")
	}
	result, err := c.fakeSchedulerClient.CompleteActivityTask(ctx, response, options...)
	if err == nil {
		c.acknowledged.Add(1)
	}
	return result, err
}

func (c *leaseAwareSchedulerClient) AbandonTaskActivityWorkItem(
	ctx context.Context,
	request *protos.AbandonActivityTaskRequest,
	options ...grpc.CallOption,
) (*protos.AbandonActivityTaskResponse, error) {
	if c.abandonStarted != nil {
		close(c.abandonStarted)
		select {
		case <-c.releaseAbandon:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	if c.stream.ctx.Err() != nil {
		c.expired.Add(1)
		return nil, status.Error(codes.NotFound, "intake ended and released the work item lease")
	}
	result, err := c.fakeSchedulerClient.AbandonTaskActivityWorkItem(ctx, request, options...)
	if err == nil {
		c.abandoned.Add(1)
	}
	return result, err
}

func TestWorkerCanceledDispatchWaitsForAbandonment(t *testing.T) {
	for _, phase := range []string{"waiting for slot", "slot reserved"} {
		t.Run(phase, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				worker := newFakeWorker(t, &fakeSchedulerClient{})
				run := newRunLoopTestRun()
				defer run.cancelIntake()
				defer run.cancelProcessing()
				connection := &grpcWorkerConnection{}
				slots := make(chan struct{})
				if phase == "waiting for slot" {
					slots = make(chan struct{}, 1)
					slots <- struct{}{}
				}
				started, release := make(chan struct{}), make(chan struct{})
				var releaseOnce sync.Once
				t.Cleanup(func() { releaseOnce.Do(func() { close(release) }) })
				dispatchDone := make(chan error, 1)
				go func() {
					dispatchDone <- worker.dispatch(run, connection, slots,
						func(context.Context) {
							close(started)
							<-release
						},
						func(context.Context) { t.Error("work was executed after logical intake stop") })
				}()
				synctest.Wait()
				if phase == "slot reserved" {
					run.dispatchMu.Lock()
					<-slots
					run.cancelIntake()
					run.dispatchMu.Unlock()
					// Return the admission token so the rejected dispatch can
					// release its slot after observing cancellation.
					slots <- struct{}{}
				} else {
					run.cancelIntake()
				}
				awaitCompletionTest(t, started)

				runDrained, connectionDrained := make(chan struct{}), make(chan struct{})
				go func() {
					run.pending.Wait()
					close(runDrained)
				}()
				go func() {
					connection.pending.Wait()
					close(connectionDrained)
				}()
				synctest.Wait()
				for _, drained := range []<-chan struct{}{runDrained, connectionDrained} {
					select {
					case <-drained:
						t.Error("drain finished before the abandonment returned")
					default:
					}
				}

				releaseOnce.Do(func() { close(release) })
				require.ErrorIs(t, awaitCompletionTest(t, dispatchDone), context.Canceled)
				awaitCompletionTest(t, runDrained)
				awaitCompletionTest(t, connectionDrained)
				if phase == "waiting for slot" {
					<-slots
				}
				require.Empty(t, slots)
			})
		})
	}
}

func TestWorkerGracefulStopKeepsRejectedItemLeaseUntilAbandoned(t *testing.T) {
	for _, stop := range []string{"shutdown", "run context", "shutdown deadline"} {
		t.Run(stop, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				client := &leaseAwareSchedulerClient{
					fakeSchedulerClient: &fakeSchedulerClient{stream: newFakeWorkItemStream(0)},
					abandonStarted:      make(chan struct{}),
					releaseAbandon:      make(chan struct{}),
				}
				worker := newFakeWorker(t, client.fakeSchedulerClient, WithMaxConcurrentActivityWorkItems(1))
				closer := &countingCloser{}
				worker.clientFactory = func(context.Context) (protos.TaskHubSidecarServiceClient, io.Closer, error) {
					return client, closer, nil
				}
				executionStarted, releaseExecution := make(chan struct{}), make(chan struct{})
				worker.executor = &recordingExecutor{
					executeActivity: func(context.Context, api.InstanceID, *protos.HistoryEvent) (*protos.HistoryEvent, error) {
						close(executionStarted)
						<-releaseExecution
						return &protos.HistoryEvent{EventType: &protos.HistoryEvent_TaskCompleted{
							TaskCompleted: &protos.TaskCompletedEvent{},
						}}, nil
					},
				}
				ctx, cancel := context.WithCancel(context.Background())
				var executionOnce, abandonOnce sync.Once
				t.Cleanup(func() {
					executionOnce.Do(func() { close(releaseExecution) })
					abandonOnce.Do(func() { close(client.releaseAbandon) })
					cancel()
					shutdownCtx, stopShutdown := context.WithTimeout(context.Background(), 5*time.Second)
					defer stopShutdown()
					require.NoError(t, worker.Shutdown(shutdownCtx))
					require.NoError(t, worker.Wait(shutdownCtx))
				})
				require.NoError(t, worker.Start(ctx))
				client.stream.results <- completionTestActivity("accepted")
				awaitCompletionTest(t, executionStarted)
				client.stream.results <- completionTestActivity("rejected")
				synctest.Wait()

				stopped := make(chan error, 1)
				if stop == "run context" {
					cancel()
					go func() { stopped <- worker.Wait(context.Background()) }()
				} else {
					go func() { stopped <- worker.Shutdown(context.Background()) }()
				}
				awaitCompletionTest(t, client.abandonStarted)
				executionOnce.Do(func() { close(releaseExecution) })
				synctest.Wait()
				require.EqualValues(t, 1, client.acknowledged.Load())
				require.NoError(t, client.stream.ctx.Err(), "the rejected item's lease must remain alive during abandonment")
				require.Zero(t, closer.closes.Load())
				require.True(t, worker.Running())

				if stop == "shutdown deadline" {
					shutdownCtx, stopShutdown := context.WithTimeout(context.Background(), time.Second)
					defer stopShutdown()
					require.ErrorIs(t, worker.Shutdown(shutdownCtx), context.DeadlineExceeded)
				} else {
					abandonOnce.Do(func() { close(client.releaseAbandon) })
				}
				require.NoError(t, awaitCompletionTest(t, stopped))
				require.EqualValues(t, 1, closer.closes.Load())
				require.Zero(t, client.expired.Load())
				if stop == "shutdown deadline" {
					require.Zero(t, client.abandoned.Load())
				} else {
					require.EqualValues(t, 1, client.abandoned.Load())
				}
			})
		})
	}
}

func TestWorkerGracefulDrainKeepsPendingAcknowledgementAndSilentIntakeAlive(t *testing.T) {
	client := &leaseAwareSchedulerClient{
		fakeSchedulerClient: &fakeSchedulerClient{stream: newFakeWorkItemStream(1)},
		ackStarted:          make(chan struct{}),
		releaseAck:          make(chan struct{}),
	}
	worker := newFakeWorker(t, client.fakeSchedulerClient,
		WithMaxConcurrentActivityWorkItems(1),
		WithWorkerSilentDisconnectTimeout(100*time.Millisecond))
	worker.clientFactory = func(context.Context) (protos.TaskHubSidecarServiceClient, io.Closer, error) {
		return client, nil, nil
	}
	worker.executor = &recordingExecutor{
		executeActivity: func(context.Context, api.InstanceID, *protos.HistoryEvent) (*protos.HistoryEvent, error) {
			return &protos.HistoryEvent{EventType: &protos.HistoryEvent_TaskCompleted{
				TaskCompleted: &protos.TaskCompletedEvent{},
			}}, nil
		},
	}
	var releaseOnce sync.Once
	t.Cleanup(func() {
		releaseOnce.Do(func() { close(client.releaseAck) })
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		require.NoError(t, worker.Shutdown(ctx))
		require.NoError(t, worker.Wait(ctx))
	})
	require.NoError(t, worker.Start(context.Background()))
	client.stream.results <- completionTestActivity("pending-ack")
	awaitCompletionTest(t, client.ackStarted)
	worker.mu.Lock()
	run := worker.run
	worker.mu.Unlock()
	done := make(chan error, 1)
	go func() { done <- worker.Shutdown(context.Background()) }()
	awaitCompletionTest(t, run.intakeCtx.Done())
	select {
	case <-client.stream.ctx.Done():
		t.Fatal("silence timeout released the lease while its acknowledgement was pending")
	case <-time.After(2 * worker.options.silentDisconnectTimeout):
	}
	releaseOnce.Do(func() { close(client.releaseAck) })
	require.NoError(t, awaitCompletionTest(t, done))
	require.EqualValues(t, 1, client.acknowledged.Load())
	require.Zero(t, client.expired.Load())
}

func TestWorkerStoppedDispatchDoesNotExecuteEvenWithFreeSlot(t *testing.T) {
	worker := newFakeWorker(t, &fakeSchedulerClient{})
	for range 100 {
		run := newRunLoopTestRun()
		run.cancelIntake()
		var abandoned bool
		err := worker.dispatch(run, &grpcWorkerConnection{}, run.activitySlots,
			func(context.Context) { abandoned = true },
			func(context.Context) { t.Error("work was executed after logical intake stop") })
		require.ErrorIs(t, err, context.Canceled)
		require.True(t, abandoned)
		require.Empty(t, run.activitySlots)
		run.pending.Wait()
		run.cancelProcessing()
	}
}

func TestWorkerGracefulDrainPreservesIntakeLeaseUntilAcknowledged(t *testing.T) {
	for _, stop := range []string{"shutdown", "run context"} {
		t.Run(stop, func(t *testing.T) {
			client := &leaseAwareSchedulerClient{fakeSchedulerClient: &fakeSchedulerClient{stream: newFakeWorkItemStream(1)}}
			worker := newFakeWorker(t, client.fakeSchedulerClient, WithMaxConcurrentActivityWorkItems(1))
			worker.clientFactory = func(context.Context) (protos.TaskHubSidecarServiceClient, io.Closer, error) {
				return client, nil, nil
			}
			started, release := make(chan struct{}), make(chan struct{})
			var releaseOnce sync.Once
			worker.executor = &recordingExecutor{
				executeActivity: func(ctx context.Context, _ api.InstanceID, _ *protos.HistoryEvent) (*protos.HistoryEvent, error) {
					close(started)
					select {
					case <-release:
						return &protos.HistoryEvent{EventType: &protos.HistoryEvent_TaskCompleted{
							TaskCompleted: &protos.TaskCompletedEvent{},
						}}, nil
					case <-ctx.Done():
						return nil, ctx.Err()
					}
				},
			}
			ctx, cancel := context.WithCancel(context.Background())
			t.Cleanup(func() {
				releaseOnce.Do(func() { close(release) })
				cancel()
				shutdownCtx, stopShutdown := context.WithTimeout(context.Background(), 5*time.Second)
				defer stopShutdown()
				require.NoError(t, worker.Shutdown(shutdownCtx))
				require.NoError(t, worker.Wait(shutdownCtx))
			})
			require.NoError(t, worker.Start(ctx))
			client.stream.results <- completionTestActivity("drain")
			awaitCompletionTest(t, started)
			worker.mu.Lock()
			run := worker.run
			worker.mu.Unlock()
			if stop == "run context" {
				cancel()
			}
			shutdownDone := make(chan error, 1)
			go func() { shutdownDone <- worker.Shutdown(context.Background()) }()
			awaitCompletionTest(t, run.intakeCtx.Done())
			require.NoError(t, client.stream.ctx.Err(), "logical intake stop must not revoke an accepted work item's lease")
			releaseOnce.Do(func() { close(release) })
			require.NoError(t, awaitCompletionTest(t, shutdownDone))
			require.EqualValues(t, 1, client.acknowledged.Load())
			require.Zero(t, client.expired.Load())
		})
	}
}

// openingSchedulerClient lets a test control how GetWorkItems establishes its
// stream, such as blocking at a saturated HTTP/2 stream limit.
type openingSchedulerClient struct {
	*fakeSchedulerClient
	open func(context.Context) (protos.TaskHubSidecarService_GetWorkItemsClient, error)
}

func (c *openingSchedulerClient) GetWorkItems(
	ctx context.Context,
	_ *protos.GetWorkItemsRequest,
	_ ...grpc.CallOption,
) (protos.TaskHubSidecarService_GetWorkItemsClient, error) {
	return c.open(ctx)
}

func TestWorkerIntakeStopInterruptsStreamOpen(t *testing.T) {
	for _, stop := range []string{"shutdown", "run context"} {
		t.Run(stop, func(t *testing.T) {
			opening := make(chan struct{})
			client := &openingSchedulerClient{
				fakeSchedulerClient: &fakeSchedulerClient{stream: newFakeWorkItemStream(0)},
				open: func(ctx context.Context) (protos.TaskHubSidecarService_GetWorkItemsClient, error) {
					close(opening)
					<-ctx.Done()
					return nil, status.FromContextError(ctx.Err()).Err()
				},
			}
			worker := newFakeWorker(t, client.fakeSchedulerClient)
			closer := &countingCloser{}
			worker.clientFactory = func(context.Context) (protos.TaskHubSidecarServiceClient, io.Closer, error) {
				return client, closer, nil
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			startDone := make(chan error, 1)
			go func() { startDone <- worker.Start(ctx) }()
			awaitCompletionTest(t, opening)

			if stop == "run context" {
				cancel()
			} else {
				shutdownCtx, stopShutdown := context.WithTimeout(context.Background(), 5*time.Second)
				defer stopShutdown()
				_ = worker.Shutdown(shutdownCtx)
			}
			require.Error(t, awaitCompletionTest(t, startDone))
			require.False(t, worker.Running())
			require.EqualValues(t, 1, closer.closes.Load())
		})
	}
}

func TestWorkerIntakeStopRacingStreamOpenDoesNotEstablishCancelledStream(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var streamCtx context.Context
	client := &openingSchedulerClient{fakeSchedulerClient: &fakeSchedulerClient{stream: newFakeWorkItemStream(0)}}
	client.open = func(ctx context.Context) (protos.TaskHubSidecarService_GetWorkItemsClient, error) {
		streamCtx = ctx
		// The stop lands after the open succeeded but before connect returns.
		cancel()
		return client.stream, nil
	}
	worker := newFakeWorker(t, client.fakeSchedulerClient)
	closer := &countingCloser{}
	worker.clientFactory = func(context.Context) (protos.TaskHubSidecarServiceClient, io.Closer, error) {
		return client, closer, nil
	}

	require.Error(t, worker.Start(ctx))
	require.False(t, worker.Running())
	require.EqualValues(t, 1, closer.closes.Load())
	awaitCompletionTest(t, streamCtx.Done())
}
