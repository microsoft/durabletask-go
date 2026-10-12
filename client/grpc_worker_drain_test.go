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

type cancelOnIntakeCheckContext struct {
	context.Context
	cancel context.CancelFunc
}

func (c cancelOnIntakeCheckContext) Err() error {
	c.cancel()
	return c.Context.Err()
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
				require.True(t, connection.registerWorkItem(run))
				slots := make(chan struct{}, 1)
				if phase == "waiting for slot" {
					slots <- struct{}{}
				} else {
					// Cancel at the post-admission check, after the only
					// ready select case has reserved the free slot.
					run.intakeCtx = cancelOnIntakeCheckContext{run.intakeCtx, run.cancelIntake}
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
				if phase == "waiting for slot" {
					run.cancelIntake()
				}
				awaitCompletionTest(t, started)

				runDrained, connectionDrained := make(chan struct{}), make(chan struct{})
				go func() {
					run.pending.Wait()
					close(runDrained)
				}()
				go func() {
					connection.waitForWorkItems()
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

func TestWorkerGracefulStopRacingBufferedReceive(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		for range 300 {
			client := &leaseAwareSchedulerClient{fakeSchedulerClient: &fakeSchedulerClient{stream: newFakeWorkItemStream(1)}}
			worker := newFakeWorker(t, client.fakeSchedulerClient)
			worker.clientFactory = func(context.Context) (protos.TaskHubSidecarServiceClient, io.Closer, error) {
				return client, nil, nil
			}
			started, release := make(chan struct{}), make(chan struct{})
			worker.executor = &recordingExecutor{
				executeActivity: func(context.Context, api.InstanceID, *protos.HistoryEvent) (*protos.HistoryEvent, error) {
					close(started)
					<-release
					return &protos.HistoryEvent{EventType: &protos.HistoryEvent_TaskCompleted{
						TaskCompleted: &protos.TaskCompletedEvent{},
					}}, nil
				},
			}
			ctx, cancel := context.WithCancel(context.Background())
			require.NoError(t, worker.Start(ctx))
			client.stream.results <- completionTestActivity("accepted")
			awaitCompletionTest(t, started)
			cancel()
			synctest.Wait()
			// Deliver a final buffered item as the previously accepted work
			// drains the count to zero and wakes the stop watcher.
			close(release)
			client.stream.results <- completionTestActivity("raced-intake-stop")
			synctest.Wait()
			require.NoError(t, worker.Shutdown(context.Background()))
			require.False(t, worker.Running())
			client.mu.Lock()
			completed, abandoned := len(client.activityCompletions), client.activityAbandons
			client.mu.Unlock()
			require.Equal(t, 1, completed)
			require.LessOrEqual(t, abandoned, 1)
			require.Zero(t, client.expired.Load(), "no acknowledgement may use a lease released by shutdown")
		}
	})
}

func TestWorkerReceiveHandoffPreservesLeaseUntilAbandoned(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		client := &leaseAwareSchedulerClient{
			fakeSchedulerClient: &fakeSchedulerClient{stream: newFakeWorkItemStream(1)},
			abandonStarted:      make(chan struct{}),
			releaseAbandon:      make(chan struct{}),
		}
		run := newRunLoopTestRun()
		defer run.cancelIntake()
		defer run.cancelProcessing()
		streamCtx, cancelStream := context.WithCancel(context.Background())
		defer cancelStream()
		client.stream.ctx = streamCtx
		connection := &grpcWorkerConnection{client: client, stream: client.stream, cancelStream: cancelStream}
		worker := newFakeWorker(t, client.fakeSchedulerClient)
		var releaseOnce sync.Once
		dispatched := false
		t.Cleanup(func() {
			releaseOnce.Do(func() { close(client.releaseAbandon) })
			if !dispatched {
				connection.pendingMu.Lock()
				registered := connection.pendingWorkItems > 0
				connection.pendingMu.Unlock()
				if registered {
					connection.finishWorkItem(run)
				}
			}
		})
		client.stream.results <- completionTestActivity("receive-handoff")
		item, err := worker.receiveWorkItem(run, connection)
		require.NoError(t, err)

		// Pause after successful receipt, before entering dispatch at all.
		run.cancelIntake()
		stopped := make(chan struct{})
		go func() {
			connection.stopIntake()
			close(stopped)
		}()
		synctest.Wait()
		require.NoError(t, streamCtx.Err(), "receipt must already be registered before dispatch")

		dispatchDone := make(chan error, 1)
		dispatched = true
		go func() {
			dispatchDone <- worker.dispatchActivity(run, connection, item.GetCompletionToken(), item.GetActivityRequest())
		}()
		awaitCompletionTest(t, client.abandonStarted)
		synctest.Wait()
		require.NoError(t, streamCtx.Err(), "the abandonment must retain the received item's lease")
		releaseOnce.Do(func() { close(client.releaseAbandon) })
		require.ErrorIs(t, awaitCompletionTest(t, dispatchDone), context.Canceled)
		awaitCompletionTest(t, stopped)
		run.pending.Wait()
		require.EqualValues(t, 1, client.abandoned.Load())
		require.Zero(t, client.expired.Load())
	})
}

func TestWorkerStoppedIntakeRejectsUnregisteredDelivery(t *testing.T) {
	run := newRunLoopTestRun()
	defer run.cancelIntake()
	defer run.cancelProcessing()
	streamCtx, cancelStream := context.WithCancel(context.Background())
	defer cancelStream()
	stream := &racingStream{ctx: streamCtx, item: completionTestActivity("late-delivery").item}
	client := &fakeSchedulerClient{}
	connection := &grpcWorkerConnection{client: client, stream: stream, cancelStream: cancelStream}
	worker := newFakeWorker(t, client)
	run.cancelIntake()
	connection.stopIntake()

	// Recv returns buffered work even though cancellation already released its lease.
	item, err := worker.receiveWorkItem(run, connection)
	require.Equal(t, codes.Canceled, status.Code(err))
	require.Nil(t, item)
	connection.waitForWorkItems()
	run.pending.Wait()
	require.Empty(t, client.activityCompletions)
	require.Zero(t, client.activityAbandons)
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
		connection := &grpcWorkerConnection{}
		require.True(t, connection.registerWorkItem(run))
		var abandoned bool
		err := worker.dispatch(run, connection, run.activitySlots,
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
