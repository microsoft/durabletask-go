package client

import (
	"context"
	"io"
	"sync"
	"sync/atomic"
	"testing"
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
	acknowledged atomic.Int32
	expired      atomic.Int32
	ackStarted   chan struct{}
	releaseAck   chan struct{}
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
