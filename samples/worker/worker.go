// Command worker demonstrates worker Start, Run, Shutdown, drain, restart, and limits.
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/microsoft/durabletask-go/api"
	durabletaskclient "github.com/microsoft/durabletask-go/client"
	"github.com/microsoft/durabletask-go/durabletaskscheduler"
	"github.com/microsoft/durabletask-go/samples/internal/dtssample"
	"github.com/microsoft/durabletask-go/task"
)

var activityGauge = &concurrencyGauge{}
var drainBarriers = newBarrierRegistry()

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
	fmt.Println("SAMPLE_OK worker")
}

func run() (err error) {
	options, err := dtssample.Options()
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	client, err := durabletaskscheduler.NewClient(ctx, options, api.DefaultLogger())
	if err != nil {
		return err
	}
	var ids []api.InstanceID
	defer func() { err = errors.Join(err, dtssample.Cleanup(client, ids...), client.Close()) }()

	registry := task.NewTaskRegistry()
	if err := registry.AddOrchestratorN("SampleWorkerEcho", workerEchoWorkflow); err != nil {
		return err
	}
	if err := registry.AddOrchestratorN("SampleWorkerDelay", workerDelayWorkflow); err != nil {
		return err
	}
	if err := registry.AddActivityN("SampleWorkerEchoActivity", workerEchoActivity); err != nil {
		return err
	}
	if err := registry.AddActivityN("SampleWorkerDelayActivity", workerDelayActivity); err != nil {
		return err
	}

	concurrencyIDs := []api.InstanceID{
		dtssample.NewInstanceID("worker-concurrency-1"),
		dtssample.NewInstanceID("worker-concurrency-2"),
		dtssample.NewInstanceID("worker-concurrency-3"),
		dtssample.NewInstanceID("worker-concurrency-4"),
	}
	ids = append(ids, concurrencyIDs...)
	drainID := dtssample.NewInstanceID("worker-drain")
	restartID := dtssample.NewInstanceID("worker-restart")
	runID := dtssample.NewInstanceID("worker-run")
	borrowedID := dtssample.NewInstanceID("worker-borrowed")
	ids = append(ids, drainID, restartID, runID, borrowedID)

	if err := verifyStartConcurrencyAndDrain(ctx, options, client, registry, concurrencyIDs, drainID); err != nil {
		return err
	}
	if err := verifyRestart(ctx, options, client, registry, restartID); err != nil {
		return err
	}
	if err := verifyRun(ctx, options, client, registry, runID); err != nil {
		return err
	}
	if err := verifyBorrowedConnectionListener(ctx, client, registry, borrowedID); err != nil {
		return err
	}
	return nil
}

func verifyStartConcurrencyAndDrain(
	ctx context.Context,
	options *durabletaskscheduler.Options,
	client *durabletaskscheduler.Client,
	registry *task.TaskRegistry,
	concurrencyIDs []api.InstanceID,
	drainID api.InstanceID,
) error {
	activityGauge.Reset()
	worker, err := newSampleWorker(options, registry, 2)
	if err != nil {
		return err
	}
	if err := worker.Start(ctx); err != nil {
		return err
	}
	workerStopped := false
	defer func() {
		if !workerStopped {
			shutdownWorker(worker)
		}
	}()
	for _, id := range concurrencyIDs {
		if _, err := client.ScheduleNewOrchestration(ctx, "SampleWorkerDelay",
			api.WithInstanceID(id), api.WithInput(workerDelayInput{Label: string(id), DelayMillis: 200})); err != nil {
			return err
		}
	}
	for _, id := range concurrencyIDs {
		if err := waitForWorkerOutput(ctx, client, id, "delay:"+string(id)); err != nil {
			return err
		}
	}
	if maxInflight := activityGauge.Max(); maxInflight > 2 {
		return fmt.Errorf("max in-flight activities = %d, want <= 2", maxInflight)
	}

	barrier := drainBarriers.Add(string(drainID))
	defer drainBarriers.Remove(string(drainID))
	if _, err := client.ScheduleNewOrchestration(ctx, "SampleWorkerDelay",
		api.WithInstanceID(drainID), api.WithInput(workerDelayInput{
			Label:          string(drainID),
			BlockOnBarrier: true,
		})); err != nil {
		return err
	}
	if err := barrier.WaitStarted(ctx); err != nil {
		return err
	}
	shutdownCtx, stopShutdown := context.WithTimeout(context.Background(), 10*time.Second)
	defer stopShutdown()
	shutdownDone := make(chan error, 1)
	go func() { shutdownDone <- worker.Shutdown(shutdownCtx) }()
	select {
	case err := <-shutdownDone:
		if err != nil {
			return fmt.Errorf("shutdown completed with an error while activity was blocked: %w", err)
		}
		return errors.New("shutdown completed before the accepted blocked activity was released")
	case <-time.After(200 * time.Millisecond):
	}
	barrier.Release()
	select {
	case err := <-shutdownDone:
		if err != nil {
			return err
		}
	case <-shutdownCtx.Done():
		return fmt.Errorf("shutdown did not drain the released activity before deadline: %w", shutdownCtx.Err())
	}
	workerStopped = true
	if worker.Running() {
		return errors.New("worker still reports running after Shutdown")
	}

	recovery, err := newSampleWorker(options, registry, 2)
	if err != nil {
		return err
	}
	if err := recovery.Start(ctx); err != nil {
		return err
	}
	defer shutdownWorker(recovery)
	if err := waitForWorkerOutput(ctx, client, drainID, "delay:"+string(drainID)); err != nil {
		return fmt.Errorf("post-shutdown work did not survive worker restart: %w", err)
	}
	fmt.Printf("verified Start, bounded activity concurrency (max %d), in-flight drain, and restart continuation\n", activityGauge.Max())
	return nil
}

func verifyRestart(
	ctx context.Context,
	options *durabletaskscheduler.Options,
	client *durabletaskscheduler.Client,
	registry *task.TaskRegistry,
	id api.InstanceID,
) error {
	worker, err := newSampleWorker(options, registry, 2)
	if err != nil {
		return err
	}
	if err := worker.Start(ctx); err != nil {
		return err
	}
	defer shutdownWorker(worker)
	if _, err := client.ScheduleNewOrchestration(ctx, "SampleWorkerEcho",
		api.WithInstanceID(id), api.WithInput("restart")); err != nil {
		return err
	}
	if err := waitForWorkerOutput(ctx, client, id, "echo:restart"); err != nil {
		return err
	}
	fmt.Println("verified a fresh worker restarts processing after shutdown")
	return nil
}

func verifyRun(
	ctx context.Context,
	options *durabletaskscheduler.Options,
	client *durabletaskscheduler.Client,
	registry *task.TaskRegistry,
	id api.InstanceID,
) error {
	worker, err := newSampleWorker(options, registry, 2)
	if err != nil {
		return err
	}
	runCtx, cancelRun := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- worker.Run(runCtx) }()
	stopped := false
	defer func() {
		if !stopped {
			cancelRun()
			<-done
		}
	}()
	if err := waitForWorkerRunning(ctx, worker); err != nil {
		return err
	}
	if _, err := client.ScheduleNewOrchestration(ctx, "SampleWorkerEcho",
		api.WithInstanceID(id), api.WithInput("run")); err != nil {
		return err
	}
	if err := waitForWorkerOutput(ctx, client, id, "echo:run"); err != nil {
		return err
	}
	cancelRun()
	select {
	case err := <-done:
		stopped = true
		if err != nil {
			return err
		}
	case <-time.After(5 * time.Second):
		return errors.New("run worker did not stop after its context was canceled")
	}
	fmt.Println("verified Run blocks and exits cleanly when its context is canceled")
	return nil
}

func verifyBorrowedConnectionListener(
	ctx context.Context,
	client *durabletaskscheduler.Client,
	registry *task.TaskRegistry,
	id api.InstanceID,
) error {
	if err := client.StartWorkItemListener(ctx, registry,
		durabletaskclient.WithAutoWorkItemFilters(),
		durabletaskclient.WithMaxConcurrentActivityWorkItems(1)); err != nil {
		return err
	}
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = client.StopWorkItemListener(shutdownCtx)
	}()
	if _, err := client.ScheduleNewOrchestration(ctx, "SampleWorkerEcho",
		api.WithInstanceID(id), api.WithInput("borrowed")); err != nil {
		return err
	}
	if err := waitForWorkerOutput(ctx, client, id, "echo:borrowed"); err != nil {
		return err
	}
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := client.StopWorkItemListener(shutdownCtx); err != nil {
		return err
	}
	fmt.Println("verified borrowed connection compatibility listener")
	return nil
}

func newSampleWorker(
	options *durabletaskscheduler.Options,
	registry *task.TaskRegistry,
	maxActivities int,
) (*durabletaskclient.TaskHubGrpcWorker, error) {
	return durabletaskscheduler.NewWorker(options, registry, api.DefaultLogger(),
		durabletaskclient.WithAutoWorkItemFilters(),
		durabletaskclient.WithMaxConcurrentActivityWorkItems(maxActivities),
	)
}

func shutdownWorker(worker *durabletaskclient.TaskHubGrpcWorker) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = worker.Shutdown(ctx)
}

func workerEchoWorkflow(ctx *task.OrchestrationContext) (any, error) {
	var input string
	if err := ctx.GetInput(&input); err != nil {
		return nil, err
	}
	var output string
	if err := ctx.CallActivity("SampleWorkerEchoActivity", task.WithActivityInput(input)).Await(&output); err != nil {
		return nil, err
	}
	return output, nil
}

func workerDelayWorkflow(ctx *task.OrchestrationContext) (any, error) {
	var input workerDelayInput
	if err := ctx.GetInput(&input); err != nil {
		return nil, err
	}
	var output string
	if err := ctx.CallActivity("SampleWorkerDelayActivity", task.WithActivityInput(input)).Await(&output); err != nil {
		return nil, err
	}
	return output, nil
}

func workerEchoActivity(ctx task.ActivityContext) (any, error) {
	var input string
	if err := ctx.GetInput(&input); err != nil {
		return nil, err
	}
	return "echo:" + input, nil
}

func workerDelayActivity(ctx task.ActivityContext) (any, error) {
	var input workerDelayInput
	if err := ctx.GetInput(&input); err != nil {
		return nil, err
	}
	activityGauge.Enter()
	defer activityGauge.Leave()
	if input.BlockOnBarrier {
		barrier := drainBarriers.Get(input.Label)
		if barrier == nil {
			return nil, fmt.Errorf("missing drain barrier for %s", input.Label)
		}
		barrier.MarkStarted()
		if err := barrier.WaitReleased(ctx.Context()); err != nil {
			return nil, err
		}
	}
	select {
	case <-ctx.Context().Done():
		return nil, ctx.Context().Err()
	case <-time.After(time.Duration(input.DelayMillis) * time.Millisecond):
		return "delay:" + input.Label, nil
	}
}

type workerDelayInput struct {
	Label          string `json:"label"`
	DelayMillis    int    `json:"delayMillis"`
	BlockOnBarrier bool   `json:"blockOnBarrier,omitempty"`
}

type concurrencyGauge struct {
	current atomic.Int32
	max     atomic.Int32
}

func (g *concurrencyGauge) Enter() {
	current := g.current.Add(1)
	for {
		previous := g.max.Load()
		if current <= previous || g.max.CompareAndSwap(previous, current) {
			return
		}
	}
}

func (g *concurrencyGauge) Leave() {
	g.current.Add(-1)
}

func (g *concurrencyGauge) Max() int32 {
	return g.max.Load()
}

func (g *concurrencyGauge) Reset() {
	g.current.Store(0)
	g.max.Store(0)
}

type barrierRegistry struct {
	mu       sync.Mutex
	barriers map[string]*activityBarrier
}

func newBarrierRegistry() *barrierRegistry {
	return &barrierRegistry{barriers: make(map[string]*activityBarrier)}
}

func (r *barrierRegistry) Add(key string) *activityBarrier {
	r.mu.Lock()
	defer r.mu.Unlock()
	barrier := newActivityBarrier()
	r.barriers[key] = barrier
	return barrier
}

func (r *barrierRegistry) Get(key string) *activityBarrier {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.barriers[key]
}

func (r *barrierRegistry) Remove(key string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.barriers, key)
}

type activityBarrier struct {
	startedOnce sync.Once
	releaseOnce sync.Once
	started     chan struct{}
	released    chan struct{}
}

func newActivityBarrier() *activityBarrier {
	return &activityBarrier{
		started:  make(chan struct{}),
		released: make(chan struct{}),
	}
}

func (b *activityBarrier) MarkStarted() {
	b.startedOnce.Do(func() { close(b.started) })
}

func (b *activityBarrier) Release() {
	b.releaseOnce.Do(func() { close(b.released) })
}

func (b *activityBarrier) WaitStarted(ctx context.Context) error {
	waitCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	select {
	case <-b.started:
		return nil
	case <-waitCtx.Done():
		return fmt.Errorf("blocked activity was not accepted before deadline: %w", waitCtx.Err())
	}
}

func (b *activityBarrier) WaitReleased(ctx context.Context) error {
	select {
	case <-b.released:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func waitForWorkerOutput(ctx context.Context, client *durabletaskscheduler.Client, id api.InstanceID, expected string) error {
	waitCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	metadata, err := client.WaitForOrchestrationCompletion(waitCtx, id, api.WithFetchPayloads(true))
	if err != nil {
		return err
	}
	if err := dtssample.RequireCompleted(metadata); err != nil {
		return err
	}
	var output string
	if err := metadata.ReadOutput(&output); err != nil {
		return err
	}
	if output != expected {
		return fmt.Errorf("%s output = %q, want %q", id, output, expected)
	}
	return nil
}

func waitForWorkerRunning(ctx context.Context, worker *durabletaskclient.TaskHubGrpcWorker) error {
	waitCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	ticker := time.NewTicker(25 * time.Millisecond)
	defer ticker.Stop()
	for {
		if worker.Running() {
			return nil
		}
		select {
		case <-waitCtx.Done():
			return fmt.Errorf("worker did not report running: %w", waitCtx.Err())
		case <-ticker.C:
		}
	}
}
