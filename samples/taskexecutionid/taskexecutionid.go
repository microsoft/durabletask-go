package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dapr/durabletask-go/backend"
	"github.com/dapr/durabletask-go/backend/sqlite"
	"github.com/dapr/durabletask-go/task"
)

func main() {
	// Create a new task registry and add the orchestrator and activities
	r := task.NewTaskRegistry()
	must(r.AddOrchestrator(RetryActivityOrchestrator))
	must(r.AddActivity(RandomFailActivity))

	// Init the client
	ctx := context.Background()
	client, worker, err := Init(ctx, r)
	if err != nil {
		log.Fatalf("Failed to initialize the client: %v", err)
	}
	defer func() {
		must(worker.Shutdown(ctx))
	}()

	// Start a new orchestration
	id, err := client.ScheduleNewOrchestration(ctx, RetryActivityOrchestrator)
	if err != nil {
		log.Fatalf("Failed to schedule new orchestration: %v", err)
	}

	// Wait for the orchestration to complete
	metadata, err := client.WaitForOrchestrationCompletion(ctx, id)
	if err != nil {
		log.Fatalf("Failed to wait for orchestration to complete: %v", err)
	}

	// Print the results
	metadataEnc, err := json.MarshalIndent(metadata, "", "  ")
	if err != nil {
		log.Fatalf("Failed to encode result to JSON: %v", err)
	}
	log.Printf("Orchestration completed: %v", string(metadataEnc))
}

// Init creates and initializes an in-memory client and worker pair with default configuration.
func Init(ctx context.Context, r *task.TaskRegistry) (backend.TaskHubClient, backend.TaskHubWorker, error) {
	logger := backend.DefaultLogger()

	// Create an executor
	executor := task.NewTaskExecutor(r)

	// Create a new backend
	// Use the in-memory sqlite provider by specifying ""
	be := sqlite.NewSqliteBackend(sqlite.NewSqliteOptions(""), logger)
	orchestrationWorker := backend.NewOrchestrationWorker(be, executor, logger)
	activityWorker := backend.NewActivityTaskWorker(be, executor, logger)
	taskHubWorker := backend.NewTaskHubWorker(be, orchestrationWorker, activityWorker, logger)

	// Start the worker
	err := taskHubWorker.Start(ctx)
	if err != nil {
		return nil, nil, err
	}

	// Get the client to the backend
	taskHubClient := backend.NewTaskHubClient(be)

	return taskHubClient, taskHubWorker, nil
}

func RetryActivityOrchestrator(ctx *task.OrchestrationContext) (any, error) {
	t := ctx.CallActivity(RandomFailActivity, task.WithActivityRetryPolicy(&task.RetryPolicy{
		MaxAttempts:          10,
		InitialRetryInterval: 100 * time.Millisecond,
		BackoffCoefficient:   2,
		MaxRetryInterval:     3 * time.Second,
	}))

	t1 := ctx.CallActivity(RandomFailActivity, task.WithActivityRetryPolicy(&task.RetryPolicy{
		MaxAttempts:          10,
		InitialRetryInterval: 100 * time.Millisecond,
		BackoffCoefficient:   2,
		MaxRetryInterval:     3 * time.Second,
	}))

	if err := t.Await(nil); err != nil {
		return nil, err
	}

	if err := t1.Await(nil); err != nil {
		return nil, err
	}

	return nil, nil
}

var (
	counters = sync.Map{}
)

// getCounter returns a Counter instance for the specified taskExecutionId.
// If no counter exists for the taskExecutionId, a new one is created.
func getCounter(taskExecutionId string) *atomic.Int32 {
	counter, _ := counters.LoadOrStore(taskExecutionId, &atomic.Int32{})
	return counter.(*atomic.Int32)
}

func RandomFailActivity(ctx task.ActivityContext) (any, error) {
	log.Println(fmt.Sprintf("#### [%v] activity %v failure", ctx.GetTaskExecutionId(), ctx.GetTaskID()))
	counter := getCounter(ctx.GetTaskExecutionId())
	// The activity should fail 5 times before succeeding.
	if counter.Load() != 5 {
		log.Println("random activity failure")
		counter.Add(1)
		return "", errors.New("random activity failure")
	}

	return "ok", nil
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}
