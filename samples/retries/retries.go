package main

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"math/rand"
	"time"

	"github.com/microsoft/durabletask-go/backend"
	"github.com/microsoft/durabletask-go/backend/sqlite"
	"github.com/microsoft/durabletask-go/task"
)

func main() {
	// Create a new task registry and add the orchestrator and activities
	r := task.NewTaskRegistry()
	r.AddOrchestrator(RetryActivityOrchestrator)
	r.AddActivity(RandomFailActivity)

	// Init the client
	ctx := context.Background()
	client, worker, err := Init(ctx, r)
	if err != nil {
		log.Fatalf("Failed to initialize the client: %v", err)
	}
	defer worker.Shutdown(ctx)

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
	if err := ctx.CallActivity(RandomFailActivity, task.WithRetryPolicy(&task.ActivityRetryPolicy{
		MaxAttempts:          10,
		InitialRetryInterval: 100 * time.Millisecond,
		BackoffCoefficient:   2,
		MaxRetryInterval:     3 * time.Second,
	})).Await(nil); err != nil {
		return nil, err
	}
	return nil, nil
}

func RandomFailActivity(ctx task.ActivityContext) (any, error) {
	// 70% possibility for activity failure
	if rand.Intn(100) <= 70 {
		log.Println("random activity failure")
		return "", errors.New("random activity failure")
	}
	return "ok", nil
}
