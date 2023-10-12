package main

import (
	"context"
	"encoding/json"
	"log"
	"math/rand"
	"time"

	"github.com/google/uuid"
	"github.com/microsoft/durabletask-go/backend"
	"github.com/microsoft/durabletask-go/backend/sqlite"
	"github.com/microsoft/durabletask-go/task"
)

func main() {
	// Create a new task registry and add the orchestrator and activities
	r := task.NewTaskRegistry()
	r.AddOrchestrator(UpdateDevicesOrchestrator)
	r.AddActivity(GetDevicesToUpdate)
	r.AddActivity(UpdateDevice)

	// Init the client
	ctx := context.Background()
	client, worker, err := Init(ctx, r)
	if err != nil {
		log.Fatalf("Failed to initialize the client: %v", err)
	}
	defer worker.Shutdown(ctx)

	// Start a new orchestration
	id, err := client.ScheduleNewOrchestration(ctx, UpdateDevicesOrchestrator)
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

// UpdateDevicesOrchestrator is an orchestrator that runs activities in parallel
func UpdateDevicesOrchestrator(ctx *task.OrchestrationContext) (any, error) {
	// Get a dynamic list of devices to perform updates on
	var devices []string
	if err := ctx.CallActivity(GetDevicesToUpdate).Await(&devices); err != nil {
		return nil, err
	}

	// Start a dynamic number of tasks in parallel, not waiting for any to complete (yet)
	tasks := make([]task.Task, len(devices))
	for i, id := range devices {
		tasks[i] = ctx.CallActivity(UpdateDevice, task.WithActivityInput(id))
	}

	// Now that all are started, wait for them to complete and then return the success rate
	successCount := 0
	for _, task := range tasks {
		var succeeded bool
		if err := task.Await(&succeeded); err == nil && succeeded {
			successCount++
		}
	}

	return float32(successCount) / float32(len(devices)), nil
}

// GetDevicesToUpdate is an activity that returns a list of random device IDs to an orchestration.
func GetDevicesToUpdate(task.ActivityContext) (any, error) {
	// Return a fake list of device IDs
	const deviceCount = 10
	deviceIDs := make([]string, deviceCount)
	for i := 0; i < deviceCount; i++ {
		deviceIDs[i] = uuid.NewString()
	}
	return deviceIDs, nil
}

// UpdateDevice is an activity that takes a device ID (string) and pretends to perform an update
// on the corresponding device, with a random 67% success rate.
func UpdateDevice(ctx task.ActivityContext) (any, error) {
	var deviceID string
	if err := ctx.GetInput(&deviceID); err != nil {
		return nil, err
	}
	log.Printf("updating device: %s", deviceID)

	// Delay and success results are randomly generated
	delay := time.Duration(rand.Int31n(500)) * time.Millisecond
	select {
	case <-ctx.Context().Done():
		return nil, ctx.Context().Err()
	case <-time.After(delay):
		// All good, continue
	}

	// Simulate random failures
	success := rand.Intn(3) > 0

	return success, nil
}
