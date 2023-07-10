package samples

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"time"

	"github.com/google/uuid"
	"github.com/microsoft/durabletask-go/task"
)

func RunParallelSample() {
	// Register the orchestrator and its activities
	r := task.NewTaskRegistry()
	r.AddOrchestrator(UpdateDevicesOrchestrator)
	r.AddActivity(GetDevicesToUpdate)
	r.AddActivity(UpdateDevice)

	// Run a single instance of the orchestration
	ctx := context.Background()
	client, worker := Init(ctx, r)
	defer worker.Shutdown(ctx)

	id, err := client.ScheduleNewOrchestration(ctx, UpdateDevicesOrchestrator)
	if err != nil {
		panic(err)
	}
	metadata, err := client.WaitForOrchestrationCompletion(ctx, id)
	if err != nil {
		panic(err)
	}
	json, err := json.MarshalIndent(metadata, "", "  ")
	if err != nil {
		panic(err)
	}
	fmt.Printf("orchestration completed: %s\nShutting down...", string(json))
	worker.Shutdown(ctx)
}

// UpdateDevicesOrchestrator is an orchestrator that runs activities in parallel
func UpdateDevicesOrchestrator(ctx *task.OrchestrationContext) (any, error) {
	// Get a dynamic list of devices to perform updates on
	var devices []string
	if err := ctx.CallActivity(GetDevicesToUpdate).Await(&devices); err != nil {
		return nil, err
	}

	// Start a dynamic number of tasks in parallel, not waiting for any to complete (yet)
	tasks := make([]task.Task, 0, len(devices))
	for _, id := range devices {
		tasks = append(tasks, ctx.CallActivity(UpdateDevice, task.WithActivityInput(id)))
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
	deviceIDs := make([]string, 0, deviceCount)
	for i := 0; i < deviceCount; i++ {
		deviceIDs = append(deviceIDs, uuid.NewString())
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
	fmt.Println("updating device:", deviceID)

	// Delay and success results are randomly generated
	delay := time.Duration(rand.Int31n(500))
	time.Sleep(delay)
	success := rand.Intn(3) > 0
	return success, nil
}
