// Command parallel demonstrates fan-out/fan-in: an orchestration starts a
// dynamic number of activities at once and then awaits all of them.
//
//	export DTS_CONNECTION_STRING="Endpoint=http://localhost:8080;TaskHub=default;Authentication=None"
//	go run ./samples/parallel
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"time"

	"github.com/google/uuid"

	"github.com/microsoft/durabletask-go/samples/internal/dtssample"
	"github.com/microsoft/durabletask-go/task"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	// Create a new task registry and add the orchestrator and activities
	r := task.NewTaskRegistry()
	if err := r.AddOrchestratorN("UpdateDevicesOrchestrator", UpdateDevicesOrchestrator); err != nil {
		return fmt.Errorf("failed to register orchestrator: %w", err)
	}
	if err := r.AddActivityN("GetDevicesToUpdate", GetDevicesToUpdate); err != nil {
		return fmt.Errorf("failed to register activity: %w", err)
	}
	if err := r.AddActivityN("UpdateDevice", UpdateDevice); err != nil {
		return fmt.Errorf("failed to register activity: %w", err)
	}

	// Connect a client and worker to the Durable Task Scheduler task hub
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	app, err := dtssample.Start(ctx, r)
	if err != nil {
		return err
	}
	defer func() {
		if err := app.Shutdown(); err != nil {
			log.Printf("Failed to shut down: %v", err)
		}
	}()

	// Start a new orchestration
	id, err := app.Client.ScheduleNewOrchestration(ctx, "UpdateDevicesOrchestrator")
	if err != nil {
		return fmt.Errorf("failed to schedule new orchestration: %w", err)
	}

	// Wait for the orchestration to complete
	metadata, err := app.Client.WaitForOrchestrationCompletion(ctx, id)
	if err != nil {
		return fmt.Errorf("failed to wait for orchestration to complete: %w", err)
	}

	// Print the results
	metadataEnc, err := json.MarshalIndent(metadata, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to encode result to JSON: %w", err)
	}
	log.Printf("Orchestration completed: %v", string(metadataEnc))
	return nil
}

// UpdateDevicesOrchestrator is an orchestrator that runs activities in parallel
func UpdateDevicesOrchestrator(ctx *task.OrchestrationContext) (any, error) {
	// Get a dynamic list of devices to perform updates on
	var devices []string
	if err := ctx.CallActivity("GetDevicesToUpdate").Await(&devices); err != nil {
		return nil, err
	}

	// Start a dynamic number of tasks in parallel, not waiting for any to complete (yet)
	tasks := make([]task.Task, len(devices))
	for i, id := range devices {
		tasks[i] = ctx.CallActivity("UpdateDevice", task.WithActivityInput(id))
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
		u, err := uuid.NewV7()
		if err != nil {
			deviceIDs[i] = uuid.NewString()
			continue
		}
		deviceIDs[i] = u.String()
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
