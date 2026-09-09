// Command retries demonstrates activity retry policies: an activity fails
// randomly and the orchestration retries it with exponential backoff.
//
//	export DTS_CONNECTION_STRING="Endpoint=http://localhost:8080;TaskHub=default;Authentication=None"
//	go run ./samples/retries
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"time"

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
	if err := r.AddOrchestratorN("RetryActivityOrchestrator", RetryActivityOrchestrator); err != nil {
		return fmt.Errorf("failed to register orchestrator: %w", err)
	}
	if err := r.AddActivityN("RandomFailActivity", RandomFailActivity); err != nil {
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
	id, err := app.Client.ScheduleNewOrchestration(ctx, "RetryActivityOrchestrator")
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

func RetryActivityOrchestrator(ctx *task.OrchestrationContext) (any, error) {
	if err := ctx.CallActivity("RandomFailActivity", task.WithActivityRetryPolicy(&task.RetryPolicy{
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
