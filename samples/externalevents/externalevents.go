// Command externalevents demonstrates raising an external event into a running
// orchestration that is blocked on WaitForSingleEvent.
//
//	export DTS_CONNECTION_STRING="Endpoint=http://localhost:8080;TaskHub=default;Authentication=None"
//	go run ./samples/externalevents
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/microsoft/durabletask-go/api"
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
	if err := r.AddOrchestratorN("ExternalEventOrchestrator", ExternalEventOrchestrator); err != nil {
		return fmt.Errorf("failed to register orchestrator: %w", err)
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
	id, err := app.Client.ScheduleNewOrchestration(ctx, "ExternalEventOrchestrator")
	if err != nil {
		return fmt.Errorf("failed to schedule new orchestration: %w", err)
	}
	if _, err := app.Client.WaitForOrchestrationStart(ctx, id); err != nil {
		return fmt.Errorf("failed to wait for orchestration to start: %w", err)
	}

	// Prompt the user for their name and send that to the orchestrator
	go func() {
		fmt.Println("Enter your first name: ")
		var nameInput string
		if _, err := fmt.Scanln(&nameInput); err != nil {
			log.Printf("Failed to read input: %v", err)
			return
		}
		if err := app.Client.RaiseEvent(ctx, id, "Name", api.WithEventPayload(nameInput)); err != nil {
			log.Printf("Failed to raise event: %v", err)
		}
	}()

	// After the orchestration receives the event, it should complete on its own
	metadata, err := app.Client.WaitForOrchestrationCompletion(ctx, id)
	if err != nil {
		return fmt.Errorf("failed to wait for orchestration to complete: %w", err)
	}
	if metadata.FailureDetails != nil {
		log.Println("orchestration failed:", metadata.FailureDetails.ErrorMessage)
	} else {
		log.Println("orchestration completed:", metadata.SerializedOutput)
	}
	return nil
}

// ExternalEventOrchestrator is an orchestrator function that blocks for 30 seconds or
// until a "Name" event is sent to it.
func ExternalEventOrchestrator(ctx *task.OrchestrationContext) (any, error) {
	var nameInput string
	if err := ctx.WaitForSingleEvent("Name", 30*time.Second).Await(&nameInput); err != nil {
		// Timeout expired
		return nil, err
	}

	return fmt.Sprintf("Hello, %s!", nameInput), nil
}
