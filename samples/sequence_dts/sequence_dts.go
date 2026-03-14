package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"github.com/microsoft/durabletask-go/backend"
	"github.com/microsoft/durabletask-go/backend/durabletaskscheduler"
	"github.com/microsoft/durabletask-go/client"
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
	if err := r.AddOrchestrator(ActivitySequenceOrchestrator); err != nil {
		return fmt.Errorf("failed to add orchestrator: %w", err)
	}
	if err := r.AddActivity(SayHelloActivity); err != nil {
		return fmt.Errorf("failed to add activity: %w", err)
	}

	// Init the gRPC client
	ctx := context.Background()
	grpcClient, be, err := Init(ctx, r)
	if err != nil {
		return fmt.Errorf("failed to initialize the client: %w", err)
	}
	defer func() {
		if err := grpcClient.StopWorkItemListener(ctx); err != nil {
			log.Printf("Failed to stop work item listener: %v", err)
		}
		if err := be.Stop(ctx); err != nil {
			log.Printf("Failed to stop backend: %v", err)
		}
	}()

	// Start a new orchestration
	id, err := grpcClient.ScheduleNewOrchestration(ctx, "ActivitySequenceOrchestrator")
	if err != nil {
		return fmt.Errorf("failed to schedule new orchestration: %w", err)
	}

	// Wait for the orchestration to complete
	metadata, err := grpcClient.WaitForOrchestrationCompletion(ctx, id)
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

// Init creates and initializes a DTS-backed client.
// TODO: Update the endpoint address, task hub name, and scheduler configuration
// to match your DTS environment.
func Init(ctx context.Context, r *task.TaskRegistry) (*client.TaskHubGrpcClient, *durabletaskscheduler.Backend, error) {
	logger := backend.DefaultLogger()

	// Create a new DTS scheduler backend and establish the gRPC connection
	opts, err := durabletaskscheduler.NewOptionsFromConnectionString("Endpoint=https://my-scheduler.westus2.durabletask.io;TaskHub=default;Authentication=DefaultAzure")
	if err != nil {
		return nil, nil, fmt.Errorf("failed to parse connection string: %w", err)
	}
	be := durabletaskscheduler.NewBackend(opts, logger)
	if err := be.Start(ctx); err != nil {
		return nil, nil, err
	}

	// Create the gRPC client and start the streaming work item listener.
	// DTS dispatches work items via a gRPC stream rather than backend polling.
	conn, err := be.Connection()
	if err != nil {
		return nil, nil, fmt.Errorf("failed to get backend connection: %w", err)
	}
	grpcClient := client.NewTaskHubGrpcClient(conn, logger)
	if err := grpcClient.StartWorkItemListener(ctx, r); err != nil {
		return nil, nil, fmt.Errorf("failed to start work item listener: %w", err)
	}

	return grpcClient, be, nil
}

// ActivitySequenceOrchestrator makes three activity calls in sequence and returns the results
// as an array.
func ActivitySequenceOrchestrator(ctx *task.OrchestrationContext) (any, error) {
	var helloTokyo string
	if err := ctx.CallActivity(SayHelloActivity, task.WithActivityInput("Tokyo")).Await(&helloTokyo); err != nil {
		return nil, err
	}
	var helloLondon string
	if err := ctx.CallActivity(SayHelloActivity, task.WithActivityInput("London")).Await(&helloLondon); err != nil {
		return nil, err
	}
	var helloSeattle string
	if err := ctx.CallActivity(SayHelloActivity, task.WithActivityInput("Seattle")).Await(&helloSeattle); err != nil {
		return nil, err
	}
	return []string{helloTokyo, helloLondon, helloSeattle}, nil
}

// SayHelloActivity can be called by an orchestrator function and will return a friendly greeting.
func SayHelloActivity(ctx task.ActivityContext) (any, error) {
	var input string
	if err := ctx.GetInput(&input); err != nil {
		return "", err
	}
	logger := backend.DefaultLogger()
	logger.Infof("Saying hello to %s", input)
	return fmt.Sprintf("Hello, %s!", input), nil
}
