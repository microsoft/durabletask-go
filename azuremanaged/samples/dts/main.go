package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"

	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/microsoft/durabletask-go/azuremanaged"
	"github.com/microsoft/durabletask-go/task"
)

// This sample connects to an Azure-managed Durable Task Scheduler (DTS) task hub, starts a
// worker, schedules a simple "hello cities" orchestration, and prints the result.
//
// Configure it with environment variables:
//
//	# Option 1: a full DTS connection string
//	DTS_CONNECTION_STRING="Endpoint=<scheduler>.durabletask.io;Authentication=DefaultAzure;TaskHub=<hub>"
//
//	# Option 2: endpoint + task hub (uses DefaultAzureCredential for auth)
//	DTS_ENDPOINT="<scheduler>.durabletask.io"
//	DTS_TASKHUB="<hub>"
//
// For the local emulator, use a connection string with Authentication=None, e.g.:
//
//	DTS_CONNECTION_STRING="Endpoint=localhost:8080;Authentication=None;TaskHub=default"
func main() {
	ctx := context.Background()

	// Build the task registry with the orchestrator and activity.
	r := task.NewTaskRegistry()
	if err := r.AddOrchestratorN(orchestratorName, HelloCitiesOrchestrator); err != nil {
		log.Fatalf("Failed to register orchestrator: %v", err)
	}
	if err := r.AddActivityN(activityName, SayHelloActivity); err != nil {
		log.Fatalf("Failed to register activity: %v", err)
	}

	// Create the worker and client connected to the DTS task hub.
	worker, err := newWorker()
	if err != nil {
		log.Fatalf("Failed to create worker: %v", err)
	}
	client, err := newClient()
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}

	// Start the worker. It connects and then processes work items in the background until ctx
	// is canceled.
	if err := worker.Start(ctx, r); err != nil {
		log.Fatalf("Failed to start worker: %v", err) //nolint:gocritic // Fatalf in sample main() is acceptable
	}

	// Schedule a new orchestration instance and wait for it to complete.
	id, err := client.ScheduleNewOrchestration(ctx, orchestratorName)
	if err != nil {
		log.Fatalf("Failed to schedule new orchestration: %v", err)
	}
	log.Printf("Scheduled orchestration %q", id)

	metadata, err := client.WaitForOrchestrationCompletion(ctx, id)
	if err != nil {
		log.Fatalf("Failed to wait for orchestration to complete: %v", err)
	}

	metadataEnc, err := json.MarshalIndent(metadata, "", "  ")
	if err != nil {
		log.Fatalf("Failed to encode result to JSON: %v", err)
	}
	log.Printf("Orchestration completed: %v", string(metadataEnc))
}

// newClient creates a DTS client from the configured environment variables.
func newClient() (*azuremanaged.Client, error) {
	if cs := os.Getenv("DTS_CONNECTION_STRING"); cs != "" {
		return azuremanaged.NewClientFromConnectionString(cs)
	}
	opts, err := optionsFromEnv()
	if err != nil {
		return nil, err
	}
	return azuremanaged.NewClient(opts)
}

// newWorker creates a DTS worker from the configured environment variables.
func newWorker() (*azuremanaged.Worker, error) {
	if cs := os.Getenv("DTS_CONNECTION_STRING"); cs != "" {
		return azuremanaged.NewWorkerFromConnectionString(cs)
	}
	opts, err := optionsFromEnv()
	if err != nil {
		return nil, err
	}
	return azuremanaged.NewWorker(opts)
}

// optionsFromEnv builds Options from DTS_ENDPOINT and DTS_TASKHUB using DefaultAzureCredential.
func optionsFromEnv() (*azuremanaged.Options, error) {
	endpoint := os.Getenv("DTS_ENDPOINT")
	taskHub := os.Getenv("DTS_TASKHUB")
	if endpoint == "" || taskHub == "" {
		return nil, fmt.Errorf("set DTS_CONNECTION_STRING, or both DTS_ENDPOINT and DTS_TASKHUB")
	}

	credential, err := azidentity.NewDefaultAzureCredential(nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create DefaultAzureCredential: %w", err)
	}

	return &azuremanaged.Options{
		Endpoint:   endpoint,
		TaskHub:    taskHub,
		Credential: credential,
	}, nil
}

// Task names used to register and schedule the orchestration and activity.
const (
	orchestratorName = "HelloCities"
	activityName     = "SayHello"
)

// HelloCitiesOrchestrator calls the SayHello activity for a few cities in sequence and returns
// the greetings as an array.
func HelloCitiesOrchestrator(ctx *task.OrchestrationContext) (any, error) {
	cities := []string{"Tokyo", "London", "Seattle"}
	greetings := make([]string, 0, len(cities))
	for _, city := range cities {
		var greeting string
		if err := ctx.CallActivity(activityName, task.WithActivityInput(city)).Await(&greeting); err != nil {
			return nil, err
		}
		greetings = append(greetings, greeting)
	}
	return greetings, nil
}

// SayHelloActivity returns a friendly greeting for the supplied name.
func SayHelloActivity(ctx task.ActivityContext) (any, error) {
	var input string
	if err := ctx.GetInput(&input); err != nil {
		return "", err
	}
	return fmt.Sprintf("Hello, %s!", input), nil
}
