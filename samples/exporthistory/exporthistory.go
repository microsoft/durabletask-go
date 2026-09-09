// Command exporthistory demonstrates the preview export history package: it
// runs a few orchestrations, then creates a batch export job that writes their
// durable histories to Azure Blob Storage as gzip-compressed JSONL.
//
// Configure it with a Durable Task Scheduler connection string and an Azure
// Storage connection string:
//
//	export DTS_CONNECTION_STRING="Endpoint=http://localhost:8080;TaskHub=default;Authentication=None"
//	export EXPORT_STORAGE_CONNECTION_STRING="UseDevelopmentStorage=true"
//	export EXPORT_CONTAINER="history-exports"
//	go run ./samples/exporthistory
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/microsoft/durabletask-go/api"
	durabletaskclient "github.com/microsoft/durabletask-go/client"
	"github.com/microsoft/durabletask-go/durabletaskscheduler"
	"github.com/microsoft/durabletask-go/exporthistory"
	"github.com/microsoft/durabletask-go/task"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	connectionString := os.Getenv("DTS_CONNECTION_STRING")
	if connectionString == "" {
		return errors.New("DTS_CONNECTION_STRING is required")
	}
	storageConnectionString := os.Getenv("EXPORT_STORAGE_CONNECTION_STRING")
	if storageConnectionString == "" {
		return errors.New("EXPORT_STORAGE_CONNECTION_STRING is required")
	}
	container := os.Getenv("EXPORT_CONTAINER")
	if container == "" {
		container = "history-exports"
	}

	options, err := durabletaskscheduler.NewOptionsFromConnectionString(connectionString)
	if err != nil {
		return fmt.Errorf("invalid DTS connection string: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	logger := api.DefaultLogger()

	// The export activities read orchestration metadata and history through the
	// same task hub client the application uses.
	client, err := durabletaskscheduler.NewClient(ctx, options, logger)
	if err != nil {
		return fmt.Errorf("failed to create the Durable Task Scheduler client: %w", err)
	}
	defer func() {
		if closeErr := client.Close(); closeErr != nil {
			log.Printf("failed to close the client: %v", closeErr)
		}
	}()

	store, err := exporthistory.NewAzureBlobHistoryStore(exporthistory.AzureBlobHistoryStoreOptions{
		ConnectionString: storageConnectionString,
		ContainerName:    container,
		// Azurite serves plaintext HTTP on loopback; production endpoints are HTTPS.
		AllowInsecureHTTP: strings.Contains(storageConnectionString, "http://"),
	})
	if err != nil {
		return fmt.Errorf("failed to create the export store: %w", err)
	}

	registry := task.NewTaskRegistry()
	if err := registry.AddOrchestratorN("Greeting", greeting); err != nil {
		return err
	}
	if err := registry.AddActivityN("SayHello", sayHello); err != nil {
		return err
	}
	if err := exporthistory.Register(registry, exporthistory.WorkerOptions{
		Source: client.TaskHubGrpcClient,
		Store:  store,
	}); err != nil {
		return err
	}

	worker, err := durabletaskscheduler.NewWorker(options, registry, logger,
		durabletaskclient.WithAutoWorkItemFilters(),
		// Keeps the unversioned export system tasks routable when the
		// application enables strict worker versioning.
		exporthistory.WithExportHistory(),
	)
	if err != nil {
		return fmt.Errorf("failed to create the worker: %w", err)
	}
	if err := worker.Start(ctx); err != nil {
		return fmt.Errorf("failed to start the worker: %w", err)
	}
	defer func() {
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer shutdownCancel()
		if shutdownErr := worker.Shutdown(shutdownCtx); shutdownErr != nil {
			log.Printf("failed to shut down the worker: %v", shutdownErr)
		}
	}()

	from := time.Now().UTC().Add(-time.Minute)
	for _, name := range []string{"Ana", "Bo", "Cyd"} {
		id, scheduleErr := client.ScheduleNewOrchestration(ctx, "Greeting", api.WithInput(name))
		if scheduleErr != nil {
			return fmt.Errorf("failed to schedule an orchestration: %w", scheduleErr)
		}
		metadata, waitErr := client.WaitForOrchestrationCompletion(ctx, id)
		if waitErr != nil {
			return fmt.Errorf("failed to wait for %s: %w", id, waitErr)
		}
		log.Printf("orchestration %s finished with status %s", id, metadata.RuntimeStatus)
	}

	exportClient, err := exporthistory.NewClient(client.TaskHubGrpcClient, exporthistory.ClientOptions{
		ContainerName: container,
	})
	if err != nil {
		return fmt.Errorf("failed to create the export client: %w", err)
	}

	job, err := exportClient.CreateJob(ctx, exporthistory.JobCreationOptions{
		Mode:              exporthistory.ExportModeBatch,
		CompletedTimeFrom: from,
		CompletedTimeTo:   time.Now().UTC(),
	})
	if err != nil {
		return fmt.Errorf("failed to create the export job: %w", err)
	}
	log.Printf("created export job %s", job.ID())

	// Batch jobs complete on their own once the window is drained.
	for {
		description, describeErr := job.Describe(ctx)
		if describeErr != nil {
			return fmt.Errorf("failed to describe the export job: %w", describeErr)
		}
		log.Printf("export job %s is %s (scanned %d, exported %d)",
			description.JobID, description.Status, description.ScannedInstances, description.ExportedInstances)
		switch description.Status {
		case exporthistory.ExportJobStatusCompleted:
			log.Printf("history exported to container %q under prefix %q",
				description.Config.Destination.Container, description.Config.Destination.Prefix)
			return nil
		case exporthistory.ExportJobStatusFailed:
			return fmt.Errorf("export job failed: %s", description.LastError)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(2 * time.Second):
		}
	}
}

func greeting(ctx *task.OrchestrationContext) (any, error) {
	var name string
	if err := ctx.GetInput(&name); err != nil {
		return nil, err
	}
	var message string
	if err := ctx.CallActivity("SayHello", task.WithActivityInput(name)).Await(&message); err != nil {
		return nil, err
	}
	return message, nil
}

func sayHello(ctx task.ActivityContext) (any, error) {
	var name string
	if err := ctx.GetInput(&name); err != nil {
		return nil, err
	}
	return "Hello, " + name + "!", nil
}
