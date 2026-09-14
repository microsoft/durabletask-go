// Package dtssample holds the Durable Task Scheduler setup that most runnable
// samples in this repository share: reading connection settings from the
// environment, opening a management client, and starting a worker against the
// same task hub.
//
// Point the samples at a scheduler with a connection string, for example the
// local emulator:
//
//	export DTS_CONNECTION_STRING="Endpoint=http://localhost:8080;TaskHub=default;Authentication=None"
package dtssample

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/microsoft/durabletask-go/api"
	durabletaskclient "github.com/microsoft/durabletask-go/client"
	"github.com/microsoft/durabletask-go/durabletaskscheduler"
	"github.com/microsoft/durabletask-go/task"
)

// ConnectionStringVariable names the environment variable used by the
// connection-string-driven samples to locate their DTS task hub.
const ConnectionStringVariable = "DTS_CONNECTION_STRING"

// App owns a Durable Task Scheduler client and worker pair that are connected to
// the same task hub.
type App struct {
	// Client schedules orchestrations and reads their state.
	Client *durabletaskscheduler.Client
	// worker executes the orchestrators, activities, and entities in the
	// registry it was started with, until [App.Shutdown] stops it.
	worker *durabletaskclient.TaskHubGrpcWorker
}

// Options builds scheduler options from ConnectionStringVariable.
func Options() (*durabletaskscheduler.Options, error) {
	connectionString := os.Getenv(ConnectionStringVariable)
	if connectionString == "" {
		return nil, fmt.Errorf("%s is required", ConnectionStringVariable)
	}
	options, err := durabletaskscheduler.NewOptionsFromConnectionString(connectionString)
	if err != nil {
		return nil, fmt.Errorf("invalid %s: %w", ConnectionStringVariable, err)
	}
	return options, nil
}

// Start connects a client and starts a worker for registry against the task hub
// named by ConnectionStringVariable. The caller owns the returned App and must
// call [App.Shutdown].
//
// The worker derives its work-item filters from registry; pass additional
// workerOptions to layer more behavior on top.
func Start(
	ctx context.Context,
	registry *task.TaskRegistry,
	workerOptions ...durabletaskclient.TaskHubGrpcWorkerOption,
) (*App, error) {
	options, err := Options()
	if err != nil {
		return nil, err
	}
	return StartWithOptions(ctx, options, registry, workerOptions...)
}

// StartWithOptions is [Start] for a sample that customizes its options, such as
// enabling versioning or large payloads.
func StartWithOptions(
	ctx context.Context,
	options *durabletaskscheduler.Options,
	registry *task.TaskRegistry,
	workerOptions ...durabletaskclient.TaskHubGrpcWorkerOption,
) (*App, error) {
	logger := api.DefaultLogger()
	client, err := durabletaskscheduler.NewClient(ctx, options, logger)
	if err != nil {
		return nil, fmt.Errorf("failed to connect the Durable Task Scheduler client: %w", err)
	}
	// Deriving the work-item filters from the registry is the recommended
	// default for every DTS worker, so samples do not repeat it.
	workerOptions = append(
		[]durabletaskclient.TaskHubGrpcWorkerOption{durabletaskclient.WithAutoWorkItemFilters()},
		workerOptions...,
	)
	worker, err := durabletaskscheduler.NewWorker(options, registry, logger, workerOptions...)
	if err != nil {
		return nil, errors.Join(fmt.Errorf("failed to create the worker: %w", err), client.Close())
	}
	if err := worker.Start(ctx); err != nil {
		return nil, errors.Join(fmt.Errorf("failed to start the worker: %w", err), client.Close())
	}
	return &App{Client: client, worker: worker}, nil
}

// Shutdown stops the worker and closes the client. It uses a background
// deadline so it still runs to completion once the sample's context is done.
func (a *App) Shutdown() error {
	if a == nil {
		return nil
	}
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	return errors.Join(a.worker.Shutdown(shutdownCtx), a.Client.Close())
}
