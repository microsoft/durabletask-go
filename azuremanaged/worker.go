package azuremanaged

import (
	"context"

	"github.com/microsoft/durabletask-go/client"
	"github.com/microsoft/durabletask-go/task"
)

// Worker executes orchestrator and activity functions for an Azure-managed Durable Task
// Scheduler (DTS) task hub. Use NewClient to schedule and manage orchestrations; a Worker
// only processes work items and cannot issue management operations.
type Worker struct {
	inner *client.TaskHubGrpcClient
}

// NewWorker creates a Worker connected to an Azure-managed DTS task hub using the supplied
// Options. The worker is assigned a unique worker id that is sent with every request.
func NewWorker(opts *Options) (*Worker, error) {
	conn, logger, err := newConnection(opts, generateWorkerID())
	if err != nil {
		return nil, err
	}
	return &Worker{inner: client.NewTaskHubGrpcClient(conn, logger)}, nil
}

// NewWorkerFromConnectionString creates a Worker from a DTS connection string of the form
// "Endpoint=<address>;Authentication=<type>;TaskHub=<name>".
func NewWorkerFromConnectionString(connectionString string) (*Worker, error) {
	opts, err := optionsFromConnectionString(connectionString)
	if err != nil {
		return nil, err
	}
	return NewWorker(opts)
}

// Start connects the worker and begins processing work items for the registered orchestrator
// and activity functions. It returns once the work-item listener is established; processing
// continues on a background goroutine until ctx is canceled.
func (w *Worker) Start(ctx context.Context, r *task.TaskRegistry) error {
	return w.inner.StartWorkItemListener(ctx, r)
}
