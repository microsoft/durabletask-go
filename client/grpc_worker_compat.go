package client

import (
	"context"
	"fmt"

	"github.com/microsoft/durabletask-go/task"
)

// StartWorkItemListener preserves the original client API. New applications
// should create a dedicated TaskHubGrpcWorker, ideally with its own connection.
//
// The listener borrows the client's connection, so it inherits the connection
// ownership limitations documented on [NewTaskHubGrpcWorker].
func (c *TaskHubGrpcClient) StartWorkItemListener(
	ctx context.Context,
	registry *task.TaskRegistry,
	workerOptions ...TaskHubGrpcWorkerOption,
) error {
	c.listenerMu.Lock()
	defer c.listenerMu.Unlock()
	if c.listener != nil && c.listener.Running() {
		return ErrTaskHubGrpcWorkerAlreadyRunning
	}

	listenerOptions := []TaskHubGrpcWorkerOption{WithWorkerDataConverter(c.converter)}
	if c.largePayloads != nil {
		listenerOptions = append(listenerOptions, WithWorkerLargePayloads(c.largePayloads))
	}
	listenerOptions = append(listenerOptions, workerOptions...)
	worker, err := NewTaskHubGrpcWorker(c.connection, registry, c.logger, listenerOptions...)
	if err != nil {
		return fmt.Errorf("failed to create gRPC worker: %w", err)
	}
	if err := worker.Start(ctx); err != nil {
		return err
	}
	c.listener = worker
	return nil
}

// StopWorkItemListener gracefully stops the compatibility listener.
func (c *TaskHubGrpcClient) StopWorkItemListener(ctx context.Context) error {
	c.listenerMu.Lock()
	worker := c.listener
	c.listenerMu.Unlock()
	if worker == nil {
		return nil
	}
	if err := worker.Shutdown(ctx); err != nil {
		return err
	}

	c.listenerMu.Lock()
	if c.listener == worker {
		c.listener = nil
	}
	c.listenerMu.Unlock()
	return nil
}
