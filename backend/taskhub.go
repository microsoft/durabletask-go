package backend

import (
	"context"
)

type TaskHubWorker interface {
	// Start starts the backend and the configured internal workers.
	Start(context.Context) error

	// Shutdown stops the backend and all internal workers.
	Shutdown(context.Context) error
}

type taskHubWorker struct {
	backend             Backend
	orchestrationWorker TaskWorker
	activityWorker      TaskWorker
	logger              Logger
}

func NewTaskHubWorker(be Backend, orchestrationWorker TaskWorker, activityWorker TaskWorker, logger Logger) TaskHubWorker {
	return &taskHubWorker{
		backend:             be,
		orchestrationWorker: orchestrationWorker,
		activityWorker:      activityWorker,
		logger:              logger,
	}
}

func (w *taskHubWorker) Start(ctx context.Context) error {
	// TODO: Check for already started worker
	if err := w.backend.CreateTaskHub(ctx); err != nil && err != ErrTaskHubExists {
		return err
	}
	if err := w.backend.Start(ctx); err != nil {
		return err
	}
	w.logger.Infof("worker started with backend %v", w.backend)

	w.orchestrationWorker.Start(ctx)
	w.activityWorker.Start(ctx)
	return nil
}

func (w *taskHubWorker) Shutdown(ctx context.Context) error {
	w.logger.Info("backend stopping...")
	if err := w.backend.Stop(ctx); err != nil {
		return err
	}

	w.logger.Info("workers stopping and draining...")
	w.orchestrationWorker.StopAndDrain()
	w.activityWorker.StopAndDrain()
	return nil
}
