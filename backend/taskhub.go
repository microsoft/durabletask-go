package backend

import (
	"context"
	"log"
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
}

func NewTaskHubWorker(be Backend, orchestrationWorker TaskWorker, activityWorker TaskWorker) TaskHubWorker {
	return &taskHubWorker{
		backend:             be,
		orchestrationWorker: orchestrationWorker,
		activityWorker:      activityWorker,
	}
}

func (w *taskHubWorker) Start(ctx context.Context) error {
	// TODO: Check for already started worker
	if err := w.backend.Start(ctx); err != nil {
		return err
	}
	log.Printf("backend started: %v", w.backend)

	w.orchestrationWorker.Start(ctx)
	w.activityWorker.Start(ctx)

	log.Printf("worker started")
	return nil
}

func (w *taskHubWorker) Shutdown(ctx context.Context) error {
	if err := w.backend.Stop(ctx); err != nil {
		return err
	}
	log.Printf("backend stopped: %v", w.backend)

	w.orchestrationWorker.StopAndDrain()
	w.activityWorker.StopAndDrain()

	log.Printf("worker stopped")
	return nil
}
