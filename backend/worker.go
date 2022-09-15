package backend

import (
	context "context"
	"log"
	"sync"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/marusama/semaphore/v2"
)

type TaskWorker interface {
	// Start starts background polling for the activity work items.
	Start(context.Context)

	// ProcessNext attempts to fetch and process a work item. This method returns
	// true if a work item was found and processing started; false otherwise. An
	// error is returned if the context is cancelled.
	ProcessNext(context.Context) (bool, error)

	// StopAndDrain stops the worker and waits for all outstanding work items to finish.
	StopAndDrain()
}

type TaskProcessor interface {
	Name() string
	FetchWorkItem(context.Context) (WorkItem, error)
	ProcessWorkItem(context.Context, WorkItem) error
	AbandonWorkItem(context.Context, WorkItem) error
	CompleteWorkItem(context.Context, WorkItem) error
}

type worker struct {
	backend Backend

	// dispatchSemaphore is for throttling orchestration concurrency.
	dispatchSemaphore semaphore.Semaphore

	// pending is for keeping track of outstanding orchestration executions.
	pending *sync.WaitGroup

	// cancel is used to cancel background polling.
	// It will be nil if background polling isn't started.
	cancel context.CancelFunc

	processor TaskProcessor

	waiting bool
}

func NewTaskWorker(be Backend, p TaskProcessor) TaskWorker {
	return &worker{
		backend:           be,
		processor:         p,
		dispatchSemaphore: semaphore.New(1), // TODO: configurable
		pending:           &sync.WaitGroup{},
		cancel:            nil, // assigned later
	}
}

func (w *worker) Name() string {
	return w.processor.Name()
}

func (w *worker) Start(ctx context.Context) {
	// TODO: Check for already started worker
	ctx, cancel := context.WithCancel(ctx)
	w.cancel = cancel

	go func() {
		b := backoff.ExponentialBackOff{
			InitialInterval:     50 * time.Millisecond,
			MaxInterval:         5 * time.Second,
			Multiplier:          1.05,
			RandomizationFactor: 0.05,
			Stop:                backoff.Stop,
			Clock:               backoff.SystemClock,
		}
		b.Reset()

		ticker := backoff.NewTicker(&b)
		defer ticker.Stop()

		w.waiting = false
	loop:
		for range ticker.C {
			select {
			case <-ctx.Done():
				break loop
			default:
				if ok, err := w.ProcessNext(ctx); ok {
					// found a work item - reset the timer to get the next one
					b.Reset()
				} else if err != nil {
					// cancelled
					break loop
				}
			}
		}
	}()
}

func (w *worker) ProcessNext(ctx context.Context) (bool, error) {
	if !w.dispatchSemaphore.TryAcquire(1) {
		log.Printf("%v: waiting for one of %v in-flight execution(s) to complete", w.Name(), w.dispatchSemaphore.GetCount())
		if err := w.dispatchSemaphore.Acquire(ctx, 1); err != nil {
			// cancelled
			return false, err
		}
	}

	w.pending.Add(1)
	wi, err := w.processor.FetchWorkItem(ctx)
	if err == ErrNoWorkItems || wi == nil {
		if !w.waiting {
			log.Printf("%v: waiting for new work items...", w.Name())
			w.waiting = true
		}
	} else if err != nil {
		log.Printf("%v: failed to fetch work item: %v", w.Name(), err)
	} else {
		// process the work-item in the background
		w.waiting = false
		go w.processWorkItem(ctx, wi)
		return true, nil
	}

	// not processing - release semaphore & mutex
	w.pending.Done()
	w.dispatchSemaphore.Release(1)
	return false, nil
}

func (w *worker) StopAndDrain() {
	// Cancel the background poller and dispatcher(s)
	if w.cancel != nil {
		w.cancel()
	}

	// Wait for outstanding work-items to finish processing.
	w.pending.Wait()
}

func (w *worker) processWorkItem(ctx context.Context, wi WorkItem) {
	defer w.dispatchSemaphore.Release(1)
	defer w.pending.Done()

	log.Printf("%v: processing work item: %s", w.Name(), wi.Description())

	if err := w.processor.ProcessWorkItem(ctx, wi); err != nil {
		log.Printf("%v: failed to process work item: %v", w.Name(), err)
		if err := w.processor.AbandonWorkItem(ctx, wi); err != nil {
			log.Printf("%v: failed to abandon work item: %v", w.Name(), err)
		}
	}

	if err := w.processor.CompleteWorkItem(ctx, wi); err != nil {
		log.Printf("%v: failed to complete work item: %v", w.Name(), err)
		if err := w.processor.AbandonWorkItem(ctx, wi); err != nil {
			log.Printf("%v: failed to abandon work item: %v", w.Name(), err)
		}
	}

	log.Printf("%v: work item processed successfully", w.Name())
}
