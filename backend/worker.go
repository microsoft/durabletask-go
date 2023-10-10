package backend

import (
	"context"
	"errors"
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
	// error is returned if the context is canceled.
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
	options *WorkerOptions
	logger  Logger
	// dispatchSemaphore is for throttling orchestration concurrency.
	dispatchSemaphore semaphore.Semaphore

	// pending is for keeping track of outstanding orchestration executions.
	pending *sync.WaitGroup

	// cancel is used to cancel background polling.
	// It will be nil if background polling isn't started.
	cancel    context.CancelFunc
	processor TaskProcessor
	waiting   bool
}

type NewTaskWorkerOptions func(*WorkerOptions)

type WorkerOptions struct {
	MaxParallelWorkItems int32
}

func NewWorkerOptions() *WorkerOptions {
	return &WorkerOptions{
		MaxParallelWorkItems: 1,
	}
}

func WithMaxParallelism(n int32) NewTaskWorkerOptions {
	return func(o *WorkerOptions) {
		o.MaxParallelWorkItems = n
	}
}

func NewTaskWorker(be Backend, p TaskProcessor, logger Logger, opts ...NewTaskWorkerOptions) TaskWorker {
	options := &WorkerOptions{MaxParallelWorkItems: 1}
	for _, configure := range opts {
		configure(options)
	}
	return &worker{
		backend:           be,
		processor:         p,
		logger:            logger,
		dispatchSemaphore: semaphore.New(int(options.MaxParallelWorkItems)),
		pending:           &sync.WaitGroup{},
		cancel:            nil, // assigned later
		options:           options,
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
		var b backoff.BackOff = &backoff.ExponentialBackOff{
			InitialInterval:     50 * time.Millisecond,
			MaxInterval:         5 * time.Second,
			Multiplier:          1.05,
			RandomizationFactor: 0.05,
			Stop:                backoff.Stop,
			Clock:               backoff.SystemClock,
		}
		b = backoff.WithContext(b, ctx)
		b.Reset()

	loop:
		for {
			// returns right away, with "ok" if a work item was found
			ok, err := w.ProcessNext(ctx)

			switch {
			case ok:
				// found a work item - reset the backoff and check for the next item
				b.Reset()
			case err != nil && errors.Is(err, ctx.Err()):
				// there's an error and it's due to the context being canceled
				w.logger.Infof("%v: received cancellation signal", w.Name())
				break loop
			case err != nil:
				// another error was encountered
				// log the error and inject some extra sleep to avoid tight failure loops
				w.logger.Errorf("unexpected worker error: %v. Adding 5 extra seconds of backoff.", err)
				t := time.NewTimer(5 * time.Second)
				select {
				case <-t.C:
					// nop - all good
				case <-ctx.Done():
					if !t.Stop() {
						<-t.C
					}
					w.logger.Infof("%v: received cancellation signal", w.Name())
					break loop
				}
			default:
				// no work item found, so sleep until the next backoff
				t := time.NewTimer(b.NextBackOff())
				select {
				case <-t.C:
					// nop - all good
				case <-ctx.Done():
					if !t.Stop() {
						<-t.C
					}
					w.logger.Infof("%v: received cancellation signal", w.Name())
					break loop
				}
			}
		}

		w.logger.Infof("%v: stopped listening for new work items", w.Name())
	}()
}

func (w *worker) ProcessNext(ctx context.Context) (bool, error) {
	if !w.dispatchSemaphore.TryAcquire(1) {
		w.logger.Debugf("%v: waiting for one of %v in-flight execution(s) to complete", w.Name(), w.dispatchSemaphore.GetCount())
		if err := w.dispatchSemaphore.Acquire(ctx, 1); err != nil {
			// canceled
			return false, err
		}
	}
	w.pending.Add(1)

	processing := false
	defer func() {
		if !processing {
			w.pending.Done()
			w.dispatchSemaphore.Release(1)
		}
	}()

	wi, err := w.processor.FetchWorkItem(ctx)
	if err == ErrNoWorkItems || wi == nil {
		if !w.waiting {
			w.logger.Debugf("%v: waiting for new work items...", w.Name())
			w.waiting = true
		}
		return false, nil
	} else if err != nil {
		if !errors.Is(err, ctx.Err()) {
			w.logger.Errorf("%v: failed to fetch work item: %v", w.Name(), err)
		}
		return false, err
	} else {
		// process the work-item in the background
		w.waiting = false
		processing = true
		go w.processWorkItem(ctx, wi)
		return true, nil
	}
}

func (w *worker) StopAndDrain() {
	// Cancel the background poller and dispatcher(s)
	if w.cancel != nil {
		w.cancel()
	}

	// Wait for outstanding work-items to finish processing.
	// TODO: Need to find a way to cancel this if it takes too long for some reason.
	w.pending.Wait()
}

func (w *worker) processWorkItem(ctx context.Context, wi WorkItem) {
	defer w.dispatchSemaphore.Release(1)
	defer w.pending.Done()

	w.logger.Debugf("%v: processing work item: %s", w.Name(), wi.Description())

	if err := w.processor.ProcessWorkItem(ctx, wi); err != nil {
		if errors.Is(err, ctx.Err()) {
			w.logger.Warnf("%v: abandoning work item due to cancellation", w.Name())
		} else {
			w.logger.Errorf("%v: failed to process work item: %v", w.Name(), err)
		}

		// Try to abandon the work item even if the existing context is canceled.
		timeoutCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()
		if err := w.processor.AbandonWorkItem(timeoutCtx, wi); err != nil {
			w.logger.Errorf("%v: failed to abandon work item: %v", w.Name(), err)
		}
		return
	}

	if err := w.processor.CompleteWorkItem(ctx, wi); err != nil {
		w.logger.Errorf("%v: failed to complete work item: %v", w.Name(), err)

		// Try to abandon the work item even if the existing context is canceled.
		timeoutCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()
		if err := w.processor.AbandonWorkItem(timeoutCtx, wi); err != nil {
			w.logger.Errorf("%v: failed to abandon work item: %v", w.Name(), err)
		}
	}

	w.logger.Debugf("%v: work item processed successfully", w.Name())
}
