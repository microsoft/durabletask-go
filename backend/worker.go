package backend

import (
	"context"
	"sync"
)

type TaskWorker[T WorkItem] interface {
	// Start starts background polling for the activity work items.
	Start(context.Context)

	// StopAndDrain stops the worker and waits for all outstanding work items to finish.
	StopAndDrain()
}

type TaskProcessor[T WorkItem] interface {
	Name() string
	ProcessWorkItem(context.Context, T) error
	NextWorkItem(context.Context) (T, error)
	AbandonWorkItem(context.Context, T) error
	CompleteWorkItem(context.Context, T) error
}

type worker[T WorkItem] struct {
	logger Logger

	processor    TaskProcessor[T]
	closeCh      chan struct{}
	wg           sync.WaitGroup
	workItems    chan T
	parallelLock chan struct{}
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

func NewTaskWorker[T WorkItem](p TaskProcessor[T], logger Logger, opts ...NewTaskWorkerOptions) TaskWorker[T] {
	options := &WorkerOptions{MaxParallelWorkItems: 1}
	for _, configure := range opts {
		configure(options)
	}
	return &worker[T]{
		processor:    p,
		logger:       logger,
		workItems:    make(chan T),
		parallelLock: make(chan struct{}, options.MaxParallelWorkItems),
		closeCh:      make(chan struct{}),
	}
}

func (w *worker[T]) Name() string {
	return w.processor.Name()
}

func (w *worker[T]) Start(ctx context.Context) {
	w.wg.Add(2)

	ctx, cancel := context.WithCancel(ctx)

	go func() {
		defer w.wg.Done()
		defer cancel()

		select {
		case <-w.closeCh:
		case <-ctx.Done():
		}
	}()

	go func() {
		defer w.wg.Done()
		defer w.logger.Infof("%v: worker stopped", w.Name())

		for {
			select {
			case w.parallelLock <- struct{}{}:
			case <-ctx.Done():
				return
			}

			wi, err := w.processor.NextWorkItem(ctx)
			if err != nil {
				<-w.parallelLock

				if ctx.Err() != nil {
					return
				}

				w.logger.Errorf("%v: failed to get next work item: %v", w.Name(), err)
				continue
			}

			w.wg.Add(1)
			go func() {
				defer func() {
					<-w.parallelLock
					w.wg.Done()
				}()
				w.processWorkItem(ctx, wi)
			}()
		}
	}()
}

func (w *worker[T]) StopAndDrain() {
	close(w.closeCh)
	w.wg.Wait()
}

func (w *worker[T]) processWorkItem(ctx context.Context, wi T) {
	w.logger.Debugf("%v: processing work item: %s", w.Name(), wi)

	if err := w.processor.ProcessWorkItem(ctx, wi); err != nil {
		w.logger.Errorf("%v: failed to process work item: %v", w.Name(), err)
		if err = w.processor.AbandonWorkItem(context.Background(), wi); err != nil {
			w.logger.Errorf("%v: failed to abandon work item: %v", w.Name(), err)
		}
		return
	}

	if err := w.processor.CompleteWorkItem(ctx, wi); err != nil {
		w.logger.Errorf("%v: failed to complete work item: %v", w.Name(), err)
		if err = w.processor.AbandonWorkItem(context.Background(), wi); err != nil {
			w.logger.Errorf("%v: failed to abandon work item: %v", w.Name(), err)
		}
		return
	}

	w.logger.Debugf("%v: work item processed successfully", w.Name())
}
