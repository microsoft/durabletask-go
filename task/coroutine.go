package task

import (
	"errors"
	"fmt"
	"runtime/debug"
	"sync"

	"github.com/microsoft/durabletask-go/api"
)

type coroutineState uint8

const (
	coroutineRunnable coroutineState = iota
	coroutineRunning
	coroutineWaiting
	coroutineCompleted
)

type coroutineSignalKind uint8

const (
	coroutineYielded coroutineSignalKind = iota
	coroutineFinished
	coroutineCanceled
	coroutinePanicked
)

type coroutineSignal struct {
	kind       coroutineSignalKind
	panicValue any
	panicStack []byte
}

type coroutine struct {
	id        uint64
	scheduler *coroutineScheduler
	ctx       *OrchestrationContext
	fn        func()
	resume    chan struct{}
	signals   chan coroutineSignal
	stop      chan struct{}
	exited    chan struct{}
	stopOnce  sync.Once
	state     coroutineState
	scope     *cancellationScope
}

func (c *coroutine) run() {
	defer close(c.exited)
	defer func() {
		if value := recover(); value != nil {
			if c.scheduler.isStopping() && isTaskBlocked(value) {
				return
			}
			if c.scope.isCanceled() && isTaskCanceled(value) {
				c.sendSignal(coroutineSignal{kind: coroutineCanceled})
				return
			}
			c.sendSignal(coroutineSignal{
				kind:       coroutinePanicked,
				panicValue: value,
				panicStack: debug.Stack(),
			})
		}
	}()

	select {
	case <-c.resume:
	case <-c.stop:
		return
	}

	c.fn()
	c.sendSignal(coroutineSignal{kind: coroutineFinished})
}

func (c *coroutine) yield() {
	c.sendSignal(coroutineSignal{kind: coroutineYielded})
	select {
	case <-c.resume:
	case <-c.stop:
		panic(ErrTaskBlocked)
	}
}

func (c *coroutine) sendSignal(signal coroutineSignal) {
	select {
	case c.signals <- signal:
	case <-c.stop:
	}
}

func (c *coroutine) exit() {
	c.stopOnce.Do(func() {
		close(c.stop)
	})
	<-c.exited
	c.state = coroutineCompleted
}

func isTaskBlocked(value any) bool {
	err, ok := value.(error)
	return ok && errors.Is(err, ErrTaskBlocked)
}

func isTaskCanceled(value any) bool {
	err, ok := value.(error)
	return ok && errors.Is(err, ErrTaskCanceled)
}

func coroutinePanicError(id uint64, value any, stack []byte) error {
	var message string
	if err, ok := value.(error); ok {
		message = fmt.Sprintf("coroutine %d panicked: %v", id, err)
		return newPanicFailureError(api.ErrorTypeOrchestratorPanic, message, string(stack), err)
	}
	message = fmt.Sprintf("coroutine %d panicked: %v", id, value)
	return newPanicFailureError(api.ErrorTypeOrchestratorPanic, message, string(stack), nil)
}
