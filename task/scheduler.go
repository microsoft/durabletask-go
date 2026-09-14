package task

import (
	"container/heap"
	"fmt"
)

type coroutineQueue []*coroutine

func (q coroutineQueue) Len() int           { return len(q) }
func (q coroutineQueue) Less(i, j int) bool { return q[i].id < q[j].id }
func (q coroutineQueue) Swap(i, j int)      { q[i], q[j] = q[j], q[i] }

func (q *coroutineQueue) Push(value any) {
	*q = append(*q, value.(*coroutine))
}

func (q *coroutineQueue) Pop() any {
	old := *q
	last := len(old) - 1
	value := old[last]
	old[last] = nil
	*q = old[:last]
	return value
}

type coroutineScheduler struct {
	ctx              *OrchestrationContext
	nextID           uint64
	runQueue         coroutineQueue
	all              []*coroutine
	current          *coroutine
	root             *coroutine
	rootResult       any
	rootErr          error
	completionID     uint64
	stopping         bool
	terminalErr      error
	rootFinalized    bool
	pendingCancel    []*cancellationScope
	pendingCancelSet map[*cancellationScope]struct{}
}

func newCoroutineScheduler(ctx *OrchestrationContext) *coroutineScheduler {
	s := &coroutineScheduler{
		ctx:              ctx,
		pendingCancelSet: make(map[*cancellationScope]struct{}),
	}
	heap.Init(&s.runQueue)
	return s
}

func (s *coroutineScheduler) startRoot(fn Orchestrator) error {
	if s.root != nil {
		return fmt.Errorf("orchestrator function was started more than once")
	}

	s.root = s.spawn(s.ctx, func() {
		s.rootResult, s.rootErr = fn(s.ctx)
	})
	return nil
}

func (s *coroutineScheduler) spawn(ctx *OrchestrationContext, fn func()) *coroutine {
	if s.stopping {
		return nil
	}
	c := &coroutine{
		id:        s.nextID,
		scheduler: s,
		ctx:       ctx,
		fn:        fn,
		resume:    make(chan struct{}),
		signals:   make(chan coroutineSignal, 1),
		stop:      make(chan struct{}),
		exited:    make(chan struct{}),
		state:     coroutineRunnable,
		scope:     ctx.scope,
	}
	s.nextID++
	if c.scope != nil {
		c.scope.addCoroutine(c)
	}
	s.all = append(s.all, c)
	go c.run()
	heap.Push(&s.runQueue, c)
	return c
}

func (s *coroutineScheduler) hasRunnable() bool {
	return s.runQueue.Len() > 0
}

func (s *coroutineScheduler) runNext() {
	c := heap.Pop(&s.runQueue).(*coroutine)
	if c.state != coroutineRunnable {
		panic(fmt.Sprintf("coroutine %d is not runnable", c.id))
	}

	c.state = coroutineRunning
	s.current = c
	c.resume <- struct{}{}
	signal := <-c.signals
	s.current = nil

	switch signal.kind {
	case coroutineYielded:
		if c.state == coroutineRunning {
			c.state = coroutineWaiting
		}
	case coroutineFinished, coroutineCanceled:
		c.state = coroutineCompleted
	case coroutinePanicked:
		c.state = coroutineCompleted
		s.terminalErr = coroutinePanicError(c.id, signal.panicValue, signal.panicStack)
	default:
		panic(fmt.Sprintf("unknown coroutine signal %d", signal.kind))
	}
	s.applyPendingCancellations()
}

func (s *coroutineScheduler) waitForTask(task *completableTask) {
	c := s.mustCurrent()
	task.addWaiter(c)
	c.state = coroutineWaiting
	c.yield()
	task.removeWaiter(c)
}

func (s *coroutineScheduler) requestCancellation(scope *cancellationScope) {
	if scope == nil || scope.isCanceled() {
		return
	}
	if _, ok := s.pendingCancelSet[scope]; ok {
		return
	}
	s.pendingCancel = append(s.pendingCancel, scope)
	s.pendingCancelSet[scope] = struct{}{}
}

func (s *coroutineScheduler) applyPendingCancellations() {
	for len(s.pendingCancel) > 0 {
		scope := s.pendingCancel[0]
		s.pendingCancel = s.pendingCancel[1:]
		delete(s.pendingCancelSet, scope)
		scope.cancel(s)
	}
	s.pendingCancel = nil
}

func (s *coroutineScheduler) makeRunnable(c *coroutine) {
	if s.stopping || c.state != coroutineWaiting {
		return
	}
	c.state = coroutineRunnable
	heap.Push(&s.runQueue, c)
}

func (s *coroutineScheduler) nextCompletionID() uint64 {
	id := s.completionID
	s.completionID++
	return id
}

func (s *coroutineScheduler) mustCurrent() *coroutine {
	if s.current == nil {
		panic("durable task operation called outside an orchestration coroutine")
	}
	return s.current
}

func (s *coroutineScheduler) isRootCompleted() bool {
	return s.root != nil && s.root.state == coroutineCompleted
}

func (s *coroutineScheduler) isStopping() bool {
	return s.stopping
}

func (s *coroutineScheduler) shutdown() {
	if s.stopping {
		return
	}
	s.stopping = true
	for _, c := range s.all {
		c.exit()
	}
}
