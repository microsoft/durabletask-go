package task

// WaitGroup coordinates orchestration coroutines. It behaves like sync.WaitGroup,
// but Wait yields to the deterministic orchestration scheduler.
type WaitGroup interface {
	Add(delta int)
	Done()
	Wait(ctx *OrchestrationContext)
}

type orchestrationWaitGroup struct {
	scheduler *coroutineScheduler
	count     int
	waiters   map[*coroutine]struct{}
}

func newOrchestrationWaitGroup(scheduler *coroutineScheduler) *orchestrationWaitGroup {
	return &orchestrationWaitGroup{
		scheduler: scheduler,
		waiters:   make(map[*coroutine]struct{}),
	}
}

func (wg *orchestrationWaitGroup) Add(delta int) {
	next := wg.count + delta
	if next < 0 {
		panic("task.WaitGroup counter became negative")
	}
	wg.count = next
	if next != 0 {
		return
	}
	for c := range wg.waiters {
		wg.scheduler.makeRunnable(c)
	}
	clear(wg.waiters)
}

func (wg *orchestrationWaitGroup) Done() {
	wg.Add(-1)
}

func (wg *orchestrationWaitGroup) Wait(ctx *OrchestrationContext) {
	if ctx.engineContext().scheduler != wg.scheduler {
		panic("task.WaitGroup belongs to a different orchestration")
	}
	for wg.count > 0 {
		c := wg.scheduler.mustCurrent()
		ctx.scope.addWaiter(c)
		if c.scope.isCanceled() || ctx.scope.isCanceled() {
			ctx.scope.removeWaiter(c)
			panic(ErrTaskCanceled)
		}
		wg.waiters[c] = struct{}{}
		c.state = coroutineWaiting
		c.yield()
		delete(wg.waiters, c)
		ctx.scope.removeWaiter(c)
	}
}
