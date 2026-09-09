package task

import "fmt"

// SelectCase is a durable selection case created by OnTask or OnEvent.
type SelectCase interface {
	ready() (bool, uint64)
	subscribe(*coroutine)
	unsubscribe(*coroutine)
	invoke()
}

type taskSelectCase struct {
	task    Task
	state   *completableTask
	handler func(Task)
}

// OnTask creates a Select case that becomes ready when task completes.
func OnTask(task Task, handler func(Task)) SelectCase {
	state, ok := taskState(task)
	if !ok {
		panic(fmt.Sprintf("task type %T cannot be used in a durable Select", task))
	}
	return &taskSelectCase{task: task, state: state, handler: handler}
}

func (c *taskSelectCase) ready() (bool, uint64) {
	return c.state.isCompleted, c.state.completionID
}

func (c *taskSelectCase) subscribe(coroutine *coroutine) {
	c.state.addWaiter(coroutine)
}

func (c *taskSelectCase) unsubscribe(coroutine *coroutine) {
	c.state.removeWaiter(coroutine)
}

func (c *taskSelectCase) invoke() {
	if c.handler != nil {
		c.handler(c.task)
	}
}

// WhenAny blocks until the first task completes and returns that task. If
// multiple tasks are ready, the task completed first in orchestration history wins.
func (ctx *OrchestrationContext) WhenAny(tasks ...Task) Task {
	if len(tasks) == 0 {
		panic("WhenAny requires at least one task")
	}
	cases := make([]SelectCase, len(tasks))
	for i, task := range tasks {
		cases[i] = OnTask(task, nil)
	}
	return ctx.selectCase(cases).(*taskSelectCase).task
}

// WhenAll blocks until every task completes and returns the earliest failure,
// ordered by orchestration history.
func (ctx *OrchestrationContext) WhenAll(tasks ...Task) error {
	var firstErr error
	var firstOrder uint64
	for _, task := range tasks {
		err := task.Await(nil)
		if err == nil {
			continue
		}
		state, ok := taskState(task)
		if !ok {
			if firstErr == nil {
				firstErr = err
				firstOrder = ^uint64(0)
			}
			continue
		}
		if firstErr == nil || state.completionID < firstOrder {
			firstErr = err
			firstOrder = state.completionID
		}
	}
	return firstErr
}

// Select waits until one case is ready and invokes its handler.
func (ctx *OrchestrationContext) Select(cases ...SelectCase) {
	ctx.selectCase(cases)
}

func (ctx *OrchestrationContext) selectCase(cases []SelectCase) SelectCase {
	if len(cases) == 0 {
		panic("Select requires at least one case")
	}
	scheduler := ctx.engineContext().scheduler
	if scheduler == nil {
		panic("Select called outside orchestrator execution")
	}
	current := scheduler.mustCurrent()

	for {
		if current.scope.isCanceled() || ctx.scope.isCanceled() {
			panic(ErrTaskCanceled)
		}
		var selected SelectCase
		var selectedOrder uint64
		for _, candidate := range cases {
			ready, order := candidate.ready()
			if ready && (selected == nil || order < selectedOrder) {
				selected = candidate
				selectedOrder = order
			}
		}
		if selected != nil {
			selected.invoke()
			return selected
		}

		ctx.scope.addWaiter(current)
		for _, candidate := range cases {
			candidate.subscribe(current)
		}
		current.state = coroutineWaiting
		current.yield()
		for _, candidate := range cases {
			candidate.unsubscribe(current)
		}
		ctx.scope.removeWaiter(current)
	}
}

func taskState(task Task) (*completableTask, bool) {
	state, ok := task.(*completableTask)
	return state, ok
}
