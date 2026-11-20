package task

import (
	"fmt"
	"slices"
)

// SelectCase is a durable selection case created by OnTask, OnEvent,
// OnReceive, or OnSend.
type SelectCase interface {
	owner() *OrchestrationContext
	ready(*selection) (bool, uint64)
	subscribe(*selection)
	unsubscribe(*selection)
	commit(*selection) bool
}

// A selection owns all its registrations. A rendezvous commits both selections
// before either handler runs, withdrawing their losing cases atomically.
type selection struct {
	ctx       *OrchestrationContext
	coroutine *coroutine
	cases     []SelectCase
	selected  SelectCase
	handler   func()
	cleaned   bool
}

func (s *selection) pending() bool {
	return s.selected == nil && !s.ctx.scope.isCanceled() && !s.coroutine.scope.isCanceled()
}

func (s *selection) preferred() SelectCase {
	var selected SelectCase
	var selectedOrder uint64
	for _, candidate := range s.cases {
		ready, order := candidate.ready(s)
		if ready && (selected == nil || order < selectedOrder) {
			selected = candidate
			selectedOrder = order
		}
	}
	return selected
}

func (s *selection) finish(candidate SelectCase, handler func()) {
	s.selected = candidate
	s.handler = handler
	s.cleanup()
	s.ctx.scheduler.makeRunnable(s.coroutine)
}

func (s *selection) cleanup() {
	if s.cleaned {
		return
	}
	s.cleaned = true
	for _, candidate := range s.cases {
		candidate.unsubscribe(s)
	}
	s.ctx.scope.removeWaiter(s.coroutine)
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

func (c *taskSelectCase) owner() *OrchestrationContext {
	return c.state.orchestrationCtx.engineContext()
}

func (c *taskSelectCase) ready(*selection) (bool, uint64) {
	return c.state.isCompleted, c.state.completionID
}

func (c *taskSelectCase) subscribe(s *selection) {
	c.state.addWaiter(s.coroutine)
}

func (c *taskSelectCase) unsubscribe(s *selection) {
	c.state.removeWaiter(s.coroutine)
}

func (c *taskSelectCase) commit(s *selection) bool {
	s.finish(c, func() {
		if c.handler != nil {
			c.handler(c.task)
		}
	})
	return true
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
// All cases must belong to the same orchestration, including its child scopes.
// Task and event cases are ordered by history; channel cases are ordered by
// deterministic scheduler readiness. Argument order breaks ties, not priority.
// Losing cases do not receive values or send messages.
func (ctx *OrchestrationContext) Select(cases ...SelectCase) {
	ctx.selectCase(cases)
}

func (ctx *OrchestrationContext) selectCase(cases []SelectCase) SelectCase {
	selected, err := ctx.selectCaseErr(cases)
	if err != nil {
		panic(err)
	}
	return selected
}

func (ctx *OrchestrationContext) selectCaseErr(cases []SelectCase) (SelectCase, error) {
	engine := ctx.effectContext()
	if len(cases) == 0 {
		panic("Select requires at least one case")
	}
	scheduler := engine.scheduler
	if scheduler == nil {
		panic("Select called outside orchestrator execution")
	}
	current := scheduler.mustCurrent()
	if current.scope.isCanceled() || ctx.scope.isCanceled() {
		return nil, ErrTaskCanceled
	}
	seen := make(map[SelectCase]struct{}, len(cases))
	for _, candidate := range cases {
		if candidate.owner() != engine {
			panic("Select case belongs to a different orchestration")
		}
		if _, exists := seen[candidate]; exists {
			panic("Select case used more than once")
		}
		seen[candidate] = struct{}{}
	}
	s := &selection{ctx: ctx, coroutine: current, cases: slices.Clone(cases)}
	current.selection = s
	defer func() {
		s.cleanup()
		current.selection = nil
	}()
	ctx.scope.addWaiter(current)
	for _, candidate := range cases {
		candidate.subscribe(s)
	}
	for {
		// A committed rendezvous is not undone by cancellation applied before
		// the other coroutine resumes.
		if s.selected != nil {
			if s.handler != nil {
				s.handler()
			}
			return s.selected, nil
		}
		if !s.pending() {
			return nil, ErrTaskCanceled
		}
		if candidate := s.preferred(); candidate != nil && candidate.commit(s) {
			continue
		}
		current.state = coroutineWaiting
		current.yield()
	}
}

func taskState(task Task) (*completableTask, bool) {
	state, ok := task.(*completableTask)
	return state, ok
}
