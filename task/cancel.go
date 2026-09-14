package task

type cancellationScope struct {
	parent      *cancellationScope
	parentIndex int
	children    []*cancellationScope
	tasks       []*completableTask
	coroutines  []*coroutine
	waiters     map[*coroutine]struct{}
	canceled    bool
}

func newCancellationScope(parent *cancellationScope) *cancellationScope {
	scope := &cancellationScope{parent: parent, parentIndex: -1, canceled: parent.isCanceled()}
	if parent != nil && !scope.canceled {
		scope.parentIndex = len(parent.children)
		parent.children = append(parent.children, scope)
	}
	return scope
}

// isCanceled reports whether the scope has been canceled. A nil scope is
// treated as never canceled, so callers don't need a separate nil check.
func (s *cancellationScope) isCanceled() bool {
	return s != nil && s.canceled
}

func (s *cancellationScope) addTask(task *completableTask) {
	task.scopeIndex = len(s.tasks)
	s.tasks = append(s.tasks, task)
	if s.canceled {
		task.cancel()
	}
}

func (s *cancellationScope) removeTask(task *completableTask) {
	if s == nil || task.scopeIndex < 0 || task.scopeIndex >= len(s.tasks) {
		return
	}
	index := task.scopeIndex
	last := len(s.tasks) - 1
	if index != last {
		replacement := s.tasks[last]
		s.tasks[index] = replacement
		replacement.scopeIndex = index
	}
	s.tasks[last] = nil
	s.tasks = s.tasks[:last]
	task.scopeIndex = -1
}

func (s *cancellationScope) addCoroutine(c *coroutine) {
	s.coroutines = append(s.coroutines, c)
}

func (s *cancellationScope) addWaiter(c *coroutine) {
	if s == nil {
		return
	}
	if s.waiters == nil {
		s.waiters = make(map[*coroutine]struct{})
	}
	s.waiters[c] = struct{}{}
}

func (s *cancellationScope) removeWaiter(c *coroutine) {
	if s != nil {
		delete(s.waiters, c)
	}
}

func (s *cancellationScope) cancel(scheduler *coroutineScheduler) {
	if s.canceled {
		return
	}
	s.canceled = true
	for _, child := range s.children {
		child.cancel(scheduler)
	}
	tasks := s.tasks
	s.tasks = nil
	for _, task := range tasks {
		task.scopeIndex = -1
		task.cancel()
	}
	for _, c := range s.coroutines {
		scheduler.makeRunnable(c)
	}
	for c := range s.waiters {
		scheduler.makeRunnable(c)
	}
	clear(s.waiters)
	if s.parent != nil && !s.parent.canceled && s.parentIndex >= 0 {
		last := len(s.parent.children) - 1
		if s.parentIndex != last {
			replacement := s.parent.children[last]
			s.parent.children[s.parentIndex] = replacement
			replacement.parentIndex = s.parentIndex
		}
		s.parent.children[last] = nil
		s.parent.children = s.parent.children[:last]
		s.parentIndex = -1
	}
}
