package mocks

import (
	context "context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	backend "github.com/dapr/durabletask-go/backend"
)

var _ backend.TaskProcessor = &TestTaskProcessor{}

// TestTaskProcessor implements a dummy task processor useful for testing
type TestTaskProcessor struct {
	name string

	processingBlocked atomic.Bool

	workItemMu sync.Mutex
	workItems  []backend.WorkItem

	abandonedWorkItemMu sync.Mutex
	abandonedWorkItems  []backend.WorkItem

	completedWorkItemMu sync.Mutex
	completedWorkItems  []backend.WorkItem
}

func NewTestTaskPocessor(name string) *TestTaskProcessor {
	return &TestTaskProcessor{
		name: name,
	}
}

func (t *TestTaskProcessor) BlockProcessing() {
	t.processingBlocked.Store(true)
}

func (t *TestTaskProcessor) UnblockProcessing() {
	t.processingBlocked.Store(false)
}

func (t *TestTaskProcessor) PendingWorkItems() []backend.WorkItem {
	t.workItemMu.Lock()
	defer t.workItemMu.Unlock()

	// copy array
	return append([]backend.WorkItem{}, t.workItems...)
}

func (t *TestTaskProcessor) AbandonedWorkItems() []backend.WorkItem {
	t.abandonedWorkItemMu.Lock()
	defer t.abandonedWorkItemMu.Unlock()

	// copy array
	return append([]backend.WorkItem{}, t.abandonedWorkItems...)
}

func (t *TestTaskProcessor) CompletedWorkItems() []backend.WorkItem {
	t.completedWorkItemMu.Lock()
	defer t.completedWorkItemMu.Unlock()

	// copy array
	return append([]backend.WorkItem{}, t.completedWorkItems...)
}

func (t *TestTaskProcessor) AddWorkItems(wis ...backend.WorkItem) {
	t.workItemMu.Lock()
	defer t.workItemMu.Unlock()

	t.workItems = append(t.workItems, wis...)
}

func (t *TestTaskProcessor) Name() string {
	return t.name
}

func (t *TestTaskProcessor) FetchWorkItem(context.Context) (backend.WorkItem, error) {
	t.workItemMu.Lock()
	defer t.workItemMu.Unlock()

	if len(t.workItems) == 0 {
		return nil, backend.ErrNoWorkItems
	}

	// pop first item
	i := 0
	wi := t.workItems[i]
	t.workItems = append(t.workItems[:i], t.workItems[i+1:]...)

	return wi, nil
}

func (t *TestTaskProcessor) ProcessWorkItem(ctx context.Context, wi backend.WorkItem) error {
	if !t.processingBlocked.Load() {
		return nil
	}
	// wait for context cancellation or until processing is unblocked
	for {
		select {
		case <-ctx.Done():
			return errors.New("dummy error processing work item")
		default:
			if !t.processingBlocked.Load() {
				return nil
			}
			time.Sleep(time.Millisecond)
		}
	}
}

func (t *TestTaskProcessor) AbandonWorkItem(ctx context.Context, wi backend.WorkItem) error {
	t.abandonedWorkItemMu.Lock()
	defer t.abandonedWorkItemMu.Unlock()

	t.abandonedWorkItems = append(t.abandonedWorkItems, wi)
	return nil
}

func (t *TestTaskProcessor) CompleteWorkItem(ctx context.Context, wi backend.WorkItem) error {
	t.completedWorkItemMu.Lock()
	defer t.completedWorkItemMu.Unlock()

	t.completedWorkItems = append(t.completedWorkItems, wi)
	return nil
}
