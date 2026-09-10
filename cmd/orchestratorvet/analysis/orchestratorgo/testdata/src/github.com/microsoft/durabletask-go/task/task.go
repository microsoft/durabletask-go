// Package task is a minimal stand-in for the durable task API. It exposes just
// enough surface for the orchestratorgo analysis test fixtures to type-check.
package task

import (
	"log/slog"
	"time"
)

type InstanceID string

type EntityID struct {
	Name string
	Key  string
}

type OrchestrationContext struct {
	ID             InstanceID
	Name           string
	Version        string
	IsReplaying    bool
	CurrentTimeUtc time.Time
}

type Task interface {
	Await(v any) error
}

type WaitGroup interface {
	Add(delta int)
	Done()
	Wait(ctx *OrchestrationContext)
}

type SelectCase interface{ isSelectCase() }

type EventChannel[T any] struct{}

func NewEventChannel[T any](ctx *OrchestrationContext, name string) *EventChannel[T] {
	return &EventChannel[T]{}
}

func (*EventChannel[T]) Receive(ctx *OrchestrationContext) T {
	var zero T
	return zero
}

func (*EventChannel[T]) ReceiveErr(ctx *OrchestrationContext) (T, error) {
	var zero T
	return zero, nil
}

func OnTask(t Task, handler func(Task)) SelectCase { return nil }

func OnEvent[T any](channel *EventChannel[T], handler func(T)) SelectCase { return nil }

func (*OrchestrationContext) Select(cases ...SelectCase) {}

func (*OrchestrationContext) WhenAny(tasks ...Task) Task { return nil }

func (*OrchestrationContext) WhenAll(tasks ...Task) error { return nil }

type CallActivityOption func()
type SubOrchestratorOption func()
type ContinueAsNewOption func()

func WithActivityInput(any) CallActivityOption            { return nil }
func WithSubOrchestrationInput(any) SubOrchestratorOption { return nil }

func (*OrchestrationContext) Go(func(*OrchestrationContext)) {}

func (*OrchestrationContext) NewWaitGroup() WaitGroup { return nil }

func (*OrchestrationContext) NewGuid() string { return "" }

func (*OrchestrationContext) Logger() *slog.Logger { return slog.Default() }

func (*OrchestrationContext) GetInput(v any) error { return nil }

func (*OrchestrationContext) CallActivity(activity any, opts ...CallActivityOption) Task { return nil }

func (*OrchestrationContext) CallSubOrchestrator(o any, opts ...SubOrchestratorOption) Task {
	return nil
}

func (*OrchestrationContext) CallEntity(id EntityID, operation string) Task { return nil }

func (*OrchestrationContext) SignalEntity(id EntityID, operation string, payload any) error {
	return nil
}

func (*OrchestrationContext) LockEntities(ids ...EntityID) (func(), error) { return nil, nil }

func (*OrchestrationContext) CreateTimer(delay time.Duration) Task { return nil }

func (*OrchestrationContext) WaitForSingleEvent(name string, timeout time.Duration) Task {
	return nil
}

func (*OrchestrationContext) ContinueAsNew(newInput any, options ...ContinueAsNewOption) {}

func (*OrchestrationContext) SetCustomStatus(status string) {}

type Orchestrator func(*OrchestrationContext) (any, error)

type ActivityContext interface {
	GetInput(v any) error
}

type Activity func(ActivityContext) (any, error)

type EntityContext interface{ Key() string }

type Entity func(EntityContext) error

type TaskRegistry struct{}

func NewTaskRegistry() *TaskRegistry {
	return &TaskRegistry{}
}

func (*TaskRegistry) AddOrchestrator(Orchestrator) error { return nil }

func (*TaskRegistry) AddOrchestratorN(string, Orchestrator) error { return nil }

func (*TaskRegistry) AddOrchestratorVersion(string, Orchestrator) error { return nil }

func (*TaskRegistry) AddOrchestratorNVersion(string, string, Orchestrator) error { return nil }

func (*TaskRegistry) AddActivity(Activity) error { return nil }

func (*TaskRegistry) AddActivityN(string, Activity) error { return nil }

func (*TaskRegistry) AddActivityVersion(string, Activity) error { return nil }

func (*TaskRegistry) AddActivityNVersion(string, string, Activity) error { return nil }

func (*TaskRegistry) AddEntity(Entity) error { return nil }

func (*TaskRegistry) AddEntityN(string, Entity) error { return nil }
