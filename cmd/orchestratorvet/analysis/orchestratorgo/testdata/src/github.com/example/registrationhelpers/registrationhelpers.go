package registrationhelpers

import "github.com/microsoft/durabletask-go/task"

// The helper's name does not limit which namespaces it can register.
func RegisterActivities(registry *task.TaskRegistry) {
	_ = registry.AddActivityN("externalWork", work)
	_ = registry.AddOrchestratorN("externalChild", child)
}

func NewRegistry() (*task.TaskRegistry, error) {
	registry := task.NewTaskRegistry()
	RegisterActivities(registry)
	return registry, nil
}

func work(task.ActivityContext) (any, error) { return nil, nil }

func child(*task.OrchestrationContext) (any, error) { return nil, nil }
