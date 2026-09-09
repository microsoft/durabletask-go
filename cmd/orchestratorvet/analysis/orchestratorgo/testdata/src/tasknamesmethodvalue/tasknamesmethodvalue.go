package tasknamesmethodvalue

import "github.com/microsoft/durabletask-go/task"

func caller(ctx *task.OrchestrationContext) (any, error) {
	_ = ctx.CallActivity("indirectWork")
	_ = ctx.CallSubOrchestrator("indirectChild")
	return nil, nil
}

func work(task.ActivityContext) (any, error) { return nil, nil }

func child(*task.OrchestrationContext) (any, error) { return nil, nil }

func register() {
	registry := task.NewTaskRegistry()
	_ = registry.AddOrchestrator(caller)
	_ = registry.AddActivity(work)
	addActivity := registry.AddActivityN
	_ = addActivity("indirectWork", work)
	addOrchestrator := registry.AddOrchestratorN
	_ = addOrchestrator("indirectChild", child)
}
