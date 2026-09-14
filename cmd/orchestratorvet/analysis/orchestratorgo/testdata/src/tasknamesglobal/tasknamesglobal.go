package tasknamesglobal

import "github.com/microsoft/durabletask-go/task"

var Shared *task.TaskRegistry

func caller(ctx *task.OrchestrationContext) (any, error) {
	_ = ctx.CallActivity("externalWork")
	_ = ctx.CallSubOrchestrator("externalChild")
	return nil, nil
}

func work(task.ActivityContext) (any, error) { return nil, nil }

func Register() {
	registry := task.NewTaskRegistry()
	_ = registry.AddOrchestrator(caller)
	_ = registry.AddActivity(work)
	Shared = registry
}
