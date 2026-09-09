package tasknamesinterface

import "github.com/microsoft/durabletask-go/task"

func caller(ctx *task.OrchestrationContext) (any, error) {
	_ = ctx.CallActivity("externalWork")
	_ = ctx.CallSubOrchestrator("externalChild")
	return nil, nil
}

func work(task.ActivityContext) (any, error) { return nil, nil }

func register(install func(any)) {
	registry := task.NewTaskRegistry()
	_ = registry.AddOrchestrator(caller)
	_ = registry.AddActivity(work)
	var erased any = registry
	install(erased)
}
