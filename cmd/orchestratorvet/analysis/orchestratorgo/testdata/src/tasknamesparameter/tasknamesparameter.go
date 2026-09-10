package tasknamesparameter

import "github.com/microsoft/durabletask-go/task"

func caller(ctx *task.OrchestrationContext) (any, error) {
	_ = ctx.CallActivity("externalWork")
	_ = ctx.CallSubOrchestrator("externalChild")
	go func() {}() // want `raw go statement is not deterministic in an orchestrator`
	return nil, nil
}

func work(task.ActivityContext) (any, error) { return nil, nil }

func Install(registry *task.TaskRegistry) {
	_ = registry.AddOrchestrator(caller)
	_ = registry.AddActivityN("local", work)
	_ = registry.AddActivity(nil) // want `task\.TaskRegistry registration with a nil activity always returns an error`
}
