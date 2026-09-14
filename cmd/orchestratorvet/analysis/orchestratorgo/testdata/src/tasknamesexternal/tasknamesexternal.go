package tasknamesexternal

import (
	external "github.com/example/registrationhelpers"
	"github.com/microsoft/durabletask-go/task"
)

func caller(ctx *task.OrchestrationContext) (any, error) {
	_ = ctx.CallActivity("externalWork")
	_ = ctx.CallSubOrchestrator("externalChild")
	go func() {}() // want `raw go statement is not deterministic in an orchestrator`
	return nil, nil
}

func work(task.ActivityContext) (any, error) { return nil, nil }

func register() {
	registry := task.NewTaskRegistry()
	_ = registry.AddOrchestrator(caller)
	_ = registry.AddActivity(work)
	external.RegisterActivities(registry)
	_ = registry.AddActivity(nil) // want `task\.TaskRegistry registration with a nil activity always returns an error`
}
