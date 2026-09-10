package tasknamesalias

import (
	external "github.com/example/registrationhelpers"
	"github.com/microsoft/durabletask-go/task"
)

type registryAlias = task.TaskRegistry

func caller(ctx *task.OrchestrationContext) (any, error) {
	_ = ctx.CallActivity("externalWork")
	_ = ctx.CallSubOrchestrator("externalChild")
	return nil, nil
}

func work(task.ActivityContext) (any, error) { return nil, nil }

func register() {
	var registry *registryAlias = task.NewTaskRegistry()
	_ = registry.AddOrchestrator(caller)
	_ = registry.AddActivity(work)
	alias := registry
	install := external.RegisterActivities
	install(alias)
}
