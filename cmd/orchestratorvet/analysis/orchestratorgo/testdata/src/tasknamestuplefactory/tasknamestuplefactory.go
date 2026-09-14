package tasknamestuplefactory

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

func register() error {
	registry, err := external.NewRegistry()
	if err != nil {
		return err
	}
	_ = registry.AddOrchestrator(caller)
	_ = registry.AddActivityN("local", work)
	return nil
}
