// Package crosspackage proves the unresolved-name check stays silent when a
// package registers orchestrators but no activities, because the activities are
// registered somewhere the analyzer cannot see.
package crosspackage

import "github.com/microsoft/durabletask-go/task"

func caller(ctx *task.OrchestrationContext) (any, error) {
	_ = ctx.CallActivity("registeredElsewhere")
	_ = ctx.CallActivity("alsoElsewhere")
	return nil, nil
}

func register() {
	registry := task.NewTaskRegistry()
	_ = registry.AddOrchestrator(caller)
}
