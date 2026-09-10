package fixesbatchalias

import (
	clock "time"

	durabletask "github.com/microsoft/durabletask-go/task"
)

func first(ctx *durabletask.OrchestrationContext) (any, error) {
	_ = clock.Now() // want `time\.Now is not deterministic in an orchestrator`
	return helper(ctx), nil
}

func helper(other *durabletask.OrchestrationContext) any {
	// A reached helper uses its own context, not the caller's parameter name.
	return clock.Now() // want `time\.Now is not deterministic in an orchestrator`
}

func register() {
	registry := durabletask.NewTaskRegistry()
	_ = registry.AddOrchestrator(first)
}
