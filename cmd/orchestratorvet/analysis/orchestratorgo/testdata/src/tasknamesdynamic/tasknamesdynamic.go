// Package tasknamesdynamic proves the unresolved-name check stays silent when
// the package's registration set cannot be proven complete.
package tasknamesdynamic

import (
	"strings"

	"github.com/microsoft/durabletask-go/task"
)

// dynamicActivityName calls a name that is absent from the literal registrations,
// but one activity is registered under a computed name, so absence proves nothing.
func dynamicActivityName(ctx *task.OrchestrationContext) (any, error) {
	_ = ctx.CallActivity("missingActivity")
	return nil, nil
}

// wildcardSubOrchestration is covered by a "*" orchestrator registration.
func wildcardSubOrchestration(ctx *task.OrchestrationContext) (any, error) {
	_ = ctx.CallSubOrchestrator("missingChild")
	return nil, nil
}

func known(ctx task.ActivityContext) (any, error) {
	return nil, nil
}

func computed(ctx task.ActivityContext) (any, error) {
	return nil, nil
}

func fallback(ctx *task.OrchestrationContext) (any, error) {
	return nil, nil
}

func register(suffix string) {
	registry := task.NewTaskRegistry()
	_ = registry.AddOrchestrator(dynamicActivityName)
	_ = registry.AddOrchestrator(wildcardSubOrchestration)
	_ = registry.AddOrchestratorN("*", fallback)
	_ = registry.AddActivity(known)
	_ = registry.AddActivityN(strings.ToLower("computed"+suffix), computed)
}
