// Package tasknames exercises the unresolved activity and sub-orchestration
// name check against a complete, fully literal registration set.
package tasknames

import "github.com/microsoft/durabletask-go/task"

func parent(ctx *task.OrchestrationContext) (any, error) {
	_ = ctx.CallActivity("sendGreeting")
	_ = ctx.CallActivity(sendGreeting)
	_ = ctx.CallActivity("format")
	_ = ctx.CallActivity("missingActivity") // want `activity "missingActivity" is not registered with task.TaskRegistry in this package`

	_ = ctx.CallSubOrchestrator("child")
	_ = ctx.CallSubOrchestrator(child)
	_ = ctx.CallSubOrchestrator("missingChild") // want `sub-orchestration "missingChild" is not registered with task.TaskRegistry in this package`
	return nil, nil
}

// dynamicNames are computed at runtime, so the analyzer cannot prove anything.
func dynamicNames(ctx *task.OrchestrationContext) (any, error) {
	var name string
	if err := ctx.GetInput(&name); err != nil {
		return nil, err
	}
	_ = ctx.CallActivity(name)
	_ = ctx.CallSubOrchestrator(name)
	return nil, nil
}

func child(ctx *task.OrchestrationContext) (any, error) {
	return nil, nil
}

func sendGreeting(ctx task.ActivityContext) (any, error) {
	return nil, nil
}

func format(ctx task.ActivityContext) (any, error) {
	return nil, nil
}

func register() {
	registry := task.NewTaskRegistry()
	_ = registry.AddOrchestrator(parent)
	_ = registry.AddOrchestrator(dynamicNames)
	_ = registry.AddOrchestrator(child)
	_ = registry.AddActivity(sendGreeting)
	_ = registry.AddActivityN("format", format)
}
