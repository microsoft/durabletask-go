package fixbindings

import (
	"time"

	"github.com/microsoft/durabletask-go/task"
)

func safe(ctx *task.OrchestrationContext) (any, error) {
	return time.Now(), nil // want `time\.Now is not deterministic in an orchestrator`
}

func shadowedContext(ctx *task.OrchestrationContext) (any, error) {
	{
		ctx := 1
		go func() { _ = ctx }() // want `raw go statement is not deterministic in an orchestrator`
	}
	return nil, nil
}

func shadowedImport(ctx *task.OrchestrationContext) (any, error) {
	task := 1
	go func() { _ = task }() // want `raw go statement is not deterministic in an orchestrator`
	return nil, nil
}

func sameTypeShadow(ctx *task.OrchestrationContext) (any, error) {
	{
		ctx := (*task.OrchestrationContext)(nil)
		_ = ctx
		return time.Now(), nil // want `time\.Now is not deterministic in an orchestrator`
	}
}

func reassignedContext(ctx *task.OrchestrationContext) (any, error) {
	ctx = nil
	return time.Now(), nil // want `time\.Now is not deterministic in an orchestrator`
}

func register() {
	registry := task.NewTaskRegistry()
	_ = registry.AddOrchestrator(safe)
	_ = registry.AddOrchestrator(shadowedContext)
	_ = registry.AddOrchestrator(shadowedImport)
	_ = registry.AddOrchestrator(sameTypeShadow)
	_ = registry.AddOrchestrator(reassignedContext)
}
