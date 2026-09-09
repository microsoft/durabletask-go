package fixespartial

import (
	"time"

	"github.com/microsoft/durabletask-go/task"
)

func clocks(ctx *task.OrchestrationContext) (any, error) {
	_ = time.Now()   // want `time\.Now is not deterministic in an orchestrator`
	time.Now()       // want `time\.Now is not deterministic in an orchestrator`
	defer time.Now() // want `time\.Now is not deterministic in an orchestrator`
	go time.Now()    // want `raw go statement is not deterministic in an orchestrator` `time\.Now is not deterministic in an orchestrator`
	{
		ctx := 0
		_ = ctx
		_ = time.Now() // want `time\.Now is not deterministic in an orchestrator`
	}
	return time.Now(), nil // want `time\.Now is not deterministic in an orchestrator`
}

func noContext(_ *task.OrchestrationContext) (any, error) {
	return time.Now(), nil // want `time\.Now is not deterministic in an orchestrator`
}

func unreachable() any {
	return time.Now()
}

func register() {
	registry := task.NewTaskRegistry()
	_ = registry.AddOrchestrator(clocks)
	_ = registry.AddOrchestrator(noContext)
}
