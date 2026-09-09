package fixesstandalonealias

import clock "time"
import durabletask "github.com/microsoft/durabletask-go/task"

func currentTime(ctx *durabletask.OrchestrationContext) (any, error) {
	return clock.Now(), nil // want `time\.Now is not deterministic in an orchestrator`
}

func register() {
	registry := durabletask.NewTaskRegistry()
	_ = registry.AddOrchestrator(currentTime)
}
