package fixesstandalone

import "time"
import "github.com/microsoft/durabletask-go/task"

func currentTime(ctx *task.OrchestrationContext) (any, error) {
	return time.Now(), nil // want `time\.Now is not deterministic in an orchestrator`
}

func register() {
	registry := task.NewTaskRegistry()
	_ = registry.AddOrchestrator(currentTime)
}
