package fixesbatchstandalone

import "time" // Preserve this comment when removing the import.
import "github.com/microsoft/durabletask-go/task"

func first(ctx *task.OrchestrationContext) (any, error) {
	return time.Now(), nil // want `time\.Now is not deterministic in an orchestrator`
}

func second(other *task.OrchestrationContext) (any, error) {
	return time.Now(), nil // want `time\.Now is not deterministic in an orchestrator`
}

func register() {
	registry := task.NewTaskRegistry()
	_ = registry.AddOrchestrator(first)
	_ = registry.AddOrchestrator(second)
}
