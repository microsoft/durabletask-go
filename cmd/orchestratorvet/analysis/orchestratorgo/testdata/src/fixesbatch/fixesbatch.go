package fixesbatch

import (
	"time"

	"github.com/microsoft/durabletask-go/task"
)

func clocks(ctx *task.OrchestrationContext) (any, error) {
	// Both reads must be replaced together with their otherwise unused import.
	first := time.Now()                 // want `time\.Now is not deterministic in an orchestrator`
	return first.Equal(time.Now()), nil // want `time\.Now is not deterministic in an orchestrator`
}

func register() {
	registry := task.NewTaskRegistry()
	_ = registry.AddOrchestrator(clocks)
}
