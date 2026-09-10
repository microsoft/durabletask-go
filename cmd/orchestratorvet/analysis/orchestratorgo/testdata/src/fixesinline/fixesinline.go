// Inline imports intentionally exercise edit boundaries before formatting.
package fixesinline

import ("time" /* Keep this inline comment. */; "github.com/microsoft/durabletask-go/task")
import (clock "time") // Keep the grouped import comment.

func clocks(ctx *task.OrchestrationContext) (any, error) {
	_ = time.Now() // want `time\.Now is not deterministic in an orchestrator`
	return clock.Now(), nil // want `time\.Now is not deterministic in an orchestrator`
}

func register() {
	registry := task.NewTaskRegistry()
	_ = registry.AddOrchestrator(clocks)
}
