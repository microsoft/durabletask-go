// Package fixesunusedliteral proves the atomic clock fix keeps the `time`
// import when it is used only in a nested literal that never executes: the
// analyzer no longer diagnoses that literal, so the import cannot be dropped.
package fixesunusedliteral

import (
	"time"

	"github.com/microsoft/durabletask-go/task"
)

func fixMe(ctx *task.OrchestrationContext) (any, error) {
	return time.Now(), nil // want `time\.Now is not deterministic in an orchestrator`
}

// dormant holds a literal that references time but is never invoked, so no
// diagnostic fires here. The atomic clock fix must NOT strip the time import.
func dormant(ctx *task.OrchestrationContext) (any, error) {
	dead := func() time.Time { return time.Now() }
	_ = dead
	return nil, nil
}

func register() {
	registry := task.NewTaskRegistry()
	_ = registry.AddOrchestrator(fixMe)
	_ = registry.AddOrchestrator(dormant)
}
