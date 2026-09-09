// Package fixes exercises the suggested fixes the analyzer offers. Fixes are
// only produced where the rewrite is unambiguous.
package fixes

import (
	"time"

	"github.com/microsoft/durabletask-go/task"
)

func clockRead(ctx *task.OrchestrationContext) (any, error) {
	now := time.Now() // want `time\.Now is not deterministic in an orchestrator`
	return now.Add(time.Hour), nil
}

func coroutine(ctx *task.OrchestrationContext) (any, error) {
	go func() { // want `raw go statement is not deterministic in an orchestrator`
		_ = ctx.CallActivity("work")
	}()
	return nil, nil
}

// commentedCoroutine proves the rewrite keeps everything written inside the
// literal, including comments, which are lost when a body is reprinted.
func commentedCoroutine(ctx *task.OrchestrationContext) (any, error) {
	go func() { // want `raw go statement is not deterministic in an orchestrator`
		// Kick off the background work.
		_ = ctx.CallActivity("work") // fire and forget
		_ = time.Now()               // want `time\.Now is not deterministic in an orchestrator`

		/* A block comment survives too. */
		if ctx.IsReplaying {
			// Nothing to do while replaying.
			return
		}
	}()
	return nil, nil
}

// noContextName has a blank context parameter, so no rewrite target exists.
func noContextName(_ *task.OrchestrationContext) (any, error) {
	return time.Now(), nil // want `time\.Now is not deterministic in an orchestrator`
}

// namedGoroutine calls a named function, which has no one-to-one coroutine form.
func namedGoroutine(ctx *task.OrchestrationContext) (any, error) {
	go background() // want `raw go statement is not deterministic in an orchestrator`
	return nil, nil
}

func background() {}

func register() {
	registry := task.NewTaskRegistry()
	_ = registry.AddOrchestrator(clockRead)
	_ = registry.AddOrchestrator(coroutine)
	_ = registry.AddOrchestrator(commentedCoroutine)
	_ = registry.AddOrchestrator(noContextName)
	_ = registry.AddOrchestrator(namedGoroutine)
}
