// Package wallclock exercises the wall-clock checks.
package wallclock

import (
	"time"

	"github.com/microsoft/durabletask-go/task"
)

func clockReads(ctx *task.OrchestrationContext) (any, error) {
	start := time.Now()   // want `time\.Now is not deterministic in an orchestrator`
	_ = time.Since(start) // want `time\.Since is not deterministic in an orchestrator`
	_ = time.Until(start) // want `time\.Until is not deterministic in an orchestrator`
	return start, nil
}

func hostTimers(ctx *task.OrchestrationContext) (any, error) {
	time.Sleep(time.Second)               // want `time\.Sleep is not deterministic in an orchestrator`
	_ = time.After(time.Second)           // want `time\.After is not deterministic in an orchestrator`
	_ = time.Tick(time.Second)            // want `time\.Tick is not deterministic in an orchestrator`
	timer := time.NewTimer(time.Second)   // want `time\.NewTimer is not deterministic in an orchestrator`
	ticker := time.NewTicker(time.Second) // want `time\.NewTicker is not deterministic in an orchestrator`
	timer.Stop()
	ticker.Stop()
	time.AfterFunc(time.Second, func() {}) // want `time\.AfterFunc is not deterministic in an orchestrator`
	return nil, nil
}

// durableClock only uses the orchestration clock and durable timers.
func durableClock(ctx *task.OrchestrationContext) (any, error) {
	now := ctx.CurrentTimeUtc
	deadline := now.Add(24 * time.Hour)
	if deadline.After(now) && !deadline.IsZero() {
		_ = ctx.CreateTimer(time.Minute)
	}
	_ = deadline.Sub(now)
	_ = time.Duration(5) * time.Second
	_ = time.Date(2024, time.January, 1, 0, 0, 0, 0, time.UTC)
	return deadline.Format(time.RFC3339), nil
}

// unreachableClock is never registered, so it is never analyzed.
func unreachableClock() time.Time {
	return time.Now()
}

func register() {
	registry := task.NewTaskRegistry()
	_ = registry.AddOrchestrator(clockReads)
	_ = registry.AddOrchestrator(hostTimers)
	_ = registry.AddOrchestrator(durableClock)
}
