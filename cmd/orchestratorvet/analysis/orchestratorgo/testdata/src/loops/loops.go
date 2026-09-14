// Package loops exercises the unbounded orchestration loop check. A loop is only
// reported when the whole-package loop and call graph proves it can neither
// leave the loop nor make durable progress.
package loops

import (
	"path/filepath"
	"strings"
	"time"

	"github.com/microsoft/durabletask-go/task"
)

func spinForever(ctx *task.OrchestrationContext) (any, error) {
	counter := 0
	for { // want `unbounded orchestrator loop never awaits a durable task`
		counter++
	}
}

func spinOnConstantCondition(ctx *task.OrchestrationContext) (any, error) {
	counter := 0
	for true { // want `unbounded orchestrator loop never awaits a durable task`
		counter += 2
	}
	return counter, nil
}

func spinThroughHelpers(ctx *task.OrchestrationContext) (any, error) {
	for { // want `unbounded orchestrator loop never awaits a durable task`
		pureStep()
	}
}

func pureStep() {
	_ = strings.Repeat("a", 2)
	pureNested()
}

func pureNested() {
	_ = strings.ToUpper("b")
}

// spinThroughRecursion proves recursion terminates the walk without a stack
// overflow and still yields a report.
func spinThroughRecursion(ctx *task.OrchestrationContext) (any, error) {
	for { // want `unbounded orchestrator loop never awaits a durable task`
		recursiveA(3)
	}
}

func recursiveA(depth int) {
	if depth > 0 {
		recursiveB(depth - 1)
	}
}

func recursiveB(depth int) {
	if depth > 0 {
		recursiveA(depth - 1)
	}
}

// eternalTimerLoop makes durable progress on every iteration.
func eternalTimerLoop(ctx *task.OrchestrationContext) (any, error) {
	for {
		if err := ctx.CreateTimer(time.Hour).Await(nil); err != nil {
			return nil, err
		}
	}
}

// eternalActivityLoop awaits an activity through a same-package helper.
func eternalActivityLoop(ctx *task.OrchestrationContext) (any, error) {
	for {
		durableStep(ctx)
	}
}

func durableStep(ctx *task.OrchestrationContext) {
	_ = ctx.CallActivity("work").Await(nil)
}

// continueAsNewLoop restarts the orchestration instead of spinning.
func continueAsNewLoop(ctx *task.OrchestrationContext) (any, error) {
	for {
		ctx.ContinueAsNew(nil)
		return nil, nil
	}
}

// boundedLoops all have an exit path or a real condition.
func boundedLoops(ctx *task.OrchestrationContext) (any, error) {
	total := 0
	for i := 0; i < 10; i++ {
		total += i
	}
	for total < 100 {
		total *= 2
	}
	for {
		total++
		if total > 200 {
			break
		}
	}
	for {
		if total > 300 {
			return total, nil
		}
		total += 3
	}
}

// opaqueLoop calls a package the analyzer does not model, so nothing is proven.
func opaqueLoop(ctx *task.OrchestrationContext) (any, error) {
	joined := ""
	for {
		joined = filepath.Join(joined, "segment")
	}
}

func register() {
	registry := task.NewTaskRegistry()
	_ = registry.AddOrchestrator(spinForever)
	_ = registry.AddOrchestrator(spinOnConstantCondition)
	_ = registry.AddOrchestrator(spinThroughHelpers)
	_ = registry.AddOrchestrator(spinThroughRecursion)
	_ = registry.AddOrchestrator(eternalTimerLoop)
	_ = registry.AddOrchestrator(eternalActivityLoop)
	_ = registry.AddOrchestrator(continueAsNewLoop)
	_ = registry.AddOrchestrator(boundedLoops)
	_ = registry.AddOrchestrator(opaqueLoop)
}
