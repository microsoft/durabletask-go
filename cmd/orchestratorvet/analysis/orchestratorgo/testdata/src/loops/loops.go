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

// pollWithoutYield checks completion but never lets the task complete.
func pollWithoutYield(ctx *task.OrchestrationContext) (any, error) {
	work := ctx.CallActivity("work")
	polls := 0
	for !task.IsCompleted(work) { // want `loop polls task.IsCompleted without awaiting`
		polls++
		_ = strings.Repeat("a", polls)
	}
	for !(task.IsCompleted(work)) { // want `loop polls task.IsCompleted without awaiting`
	}
	return polls, work.Await(nil)
}

// spinCheckingCompletion treats IsCompleted as a pure read inside the body.
func spinCheckingCompletion(ctx *task.OrchestrationContext) (any, error) {
	work := ctx.CallActivity("work")
	ready := 0
	for { // want `unbounded orchestrator loop never awaits a durable task`
		if task.IsCompleted(work) {
			ready++
		}
	}
}

// pollingLoopsThatProgress are not reported: each can make durable progress,
// leave the loop, or observe a different task.
func pollingLoopsThatProgress(ctx *task.OrchestrationContext) (any, error) {
	other := ctx.CallActivity("other")
	work := ctx.CallActivity("work")
	for !task.IsCompleted(work) {
		_ = ctx.CreateTimer(time.Second).Await(nil)
	}
	wait := func(yield func() bool) {
		_ = ctx.CreateTimer(time.Second).Await(nil)
		yield()
	}
	for !task.IsCompleted(work) {
		for range wait {
		}
	}
	pollWithGenericIterator(work, wait)
	for !task.IsCompleted(work) {
		break
	}
	for !task.IsCompleted(work) {
		work = other
	}
	swapped := ctx.CallActivity("swapped")
	pointer := &swapped
	for !task.IsCompleted(swapped) {
		*pointer = other
	}
	captured := ctx.CallActivity("captured")
	swap := func() { captured = other }
	for !task.IsCompleted(captured) {
		swap()
	}
	tasks := []task.Task{work}
	for !task.IsCompleted(tasks[0]) {
	}
	for i := 0; !task.IsCompleted(work) && i < 3; i++ {
	}
	return nil, nil
}

func pollWithGenericIterator[F ~func(func() bool)](work task.Task, wait F) {
	for !task.IsCompleted(work) {
		for range wait {
		}
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
	_ = registry.AddOrchestrator(pollWithoutYield)
	_ = registry.AddOrchestrator(spinCheckingCompletion)
	_ = registry.AddOrchestrator(pollingLoopsThatProgress)
}
