// Package unusedliterals verifies that function literals declared but never
// statically invoked are not analyzed as executable orchestrator code, while
// literals reached through statically resolvable calls, raw go/defer, or the
// (*task.OrchestrationContext).Go callback still report their hazards.
package unusedliterals

import (
	"time"

	"github.com/microsoft/durabletask-go/task"
)

// unusedNestedLiteral assigns hazards to a literal that never runs, and
// references a helper only through that literal, so neither must be reported.
func unusedNestedLiteral(ctx *task.OrchestrationContext) (any, error) {
	dormant := func() {
		_ = time.Now()
		dormantHelper()
	}
	_ = dormant
	return nil, nil
}

// dormantHelper is only reachable through the unused literal above and must
// therefore stay silent.
func dormantHelper() {
	time.Sleep(time.Second)
}

// invokedNestedLiteral is the same shape but the literal actually runs, so
// hazards inside it -- and inside helpers reached only through it -- report.
func invokedNestedLiteral(ctx *task.OrchestrationContext) (any, error) {
	live := func() {
		_ = time.Now() // want `time\.Now is not deterministic in an orchestrator`
		liveHelper()
	}
	live()
	return nil, nil
}

func liveHelper() {
	time.Sleep(time.Second) // want `time\.Sleep is not deterministic in an orchestrator`
}

// contextGoCallback verifies (*task.OrchestrationContext).Go arguments still
// count as executed.
func contextGoCallback(ctx *task.OrchestrationContext) (any, error) {
	ctx.Go(func(child *task.OrchestrationContext) {
		go func() {}() // want `raw go statement is not deterministic in an orchestrator`
	})
	return nil, nil
}

func contextGoMethodExpression(ctx *task.OrchestrationContext) (any, error) {
	(*task.OrchestrationContext).Go(ctx, func(child *task.OrchestrationContext) {
		_ = time.Now() // want `time\.Now is not deterministic in an orchestrator`
	})
	return nil, nil
}

func contextGoNamedCallback(ctx *task.OrchestrationContext) (any, error) {
	callback := func(child *task.OrchestrationContext) {
		_ = time.Now() // want `time\.Now is not deterministic in an orchestrator`
	}
	ctx.Go(callback)
	return nil, nil
}

// deferredLiteral runs at return, so its hazards report.
func deferredLiteral(ctx *task.OrchestrationContext) (any, error) {
	defer func() {
		_ = time.Now() // want `time\.Now is not deterministic in an orchestrator`
	}()
	return nil, nil
}

// immediateGoLiteral is executed by the go statement itself.
func immediateGoLiteral(ctx *task.OrchestrationContext) (any, error) {
	go func() { // want `raw go statement is not deterministic in an orchestrator`
		_ = time.Now() // want `time\.Now is not deterministic in an orchestrator`
	}()
	return nil, nil
}

// arbitraryFunctionArgument passes a literal to a helper that does not invoke
// it. The analyzer must not assume arbitrary function arguments execute.
func arbitraryFunctionArgument(ctx *task.OrchestrationContext) (any, error) {
	acceptCallback(func() {
		_ = time.Now()
	})
	return nil, nil
}

// acceptCallback stores its argument but never invokes it, so the literal
// passed above is not reachable from the orchestrator.
func acceptCallback(fn func()) {
	_ = fn
}

// singleAssignmentFuncVar exercises a package-level function variable held to
// a single literal and invoked through a local alias. The literal executes,
// so its hazards report.
var singleAssignmentFuncVar = func() {
	_ = time.Now() // want `time\.Now is not deterministic in an orchestrator`
}

func singleAssignmentInvoker(ctx *task.OrchestrationContext) (any, error) {
	local := singleAssignmentFuncVar
	local()
	return nil, nil
}

func register() {
	registry := task.NewTaskRegistry()
	_ = registry.AddOrchestrator(unusedNestedLiteral)
	_ = registry.AddOrchestrator(invokedNestedLiteral)
	_ = registry.AddOrchestrator(contextGoCallback)
	_ = registry.AddOrchestrator(contextGoMethodExpression)
	_ = registry.AddOrchestrator(contextGoNamedCallback)
	_ = registry.AddOrchestrator(deferredLiteral)
	_ = registry.AddOrchestrator(immediateGoLiteral)
	_ = registry.AddOrchestrator(arbitraryFunctionArgument)
	_ = registry.AddOrchestrator(singleAssignmentInvoker)
}
