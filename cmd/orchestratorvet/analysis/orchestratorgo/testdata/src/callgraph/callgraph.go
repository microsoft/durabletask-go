// Package callgraph exercises whole-package call-graph following: named helpers,
// methods, function variables, nested literals, recursion, and unreachable code.
package callgraph

import (
	"os"
	"time"

	"github.com/microsoft/durabletask-go/task"
)

// nestedHelpers reaches a hazard two named calls away.
func nestedHelpers(ctx *task.OrchestrationContext) (any, error) {
	return firstLevel(), nil
}

func firstLevel() time.Time {
	return secondLevel()
}

func secondLevel() time.Time {
	return time.Now() // want `time\.Now is not deterministic in an orchestrator`
}

// methodReceiver reaches a hazard through a method on a package type.
type loader struct{ path string }

func (l loader) read() string {
	return os.Getenv(l.path) // want `os\.Getenv performs external I/O`
}

func (l *loader) refresh() {
	l.path = os.TempDir() // want `os\.TempDir performs external I/O`
}

func methodReceiver(ctx *task.OrchestrationContext) (any, error) {
	instance := &loader{path: "REGION"}
	instance.refresh()
	return instance.read(), nil
}

// packageFunctionVariable is a package-level variable holding a helper.
var packageFunctionVariable = func() string {
	return os.Getenv("TIER") // want `os\.Getenv performs external I/O`
}

func functionVariable(ctx *task.OrchestrationContext) (any, error) {
	local := packageFunctionVariable
	return local(), nil
}

// nestedLiteral hides a hazard inside a closure that is only ever invoked
// through the orchestration scheduler.
func nestedLiteral(ctx *task.OrchestrationContext) (any, error) {
	ctx.Go(func(child *task.OrchestrationContext) {
		inner := func() {
			go func() {}() // want `raw go statement is not deterministic in an orchestrator`
		}
		inner()
	})
	return nil, nil
}

// mutualRecursion proves the walk terminates and still reports once.
func mutualRecursion(ctx *task.OrchestrationContext) (any, error) {
	return recurseA(4), nil
}

func recurseA(depth int) time.Time {
	if depth <= 0 {
		return time.Now() // want `time\.Now is not deterministic in an orchestrator`
	}
	return recurseB(depth - 1)
}

func recurseB(depth int) time.Time {
	return recurseA(depth - 1)
}

// sharedHelper is reached from two orchestrators but reported only once.
func sharedHelper() string {
	return os.Getenv("SHARED") // want `os\.Getenv performs external I/O`
}

func firstSharer(ctx *task.OrchestrationContext) (any, error) {
	return sharedHelper(), nil
}

func secondSharer(ctx *task.OrchestrationContext) (any, error) {
	return sharedHelper(), nil
}

// unreachableHelper is never reached from a registered orchestrator.
func unreachableHelper() string {
	go func() {}()
	return os.Getenv("UNREACHABLE")
}

// unregisteredOrchestrator has an orchestrator signature but is never registered.
func unregisteredOrchestrator(ctx *task.OrchestrationContext) (any, error) {
	return time.Now(), nil
}

// reassignedHelper is assigned twice, so the analyzer cannot prove which body
// runs and follows neither.
var reassignedHelper = func() string { return os.Getenv("FIRST") }

func reassign() {
	reassignedHelper = func() string { return os.Getenv("SECOND") }
}

func ambiguousVariable(ctx *task.OrchestrationContext) (any, error) {
	return reassignedHelper(), nil
}

func register() {
	registry := task.NewTaskRegistry()
	_ = registry.AddOrchestrator(nestedHelpers)
	_ = registry.AddOrchestrator(methodReceiver)
	_ = registry.AddOrchestrator(functionVariable)
	_ = registry.AddOrchestrator(nestedLiteral)
	_ = registry.AddOrchestrator(mutualRecursion)
	_ = registry.AddOrchestrator(firstSharer)
	_ = registry.AddOrchestrator(secondSharer)
	_ = registry.AddOrchestrator(ambiguousVariable)
}
