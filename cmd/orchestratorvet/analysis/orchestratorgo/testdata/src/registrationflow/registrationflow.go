package registrationflow

import "github.com/microsoft/durabletask-go/task"

func root(*task.OrchestrationContext) (any, error) { return nil, nil }

func work(task.ActivityContext) (any, error) { return nil, nil }

func branches(flag bool) {
	registry := task.NewTaskRegistry()
	if flag {
		_ = registry.AddOrchestratorN("branch", root)
	} else {
		_ = registry.AddOrchestratorN("branch", root)
	}
}

func switchClauses(value int) {
	registry := task.NewTaskRegistry()
	switch value {
	case 1:
		_ = registry.AddActivityN("clause", work)
	case 2:
		_ = registry.AddActivityN("clause", work)
	default:
		_ = registry.AddActivityN("clause", work)
	}
}

func typeSwitchClauses(value any) {
	registry := task.NewTaskRegistry()
	switch value.(type) {
	case int:
		_ = registry.AddActivityN("clause", work)
	case string:
		_ = registry.AddActivityN("clause", work)
	}
}

func selectClauses(first, second <-chan struct{}) {
	registry := task.NewTaskRegistry()
	select {
	case <-first:
		_ = registry.AddActivityN("clause", work)
	case <-second:
		_ = registry.AddActivityN("clause", work)
	}
}

func shortCircuit(flag bool) {
	registry := task.NewTaskRegistry()
	_ = flag && registry.AddActivityN("conditional", work) == nil
	_ = !flag && registry.AddActivityN("conditional", work) == nil
}

func reassigned() {
	registry := task.NewTaskRegistry()
	_ = registry.AddActivityN("fresh", work)
	registry = task.NewTaskRegistry()
	_ = registry.AddActivityN("fresh", work)
}

func registryResult() (*task.TaskRegistry, error) { return task.NewTaskRegistry(), nil }

func tupleAssignment() {
	registry := task.NewTaskRegistry()
	_ = registry.AddActivityN("fresh", work)
	registry, _ = registryResult()
	_ = registry.AddActivityN("fresh", work)
}

func unknownInitializer() {
	var registry, _ = registryResult()
	_ = registry.AddActivityN("unknown", work)
	_ = registry.AddActivityN("unknown", work)
}

func rangeAssignment(registries []*task.TaskRegistry) {
	registry := task.NewTaskRegistry()
	_ = registry.AddActivityN("fresh", work)
	for _, (registry) = range registries {
	}
	_ = registry.AddActivityN("fresh", work)
}

func closures(flag bool) {
	registry := task.NewTaskRegistry()
	first := func() { _ = registry.AddActivityN("closure", work) }
	second := func() { _ = registry.AddActivityN("closure", work) }
	if flag {
		first()
	} else {
		second()
	}
}

var shared = task.NewTaskRegistry()

func firstFunction()  { _ = shared.AddActivityN("shared", work) }
func secondFunction() { _ = shared.AddActivityN("shared", work) }

func indirectWrite() {
	registry := task.NewTaskRegistry()
	_ = registry.AddActivityN("fresh", work)
	*registry = *task.NewTaskRegistry()
	_ = registry.AddActivityN("fresh", work)
}

func aliasWrite() {
	registry := task.NewTaskRegistry()
	alias := registry
	_ = registry.AddActivityN("fresh", work)
	*alias = *task.NewTaskRegistry()
	_ = registry.AddActivityN("fresh", work)
}

func writeThroughOriginal() {
	registry := task.NewTaskRegistry()
	alias := registry
	_ = alias.AddActivityN("fresh", work)
	*registry = *task.NewTaskRegistry()
	_ = alias.AddActivityN("fresh", work)
}

func sharedRegistry() *task.TaskRegistry { return shared }

func resetShared() { *shared = *task.NewTaskRegistry() }

func unknownSource() {
	registry := sharedRegistry()
	_ = registry.AddActivityN("fresh", work)
	resetShared()
	_ = registry.AddActivityN("fresh", work)
}

func reset(registry *task.TaskRegistry) { *registry = *task.NewTaskRegistry() }

func helperWrite() {
	registry := task.NewTaskRegistry()
	_ = registry.AddActivityN("fresh", work)
	reset(registry)
	_ = registry.AddActivityN("fresh", work)
}

func straightLine() {
	registry := task.NewTaskRegistry()
	_ = registry.AddOrchestratorN("root", root)
	_ = registry.AddOrchestratorN("ROOT", root) // want `orchestrator "ROOT" is registered more than once`
	registry.AddActivityNVersion("work", "V1", work)
	registry.AddActivityNVersion("WORK", "v1", work) // want `activity "WORK" is registered more than once`
}

func sameClause(value int) {
	registry := task.NewTaskRegistry()
	switch value {
	case 1:
		_ = registry.AddActivityN("work", work)
		_ = registry.AddActivityN("work", work) // want `activity "work" is registered more than once`
	case 2:
		_ = registry.AddActivityN("work", work)
	}
}

func exclusiveJumps(flag bool) {
	registry := task.NewTaskRegistry()
	if flag {
		goto second
	}
	_ = registry.AddActivityN("once", work)
	goto done
second:
	_ = flag
	_ = registry.AddActivityN("once", work)
done:
	_ = flag
}

func mapKeyAliasWrite() {
	registry := task.NewTaskRegistry()
	_ = registry.AddActivityN("fresh", work)
	registries := map[*task.TaskRegistry]bool{registry: true}
	for alias := range registries {
		*alias = *task.NewTaskRegistry()
	}
	_ = registry.AddActivityN("fresh", work)
}
