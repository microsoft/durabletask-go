// Package registration exercises the registration-form checks.
package registration

import "github.com/microsoft/durabletask-go/task"

func alpha(ctx *task.OrchestrationContext) (any, error) { return nil, nil }

func beta(ctx *task.OrchestrationContext) (any, error) { return nil, nil }

func work(ctx task.ActivityContext) (any, error) { return nil, nil }

type workflow struct{}

func (workflow) Run(ctx *task.OrchestrationContext) (any, error) { return nil, nil }

func registerInvalid() {
	registry := task.NewTaskRegistry()

	_ = registry.AddOrchestrator(nil)                    // want `task\.TaskRegistry registration with a nil orchestrator always returns an error`
	_ = registry.AddActivity(nil)                        // want `task\.TaskRegistry registration with a nil activity always returns an error`
	_ = registry.AddOrchestratorN("", alpha)             // want `task\.TaskRegistry rejects an empty orchestrator name`
	_ = registry.AddOrchestratorN("  ", alpha)           // want `task\.TaskRegistry registers this orchestrator under a name that is only whitespace`
	_ = registry.AddActivityN("", work)                  // want `task\.TaskRegistry rejects an empty activity name`
	_ = registry.AddOrchestratorVersion(" ", alpha)      // want `task\.TaskRegistry rejects an orchestrator version that is only whitespace`
	_ = registry.AddActivityNVersion("work", "\t", work) // want `task\.TaskRegistry rejects an activity version that is only whitespace`

	_ = registry.AddOrchestratorN("duplicate", alpha)
	_ = registry.AddOrchestratorN("duplicate", beta) // want `orchestrator "duplicate" is registered more than once`
	_ = registry.AddOrchestratorN("Duplicate", beta) // want `orchestrator "Duplicate" is registered more than once`

	_ = registry.AddOrchestrator(func(ctx *task.OrchestrationContext) (any, error) { // want `task\.TaskRegistry derives the orchestrator name from a function literal`
		return nil, nil
	})
	assigned := func(ctx *task.OrchestrationContext) (any, error) { return nil, nil }
	_ = registry.AddOrchestrator(assigned)                                 // want `task\.TaskRegistry derives the orchestrator name from a function literal`
	_ = registry.AddActivity(func(ctx task.ActivityContext) (any, error) { // want `task\.TaskRegistry derives the activity name from a function literal`
		return nil, nil
	})

	instance := workflow{}
	_ = registry.AddOrchestrator(instance.Run) // want `task\.TaskRegistry derives the orchestrator name from a method value`
}

// registerValid uses every well-formed registration shape.
func registerValid() {
	registry := task.NewTaskRegistry()
	instance := workflow{}
	_ = registry.AddOrchestrator(alpha)
	_ = registry.AddOrchestratorN("named", beta)
	_ = registry.AddOrchestratorVersion("2.0", beta)
	_ = registry.AddOrchestratorNVersion("named", "2.0", beta)
	_ = registry.AddOrchestratorN("method", instance.Run)
	_ = registry.AddActivity(work)
	_ = registry.AddActivityN("work-alias", work)
}

// versionedNames prove the duplicate key matches the registry's own
// normalization, which lowercases the version and trims nothing.
func versionedNames() {
	registry := task.NewTaskRegistry()
	_ = registry.AddOrchestratorNVersion("versioned", "1.0", beta)
	_ = registry.AddOrchestratorNVersion("versioned", "2.0", beta)
	_ = registry.AddOrchestratorNVersion("versioned", "V3", beta)
	_ = registry.AddOrchestratorNVersion("versioned", "v3", beta) // want `orchestrator "versioned" is registered more than once`

	// A version padded with whitespace is a distinct registry key, because the
	// registry lowercases the version without trimming it.
	_ = registry.AddOrchestratorNVersion("padded", "4.0", beta)
	_ = registry.AddOrchestratorNVersion("padded", " 4.0", beta)
}

// computedVersions register the same name under versions that are only known at
// runtime, so no duplicate can be proven.
func computedVersions(suffix string) {
	registry := task.NewTaskRegistry()
	_ = registry.AddOrchestratorNVersion("rolling", "1."+suffix, beta)
	_ = registry.AddOrchestratorNVersion("rolling", "2."+suffix, beta)
	_ = registry.AddActivityNVersion("rollingWork", suffix, work)
	_ = registry.AddActivityNVersion("rollingWork", suffix, work)
}

// separateRegistry proves duplicate detection is scoped to one registry value.
func separateRegistry() {
	registry := task.NewTaskRegistry()
	_ = registry.AddOrchestratorN("duplicate", alpha)
}

// directInvocation calls a registered orchestrator as a plain Go function.
func directInvocation(ctx *task.OrchestrationContext) (any, error) {
	if _, err := alpha(ctx); err != nil { // want `orchestrator "alpha" is invoked directly`
		return nil, err
	}
	return ctx.CallSubOrchestrator(alpha), nil
}

func registerDirect() {
	registry := task.NewTaskRegistry()
	_ = registry.AddOrchestrator(directInvocation)
	_ = registry.AddOrchestrator(alpha)
}
