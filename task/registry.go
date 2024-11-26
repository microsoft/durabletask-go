package task

import (
	"fmt"

	"github.com/dapr/durabletask-go/internal/helpers"
)

// TaskRegistry contains maps of names to corresponding orchestrator and activity functions.
type TaskRegistry struct {
	orchestrators map[string]Orchestrator
	activities    map[string]Activity
}

// NewTaskRegistry returns a new [TaskRegistry] struct.
func NewTaskRegistry() *TaskRegistry {
	r := &TaskRegistry{
		orchestrators: make(map[string]Orchestrator),
		activities:    make(map[string]Activity),
	}
	return r
}

// AddOrchestrator adds an orchestrator function to the registry. The name of the orchestrator
// function is determined using reflection.
func (r *TaskRegistry) AddOrchestrator(o Orchestrator) error {
	name := helpers.GetTaskFunctionName(o)
	return r.AddOrchestratorN(name, o)
}

// AddOrchestratorN adds an orchestrator function to the registry with a specified name.
func (r *TaskRegistry) AddOrchestratorN(name string, o Orchestrator) error {
	if _, ok := r.orchestrators[name]; ok {
		return fmt.Errorf("orchestrator named '%s' is already registered", name)
	}
	r.orchestrators[name] = o
	return nil
}

// AddActivity adds an activity function to the registry. The name of the activity
// function is determined using reflection.
func (r *TaskRegistry) AddActivity(a Activity) error {
	name := helpers.GetTaskFunctionName(a)
	return r.AddActivityN(name, a)
}

// AddActivityN adds an activity function to the registry with a specified name.
func (r *TaskRegistry) AddActivityN(name string, a Activity) error {
	if _, ok := r.activities[name]; ok {
		return fmt.Errorf("activity named '%s' is already registered", name)
	}
	r.activities[name] = a
	return nil
}
