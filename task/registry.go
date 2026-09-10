package task

import (
	"fmt"
	"slices"
	"strings"
	"sync"

	"github.com/microsoft/durabletask-go/internal/helpers"
)

type taskVersionKey struct {
	name    string
	version string
}

type taskRegistration[T any] struct {
	Name    string
	Version string
	Handler T
}

// TaskRegistration identifies one registered task implementation.
type TaskRegistration struct {
	Name    string
	Version string
}

// TaskRegistrySnapshot is an immutable view of the tasks registered in a registry.
type TaskRegistrySnapshot struct {
	Orchestrators []TaskRegistration
	Activities    []TaskRegistration
	Entities      []string
}

// TaskRegistry contains registered orchestrator, activity, and entity functions.
type TaskRegistry struct {
	mu            sync.RWMutex
	orchestrators map[taskVersionKey]taskRegistration[Orchestrator]
	activities    map[taskVersionKey]taskRegistration[Activity]
	entities      map[string]EntityFactory
}

// NewTaskRegistry returns a new [TaskRegistry] struct.
func NewTaskRegistry() *TaskRegistry {
	return &TaskRegistry{
		orchestrators: make(map[taskVersionKey]taskRegistration[Orchestrator]),
		activities:    make(map[taskVersionKey]taskRegistration[Activity]),
		entities:      make(map[string]EntityFactory),
	}
}

// AddOrchestrator adds an orchestrator function to the registry. The name of the orchestrator
// function is determined using reflection.
func (r *TaskRegistry) AddOrchestrator(o Orchestrator) error {
	if o == nil {
		return fmt.Errorf("orchestrator function must not be nil")
	}
	return r.AddOrchestratorN(helpers.GetTaskFunctionName(o), o)
}

// AddOrchestratorN adds an orchestrator function to the registry with a specified name.
func (r *TaskRegistry) AddOrchestratorN(name string, o Orchestrator) error {
	return r.AddOrchestratorNVersion(name, "", o)
}

// AddOrchestratorVersion adds a versioned orchestrator whose name is determined using reflection.
func (r *TaskRegistry) AddOrchestratorVersion(version string, o Orchestrator) error {
	if o == nil {
		return fmt.Errorf("orchestrator function must not be nil")
	}
	return r.AddOrchestratorNVersion(helpers.GetTaskFunctionName(o), version, o)
}

// AddOrchestratorNVersion adds an orchestrator with an explicit name and version.
func (r *TaskRegistry) AddOrchestratorNVersion(name, version string, o Orchestrator) error {
	if o == nil {
		return fmt.Errorf("orchestrator function must not be nil")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return addTaskRegistration(r.orchestrators, "orchestrator", name, version, o)
}

// AddActivity adds an activity function to the registry. The name of the activity
// function is determined using reflection.
func (r *TaskRegistry) AddActivity(a Activity) error {
	if a == nil {
		return fmt.Errorf("activity function must not be nil")
	}
	return r.AddActivityN(helpers.GetTaskFunctionName(a), a)
}

// AddActivityN adds an activity function to the registry with a specified name.
func (r *TaskRegistry) AddActivityN(name string, a Activity) error {
	return r.AddActivityNVersion(name, "", a)
}

// AddActivityVersion adds a versioned activity whose name is determined using reflection.
func (r *TaskRegistry) AddActivityVersion(version string, a Activity) error {
	if a == nil {
		return fmt.Errorf("activity function must not be nil")
	}
	return r.AddActivityNVersion(helpers.GetTaskFunctionName(a), version, a)
}

// AddActivityNVersion adds an activity with an explicit name and version.
func (r *TaskRegistry) AddActivityNVersion(name, version string, a Activity) error {
	if a == nil {
		return fmt.Errorf("activity function must not be nil")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return addTaskRegistration(r.activities, "activity", name, version, a)
}

// AddEntity adds an entity function to the registry. The name of the entity
// function is determined using reflection.
func (r *TaskRegistry) AddEntity(e Entity) error {
	if e == nil {
		return fmt.Errorf("entity function must not be nil")
	}
	return r.AddEntityN(helpers.GetTaskFunctionName(e), e)
}

// AddEntityN adds a shared entity function with a specified name. The function
// can be invoked concurrently by different entity batches and must be thread-safe.
func (r *TaskRegistry) AddEntityN(name string, e Entity) error {
	if e == nil {
		return fmt.Errorf("entity function must not be nil")
	}
	return r.AddEntityFactoryN(name, func(EntityFactoryContext) (EntityBatch, error) {
		return EntityBatch{Entity: e}, nil
	})
}

// AddEntityFactory adds an entity factory whose name is determined using reflection.
func (r *TaskRegistry) AddEntityFactory(factory EntityFactory) error {
	if factory == nil {
		return fmt.Errorf("entity factory must not be nil")
	}
	return r.AddEntityFactoryN(helpers.GetTaskFunctionName(factory), factory)
}

// AddEntityFactoryN adds a named factory that creates one entity implementation per batch.
func (r *TaskRegistry) AddEntityFactoryN(name string, factory EntityFactory) error {
	if factory == nil {
		return fmt.Errorf("entity factory must not be nil")
	}
	if name != "*" {
		if err := helpers.ValidateEntityName(name); err != nil {
			return err
		}
		name = helpers.ToLowerInvariant(name)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.entities[name]; ok {
		return fmt.Errorf("entity named '%s' is already registered", name)
	}
	r.entities[name] = factory
	return nil
}

func (r *TaskRegistry) getOrchestrator(name, version string) (Orchestrator, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return resolveTaskRegistration(r.orchestrators, name, version)
}

func (r *TaskRegistry) getActivity(name, version string) (Activity, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return resolveTaskRegistration(r.activities, name, version)
}

func (r *TaskRegistry) hasOrchestrator(name, version string) bool {
	_, ok := r.getOrchestrator(name, version)
	return ok
}

func (r *TaskRegistry) getEntityFactory(name string) (EntityFactory, bool) {
	if name != "*" {
		name = helpers.ToLowerInvariant(name)
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	factory, ok := r.entities[name]
	if !ok {
		factory, ok = r.entities["*"]
	}
	return factory, ok
}

// Snapshot returns a deterministic, immutable view of the registry.
func (r *TaskRegistry) Snapshot() TaskRegistrySnapshot {
	r.mu.RLock()
	defer r.mu.RUnlock()
	snapshot := TaskRegistrySnapshot{
		Orchestrators: snapshotTaskRegistrations(r.orchestrators),
		Activities:    snapshotTaskRegistrations(r.activities),
		Entities:      make([]string, 0, len(r.entities)),
	}
	for name := range r.entities {
		snapshot.Entities = append(snapshot.Entities, name)
	}
	sortTaskRegistrations(snapshot.Orchestrators)
	sortTaskRegistrations(snapshot.Activities)
	slices.Sort(snapshot.Entities)
	return snapshot
}

func resolveTaskRegistration[T any](
	registrations map[taskVersionKey]taskRegistration[T],
	name string,
	version string,
) (T, bool) {
	key := taskVersionKey{
		name:    normalizeTaskName(name),
		version: normalizeTaskVersionForComparison(version),
	}
	if registration, ok := registrations[key]; ok {
		return registration.Handler, true
	}

	nameHasVersions := hasVersionedTaskRegistration(registrations, key.name)
	if key.version != "" && !nameHasVersions {
		if registration, ok := registrations[taskVersionKey{name: key.name}]; ok {
			return registration.Handler, true
		}
	}
	if key.name == "*" || nameHasVersions {
		var zero T
		return zero, false
	}

	key.name = "*"
	if registration, ok := registrations[key]; ok {
		return registration.Handler, true
	}
	if key.version != "" && !hasVersionedTaskRegistration(registrations, key.name) {
		if registration, ok := registrations[taskVersionKey{name: key.name}]; ok {
			return registration.Handler, true
		}
	}
	var zero T
	return zero, false
}

func addTaskRegistration[T any](
	registrations map[taskVersionKey]taskRegistration[T],
	kind string,
	name string,
	version string,
	handler T,
) error {
	key, err := newTaskVersionKey(name, version)
	if err != nil {
		return err
	}
	if _, ok := registrations[key]; ok {
		return duplicateTaskRegistrationError(kind, name, version)
	}
	registrations[key] = taskRegistration[T]{Name: name, Version: version, Handler: handler}
	return nil
}

func hasVersionedTaskRegistration[T any](
	registrations map[taskVersionKey]taskRegistration[T],
	name string,
) bool {
	for key := range registrations {
		if key.name == name && key.version != "" {
			return true
		}
	}
	return false
}

func snapshotTaskRegistrations[T any](
	registrations map[taskVersionKey]taskRegistration[T],
) []TaskRegistration {
	snapshot := make([]TaskRegistration, 0, len(registrations))
	for _, registration := range registrations {
		snapshot = append(snapshot, TaskRegistration{
			Name:    registration.Name,
			Version: registration.Version,
		})
	}
	return snapshot
}

func newTaskVersionKey(name, version string) (taskVersionKey, error) {
	if name == "" {
		return taskVersionKey{}, fmt.Errorf("task name must not be empty")
	}
	normalizedVersion, err := normalizeTaskVersionForRegistration(version)
	if err != nil {
		return taskVersionKey{}, err
	}
	return taskVersionKey{name: normalizeTaskName(name), version: normalizedVersion}, nil
}

func normalizeTaskName(name string) string {
	return strings.ToLower(name)
}

func normalizeTaskVersionForRegistration(version string) (string, error) {
	if version != "" && strings.TrimSpace(version) == "" {
		return "", fmt.Errorf("task version cannot consist only of whitespace")
	}
	return strings.ToLower(version), nil
}

func normalizeTaskVersionForComparison(version string) string {
	if strings.TrimSpace(version) == "" {
		return ""
	}
	return strings.ToLower(version)
}

func duplicateTaskRegistrationError(kind, name, version string) error {
	if version == "" {
		return fmt.Errorf("%s named '%s' is already registered", kind, name)
	}
	return fmt.Errorf("%s named '%s' with version '%s' is already registered", kind, name, version)
}

func sortTaskRegistrations(registrations []TaskRegistration) {
	slices.SortFunc(registrations, func(left, right TaskRegistration) int {
		if comparison := strings.Compare(strings.ToLower(left.Name), strings.ToLower(right.Name)); comparison != 0 {
			return comparison
		}
		return strings.Compare(strings.ToLower(left.Version), strings.ToLower(right.Version))
	})
}
