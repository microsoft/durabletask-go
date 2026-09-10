package task

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTaskRegistryDispatchesByNameAndVersion(t *testing.T) {
	registry := NewTaskRegistry()
	require.NoError(t, registry.AddOrchestratorN("Process", func(*OrchestrationContext) (any, error) {
		return "unversioned", nil
	}))
	require.NoError(t, registry.AddOrchestratorNVersion("process", "V1", func(*OrchestrationContext) (any, error) {
		return "v1", nil
	}))

	handler, ok := registry.getOrchestrator("PROCESS", "v1")
	require.True(t, ok)
	result, err := handler(nil)
	require.NoError(t, err)
	require.Equal(t, "v1", result)

	handler, ok = registry.getOrchestrator("process", "")
	require.True(t, ok)
	result, err = handler(nil)
	require.NoError(t, err)
	require.Equal(t, "unversioned", result)

	_, ok = registry.getOrchestrator("process", "v2")
	require.False(t, ok, "unknown versions must not fall back when versioned registrations exist")
}

func TestTaskRegistryFallsBackToUnversionedOnlyRegistration(t *testing.T) {
	registry := NewTaskRegistry()
	require.NoError(t, registry.AddActivityN("legacy", func(ActivityContext) (any, error) {
		return "legacy", nil
	}))

	handler, ok := registry.getActivity("LEGACY", "v2")
	require.True(t, ok)
	result, err := handler(nil)
	require.NoError(t, err)
	require.Equal(t, "legacy", result)
}

func TestTaskRegistryDoesNotUseWildcardForUnknownNamedVersion(t *testing.T) {
	registry := NewTaskRegistry()
	require.NoError(t, registry.AddActivityNVersion("activity", "v1", func(ActivityContext) (any, error) {
		return "v1", nil
	}))
	require.NoError(t, registry.AddActivityN("*", func(ActivityContext) (any, error) {
		return "wildcard", nil
	}))

	_, ok := registry.getActivity("activity", "v2")
	require.False(t, ok)

	_, ok = registry.getActivity("activity", "")
	require.False(t, ok)
}

func TestTaskRegistryRejectsDuplicateVersionKeys(t *testing.T) {
	registry := NewTaskRegistry()
	handler := func(*OrchestrationContext) (any, error) { return nil, nil }
	require.NoError(t, registry.AddOrchestratorNVersion("Process", "V1", handler))
	require.Error(t, registry.AddOrchestratorNVersion("process", "v1", handler))
	require.NoError(t, registry.AddOrchestratorNVersion("process", "v2", handler))
	require.Error(t, registry.AddOrchestratorNVersion("process", " ", handler))
	require.Error(t, registry.AddOrchestrator(nil))
	require.Error(t, registry.AddActivity(nil))
	require.Error(t, registry.AddEntity(nil))
}

func TestTaskRegistrySnapshotIsSortedAndImmutable(t *testing.T) {
	registry := NewTaskRegistry()
	require.NoError(t, registry.AddOrchestratorNVersion("B", "v2", func(*OrchestrationContext) (any, error) {
		return nil, nil
	}))
	require.NoError(t, registry.AddOrchestratorN("a", func(*OrchestrationContext) (any, error) {
		return nil, nil
	}))
	require.NoError(t, registry.AddActivityNVersion("A", "v1", func(ActivityContext) (any, error) {
		return nil, nil
	}))
	require.NoError(t, registry.AddEntityN("Counter", func(*EntityContext) (any, error) {
		return nil, nil
	}))

	snapshot := registry.Snapshot()
	require.Equal(t, []TaskRegistration{{Name: "a"}, {Name: "B", Version: "v2"}}, snapshot.Orchestrators)
	require.Equal(t, []TaskRegistration{{Name: "A", Version: "v1"}}, snapshot.Activities)
	require.Equal(t, []string{"counter"}, snapshot.Entities)

	snapshot.Orchestrators[0].Name = "changed"
	require.Equal(t, "a", registry.Snapshot().Orchestrators[0].Name)
}
