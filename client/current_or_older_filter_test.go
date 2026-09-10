package client

import (
	"slices"
	"strings"
	"testing"

	"github.com/microsoft/durabletask-go/internal/helpers"
	"github.com/microsoft/durabletask-go/task"
	"github.com/stretchr/testify/require"
)

func TestCurrentOrOlderAutoFiltersBoundRegisteredVersions(t *testing.T) {
	tests := []struct {
		name           string
		worker         string
		defaultVersion string
		versions       []string
		want           []string
	}{
		{"numeric", "2.0", "", []string{"", "1.0", "2.0", "2.0.0", "10.0"}, []string{"", "1.0", "2.0"}},
		{"opaque", "v2", "", []string{"", "v1", "V2", "v3"}, []string{"", "v1", "V2"}},
		{"unversioned fallback", "2.0", "", []string{""}, []string{"", "2.0"}},
		{"older default fallback", "2.0", "1.0", []string{""}, []string{"", "1.0", "2.0"}},
		{"equivalent default fallback", "v2", "V2", []string{""}, []string{"", "v2"}},
		{"newer default excluded", "2.0", "3.0", []string{""}, []string{"", "2.0"}},
		{"versioned handler disables fallback", "2.0", "1.0", []string{"", "2.0"}, []string{"", "2.0"}},
		{"newer only", "2.0", "", []string{"3.0"}, nil},
		{"unversioned worker", "", "1.0", []string{"", "1.0"}, []string{""}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			registrations := make([]task.TaskRegistration, 0, len(test.versions))
			for _, version := range test.versions {
				registrations = append(registrations, task.TaskRegistration{Name: "mixed", Version: version})
			}
			filters := workItemFiltersFromRegistry(task.TaskRegistrySnapshot{
				Orchestrators: registrations,
				Activities:    registrations,
			}, &task.VersioningOptions{
				Version: test.worker, DefaultVersion: test.defaultVersion, MatchStrategy: task.VersionMatchCurrentOrOlder,
			}, nil, nil)
			require.Equal(t, len(test.want) == 0, filters.RejectAllOrchestrations)
			require.Equal(t, filters.RejectAllOrchestrations, filters.RejectAllActivities)
			if len(test.want) == 0 {
				require.Empty(t, filters.Orchestrations)
				require.Empty(t, filters.Activities)
			} else {
				require.Equal(t, []WorkItemFilter{{Name: "mixed", Versions: test.want}}, filters.Orchestrations)
				require.Equal(t, filters.Orchestrations, filters.Activities)
			}
			normalized, err := cloneWorkItemFilters(filters)
			require.NoError(t, err)
			wire := workItemFiltersToProto(normalized)
			require.Len(t, wire.Orchestrations, 1)
			require.Len(t, wire.Activities, 1)
			if len(test.want) == 0 {
				require.Equal(t, helpers.RejectAllWorkItemFilterName, wire.Orchestrations[0].Name)
				require.Equal(t, helpers.RejectAllWorkItemFilterName, wire.Activities[0].Name)
			} else {
				require.Len(t, wire.Orchestrations[0].Versions, len(test.want))
				require.Len(t, wire.Activities[0].Versions, len(test.want))
			}
			for _, version := range append(slices.Clone(test.versions), test.worker, test.defaultVersion, "future") {
				want := slices.ContainsFunc(test.want, func(allowed string) bool { return strings.EqualFold(allowed, version) })
				require.Equal(t, want, matchesWorkItemFilters(normalized, true, "mixed", version), version)
				require.Equal(t, want, matchesWorkItemFilters(normalized, false, "mixed", version), version)
			}
		})
	}
}

func TestCurrentOrOlderAutoFiltersRejectWildcardRegistrations(t *testing.T) {
	for _, snapshot := range []task.TaskRegistrySnapshot{
		{Orchestrators: []task.TaskRegistration{{Name: "*"}}},
		{Activities: []task.TaskRegistration{{Name: "*", Version: "1.0"}}},
	} {
		err := validateAutoFilters(snapshot, &task.VersioningOptions{
			Version: "2.0", MatchStrategy: task.VersionMatchCurrentOrOlder,
		})
		require.ErrorContains(t, err, "require named registrations")
		require.NoError(t, validateAutoFilters(snapshot, nil))
	}
}
