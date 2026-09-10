package client

import (
	"fmt"
	"testing"

	"github.com/microsoft/durabletask-go/task"
)

var workItemFilterBenchmarkSizes = [...]int{4, 64, 512}

// benchmarkRegistrySnapshot builds a snapshot with size orchestrator and
// activity names plus entities. Every other orchestrator carries two explicit
// versions so filter construction exercises the version-grouping path.
func benchmarkRegistrySnapshot(b *testing.B, size int) task.TaskRegistrySnapshot {
	b.Helper()
	registry := task.NewTaskRegistry()
	orchestrator := func(*task.OrchestrationContext) (any, error) { return nil, nil }
	activity := func(task.ActivityContext) (any, error) { return nil, nil }
	entity := func(ctx *task.EntityContext) (any, error) { return nil, nil }
	for i := 0; i < size; i++ {
		name := fmt.Sprintf("Orchestrator%d", i)
		if err := registry.AddOrchestratorN(name, orchestrator); err != nil {
			b.Fatalf("add orchestrator: %v", err)
		}
		if err := registry.AddActivityN(fmt.Sprintf("Activity%d", i), activity); err != nil {
			b.Fatalf("add activity: %v", err)
		}
		if err := registry.AddEntityN(fmt.Sprintf("Entity%d", i), entity); err != nil {
			b.Fatalf("add entity: %v", err)
		}
		if i%2 != 0 {
			continue
		}
		for _, version := range []string{"1.0", "2.0"} {
			if err := registry.AddOrchestratorNVersion(name, version, orchestrator); err != nil {
				b.Fatalf("add versioned orchestrator: %v", err)
			}
		}
	}
	return registry.Snapshot()
}

// BenchmarkWorkItemFiltersFromRegistry measures the automatic filter derivation
// a worker performs once per start, for both version match strategies.
func BenchmarkWorkItemFiltersFromRegistry(b *testing.B) {
	strict := &task.VersioningOptions{Version: "1.0", MatchStrategy: task.VersionMatchStrict}
	for _, size := range workItemFilterBenchmarkSizes {
		snapshot := benchmarkRegistrySnapshot(b, size)
		b.Run(fmt.Sprintf("unversioned/size=%d", size), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				filters := workItemFiltersFromRegistry(snapshot, nil, nil, nil)
				if len(filters.Orchestrations) == 0 {
					b.Fatal("no orchestration filters were derived")
				}
			}
		})
		b.Run(fmt.Sprintf("strict/size=%d", size), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				filters := workItemFiltersFromRegistry(snapshot, strict, nil, nil)
				if len(filters.Orchestrations) == 0 {
					b.Fatal("no orchestration filters were derived")
				}
			}
		})
	}
}

// BenchmarkWorkItemFiltersToProto measures the wire conversion of the derived
// filters, which is sent on every worker stream generation.
func BenchmarkWorkItemFiltersToProto(b *testing.B) {
	for _, size := range workItemFilterBenchmarkSizes {
		filters := workItemFiltersFromRegistry(benchmarkRegistrySnapshot(b, size), nil, nil, nil)
		b.Run(fmt.Sprintf("size=%d", size), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				wire := workItemFiltersToProto(filters)
				if wire == nil {
					b.Fatal("filters did not convert to the wire form")
				}
			}
		})
	}
}

// BenchmarkValidateWorkItemFilters measures the validation applied to explicit
// filter configuration at worker construction.
func BenchmarkValidateWorkItemFilters(b *testing.B) {
	for _, size := range workItemFilterBenchmarkSizes {
		snapshot := benchmarkRegistrySnapshot(b, size)
		filters := workItemFiltersFromRegistry(snapshot, nil, nil, nil)
		b.Run(fmt.Sprintf("size=%d", size), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				if err := validateWorkItemFilters(filters, snapshot); err != nil {
					b.Fatalf("validate filters: %v", err)
				}
			}
		})
	}
}

// BenchmarkMatchesWorkItemFilters measures the per-work-item filter check that
// runs on the worker's hot path.
func BenchmarkMatchesWorkItemFilters(b *testing.B) {
	for _, size := range workItemFilterBenchmarkSizes {
		filters := workItemFiltersFromRegistry(benchmarkRegistrySnapshot(b, size), nil, nil, nil)
		name := fmt.Sprintf("Orchestrator%d", size/2)
		b.Run(fmt.Sprintf("accept/size=%d", size), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				if !matchesWorkItemFilters(filters, true, name, "") {
					b.Fatalf("filters rejected registered orchestration %q", name)
				}
			}
		})
		b.Run(fmt.Sprintf("reject/size=%d", size), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				if matchesWorkItemFilters(filters, true, "Absent", "") {
					b.Fatal("filters accepted an unregistered orchestration")
				}
			}
		})
	}
}
