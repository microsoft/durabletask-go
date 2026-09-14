package task

import (
	"fmt"
	"testing"
)

var taskRegistryBenchmarkSizes = [...]int{4, 64, 1024}

// buildBenchmarkRegistry fills a registry with size orchestrator and activity
// names. Every other name also gets two explicit versions so the benchmark
// covers the versioned lookup and fallback paths, not just exact hits.
func buildBenchmarkRegistry(b *testing.B, size int) *TaskRegistry {
	b.Helper()
	registry := NewTaskRegistry()
	orchestrator := func(*OrchestrationContext) (any, error) { return nil, nil }
	activity := func(ActivityContext) (any, error) { return nil, nil }
	for i := 0; i < size; i++ {
		name := fmt.Sprintf("Orchestrator%d", i)
		if err := registry.AddOrchestratorN(name, orchestrator); err != nil {
			b.Fatalf("add orchestrator: %v", err)
		}
		if err := registry.AddActivityN(fmt.Sprintf("Activity%d", i), activity); err != nil {
			b.Fatalf("add activity: %v", err)
		}
		if i%2 != 0 {
			continue
		}
		versioned := fmt.Sprintf("Versioned%d", i)
		for _, version := range []string{"1.0", "2.0"} {
			if err := registry.AddOrchestratorNVersion(versioned, version, orchestrator); err != nil {
				b.Fatalf("add versioned orchestrator: %v", err)
			}
		}
	}
	return registry
}

// BenchmarkTaskRegistryGetOrchestrator measures the dispatch lookup that runs
// once per orchestration work item, across registry sizes.
func BenchmarkTaskRegistryGetOrchestrator(b *testing.B) {
	for _, size := range taskRegistryBenchmarkSizes {
		registry := buildBenchmarkRegistry(b, size)
		name := fmt.Sprintf("orchestrator%d", size/2)
		b.Run(fmt.Sprintf("hit/size=%d", size), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				if _, ok := registry.getOrchestrator(name, ""); !ok {
					b.Fatalf("orchestrator %q not found", name)
				}
			}
		})
		b.Run(fmt.Sprintf("miss/size=%d", size), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				if _, ok := registry.getOrchestrator("absent", ""); ok {
					b.Fatal("absent orchestrator was found")
				}
			}
		})
	}
}

// BenchmarkTaskRegistryGetOrchestratorVersioned measures the version-matching
// paths: an exact version hit, and the unversioned fallback scan that has to
// prove a name has no versioned registrations.
func BenchmarkTaskRegistryGetOrchestratorVersioned(b *testing.B) {
	for _, size := range taskRegistryBenchmarkSizes {
		registry := buildBenchmarkRegistry(b, size)
		versionedIndex := size / 2
		versionedIndex -= versionedIndex % 2
		versioned := fmt.Sprintf("versioned%d", versionedIndex)
		unversioned := fmt.Sprintf("orchestrator%d", size/2)
		b.Run(fmt.Sprintf("exact/size=%d", size), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				if _, ok := registry.getOrchestrator(versioned, "1.0"); !ok {
					b.Fatalf("versioned orchestrator %q not found", versioned)
				}
			}
		})
		b.Run(fmt.Sprintf("fallback/size=%d", size), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				if _, ok := registry.getOrchestrator(unversioned, "9.9"); !ok {
					b.Fatalf("unversioned fallback for %q not found", unversioned)
				}
			}
		})
	}
}

// BenchmarkTaskRegistryGetActivity measures the activity dispatch lookup.
func BenchmarkTaskRegistryGetActivity(b *testing.B) {
	for _, size := range taskRegistryBenchmarkSizes {
		registry := buildBenchmarkRegistry(b, size)
		name := fmt.Sprintf("activity%d", size/2)
		b.Run(fmt.Sprintf("size=%d", size), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				if _, ok := registry.getActivity(name, ""); !ok {
					b.Fatalf("activity %q not found", name)
				}
			}
		})
	}
}

// BenchmarkTaskRegistrySnapshot measures the sorted snapshot the worker builds
// once per start to derive its capabilities and work-item filters.
func BenchmarkTaskRegistrySnapshot(b *testing.B) {
	for _, size := range taskRegistryBenchmarkSizes {
		registry := buildBenchmarkRegistry(b, size)
		b.Run(fmt.Sprintf("size=%d", size), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				snapshot := registry.Snapshot()
				if len(snapshot.Orchestrators) == 0 {
					b.Fatal("snapshot is empty")
				}
			}
		})
	}
}

// BenchmarkTaskRegistryConcurrentLookup measures the read-locked lookup under
// the concurrency a worker actually applies to it.
func BenchmarkTaskRegistryConcurrentLookup(b *testing.B) {
	registry := buildBenchmarkRegistry(b, 256)
	name := "orchestrator128"
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if _, ok := registry.getOrchestrator(name, ""); !ok {
				b.Errorf("orchestrator %q not found", name)
				return
			}
		}
	})
}
