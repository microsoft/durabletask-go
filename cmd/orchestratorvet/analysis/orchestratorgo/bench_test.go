package orchestratorgo_test

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/microsoft/durabletask-go/cmd/orchestratorvet/analysis/orchestratorgo"
	"golang.org/x/tools/go/analysis"
	"golang.org/x/tools/go/analysis/analysistest"
	"golang.org/x/tools/go/packages"
)

// taskStubPath is the durable task stand-in the analysistest fixtures already
// type-check against. The benchmarks reuse that one file so the surface they
// measure cannot drift away from the surface the fixtures assert on.
var taskStubPath = filepath.Join(
	"testdata", "src", "github.com", "microsoft", "durabletask-go", "task", "task.go",
)

func taskStubSource(b *testing.B) string {
	b.Helper()
	source, err := os.ReadFile(taskStubPath)
	if err != nil {
		b.Fatalf("read task stub: %v", err)
	}
	return string(source)
}

// generateBenchmarkPackage builds a synthetic package with the requested number
// of orchestrators and helper-chain depth. Every orchestrator body mixes
// reported and clean constructs so the benchmark measures the full check
// pipeline rather than an early exit. When register is false the functions are
// declared but never registered, which is the shape of almost every package the
// analyzer sees.
func generateBenchmarkPackage(orchestrators, depth int, register bool) string {
	var builder strings.Builder
	builder.WriteString("package bench\n\nimport (\n\t\"os\"\n\t\"time\"\n\n")
	builder.WriteString("\t\"github.com/microsoft/durabletask-go/task\"\n)\n\n")

	for i := 0; i < orchestrators; i++ {
		fmt.Fprintf(&builder, `
func orchestrator%d(ctx *task.OrchestrationContext) (any, error) {
	total := 0
	for j := 0; j < 4; j++ {
		total += j
	}
	if err := ctx.CallActivity("work").Await(nil); err != nil {
		return nil, err
	}
	go func() {}()
	_ = ctx.CurrentTimeUtc
	_ = ctx.CreateTimer(time.Second)
	return helper%d_0(total), nil
}
`, i, i)
		for level := 0; level < depth; level++ {
			next := fmt.Sprintf("helper%d_%d(value + 1)", i, level+1)
			if level == depth-1 {
				next = `len(os.Getenv("REGION")) + value`
			}
			fmt.Fprintf(&builder, `
func helper%d_%d(value int) int {
	if value%%2 == 0 {
		value = value * 3
	}
	return %s
}
`, i, level, next)
		}
	}

	builder.WriteString("\nfunc register() {\n\tregistry := task.NewTaskRegistry()\n")
	builder.WriteString("\t_ = registry\n")
	if register {
		builder.WriteString("\t_ = registry.AddActivityN(\"work\", func(task.ActivityContext) (any, error) { return nil, nil })\n")
	}
	for i := 0; i < orchestrators; i++ {
		if register {
			fmt.Fprintf(&builder, "\t_ = registry.AddOrchestrator(orchestrator%d)\n", i)
		} else {
			fmt.Fprintf(&builder, "\t_ = orchestrator%d\n", i)
		}
	}
	builder.WriteString("}\n")
	return builder.String()
}

// loadBenchmarkPass type-checks a generated package once and returns a pass the
// benchmark loop can run the analyzer against repeatedly.
func loadBenchmarkPass(b *testing.B, source string) *analysis.Pass {
	b.Helper()
	dir, cleanup, err := analysistest.WriteFiles(map[string]string{
		"github.com/microsoft/durabletask-go/task/task.go": taskStubSource(b),
		"bench/bench.go": source,
	})
	if err != nil {
		b.Fatalf("write benchmark files: %v", err)
	}
	b.Cleanup(cleanup)

	config := &packages.Config{
		Mode: fixtureLoadMode | packages.NeedTypesSizes,
		Dir:  dir,
		Env:  fixtureEnvironment(dir),
	}
	loaded, err := packages.Load(config, "bench")
	if err != nil {
		b.Fatalf("load benchmark package: %v", err)
	}
	if len(loaded) != 1 || len(loaded[0].Errors) > 0 {
		b.Fatalf("load benchmark package: %d packages, errors %v", len(loaded), loaded[0].Errors)
	}
	pkg := loaded[0]
	return &analysis.Pass{
		Analyzer:   orchestratorgo.Analyzer,
		Fset:       pkg.Fset,
		Files:      pkg.Syntax,
		Pkg:        pkg.Types,
		TypesInfo:  pkg.TypesInfo,
		TypesSizes: pkg.TypesSizes,
		Report:     func(analysis.Diagnostic) {},
		ResultOf:   map[*analysis.Analyzer]any{},
	}
}

func runAnalyzerBenchmark(b *testing.B, source string) {
	pass := loadBenchmarkPass(b, source)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := orchestratorgo.Analyzer.Run(pass); err != nil {
			b.Fatalf("run analyzer: %v", err)
		}
	}
}

// BenchmarkAnalyzerOrchestratorScaling measures how analysis cost grows with the
// number of registered orchestrators at a fixed call-graph depth.
func BenchmarkAnalyzerOrchestratorScaling(b *testing.B) {
	for _, orchestrators := range []int{1, 8, 32, 128} {
		b.Run(fmt.Sprintf("orchestrators=%d", orchestrators), func(b *testing.B) {
			runAnalyzerBenchmark(b, generateBenchmarkPackage(orchestrators, 4, true))
		})
	}
}

// BenchmarkAnalyzerCallGraphDepth measures how analysis cost grows with the
// length of the same-package helper chain each orchestrator reaches through.
func BenchmarkAnalyzerCallGraphDepth(b *testing.B) {
	for _, depth := range []int{1, 4, 16, 64} {
		b.Run(fmt.Sprintf("depth=%d", depth), func(b *testing.B) {
			runAnalyzerBenchmark(b, generateBenchmarkPackage(4, depth, true))
		})
	}
}

// BenchmarkAnalyzerNoOrchestrators measures the cost of the early exit taken by
// the overwhelming majority of packages, which register nothing at all.
func BenchmarkAnalyzerNoOrchestrators(b *testing.B) {
	runAnalyzerBenchmark(b, generateBenchmarkPackage(8, 4, false))
}
