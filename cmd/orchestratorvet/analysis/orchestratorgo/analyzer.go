// Package orchestratorgo reports replay hazards in the orchestrator functions a
// package registers with task.TaskRegistry.
//
// The analyzer starts from every proven registration in the package under
// analysis, follows the call graph through same-package named functions,
// methods, resolvable function variables, and the nested function literals
// whose bodies are proven to execute -- through a direct call whose callee
// resolves to the literal, a call through a single-assignment variable, a raw go or
// defer call, or the callback argument of an explicitly modeled invoker such
// as (*task.OrchestrationContext).Go. It then reports only constructs whose
// nondeterminism is provable from that syntax and type information alone.
// Literals passed to unmodeled helpers, or assigned and never invoked, are not
// followed: their bodies never contribute a diagnostic.
//
// It reports:
//
//   - wall-clock reads and host timers (time.Now, time.Sleep, time.After, and
//     friends) that are not routed through the orchestration APIs;
//   - nondeterministic identifier and random sources (google/uuid random
//     constructors, crypto/rand, the math/rand global source, and generators
//     whose seed provably reaches a value that varies between runs);
//   - raw goroutines and unsafe parallelism: go statements, sync and sync/atomic
//     primitives, channel operations, and select, while allowing the durable task
//     primitives that replace them;
//   - direct filesystem, network, process, and environment I/O;
//   - replay-unsafe logging through the log, log/slog, and fmt global writers,
//     while allowing (*task.OrchestrationContext).Logger;
//   - condition-free loops that the whole-package loop and call graph proves can
//     neither exit nor make durable progress;
//   - activity and sub-orchestration names that the package's own complete
//     registration set proves are missing;
//   - registration forms that task.TaskRegistry rejects or that derive unstable
//     names, and registered orchestrators invoked as plain Go calls.
//
// The analyzer stays silent whenever proof is unavailable. Dynamic registration,
// wildcard registration, cross-package registration, functions reached only
// through values it cannot resolve, and code outside orchestrator reachability
// are all left alone.
package orchestratorgo

import (
	"golang.org/x/tools/go/analysis"
)

var includeTestFiles bool

// Analyzer reports replay hazards in registered durable task orchestrators.
var Analyzer = &analysis.Analyzer{
	Name: "orchestratorgo",
	Doc:  "report nondeterministic and replay-unsafe code in durable task orchestrators",
	URL:  "https://github.com/microsoft/durabletask-go/tree/main/cmd/orchestratorvet/analysis/orchestratorgo",
	Run:  run,
}

func init() {
	Analyzer.Flags.BoolVar(
		&includeTestFiles,
		"test-files",
		false,
		"analyze orchestrators declared in _test.go files",
	)
}

func run(pass *analysis.Pass) (any, error) {
	index := newPackageIndex(pass)
	registry := collectRegistrations(pass, index)
	if len(registry.roots) == 0 {
		return nil, nil
	}
	reach := reachableFunctions(index, registry.roots)

	check := &checker{
		pass:     pass,
		index:    index,
		registry: registry,
		reported: make(map[reportKey]bool),
	}
	for _, node := range reach.order {
		check.checkFunction(node)
	}
	check.reportClockDiagnostics()
	return nil, nil
}
