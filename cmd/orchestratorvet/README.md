# orchestratorvet

`orchestratorvet` is a [`go vet`](https://pkg.go.dev/cmd/vet)-compatible driver
for the `orchestratorgo` analyzer. The analyzer reports replay hazards in the
orchestrator functions a package registers with `task.TaskRegistry`.

## Running it

```bash
cd cmd/orchestratorvet && go build -o ../../bin/orchestratorvet .
cd ../..
go vet -vettool=$PWD/bin/orchestratorvet ./...
```

Files ending in `_test.go` are excluded by default because test suites often
register intentionally invalid or nondeterministic orchestrators. Include them
when desired:

```bash
go vet -vettool=$PWD/bin/orchestratorvet -orchestratorgo.test-files ./...
```

The binary also runs standalone under any
[`unitchecker`](https://pkg.go.dev/golang.org/x/tools/go/analysis/unitchecker)
driver, and the analyzer itself is available as
`github.com/microsoft/durabletask-go/cmd/orchestratorvet/analysis/orchestratorgo.Analyzer`
for embedding in a multichecker.

## What it analyzes

Orchestrator code is replayed from history on every turn, so it must produce the
same actions in the same order every time it runs. The analyzer therefore starts
from registrations rather than from signatures: a function is only analyzed when
the package under analysis passes it to one of

- `(*task.TaskRegistry).AddOrchestrator`
- `(*task.TaskRegistry).AddOrchestratorN`
- `(*task.TaskRegistry).AddOrchestratorVersion`
- `(*task.TaskRegistry).AddOrchestratorNVersion`

From each of those roots it follows the whole-package call graph through
same-package named functions, methods, function variables whose target can be
proven, and nested function literals. Recursion terminates because each function
is visited once, and a helper reached from several orchestrators is reported
once. Activity bodies, entity bodies, and any function not reachable from a
registered orchestrator are never reported.

## Checks

### Wall clock

`time.Now`, `time.Since`, and `time.Until` read the host clock, which differs on
every replay. `time.Sleep`, `time.After`, `time.Tick`, `time.NewTimer`,
`time.NewTicker`, and `time.AfterFunc` block or fire on host time instead of
durable time.

Use `ctx.CurrentTimeUtc` for the current orchestration time and
`ctx.CreateTimer` for a durable delay.

### Nondeterministic identifiers and randomness

Reported: the `github.com/google/uuid` random and time-based constructors
(`New`, `NewString`, `NewRandom`, `NewRandomFromReader`, `NewUUID`), every
`crypto/rand` function, the `math/rand` and `math/rand/v2` package-level global
source, and methods on a `*rand.Rand` value whose seed the analyzer proves
reaches a source that varies between runs, such as the host clock, the process
environment, or another random source.

Not reported: `uuid.NewSHA1`, `uuid.NewMD5`, `uuid.Parse`, a generator seeded
from a constant (including a constant held in a variable), and a generator
seeded from orchestration input, which is restored from history and therefore
replays identically. A seed the analyzer cannot follow to either kind of source
is left alone rather than guessed at.

Use `ctx.NewGuid` for a deterministic identifier, or generate the value in an
activity and pass it back.

### Unsafe parallelism and synchronization

Reported: `go` statements, `sync` and `sync/atomic` primitives, channel
creation, send, receive, range, and close, and `select` statements. The check
keys on the package that declares a method, so an embedded `sync.Mutex` is
recognized through the embedding type.

Not reported: `ctx.Go`, `ctx.NewWaitGroup`, `ctx.Select` with `task.OnTask` or
`task.OnEvent`, `task.NewEventChannel`, and `ctx.WaitForSingleEvent`. These are
scheduled by the deterministic orchestration scheduler.

### External I/O

Reported: `os` filesystem, environment, and process functions, `net/http`
package functions and `*http.Client` request methods, `net.Dial*`, `net.Listen*`,
`net.Lookup*`, and `net.Resolve*`, `os/exec` and `*exec.Cmd` execution methods,
and every `syscall` function.

Move the side effect into an activity, whose result is recorded in history.
Activities are delivered at least once and must be idempotent because a worker
can finish the side effect but lose its completion response before DTS records it.

### Replay-unsafe logging

Reported: the `log` package functions and `*log.Logger` methods, the
package-level `log/slog` functions, `slog.Default` and `slog.SetDefault`, which
reach the process-wide logger, `fmt.Print`, `fmt.Printf`, `fmt.Println`, and
`fmt.Fprint*` targeting `os.Stdout` or `os.Stderr`.

Not reported: methods on a `*slog.Logger`, because `ctx.Logger()` returns a
`*slog.Logger` that suppresses output while replaying. `fmt.Sprintf` and the
other pure formatting helpers are also left alone.

### Unbounded orchestration loops

A loop with no condition, or with a constant `true` condition, is reported only
when the whole-package loop and call graph proves that the body can neither
leave the loop nor make durable progress. Any `return`, `break`, `goto`,
labelled statement, closure, `go` statement, `defer`, or `panic` in the loop
body ends the proof, as does any call the analyzer cannot follow, except calls
to packages it explicitly models as pure. Any interaction with the durable task
package counts as possible progress and suppresses the report.

Await a task, wait for an event, or call `ctx.ContinueAsNew` to make the loop
durable.

### Unresolved activity and sub-orchestration names

`ctx.CallActivity` and `ctx.CallSubOrchestrator` accept either a name or a
function, and both resolve to a registered name at runtime. When the package's
own registration set for that namespace is complete, a referenced name that is
absent from it is reported.

A registration set is only treated as complete when the package registers at
least one task in that namespace, every registered name is proven statically,
no `"*"` wildcard registration exists, and no typed registry escapes to opaque
code or comes from an unknown origin, including caller-owned parameters and
tuple-returning factories. Passing a registry to a helper, returning
or storing it outside a local variable, erasing its type behind an interface, or
taking a registration method value
ends the completeness proof for both namespaces: a helper's name does not limit
what it can register. Registration validation and orchestrator discovery still
run even when absence cannot be proven.

### Registration forms

`task.TaskRegistry` rejects a registration outright when the handler is nil,
when the name is the empty string, or when the version is a non-empty string of
only whitespace. Each of those is reported, because the call always returns an
error.

A whitespace-only name is not rejected: the registry lowercases it and stores it
verbatim, so the task really is registered under a name every caller has to
reproduce exactly. That is reported separately, as a name to replace rather than
a call that fails.

Also reported: a duplicate name and version pair on the same registry value, a
name derived by reflection from a function literal or a method value (which
produces a compiler-generated name such as `func1` or `Run-fm`), and a
registered orchestrator invoked as a plain Go call instead of through
`ctx.CallSubOrchestrator`.

Duplicate detection compares direct expression or assignment statements in the
same block or switch/select clause, on a variable initialized once by
`task.NewTaskRegistry()`. Reassignment (including tuple and range writes),
aliasing (including registry pointers stored as map keys), escapes, unknown
initializers, conditional expressions, and separate functions or closures
prevent a proof of duplication. Functions containing `goto` are excluded rather
than assuming that registrations in one block execute on compatible paths. Its key mirrors the
registry's own normalization, which lowercases the name and the version and
trims neither, so `"1.0"` and `" 1.0"` are distinct registrations just as they
are at runtime. A registration whose version is computed at runtime is left out
of the comparison entirely, because it could land on any key.

## Suggested fixes

Two diagnostics carry an `analysis.SuggestedFix` that `gopls` and `go vet -fix`
can apply:

- `time.Now()` becomes `ctx.CurrentTimeUtc`, when a non-blank
  `*task.OrchestrationContext` parameter is in scope and neither shadowed nor
  reassigned. The visible binding must be the original parameter object, not
  merely another value of the same type. One atomic
  fix, offered on the first fixable clock diagnostic in each file, rewrites all
  fixable reads in that file using each call's own context binding. It removes
  the `time` import only if no other uses remain, while retaining all diagnostics.
  Standalone call statements and direct `go`/`defer` calls are not rewritten,
  since a field read is not a valid replacement there.
- `go func() { ... }()` becomes `ctx.Go(func(*task.OrchestrationContext) { ... })`,
  when the goroutine is an immediately invoked literal with no parameters, no
  results, and no arguments, and the file imports the durable task package under
  a usable, unshadowed name and the original, unmodified context parameter is
  visible. Only the text around the braces is rewritten, so the body and
  every comment in it are preserved exactly.

Every other diagnostic is left without a fix because no single rewrite is always
correct.

Before file-level batching, the pinned x/tools v0.49 CLI already handled the
multiple-`time.Now()` unused-import case: `go vet -fix` merges edits and removes
unused imports internally. That case is not a reproduced CLI failure. The atomic
clock fix deliberately offers a stronger raw-edit guarantee for consumers applying
our `TextEdits` without cleanup. x/tools does not promise that arbitrary fixes
from different diagnostics compose into compiling code.

## False-positive guardrails

The analyzer reports only what it can prove, and stays silent otherwise.

- Only registered orchestrators and the code they reach are analyzed.
- A function variable assigned more than once is treated as unresolved, and its
  bodies are not followed.
- Calls into `github.com/microsoft/durabletask-go/task` are never followed,
  because the channels, goroutines, and locks there implement the deterministic
  orchestration scheduler. Without this the durable task package would report
  its own primitives when analyzing itself.
- Dynamic, wildcard, and cross-package registration all suppress the unresolved
  name check.
- Unbounded loop reporting requires a complete proof; any unmodelled call, exit
  path, or durable interaction suppresses it.
- A random generator is only reported when its seed provably reaches a source
  that varies between runs. A constant seed, a seed taken from orchestration
  input, and a seed the analyzer cannot follow are all left alone.
- Every diagnostic is deduplicated by source position and category, so a shared
  helper is reported once no matter how many orchestrators reach it.

## Limitations

- The analyzer runs one package at a time. Helpers, activity registrations, and
  orchestrator bodies in other packages are invisible to it, so hazards there
  are not reported.
- Calls through interface values, struct fields, and function parameters cannot
  be resolved to a body and are not followed.
- Version-aware name resolution is approximated: a task name is considered
  registered when any version of it is registered.
- The checks are syntactic and type-based. A hazard reached only through
  reflection, `unsafe`, code generation, or an indirection the analyzer cannot
  resolve is not reported.
- Nondeterminism that is legal Go and legal durable code, such as iterating a
  map without sorting, is out of scope.

## Testing and benchmarking

The analyzer's `analysistest` fixtures live in
`cmd/orchestratorvet/analysis/orchestratorgo/testdata/src`, split one package per scenario,
with a dedicated package of negative cases. The scenario list is discovered from
that directory, so a new fixture package is exercised as soon as it is added.

The fix fixture packages carry `.golden` files for suggested fixes. The suite
type-checks both the goldens and the actual `TextEdits`, applied individually and
combined. Raw-edit checks do not remove unused imports or merge overlapping
edits: they enforce this analyzer's self-contained edit policy, not a general
`analysis.SuggestedFix` requirement. Golden tests exercise the pinned framework's
merge-and-import-cleanup behavior separately.

```bash
cd cmd/orchestratorvet && go test ./...
cd cmd/orchestratorvet && go test -run '^$' -bench . ./analysis/orchestratorgo/...
```

The benchmarks measure orchestrator-count scaling, call-graph depth, and the
early exit taken by the overwhelming majority of packages, which register no
orchestrators at all. They type-check against the same durable task stub the
fixtures use, so the two cannot drift apart.
