package orchestratorgo

import (
	"fmt"
	"go/ast"
	"go/token"
	"go/types"
	"strings"

	"golang.org/x/tools/go/analysis"
)

// Diagnostic categories keep one report per source position per concern, so a
// helper reached from several orchestrators is only reported once.
const (
	categoryGoroutine    = "goroutine"
	categoryWallClock    = "wallclock"
	categoryRandom       = "random"
	categorySync         = "sync"
	categoryChannel      = "channel"
	categoryIO           = "io"
	categoryLogging      = "logging"
	categoryLoop         = "loop"
	categoryUnregistered = "unregistered"
	categoryDirectInvoke = "directinvoke"
)

type reportKey struct {
	pos      token.Pos
	category string
}

type checker struct {
	pass     *analysis.Pass
	index    *packageIndex
	registry *registrySet
	reported map[reportKey]bool

	// Clock fixes are reported after all reachable functions have been checked,
	// so each file's replacements and import cleanup form one atomic fix.
	clockDiagnostics []analysis.Diagnostic
}

func (c *checker) report(
	pos token.Pos,
	category string,
	fixes []analysis.SuggestedFix,
	format string,
	args ...any,
) {
	key := reportKey{pos: pos, category: category}
	if c.reported[key] {
		return
	}
	c.reported[key] = true
	diagnostic := analysis.Diagnostic{
		Pos:            pos,
		Message:        fmt.Sprintf(format, args...),
		SuggestedFixes: fixes,
	}
	if category == categoryWallClock && len(fixes) > 0 {
		c.clockDiagnostics = append(c.clockDiagnostics, diagnostic)
		return
	}
	c.pass.Report(diagnostic)
}

// checkFunction walks one reachable function and reports every replay hazard it
// contains. Nested function literals are skipped here: reach records only the
// literals whose bodies are proven to execute and walks each one separately, so
// descending into a literal here would double-check ones that run and audit
// ones that never do. Enclosing function nodes tracked by the package index
// prime the ancestor stack so a fix generated inside a nested literal can still
// see the surrounding orchestration context binding.
func (c *checker) checkFunction(node ast.Node) {
	body := funcBody(node)
	if body == nil {
		return
	}
	file := c.index.fileOf[node]
	stack := c.index.enclosingFunctionStack(node)
	ast.Inspect(body, func(current ast.Node) bool {
		if current == nil {
			stack = stack[:len(stack)-1]
			return true
		}
		if _, ok := current.(*ast.FuncLit); ok {
			return false
		}
		stack = append(stack, current)
		c.inspect(current, stack, file)
		return true
	})
}

func (c *checker) inspect(node ast.Node, stack []ast.Node, file *ast.File) {
	switch node := node.(type) {
	case *ast.GoStmt:
		c.checkGoStmt(node, stack, file)
	case *ast.SelectStmt:
		c.report(
			node.Select,
			categoryChannel,
			nil,
			"select statement is not deterministic in an orchestrator; "+
				"use (*task.OrchestrationContext).Select with task.OnTask or task.OnEvent",
		)
	case *ast.SendStmt:
		c.report(
			node.Arrow,
			categoryChannel,
			nil,
			"channel send is not deterministic in an orchestrator; "+
				"use task.NewEventChannel or (*task.OrchestrationContext).SendEvent",
		)
	case *ast.UnaryExpr:
		if node.Op == token.ARROW {
			c.report(
				node.OpPos,
				categoryChannel,
				nil,
				"channel receive is not deterministic in an orchestrator; "+
					"use task.NewEventChannel or (*task.OrchestrationContext).WaitForSingleEvent",
			)
		}
	case *ast.RangeStmt:
		if isChannel(c.pass.TypesInfo.TypeOf(node.X)) {
			c.report(
				node.TokPos,
				categoryChannel,
				nil,
				"ranging over a channel is not deterministic in an orchestrator; "+
					"use task.NewEventChannel",
			)
		}
	case *ast.ForStmt:
		c.checkUnboundedLoop(node)
	case *ast.CallExpr:
		c.checkCall(node, stack, file)
	}
}

func (c *checker) checkGoStmt(node *ast.GoStmt, stack []ast.Node, file *ast.File) {
	c.report(
		node.Go,
		categoryGoroutine,
		c.goStatementFix(node, contextParameter(c.pass, stack), file),
		"raw go statement is not deterministic in an orchestrator; "+
			"use (*task.OrchestrationContext).Go",
	)
}

func (c *checker) checkCall(call *ast.CallExpr, stack []ast.Node, file *ast.File) {
	if c.checkBuiltinCall(call) {
		return
	}
	function := staticFunc(c.pass, call.Fun)
	if function == nil {
		return
	}
	c.checkDirectOrchestratorCall(call, function)
	if owner, name, ok := methodOwner(function); ok {
		c.checkMethodCall(call, function, owner, name)
		return
	}
	c.checkPackageCall(call, function, stack, file)
}

func (c *checker) checkBuiltinCall(call *ast.CallExpr) bool {
	identifier, ok := call.Fun.(*ast.Ident)
	if !ok {
		return false
	}
	builtin, ok := c.pass.TypesInfo.ObjectOf(identifier).(*types.Builtin)
	if !ok {
		return false
	}
	switch builtin.Name() {
	case "make":
		if len(call.Args) > 0 && isChannel(c.pass.TypesInfo.TypeOf(call.Args[0])) {
			c.report(
				call.Lparen,
				categoryChannel,
				nil,
				"creating a channel is not deterministic in an orchestrator; "+
					"use task.NewEventChannel or (*task.OrchestrationContext).NewWaitGroup",
			)
		}
		return true
	case "close":
		if len(call.Args) == 1 && isChannel(c.pass.TypesInfo.TypeOf(call.Args[0])) {
			c.report(
				call.Lparen,
				categoryChannel,
				nil,
				"closing a channel is not deterministic in an orchestrator; use task.NewEventChannel",
			)
		}
		return true
	}
	return false
}

// checkPackageCall handles calls to package-level functions.
func (c *checker) checkPackageCall(
	call *ast.CallExpr,
	function *types.Func,
	stack []ast.Node,
	file *ast.File,
) {
	if function.Pkg() == nil {
		return
	}
	path, name := function.Pkg().Path(), function.Name()
	switch path {
	case "time":
		if replacement, ok := wallClockFunctions[name]; ok {
			c.report(
				call.Lparen,
				categoryWallClock,
				c.currentTimeFix(call, name, stack, file),
				"time.%s is not deterministic in an orchestrator; "+
					"use (*task.OrchestrationContext).%s",
				name,
				replacement,
			)
		}
	case "crypto/rand":
		c.report(
			call.Lparen,
			categoryRandom,
			nil,
			"crypto/rand.%s is not deterministic in an orchestrator; "+
				"use (*task.OrchestrationContext).NewGuid or generate the value in an activity",
			name,
		)
	case "math/rand", "math/rand/v2":
		if randomConstructors[name] {
			return
		}
		c.report(
			call.Lparen,
			categoryRandom,
			nil,
			"%s.%s uses the global random source and is not deterministic in an orchestrator; "+
				"use (*task.OrchestrationContext).NewGuid or generate the value in an activity",
			path,
			name,
		)
	case "github.com/google/uuid":
		if !nondeterministicUUIDFunctions[name] {
			return
		}
		c.report(
			call.Lparen,
			categoryRandom,
			nil,
			"uuid.%s is not deterministic in an orchestrator; "+
				"use (*task.OrchestrationContext).NewGuid",
			name,
		)
	case "sync":
		if !syncOnceFunctions[name] {
			return
		}
		c.report(
			call.Lparen,
			categorySync,
			nil,
			"sync.%s is not replay safe in an orchestrator; orchestrator coroutines are "+
				"scheduled deterministically by the task runtime",
			name,
		)
	case "sync/atomic":
		c.report(
			call.Lparen,
			categorySync,
			nil,
			"sync/atomic.%s is not replay safe in an orchestrator; orchestrator coroutines are "+
				"scheduled deterministically by the task runtime",
			name,
		)
	case "os":
		if externalOSFunctions[name] {
			c.reportIO(call, "os."+name)
		}
	case "os/exec":
		if name == "Command" || name == "CommandContext" || name == "LookPath" {
			c.reportIO(call, "exec."+name)
		}
	case "net":
		if hasAnyPrefix(name, "Dial", "Listen", "Lookup", "Resolve") {
			c.reportIO(call, "net."+name)
		}
	case "net/http":
		if externalHTTPFunctions[name] {
			c.reportIO(call, "http."+name)
		}
	case "syscall":
		c.reportIO(call, "syscall."+name)
	case "log":
		if replayUnsafeLogFunctions[name] {
			c.reportLogging(call, "log."+name)
		}
	case "log/slog":
		if processLoggerFunctions[name] {
			c.report(
				call.Lparen,
				categoryLogging,
				nil,
				"slog.%s reaches the process-wide logger, which writes on every replay; "+
					"use (*task.OrchestrationContext).Logger, which suppresses output while replaying",
				name,
			)
			return
		}
		if replayUnsafeSlogFunctions[name] {
			c.reportLogging(call, "slog."+name)
		}
	case "fmt":
		c.checkFmtCall(call, name)
	}
}

// checkMethodCall handles method calls, keyed on the package and named type that
// declares the method rather than the syntactic receiver, so embedded fields and
// interface values are classified correctly.
func (c *checker) checkMethodCall(
	call *ast.CallExpr,
	function *types.Func,
	ownerPath string,
	ownerName string,
) {
	name := function.Name()
	switch ownerPath {
	case taskPackagePath:
		if ownerName == "OrchestrationContext" {
			c.checkOrchestrationContextCall(call, name)
		}
		return
	case "sync":
		c.report(
			call.Lparen,
			categorySync,
			nil,
			"(sync.%s).%s is not replay safe in an orchestrator; "+
				"use (*task.OrchestrationContext).NewWaitGroup and (*task.OrchestrationContext).Go",
			ownerName,
			name,
		)
	case "sync/atomic":
		c.report(
			call.Lparen,
			categorySync,
			nil,
			"(atomic.%s).%s is not replay safe in an orchestrator; orchestrator coroutines are "+
				"scheduled deterministically by the task runtime",
			ownerName,
			name,
		)
	case "math/rand", "math/rand/v2":
		if ownerName == "Rand" && c.usesNondeterministicSource(call) {
			c.report(
				call.Lparen,
				categoryRandom,
				nil,
				"(*rand.Rand).%s is seeded from a nondeterministic source; "+
					"use (*task.OrchestrationContext).NewGuid or seed from orchestration input",
				name,
			)
		}
	case "os":
		if ownerName == "Process" {
			c.reportIO(call, "(*os.Process)."+name)
		}
	case "os/exec":
		if ownerName == "Cmd" && externalCmdMethods[name] {
			c.reportIO(call, "(*exec.Cmd)."+name)
		}
	case "net/http":
		if ownerName == "Client" && externalHTTPClientMethods[name] {
			c.reportIO(call, "(*http.Client)."+name)
		}
	case "log":
		if ownerName == "Logger" && replayUnsafeLogFunctions[name] {
			c.reportLogging(call, "(*log.Logger)."+name)
		}
	}
}

// checkOrchestrationContextCall validates durable task names referenced from an
// orchestrator against the registrations proven for this package.
func (c *checker) checkOrchestrationContextCall(call *ast.CallExpr, method string) {
	var kind registrationKind
	switch method {
	case "CallActivity":
		kind = activityKind
	case "CallSubOrchestrator":
		kind = orchestratorKind
	default:
		return
	}
	if len(call.Args) == 0 || !c.registry.provesAbsence(kind) {
		return
	}
	name, ok := c.index.taskName(call.Args[0])
	if !ok || c.registry.registered(kind, name) {
		return
	}
	label := "activity"
	if kind == orchestratorKind {
		label = "sub-orchestration"
	}
	c.report(
		call.Args[0].Pos(),
		categoryUnregistered,
		nil,
		"%s %q is not registered with task.TaskRegistry in this package",
		label,
		name,
	)
}

func (c *checker) checkFmtCall(call *ast.CallExpr, name string) {
	switch name {
	case "Print", "Printf", "Println":
		c.reportLogging(call, "fmt."+name)
	case "Fprint", "Fprintf", "Fprintln":
		if len(call.Args) > 0 && isStandardStream(c.pass, call.Args[0]) {
			c.reportLogging(call, "fmt."+name)
		}
	}
}

func (c *checker) reportIO(call *ast.CallExpr, label string) {
	c.report(
		call.Lparen,
		categoryIO,
		nil,
		"%s performs external I/O that is not replay safe in an orchestrator; "+
			"move it into an activity",
		label,
	)
}

func (c *checker) reportLogging(call *ast.CallExpr, label string) {
	c.report(
		call.Lparen,
		categoryLogging,
		nil,
		"%s writes on every replay; use (*task.OrchestrationContext).Logger, "+
			"which suppresses output while replaying",
		label,
	)
}

// checkDirectOrchestratorCall reports a registered orchestrator invoked as a
// plain Go function instead of through CallSubOrchestrator.
func (c *checker) checkDirectOrchestratorCall(call *ast.CallExpr, function *types.Func) {
	name, ok := c.registry.orchestratorObjects[function]
	if !ok {
		return
	}
	if name == "" {
		name = function.Name()
	}
	c.report(
		call.Lparen,
		categoryDirectInvoke,
		nil,
		"orchestrator %q is invoked directly; "+
			"use (*task.OrchestrationContext).CallSubOrchestrator so the call is durable",
		name,
	)
}

// usesNondeterministicSource reports whether the generator a *rand.Rand method
// is called on was provably built from a seed that differs between replays of
// the same orchestration.
//
// The proof is positive: a generator is only reported when its seed reaches a
// source the analyzer knows varies between runs, such as the host clock, the
// process environment, or another random source. A constant seed, a seed taken
// from orchestration input, and a seed the analyzer cannot follow are all left
// unreported, because all three replay identically or cannot be judged at all.
func (c *checker) usesNondeterministicSource(call *ast.CallExpr) bool {
	selector, ok := call.Fun.(*ast.SelectorExpr)
	if !ok {
		return false
	}
	identifier, ok := selector.X.(*ast.Ident)
	if !ok {
		// A generator reached through a field or another call cannot be proven
		// deterministic or nondeterministic, so it is left alone.
		return false
	}
	constructor, ok := c.generatorConstructor(identifier)
	if !ok {
		return false
	}
	seen := make(map[types.Object]struct{})
	for _, seed := range constructor.Args {
		if c.isNondeterministicValue(seed, seen) {
			return true
		}
	}
	return false
}

// generatorConstructor returns the rand.New call an identifier holds, taken from
// the single-assignment values the package index already recorded. An identifier
// written more than once has no proven value and yields nothing.
func (c *checker) generatorConstructor(identifier *ast.Ident) (*ast.CallExpr, bool) {
	value := c.index.singleValue(c.pass.TypesInfo.ObjectOf(identifier))
	call, ok := value.(*ast.CallExpr)
	if !ok {
		return nil, false
	}
	function := staticFunc(c.pass, call.Fun)
	if function == nil || function.Pkg() == nil || function.Name() != "New" {
		return nil, false
	}
	if path := function.Pkg().Path(); path != "math/rand" && path != "math/rand/v2" {
		return nil, false
	}
	return call, true
}

// isNondeterministicValue reports whether an expression provably reaches a value
// that differs between runs. Identifiers are followed to their single assigned
// value, so a seed hoisted into a local is judged exactly like an inline one.
func (c *checker) isNondeterministicValue(expression ast.Expr, seen map[types.Object]struct{}) bool {
	found := false
	ast.Inspect(expression, func(node ast.Node) bool {
		if found {
			return false
		}
		switch node := node.(type) {
		case *ast.Ident:
			object := c.pass.TypesInfo.ObjectOf(node)
			if object == nil {
				return true
			}
			if _, ok := seen[object]; ok {
				return true
			}
			value := c.index.singleValue(object)
			if value == nil {
				return true
			}
			seen[object] = struct{}{}
			if c.isNondeterministicValue(value, seen) {
				found = true
				return false
			}
		case *ast.CallExpr:
			if c.isNondeterministicCall(node) {
				found = true
				return false
			}
		}
		return true
	})
	return found
}

// isNondeterministicCall reports whether a call yields a value that is not
// reproducible on replay.
func (c *checker) isNondeterministicCall(call *ast.CallExpr) bool {
	function := staticFunc(c.pass, call.Fun)
	if function == nil || function.Pkg() == nil {
		return false
	}
	name := function.Name()
	switch function.Pkg().Path() {
	case "time":
		_, ok := wallClockFunctions[name]
		return ok
	case "crypto/rand":
		return true
	case "math/rand", "math/rand/v2":
		// A constructor only wraps the seed it is handed, so it is neutral;
		// the seed itself is judged by the surrounding walk.
		return !randomConstructors[name]
	case "github.com/google/uuid":
		return nondeterministicUUIDFunctions[name]
	case "os":
		return externalOSFunctions[name]
	}
	return false
}

// contextParameter returns the object of the innermost *task.OrchestrationContext
// parameter in scope, which suggested fixes need to rewrite calls.
func contextParameter(pass *analysis.Pass, stack []ast.Node) types.Object {
	for i := len(stack) - 1; i >= 0; i-- {
		signature := funcType(stack[i])
		if signature == nil {
			continue
		}
		if parameter := orchestrationContextParam(pass, signature); parameter != nil {
			return parameter
		}
	}
	return nil
}

func orchestrationContextParam(pass *analysis.Pass, signature *ast.FuncType) types.Object {
	if signature.Params == nil {
		return nil
	}
	for _, field := range signature.Params.List {
		if !isOrchestrationContextType(pass.TypesInfo.TypeOf(field.Type)) {
			continue
		}
		for _, name := range field.Names {
			if name.Name != "_" && name.Name != "" {
				return pass.TypesInfo.Defs[name]
			}
		}
	}
	return nil
}

func isOrchestrationContextType(value types.Type) bool {
	pointer, ok := types.Unalias(value).(*types.Pointer)
	if !ok {
		return false
	}
	named, ok := types.Unalias(pointer.Elem()).(*types.Named)
	if !ok {
		return false
	}
	object := named.Obj()
	return object.Pkg() != nil &&
		object.Pkg().Path() == taskPackagePath &&
		object.Name() == "OrchestrationContext"
}

// methodOwner returns the package path and named type that declares a method.
func methodOwner(function *types.Func) (string, string, bool) {
	signature, ok := function.Type().(*types.Signature)
	if !ok || signature.Recv() == nil {
		return "", "", false
	}
	receiver := types.Unalias(signature.Recv().Type())
	if pointer, ok := receiver.(*types.Pointer); ok {
		receiver = types.Unalias(pointer.Elem())
	}
	named, ok := receiver.(*types.Named)
	if !ok || named.Obj().Pkg() == nil {
		return "", "", false
	}
	return named.Obj().Pkg().Path(), named.Obj().Name(), true
}

func isChannel(value types.Type) bool {
	if value == nil {
		return false
	}
	_, ok := value.Underlying().(*types.Chan)
	return ok
}

func isStandardStream(pass *analysis.Pass, expression ast.Expr) bool {
	selector, ok := expression.(*ast.SelectorExpr)
	if !ok {
		return false
	}
	object := pass.TypesInfo.ObjectOf(selector.Sel)
	variable, ok := object.(*types.Var)
	if !ok || variable.Pkg() == nil || variable.Pkg().Path() != "os" {
		return false
	}
	return variable.Name() == "Stdout" || variable.Name() == "Stderr"
}

func hasAnyPrefix(value string, prefixes ...string) bool {
	for _, prefix := range prefixes {
		if strings.HasPrefix(value, prefix) {
			return true
		}
	}
	return false
}
