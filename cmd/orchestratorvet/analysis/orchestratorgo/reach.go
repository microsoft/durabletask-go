package orchestratorgo

import (
	"go/ast"

	"golang.org/x/tools/go/analysis"
)

// reachSet is the deterministic set of package functions reachable from the
// registered orchestrators. Every entry is analyzed on its own -- the checker
// walks only that node's own body -- so a nested literal that never executes
// stays out of the set and is not audited.
type reachSet struct {
	order []ast.Node
	set   map[ast.Node]bool
}

// reachableFunctions walks the whole-package call graph from roots, following
// named helpers, methods, resolvable function variables, and the nested literals
// that are provably invoked. Recursion terminates because each function node is
// added at most once.
//
// A nested function literal is added only when its body is proven to execute:
// through a direct call, a call through a single-assignment function variable,
// a raw go or defer call, or the
// callback argument of an explicitly modeled invoker such as
// (*task.OrchestrationContext).Go. Arbitrary function arguments are not assumed
// to run, so a literal passed to a helper that never invokes it stays out of the
// reachable set.
func reachableFunctions(index *packageIndex, roots []ast.Node) *reachSet {
	reach := &reachSet{set: make(map[ast.Node]bool)}
	add := func(node ast.Node) {
		if node == nil || reach.set[node] {
			return
		}
		reach.set[node] = true
		reach.order = append(reach.order, node)
	}
	for _, root := range roots {
		add(root)
	}
	for next := 0; next < len(reach.order); next++ {
		current := reach.order[next]
		forEachOwnedCall(current, func(call *ast.CallExpr) {
			add(index.callee(call))
			add(index.resolveFunction(orchestrationGoCallback(index.pass, call), nil))
		})
	}
	return reach
}

// forEachOwnedCall visits every call expression lexically contained in the
// current function's own body, including calls in raw go and defer statements
// but NOT calls written inside a nested function literal. Those literals are
// separate reachable units: reach adds only the ones actually invoked and walks
// them in their own turn, so descending here would follow calls that never run.
func forEachOwnedCall(node ast.Node, visit func(*ast.CallExpr)) {
	body := funcBody(node)
	if body == nil {
		return
	}
	ast.Inspect(body, func(current ast.Node) bool {
		if _, ok := current.(*ast.FuncLit); ok {
			return false
		}
		if call, ok := current.(*ast.CallExpr); ok {
			visit(call)
		}
		return true
	})
}

// orchestrationGoCallback models Go without assuming arbitrary callbacks execute.
func orchestrationGoCallback(pass *analysis.Pass, call *ast.CallExpr) ast.Expr {
	function := staticFunc(pass, call.Fun)
	if function == nil || function.Name() != "Go" || len(call.Args) == 0 {
		return nil
	}
	ownerPath, ownerName, ok := methodOwner(function)
	if !ok || ownerPath != taskPackagePath || ownerName != "OrchestrationContext" {
		return nil
	}
	// The callback is last for both ctx.Go(fn) and (*OrchestrationContext).Go(ctx, fn).
	return call.Args[len(call.Args)-1]
}
