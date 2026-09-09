package orchestratorgo

import (
	"go/ast"
)

// reachSet is the deterministic set of package functions reachable from the
// registered orchestrators.
type reachSet struct {
	order []ast.Node
	set   map[ast.Node]bool
}

// lexicallyCovered reports whether node is a function literal nested inside
// another reachable function. Walking that outer function already visits the
// literal's body, so checking the literal separately would walk it a second
// time. It would also lose context: a fix rewritten from the outer walk can see
// the enclosing orchestration context parameter, which is out of scope when the
// literal is walked on its own.
//
// A literal written outside any function, such as a package-level function
// variable, has no enclosing function and is therefore never covered.
func (reach *reachSet) lexicallyCovered(index *packageIndex, node ast.Node) bool {
	for parent := index.enclosingFunc[node]; parent != nil; parent = index.enclosingFunc[parent] {
		if reach.set[parent] {
			return true
		}
	}
	return false
}

// reachableFunctions walks the whole-package call graph from roots, following
// named helpers, methods, resolvable function variables, and nested literals.
// Recursion terminates because each function node is added at most once.
func reachableFunctions(index *packageIndex, roots []ast.Node) *reachSet {
	reach := &reachSet{set: make(map[ast.Node]bool)}
	for _, root := range roots {
		if root == nil || reach.set[root] {
			continue
		}
		reach.set[root] = true
		reach.order = append(reach.order, root)
	}
	for next := 0; next < len(reach.order); next++ {
		current := reach.order[next]
		forEachCall(current, func(call *ast.CallExpr) {
			callee := index.callee(call)
			if callee == nil || reach.set[callee] {
				return
			}
			reach.set[callee] = true
			reach.order = append(reach.order, callee)
		})
	}
	return reach
}

// forEachCall visits every call expression lexically contained in a function,
// including calls inside nested function literals, go statements, and defers.
func forEachCall(node ast.Node, visit func(*ast.CallExpr)) {
	body := funcBody(node)
	if body == nil {
		return
	}
	ast.Inspect(body, func(node ast.Node) bool {
		if call, ok := node.(*ast.CallExpr); ok {
			visit(call)
		}
		return true
	})
}
