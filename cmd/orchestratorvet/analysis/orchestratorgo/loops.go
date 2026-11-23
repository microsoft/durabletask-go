package orchestratorgo

import (
	"go/ast"
	"go/constant"
	"go/token"
	"go/types"

	"golang.org/x/tools/go/analysis"
)

// checkUnboundedLoop reports a condition-free orchestrator loop only when the
// whole-package loop and call graph proves the body can neither leave the loop
// nor make durable progress. Any call the analyzer cannot follow, and any use of
// the task package other than the pure task.IsCompleted check, leaves the loop
// unreported. A loop whose only condition is !task.IsCompleted(t) is held to
// the same proof, because its condition cannot change while nothing yields.
func (c *checker) checkUnboundedLoop(loop *ast.ForStmt, stack []ast.Node) {
	var polled *types.Var
	if loop.Cond != nil && !isConstantTrue(c.pass, loop.Cond) {
		polled = c.polledTask(loop, stack)
		if polled == nil {
			return
		}
	}
	probe := &loopProbe{
		checker:        c,
		visitedCallees: make(map[ast.Node]bool),
	}
	if loop.Body != nil {
		probe.scan(loop.Body, true)
	}
	if loop.Post != nil {
		probe.scan(&ast.BlockStmt{List: []ast.Stmt{loop.Post}}, true)
	}
	if probe.inconclusive {
		return
	}
	if polled != nil {
		c.report(
			loop.For,
			categoryLoop,
			nil,
			"loop polls task.IsCompleted without awaiting a durable task or waiting for an event, "+
				"so the task can never complete; use Await, WhenAny, or Select",
		)
		return
	}
	c.reportUnboundedLoop(loop)
}

// polledTask returns the variable t when the loop condition is exactly
// !task.IsCompleted(t) and t is a local that the loop cannot change: it is not
// assigned in the loop, its address is never taken, and no closure captures it.
func (c *checker) polledTask(loop *ast.ForStmt, stack []ast.Node) *types.Var {
	negated, ok := ast.Unparen(loop.Cond).(*ast.UnaryExpr)
	if !ok || negated.Op != token.NOT {
		return nil
	}
	call, ok := ast.Unparen(negated.X).(*ast.CallExpr)
	if !ok || len(call.Args) != 1 || !isTaskIsCompleted(staticFunc(c.pass, call.Fun)) {
		return nil
	}
	identifier, ok := ast.Unparen(call.Args[0]).(*ast.Ident)
	if !ok {
		return nil
	}
	variable, ok := c.pass.TypesInfo.Uses[identifier].(*types.Var)
	if !ok || variable.IsField() {
		return nil
	}
	var function ast.Node
	for i := len(stack) - 1; i >= 0 && function == nil; i-- {
		switch stack[i].(type) {
		case *ast.FuncDecl, *ast.FuncLit:
			function = stack[i]
		}
	}
	if function == nil || variable.Pos() < function.Pos() || variable.Pos() >= function.End() {
		return nil
	}
	if c.mayChange(function, loop, variable) {
		return nil
	}
	return variable
}

// mayChange reports whether variable could be modified while loop runs.
func (c *checker) mayChange(function ast.Node, loop *ast.ForStmt, variable *types.Var) bool {
	uses := func(expression ast.Expr) bool {
		identifier, ok := ast.Unparen(expression).(*ast.Ident)
		return ok && c.pass.TypesInfo.Uses[identifier] == variable
	}
	changed := false
	var walk func(node ast.Node, inLoop, inClosure bool)
	walk = func(node ast.Node, inLoop, inClosure bool) {
		ast.Inspect(node, func(current ast.Node) bool {
			if changed {
				return false
			}
			switch current := current.(type) {
			case *ast.FuncLit:
				if current != function {
					walk(current.Body, inLoop, true)
					return false
				}
			case *ast.ForStmt:
				if current == loop && !inLoop {
					walk(current, true, inClosure)
					return false
				}
			case *ast.Ident:
				if inClosure && c.pass.TypesInfo.Uses[current] == variable {
					changed = true
				}
			case *ast.UnaryExpr:
				if current.Op == token.AND && uses(current.X) {
					changed = true
				}
			case *ast.AssignStmt:
				if inLoop {
					for _, target := range current.Lhs {
						if uses(target) {
							changed = true
						}
					}
				}
			case *ast.RangeStmt:
				if inLoop && current.Tok == token.ASSIGN && (uses(current.Key) || uses(current.Value)) {
					changed = true
				}
			}
			return true
		})
	}
	walk(function, false, false)
	return changed
}

func isTaskIsCompleted(function *types.Func) bool {
	if function == nil || function.Pkg() == nil || function.Pkg().Path() != taskPackagePath {
		return false
	}
	signature, ok := function.Type().(*types.Signature)
	return ok && signature.Recv() == nil && function.Name() == "IsCompleted"
}

func (c *checker) reportUnboundedLoop(loop *ast.ForStmt) {
	c.report(
		loop.For,
		categoryLoop,
		nil,
		"unbounded orchestrator loop never awaits a durable task, waits for an event, "+
			"or calls ContinueAsNew, so it cannot make progress or complete",
	)
}

// loopProbe walks a loop body and everything it calls inside the package.
// inconclusive means the loop could not be proven to be a pure spin.
type loopProbe struct {
	checker        *checker
	visitedCallees map[ast.Node]bool
	inconclusive   bool
}

// scan inspects one block and everything it calls inside the package. Blocking
// operations and calls the analyzer cannot follow end the proof everywhere.
//
// inLoop distinguishes the statements written directly in the loop from the body
// of a function the loop calls. In the loop, a control transfer or a closure
// means the loop may exit or do something the probe cannot see, so the proof
// ends; in a callee, returning is ordinary control flow and says nothing about
// the loop.
func (p *loopProbe) scan(body *ast.BlockStmt, inLoop bool) {
	ast.Inspect(body, func(node ast.Node) bool {
		if p.inconclusive {
			return false
		}
		switch node := node.(type) {
		case *ast.SelectStmt, *ast.SendStmt, *ast.GoStmt:
			p.inconclusive = true
			return false
		case *ast.ReturnStmt, *ast.BranchStmt, *ast.LabeledStmt,
			*ast.FuncLit, *ast.DeferStmt:
			if inLoop {
				p.inconclusive = true
				return false
			}
		case *ast.UnaryExpr:
			if node.Op == token.ARROW {
				p.inconclusive = true
				return false
			}
		case *ast.RangeStmt:
			rangeType := p.checker.pass.TypesInfo.TypeOf(node.X)
			if isChannel(rangeType) || isFunction(rangeType) || isTypeParameter(rangeType) {
				p.inconclusive = true
				return false
			}
		case *ast.CallExpr:
			p.scanCall(node)
		}
		return true
	})
}

func (p *loopProbe) scanCall(call *ast.CallExpr) {
	if identifier, ok := call.Fun.(*ast.Ident); ok {
		if builtin, ok := p.checker.pass.TypesInfo.ObjectOf(identifier).(*types.Builtin); ok {
			switch builtin.Name() {
			case "panic", "recover":
				p.inconclusive = true
			}
			return
		}
	}
	if callee := p.checker.index.callee(call); callee != nil {
		p.scanCallee(callee)
		return
	}
	function := staticFunc(p.checker.pass, call.Fun)
	if function == nil || function.Pkg() == nil {
		p.inconclusive = true
		return
	}
	switch path := function.Pkg().Path(); {
	case isTaskIsCompleted(function):
		// IsCompleted only reads completion state; it never yields.
	case path == taskPackagePath:
		// Any durable task interaction may make progress or block.
		p.inconclusive = true
	case pureLoopPackages[path]:
	default:
		p.inconclusive = true
	}
}

func (p *loopProbe) scanCallee(node ast.Node) {
	if p.visitedCallees[node] {
		// Recursion is safe because the function is already being scanned.
		return
	}
	p.visitedCallees[node] = true
	body := funcBody(node)
	if body == nil {
		p.inconclusive = true
		return
	}
	p.scan(body, false)
}

func isFunction(value types.Type) bool {
	if value == nil {
		return false
	}
	_, ok := value.Underlying().(*types.Signature)
	return ok
}

func isTypeParameter(value types.Type) bool {
	if value == nil {
		return false
	}
	_, ok := types.Unalias(value).(*types.TypeParam)
	return ok
}

func isConstantTrue(pass *analysis.Pass, expression ast.Expr) bool {
	typeInfo, ok := pass.TypesInfo.Types[expression]
	if !ok || typeInfo.Value == nil || typeInfo.Value.Kind() != constant.Bool {
		return false
	}
	return constant.BoolVal(typeInfo.Value)
}
