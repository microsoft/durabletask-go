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
// the task package, leaves the loop unreported.
func (c *checker) checkUnboundedLoop(loop *ast.ForStmt) {
	if loop.Cond != nil && !isConstantTrue(c.pass, loop.Cond) {
		return
	}
	if loop.Body == nil {
		// `for {}` with an empty body is unambiguously a spin.
		c.reportUnboundedLoop(loop)
		return
	}
	probe := &loopProbe{
		checker:        c,
		visitedCallees: make(map[ast.Node]bool),
	}
	probe.scan(loop.Body, true)
	if probe.inconclusive {
		return
	}
	c.reportUnboundedLoop(loop)
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
			if isChannel(p.checker.pass.TypesInfo.TypeOf(node.X)) {
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

func isConstantTrue(pass *analysis.Pass, expression ast.Expr) bool {
	typeInfo, ok := pass.TypesInfo.Types[expression]
	if !ok || typeInfo.Value == nil || typeInfo.Value.Kind() != constant.Bool {
		return false
	}
	return constant.BoolVal(typeInfo.Value)
}
