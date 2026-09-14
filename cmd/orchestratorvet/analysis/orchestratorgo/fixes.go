package orchestratorgo

import (
	"go/ast"
	"go/token"
	"go/types"

	"golang.org/x/tools/go/analysis"
)

// currentTimeFix rewrites time.Now() as ctx.CurrentTimeUtc when an orchestration
// context is in scope. Only the zero-argument clock reads have a one-to-one
// replacement, so no other wall-clock call is offered a fix.
//
// reportClockDiagnostics combines these per-call bindings before offering a fix.
func (c *checker) currentTimeFix(
	call *ast.CallExpr,
	name string,
	stack []ast.Node,
	file *ast.File,
) []analysis.SuggestedFix {
	context := contextParameter(c.pass, stack)
	if name != "Now" || len(call.Args) != 0 || !c.bindingAvailable(context, file, call.Pos()) {
		return nil
	}
	// A field read cannot replace a call used as a statement, go, or defer.
	for i := len(stack) - 2; i >= 0; i-- {
		switch stack[i].(type) {
		case *ast.ParenExpr:
			continue
		case *ast.ExprStmt, *ast.GoStmt, *ast.DeferStmt:
			return nil
		}
		break
	}
	edits := []analysis.TextEdit{{
		Pos:     call.Pos(),
		End:     call.End(),
		NewText: []byte(context.Name() + ".CurrentTimeUtc"),
	}}
	return []analysis.SuggestedFix{{
		Message:   "use (*task.OrchestrationContext).CurrentTimeUtc for fixable time.Now calls in this file",
		TextEdits: edits,
	}}
}

// reportClockDiagnostics attaches each file's complete clock fix to only its
// first fixable diagnostic. Every other clock diagnostic remains visible, but
// offers no partial fix that could conflict with the file's import cleanup.
func (c *checker) reportClockDiagnostics() {
	if len(c.clockDiagnostics) == 0 {
		return
	}
	fixes := make(map[*token.File]*analysis.SuggestedFix)
	for i := range c.clockDiagnostics {
		diagnostic := &c.clockDiagnostics[i]
		file := c.pass.Fset.File(diagnostic.Pos)
		fix := &diagnostic.SuggestedFixes[0]
		if first := fixes[file]; first != nil {
			first.TextEdits = append(first.TextEdits, fix.TextEdits...)
			diagnostic.SuggestedFixes = nil
		} else {
			fixes[file] = fix
		}
	}
	for _, file := range c.pass.Files {
		if fix := fixes[c.pass.Fset.File(file.Pos())]; fix != nil {
			fix.TextEdits = append(fix.TextEdits, c.unusedTimeImportRemovals(file, fix.TextEdits)...)
		}
	}
	for _, diagnostic := range c.clockDiagnostics {
		c.pass.Report(diagnostic)
	}
}

// goStatementFix rewrites `go func() { ... }()` as an orchestration coroutine.
// It only applies to an immediately invoked literal with no parameters, no
// results, and no arguments, because any other form would change the meaning of
// the captured values or the call.
//
// The literal's body is never reprinted. Comments live on the file rather than
// on the statements they annotate, so printing the block would silently drop
// them; editing only the text around the braces leaves the body, and everything
// written inside it, exactly as the author wrote it.
func (c *checker) goStatementFix(
	statement *ast.GoStmt,
	context types.Object,
	file *ast.File,
) []analysis.SuggestedFix {
	if !c.bindingAvailable(context, file, statement.Pos()) {
		return nil
	}
	literal, ok := statement.Call.Fun.(*ast.FuncLit)
	if !ok || len(statement.Call.Args) != 0 || statement.Call.Ellipsis.IsValid() {
		return nil
	}
	if literal.Body == nil || !literal.Body.Lbrace.IsValid() || !literal.Body.Rbrace.IsValid() {
		return nil
	}
	signature := literal.Type
	if signature.Params != nil && len(signature.Params.List) != 0 {
		return nil
	}
	if signature.Results != nil && len(signature.Results.List) != 0 {
		return nil
	}
	qualifier := taskImport(c.pass, file)
	if qualifier == nil || !c.bindingAvailable(qualifier, file, statement.Pos()) {
		return nil
	}
	return []analysis.SuggestedFix{{
		Message: "use (*task.OrchestrationContext).Go",
		TextEdits: []analysis.TextEdit{
			{
				// `go func() ` becomes `ctx.Go(func(*task.OrchestrationContext) `,
				// stopping short of the brace so the body is untouched.
				Pos:     statement.Pos(),
				End:     literal.Body.Lbrace,
				NewText: []byte(context.Name() + ".Go(func(*" + qualifier.Name() + ".OrchestrationContext) "),
			},
			{
				// The trailing `()` of the immediate invocation becomes the
				// closing paren of the Go call.
				Pos:     literal.Body.Rbrace + 1,
				End:     statement.End(),
				NewText: []byte(")"),
			},
		},
	}}
}

func (c *checker) bindingAvailable(object types.Object, file *ast.File, position token.Pos) bool {
	if object == nil || file == nil || len(c.index.functionValues[object]) != 0 {
		return false
	}
	scope := c.pass.TypesInfo.Scopes[file]
	if scope == nil {
		return false
	}
	scope = scope.Innermost(position)
	if scope == nil {
		return false
	}
	_, visible := scope.LookupParent(object.Name(), position)
	// A shadow can have the same type while referring to an unrelated value.
	return visible == object
}

// unusedTimeImportRemovals checks the file once, ignoring only expressions the
// atomic clock fix replaces. Unreachable or unfixable uses keep their imports.
func (c *checker) unusedTimeImportRemovals(
	file *ast.File,
	replacements []analysis.TextEdit,
) []analysis.TextEdit {
	replaced := make(map[token.Pos]token.Pos, len(replacements))
	for _, edit := range replacements {
		replaced[edit.Pos] = edit.End
	}
	usedImports := make(map[*types.PkgName]bool)
	usedPackages := make(map[*types.Package]bool)
	ast.Inspect(file, func(node ast.Node) bool {
		if node == nil {
			return true
		}
		if replaced[node.Pos()] == node.End() {
			return false
		}
		if identifier, ok := node.(*ast.Ident); ok {
			object := c.pass.TypesInfo.Uses[identifier]
			if name, ok := object.(*types.PkgName); ok {
				usedImports[name] = true
			} else if object != nil {
				usedPackages[object.Pkg()] = true
			}
		}
		return true
	})
	var edits []analysis.TextEdit
	for declarationIndex, declaration := range file.Decls {
		importDeclaration, ok := declaration.(*ast.GenDecl)
		if !ok || importDeclaration.Tok != token.IMPORT {
			continue
		}
		var unused []int
		for i, specification := range importDeclaration.Specs {
			specification := specification.(*ast.ImportSpec)
			object := c.pass.TypesInfo.Implicits[specification]
			if specification.Name != nil {
				object = c.pass.TypesInfo.Defs[specification.Name]
			}
			name, ok := object.(*types.PkgName)
			if !ok || name.Imported().Path() != "time" || name.Name() == "_" ||
				usedImports[name] || name.Name() == "." && usedPackages[name.Imported()] {
				continue
			}
			unused = append(unused, i)
		}
		if len(unused) == 0 {
			continue
		}
		if len(unused) == len(importDeclaration.Specs) {
			end := importDeclaration.End()
			if tokenFile := c.pass.Fset.File(end); tokenFile != nil {
				line := tokenFile.Line(end)
				end = tokenFile.Pos(tokenFile.Size())
				if line < tokenFile.LineCount() {
					end = tokenFile.LineStart(line + 1)
				}
			}
			if declarationIndex+1 < len(file.Decls) && file.Decls[declarationIndex+1].Pos() < end {
				end = file.Decls[declarationIndex+1].Pos()
			}
			edits = append(edits, analysis.TextEdit{
				Pos: importDeclaration.Pos(),
				End: end,
			})
			continue
		}
		for _, i := range unused {
			end := importDeclaration.Rparen
			if i+1 < len(importDeclaration.Specs) {
				end = importDeclaration.Specs[i+1].Pos()
			}
			// Include separators, but never a grouped import's closing paren.
			edits = append(edits, analysis.TextEdit{
				Pos: importDeclaration.Specs[i].Pos(),
				End: end,
			})
		}
	}
	// Ranges are in source order. Preserve comments swept up with import
	// separators, including comments between declarations, in one linear pass.
	i := 0
	for _, group := range file.Comments {
		for _, comment := range group.List {
			for i < len(edits) && edits[i].End <= comment.Pos() {
				i++
			}
			if i == len(edits) {
				return edits
			}
			if edits[i].Pos <= comment.Pos() {
				edits[i].NewText = append(edits[i].NewText, comment.Text...)
				edits[i].NewText = append(edits[i].NewText, '\n')
			}
		}
	}
	return edits
}

// taskImport returns the package binding the file uses for the durable task
// package, so generated code compiles under dot-free aliases too.
func taskImport(pass *analysis.Pass, file *ast.File) *types.PkgName {
	for _, specification := range file.Imports {
		object := pass.TypesInfo.Implicits[specification]
		if specification.Name != nil {
			object = pass.TypesInfo.Defs[specification.Name]
		}
		name, ok := object.(*types.PkgName)
		if ok && name.Imported().Path() == taskPackagePath && name.Name() != "." && name.Name() != "_" {
			return name
		}
	}
	return nil
}
