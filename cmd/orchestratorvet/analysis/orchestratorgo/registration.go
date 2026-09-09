package orchestratorgo

import (
	"go/ast"
	"go/constant"
	"go/types"
	"strings"

	"golang.org/x/tools/go/analysis"
)

const taskPackagePath = "github.com/microsoft/durabletask-go/task"

// registrationKind distinguishes the two task namespaces the analyzer tracks.
type registrationKind int

const (
	orchestratorKind registrationKind = iota
	activityKind
)

func (kind registrationKind) String() string {
	if kind == orchestratorKind {
		return "orchestrator"
	}
	return "activity"
}

// registrationShape describes where the name, version, and handler live in the
// argument list of one task.TaskRegistry registration method.
type registrationShape struct {
	kind         registrationKind
	nameIndex    int // -1 when the name is derived by reflection
	handlerIndex int
	versionIndex int // -1 when the method has no version parameter
	arity        int
}

var registrationShapes = map[string]registrationShape{
	"AddOrchestrator":         {kind: orchestratorKind, nameIndex: -1, handlerIndex: 0, versionIndex: -1, arity: 1},
	"AddOrchestratorN":        {kind: orchestratorKind, nameIndex: 0, handlerIndex: 1, versionIndex: -1, arity: 2},
	"AddOrchestratorVersion":  {kind: orchestratorKind, nameIndex: -1, handlerIndex: 1, versionIndex: 0, arity: 2},
	"AddOrchestratorNVersion": {kind: orchestratorKind, nameIndex: 0, handlerIndex: 2, versionIndex: 1, arity: 3},
	"AddActivity":             {kind: activityKind, nameIndex: -1, handlerIndex: 0, versionIndex: -1, arity: 1},
	"AddActivityN":            {kind: activityKind, nameIndex: 0, handlerIndex: 1, versionIndex: -1, arity: 2},
	"AddActivityVersion":      {kind: activityKind, nameIndex: -1, handlerIndex: 1, versionIndex: 0, arity: 2},
	"AddActivityNVersion":     {kind: activityKind, nameIndex: 0, handlerIndex: 2, versionIndex: 1, arity: 3},
}

// registration is a single proven call to a task.TaskRegistry Add* method.
type registration struct {
	// name is the registered task name when it can be proven statically.
	name string
	// nameKnown reports whether name was proven rather than guessed.
	nameKnown bool
	// version is the registered version when it is a string literal.
	version string
	// versionKnown reports whether the registration's version was proven. A
	// version computed at runtime leaves it false, which keeps the registration
	// out of duplicate detection: two calls with different computed versions
	// are not a conflict.
	versionKnown bool
}

// registrySet is the whole-package view of task.TaskRegistry registrations.
type registrySet struct {
	// names holds the lowercased proven names per namespace.
	names map[registrationKind]map[string]struct{}
	// dynamic reports that at least one registration in the namespace has a name
	// the analyzer could not prove, so absence from names proves nothing.
	dynamic map[registrationKind]bool
	// incomplete covers opaque registry uses that can register either kind.
	incomplete bool

	// orchestratorObjects holds the objects of registered orchestrator
	// declarations, used to detect direct orchestrator invocation.
	orchestratorObjects map[types.Object]string
	// roots holds every function node registered as an orchestrator.
	roots []ast.Node
}

func (set *registrySet) registered(kind registrationKind, name string) bool {
	_, ok := set.names[kind][strings.ToLower(name)]
	return ok
}

// provesAbsence reports whether the package's registrations are complete enough
// that a missing name is a real error rather than an artifact of cross-package
// or dynamic registration.
func (set *registrySet) provesAbsence(kind registrationKind) bool {
	if set.incomplete || set.dynamic[kind] || set.registered(kind, "*") {
		return false
	}
	return len(set.names[kind]) > 0
}

// collectRegistrations walks the package's registration calls and reports
// registration forms that task.TaskRegistry rejects or that derive an unstable
// name. The candidate calls were gathered during package indexing, so packages
// that register nothing cost nothing here.
func collectRegistrations(pass *analysis.Pass, index *packageIndex) *registrySet {
	set := &registrySet{
		names:               map[registrationKind]map[string]struct{}{orchestratorKind: {}, activityKind: {}},
		dynamic:             map[registrationKind]bool{},
		incomplete:          index.registrationsIncomplete,
		orchestratorObjects: make(map[types.Object]string),
	}

	// Only compatible direct statements on a stable registry are compared.
	// The key mirrors the registry's normalization: lowercase, without trimming.
	type duplicateKey struct {
		registry types.Object
		scope    ast.Node
		kind     registrationKind
		name     string
		version  string
	}
	duplicates := make(map[duplicateKey]bool)

	seenRoots := make(map[ast.Node]bool)
	for _, call := range index.registrationCandidates {
		shape, receiver, ok := registrationCall(pass, call)
		if !ok {
			continue
		}
		if !isRegistryConstructor(pass, index.singleValue(receiver)) {
			// Incoming, tuple-returned, or otherwise unproven registries may
			// already contain registrations invisible to this package.
			set.incomplete = true
		}

		handler := call.Args[shape.handlerIndex]
		if isNilExpr(pass, handler) {
			pass.Reportf(
				call.Lparen,
				"task.TaskRegistry registration with a nil %s always returns an error",
				shape.kind,
			)
			continue
		}
		handlerNode := index.resolveFunction(handler, nil)
		handlerObj := handlerObject(pass, handler)

		// rejected records that this call provably fails, so it contributes no
		// name to the registry and cannot conflict with another registration.
		version, versionKnown, versionRejected := registrationVersion(pass, call, shape)
		name, nameKnown, nameRejected := registrationName(pass, call, shape, handlerNode)
		entry := registration{
			name:         name,
			nameKnown:    nameKnown,
			version:      version,
			versionKnown: versionKnown,
		}
		rejected := versionRejected || nameRejected

		if entry.nameKnown {
			// A rejected call registers nothing, but its name is still recorded
			// so the unresolved-name check does not pile a second diagnostic
			// onto a call site whose registration was already reported here.
			set.names[shape.kind][strings.ToLower(entry.name)] = struct{}{}
			// A registration whose version is computed at runtime may land on
			// any key, so it neither proves nor disproves a conflict.
			scope := index.registrationScopes[call]
			if scope != nil && isRegistryConstructor(pass, index.singleValue(receiver)) &&
				!index.unstableRegistries[receiver] && entry.versionKnown && !rejected {
				key := duplicateKey{
					registry: receiver,
					scope:    scope,
					kind:     shape.kind,
					name:     strings.ToLower(entry.name),
					version:  strings.ToLower(entry.version),
				}
				if duplicates[key] {
					pass.Reportf(
						call.Lparen,
						"%s %q is registered more than once on the same task.TaskRegistry; "+
							"the duplicate registration returns an error",
						shape.kind,
						entry.name,
					)
				}
				duplicates[key] = true
			}
		} else {
			set.dynamic[shape.kind] = true
		}

		if handlerObj != nil && shape.kind == orchestratorKind {
			set.orchestratorObjects[handlerObj] = entry.name
		}

		if shape.kind == orchestratorKind && handlerNode != nil && !seenRoots[handlerNode] {
			seenRoots[handlerNode] = true
			set.roots = append(set.roots, handlerNode)
		}
	}
	return set
}

// registrationVersion returns the literal version, whether it is known, and
// whether the registry will reject it. Methods without a version argument have
// a known empty version.
func registrationVersion(
	pass *analysis.Pass,
	call *ast.CallExpr,
	shape registrationShape,
) (version string, known, rejected bool) {
	if shape.versionIndex < 0 {
		return "", true, false
	}
	version, known = stringLiteral(pass, call.Args[shape.versionIndex])
	if !known || version == "" || strings.TrimSpace(version) != "" {
		return version, known, false
	}
	pass.Reportf(
		call.Args[shape.versionIndex].Pos(),
		"task.TaskRegistry rejects an %s version that is only whitespace",
		shape.kind,
	)
	return version, true, true
}

// registrationName returns a statically proven name and reports explicit names
// the registry rejects or reflection-derived names that are unstable.
func registrationName(
	pass *analysis.Pass,
	call *ast.CallExpr,
	shape registrationShape,
	handler ast.Node,
) (name string, known, rejected bool) {
	if shape.nameIndex >= 0 {
		name, known = stringLiteral(pass, call.Args[shape.nameIndex])
		if !known {
			return "", false, false
		}
		switch {
		case name == "":
			pass.Reportf(
				call.Args[shape.nameIndex].Pos(),
				"task.TaskRegistry rejects an empty %s name",
				shape.kind,
			)
			return "", false, true
		case strings.TrimSpace(name) == "":
			// The registry accepts this name verbatim, so the task is
			// registered under a name no caller can type readably.
			pass.Reportf(
				call.Args[shape.nameIndex].Pos(),
				"task.TaskRegistry registers this %s under a name that is only whitespace, "+
					"which callers must reproduce exactly; use a non-blank name",
				shape.kind,
			)
		}
		return name, true, false
	}

	declaration, ok := handler.(*ast.FuncDecl)
	if ok {
		if declaration.Recv == nil {
			return declaration.Name.Name, true, false
		}
		pass.Reportf(
			call.Lparen,
			"task.TaskRegistry derives the %s name from a method value, which produces a "+
				"name with a compiler-generated \"-fm\" suffix; register it with an explicit name instead",
			shape.kind,
		)
		return "", false, false
	}
	if handler != nil {
		pass.Reportf(
			call.Lparen,
			"task.TaskRegistry derives the %s name from a function literal, "+
				"which produces a compiler-generated name; register it with an explicit name instead",
			shape.kind,
		)
	}
	return "", false, false
}

// registrationCall matches a call to a task.TaskRegistry Add* method and returns
// its shape plus the receiver variable when the receiver is a plain identifier.
func registrationCall(pass *analysis.Pass, call *ast.CallExpr) (registrationShape, types.Object, bool) {
	selector, ok := call.Fun.(*ast.SelectorExpr)
	if !ok {
		return registrationShape{}, nil, false
	}
	selection := pass.TypesInfo.Selections[selector]
	if selection == nil || !isTaskRegistry(selection.Recv()) {
		return registrationShape{}, nil, false
	}
	method, ok := selection.Obj().(*types.Func)
	if !ok || method.Pkg() == nil || method.Pkg().Path() != taskPackagePath {
		return registrationShape{}, nil, false
	}
	shape, ok := registrationShapes[method.Name()]
	if !ok || len(call.Args) != shape.arity {
		return registrationShape{}, nil, false
	}
	var receiver types.Object
	if identifier, ok := selector.X.(*ast.Ident); ok {
		receiver = pass.TypesInfo.ObjectOf(identifier)
	}
	return shape, receiver, true
}

func isTaskRegistry(value types.Type) bool {
	value = types.Unalias(value)
	if pointer, ok := value.(*types.Pointer); ok {
		value = types.Unalias(pointer.Elem())
	}
	named, ok := value.(*types.Named)
	if !ok {
		return false
	}
	object := named.Obj()
	return object.Pkg() != nil &&
		object.Pkg().Path() == taskPackagePath &&
		object.Name() == "TaskRegistry"
}

func isRegistryConstructor(pass *analysis.Pass, expression ast.Expr) bool {
	call, ok := ast.Unparen(expression).(*ast.CallExpr)
	if !ok {
		return false
	}
	function := staticFunc(pass, call.Fun)
	return function != nil && function.Pkg() != nil &&
		function.Pkg().Path() == taskPackagePath && function.Name() == "NewTaskRegistry"
}

// handlerObject returns the declared function object a handler expression names.
func handlerObject(pass *analysis.Pass, expression ast.Expr) types.Object {
	switch expression := expression.(type) {
	case *ast.ParenExpr:
		return handlerObject(pass, expression.X)
	case *ast.Ident:
		if function, ok := pass.TypesInfo.ObjectOf(expression).(*types.Func); ok {
			return function
		}
	case *ast.SelectorExpr:
		if selection := pass.TypesInfo.Selections[expression]; selection != nil {
			if function, ok := selection.Obj().(*types.Func); ok {
				return function
			}
			return nil
		}
		if function, ok := pass.TypesInfo.ObjectOf(expression.Sel).(*types.Func); ok {
			return function
		}
	case *ast.CallExpr:
		if typeInfo, ok := pass.TypesInfo.Types[expression.Fun]; ok &&
			typeInfo.IsType() &&
			len(expression.Args) == 1 {
			return handlerObject(pass, expression.Args[0])
		}
	}
	return nil
}

// stringLiteral returns the constant string value of an expression.
func stringLiteral(pass *analysis.Pass, expression ast.Expr) (string, bool) {
	typeInfo, ok := pass.TypesInfo.Types[expression]
	if !ok || typeInfo.Value == nil || typeInfo.Value.Kind() != constant.String {
		return "", false
	}
	return constant.StringVal(typeInfo.Value), true
}

func isNilExpr(pass *analysis.Pass, expression ast.Expr) bool {
	identifier, ok := expression.(*ast.Ident)
	if !ok || identifier.Name != "nil" {
		return false
	}
	_, isNil := pass.TypesInfo.ObjectOf(identifier).(*types.Nil)
	return isNil
}

// taskName reports the durable task name an expression passed to CallActivity or
// CallSubOrchestrator resolves to, mirroring helpers.GetTaskFunctionName.
//
// Method values are excluded because reflection appends a compiler-generated
// "-fm" suffix to their name, which cannot be derived from the syntax.
func (index *packageIndex) taskName(expression ast.Expr) (string, bool) {
	if name, ok := stringLiteral(index.pass, expression); ok {
		return name, true
	}
	function, ok := handlerObject(index.pass, expression).(*types.Func)
	if !ok {
		return "", false
	}
	if signature, ok := function.Type().(*types.Signature); ok && signature.Recv() != nil {
		return "", false
	}
	return function.Name(), true
}
