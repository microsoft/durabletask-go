package orchestratorgo

import (
	"cmp"
	"go/ast"
	"go/token"
	"go/types"
	"slices"
	"strings"
)

// A fact carries positive evidence only, with one witness per hazard category.
// Positions and suggested fixes belong to the declaring package's analysis pass.
type hazardFact struct {
	Module    string
	Witnesses []hazardWitness
}

func (*hazardFact) AFact() {}

type hazardWitness struct {
	Category string
	Path     []string
	Message  string
}

type hazardFacts struct {
	index  *packageIndex
	module string
	cache  map[*types.Func][]hazardWitness
}

func newHazardFacts(index *packageIndex) *hazardFacts {
	pass := index.pass
	if pass.Module == nil || pass.Module.Path == "" || pass.Pkg.Path() == taskPackagePath {
		return nil
	}
	return &hazardFacts{
		index: index, module: pass.Module.Path, cache: make(map[*types.Func][]hazardWitness),
	}
}

func (f *hazardFacts) imported(function *types.Func) []hazardWitness {
	if function == nil || function.Pkg().Path() == taskPackagePath {
		return nil
	}
	if witnesses, ok := f.cache[function]; ok {
		return witnesses
	}
	var fact hazardFact
	var witnesses []hazardWitness
	if f.index.pass.ImportObjectFact(function, &fact) && fact.Module == f.module {
		witnesses = fact.Witnesses
	}
	f.cache[function] = witnesses
	return witnesses
}

func (f *hazardFacts) report(c *checker, pos token.Pos, function *types.Func) {
	for _, w := range f.imported(function) {
		c.report(pos, w.Category, nil, "%s: %s", strings.Join(w.Path, " -> "), w.Message)
	}
}

func (f *hazardFacts) export() {
	index := f.index
	index.summarizing = true
	defer func() { index.summarizing = false }()

	var bodies []ast.Node
	names := make(map[ast.Node]*types.Func, len(index.functions))
	for function, decl := range index.functions {
		names[decl] = function
		if function.Exported() && decl.Body != nil {
			bodies = append(bodies, decl)
		}
	}
	slices.SortFunc(bodies, func(a, b ast.Node) int { return cmp.Compare(a.Pos(), b.Pos()) })
	exports := len(bodies)
	seen := make(map[ast.Node]bool, exports)
	for _, body := range bodies {
		seen[body] = true
	}

	// Build reverse edges, then propagate evidence back to callers. Each body
	// is checked once, and receives each category at most once even in a cycle.
	parents := make(map[ast.Node][]ast.Node)
	known := make(map[ast.Node][]hazardWitness)
	type finding struct {
		body ast.Node
		hazardWitness
	}
	var findings []finding
	add := func(body ast.Node, w hazardWitness) {
		for _, previous := range known[body] {
			if previous.Category == w.Category {
				return
			}
		}
		label := "func literal"
		if function := names[body]; function != nil {
			label = function.FullName()
		}
		w.Path = prependWitness(label, w.Path)
		known[body] = append(known[body], w)
		findings = append(findings, finding{body, w})
	}

	c := &checker{pass: index.pass, index: index}
	for i := 0; i < len(bodies); i++ {
		body := bodies[i]
		c.collectHazard = func(category, message string) {
			add(body, hazardWitness{Category: category, Message: message})
		}
		c.checkFunction(body)
		index.forEachTarget(body, func(_ token.Pos, target functionTarget) {
			if target.local != nil {
				parents[target.local] = append(parents[target.local], body)
				if !seen[target.local] {
					seen[target.local] = true
					bodies = append(bodies, target.local)
				}
			}
			for _, w := range f.imported(target.imported) {
				add(body, w)
			}
		})
	}

	for i := 0; i < len(findings); i++ {
		hit := findings[i]
		for _, parent := range parents[hit.body] {
			add(parent, hit.hazardWitness)
		}
	}
	for _, body := range bodies[:exports] {
		if witnesses := known[body]; len(witnesses) != 0 {
			index.pass.ExportObjectFact(names[body], &hazardFact{Module: f.module, Witnesses: witnesses})
		}
	}
}

// Bound the witness size while retaining both ends of a long path.
func prependWitness(label string, path []string) []string {
	const maxSteps = 8
	if len(path) >= maxSteps {
		result := make([]string, maxSteps)
		result[0] = label
		copy(result[1:maxSteps-2], path)
		result[maxSteps-2], result[maxSteps-1] = "...", path[len(path)-1]
		return result
	}
	return append([]string{label}, path...)
}
