package orchestratorgo_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"go/token"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/microsoft/durabletask-go/cmd/orchestratorvet/analysis/orchestratorgo"
	"golang.org/x/tools/go/analysis"
	"golang.org/x/tools/go/analysis/analysistest"
	"golang.org/x/tools/go/packages"
)

// fixPackages are the fixture packages that carry a .golden file and are
// therefore driven by RunWithSuggestedFixes rather than by Run.
var fixPackages = []string{
	"fixes", "fixesimport", "fixesstandalone", "fixesstandalonealias",
	"fixesbatch", "fixesbatchstandalone", "fixesbatchalias", "fixespartial", "fixesinline",
	"fixbindings", "fixesunusedliteral",
}

// stubPackageRoot is the fixture tree holding the stand-in dependencies the
// scenario packages import. It is not a scenario itself.
const stubPackageRoot = "github.com"

const fixtureLoadMode = packages.NeedName |
	packages.NeedFiles |
	packages.NeedCompiledGoFiles |
	packages.NeedImports |
	packages.NeedTypes |
	packages.NeedSyntax |
	packages.NeedTypesInfo |
	packages.NeedDeps

func fixtureEnvironment(root string) []string {
	return append(os.Environ(), "GOPATH="+root, "GO111MODULE=off", "GOWORK=off")
}

// scenarioPackages discovers the analysistest fixture packages, one per
// concern, so a new fixture directory is exercised without also having to be
// listed here. Discovery failing is a test failure: a silently empty list would
// make the whole suite pass without analyzing anything.
func scenarioPackages(t *testing.T) []string {
	t.Helper()
	root := filepath.Join(analysistest.TestData(), "src")
	entries, err := os.ReadDir(root)
	if err != nil {
		t.Fatalf("read fixture root %s: %v", root, err)
	}
	var scenarios []string
	for _, entry := range entries {
		if !entry.IsDir() || entry.Name() == stubPackageRoot {
			continue
		}
		if slices.Contains(fixPackages, entry.Name()) {
			continue
		}
		scenarios = append(scenarios, entry.Name())
	}
	if len(scenarios) == 0 {
		t.Fatalf("no fixture packages found under %s", root)
	}
	slices.Sort(scenarios)
	return scenarios
}

func TestAnalyzer(t *testing.T) {
	for _, scenario := range scenarioPackages(t) {
		t.Run(scenario, func(t *testing.T) {
			analysistest.Run(t, analysistest.TestData(), orchestratorgo.Analyzer, scenario)
		})
	}
}

func TestAnalyzerSuggestedFixes(t *testing.T) {
	analysistest.RunWithSuggestedFixes(t, analysistest.TestData(), orchestratorgo.Analyzer, fixPackages...)
}

// TestSuggestedFixesCompile checks that the expected golden files compile.
// TestSuggestedFixTextEditsCompile separately checks the actual edits, without
// analysistest's automatic removal of unused imports.
func TestSuggestedFixesCompile(t *testing.T) {
	for _, name := range fixPackages {
		t.Run(name, func(t *testing.T) {
			testdata := analysistest.TestData()
			source := filepath.Join(testdata, "src", name, name+".go")
			fixed, err := os.ReadFile(source + ".golden")
			if err != nil {
				t.Fatalf("read golden: %v", err)
			}
			checkFixedPackage(t, name, map[string][]byte{source: fixed})
		})
	}
}

// TestSuggestedFixTextEditsCompile enforces this analyzer's stronger raw-edit
// policy. Unlike this test, x/tools' CLI resolves merges and removes unused
// imports; raw composition failing alone does not establish a CLI failure.
func TestSuggestedFixTextEditsCompile(t *testing.T) {
	results := analysistest.Run(t, analysistest.TestData(), orchestratorgo.Analyzer, fixPackages...)
	for _, result := range results {
		if result.Pass.Pkg.Name() == "main" && strings.HasSuffix(result.Pass.Pkg.Path(), ".test") {
			// Runtime fixtures also load a generated test main, not a fix scenario.
			continue
		}
		t.Run(result.Pass.Pkg.Name(), func(t *testing.T) {
			var combined []analysis.TextEdit
			clockFixes := make(map[string]int)
			for i, diagnostic := range result.Diagnostics {
				for j, fix := range diagnostic.SuggestedFixes {
					if strings.HasPrefix(diagnostic.Message, "time.Now ") {
						clockFixes[result.Pass.Fset.Position(diagnostic.Pos).Filename]++
					}
					t.Run(fmt.Sprintf("individual_%d_%d", i, j), func(t *testing.T) {
						checkFixedPackage(t, result.Pass.Pkg.Path(), applyTextEdits(t, result.Pass.Fset, fix.TextEdits))
					})
					combined = append(combined, fix.TextEdits...)
				}
			}
			for file, count := range clockFixes {
				if count != 1 {
					t.Errorf("%s: got %d clock fixes, want one atomic file-level fix", file, count)
				}
			}
			if len(combined) == 0 {
				t.Fatal("fixture produced no suggested fixes")
			}
			t.Run("combined", func(t *testing.T) {
				checkFixedPackage(t, result.Pass.Pkg.Path(), applyTextEdits(t, result.Pass.Fset, combined))
			})
		})
	}
}

func TestSuggestedFixesPreserveContextBindings(t *testing.T) {
	results := analysistest.Run(t, analysistest.TestData(), orchestratorgo.Analyzer, "fixbindings")
	var edits []analysis.TextEdit
	for _, diagnostic := range results[0].Diagnostics {
		if strings.HasPrefix(diagnostic.Message, "time.Now ") {
			for _, fix := range diagnostic.SuggestedFixes {
				edits = append(edits, fix.TextEdits...)
			}
		}
	}
	if len(edits) == 0 {
		t.Fatal("fixture produced no clock fix")
	}
	replacements := make(map[string]string)
	for original, content := range applyTextEdits(t, results[0].Pass.Fset, edits) {
		fixed := filepath.Join(t.TempDir(), "fixed.go")
		if err := os.WriteFile(fixed, content, 0600); err != nil {
			t.Fatal(err)
		}
		replacements[original] = fixed
	}
	overlay, err := json.Marshal(struct{ Replace map[string]string }{replacements})
	if err != nil {
		t.Fatal(err)
	}
	overlayFile := filepath.Join(t.TempDir(), "overlay.json")
	if err := os.WriteFile(overlayFile, overlay, 0600); err != nil {
		t.Fatal(err)
	}
	command := exec.CommandContext(t.Context(), "go", "test", "-vet=off", "-overlay", overlayFile,
		"-run", "^TestContextBindingsRemainSafe$", "-count=1", "-timeout=30s", "fixbindings")
	command.Dir = analysistest.TestData()
	command.Env = fixtureEnvironment(command.Dir)
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("fixed context binding changed runtime behavior: %v\n%s", err, output)
	}
}

func checkFixedPackage(t *testing.T, name string, overlay map[string][]byte) {
	t.Helper()
	testdata := analysistest.TestData()
	loaded, err := packages.Load(&packages.Config{
		Mode:    fixtureLoadMode,
		Dir:     testdata,
		Env:     fixtureEnvironment(testdata),
		Overlay: overlay,
	}, name)
	if err != nil {
		t.Fatalf("load fixed package: %v", err)
	}
	if len(loaded) != 1 {
		t.Fatalf("loaded %d packages, want 1", len(loaded))
	}
	for _, loadError := range loaded[0].Errors {
		t.Errorf("fixed %s does not compile: %v", name, loadError)
	}
}

// applyTextEdits applies only the edits supplied by the analyzer. In particular,
// it neither cleans imports nor coalesces overlapping edits from separate fixes.
func applyTextEdits(t *testing.T, fset *token.FileSet, edits []analysis.TextEdit) map[string][]byte {
	t.Helper()
	byFile := make(map[*token.File][]analysis.TextEdit)
	for _, edit := range edits {
		file := fset.File(edit.Pos)
		if file == nil || fset.File(edit.End) != file {
			t.Fatalf("invalid edit range: %v", edit)
		}
		byFile[file] = append(byFile[file], edit)
	}
	overlay := make(map[string][]byte)
	for file, edits := range byFile {
		source, err := os.ReadFile(file.Name())
		if err != nil {
			t.Fatal(err)
		}
		slices.SortFunc(edits, func(a, b analysis.TextEdit) int { return int(a.Pos - b.Pos) })
		var fixed bytes.Buffer
		offset := 0
		for _, edit := range edits {
			start, end := file.Offset(edit.Pos), file.Offset(edit.End)
			if start < offset || end < start || end > len(source) {
				t.Fatalf("%s: overlapping or invalid edit at %d:%d", file.Name(), start, end)
			}
			fixed.Write(source[offset:start])
			fixed.Write(edit.NewText)
			offset = end
		}
		fixed.Write(source[offset:])
		overlay[file.Name()] = fixed.Bytes()
	}
	return overlay
}

// TestAnalyzerMetadata guards the identity the vet tool and documentation use.
func TestAnalyzerMetadata(t *testing.T) {
	if orchestratorgo.Analyzer.Name != "orchestratorgo" {
		t.Fatalf("analyzer name = %q, want %q", orchestratorgo.Analyzer.Name, "orchestratorgo")
	}
	if orchestratorgo.Analyzer.Doc == "" {
		t.Fatal("analyzer doc must be set so go vet can describe the check")
	}
	if orchestratorgo.Analyzer.Run == nil {
		t.Fatal("analyzer must have a run function")
	}
	if !strings.Contains(orchestratorgo.Analyzer.URL, "orchestratorgo") {
		t.Fatalf("analyzer URL = %q, want it to point at the analyzer", orchestratorgo.Analyzer.URL)
	}
}
