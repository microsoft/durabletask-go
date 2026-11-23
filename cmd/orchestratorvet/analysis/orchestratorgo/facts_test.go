package orchestratorgo_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func TestCrossPackageVet(t *testing.T) {
	stub, err := os.ReadFile(taskStubPath)
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	for name, contents := range map[string]string{
		`go.mod`: `module example.com/workflows
go 1.26.0
require (
	github.com/microsoft/durabletask-go v0.0.0
	example.com/workflows/external v0.0.0
)
replace github.com/microsoft/durabletask-go => ./sdk
replace example.com/workflows/external => ./external
`,
		`sdk/go.mod`:        "module github.com/microsoft/durabletask-go\ngo 1.26.0\n",
		`sdk/task/task.go`:  string(stub),
		`external/go.mod`:   "module example.com/workflows/external\ngo 1.26.0\n",
		`external/clock.go`: "package external\nimport \"time\"\nfunc Clock() { _ = time.Now() }\n",
		`billing/billing.go`: `package billing
import (
	"math/rand"
	"os"
	"time"
	"github.com/microsoft/durabletask-go/task"
)
func Cutoff() { first(0) }
func first(depth int) { second(depth) }
func second(depth int) {
	if depth > 0 { first(depth - 1) }
	_ = time.Now()
}
type Ledger[T any] struct{}
func (Ledger[T]) Read() { _ = os.Getenv("REGION") }
func Callback(*task.OrchestrationContext) { _ = time.Now() }
func Clean() { _ = time.Unix(0, 0) }
type Hook func()
func (f Hook) RunAndNext() Hook {
	f()
	f = Cutoff
	return f
}
var Seed = time.Now().UnixNano()
func Sample() int {
	r := rand.New(rand.NewSource(Seed))
	return r.Int()
}
`,
		`orders/orders.go`: `package orders
import (
	"example.com/workflows/billing"
	"github.com/microsoft/durabletask-go/task"
)
func Fulfill(*task.OrchestrationContext) (any, error) {
	billing.Cutoff()
	return nil, nil
}
func Activity(task.ActivityContext) (any, error) {
	billing.Cutoff()
	return nil, nil
}
`,
		`worker/worker.go`: `package worker
import (
	"example.com/workflows/billing"
	"example.com/workflows/external"
	"example.com/workflows/orders"
	"github.com/microsoft/durabletask-go/task"
)
func register(r *task.TaskRegistry) {
	_ = r.AddOrchestrator(orders.Fulfill)
	_ = r.AddOrchestrator(local)
	_ = r.AddActivity(orders.Activity)
}
type reader interface{ Read() }
var Handler = billing.Cutoff
func local(ctx *task.OrchestrationContext) (any, error) {
	read := billing.Ledger[int]{}.Read
	read()
	ctx.Go(billing.Callback)
	billing.Clean()
	external.Clock()
	ignore(func() { billing.Cutoff() })
	dynamic(billing.Ledger[int]{})
	Handler()
	fn := billing.Cutoff
	p := &fn
	*p = billing.Clean
	fn()
	billing.Hook(func() {}).RunAndNext()
	_ = billing.Sample()
	return nil, nil
}
func init() { billing.Seed = 1 }
func ignore(func()) {}
func dynamic(r reader) { r.Read() }
`,
	} {
		path := filepath.Join(dir, filepath.FromSlash(name))
		if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(contents), 0600); err != nil {
			t.Fatal(err)
		}
	}

	tool := filepath.Join(dir, "orchestratorvet")
	if runtime.GOOS == "windows" {
		tool += ".exe"
	}
	env := append(os.Environ(), "GOWORK=off", "GO111MODULE=on")
	build := exec.CommandContext(t.Context(), "go", "build", "-o", tool, ".")
	build.Dir = filepath.Join("..", "..")
	build.Env = env
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build orchestratorvet: %v\n%s", err, output)
	}

	for _, target := range []string{"./worker", "./..."} {
		t.Run(target, func(t *testing.T) {
			vet := exec.CommandContext(t.Context(), "go", "vet", "-vettool="+tool, target)
			vet.Dir, vet.Env = dir, env
			output, err := vet.CombinedOutput()
			if err == nil {
				t.Fatalf("expected replay hazards from %s", target)
			}
			got := string(output)
			for _, want := range []string{
				"worker.go:9:24: example.com/workflows/orders.Fulfill -> example.com/workflows/billing.Cutoff -> example.com/workflows/billing.first -> example.com/workflows/billing.second: time.Now",
				"worker.go:17:2: (example.com/workflows/billing.Ledger[T]).Read: os.Getenv",
				"worker.go:18:9: example.com/workflows/billing.Callback: time.Now",
			} {
				if !strings.Contains(got, want) {
					t.Errorf("missing diagnostic %q in:\n%s", want, got)
				}
			}
			if count := strings.Count(got, ".go:"); count != 3 {
				t.Errorf("got %d diagnostic locations; want exactly 3:\n%s", count, got)
			}
		})
	}
}
