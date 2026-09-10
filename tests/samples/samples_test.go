package samples_test

import (
	"context"
	"go/parser"
	"go/token"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

type sample struct {
	name    string
	group   string
	input   string
	timeout time.Duration
}

// Keep this execution list aligned with the feature map in samples/README.md.
var catalogue = []sample{
	{name: "durabletaskscheduler", group: "emulator"},
	{name: "parallel", group: "emulator"},
	{name: "coroutines", group: "emulator"},
	{name: "timers", group: "emulator"},
	{name: "externalevents", group: "emulator", input: "Taylor\n"},
	{name: "suborchestrations", group: "emulator"},
	{name: "retries", group: "emulator"},
	{name: "continueasnew", group: "emulator"},
	{name: "management", group: "emulator"},
	{name: "scheduledtasks", group: "emulator"},
	{name: "versioning", group: "emulator"},
	{name: "entity", group: "emulator"},
	{name: "dataconverter", group: "emulator"},
	{name: "largepayloads", group: "storage"},
	{name: "history", group: "emulator"},
	{name: "observability", group: "emulator"},
	{name: "worker", group: "emulator"},
	{name: "authentication", group: "azure", timeout: 3 * time.Minute},
	{name: "distributedtracing", group: "telemetry"},
	{name: "exporthistory", group: "storage", timeout: 8 * time.Minute},
	{name: "replayanalysis", group: "emulator"},
	{name: "serviceoperations", group: "admin", timeout: 6 * time.Minute},
}

func repositoryRoot(t *testing.T) string {
	t.Helper()
	root, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	return root
}

func TestSampleCatalogue(t *testing.T) {
	root := repositoryRoot(t)
	listed := make(map[string]bool, len(catalogue))
	for _, entry := range catalogue {
		if listed[entry.name] {
			t.Fatalf("duplicate sample %q", entry.name)
		}
		listed[entry.name] = true
		if _, err := os.Stat(filepath.Join(root, "samples", entry.name, "README.md")); err != nil {
			t.Errorf("%s needs run instructions: %v", entry.name, err)
		}
	}
	directories, err := os.ReadDir(filepath.Join(root, "samples"))
	if err != nil {
		t.Fatal(err)
	}
	for _, directory := range directories {
		if !directory.IsDir() || directory.Name() == "internal" || directory.Name() == "testdata" {
			continue
		}
		files, err := os.ReadDir(filepath.Join(root, "samples", directory.Name()))
		if err != nil {
			t.Fatal(err)
		}
		runnable := false
		for _, file := range files {
			if !strings.HasSuffix(file.Name(), ".go") || strings.HasSuffix(file.Name(), "_test.go") {
				continue
			}
			path := filepath.Join(root, "samples", directory.Name(), file.Name())
			parsed, err := parser.ParseFile(token.NewFileSet(), path, nil, parser.PackageClauseOnly)
			if err != nil {
				t.Fatal(err)
			}
			runnable = runnable || parsed.Name.Name == "main"
		}
		if runnable && !listed[directory.Name()] {
			t.Errorf("runnable sample %q has no E2E catalogue entry", directory.Name())
		}
		if runnable {
			delete(listed, directory.Name())
		}
	}
	for name := range listed {
		t.Errorf("catalogue entry %q has no runnable package", name)
	}
}

func TestSamplesE2E(t *testing.T) {
	if os.Getenv("DTS_SAMPLES_E2E") != "1" {
		t.Skip("set DTS_SAMPLES_E2E=1 to execute real sample programs; this skip is not E2E coverage")
	}
	group := os.Getenv("DTS_SAMPLES_GROUP")
	if group == "" {
		group = "all"
	}
	switch group {
	case "all", "emulator", "storage", "telemetry", "azure", "admin":
	default:
		t.Fatalf("unknown DTS_SAMPLES_GROUP %q", group)
	}
	root := repositoryRoot(t)
	for _, entry := range catalogue {
		if group != "all" && entry.group != group {
			continue
		}
		t.Run(entry.name, func(t *testing.T) {
			environment := sampleEnvironment(t, entry.group)
			binary := filepath.Join(t.TempDir(), entry.name)
			if runtime.GOOS == "windows" {
				binary += ".exe"
			}
			buildArgs := []string{"build", "-mod=readonly", "-o", binary}
			if os.Getenv("DTS_SAMPLES_RACE") == "1" {
				buildArgs = append(buildArgs, "-race")
			}
			buildArgs = append(buildArgs, ".")
			buildCtx, stopBuild := context.WithTimeout(t.Context(), 3*time.Minute)
			build := exec.CommandContext(buildCtx, "go", buildArgs...)
			build.Dir = filepath.Join(root, "samples", entry.name)
			build.Env = append(environment, "GOWORK=off")
			output, err := build.CombinedOutput()
			stopBuild()
			if err != nil {
				t.Fatalf("build %s: %v\n%s", entry.name, err, output)
			}

			timeout := entry.timeout
			if timeout == 0 {
				timeout = 2 * time.Minute
			}
			runCtx, stopRun := context.WithTimeout(t.Context(), timeout)
			command := exec.CommandContext(runCtx, binary)
			command.Dir = build.Dir
			command.Env = environment
			command.Stdin = strings.NewReader(entry.input)
			output, err = command.CombinedOutput()
			stopRun()
			t.Logf("%s\n%s", entry.name, output)
			if err != nil {
				t.Fatalf("sample %s did not validate end to end: %v", entry.name, err)
			}
			if !strings.Contains(string(output), "SAMPLE_OK "+entry.name) {
				t.Fatalf("%s exited without its validation receipt", entry.name)
			}
			if entry.name == "replayanalysis" {
				checkReplayAnalysis(t, root)
			}
		})
	}
}

func sampleEnvironment(t *testing.T, group string) []string {
	t.Helper()
	connectionVariable := "DTS_CONNECTION_STRING"
	switch group {
	case "azure":
		connectionVariable = "DTS_SAMPLES_AZURE_CONNECTION_STRING"
	case "admin":
		connectionVariable = "DTS_SAMPLES_ADMIN_CONNECTION_STRING"
		if requiredEnvironment(t, "DTS_SAMPLE_ALLOW_HUB_MAINTENANCE") != "1" {
			t.Fatal("admin E2E requires explicit hub-maintenance acknowledgement")
		}
	case "telemetry":
		requiredEnvironment(t, "OTEL_EXPORTER_OTLP_ENDPOINT")
		requiredEnvironment(t, "OTEL_CAPTURE_FILE")
	}
	environment := append(os.Environ(), "DTS_CONNECTION_STRING="+requiredEnvironment(t, connectionVariable))
	if group == "storage" {
		if requiredEnvironment(t, "DTS_SAMPLE_ISOLATED_TASKHUB") != "1" {
			t.Fatal("storage E2E includes whole-window history export and requires an isolated task hub")
		}
		connection := os.Getenv("AZURE_STORAGE_CONNECTION_STRING")
		if connection == "" {
			connection = requiredEnvironment(t, "EXPORT_STORAGE_CONNECTION_STRING")
		}
		environment = append(environment, "AZURE_STORAGE_CONNECTION_STRING="+connection,
			"EXPORT_STORAGE_CONNECTION_STRING="+connection)
	}
	return environment
}

func requiredEnvironment(t *testing.T, name string) string {
	t.Helper()
	value := os.Getenv(name)
	if value == "" {
		t.Fatalf("%s is required for the selected E2E group; missing configuration is not a pass", name)
	}
	return value
}

func checkReplayAnalysis(t *testing.T, root string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	defer cancel()
	tool := filepath.Join(t.TempDir(), "orchestratorvet")
	if runtime.GOOS == "windows" {
		tool += ".exe"
	}
	build := exec.CommandContext(ctx, "go", "build", "-mod=readonly", "-o", tool, ".")
	build.Dir = filepath.Join(root, "cmd", "orchestratorvet")
	build.Env = append(os.Environ(), "GOWORK=off")
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build replay analyzer: %v\n%s", err, output)
	}
	good := exec.CommandContext(ctx, "go", "vet", "-vettool="+tool, "./samples/replayanalysis")
	good.Dir = root
	if output, err := good.CombinedOutput(); err != nil {
		t.Fatalf("safe replay sample has diagnostics: %v\n%s", err, output)
	}
	bad := exec.CommandContext(ctx, "go", "vet", "-vettool="+tool, "./samples/replayanalysis/testdata/bad")
	bad.Dir = root
	output, err := bad.CombinedOutput()
	if err == nil || !strings.Contains(string(output), "not deterministic") {
		t.Fatalf("unsafe fixture must produce a replay diagnostic, got %v\n%s", err, output)
	}
}
