// Package externalio exercises the filesystem, network, process, and
// environment checks.
package externalio

import (
	"net"
	"net/http"
	"os"
	"os/exec"
	"syscall"

	"github.com/microsoft/durabletask-go/task"
)

func filesystem(ctx *task.OrchestrationContext) (any, error) {
	data, err := os.ReadFile("config.json") // want `os\.ReadFile performs external I/O`
	if err != nil {
		return nil, err
	}
	if err := os.WriteFile("out.json", data, 0o600); err != nil { // want `os\.WriteFile performs external I/O`
		return nil, err
	}
	handle, err := os.Open("config.json") // want `os\.Open performs external I/O`
	if err != nil {
		return nil, err
	}
	defer handle.Close()
	if err := os.MkdirAll("cache", 0o700); err != nil { // want `os\.MkdirAll performs external I/O`
		return nil, err
	}
	return os.Remove("out.json"), nil // want `os\.Remove performs external I/O`
}

func environment(ctx *task.OrchestrationContext) (any, error) {
	region := os.Getenv("REGION")          // want `os\.Getenv performs external I/O`
	if _, ok := os.LookupEnv("TIER"); ok { // want `os\.LookupEnv performs external I/O`
		_ = os.Setenv("TIER", "gold") // want `os\.Setenv performs external I/O`
	}
	_ = os.Environ() // want `os\.Environ performs external I/O`
	return region, nil
}

func network(ctx *task.OrchestrationContext) (any, error) {
	response, err := http.Get("https://example.invalid") // want `http\.Get performs external I/O`
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()

	client := &http.Client{}
	request, err := http.NewRequest(http.MethodGet, "https://example.invalid", nil)
	if err != nil {
		return nil, err
	}
	direct, err := client.Do(request) // want `\(\*http\.Client\)\.Do performs external I/O`
	if err != nil {
		return nil, err
	}
	defer direct.Body.Close()

	connection, err := net.Dial("tcp", "example.invalid:80") // want `net\.Dial performs external I/O`
	if err != nil {
		return nil, err
	}
	defer connection.Close()

	addresses, err := net.LookupHost("example.invalid") // want `net\.LookupHost performs external I/O`
	if err != nil {
		return nil, err
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0") // want `net\.Listen performs external I/O`
	if err != nil {
		return nil, err
	}
	defer listener.Close()
	return addresses, nil
}

func process(ctx *task.OrchestrationContext) (any, error) {
	command := exec.Command("echo", "hello") // want `exec\.Command performs external I/O`
	output, err := command.Output()          // want `\(\*exec\.Cmd\)\.Output performs external I/O`
	if err != nil {
		return nil, err
	}
	_ = syscall.Getpid() // want `syscall\.Getpid performs external I/O`
	return string(output), nil
}

// durableIO routes every side effect through an activity.
func durableIO(ctx *task.OrchestrationContext) (any, error) {
	var result string
	if err := ctx.CallActivity("readConfig").Await(&result); err != nil {
		return nil, err
	}
	if err := ctx.CallActivity("callService", task.WithActivityInput(result)).Await(nil); err != nil {
		return nil, err
	}
	return result, nil
}

func register() {
	registry := task.NewTaskRegistry()
	_ = registry.AddOrchestrator(filesystem)
	_ = registry.AddOrchestrator(environment)
	_ = registry.AddOrchestrator(network)
	_ = registry.AddOrchestrator(process)
	_ = registry.AddOrchestrator(durableIO)
}
