package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"github.com/microsoft/durabletask-go/backend"
	"github.com/microsoft/durabletask-go/backend/sqlite"
	"github.com/microsoft/durabletask-go/task"
)

type employee struct {
	Id        string `json:"id"`
	FirstName string `json:"first_name"`
	LastName  string `json:"last_name"`
	Age       int    `json:"age"`
}

var employees = []employee{
	{"1", "John", "Doe", 30},
	{"2", "Jane", "Doe", 25},
	{"3", "Steven", "Smith", 40},
	{"4", "Lily", "Smith", 35},
}

func main() {
	// Create a new task registry and add the orchestrator and activities
	r := task.NewTaskRegistry()
	r.AddOrchestrator(ActivitySequenceOrchestrator)
	r.AddActivity(GetEmployeeDetailById)

	// Init the client
	ctx := context.Background()
	logger := backend.DefaultLogger()
	be := getBackend(logger)
	client, worker, err := Init(ctx, r, be, logger)
	if err != nil {
		log.Fatalf("Failed to initialize the client: %v", err)
	}
	defer worker.Shutdown(ctx)

	// Start a new orchestration
	id, err := client.ScheduleNewOrchestration(ctx, ActivitySequenceOrchestrator)
	if err != nil {
		log.Fatalf("Failed to schedule new orchestration: %v", err)
	}

	// Wait for the orchestration to complete
	metadata, err := client.WaitForOrchestrationCompletion(ctx, id)
	if err != nil {
		log.Fatalf("Failed to wait for orchestration to complete: %v", err)
	}
	var emps []employee
	json.Unmarshal([]byte(metadata.SerializedOutput), &emps)
	// Print the results
	log.Printf("Orchestration completed: %v", emps)
	be.DeleteTaskHub(ctx)

}

func getBackend(logger backend.Logger) backend.Backend {
	// Use the in-memory sqlite provider by specifying ""
	return sqlite.NewSqliteBackend(sqlite.NewSqliteOptions("http://localhost:8080"), logger)
}

// Init creates and initializes an in-memory client and worker pair with default configuration.
func Init(ctx context.Context, r *task.TaskRegistry, be backend.Backend, logger backend.Logger) (backend.TaskHubClient, backend.TaskHubWorker, error) {

	// Create an executor
	executor := task.NewTaskExecutor(r)

	// Create a new backend

	orchestrationWorker := backend.NewOrchestrationWorker(be, executor, logger)
	activityWorker := backend.NewActivityTaskWorker(be, executor, logger)
	taskHubWorker := backend.NewTaskHubWorker(be, orchestrationWorker, activityWorker, logger)

	// Start the worker
	err := taskHubWorker.Start(ctx)
	if err != nil {
		return nil, nil, err
	}

	// Get the client to the backend
	taskHubClient := backend.NewTaskHubClient(be)

	return taskHubClient, taskHubWorker, nil
}

// ActivitySequenceOrchestrator makes 2 activity calls in sequence and results the results
// as an array.
func ActivitySequenceOrchestrator(ctx *task.OrchestrationContext) (any, error) {
	var john employee
	if err := ctx.CallActivity(GetEmployeeDetailById, task.WithActivityInput("1")).Await(&john); err != nil {
		return nil, err
	}
	var lily employee
	if err := ctx.CallActivity(GetEmployeeDetailById, task.WithActivityInput("4")).Await(&lily); err != nil {
		return nil, err
	}

	return []employee{john, lily}, nil
}

// GetEmployeeDetailById can be called by an orchestrator function and will return a friendly greeting.
func GetEmployeeDetailById(ctx task.ActivityContext) (any, error) {
	var id string
	if err := ctx.GetInput(&id); err != nil {
		return nil, err
	}

	for i, e := range employees {
		if e.Id == id {
			return &employees[i], nil
		}
	}
	return nil, fmt.Errorf("employee not found")

}
