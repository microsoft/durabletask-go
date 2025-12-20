package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"time"

	"github.com/microsoft/durabletask-go/api"
	"github.com/microsoft/durabletask-go/backend"
	"github.com/microsoft/durabletask-go/backend/sqlite"
	"github.com/microsoft/durabletask-go/client"
	"github.com/microsoft/durabletask-go/task"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	doubleActivityName = "double"
	tripleActivityName = "triple"
	orchestratorName   = "sixTimes"

	localExecutorName = "local"
	grpcExecutorName  = "grpc"
)

// Config defines the routing configuration for tasks.
// It maps task names to executor names and specifies a default executor.
type Config struct {
	Routes          map[string]string
	DefaultExecutor string
}

// Resolve returns the executor name for a given task name.
// If no specific route is found, it returns the default executor name.
func (c *Config) Resolve(taskName string) string {
	executorName, ok := c.Routes[taskName]
	if !ok {
		executorName = c.DefaultExecutor
	}
	return executorName
}

// RoutingExecutor is a backend.Executor implementation that Routes tasks to different Executors
// based on a configuration. This allows for heterogeneous execution environments where
// different tasks (orchestrators or activities) can be executed by different Executors
// (e.g., local vs gRPC).
type RoutingExecutor struct {
	Config    Config
	Executors map[string]backend.Executor
}

// getExecutor retrieves the appropriate backend.Executor for a given task name
// based on the routing configuration.
func (e *RoutingExecutor) getExecutor(taskName string) (backend.Executor, error) {
	executorName := e.Config.Resolve(taskName)
	executor, ok := e.Executors[executorName]
	if !ok {
		return nil, fmt.Errorf("executor %s for task %s not found", executorName, taskName)
	}
	return executor, nil
}

// getOrchestratorName extracts the orchestrator name from the provided history events.
// It iterates through both old and new events to find the ExecutionStarted event,
// which contains the orchestrator's name. This is crucial for routing orchestrations
// to the correct executor based on their name.
func (e *RoutingExecutor) getOrchestratorName(oldEvents, newEvents []*backend.HistoryEvent) string {

	for _, event := range oldEvents {
		if x := event.GetExecutionStarted(); x != nil {
			return x.Name
		}
	}
	for _, event := range newEvents {
		if x := event.GetExecutionStarted(); x != nil {
			return x.Name
		}
	}
	return ""
}

// ExecuteOrchestrator Routes the orchestration execution request to the appropriate executor.
// It determines the orchestrator name from history events and uses it to Resolve the executor.
func (e *RoutingExecutor) ExecuteOrchestrator(ctx context.Context, id api.InstanceID, oldEvents []*backend.HistoryEvent, newEvents []*backend.HistoryEvent) (*backend.ExecutionResults, error) {
	name := e.getOrchestratorName(oldEvents, newEvents)

	executor, err := e.getExecutor(name)
	if err != nil {
		return nil, err
	}

	return executor.ExecuteOrchestrator(ctx, id, oldEvents, newEvents)
}

// ExecuteActivity Routes the activity execution request to the appropriate executor.
// It extracts the activity name from the task scheduled event and uses it to Resolve the executor.
func (e *RoutingExecutor) ExecuteActivity(ctx context.Context, id api.InstanceID, event *backend.HistoryEvent) (*backend.HistoryEvent, error) {
	name := event.GetTaskScheduled().GetName()
	executor, err := e.getExecutor(name)
	if err != nil {
		return nil, err
	}
	return executor.ExecuteActivity(ctx, id, event)
}

// Shutdown shuts down all registered Executors in parallel.
// It returns a joined error if any of the Executors fail to shut down.
func (e *RoutingExecutor) Shutdown(ctx context.Context) error {
	var errs []error
	for _, executor := range e.Executors {
		err := executor.Shutdown(ctx)
		if err != nil {
			errs = append(errs, err)
		}
	}

	if len(errs) == 0 {
		return nil
	}

	return errors.Join(errs...)
}

func main() {

	ctx, cancelFunc := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelFunc()

	logger := backend.DefaultLogger()
	be := sqlite.NewSqliteBackend(sqlite.NewSqliteOptions(""), logger)

	executor, err := initExecutor(ctx, be, logger)
	if err != nil {
		panic(err)
	}

	workflowWorker := backend.NewOrchestrationWorker(be, executor, logger)
	activityWorker := backend.NewActivityTaskWorker(be, executor, logger)
	taskHubWorker := backend.NewTaskHubWorker(be, workflowWorker, activityWorker, logger)

	if err := taskHubWorker.Start(ctx); err != nil {
		panic(err)
	}
	defer taskHubWorker.Shutdown(ctx)

	taskHubClient := backend.NewTaskHubClient(be)
	id, err := taskHubClient.ScheduleNewOrchestration(ctx, orchestratorName, api.WithInput(1))
	if err != nil {
		panic(err)
	}
	metadata, err := taskHubClient.WaitForOrchestrationCompletion(ctx, id)
	if err != nil {
		panic(err)
	}
	fmt.Println(metadata.SerializedOutput)
}

func initExecutor(ctx context.Context, be backend.Backend, logger backend.Logger) (backend.Executor, error) {
	localExecutor, err := setupLocalExecutor()
	if err != nil {
		return nil, err
	}

	grpcExecutor, err := setupGrpcExecutor(ctx, be, logger)
	if err != nil {
		return nil, err
	}

	return &RoutingExecutor{
		Config: Config{
			Routes: map[string]string{
				doubleActivityName: localExecutorName,
				tripleActivityName: localExecutorName,
			},
			DefaultExecutor: grpcExecutorName,
		},
		Executors: map[string]backend.Executor{
			localExecutorName: localExecutor,
			grpcExecutorName:  grpcExecutor,
		},
	}, nil
}

func setupLocalExecutor() (backend.Executor, error) {
	timesActivity := func(times int) task.Activity {
		return func(ctx task.ActivityContext) (any, error) {
			var input int
			if err := ctx.GetInput(&input); err != nil {
				return nil, err
			}
			return input * times, nil
		}
	}
	doubleActivity := timesActivity(2)
	tripleActivity := timesActivity(3)

	r := task.NewTaskRegistry()
	if err := r.AddActivityN(doubleActivityName, doubleActivity); err != nil {
		return nil, err
	}

	if err := r.AddActivityN(tripleActivityName, tripleActivity); err != nil {
		return nil, err
	}

	return task.NewTaskExecutor(r), nil
}

func setupGrpcExecutor(ctx context.Context, be backend.Backend, logger backend.Logger) (backend.Executor, error) {
	address := "localhost:0"
	grpcServer := grpc.NewServer()
	executor, registerFn := backend.NewGrpcExecutor(be, logger)
	registerFn(grpcServer)

	lis, err := net.Listen("tcp", address)
	if err != nil {
		return nil, err
	}
	go func() {
		if err := grpcServer.Serve(lis); err != nil {
			panic(err)
		}
	}()

	go func() {
		<-ctx.Done()
		grpcServer.GracefulStop()
		_ = lis.Close()
	}()

	// Create a worker that connects to the gRPC server.
	// establish a gRPC connection, blocking until the server is ready or the timeout expires
	conn, err := grpc.DialContext(
		ctx,
		lis.Addr().String(),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	)
	if err != nil {
		return nil, err
	}

	go func() {
		<-ctx.Done()
		_ = conn.Close()
	}()

	orchestrator := func(ctx *task.OrchestrationContext) (any, error) {
		var input int
		if err := ctx.GetInput(&input); err != nil {
			return nil, err
		}

		var intermediateResult int
		if err := ctx.CallActivity(doubleActivityName, task.WithActivityInput(input)).Await(&intermediateResult); err != nil {
			return nil, err
		}

		var finalResult int
		if err := ctx.CallActivity(tripleActivityName, task.WithActivityInput(intermediateResult)).Await(&finalResult); err != nil {
			return nil, err
		}
		return finalResult, nil
	}

	workerClient := client.NewTaskHubGrpcClient(conn, logger)
	r := task.NewTaskRegistry()
	if err := r.AddOrchestratorN(orchestratorName, orchestrator); err != nil {
		return nil, err
	}

	// StartWorkItemListener is not blocking
	if err := workerClient.StartWorkItemListener(ctx, r); err != nil {
		return nil, err
	}

	return executor, nil
}
