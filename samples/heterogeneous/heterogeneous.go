package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
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

type Config struct {
	routes          map[string]string
	defaultExecutor string
}

func (c *Config) resolve(taskName string) string {
	executorName, ok := c.routes[taskName]
	if !ok {
		executorName = c.defaultExecutor
	}
	return executorName
}

type RoutingExecutor struct {
	config    Config
	executors map[string]backend.Executor
}

func (e *RoutingExecutor) getExecutor(taskName string) (backend.Executor, error) {
	executorName := e.config.resolve(taskName)
	executor, ok := e.executors[executorName]
	if !ok {
		return nil, fmt.Errorf("executor %s for task %s not found", executorName, taskName)
	}
	return executor, nil
}

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

func (e *RoutingExecutor) ExecuteOrchestrator(ctx context.Context, id api.InstanceID, oldEvents []*backend.HistoryEvent, newEvents []*backend.HistoryEvent) (*backend.ExecutionResults, error) {
	name := e.getOrchestratorName(oldEvents, newEvents)

	executor, err := e.getExecutor(name)
	if err != nil {
		return nil, err
	}

	return executor.ExecuteOrchestrator(ctx, id, oldEvents, newEvents)
}

func (e *RoutingExecutor) ExecuteActivity(ctx context.Context, id api.InstanceID, event *backend.HistoryEvent) (*backend.HistoryEvent, error) {
	name := event.GetTaskScheduled().GetName()
	executor, err := e.getExecutor(name)
	if err != nil {
		return nil, err
	}
	return executor.ExecuteActivity(ctx, id, event)
}

func (e *RoutingExecutor) Shutdown(ctx context.Context) error {
	ch := make(chan error)
	defer close(ch)
	for _, executor := range e.executors {
		go func() {
			err := executor.Shutdown(ctx)
			if err != nil {
				ch <- err
			}
		}()
	}
	var errs []error
	for err := range ch {
		errs = append(errs, err)
	}

	if len(errs) == 0 {
		return nil
	}

	return errors.Join(errs...)
}

func main() {

	ctx, cancelFunc := context.WithCancel(context.Background())
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

	// init a local executor register double and triple activity
	localExecutor, err := func() (backend.Executor, error) {
		r := task.NewTaskRegistry()
		if err := r.AddActivityN(doubleActivityName, doubleActivity); err != nil {
			return nil, err
		}

		if err := r.AddActivityN(tripleActivityName, tripleActivity); err != nil {
			return nil, err
		}

		executor := task.NewTaskExecutor(r)
		return executor, nil
	}()
	if err != nil {
		return nil, err
	}

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

	// init a grpc executor register the orchestrator, and connect a worker to the grpc executor
	grpcExecutor, err := func() (backend.Executor, error) {
		address := "localhost:4001"
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

		// create a worker connects to the grpc server
		var wg sync.WaitGroup
		wg.Add(1)

		go func() {
			// wait for grpc started
			time.Sleep(time.Second)
			conn, err := grpc.Dial(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				panic(err)
			}

			workerClient := client.NewTaskHubGrpcClient(conn, logger)
			r := task.NewTaskRegistry()
			if err := r.AddOrchestratorN(orchestratorName, orchestrator); err != nil {
				panic(err)
			}
			wg.Done()

			if err := workerClient.StartWorkItemListener(ctx, r); err != nil {
				panic(err)
			}
		}()

		wg.Wait()

		return executor, nil
	}()
	if err != nil {
		return nil, err
	}

	executor := &RoutingExecutor{
		config: Config{
			routes: map[string]string{
				doubleActivityName: localExecutorName,
				tripleActivityName: localExecutorName,
			},
			defaultExecutor: grpcExecutorName,
		},
		executors: map[string]backend.Executor{
			localExecutorName: localExecutor,
			grpcExecutorName:  grpcExecutor,
		},
	}

	return executor, nil
}
