package tests_grpc

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"testing"
	"time"

	"github.com/microsoft/durabletask-go/api"
	"github.com/microsoft/durabletask-go/backend"
	"github.com/microsoft/durabletask-go/backend/sqlite"
	"github.com/microsoft/durabletask-go/client"
	"github.com/microsoft/durabletask-go/task"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var (
	grpcClient *client.TaskHubGrpcClient
	ctx        = context.Background()
)

// TestMain is the entry point for the test suite. We use this to set up a gRPC server and client instance
// which are used by all tests in the suite.
func TestMain(m *testing.M) {
	sqliteOptions := sqlite.NewSqliteOptions("")
	logger := backend.DefaultLogger()
	be := sqlite.NewSqliteBackend(sqliteOptions, logger)
	grpcServer := grpc.NewServer()
	grpcExecutor, registerFn := backend.NewGrpcExecutor(be, logger)
	registerFn(grpcServer)
	orchestrationWorker := backend.NewOrchestrationWorker(be, grpcExecutor, logger)
	activityWorker := backend.NewActivityTaskWorker(be, grpcExecutor, logger)
	taskHubWorker := backend.NewTaskHubWorker(be, orchestrationWorker, activityWorker, logger)
	if err := taskHubWorker.Start(ctx); err != nil {
		log.Fatalf("failed to start worker: %v", err)
	}

	lis, err := net.Listen("tcp", ":0")
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	go func() {
		if err := grpcServer.Serve(lis); err != nil {
			log.Fatalf("failed to serve: %v", err)
		}
		fmt.Printf("server listening at %v\n", lis.Addr())
	}()

	time.Sleep(1 * time.Second)

	conn, err := grpc.Dial(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("failed to connect to gRPC server: %v", err)
	}
	defer conn.Close()
	grpcClient = client.NewTaskHubGrpcClient(conn, logger)

	// Run the test exitCode
	exitCode := m.Run()

	timeoutCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	err = grpcExecutor.Shutdown(timeoutCtx)
	if err != nil {
		log.Fatalf("failed to shutdown grpc Executor: %v", err)
	}

	timeoutCtx, cancel = context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := taskHubWorker.Shutdown(timeoutCtx); err != nil {
		log.Fatalf("failed to shutdown worker: %v", err)
	}
	grpcServer.Stop()
	os.Exit(exitCode)
}

func startGrpcListener(t *testing.T, r *task.TaskRegistry) context.CancelFunc {
	cancelCtx, cancel := context.WithCancel(ctx)
	require.NoError(t, grpcClient.StartWorkItemListener(cancelCtx, r))
	return cancel
}

func Test_Grpc_WaitForInstanceStart_Timeout(t *testing.T) {
	r := task.NewTaskRegistry()
	r.AddOrchestratorN("WaitForInstanceStartThrowsException", func(ctx *task.OrchestrationContext) (any, error) {
		// sleep 5 seconds
		time.Sleep(5 * time.Second)
		return 20, nil
	})

	cancelListener := startGrpcListener(t, r)
	defer cancelListener()

	id, err := grpcClient.ScheduleNewOrchestration(ctx, "WaitForInstanceStartThrowsException", api.WithInput("世界"))
	require.NoError(t, err)
	timeoutCtx, cancelTimeout := context.WithTimeout(ctx, time.Second)
	defer cancelTimeout()
	_, err = grpcClient.WaitForOrchestrationStart(timeoutCtx, id, api.WithFetchPayloads(true))
	if assert.Error(t, err) {
		assert.Contains(t, err.Error(), "context deadline exceeded")
	}
	time.Sleep(1 * time.Second)
}

func Test_Grpc_HelloOrchestration(t *testing.T) {
	r := task.NewTaskRegistry()
	r.AddOrchestratorN("SingleActivity", func(ctx *task.OrchestrationContext) (any, error) {
		var input string
		if err := ctx.GetInput(&input); err != nil {
			return nil, err
		}
		var output string
		err := ctx.CallActivity("SayHello", task.WithActivityInput(input)).Await(&output)
		return output, err
	})
	r.AddActivityN("SayHello", func(ctx task.ActivityContext) (any, error) {
		var name string
		if err := ctx.GetInput(&name); err != nil {
			return nil, err
		}
		return fmt.Sprintf("Hello, %s!", name), nil
	})

	cancelListener := startGrpcListener(t, r)
	defer cancelListener()

	id, err := grpcClient.ScheduleNewOrchestration(ctx, "SingleActivity", api.WithInput("世界"))
	require.NoError(t, err)
	timeoutCtx, cancelTimeout := context.WithTimeout(ctx, 30*time.Second)
	defer cancelTimeout()
	metadata, err := grpcClient.WaitForOrchestrationCompletion(timeoutCtx, id, api.WithFetchPayloads(true))
	require.NoError(t, err)
	assert.Equal(t, true, metadata.IsComplete())
	assert.Equal(t, `"Hello, 世界!"`, metadata.SerializedOutput)
	time.Sleep(1 * time.Second)
}
