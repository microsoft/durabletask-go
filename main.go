package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"

	"google.golang.org/grpc"

	"github.com/dapr/durabletask-go/backend"
	"github.com/dapr/durabletask-go/backend/sqlite"
)

var (
	port       = flag.Int("port", 4001, "The server port")
	host       = flag.String("host", "localhost", "The host to bind to")
	dbFilePath = flag.String("db", "", "The path to the sqlite file to use (or create if not exists)")
	ctx        = context.Background()
)

func main() {
	// Parse command-line arguments
	flag.Parse()

	grpcServer := grpc.NewServer()
	worker := createTaskHubWorker(grpcServer, *dbFilePath, backend.DefaultLogger())
	if err := worker.Start(ctx); err != nil {
		log.Fatalf("failed to start worker: %v", err)
	}

	lis, err := net.Listen("tcp", fmt.Sprintf("%s:%d", *host, *port))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	fmt.Printf("server listening at %v\n", lis.Addr())
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}

func createTaskHubWorker(server *grpc.Server, sqliteFilePath string, logger backend.Logger) backend.TaskHubWorker {
	sqliteOptions := sqlite.NewSqliteOptions(sqliteFilePath)
	be := sqlite.NewSqliteBackend(sqliteOptions, logger)
	executor, registerFn := backend.NewGrpcExecutor(be, logger)
	registerFn(server)
	orchestrationWorker := backend.NewOrchestrationWorker(be, executor, logger)
	activityWorker := backend.NewActivityTaskWorker(be, executor, logger)
	taskHubWorker := backend.NewTaskHubWorker(be, orchestrationWorker, activityWorker, logger)
	return taskHubWorker
}
