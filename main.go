package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"

	"github.com/microsoft/durabletask-go/backend"
	"github.com/microsoft/durabletask-go/backend/sqlite"
	"google.golang.org/grpc"
)

var port = flag.Int("port", 4001, "The server port")
var dbFilePath = flag.String("db", "taskhub.sqlite3", "The path to the sqlite file to use (or create if not exists)")
var ctx = context.Background()

func main() {
	// Parse command-line arguments
	flag.Parse()

	grpcServer := grpc.NewServer()
	grpcExecutorFunc := func(be backend.Backend) backend.Executor {
		return backend.NewGrpcExecutor(grpcServer, be)
	}
	configureFunc := func(opts *sqlite.SqliteOptions) {
		opts.FilePath = *dbFilePath
	}

	worker := sqlite.NewTaskHubWorker(grpcExecutorFunc, configureFunc)
	if err := worker.Start(ctx); err != nil {
		log.Fatalf("failed to start worker: %v", err)
	}

	lis, err := net.Listen("tcp", fmt.Sprintf("localhost:%d", *port))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	log.Printf("server listening at %v", lis.Addr())
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
