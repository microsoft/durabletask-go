package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/zipkin"
	"go.opentelemetry.io/otel/sdk/resource"
	"go.opentelemetry.io/otel/sdk/trace"
	"google.golang.org/grpc"

	"github.com/microsoft/durabletask-go/backend"
	"github.com/microsoft/durabletask-go/backend/sqlite"
)

var (
	port       = flag.Int("port", 4001, "The server port")
	dbFilePath = flag.String("db", "", "The path to the sqlite file to use (or create if not exists)")
	ctx        = context.Background()
)

func main() {
	// Parse command-line arguments
	flag.Parse()

	// Tracing can be configured independently of the orchestration code.
	tp, err := ConfigureZipkinTracing()
	if err != nil {
		log.Fatalf("Failed to create tracer: %v", err)
	}
	defer func() {
		if err := tp.Shutdown(context.Background()); err != nil {
			log.Fatalf("Failed to stop tracer: %v", err)
		}
	}()

	grpcServer := grpc.NewServer()
	worker := createTaskHubWorker(grpcServer, *dbFilePath, backend.DefaultLogger())
	if err := worker.Start(ctx); err != nil {
		log.Fatalf("failed to start worker: %v", err)
	}

	lis, err := net.Listen("tcp", fmt.Sprintf("localhost:%d", *port))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	fmt.Printf("server listening at %v\n", lis.Addr())
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}

func ConfigureZipkinTracing() (*trace.TracerProvider, error) {
	// Inspired by this sample: https://github.com/open-telemetry/opentelemetry-go/blob/main/example/zipkin/main.go
	exp, err := zipkin.New("http://localhost:9411/api/v2/spans")
	if err != nil {
		return nil, err
	}

	// NOTE: The simple span processor is not recommended for production.
	//       Instead, the batch span processor should be used for production.
	processor := trace.NewSimpleSpanProcessor(exp)
	// processor := trace.NewBatchSpanProcessor(exp)

	tp := trace.NewTracerProvider(
		trace.WithSpanProcessor(processor),
		trace.WithSampler(trace.AlwaysSample()),
		trace.WithResource(resource.NewWithAttributes(
			"durabletask.io",
			attribute.KeyValue{Key: "service.name", Value: attribute.StringValue("sample-app")},
		)),
	)
	otel.SetTracerProvider(tp)
	return tp, nil
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
