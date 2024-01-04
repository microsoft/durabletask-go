package main

import (
	"context"
	"encoding/json"
	"log"
	"time"

	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/zipkin"
	"go.opentelemetry.io/otel/sdk/resource"
	"go.opentelemetry.io/otel/sdk/trace"

	"github.com/microsoft/durabletask-go/backend"
	"github.com/microsoft/durabletask-go/backend/sqlite"
	"github.com/microsoft/durabletask-go/task"
)

var tracer = otel.Tracer("distributedtracing-example")

func main() {
	// Tracing can be configured independently of the orchestration code.
	tp, err := ConfigureZipkinTracing()
	if err != nil {
		log.Fatalf("Failed to create tracer: %w", err)
	}
	defer func() {
		if err := tp.Shutdown(context.Background()); err != nil {
			log.Fatalf("Failed to stop tracer: %v", err)
		}
	}()

	// Create a new task registry and add the orchestrator and activities
	r := task.NewTaskRegistry()
	r.AddOrchestrator(DistributedTraceSampleOrchestrator)
	r.AddActivity(DoWorkActivity)
	r.AddActivity(CallHttpEndpointActivity)

	// Init the client
	ctx := context.Background()
	client, worker, err := Init(ctx, r)
	if err != nil {
		log.Fatalf("Failed to initialize the client: %v", err)
	}
	defer worker.Shutdown(ctx)

	// Start a new orchestration
	id, err := client.ScheduleNewOrchestration(ctx, DistributedTraceSampleOrchestrator)
	if err != nil {
		log.Fatalf("Failed to schedule new orchestration: %v", err)
	}

	// Wait for the orchestration to complete
	metadata, err := client.WaitForOrchestrationCompletion(ctx, id)
	if err != nil {
		log.Fatalf("Failed to wait for orchestration to complete: %v", err)
	}

	// Print the results
	metadataEnc, err := json.MarshalIndent(metadata, "", "  ")
	if err != nil {
		log.Fatalf("Failed to encode result to JSON: %v", err)
	}
	log.Printf("Orchestration completed: %v", string(metadataEnc))
}

// Init creates and initializes an in-memory client and worker pair with default configuration.
func Init(ctx context.Context, r *task.TaskRegistry) (backend.TaskHubClient, backend.TaskHubWorker, error) {
	logger := backend.DefaultLogger()

	// Create an executor
	executor := task.NewTaskExecutor(r)

	// Create a new backend
	// Use the in-memory sqlite provider by specifying ""
	be := sqlite.NewSqliteBackend(sqlite.NewSqliteOptions(""), logger)
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

// DistributedTraceSampleOrchestrator is a simple orchestration that's intended to generate
// distributed trace output to the configured exporter (e.g. zipkin).
func DistributedTraceSampleOrchestrator(ctx *task.OrchestrationContext) (any, error) {
	if err := ctx.CallActivity(DoWorkActivity, task.WithActivityInput(1*time.Second)).Await(nil); err != nil {
		return nil, err
	}
	if err := ctx.CreateTimer(2 * time.Second).Await(nil); err != nil {
		return nil, err
	}
	if err := ctx.CallActivity(CallHttpEndpointActivity, task.WithActivityInput("https://bing.com")).Await(nil); err != nil {
		return nil, err
	}
	return nil, nil
}

// DoWorkActivity is a no-op activity function that sleeps for a specified amount of time.
func DoWorkActivity(ctx task.ActivityContext) (any, error) {
	var duration time.Duration
	if err := ctx.GetInput(&duration); err != nil {
		return "", err
	}

	_, childSpan := tracer.Start(ctx.Context(), "activity-subwork")
	// Simulate doing some sub work
	select {
	case <-time.After(2 * time.Second):
		// Ok
	case <-ctx.Context().Done():
		return nil, ctx.Context().Err()
	}
	childSpan.End()

	// Simulate doing work
	select {
	case <-time.After(duration):
		// Ok
	case <-ctx.Context().Done():
		return nil, ctx.Context().Err()
	}

	return nil, nil
}

func CallHttpEndpointActivity(ctx task.ActivityContext) (any, error) {
	var url string
	if err := ctx.GetInput(&url); err != nil {
		return "", err
	}

	// ActivityContext.Context() returns a context instrumented with span information.
	// The OTel HTTP client will use this to produce child spans accordingly.
	_, err := otelhttp.Get(ctx.Context(), url)
	if err != nil {
		return nil, err
	}
	return nil, nil
}
