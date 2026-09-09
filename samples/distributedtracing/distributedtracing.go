// Command distributedtracing starts an application caller span, propagates its
// W3C trace context into Durable Task Scheduler, and exports application-process
// spans to a local OpenTelemetry collector over OTLP/HTTP. DTS emits orchestration,
// activity, and timer telemetry service-side.
//
//	export DTS_CONNECTION_STRING="Endpoint=http://localhost:8080;TaskHub=default;Authentication=None"
//	cd samples/distributedtracing && go run .
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"

	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	oteltrace "go.opentelemetry.io/otel/trace"

	"github.com/microsoft/durabletask-go/samples/internal/dtssample"
	"github.com/microsoft/durabletask-go/task"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	// Tracing can be configured independently of the orchestration code.
	tp, err := ConfigureOTLPTracing(context.Background())
	if err != nil {
		return fmt.Errorf("failed to create tracer: %w", err)
	}
	defer func() {
		if err := tp.Shutdown(context.Background()); err != nil {
			log.Printf("Failed to stop tracer: %v", err)
		}
	}()

	// Create a new task registry and add the orchestrator and activities
	r := task.NewTaskRegistry()
	if err := r.AddOrchestratorN("DistributedTraceSampleOrchestrator", DistributedTraceSampleOrchestrator); err != nil {
		return fmt.Errorf("failed to register orchestrator: %w", err)
	}
	if err := r.AddActivityN("DoWorkActivity", DoWorkActivity); err != nil {
		return fmt.Errorf("failed to register activity: %w", err)
	}
	if err := r.AddActivityN("CallHttpEndpointActivity", CallHttpEndpointActivity); err != nil {
		return fmt.Errorf("failed to register activity: %w", err)
	}

	// Connect a client and worker to the Durable Task Scheduler task hub
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	app, err := dtssample.Start(ctx, r)
	if err != nil {
		return err
	}
	defer func() {
		if err := app.Shutdown(); err != nil {
			log.Printf("Failed to shut down: %v", err)
		}
	}()

	// A sampled caller span is propagated to DTS when the orchestration is
	// scheduled, allowing service-side spans to join the application's trace.
	callerCtx, callerSpan := otel.Tracer("durabletask-sample").Start(
		ctx,
		"schedule_distributed_trace_sample",
		oteltrace.WithSpanKind(oteltrace.SpanKindClient),
	)
	defer callerSpan.End()
	id, err := app.Client.ScheduleNewOrchestration(callerCtx, "DistributedTraceSampleOrchestrator")
	if err != nil {
		return fmt.Errorf("failed to schedule new orchestration: %w", err)
	}

	// Wait for the orchestration to complete
	metadata, err := app.Client.WaitForOrchestrationCompletion(callerCtx, id)
	if err != nil {
		return fmt.Errorf("failed to wait for orchestration to complete: %w", err)
	}

	// Print the results
	metadataEnc, err := json.MarshalIndent(metadata, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to encode result to JSON: %w", err)
	}
	log.Printf("Orchestration completed: %v", string(metadataEnc))
	return nil
}

func ConfigureOTLPTracing(ctx context.Context) (*sdktrace.TracerProvider, error) {
	exp, err := otlptracehttp.New(
		ctx,
		otlptracehttp.WithEndpoint("localhost:4318"),
		otlptracehttp.WithInsecure(),
	)
	if err != nil {
		return nil, err
	}

	// NOTE: The simple span processor is not recommended for production.
	//       Instead, the batch span processor should be used for production.
	processor := sdktrace.NewSimpleSpanProcessor(exp)
	// processor := sdktrace.NewBatchSpanProcessor(exp)

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithSpanProcessor(processor),
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
		sdktrace.WithResource(resource.NewWithAttributes(
			"durabletask.io",
			attribute.KeyValue{Key: "service.name", Value: attribute.StringValue("sample-app")},
		)),
	)
	otel.SetTracerProvider(tp)
	return tp, nil
}

// DistributedTraceSampleOrchestrator is a simple orchestration that's intended to generate
// distributed trace output to the configured exporter.
func DistributedTraceSampleOrchestrator(ctx *task.OrchestrationContext) (any, error) {
	if err := ctx.CallActivity("DoWorkActivity", task.WithActivityInput(1*time.Second)).Await(nil); err != nil {
		return nil, err
	}
	if err := ctx.CreateTimer(2 * time.Second).Await(nil); err != nil {
		return nil, err
	}
	if err := ctx.CallActivity("CallHttpEndpointActivity", task.WithActivityInput("https://bing.com")).Await(nil); err != nil {
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

	// The OTel HTTP client records the outbound request in the worker process.
	// DTS owns the service-side activity span.
	req, err := http.NewRequestWithContext(ctx.Context(), http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	httpClient := &http.Client{
		Transport: otelhttp.NewTransport(http.DefaultTransport),
	}
	resp, err := httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err := resp.Body.Close(); err != nil {
			log.Printf("Failed to close HTTP response body: %v", err)
		}
	}()
	return nil, nil
}
