package samples

import (
	"context"
	"fmt"
	"log"
	"time"

	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/zipkin"
	"go.opentelemetry.io/otel/sdk/resource"
	"go.opentelemetry.io/otel/sdk/trace"

	"github.com/microsoft/durabletask-go/task"
)

func RunDistributedTracingSample() {
	// Tracing can be configured independently of the orchestration code.
	tp := ConfigureZipkinTracing()
	defer func() {
		if err := tp.Shutdown(context.Background()); err != nil {
			log.Fatal(err)
		}
	}()

	// Set up and run the orchestration, which will automatically discover the zipkin configuration.
	r := task.NewTaskRegistry()
	r.AddOrchestrator(DistributedTraceSampleOrchestrator)
	r.AddActivity(DoWorkActivity)
	r.AddActivity(CallHttpEndpointActivity)

	ctx := context.Background()
	client, worker := Init(ctx, r)
	defer worker.Shutdown(ctx)

	id, err := client.ScheduleNewOrchestration(ctx, DistributedTraceSampleOrchestrator)
	if err != nil {
		panic(err)
	}
	metadata, err := client.WaitForOrchestrationCompletion(ctx, id)
	if err != nil {
		panic(err)
	}
	fmt.Printf("orchestration completed: %v\n", metadata)
}

func ConfigureZipkinTracing() *trace.TracerProvider {
	// Inspired by this sample: https://github.com/open-telemetry/opentelemetry-go/blob/main/example/zipkin/main.go
	exp, err := zipkin.New("http://localhost:9411/api/v2/spans")
	if err != nil {
		log.Fatal(err)
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
	return tp
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

	// Simulate doing work
	time.Sleep(duration)
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
