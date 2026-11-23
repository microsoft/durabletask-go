// Command distributedtracing exports replay-aware DTS worker and client spans
// alongside application spans to a real OTLP/HTTP collector.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"time"

	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.43.0"
	oteltrace "go.opentelemetry.io/otel/trace"

	"github.com/microsoft/durabletask-go/api"
	durabletaskclient "github.com/microsoft/durabletask-go/client"
	"github.com/microsoft/durabletask-go/durabletaskscheduler"
	"github.com/microsoft/durabletask-go/samples/internal/dtssample"
	"github.com/microsoft/durabletask-go/task"
	"github.com/microsoft/durabletask-go/tracing"
)

const callerSpanName = "schedule_distributed_trace_sample"

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
	fmt.Println("SAMPLE_OK distributedtracing")
}

func run() (err error) {
	traceFile := os.Getenv("OTEL_CAPTURE_FILE")
	if traceFile == "" {
		return errors.New("OTEL_CAPTURE_FILE is required so the sample can verify collector receipt")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	tp, err := ConfigureOTLPTracing(ctx)
	if err != nil {
		return fmt.Errorf("create tracer: %w", err)
	}
	defer func() { err = errors.Join(err, shutdownTracer(tp)) }()
	tracedTarget := httptest.NewServer(otelhttp.NewHandler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, "ok")
	}), "local_trace_target", otelhttp.WithSpanNameFormatter(func(string, *http.Request) string {
		return "local_trace_target"
	})))
	defer tracedTarget.Close()

	registry := task.NewTaskRegistry()
	if err := registry.AddOrchestratorN("DistributedTraceSampleOrchestrator", DistributedTraceSampleOrchestrator); err != nil {
		return fmt.Errorf("register orchestrator: %w", err)
	}
	if err := registry.AddActivityN("DoWorkActivity", DoWorkActivity); err != nil {
		return fmt.Errorf("register activity: %w", err)
	}
	if err := registry.AddActivityN("CallHttpEndpointActivity", CallHttpEndpointActivity); err != nil {
		return fmt.Errorf("register activity: %w", err)
	}

	options, err := dtssample.Options()
	if err != nil {
		return err
	}
	managementClient, err := durabletaskscheduler.NewClient(ctx, options, api.DefaultLogger(),
		durabletaskclient.WithTracerProvider(tp))
	if err != nil {
		return fmt.Errorf("create client: %w", err)
	}
	defer func() { err = errors.Join(err, managementClient.Close()) }()
	worker, err := durabletaskscheduler.NewWorker(options, registry, api.DefaultLogger(),
		durabletaskclient.WithAutoWorkItemFilters(),
		durabletaskclient.WithWorkerTracerProvider(tp))
	if err != nil {
		return fmt.Errorf("create worker: %w", err)
	}
	if err := worker.Start(ctx); err != nil {
		return fmt.Errorf("start worker: %w", err)
	}
	shutdownWorker := func() error {
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer shutdownCancel()
		return worker.Shutdown(shutdownCtx)
	}
	var ownedIDs []api.InstanceID
	cleanupDone := false
	defer func() {
		if !cleanupDone {
			err = errors.Join(err, dtssample.Cleanup(managementClient, ownedIDs...), shutdownWorker())
		}
	}()

	callerCtx, callerSpan := otel.Tracer("durabletask-sample").Start(
		ctx,
		callerSpanName,
		oteltrace.WithSpanKind(oteltrace.SpanKindClient),
	)
	traceID := callerSpan.SpanContext().TraceID().String()
	instanceID := dtssample.NewInstanceID("distributedtracing")
	ownedIDs = append(ownedIDs, instanceID)
	if _, err := managementClient.ScheduleNewOrchestration(callerCtx, "DistributedTraceSampleOrchestrator",
		api.WithInstanceID(instanceID),
		api.WithInput(tracedTarget.URL),
		api.WithTags(map[string]string{"sample": "distributedtracing"})); err != nil {
		callerSpan.End()
		return fmt.Errorf("schedule orchestration: %w", err)
	}
	callerSpan.End()

	metadata, err := managementClient.WaitForOrchestrationCompletion(ctx, instanceID, api.WithFetchPayloads(true))
	if err != nil {
		return fmt.Errorf("wait for orchestration: %w", err)
	}
	if err := dtssample.RequireCompleted(metadata); err != nil {
		return err
	}
	history, err := managementClient.GetOrchestrationHistory(ctx, instanceID, api.HistoryQuery{ExecutionID: metadata.ExecutionID})
	if err != nil {
		return fmt.Errorf("read orchestration history: %w", err)
	}
	if err := requireHistoryTraceContext(history.Events, traceID); err != nil {
		return err
	}
	metadataEnc, err := json.MarshalIndent(metadata, "", "  ")
	if err != nil {
		return fmt.Errorf("encode result to JSON: %w", err)
	}
	log.Printf("Orchestration completed: %s", metadataEnc)

	cleanupErr := dtssample.Cleanup(managementClient, ownedIDs...)
	workerErr := shutdownWorker()
	flushCtx, flushCancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer flushCancel()
	traceErr := tp.ForceFlush(flushCtx)
	cleanupDone = true
	receiptCtx, receiptCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer receiptCancel()
	receiptErr := waitForCollectorReceipt(receiptCtx, traceFile, traceID, []expectedSpan{
		{name: callerSpanName, kind: oteltrace.SpanKindClient},
		{name: "create_orchestration:DistributedTraceSampleOrchestrator", kind: oteltrace.SpanKindProducer},
		{name: "orchestration:DistributedTraceSampleOrchestrator", kind: oteltrace.SpanKindServer},
		{name: "activity:DoWorkActivity", kind: oteltrace.SpanKindClient},
		{name: "activity:DoWorkActivity", kind: oteltrace.SpanKindServer},
		{name: "orchestration:DistributedTraceSampleOrchestrator:timer", kind: oteltrace.SpanKindInternal},
		{name: "activity:CallHttpEndpointActivity", kind: oteltrace.SpanKindClient},
		{name: "activity:CallHttpEndpointActivity", kind: oteltrace.SpanKindServer},
		{name: "local_trace_target", kind: oteltrace.SpanKindServer},
	})
	return errors.Join(cleanupErr, workerErr, traceErr, receiptErr)
}

func ConfigureOTLPTracing(ctx context.Context) (*sdktrace.TracerProvider, error) {
	options := []otlptracehttp.Option{otlptracehttp.WithTimeout(10 * time.Second)}
	if os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT") == "" && os.Getenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT") == "" {
		options = append(options, otlptracehttp.WithEndpointURL("http://localhost:4318/v1/traces"))
	}
	exp, err := otlptracehttp.New(ctx, options...)
	if err != nil {
		return nil, err
	}
	// OTEL_SERVICE_NAME and OTEL_RESOURCE_ATTRIBUTES override the service name.
	res, err := resource.New(ctx,
		resource.WithAttributes(semconv.ServiceName("distributedtracing-sample")),
		resource.WithFromEnv(),
		resource.WithTelemetrySDK(),
	)
	if err != nil {
		return nil, errors.Join(err, exp.Shutdown(ctx))
	}
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithSpanProcessor(sdktrace.NewBatchSpanProcessor(exp)),
		tracing.WithIDGenerator(nil),
		tracing.WithSampler(sdktrace.ParentBased(sdktrace.AlwaysSample())),
		sdktrace.WithResource(res),
	)
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.TraceContext{})
	return tp, nil
}

func shutdownTracer(tp *sdktrace.TracerProvider) error {
	if tp == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	return tp.Shutdown(ctx)
}

func DistributedTraceSampleOrchestrator(ctx *task.OrchestrationContext) (any, error) {
	var targetURL string
	if err := ctx.GetInput(&targetURL); err != nil {
		return nil, err
	}
	if err := ctx.CallActivity("DoWorkActivity", task.WithActivityInput(100*time.Millisecond)).Await(nil); err != nil {
		return nil, err
	}
	if err := ctx.CreateTimer(100 * time.Millisecond).Await(nil); err != nil {
		return nil, err
	}
	if err := ctx.CallActivity("CallHttpEndpointActivity", task.WithActivityInput(targetURL)).Await(nil); err != nil {
		return nil, err
	}
	return "trace-complete", nil
}

func DoWorkActivity(ctx task.ActivityContext) (any, error) {
	var duration time.Duration
	if err := ctx.GetInput(&duration); err != nil {
		return nil, err
	}
	select {
	case <-time.After(duration):
	case <-ctx.Context().Done():
		return nil, ctx.Context().Err()
	}
	return "worked", nil
}

func CallHttpEndpointActivity(ctx task.ActivityContext) (any, error) {
	var targetURL string
	if err := ctx.GetInput(&targetURL); err != nil {
		return nil, err
	}
	if err := callLocalTarget(ctx.Context(), targetURL); err != nil {
		return nil, err
	}
	return "called", nil
}

func callLocalTarget(ctx context.Context, targetURL string) (err error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, targetURL, nil)
	if err != nil {
		return err
	}
	client := &http.Client{Transport: otelhttp.NewTransport(http.DefaultTransport)}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, resp.Body.Close()) }()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("local target returned %s", resp.Status)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	if string(body) != "ok" {
		return fmt.Errorf("local target body=%q, want ok", body)
	}
	return nil
}

func requireHistoryTraceContext(events []*api.HistoryEvent, traceID string) error {
	if traceID == "" {
		return errors.New("caller trace ID is empty")
	}
	var sawStarted, sawActivity bool
	for _, event := range events {
		if event == nil {
			continue
		}
		if event.ExecutionStarted != nil && event.ExecutionStarted.ParentTraceContext != nil {
			sawStarted = sawStarted || strings.Contains(event.ExecutionStarted.ParentTraceContext.TraceParent, traceID)
		}
		if event.TaskScheduled != nil && event.TaskScheduled.Name == "CallHttpEndpointActivity" &&
			event.TaskScheduled.ParentTraceContext != nil {
			sawActivity = sawActivity || strings.Contains(event.TaskScheduled.ParentTraceContext.TraceParent, traceID)
		}
	}
	if !sawStarted || !sawActivity {
		return fmt.Errorf("history trace context missing caller trace: started=%v activity=%v", sawStarted, sawActivity)
	}
	return nil
}

// expectedSpan is a span the collector must receive. An unspecified kind
// matches any kind.
type expectedSpan struct {
	name string
	kind oteltrace.SpanKind
}

func (s expectedSpan) String() string {
	if s.kind == oteltrace.SpanKindUnspecified {
		return s.name
	}
	return s.name + " (" + s.kind.String() + ")"
}

func waitForCollectorReceipt(ctx context.Context, path string, traceID string, expected []expectedSpan) error {
	var lastErr error
	var missing []string
	for {
		body, err := os.ReadFile(path)
		if err == nil {
			missing, lastErr = missingSpansForTrace(string(body), traceID, expected)
			if lastErr == nil && len(missing) == 0 {
				return nil
			}
		} else {
			lastErr = err
		}
		select {
		case <-ctx.Done():
			if lastErr != nil {
				return fmt.Errorf("collector trace file %s could not be verified: %w", path, lastErr)
			}
			return fmt.Errorf("collector trace file %s did not contain trace %s spans %v; missing %v: %w",
				path, traceID, expected, missing, ctx.Err())
		case <-time.After(500 * time.Millisecond):
		}
	}
}

func traceFileContains(body, traceID string, expected []expectedSpan) bool {
	missing, err := missingSpansForTrace(body, traceID, expected)
	return err == nil && len(missing) == 0
}

func missingSpansForTrace(body, traceID string, expected []expectedSpan) ([]string, error) {
	spans, err := traceSpansFromCollectorFile(body)
	var missing []string
	for _, want := range expected {
		found := false
		for _, span := range spans {
			if strings.EqualFold(span.traceID, traceID) && span.name == want.name &&
				(want.kind == oteltrace.SpanKindUnspecified || span.kind == want.kind) {
				found = true
				break
			}
		}
		if !found {
			missing = append(missing, want.String())
		}
	}
	return missing, err
}

type collectedSpan struct {
	traceID string
	name    string
	kind    oteltrace.SpanKind
}

func traceSpansFromCollectorFile(body string) ([]collectedSpan, error) {
	decoder := json.NewDecoder(strings.NewReader(body))
	var spans []collectedSpan
	for {
		var value any
		err := decoder.Decode(&value)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, err
		}
		collectSpans(value, &spans)
	}
	return spans, nil
}

func collectSpans(value any, spans *[]collectedSpan) {
	switch typed := value.(type) {
	case map[string]any:
		traceID := stringField(typed, "traceId", "traceID", "trace_id")
		name := stringField(typed, "name")
		if traceID != "" && name != "" {
			*spans = append(*spans, collectedSpan{traceID: traceID, name: name, kind: spanKind(typed["kind"])})
		}
		for _, child := range typed {
			collectSpans(child, spans)
		}
	case []any:
		for _, child := range typed {
			collectSpans(child, spans)
		}
	}
}

// spanKind decodes an OTLP JSON span kind, which is an enum number such as 2
// or, from some encoders, an enum name such as "SPAN_KIND_SERVER". OTLP and
// the OpenTelemetry Go API number span kinds the same way.
func spanKind(value any) oteltrace.SpanKind {
	switch typed := value.(type) {
	case float64:
		return oteltrace.SpanKind(typed)
	case string:
		switch typed {
		case "SPAN_KIND_INTERNAL":
			return oteltrace.SpanKindInternal
		case "SPAN_KIND_SERVER":
			return oteltrace.SpanKindServer
		case "SPAN_KIND_CLIENT":
			return oteltrace.SpanKindClient
		case "SPAN_KIND_PRODUCER":
			return oteltrace.SpanKindProducer
		case "SPAN_KIND_CONSUMER":
			return oteltrace.SpanKindConsumer
		}
	}
	return oteltrace.SpanKindUnspecified
}

func stringField(fields map[string]any, names ...string) string {
	for _, name := range names {
		value, ok := fields[name].(string)
		if ok {
			return value
		}
	}
	return ""
}
