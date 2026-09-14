// Command distributedtracing propagates an application caller span into DTS and
// verifies that a real OTLP/HTTP collector received application spans.
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
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	oteltrace "go.opentelemetry.io/otel/trace"

	"github.com/microsoft/durabletask-go/api"
	"github.com/microsoft/durabletask-go/samples/internal/dtssample"
	"github.com/microsoft/durabletask-go/task"
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

	app, err := dtssample.Start(ctx, registry)
	if err != nil {
		return err
	}
	var ownedIDs []api.InstanceID
	cleanupDone := false
	defer func() {
		if !cleanupDone {
			err = errors.Join(err, dtssample.Cleanup(app.Client, ownedIDs...), app.Shutdown())
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
	if _, err := app.Client.ScheduleNewOrchestration(callerCtx, "DistributedTraceSampleOrchestrator",
		api.WithInstanceID(instanceID),
		api.WithInput(tracedTarget.URL),
		api.WithTags(map[string]string{"sample": "distributedtracing"})); err != nil {
		callerSpan.End()
		return fmt.Errorf("schedule orchestration: %w", err)
	}
	callerSpan.End()

	metadata, err := app.Client.WaitForOrchestrationCompletion(ctx, instanceID, api.WithFetchPayloads(true))
	if err != nil {
		return fmt.Errorf("wait for orchestration: %w", err)
	}
	if err := dtssample.RequireCompleted(metadata); err != nil {
		return err
	}
	history, err := app.Client.GetOrchestrationHistory(ctx, instanceID, api.HistoryQuery{ExecutionID: metadata.ExecutionID})
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

	cleanupErr := dtssample.Cleanup(app.Client, ownedIDs...)
	appErr := app.Shutdown()
	traceErr := shutdownTracer(tp)
	cleanupDone = true
	receiptCtx, receiptCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer receiptCancel()
	receiptErr := waitForCollectorReceipt(receiptCtx, traceFile, traceID, []string{callerSpanName, "local_trace_target"})
	return errors.Join(cleanupErr, appErr, traceErr, receiptErr)
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
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithSpanProcessor(sdktrace.NewBatchSpanProcessor(exp)),
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
		sdktrace.WithResource(resource.NewWithAttributes(
			"durabletask.io",
			attribute.String("service.name", "distributedtracing-sample"),
		)),
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

func waitForCollectorReceipt(ctx context.Context, path string, traceID string, spanNames []string) error {
	var lastErr error
	var missing []string
	for {
		body, err := os.ReadFile(path)
		if err == nil {
			missing, lastErr = missingSpansForTrace(string(body), traceID, spanNames)
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
				path, traceID, spanNames, missing, ctx.Err())
		case <-time.After(500 * time.Millisecond):
		}
	}
}

func traceFileContains(body, traceID string, spanNames []string) bool {
	missing, err := missingSpansForTrace(body, traceID, spanNames)
	return err == nil && len(missing) == 0
}

func missingSpansForTrace(body, traceID string, spanNames []string) ([]string, error) {
	spans, err := traceSpansFromCollectorFile(body)
	if err != nil {
		return spanNames, err
	}
	found := map[string]struct{}{}
	for _, span := range spans {
		if strings.EqualFold(span.traceID, traceID) {
			found[span.name] = struct{}{}
		}
	}
	var missing []string
	for _, name := range spanNames {
		if _, ok := found[name]; !ok {
			missing = append(missing, name)
		}
	}
	return missing, nil
}

type collectedSpan struct {
	traceID string
	name    string
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
			*spans = append(*spans, collectedSpan{traceID: traceID, name: name})
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

func stringField(fields map[string]any, names ...string) string {
	for _, name := range names {
		value, ok := fields[name].(string)
		if ok {
			return value
		}
	}
	return ""
}
