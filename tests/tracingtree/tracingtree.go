// Package tracingtree provides the application-process OpenTelemetry plumbing
// used by the Durable Task Scheduler tracing tests: a process-wide in-memory
// exporter and the caller span whose W3C context the SDK propagates to DTS.
// Durable operation spans are emitted by DTS service-side, not by the Go worker.
package tracingtree

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

// attributeInstanceID is the instance-ID attribute DTS stamps on its
// service-side durable operation spans. It is used only in diagnostic output.
const attributeInstanceID = "durabletask.task.instance_id"

var (
	initOnce       sync.Once
	sharedExporter = tracetest.NewInMemoryExporter()
)

// Init installs an in-memory tracer provider as the global OTel provider and
// resets the previously collected spans. The global provider can only be
// installed once per process, so the exporter is shared and reset per test.
func Init() *tracetest.InMemoryExporter {
	initOnce.Do(func() {
		processor := sdktrace.NewSimpleSpanProcessor(sharedExporter)
		otel.SetTracerProvider(sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(processor)))
	})
	sharedExporter.Reset()
	return sharedExporter
}

// StartCallerSpan starts the client span that stands in for the application
// code scheduling an orchestration. The orchestration's whole span tree must
// hang off it, so the span is asserted to be sampled: an unsampled caller span
// would silently stop propagating trace context.
func StartCallerSpan(
	t require.TestingT,
	tracerName string,
	parent context.Context,
	name string,
) (context.Context, trace.Span) {
	if h, ok := t.(interface{ Helper() }); ok {
		h.Helper()
	}
	callerCtx, span := otel.Tracer(tracerName).Start(parent, name, trace.WithSpanKind(trace.SpanKindClient))
	require.True(t, span.SpanContext().IsSampled(), "the caller span must be sampled for trace propagation")
	return callerCtx, span
}

// Describe renders exported spans for assertion failure messages.
func Describe(spans []sdktrace.ReadOnlySpan) string {
	if len(spans) == 0 {
		return "(none)"
	}
	descriptions := make([]string, 0, len(spans))
	for _, span := range spans {
		instanceID := ""
		for _, kv := range span.Attributes() {
			if string(kv.Key) == attributeInstanceID {
				instanceID = kv.Value.AsString()
				break
			}
		}
		descriptions = append(descriptions, fmt.Sprintf("%s(instance=%s)", span.Name(), instanceID))
	}
	return strings.Join(descriptions, ", ")
}
