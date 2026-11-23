package main

import (
	"testing"

	oteltrace "go.opentelemetry.io/otel/trace"
)

func TestTraceFileContains(t *testing.T) {
	body := `{"resourceSpans":[{"scopeSpans":[{"spans":[{"traceId":"abc123","name":"schedule_distributed_trace_sample"},{"traceId":"abc123","name":"local_trace_target"}]}]}]}`
	if !traceFileContains(body, "abc123", []expectedSpan{{name: callerSpanName}, {name: "local_trace_target"}}) {
		t.Fatal("expected trace file body to contain required trace and spans")
	}
	if traceFileContains(body, "missing", []expectedSpan{{name: callerSpanName}}) {
		t.Fatal("unexpected trace match")
	}
	if traceFileContains(body, "abc123", []expectedSpan{{name: "missing"}}) {
		t.Fatal("unexpected span match")
	}
}

func TestTraceFileContainsRequiresSpanKinds(t *testing.T) {
	body := `{"resourceSpans":[{"scopeSpans":[{"spans":[{"traceId":"abc123","name":"activity:A","kind":3},{"traceId":"abc123","name":"activity:B","kind":"SPAN_KIND_SERVER"}]}]}]}`
	if !traceFileContains(body, "abc123", []expectedSpan{
		{name: "activity:A", kind: oteltrace.SpanKindClient},
		{name: "activity:B", kind: oteltrace.SpanKindServer},
	}) {
		t.Fatal("expected numeric and named span kinds to match")
	}
	missing, err := missingSpansForTrace(body, "abc123", []expectedSpan{{name: "activity:A", kind: oteltrace.SpanKindServer}})
	if err != nil || len(missing) != 1 || missing[0] != "activity:A (server)" {
		t.Fatalf("missing=%v err=%v, want the SERVER span reported missing", missing, err)
	}
}

func TestTraceFileContainsRejectsStaleSpansFromOtherTraces(t *testing.T) {
	body := `
{"resourceSpans":[{"scopeSpans":[{"spans":[{"traceId":"current-trace","name":"schedule_distributed_trace_sample"}]}]}]}
{"resourceSpans":[{"scopeSpans":[{"spans":[{"traceId":"stale-trace","name":"local_trace_target"}]}]}]}
`
	if traceFileContains(body, "current-trace", []expectedSpan{{name: callerSpanName}, {name: "local_trace_target"}}) {
		t.Fatal("expected spans split across traces to fail")
	}
}

func TestTraceSpansFromCollectorFileRejectsInvalidJSON(t *testing.T) {
	if _, err := traceSpansFromCollectorFile(`{"traceId":"abc"`); err == nil {
		t.Fatal("expected invalid collector JSON to fail")
	}
}
