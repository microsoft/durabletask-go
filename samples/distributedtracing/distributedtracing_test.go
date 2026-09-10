package main

import "testing"

func TestTraceFileContains(t *testing.T) {
	body := `{"resourceSpans":[{"scopeSpans":[{"spans":[{"traceId":"abc123","name":"schedule_distributed_trace_sample"},{"traceId":"abc123","name":"local_trace_target"}]}]}]}`
	if !traceFileContains(body, "abc123", []string{callerSpanName, "local_trace_target"}) {
		t.Fatal("expected trace file body to contain required trace and spans")
	}
	if traceFileContains(body, "missing", []string{callerSpanName}) {
		t.Fatal("unexpected trace match")
	}
	if traceFileContains(body, "abc123", []string{"missing"}) {
		t.Fatal("unexpected span match")
	}
}

func TestTraceFileContainsRejectsStaleSpansFromOtherTraces(t *testing.T) {
	body := `
{"resourceSpans":[{"scopeSpans":[{"spans":[{"traceId":"current-trace","name":"schedule_distributed_trace_sample"}]}]}]}
{"resourceSpans":[{"scopeSpans":[{"spans":[{"traceId":"stale-trace","name":"local_trace_target"}]}]}]}
`
	if traceFileContains(body, "current-trace", []string{callerSpanName, "local_trace_target"}) {
		t.Fatal("expected spans split across traces to fail")
	}
}

func TestTraceSpansFromCollectorFileRejectsInvalidJSON(t *testing.T) {
	if _, err := traceSpansFromCollectorFile(`{"traceId":"abc"`); err == nil {
		t.Fatal("expected invalid collector JSON to fail")
	}
}
