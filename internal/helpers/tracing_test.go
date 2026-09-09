package helpers

import (
	"strings"
	"testing"

	"github.com/microsoft/durabletask-go/internal/protos"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

func TestOrchestratorActionTraceContextCreatesDistinctScheduleSpans(t *testing.T) {
	parent := &protos.TraceContext{
		TraceParent: "00-0123456789abcdef0123456789abcdef-0123456789abcdef-01",
		TraceState:  wrapperspb.String("vendor=value"),
	}
	first, err := OrchestratorActionTraceContext(parent)
	if err != nil {
		t.Fatal(err)
	}
	second, err := OrchestratorActionTraceContext(parent)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(first.TraceParent, "00-0123456789abcdef0123456789abcdef-") {
		t.Fatalf("first trace parent = %q", first.TraceParent)
	}
	if first.TraceParent == parent.TraceParent || first.TraceParent == second.TraceParent {
		t.Fatalf("action trace parents must use fresh span IDs: first=%q second=%q", first.TraceParent, second.TraceParent)
	}
	if first.GetTraceState().GetValue() != "vendor=value" {
		t.Fatalf("trace state = %q", first.GetTraceState().GetValue())
	}
}

func TestOrchestratorActionTraceContextRejectsInvalidContext(t *testing.T) {
	for _, parent := range []*protos.TraceContext{
		{TraceParent: "invalid"},
		{TraceParent: "00-00000000000000000000000000000000-0123456789abcdef-01"},
	} {
		if got, err := OrchestratorActionTraceContext(parent); err == nil || got != nil {
			t.Fatalf("OrchestratorActionTraceContext(%v) = %v, %v", parent, got, err)
		}
	}
	if got, err := OrchestratorActionTraceContext(nil); err != nil || got != nil {
		t.Fatalf("OrchestratorActionTraceContext(nil) = %v, %v", got, err)
	}
}
