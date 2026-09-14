package task

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/microsoft/durabletask-go/internal/helpers"
	"github.com/microsoft/durabletask-go/internal/protos"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

func TestActivityRestoresRemoteTraceParentWithoutRecordingSpan(t *testing.T) {
	type contextKey struct{}
	parent, cancel := context.WithTimeout(context.WithValue(context.Background(), contextKey{}, "retained"), time.Minute)
	defer cancel()
	deadline, _ := parent.Deadline()
	registry := NewTaskRegistry()
	if err := registry.AddActivityN("trace-parent", func(ctx ActivityContext) (any, error) {
		span := trace.SpanFromContext(ctx.Context())
		sc := span.SpanContext()
		if !sc.IsRemote() || !sc.IsSampled() || span.IsRecording() ||
			sc.TraceID().String() != "0123456789abcdef0123456789abcdef" ||
			sc.SpanID().String() != "0123456789abcdef" || sc.TraceState().String() != "vendor=value" {
			return nil, errors.New("activity did not receive the non-recording remote trace context")
		}
		actualDeadline, ok := ctx.Context().Deadline()
		if !ok || actualDeadline != deadline || ctx.Context().Value(contextKey{}) != "retained" ||
			ctx.Context().Done() != parent.Done() {
			return nil, errors.New("trace extraction changed the host context")
		}
		return "linked", nil
	}); err != nil {
		t.Fatal(err)
	}
	event := helpers.NewTaskScheduledEvent(0, "trace-parent", nil, nil, &protos.TraceContext{
		TraceParent: "00-0123456789abcdef0123456789abcdef-0123456789abcdef-01",
		TraceState:  wrapperspb.String("vendor=value"),
	})
	result, err := NewTaskExecutor(registry).ExecuteActivity(parent, "instance", event)
	if err != nil {
		t.Fatal(err)
	}
	if result.GetTaskCompleted().GetResult().GetValue() != `"linked"` {
		t.Fatalf("activity trace propagation failed: %v", result)
	}
}
