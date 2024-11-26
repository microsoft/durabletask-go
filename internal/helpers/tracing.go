package helpers

import (
	"context"
	"encoding/hex"
	"reflect"
	"strings"
	"time"
	"unsafe"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/dapr/durabletask-go/internal/protos"
)

var tracer = otel.Tracer("durabletask")

func StartNewCreateOrchestrationSpan(
	ctx context.Context, name string, version string, instanceID string,
) (context.Context, trace.Span) {
	attributes := []attribute.KeyValue{
		{Key: "durabletask.type", Value: attribute.StringValue("orchestration")},
		{Key: "durabletask.task.name", Value: attribute.StringValue(name)},
		{Key: "durabletask.task.instance_id", Value: attribute.StringValue(instanceID)},
	}
	return startNewSpan(ctx, "create_orchestration", name, version, attributes, trace.SpanKindClient, time.Now().UTC())
}

func StartNewRunOrchestrationSpan(
	ctx context.Context, es *protos.ExecutionStartedEvent, startedTime time.Time,
) (context.Context, trace.Span) {
	name := es.Name
	instanceID := es.OrchestrationInstance.InstanceId
	version := es.Version.GetValue()
	attributes := []attribute.KeyValue{
		{Key: "durabletask.type", Value: attribute.StringValue("orchestration")},
		{Key: "durabletask.task.name", Value: attribute.StringValue(name)},
		{Key: "durabletask.task.instance_id", Value: attribute.StringValue(instanceID)},
	}
	return startNewSpan(ctx, "orchestration", name, version, attributes, trace.SpanKindServer, startedTime)
}

func StartNewActivitySpan(
	ctx context.Context, name string, version string, instanceID string, taskID int32,
) (context.Context, trace.Span) {
	attributes := []attribute.KeyValue{
		{Key: "durabletask.type", Value: attribute.StringValue("activity")},
		{Key: "durabletask.task.name", Value: attribute.StringValue(name)},
		{Key: "durabletask.task.task_id", Value: attribute.Int64Value(int64(taskID))},
		{Key: "durabletask.task.instance_id", Value: attribute.StringValue(instanceID)},
	}
	return startNewSpan(ctx, "activity", name, version, attributes, trace.SpanKindServer, time.Now().UTC())
}

func StartAndEndNewTimerSpan(ctx context.Context, tf *protos.TimerFiredEvent, createdTime time.Time, instanceID string) error {
	attributes := []attribute.KeyValue{
		{Key: "durabletask.type", Value: attribute.StringValue("timer")},
		{Key: "durabletask.fire_at", Value: attribute.StringValue(tf.FireAt.AsTime().Format(time.RFC3339))}, // time.RFC3339 most closely maps to ISO 8601
		{Key: "durabletask.task.task_id", Value: attribute.Int64Value(int64(tf.TimerId))},
		{Key: "durabletask.task.instance_id", Value: attribute.StringValue(instanceID)},
	}

	_, span := startNewSpan(ctx, "timer", "", "", attributes, trace.SpanKindInternal, createdTime)
	span.End()
	return nil
}

func startNewSpan(
	ctx context.Context,
	taskType string,
	taskName string,
	taskVersion string,
	attributes []attribute.KeyValue,
	kind trace.SpanKind,
	timestamp time.Time,
) (context.Context, trace.Span) {
	var spanName string
	if taskVersion != "" {
		spanName = taskType + "||" + taskName + "||" + taskVersion
		attributes = append(attributes, attribute.KeyValue{
			Key:   "durabletask.task.version",
			Value: attribute.StringValue(taskVersion),
		})
	} else if taskName != "" {
		spanName = taskType + "||" + taskName
	} else {
		spanName = taskType
	}

	var span trace.Span
	ctx, span = tracer.Start(
		ctx,
		spanName,
		trace.WithSpanKind(kind),
		trace.WithTimestamp(timestamp),
		trace.WithAttributes(attributes...),
	)
	return ctx, span
}

func UnsafeSetSpanContext(span trace.Span, spanContext trace.SpanContext) bool {
	if !span.IsRecording() {
		// this logic only applies to recording spans
		return false
	}
	spanPtr := reflect.ValueOf(span)
	spanVal := reflect.Indirect(spanPtr)
	spanContextField := spanVal.FieldByName("spanContext")
	if !spanContextField.IsValid() || spanContextField.IsZero() {
		// The spanContext field doesn't exist
		return false
	}
	// TODO: Validate the type of the field?
	spanContextPtr := unsafe.Pointer(spanContextField.UnsafeAddr())
	realPtrToSpanContext := (*trace.SpanContext)(spanContextPtr)
	*realPtrToSpanContext = spanContext
	return true
}

func ContextFromTraceContext(ctx context.Context, tc *protos.TraceContext) (context.Context, error) {
	if tc == nil {
		return ctx, nil
	}

	spanContext, err := SpanContextFromTraceContext(tc)
	if err != nil {
		return ctx, err
	}

	ctx = trace.ContextWithRemoteSpanContext(ctx, spanContext)
	return ctx, nil
}

func SpanContextFromTraceContext(tc *protos.TraceContext) (trace.SpanContext, error) {
	var decodedTraceID trace.TraceID
	var err error
	var traceID string
	var spanID string
	var traceFlags string

	parts := strings.Split(tc.TraceParent, "-")
	if len(parts) == 4 {
		traceID = parts[1]
		spanID = parts[2]
		traceFlags = parts[3]
	} else {
		// backwards compatibility with older versions of the protobuf
		traceID = tc.GetTraceParent()
		spanID = tc.GetSpanID()
		traceFlags = "01" // sampled
	}

	decodedTraceID, err = trace.TraceIDFromHex(traceID)
	if err != nil {
		return trace.SpanContext{}, err
	}

	var decodedSpanID trace.SpanID
	decodedSpanID, err = trace.SpanIDFromHex(spanID)
	if err != nil {
		return trace.SpanContext{}, err
	}

	var decodedTraceFlags []byte
	decodedTraceFlags, err = hex.DecodeString(traceFlags)
	if err != nil {
		return trace.SpanContext{}, err
	}

	spanContextConfig := trace.SpanContextConfig{
		TraceID:    decodedTraceID,
		SpanID:     decodedSpanID,
		TraceFlags: trace.TraceFlags(decodedTraceFlags[0]),
	}

	// Trace state is optional
	if traceState := tc.TraceState.GetValue(); traceState != "" {
		var ts trace.TraceState
		ts, err = trace.ParseTraceState(traceState)
		if err != nil {
			return trace.SpanContext{}, err
		}
		spanContextConfig.TraceState = ts
	}
	spanContext := trace.NewSpanContext(spanContextConfig)
	return spanContext, nil
}

func TraceContextFromSpan(span trace.Span) *protos.TraceContext {
	if span == nil {
		return nil
	} else if !span.SpanContext().IsSampled() {
		// Don't apply trace context for anything that's not being sampled. Note that by doing this,
		// we're ensuring a parent-based sampling strategy. More information on OTel sampling here:
		// https://opentelemetry.io/docs/instrumentation/go/exporting_data/#sampling
		return nil
	}

	var tc *protos.TraceContext
	spanContext := span.SpanContext()
	if spanContext.IsValid() {
		tc = &protos.TraceContext{
			TraceParent: "00-" + spanContext.TraceID().String() + "-" + spanContext.SpanID().String() + "-" + spanContext.TraceFlags().String(),
		}
		if ts := spanContext.TraceState().String(); ts != "" {
			tc.TraceState = wrapperspb.String(ts)
		}
	}
	return tc
}

func ChangeSpanID(span trace.Span, newSpanID trace.SpanID) {
	modifiedSpanContext := span.SpanContext().WithSpanID(newSpanID)
	UnsafeSetSpanContext(span, modifiedSpanContext)
}

func CancelSpan(span trace.Span) {
	if span.SpanContext().IsSampled() {
		// set the IsSampled flag to 0 (not sampled)
		modifiedSpanContext := span.SpanContext().WithTraceFlags(trace.TraceFlags(0))
		UnsafeSetSpanContext(span, modifiedSpanContext)
	}
}

func NoopSpan() trace.Span {
	return trace.SpanFromContext(context.Background())
}
