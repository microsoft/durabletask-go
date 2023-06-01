package tests

import (
	"fmt"
	"sync"
	"time"

	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"github.com/microsoft/durabletask-go/api"
)

type (
	spanValidator          func(t assert.TestingT, spans []trace.ReadOnlySpan, index int)
	spanAttributeValidator func(t assert.TestingT, span trace.ReadOnlySpan) bool
	spanEventValidator     func(t assert.TestingT, span trace.ReadOnlySpan, eventIndex int) bool
)

var (
	initTracingOnce     sync.Once
	sharedTraceExporter = tracetest.NewInMemoryExporter()
)

func assertSpanSequence(t assert.TestingT, spans []trace.ReadOnlySpan, spanAsserts ...spanValidator) {
	for i, f := range spanAsserts {
		f(t, spans, i)
	}
}

// assertOrchestratorCreated validates a create_orchestration span
func assertOrchestratorCreated(name string, id api.InstanceID, optionalAsserts ...spanAttributeValidator) spanValidator {
	spanName := fmt.Sprintf("create_orchestration||%s", name)
	opts := []spanAttributeValidator{
		assertTaskType("orchestration"),
		assertTaskName(name),
		assertInstanceID(id),
	}
	opts = append(opts, optionalAsserts...)
	return assertSpan(spanName, opts...)
}

// assertOrchestratorCreated validates an orchestration span
func assertOrchestratorExecuted(name string, id api.InstanceID, status string, optionalAsserts ...spanAttributeValidator) spanValidator {
	spanName := fmt.Sprintf("orchestration||%s", name)
	opts := []spanAttributeValidator{
		assertTaskType("orchestration"),
		assertTaskName(name),
		assertInstanceID(id),
		assertStatus(status),
	}
	opts = append(opts, optionalAsserts...)
	return assertSpan(spanName, opts...)
}

func assertActivity(name string, id api.InstanceID, taskID int64, optionalAsserts ...spanAttributeValidator) spanValidator {
	spanName := fmt.Sprintf("activity||%s", name)
	opts := []spanAttributeValidator{
		assertTaskType("activity"),
		assertTaskName(name),
		assertInstanceID(id),
		assertTaskID(taskID),
	}
	opts = append(opts, optionalAsserts...)
	return assertSpan(spanName, opts...)
}

func assertTimer(id api.InstanceID) spanValidator {
	return assertSpan("timer", assertInstanceID(id), assertTimerFired())
}

func assertSpanEvents(eventAsserts ...spanEventValidator) spanAttributeValidator {
	return func(t assert.TestingT, span trace.ReadOnlySpan) bool {
		if assert.Equal(t, len(eventAsserts), len(span.Events()), "unexpected number of span events") {
			for i, f := range eventAsserts {
				if !f(t, span, i) {
					return false
				}
			}
		}
		return true
	}
}

func assertExternalEvent(eventName string, payloadSize int) spanEventValidator {
	return func(t assert.TestingT, span trace.ReadOnlySpan, eventIndex int) bool {
		event := span.Events()[eventIndex]
		hasMessage := assert.Equal(t, "Received external event", event.Name)
		hasNameAttribute := assert.Contains(t, event.Attributes, attribute.KeyValue{
			Key:   "name",
			Value: attribute.StringValue(eventName),
		})
		hasSizeAttribute := assert.Contains(t, event.Attributes, attribute.KeyValue{
			Key:   "size",
			Value: attribute.IntValue(payloadSize),
		})
		return hasMessage && hasNameAttribute && hasSizeAttribute
	}
}

func assertSuspendedEvent() spanEventValidator {
	return func(t assert.TestingT, span trace.ReadOnlySpan, eventIndex int) bool {
		event := span.Events()[eventIndex]
		return assert.Equal(t, "Execution suspended", event.Name)
	}
}

func assertResumedEvent() spanEventValidator {
	return func(t assert.TestingT, span trace.ReadOnlySpan, eventIndex int) bool {
		event := span.Events()[eventIndex]
		return assert.Equal(t, "Execution resumed", event.Name)
	}
}

func assertSpan(name string, optionalAsserts ...spanAttributeValidator) spanValidator {
	return func(t assert.TestingT, spans []trace.ReadOnlySpan, index int) {
		if !doAssertSpan(t, spans, index, name, optionalAsserts...) {
			fmt.Printf("span assertion for %s (index=%d) failed\n", name, index)
		}
	}
}

func doAssertSpan(t assert.TestingT, spans []trace.ReadOnlySpan, index int, name string, optionalAsserts ...spanAttributeValidator) bool {
	// array bounds check
	if !assert.Lessf(t, index, len(spans), "%d spans were exported, but more were expected by the test", len(spans)) {
		return false
	}

	span := spans[index]

	// All spans have a name that we must validate
	success := assert.Equal(t, name, span.Name())

	// Optional validations that are span-specific
	for _, optionalAssert := range optionalAsserts {
		if !optionalAssert(t, span) {
			success = false
		}
	}

	return success
}

func assertTaskType(expectedTaskType string) spanAttributeValidator {
	return func(t assert.TestingT, span trace.ReadOnlySpan) bool {
		return assert.Contains(t, span.Attributes(), attribute.KeyValue{
			Key:   "durabletask.type",
			Value: attribute.StringValue(expectedTaskType),
		})
	}
}

func assertTaskName(expectedTaskName string) spanAttributeValidator {
	return func(t assert.TestingT, span trace.ReadOnlySpan) bool {
		return assert.Contains(t, span.Attributes(), attribute.KeyValue{
			Key:   "durabletask.task.name",
			Value: attribute.StringValue(expectedTaskName),
		})
	}
}

func assertTaskID(expectedTaskID int64) spanAttributeValidator {
	return func(t assert.TestingT, span trace.ReadOnlySpan) bool {
		return assert.Contains(t, span.Attributes(), attribute.KeyValue{
			Key:   "durabletask.task.task_id",
			Value: attribute.Int64Value(expectedTaskID),
		})
	}
}

func assertInstanceID(expectedID api.InstanceID) spanAttributeValidator {
	return func(t assert.TestingT, span trace.ReadOnlySpan) bool {
		return assert.Contains(t, span.Attributes(), attribute.KeyValue{
			Key:   "durabletask.task.instance_id",
			Value: attribute.StringValue(string(expectedID)),
		})
	}
}

func assertStatus(expectedStatus string) spanAttributeValidator {
	return func(t assert.TestingT, span trace.ReadOnlySpan) bool {
		return assert.Contains(t, span.Attributes(), attribute.KeyValue{
			Key:   "durabletask.runtime_status",
			Value: attribute.StringValue(expectedStatus),
		})
	}
}

func assertTimerFired() spanAttributeValidator {
	return func(t assert.TestingT, span trace.ReadOnlySpan) bool {
		var firedAtStr string
		for _, a := range span.Attributes() {
			if a.Key == "durabletask.fire_at" {
				firedAtStr = a.Value.AsString()
				break
			}
		}

		if assert.NotEmptyf(t, firedAtStr, "couldn't find the durabletask.fire_at attribute") {
			// Ensure we can parse the value and that the value fits into a general range.
			// Note that we're not attempting to validate a specific time.
			firedAt, err := time.Parse(time.RFC3339, firedAtStr)
			now := time.Now().UTC()
			return assert.NoError(t, err) &&
				assert.Less(t, firedAt, now) &&
				assert.Greater(t, firedAt, now.Add(-1*time.Hour))
		}

		return false
	}
}

// initTracing configures in-memory OTel tracing and returns an exporter which can be used
// to examine the exported traces. We only want to look at exported traces because we do
// tricks to mark certain spans as non-exported (i.e. orchestration replays), and want
// to ensure that those spans are never actually exported.
func initTracing() *tracetest.InMemoryExporter {
	// The global tracer provider can only be initialized once.
	// Subsequent initializations will silently fail.
	initTracingOnce.Do(func() {
		processor := trace.NewSimpleSpanProcessor(sharedTraceExporter)
		provider := trace.NewTracerProvider(trace.WithSpanProcessor(processor))
		otel.SetTracerProvider(provider)
	})

	// Reset the shared exporter so that new tests don't see traces from previous tests.
	sharedTraceExporter.Reset()
	return sharedTraceExporter
}
