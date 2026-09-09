package failure_test

import (
	"bytes"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/microsoft/durabletask-go/api"
	"github.com/microsoft/durabletask-go/internal/failure"
	"github.com/microsoft/durabletask-go/task"
)

func TestTaskFailedErrorConversionPreservesInnerFailure(t *testing.T) {
	inner := &api.FailureDetails{
		ErrorType:      "ActivityTaskNotFound",
		ErrorMessage:   "No activity task named 'missing' was found.",
		IsNonRetriable: true,
		Properties:     map[string]any{"attempt": float64(2)},
	}

	err := &task.TaskFailedError{
		TaskName:       "missing",
		TaskID:         7,
		FailureDetails: inner,
	}

	wire := failure.FromError(err)
	if wire.GetErrorType() != "TaskFailedException" {
		t.Fatalf("ErrorType = %q", wire.GetErrorType())
	}
	if wire.GetInnerFailure().GetErrorType() != string(inner.ErrorType) {
		t.Fatalf("inner ErrorType = %q", wire.GetInnerFailure().GetErrorType())
	}
	if !wire.GetIsNonRetriable() {
		t.Fatal("outer failure did not preserve non-retriable state")
	}

	roundTrip := failure.FromProto(wire)
	if roundTrip.InnerFailure == nil || roundTrip.InnerFailure.ErrorMessage != inner.ErrorMessage {
		t.Fatalf("round-trip failure = %#v", roundTrip)
	}
}

func TestWrappedDurableMarkersRemainVisible(t *testing.T) {
	inner := &task.VersionMismatchError{
		TaskVersion:   "2.0",
		WorkerVersion: "1.0",
		Strategy:      task.VersionMatchStrict,
	}

	wire := failure.FromError(fmt.Errorf("dispatch failed: %w", inner))
	if wire.GetErrorType() != string(api.ErrorTypeVersionMismatch) {
		t.Fatalf("ErrorType = %q", wire.GetErrorType())
	}
	if !wire.GetIsNonRetriable() {
		t.Fatal("wrapped non-retriable marker was lost")
	}
}

func TestFailurePropertiesProviderRoundTripsStructuredValues(t *testing.T) {
	when := time.Date(2026, time.August, 27, 12, 30, 0, 0, time.FixedZone("offset", -7*60*60))
	provider := api.ErrorPropertiesProviderFunc(func(error) map[string]any {
		return map[string]any{
			"attempt": 3,
			"when":    when,
			"nested":  map[string]any{"values": []any{"a", true, 4}},
		}
	})

	wire := failure.FromError(errors.New("boom"), provider)
	roundTrip := failure.FromProto(wire)
	if roundTrip.Properties["attempt"] != float64(3) {
		t.Fatalf("attempt = %#v", roundTrip.Properties["attempt"])
	}
	if got, ok := roundTrip.Properties["when"].(string); !ok || got != "dt:"+when.Format(time.RFC3339Nano) {
		t.Fatalf("when = %#v", roundTrip.Properties["when"])
	}
	nested, ok := roundTrip.Properties["nested"].(map[string]any)
	if !ok {
		t.Fatalf("nested = %#v", roundTrip.Properties["nested"])
	}
	if _, ok := nested["values"].([]any); !ok {
		t.Fatalf("nested values = %#v", nested["values"])
	}
}

func TestJoinedErrorsUseOneCauseAndAdditionalProperties(t *testing.T) {
	wire := failure.FromError(errors.Join(errors.New("primary"), errors.New("secondary")))

	if wire.GetInnerFailure().GetErrorMessage() != "primary" {
		t.Fatalf("inner failure = %#v", wire.GetInnerFailure())
	}
	additional := wire.GetProperties()["go.additionalErrors"].GetListValue()
	if additional == nil || len(additional.Values) != 1 {
		t.Fatalf("additional errors = %#v", additional)
	}
}

func TestJoinedErrorsPromoteDurableFailure(t *testing.T) {
	taskErr := &task.TaskFailedError{
		TaskName: "activity",
		TaskID:   1,
		FailureDetails: &api.FailureDetails{
			ErrorType:      "ActivityTaskNotFound",
			ErrorMessage:   "missing",
			IsNonRetriable: true,
		},
	}

	wire := failure.FromError(errors.Join(errors.New("cleanup"), taskErr))
	if wire.GetErrorType() != "TaskFailedException" {
		t.Fatalf("failure = %#v", wire)
	}
	if wire.GetInnerFailure().GetErrorType() != "ActivityTaskNotFound" {
		t.Fatalf("durable chain = %#v", wire)
	}
}

func TestPropertiesHandleTypedNilStringer(t *testing.T) {
	var buffer *bytes.Buffer
	provider := api.ErrorPropertiesProviderFunc(func(error) map[string]any {
		return map[string]any{"buffer": buffer}
	})

	wire := failure.FromError(errors.New("boom"), provider)
	if wire.GetProperties()["buffer"].GetNullValue().String() != "NULL_VALUE" {
		t.Fatalf("buffer = %#v", wire.GetProperties()["buffer"])
	}
}

func TestPropertyBudgetAppliesAcrossFailureChain(t *testing.T) {
	properties := make(map[string]any, 100)
	for i := 0; i < 100; i++ {
		properties[fmt.Sprintf("property-%03d", i)] = "value"
	}
	provider := api.ErrorPropertiesProviderFunc(func(error) map[string]any {
		return properties
	})

	wire := failure.FromError(fmt.Errorf("outer: %w", errors.New("inner")), provider)
	propertyCount := 0
	truncationMarkers := 0
	for current := wire; current != nil; current = current.GetInnerFailure() {
		propertyCount += len(current.GetProperties())
		if current.GetProperties()["go.propertiesTruncated"].GetBoolValue() {
			truncationMarkers++
		}
	}
	if propertyCount > 65 {
		t.Fatalf("property count = %d", propertyCount)
	}
	if truncationMarkers != 1 {
		t.Fatalf("truncation markers = %d", truncationMarkers)
	}
}
