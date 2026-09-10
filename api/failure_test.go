package api

import (
	"encoding/json"
	"errors"
	"testing"
)

func TestFailureDetailsHelpers(t *testing.T) {
	details := &FailureDetails{
		ErrorType:    "Outer",
		ErrorMessage: "outer failed",
		InnerFailure: &FailureDetails{
			ErrorType:      "ActivityTaskNotFound",
			ErrorMessage:   "missing",
			IsNonRetriable: true,
		},
	}

	if details.String() != "Outer: outer failed" {
		t.Fatalf("String() = %q", details.String())
	}
	if !details.IsCausedBy(ErrorTypeActivityTaskNotFound) {
		t.Fatal("IsCausedBy() did not inspect the full chain")
	}
	if details.NonRetriable() {
		t.Fatal("outer failure should remain retriable")
	}
	if !details.InnerFailure.NonRetriable() {
		t.Fatal("inner failure should be non-retriable")
	}
}

func TestFailureDetailsRejectsCyclesDuringJSONMarshal(t *testing.T) {
	details := &FailureDetails{ErrorType: "Cycle", ErrorMessage: "cycle"}
	details.InnerFailure = details

	if _, err := json.Marshal(details); err == nil {
		t.Fatal("json.Marshal() succeeded for a cyclic failure chain")
	}
}

func TestWrapInvalidArgumentPreservesClassificationAndCause(t *testing.T) {
	cause := errors.New("bad value")
	err := WrapInvalidArgument(cause)

	if !errors.Is(err, ErrInvalidArgument) {
		t.Fatal("error does not match ErrInvalidArgument")
	}
	if !errors.Is(err, cause) {
		t.Fatal("error does not preserve its cause")
	}
}
