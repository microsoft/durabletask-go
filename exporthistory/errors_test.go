package exporthistory

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/microsoft/durabletask-go/api"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestIsValidTransition ports the upstream ExportJobTransitionsTests matrix.
func TestIsValidTransition(t *testing.T) {
	tests := []struct {
		operation string
		from      ExportJobStatus
		to        ExportJobStatus
		expected  bool
	}{
		{createOperation, ExportJobStatusPending, ExportJobStatusActive, true},
		{createOperation, ExportJobStatusFailed, ExportJobStatusActive, true},
		{createOperation, ExportJobStatusCompleted, ExportJobStatusActive, true},
		{createOperation, ExportJobStatusActive, ExportJobStatusActive, false},
		{createOperation, ExportJobStatusActive, ExportJobStatusFailed, false},
		{createOperation, ExportJobStatusActive, ExportJobStatusCompleted, false},
		{createOperation, ExportJobStatusPending, ExportJobStatusCompleted, false},
		{createOperation, ExportJobStatusPending, ExportJobStatusFailed, false},

		{markAsCompletedOperation, ExportJobStatusActive, ExportJobStatusCompleted, true},
		{markAsCompletedOperation, ExportJobStatusPending, ExportJobStatusCompleted, false},
		{markAsCompletedOperation, ExportJobStatusFailed, ExportJobStatusCompleted, false},
		{markAsCompletedOperation, ExportJobStatusCompleted, ExportJobStatusCompleted, false},
		{markAsCompletedOperation, ExportJobStatusActive, ExportJobStatusFailed, false},

		{markAsFailedOperation, ExportJobStatusActive, ExportJobStatusFailed, true},
		{markAsFailedOperation, ExportJobStatusPending, ExportJobStatusFailed, false},
		{markAsFailedOperation, ExportJobStatusFailed, ExportJobStatusFailed, false},
		{markAsFailedOperation, ExportJobStatusCompleted, ExportJobStatusFailed, false},
		{markAsFailedOperation, ExportJobStatusActive, ExportJobStatusCompleted, false},

		{"UnknownOperation", ExportJobStatusPending, ExportJobStatusActive, false},
		{getOperation, ExportJobStatusActive, ExportJobStatusActive, false},
		{deleteOperation, ExportJobStatusActive, ExportJobStatusPending, false},
	}
	for _, test := range tests {
		name := fmt.Sprintf("%s/%s->%s", test.operation, test.from, test.to)
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, test.expected, isValidTransition(test.operation, test.from, test.to))
		})
	}
}

func TestValidationError(t *testing.T) {
	err := &ValidationError{JobID: "job-1", Message: "bad input"}
	assert.Equal(t, `validation failed for export job "job-1": bad input`, err.Error())
	assert.ErrorIs(t, err, ErrValidation)
	assert.Equal(t, validationErrorType, err.DurableTaskErrorType())
	assert.Equal(t, map[string]any{"jobId": "job-1", "message": "bad input"}, err.DurableTaskErrorProperties())

	bare := &ValidationError{Message: "bad input"}
	assert.Equal(t, "export job validation failed: bad input", bare.Error())

	var target *ValidationError
	require.True(t, errors.As(fmt.Errorf("wrapped: %w", err), &target))
	assert.Equal(t, "job-1", target.JobID)
}

func TestNotFoundError(t *testing.T) {
	err := &NotFoundError{JobID: "missing"}
	assert.Equal(t, `export history job with ID "missing" was not found`, err.Error())
	assert.ErrorIs(t, err, ErrJobNotFound)
	assert.Equal(t, notFoundErrorType, err.DurableTaskErrorType())
	assert.Equal(t, map[string]any{"jobId": "missing"}, err.DurableTaskErrorProperties())
}

func TestInvalidTransitionError(t *testing.T) {
	err := &InvalidTransitionError{
		JobID:     "job-1",
		From:      ExportJobStatusActive,
		To:        ExportJobStatusActive,
		Operation: createOperation,
	}
	assert.Equal(t,
		`invalid state transition attempted for export job "job-1": `+
			`cannot transition from Active to Active during Create operation`,
		err.Error())
	assert.ErrorIs(t, err, ErrJobInvalidTransition)
	assert.Equal(t, invalidTransitionErrorType, err.DurableTaskErrorType())
	assert.Equal(t, map[string]any{
		"jobId":     "job-1",
		"from":      int(ExportJobStatusActive),
		"to":        int(ExportJobStatusActive),
		"operation": createOperation,
	}, err.DurableTaskErrorProperties())
}

func TestOperationError(t *testing.T) {
	err := &OperationError{
		JobID:         "job-1",
		Operation:     createOperation,
		RuntimeStatus: api.RUNTIME_STATUS_FAILED,
	}
	assert.Contains(t, err.Error(), `export job "job-1" operation Create failed with status`)
	assert.ErrorIs(t, err, ErrJobOperationFailed)
	assert.Equal(t, operationFailedErrorType, err.DurableTaskErrorType())

	withDetails := &OperationError{
		JobID:          "job-1",
		Operation:      deleteOperation,
		RuntimeStatus:  api.RUNTIME_STATUS_FAILED,
		FailureDetails: &api.FailureDetails{ErrorMessage: "boom"},
	}
	assert.Equal(t, `export job "job-1" operation Delete failed: boom`, withDetails.Error())
}

// TestFindFailureMatchesNamespacedErrorTypes keeps typed errors recoverable when
// another SDK reports the fully qualified .NET exception name.
func TestFindFailureMatchesNamespacedErrorTypes(t *testing.T) {
	details := &api.FailureDetails{
		ErrorType:    "System.InvalidOperationException",
		ErrorMessage: "outer",
		InnerFailure: &api.FailureDetails{
			ErrorType:    "Microsoft.DurableTask.ExportHistory.ExportJobInvalidTransitionException",
			ErrorMessage: "inner",
		},
	}
	found := findFailure(details, invalidTransitionErrorType)
	require.NotNil(t, found)
	assert.Equal(t, "inner", found.ErrorMessage)
	assert.Nil(t, findFailure(details, notFoundErrorType))
	assert.Nil(t, findFailure(nil, notFoundErrorType))
}

func TestOperationFailureReconstructsTypedErrors(t *testing.T) {
	t.Run("invalid transition", func(t *testing.T) {
		err := operationFailure("job-1", createOperation, &api.OrchestrationMetadata{
			RuntimeStatus: api.RUNTIME_STATUS_FAILED,
			FailureDetails: &api.FailureDetails{
				ErrorType: invalidTransitionErrorType,
				Properties: map[string]any{
					"jobId":     "job-1",
					"from":      float64(ExportJobStatusActive),
					"to":        float64(ExportJobStatusActive),
					"operation": createOperation,
				},
			},
		})
		var transition *InvalidTransitionError
		require.ErrorAs(t, err, &transition)
		assert.Equal(t, ExportJobStatusActive, transition.From)
		assert.Equal(t, ExportJobStatusActive, transition.To)
		assert.Equal(t, createOperation, transition.Operation)
	})

	t.Run("validation", func(t *testing.T) {
		err := operationFailure("job-1", createOperation, &api.OrchestrationMetadata{
			RuntimeStatus: api.RUNTIME_STATUS_FAILED,
			FailureDetails: &api.FailureDetails{
				ErrorType:    validationErrorType,
				ErrorMessage: "fallback message",
				Properties:   map[string]any{"message": "explicit message"},
			},
		})
		var validation *ValidationError
		require.ErrorAs(t, err, &validation)
		assert.Equal(t, "explicit message", validation.Message)
	})

	t.Run("validation falls back to the failure message", func(t *testing.T) {
		err := operationFailure("job-1", createOperation, &api.OrchestrationMetadata{
			RuntimeStatus:  api.RUNTIME_STATUS_FAILED,
			FailureDetails: &api.FailureDetails{ErrorType: validationErrorType, ErrorMessage: "fallback"},
		})
		var validation *ValidationError
		require.ErrorAs(t, err, &validation)
		assert.Equal(t, "fallback", validation.Message)
	})

	t.Run("not found", func(t *testing.T) {
		err := operationFailure("job-1", getOperation, &api.OrchestrationMetadata{
			RuntimeStatus:  api.RUNTIME_STATUS_FAILED,
			FailureDetails: &api.FailureDetails{ErrorType: notFoundErrorType},
		})
		var notFound *NotFoundError
		require.ErrorAs(t, err, &notFound)
		assert.Equal(t, "job-1", notFound.JobID)
	})

	t.Run("unmapped failures stay generic", func(t *testing.T) {
		err := operationFailure("job-1", createOperation, &api.OrchestrationMetadata{
			RuntimeStatus:  api.RUNTIME_STATUS_TERMINATED,
			FailureDetails: &api.FailureDetails{ErrorType: "Contoso.Boom", ErrorMessage: "boom"},
		})
		var operation *OperationError
		require.ErrorAs(t, err, &operation)
		assert.Equal(t, api.RUNTIME_STATUS_TERMINATED, operation.RuntimeStatus)
	})

	t.Run("missing metadata", func(t *testing.T) {
		err := operationFailure("job-1", createOperation, nil)
		var operation *OperationError
		require.ErrorAs(t, err, &operation)
		assert.Equal(t, "job-1", operation.JobID)
	})
}

func TestPropertyReaders(t *testing.T) {
	properties := map[string]any{
		"int":     7,
		"int32":   int32(8),
		"int64":   int64(9),
		"float":   float64(10),
		"string":  "value",
		"empty":   "",
		"unknown": []int{1},
	}
	assert.Equal(t, 7, intProperty(properties, "int"))
	assert.Equal(t, 8, intProperty(properties, "int32"))
	assert.Equal(t, 9, intProperty(properties, "int64"))
	assert.Equal(t, 10, intProperty(properties, "float"))
	assert.Equal(t, 0, intProperty(properties, "unknown"))
	assert.Equal(t, 0, intProperty(properties, "absent"))
	assert.Equal(t, 0, intProperty(nil, "absent"))

	assert.Equal(t, "value", stringProperty(properties, "string", "fallback"))
	assert.Equal(t, "fallback", stringProperty(properties, "empty", "fallback"))
	assert.Equal(t, "fallback", stringProperty(properties, "absent", "fallback"))
	assert.Equal(t, "fallback", stringProperty(nil, "absent", "fallback"))
}

func TestWithJobIDOnlyFillsMissingIdentifiers(t *testing.T) {
	filled := withJobID(&ValidationError{Message: "bad"}, "job-1")
	var validation *ValidationError
	require.ErrorAs(t, filled, &validation)
	assert.Equal(t, "job-1", validation.JobID)

	preserved := withJobID(&ValidationError{JobID: "original", Message: "bad"}, "job-1")
	require.ErrorAs(t, preserved, &validation)
	assert.Equal(t, "original", validation.JobID)

	other := errors.New("other")
	assert.Equal(t, other, withJobID(other, "job-1"))
}

// TestSummarizeFailures pins the single bounded summarizer the entity and the
// orchestration share, so both report the same detail under the same limit.
func TestSummarizeFailures(t *testing.T) {
	assert.Equal(t, "no failure details available", summarizeFailures(nil))
	assert.Equal(t, "no failure details available", summarizeFailures([]ExportFailure{}))

	failures := make([]ExportFailure, 0, 12)
	for i := 0; i < 12; i++ {
		failures = append(failures, ExportFailure{InstanceID: fmt.Sprintf("i%d", i), Reason: "boom"})
	}
	summary := summarizeFailures(failures)
	assert.Contains(t, summary, "InstanceId: i0, Reason: boom")
	assert.Contains(t, summary, "InstanceId: i9, Reason: boom")
	assert.NotContains(t, summary, "InstanceId: i10")
	assert.Contains(t, summary, "and 2 more failures")
	assert.Equal(t, maxSummarizedFailures, strings.Count(summary, "InstanceId: "))
}
