package exporthistory

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/microsoft/durabletask-go/api"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestJobCreationOptionsNormalize ports the upstream
// ExportJobCreationOptionsTests matrix to the Go normalization entry point.
func TestJobCreationOptionsNormalize(t *testing.T) {
	from := time.Now().UTC().Add(-24 * time.Hour)
	to := time.Now().UTC()

	t.Run("batch mode with valid parameters", func(t *testing.T) {
		options, err := JobCreationOptions{
			Mode:              ExportModeBatch,
			CompletedTimeFrom: from,
			CompletedTimeTo:   to,
			Destination:       &ExportDestination{Container: "test-container"},
		}.Normalize()
		require.NoError(t, err)
		assert.Equal(t, ExportModeBatch, options.Mode)
		assert.Equal(t, from, options.CompletedTimeFrom)
		assert.Equal(t, to, options.CompletedTimeTo)
		assert.Equal(t, "test-container", options.Destination.Container)
		assert.NotEmpty(t, options.JobID)
		assert.Equal(t, DefaultExportFormat(), *options.Format)
		assert.Equal(t, TerminalStatuses(), options.RuntimeStatus)
		assert.Equal(t, DefaultMaxInstancesPerBatch, options.MaxInstancesPerBatch)
	})

	t.Run("custom job ID is preserved", func(t *testing.T) {
		options, err := JobCreationOptions{
			JobID:             "custom-job-id",
			Mode:              ExportModeBatch,
			CompletedTimeFrom: from,
			CompletedTimeTo:   to,
		}.Normalize()
		require.NoError(t, err)
		assert.Equal(t, "custom-job-id", options.JobID)
	})

	t.Run("empty job ID generates a stable GUID", func(t *testing.T) {
		options, err := JobCreationOptions{
			Mode:              ExportModeBatch,
			CompletedTimeFrom: from,
			CompletedTimeTo:   to,
		}.Normalize()
		require.NoError(t, err)
		require.Len(t, options.JobID, 32)
		assert.NotContains(t, options.JobID, "-")

		other, err := JobCreationOptions{
			Mode:              ExportModeBatch,
			CompletedTimeFrom: from,
			CompletedTimeTo:   to,
		}.Normalize()
		require.NoError(t, err)
		assert.NotEqual(t, options.JobID, other.JobID)
	})

	t.Run("whitespace job ID generates a GUID", func(t *testing.T) {
		options, err := JobCreationOptions{
			JobID:             "   ",
			Mode:              ExportModeBatch,
			CompletedTimeFrom: from,
			CompletedTimeTo:   to,
		}.Normalize()
		require.NoError(t, err)
		assert.Len(t, options.JobID, 32)
	})

	invalid := []struct {
		name    string
		options JobCreationOptions
		message string
	}{
		{
			name:    "batch without CompletedTimeFrom",
			options: JobCreationOptions{Mode: ExportModeBatch, CompletedTimeTo: to},
			message: "CompletedTimeFrom is required for Batch export mode",
		},
		{
			name:    "batch without CompletedTimeTo",
			options: JobCreationOptions{Mode: ExportModeBatch, CompletedTimeFrom: from},
			message: "CompletedTimeTo is required for Batch export mode",
		},
		{
			name: "batch with CompletedTimeTo before CompletedTimeFrom",
			options: JobCreationOptions{
				Mode:              ExportModeBatch,
				CompletedTimeFrom: to,
				CompletedTimeTo:   from,
			},
			message: "must be greater than CompletedTimeFrom",
		},
		{
			name: "batch with CompletedTimeTo equal to CompletedTimeFrom",
			options: JobCreationOptions{
				Mode:              ExportModeBatch,
				CompletedTimeFrom: from,
				CompletedTimeTo:   from,
			},
			message: "must be greater than CompletedTimeFrom",
		},
		{
			name: "batch with CompletedTimeTo in the future",
			options: JobCreationOptions{
				Mode:              ExportModeBatch,
				CompletedTimeFrom: from,
				CompletedTimeTo:   time.Now().UTC().Add(24 * time.Hour),
			},
			message: "cannot be in the future",
		},
		{
			name:    "continuous with CompletedTimeTo",
			options: JobCreationOptions{Mode: ExportModeContinuous, CompletedTimeTo: to},
			message: "CompletedTimeTo is not allowed for Continuous export mode",
		},
		{
			name:    "unknown mode",
			options: JobCreationOptions{Mode: ExportMode(999), CompletedTimeFrom: from, CompletedTimeTo: to},
			message: "invalid export mode 999",
		},
		{
			name:    "zero mode",
			options: JobCreationOptions{CompletedTimeFrom: from, CompletedTimeTo: to},
			message: "invalid export mode 0",
		},
		{
			name: "job ID containing an entity separator",
			options: JobCreationOptions{
				JobID:             "job@id",
				Mode:              ExportModeBatch,
				CompletedTimeFrom: from,
				CompletedTimeTo:   to,
			},
			message: "must not contain '@'",
		},
		{
			name: "invalid destination container",
			options: JobCreationOptions{
				Mode:              ExportModeBatch,
				CompletedTimeFrom: from,
				CompletedTimeTo:   to,
				Destination:       &ExportDestination{Container: "Invalid_Container"},
			},
			message: "is not a valid Azure Blob container name",
		},
		{
			name: "destination prefix escaping the container",
			options: JobCreationOptions{
				Mode:              ExportModeBatch,
				CompletedTimeFrom: from,
				CompletedTimeTo:   to,
				Destination:       &ExportDestination{Container: "container", Prefix: "a/../b"},
			},
			message: "relative path segments",
		},
		{
			name: "invalid format kind",
			options: JobCreationOptions{
				Mode:              ExportModeBatch,
				CompletedTimeFrom: from,
				CompletedTimeTo:   to,
				Format:            &ExportFormat{Kind: ExportFormatKind(7)},
			},
			message: "invalid export format kind 7",
		},
	}
	for _, test := range invalid {
		t.Run(test.name, func(t *testing.T) {
			_, err := test.options.Normalize()
			require.Error(t, err)
			require.ErrorIs(t, err, ErrValidation)
			var validation *ValidationError
			require.ErrorAs(t, err, &validation)
			assert.Contains(t, err.Error(), test.message)
		})
	}

	t.Run("continuous mode defaults CompletedTimeFrom to now", func(t *testing.T) {
		before := time.Now().UTC()
		options, err := JobCreationOptions{Mode: ExportModeContinuous}.Normalize()
		require.NoError(t, err)
		after := time.Now().UTC()
		assert.False(t, options.CompletedTimeFrom.Before(before))
		assert.False(t, options.CompletedTimeFrom.After(after))
		assert.True(t, options.CompletedTimeTo.IsZero())
	})

	t.Run("continuous mode preserves an explicit CompletedTimeFrom", func(t *testing.T) {
		options, err := JobCreationOptions{Mode: ExportModeContinuous, CompletedTimeFrom: from}.Normalize()
		require.NoError(t, err)
		assert.Equal(t, from, options.CompletedTimeFrom)
	})
}

// TestJobCreationOptionsFutureWindowSkew pins the two-sided rule for a batch
// window's upper bound: clients validate strictly against their own clock, while
// the entity absorbs a bounded skew so a worker whose clock trails the client's
// does not reject a job the client accepted.
func TestJobCreationOptionsFutureWindowSkew(t *testing.T) {
	now := time.Date(2024, time.April, 1, 12, 0, 0, 0, time.UTC)
	base := JobCreationOptions{
		JobID:             "job-1",
		Mode:              ExportModeBatch,
		CompletedTimeFrom: now.Add(-time.Hour),
	}

	t.Run("strict validation rejects any future upper bound", func(t *testing.T) {
		options := base
		options.CompletedTimeTo = now.Add(time.Second)
		_, err := options.normalize(now, 0)
		require.ErrorIs(t, err, ErrValidation)
		assert.Contains(t, err.Error(), "cannot be in the future")
	})

	t.Run("the entity tolerates an upper bound within the documented skew", func(t *testing.T) {
		for _, ahead := range []time.Duration{time.Second, MaxCreationClockSkew / 2, MaxCreationClockSkew} {
			options := base
			options.CompletedTimeTo = now.Add(ahead)
			normalized, err := options.normalize(now, MaxCreationClockSkew)
			require.NoError(t, err, "%s ahead", ahead)
			assert.Equal(t, options.CompletedTimeTo, normalized.CompletedTimeTo)
		}
	})

	t.Run("beyond the skew it is still rejected", func(t *testing.T) {
		options := base
		options.CompletedTimeTo = now.Add(MaxCreationClockSkew + time.Second)
		_, err := options.normalize(now, MaxCreationClockSkew)
		require.ErrorIs(t, err, ErrValidation)
		assert.Contains(t, err.Error(), "cannot be in the future")
	})

	t.Run("the skew never shifts the window a continuous job starts from", func(t *testing.T) {
		normalized, err := JobCreationOptions{
			JobID: "job-1",
			Mode:  ExportModeContinuous,
		}.normalize(now, MaxCreationClockSkew)
		require.NoError(t, err)
		assert.Equal(t, now, normalized.CompletedTimeFrom)
	})
}

func TestJobCreationOptionsMaxInstancesPerBatch(t *testing.T) {
	base := JobCreationOptions{
		Mode:              ExportModeBatch,
		CompletedTimeFrom: time.Now().UTC().Add(-time.Hour),
		CompletedTimeTo:   time.Now().UTC(),
	}
	for _, value := range []int{-1, 1001, 2000} {
		options := base
		options.MaxInstancesPerBatch = value
		_, err := options.Normalize()
		require.ErrorIs(t, err, ErrValidation)
		assert.Contains(t, err.Error(), "MaxInstancesPerBatch must be between 1 and 1000")
	}
	for _, value := range []int{1, 100, 500, 1000} {
		options := base
		options.MaxInstancesPerBatch = value
		normalized, err := options.Normalize()
		require.NoError(t, err)
		assert.Equal(t, value, normalized.MaxInstancesPerBatch)
	}
	options := base
	normalized, err := options.Normalize()
	require.NoError(t, err)
	assert.Equal(t, DefaultMaxInstancesPerBatch, normalized.MaxInstancesPerBatch)
}

func TestJobCreationOptionsRuntimeStatusFilters(t *testing.T) {
	base := JobCreationOptions{
		Mode:              ExportModeBatch,
		CompletedTimeFrom: time.Now().UTC().Add(-time.Hour),
		CompletedTimeTo:   time.Now().UTC(),
	}

	t.Run("rejects non-terminal statuses", func(t *testing.T) {
		for _, status := range []api.OrchestrationStatus{
			api.RUNTIME_STATUS_RUNNING,
			api.RUNTIME_STATUS_PENDING,
			api.RUNTIME_STATUS_SUSPENDED,
			api.RUNTIME_STATUS_CONTINUED_AS_NEW,
			api.RUNTIME_STATUS_CANCELED,
		} {
			options := base
			options.RuntimeStatus = []api.OrchestrationStatus{status}
			_, err := options.Normalize()
			require.ErrorIs(t, err, ErrValidation, "status %v must be rejected", status)
			assert.Contains(t, err.Error(), "terminal orchestration statuses only")
		}
	})

	t.Run("accepts terminal statuses", func(t *testing.T) {
		options := base
		options.RuntimeStatus = TerminalStatuses()
		normalized, err := options.Normalize()
		require.NoError(t, err)
		assert.Equal(t, TerminalStatuses(), normalized.RuntimeStatus)
	})

	t.Run("empty and nil default to every terminal status", func(t *testing.T) {
		for _, statuses := range [][]api.OrchestrationStatus{nil, {}} {
			options := base
			options.RuntimeStatus = statuses
			normalized, err := options.Normalize()
			require.NoError(t, err)
			require.Len(t, normalized.RuntimeStatus, 3)
			assert.Equal(t, TerminalStatuses(), normalized.RuntimeStatus)
		}
	})

	t.Run("normalization does not alias the caller slice", func(t *testing.T) {
		statuses := []api.OrchestrationStatus{api.RUNTIME_STATUS_COMPLETED}
		options := base
		options.RuntimeStatus = statuses
		normalized, err := options.Normalize()
		require.NoError(t, err)
		normalized.RuntimeStatus[0] = api.RUNTIME_STATUS_FAILED
		assert.Equal(t, api.RUNTIME_STATUS_COMPLETED, statuses[0])
	})
}

func TestJobCreationOptionsCustomFormat(t *testing.T) {
	options, err := JobCreationOptions{
		Mode:              ExportModeBatch,
		CompletedTimeFrom: time.Now().UTC().Add(-time.Hour),
		CompletedTimeTo:   time.Now().UTC(),
		Format:            &ExportFormat{Kind: ExportFormatJSON, SchemaVersion: "2.0"},
	}.Normalize()
	require.NoError(t, err)
	assert.Equal(t, ExportFormat{Kind: ExportFormatJSON, SchemaVersion: "2.0"}, *options.Format)

	blank, err := JobCreationOptions{
		Mode:              ExportModeBatch,
		CompletedTimeFrom: time.Now().UTC().Add(-time.Hour),
		CompletedTimeTo:   time.Now().UTC(),
		Format:            &ExportFormat{Kind: ExportFormatJSON},
	}.Normalize()
	require.NoError(t, err)
	assert.Equal(t, DefaultSchemaVersion, blank.Format.SchemaVersion)
}

func TestJobCreationOptionsValidate(t *testing.T) {
	require.NoError(t, JobCreationOptions{Mode: ExportModeContinuous}.Validate())
	require.ErrorIs(t, JobCreationOptions{}.Validate(), ErrValidation)
}

// TestJobCreationOptionsJSONRoundTrip pins the .NET-compatible wire shape used
// as the Create entity operation input.
func TestJobCreationOptionsJSONRoundTrip(t *testing.T) {
	from := time.Date(2024, time.March, 1, 10, 0, 0, 0, time.UTC)
	to := time.Date(2024, time.March, 2, 10, 0, 0, 0, time.UTC)
	options := JobCreationOptions{
		JobID:                "job-1",
		Mode:                 ExportModeBatch,
		CompletedTimeFrom:    from,
		CompletedTimeTo:      to,
		Destination:          &ExportDestination{Container: "container", Prefix: "batch-job-1/"},
		Format:               &ExportFormat{Kind: ExportFormatJSONL, SchemaVersion: "1.0"},
		RuntimeStatus:        TerminalStatuses(),
		MaxInstancesPerBatch: 25,
	}
	encoded, err := json.Marshal(options)
	require.NoError(t, err)

	var raw map[string]any
	require.NoError(t, json.Unmarshal(encoded, &raw))
	assert.Equal(t, "job-1", raw["JobId"])
	assert.Equal(t, float64(1), raw["Mode"])
	assert.Equal(t, float64(25), raw["MaxInstancesPerBatch"])
	assert.Contains(t, string(encoded), `"Kind":"Jsonl"`)

	var decoded JobCreationOptions
	require.NoError(t, json.Unmarshal(encoded, &decoded))
	assert.Equal(t, options.JobID, decoded.JobID)
	assert.Equal(t, options.Mode, decoded.Mode)
	assert.True(t, options.CompletedTimeFrom.Equal(decoded.CompletedTimeFrom))
	assert.True(t, options.CompletedTimeTo.Equal(decoded.CompletedTimeTo))
	assert.Equal(t, options.Destination, decoded.Destination)
	assert.Equal(t, options.Format, decoded.Format)
	assert.Equal(t, options.MaxInstancesPerBatch, decoded.MaxInstancesPerBatch)
}

// TestJobCreationOptionsUnsetUpperBoundSerializesNull keeps a continuous job's
// open-ended window distinguishable from the zero instant on the wire.
func TestJobCreationOptionsUnsetUpperBoundSerializesNull(t *testing.T) {
	options, err := JobCreationOptions{JobID: "job", Mode: ExportModeContinuous}.Normalize()
	require.NoError(t, err)
	encoded, err := json.Marshal(options)
	require.NoError(t, err)
	assert.Contains(t, string(encoded), `"CompletedTimeTo":null`)

	var decoded JobCreationOptions
	require.NoError(t, json.Unmarshal(encoded, &decoded))
	assert.True(t, decoded.CompletedTimeTo.IsZero())
}

func TestJobCreationOptionsIsZero(t *testing.T) {
	assert.True(t, JobCreationOptions{}.isZero())
	assert.False(t, JobCreationOptions{Mode: ExportModeBatch}.isZero())
	assert.False(t, JobCreationOptions{JobID: "x"}.isZero())
	assert.False(t, JobCreationOptions{MaxInstancesPerBatch: 1}.isZero())
	assert.False(t, JobCreationOptions{RuntimeStatus: TerminalStatuses()}.isZero())
	assert.False(t, JobCreationOptions{Format: &ExportFormat{}}.isZero())
	assert.False(t, JobCreationOptions{Destination: &ExportDestination{}}.isZero())
	assert.False(t, JobCreationOptions{CompletedTimeFrom: time.Unix(1, 0)}.isZero())
	assert.False(t, JobCreationOptions{CompletedTimeTo: time.Unix(1, 0)}.isZero())
}

func TestJobCreationOptionsConfiguration(t *testing.T) {
	t.Run("requires a destination", func(t *testing.T) {
		_, err := JobCreationOptions{JobID: "job", Mode: ExportModeBatch}.configuration()
		require.ErrorIs(t, err, ErrValidation)
		assert.Contains(t, err.Error(), "export destination is required")
	})

	t.Run("carries filter, format, and limits", func(t *testing.T) {
		from := time.Now().UTC().Add(-time.Hour)
		to := time.Now().UTC()
		options, err := JobCreationOptions{
			JobID:                "job",
			Mode:                 ExportModeBatch,
			CompletedTimeFrom:    from,
			CompletedTimeTo:      to,
			Destination:          &ExportDestination{Container: "container"},
			MaxInstancesPerBatch: 42,
		}.Normalize()
		require.NoError(t, err)
		config, err := options.configuration()
		require.NoError(t, err)
		assert.Equal(t, ExportModeBatch, config.Mode)
		assert.Equal(t, from, config.Filter.CompletedTimeFrom)
		assert.Equal(t, to, config.Filter.CompletedTimeTo)
		assert.Equal(t, TerminalStatuses(), config.Filter.RuntimeStatus)
		assert.Equal(t, "container", config.Destination.Container)
		assert.Equal(t, DefaultExportFormat(), config.Format)
		assert.Equal(t, DefaultMaxParallelExports, config.MaxParallelExports)
		assert.Equal(t, 42, config.MaxInstancesPerBatch)
	})
}

// TestValidateJobIDRejectsControlCharacters keeps a job ID from producing an
// unusable entity or orchestration instance ID.
func TestValidateJobIDRejectsControlCharacters(t *testing.T) {
	for _, jobID := range []string{"", "  ", "a\x00b", "a\nb", "a\tb", "a\rb", "a@b"} {
		require.ErrorIs(t, validateJobID(jobID), ErrValidation, "job ID %q must be rejected", jobID)
	}
	require.NoError(t, validateJobID("job-1"))
	require.NoError(t, validateJobID(strings.Repeat("a", 128)))
}
