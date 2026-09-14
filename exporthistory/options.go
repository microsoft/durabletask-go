package exporthistory

import (
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/microsoft/durabletask-go/api"
)

// JobCreationOptions configures a new export job.
//
// Zero values select defaults: an empty JobID generates one, a nil Format uses
// [DefaultExportFormat], an empty RuntimeStatus exports every terminal status,
// and a zero MaxInstancesPerBatch uses [DefaultMaxInstancesPerBatch]. A zero
// CompletedTimeTo means the window has no upper bound, which is required for
// [ExportModeContinuous] and rejected for [ExportModeBatch].
//
// Destination may be left nil when the client is configured with a default
// container; the client fills it in before the job reaches the entity.
type JobCreationOptions struct {
	JobID                string
	Mode                 ExportMode
	CompletedTimeFrom    time.Time
	CompletedTimeTo      time.Time
	Destination          *ExportDestination
	Format               *ExportFormat
	RuntimeStatus        []api.OrchestrationStatus
	MaxInstancesPerBatch int
}

// jobCreationJSON is the .NET-compatible wire shape of JobCreationOptions.
type jobCreationJSON struct {
	JobID                string                    `json:"JobId"`
	Mode                 ExportMode                `json:"Mode"`
	CompletedTimeFrom    time.Time                 `json:"CompletedTimeFrom"`
	CompletedTimeTo      *time.Time                `json:"CompletedTimeTo"`
	Destination          *ExportDestination        `json:"Destination,omitempty"`
	Format               *ExportFormat             `json:"Format,omitempty"`
	RuntimeStatus        []api.OrchestrationStatus `json:"RuntimeStatus,omitempty"`
	MaxInstancesPerBatch int                       `json:"MaxInstancesPerBatch"`
}

func (o JobCreationOptions) MarshalJSON() ([]byte, error) {
	return json.Marshal(jobCreationJSON{
		JobID:                o.JobID,
		Mode:                 o.Mode,
		CompletedTimeFrom:    o.CompletedTimeFrom,
		CompletedTimeTo:      optionalTime(o.CompletedTimeTo),
		Destination:          o.Destination,
		Format:               o.Format,
		RuntimeStatus:        o.RuntimeStatus,
		MaxInstancesPerBatch: o.MaxInstancesPerBatch,
	})
}

func (o *JobCreationOptions) UnmarshalJSON(data []byte) error {
	var decoded jobCreationJSON
	if err := json.Unmarshal(data, &decoded); err != nil {
		return err
	}
	*o = JobCreationOptions{
		JobID:                decoded.JobID,
		Mode:                 decoded.Mode,
		CompletedTimeFrom:    decoded.CompletedTimeFrom,
		Destination:          decoded.Destination,
		Format:               decoded.Format,
		RuntimeStatus:        decoded.RuntimeStatus,
		MaxInstancesPerBatch: decoded.MaxInstancesPerBatch,
	}
	if decoded.CompletedTimeTo != nil {
		o.CompletedTimeTo = *decoded.CompletedTimeTo
	}
	return nil
}

// isZero reports whether the options carry no caller-supplied value, which is
// how an entity operation invoked without an input is detected.
func (o JobCreationOptions) isZero() bool {
	return o.JobID == "" &&
		o.Mode == 0 &&
		o.CompletedTimeFrom.IsZero() &&
		o.CompletedTimeTo.IsZero() &&
		o.Destination == nil &&
		o.Format == nil &&
		len(o.RuntimeStatus) == 0 &&
		o.MaxInstancesPerBatch == 0
}

// Normalize validates the options against the wall clock and returns a copy
// with every default applied. It generates a job ID when one is not supplied,
// so calling it twice produces two different jobs.
func (o JobCreationOptions) Normalize() (JobCreationOptions, error) {
	return o.normalize(time.Now().UTC(), 0)
}

// Validate reports whether the options are usable, applying the same rules as
// [JobCreationOptions.Normalize] without mutating anything.
func (o JobCreationOptions) Validate() error {
	_, err := o.normalize(time.Now().UTC(), 0)
	return err
}

// MaxCreationClockSkew is how far a batch job's CompletedTimeTo may sit ahead of
// the worker's clock and still be accepted by the ExportJob entity.
//
// A client validates the upper bound strictly against its own clock, but the
// entity runs on a worker whose clock is independent. Without this tolerance a
// window the client accepted would be rejected by a worker running slightly
// behind, so the entity allows a bounded skew while clients stay strict.
const MaxCreationClockSkew = 5 * time.Minute

// normalize applies validation and defaults against an explicit clock. A
// positive futureSkew relaxes only the "upper bound is not in the future" rule,
// which is how entity-side validation absorbs client/worker clock skew without
// shifting the window a continuous job starts from.
func (o JobCreationOptions) normalize(now time.Time, futureSkew time.Duration) (JobCreationOptions, error) {
	normalized := o
	normalized.RuntimeStatus = slices.Clone(o.RuntimeStatus)

	if strings.TrimSpace(normalized.JobID) == "" {
		normalized.JobID = newCompactUUID()
	}
	if err := validateJobID(normalized.JobID); err != nil {
		return JobCreationOptions{}, err
	}
	// invalidf reports a validation failure against the job ID resolved above,
	// which every rule below shares.
	invalidf := func(format string, args ...any) (JobCreationOptions, error) {
		return JobCreationOptions{}, &ValidationError{
			JobID:   normalized.JobID,
			Message: fmt.Sprintf(format, args...),
		}
	}

	switch normalized.Mode {
	case ExportModeBatch:
		switch {
		case normalized.CompletedTimeFrom.IsZero():
			return invalidf("CompletedTimeFrom is required for Batch export mode")
		case normalized.CompletedTimeTo.IsZero():
			return invalidf("CompletedTimeTo is required for Batch export mode")
		case !normalized.CompletedTimeTo.After(normalized.CompletedTimeFrom):
			return invalidf(
				"CompletedTimeTo (%s) must be greater than CompletedTimeFrom (%s) for Batch export mode",
				formatInstant(normalized.CompletedTimeTo), formatInstant(normalized.CompletedTimeFrom))
		case normalized.CompletedTimeTo.After(now.Add(futureSkew)):
			return invalidf(
				"CompletedTimeTo (%s) cannot be in the future; it must be less than or equal to the current time (%s)",
				formatInstant(normalized.CompletedTimeTo), formatInstant(now))
		}
	case ExportModeContinuous:
		if !normalized.CompletedTimeTo.IsZero() {
			return invalidf("CompletedTimeTo is not allowed for Continuous export mode")
		}
		if normalized.CompletedTimeFrom.IsZero() {
			normalized.CompletedTimeFrom = now
		}
	default:
		return invalidf("invalid export mode %d", int(normalized.Mode))
	}

	switch {
	case normalized.MaxInstancesPerBatch == 0:
		normalized.MaxInstancesPerBatch = DefaultMaxInstancesPerBatch
	case normalized.MaxInstancesPerBatch < 1 || normalized.MaxInstancesPerBatch > MaxInstancesPerBatchLimit:
		return invalidf("MaxInstancesPerBatch must be between 1 and %d, but was %d",
			MaxInstancesPerBatchLimit, normalized.MaxInstancesPerBatch)
	}

	if len(normalized.RuntimeStatus) == 0 {
		normalized.RuntimeStatus = TerminalStatuses()
	} else if err := validateTerminalStatuses(normalized.RuntimeStatus); err != nil {
		return JobCreationOptions{}, withJobID(err, normalized.JobID)
	}

	if normalized.Format == nil {
		format := DefaultExportFormat()
		normalized.Format = &format
	} else {
		format := *normalized.Format
		if !format.Kind.IsValid() {
			return invalidf("invalid export format kind %d", int(format.Kind))
		}
		if format.SchemaVersion == "" {
			format.SchemaVersion = DefaultSchemaVersion
		}
		normalized.Format = &format
	}

	if normalized.Destination != nil {
		destination := *normalized.Destination
		if err := destination.Validate(); err != nil {
			return JobCreationOptions{}, withJobID(err, normalized.JobID)
		}
		normalized.Destination = &destination
	}

	return normalized, nil
}

// configuration converts normalized options into the durable job configuration.
// The destination must already be resolved.
func (o JobCreationOptions) configuration() (*ExportJobConfiguration, error) {
	if o.Destination == nil {
		return nil, &ValidationError{JobID: o.JobID, Message: "export destination is required"}
	}
	if err := o.Destination.Validate(); err != nil {
		return nil, withJobID(err, o.JobID)
	}
	format := DefaultExportFormat()
	if o.Format != nil {
		format = *o.Format
	}
	return &ExportJobConfiguration{
		Mode: o.Mode,
		Filter: ExportFilter{
			CompletedTimeFrom: o.CompletedTimeFrom,
			CompletedTimeTo:   o.CompletedTimeTo,
			RuntimeStatus:     slices.Clone(o.RuntimeStatus),
		},
		Destination:          *o.Destination,
		Format:               format,
		MaxParallelExports:   DefaultMaxParallelExports,
		MaxInstancesPerBatch: o.MaxInstancesPerBatch,
	}, nil
}

func newCompactUUID() string {
	return strings.ReplaceAll(uuid.NewString(), "-", "")
}

// validateJobID rejects IDs that would produce an ambiguous entity instance ID
// or an unusable orchestration instance ID.
func validateJobID(jobID string) error {
	switch {
	case strings.TrimSpace(jobID) == "":
		return &ValidationError{Message: "export job ID is required"}
	case strings.Contains(jobID, "@"):
		return &ValidationError{JobID: jobID, Message: "export job ID must not contain '@'"}
	case strings.ContainsAny(jobID, "\x00\r\n\t"):
		return &ValidationError{JobID: jobID, Message: "export job ID must not contain control characters"}
	default:
		return nil
	}
}

func withJobID(err error, jobID string) error {
	var validation *ValidationError
	if errors.As(err, &validation) && validation.JobID == "" {
		return &ValidationError{JobID: jobID, Message: validation.Message}
	}
	return err
}

func formatInstant(value time.Time) string {
	return value.UTC().Format(time.RFC3339Nano)
}
