package exporthistory

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/microsoft/durabletask-go/api"
)

const (
	// ExportJobEntityName is the system entity that owns export job state.
	ExportJobEntityName = "ExportJob"
	// ExportJobOrchestratorName is the system orchestrator that performs the export work.
	ExportJobOrchestratorName = "ExportJobOrchestrator"
	// ExecuteExportJobOperationOrchestratorName is the system orchestrator that
	// invokes a single export job entity operation on behalf of a client.
	ExecuteExportJobOperationOrchestratorName = "ExecuteExportJobOperationOrchestrator"
	// ListTerminalInstancesActivityName lists a page of terminal instance IDs.
	ListTerminalInstancesActivityName = "ListTerminalInstancesActivity"
	// ExportInstanceHistoryActivityName exports one instance's history.
	ExportInstanceHistoryActivityName = "ExportInstanceHistoryActivity"

	// OrchestratorInstanceIDPrefix prefixes each generation-specific export
	// orchestration ID. Read ExportJobDescription.OrchestratorInstanceID for
	// the current generation's ID.
	OrchestratorInstanceIDPrefix = "ExportJob-"

	createOperation           = "Create"
	getOperation              = "Get"
	runOperation              = "Run"
	commitCheckpointOperation = "CommitCheckpoint"
	markAsCompletedOperation  = "MarkAsCompleted"
	markAsFailedOperation     = "MarkAsFailed"
	deleteOperation           = "Delete"
)

// Defaults applied when a job creation option is not supplied.
const (
	DefaultMaxInstancesPerBatch = 100
	// MaxInstancesPerBatchLimit is the inclusive upper bound accepted for
	// JobCreationOptions.MaxInstancesPerBatch.
	MaxInstancesPerBatchLimit = 1000
	// DefaultMaxParallelExports bounds concurrent per-instance export activities.
	DefaultMaxParallelExports = 32
	// DefaultSchemaVersion is the schema version stamped on exported objects.
	DefaultSchemaVersion = "1.0"
	// DefaultJobQueryPageSize is used when ExportJobQuery.PageSize is zero.
	DefaultJobQueryPageSize = 100
)

const terminalStatusesValidationMessage = "export supports terminal orchestration statuses only; " +
	"valid statuses are COMPLETED, FAILED, and TERMINATED"

// EntityID returns the export job entity ID for jobID.
func EntityID(jobID string) api.EntityID {
	return api.NewEntityID(ExportJobEntityName, jobID)
}

// ExportMode selects how an export job consumes its filter window.
type ExportMode int

const (
	// ExportModeBatch exports a fixed completion-time window and then completes.
	ExportModeBatch ExportMode = 1
	// ExportModeContinuous tails terminal instances indefinitely.
	ExportModeContinuous ExportMode = 2
)

func (m ExportMode) String() string {
	switch m {
	case ExportModeBatch:
		return "Batch"
	case ExportModeContinuous:
		return "Continuous"
	default:
		return fmt.Sprintf("ExportMode(%d)", int(m))
	}
}

// IsValid reports whether m is a mode the export job supports.
func (m ExportMode) IsValid() bool {
	return m == ExportModeBatch || m == ExportModeContinuous
}

// ExportJobStatus is the persisted lifecycle status of an export job.
type ExportJobStatus int

const (
	// ExportJobStatusPending is the zero value: the entity has no configuration yet.
	ExportJobStatusPending ExportJobStatus = iota
	// ExportJobStatusActive means the job is running.
	ExportJobStatusActive
	// ExportJobStatusFailed means the job stopped because of an error.
	ExportJobStatusFailed
	// ExportJobStatusCompleted means the job exported its whole window.
	ExportJobStatusCompleted
)

func (s ExportJobStatus) String() string {
	switch s {
	case ExportJobStatusPending:
		return "Pending"
	case ExportJobStatusActive:
		return "Active"
	case ExportJobStatusFailed:
		return "Failed"
	case ExportJobStatusCompleted:
		return "Completed"
	default:
		return fmt.Sprintf("ExportJobStatus(%d)", int(s))
	}
}

// IsValid reports whether s is a persisted export job status.
func (s ExportJobStatus) IsValid() bool {
	return s >= ExportJobStatusPending && s <= ExportJobStatusCompleted
}

// ExportFormatKind selects the serialization of an exported history.
type ExportFormatKind int

const (
	// ExportFormatJSONL writes one history event per line and gzip-compresses the object.
	ExportFormatJSONL ExportFormatKind = iota
	// ExportFormatJSON writes an uncompressed JSON array of history events.
	ExportFormatJSON
)

func (k ExportFormatKind) String() string {
	switch k {
	case ExportFormatJSONL:
		return "Jsonl"
	case ExportFormatJSON:
		return "Json"
	default:
		return fmt.Sprintf("ExportFormatKind(%d)", int(k))
	}
}

// IsValid reports whether k is a supported format kind.
func (k ExportFormatKind) IsValid() bool {
	return k == ExportFormatJSONL || k == ExportFormatJSON
}

// MarshalJSON writes the .NET-compatible string form of the format kind.
func (k ExportFormatKind) MarshalJSON() ([]byte, error) {
	if !k.IsValid() {
		return nil, fmt.Errorf("invalid export format kind %d", int(k))
	}
	return json.Marshal(k.String())
}

// UnmarshalJSON accepts either the .NET string form or the numeric form.
func (k *ExportFormatKind) UnmarshalJSON(data []byte) error {
	var name string
	if err := json.Unmarshal(data, &name); err == nil {
		switch strings.ToLower(name) {
		case "jsonl":
			*k = ExportFormatJSONL
			return nil
		case "json":
			*k = ExportFormatJSON
			return nil
		default:
			return fmt.Errorf("invalid export format kind %q", name)
		}
	}
	var value int
	if err := json.Unmarshal(data, &value); err != nil {
		return fmt.Errorf("invalid export format kind: %w", err)
	}
	kind := ExportFormatKind(value)
	if !kind.IsValid() {
		return fmt.Errorf("invalid export format kind %d", value)
	}
	*k = kind
	return nil
}

// ExportFormat describes the serialization and schema of exported objects.
type ExportFormat struct {
	Kind          ExportFormatKind `json:"Kind"`
	SchemaVersion string           `json:"SchemaVersion"`
}

// DefaultExportFormat is gzip-compressed JSONL at the default schema version.
func DefaultExportFormat() ExportFormat {
	return ExportFormat{Kind: ExportFormatJSONL, SchemaVersion: DefaultSchemaVersion}
}

// FileExtension returns the object suffix used for this format.
func (f ExportFormat) FileExtension() string {
	if f.Kind == ExportFormatJSON {
		return "json"
	}
	return "jsonl.gz"
}

// ContentType returns the MIME type exported objects are stored under.
//
// JSONL objects are gzip-compressed and stored as opaque gzip files, so they
// use application/gzip and carry no content coding: the bytes a reader
// downloads are always the gzip stream the object name promises.
func (f ExportFormat) ContentType() string {
	if f.Kind == ExportFormatJSON {
		return "application/json"
	}
	return "application/gzip"
}

// ExportDestination is the blob container and optional path prefix that
// receives exported objects.
type ExportDestination struct {
	Container string `json:"Container"`
	Prefix    string `json:"Prefix,omitempty"`
}

// Validate reports whether the destination can receive exported objects.
func (d ExportDestination) Validate() error {
	if strings.TrimSpace(d.Container) == "" {
		return &ValidationError{Message: "export destination container is required"}
	}
	if !validBlobContainerName(d.Container) {
		return &ValidationError{
			Message: fmt.Sprintf("export destination container %q is not a valid Azure Blob container name", d.Container),
		}
	}
	if err := validateBlobPrefix(d.Prefix); err != nil {
		return err
	}
	return nil
}

// BlobPath joins the destination prefix with name using a single separator.
func (d ExportDestination) BlobPath(name string) string {
	prefix := strings.TrimRight(d.Prefix, "/")
	if prefix == "" {
		return name
	}
	return prefix + "/" + name
}

// ExportFilter selects the orchestration instances an export job reads.
// Only terminal runtime statuses are supported. A zero CompletedTimeTo means
// the window has no upper bound.
type ExportFilter struct {
	CompletedTimeFrom time.Time
	CompletedTimeTo   time.Time
	RuntimeStatus     []api.OrchestrationStatus
}

// filterJSON is the .NET-compatible wire shape of ExportFilter. An unset upper
// bound must serialize as null rather than as the zero instant.
type filterJSON struct {
	CompletedTimeFrom time.Time                 `json:"CompletedTimeFrom"`
	CompletedTimeTo   *time.Time                `json:"CompletedTimeTo"`
	RuntimeStatus     []api.OrchestrationStatus `json:"RuntimeStatus,omitempty"`
}

func (f ExportFilter) MarshalJSON() ([]byte, error) {
	return json.Marshal(filterJSON{
		CompletedTimeFrom: f.CompletedTimeFrom,
		CompletedTimeTo:   optionalTime(f.CompletedTimeTo),
		RuntimeStatus:     f.RuntimeStatus,
	})
}

func (f *ExportFilter) UnmarshalJSON(data []byte) error {
	var decoded filterJSON
	if err := json.Unmarshal(data, &decoded); err != nil {
		return err
	}
	*f = ExportFilter{
		CompletedTimeFrom: decoded.CompletedTimeFrom,
		RuntimeStatus:     decoded.RuntimeStatus,
	}
	if decoded.CompletedTimeTo != nil {
		f.CompletedTimeTo = *decoded.CompletedTimeTo
	}
	return nil
}

func optionalTime(value time.Time) *time.Time {
	if value.IsZero() {
		return nil
	}
	return &value
}

// ExportCheckpoint records the durable pagination cursor of an export job.
type ExportCheckpoint struct {
	LastInstanceKey string `json:"LastInstanceKey,omitempty"`
}

// ExportFailure describes one instance that could not be exported.
type ExportFailure struct {
	InstanceID   string    `json:"InstanceId"`
	Reason       string    `json:"Reason"`
	AttemptCount int       `json:"AttemptCount"`
	LastAttempt  time.Time `json:"LastAttempt"`
}

// ExportJobConfiguration is the durable configuration of an export job.
type ExportJobConfiguration struct {
	Mode                 ExportMode        `json:"Mode"`
	Filter               ExportFilter      `json:"Filter"`
	Destination          ExportDestination `json:"Destination"`
	Format               ExportFormat      `json:"Format"`
	MaxParallelExports   int               `json:"MaxParallelExports"`
	MaxInstancesPerBatch int               `json:"MaxInstancesPerBatch"`
}

// ExportJobState is the persisted entity state of an export job.
type ExportJobState struct {
	Status                 ExportJobStatus         `json:"Status"`
	Config                 *ExportJobConfiguration `json:"Config,omitempty"`
	Checkpoint             *ExportCheckpoint       `json:"Checkpoint,omitempty"`
	CreatedAt              *time.Time              `json:"CreatedAt,omitempty"`
	LastModifiedAt         *time.Time              `json:"LastModifiedAt,omitempty"`
	LastCheckpointTime     *time.Time              `json:"LastCheckpointTime,omitempty"`
	LastError              string                  `json:"LastError,omitempty"`
	ScannedInstances       int64                   `json:"ScannedInstances"`
	ExportedInstances      int64                   `json:"ExportedInstances"`
	OrchestratorInstanceID string                  `json:"OrchestratorInstanceId,omitempty"`
	// RunToken identifies the job's current run generation. Every Create mints
	// a new one, so a run that started before the job was deleted and recreated
	// carries a stale token and is fenced out of the new generation's state.
	// Mutations require a matching, nonempty token.
	RunToken string `json:"RunToken,omitempty"`
}

// ExportJobDescription is the client-facing view of an export job.
type ExportJobDescription struct {
	JobID          string
	Status         ExportJobStatus
	CreatedAt      time.Time
	LastModifiedAt time.Time
	Config         *ExportJobConfiguration
	// OrchestratorInstanceID identifies the current generation, reserved during
	// Create before its Run signal is delivered. Recreation assigns a new ID.
	OrchestratorInstanceID string
	ScannedInstances       int64
	ExportedInstances      int64
	LastError              string
	Checkpoint             *ExportCheckpoint
	LastCheckpointTime     time.Time
}

// ExportJobQuery filters a single page of export jobs. Status and creation-time
// filters are applied after the service returns the entity page, so a page can
// contain fewer than PageSize items.
type ExportJobQuery struct {
	Status *ExportJobStatus
	// JobIDPrefix restricts results to jobs whose ID starts with this value.
	JobIDPrefix string
	// CreatedFrom is an exclusive lower creation-time bound, matching .NET.
	CreatedFrom time.Time
	// CreatedTo is an exclusive upper creation-time bound, matching .NET.
	CreatedTo         time.Time
	PageSize          int32
	ContinuationToken string
}

// ExportJobQueryResult is one page of export jobs.
type ExportJobQueryResult struct {
	Jobs              []*ExportJobDescription
	ContinuationToken string
}

// CommitCheckpointRequest is the entity input that records a batch's progress.
// A nil Checkpoint keeps the current cursor so the same batch can be retried;
// combined with a non-empty Failures list it implicitly fails the job.
type CommitCheckpointRequest struct {
	ScannedInstances  int64             `json:"ScannedInstances"`
	ExportedInstances int64             `json:"ExportedInstances"`
	Checkpoint        *ExportCheckpoint `json:"Checkpoint,omitempty"`
	Failures          []ExportFailure   `json:"Failures,omitempty"`
	// RunToken fences the commit to the run generation that produced it. See
	// [ExportJobState.RunToken].
	RunToken string `json:"RunToken,omitempty"`
}

// RunJobRequest is the entity input of the Run operation, which starts the
// job's export orchestration.
type RunJobRequest struct {
	// RunToken fences the run signal to the generation that emitted it. See
	// [ExportJobState.RunToken].
	RunToken string `json:"RunToken,omitempty"`
}

// MarkAsCompletedRequest is the entity input of the MarkAsCompleted operation.
type MarkAsCompletedRequest struct {
	// RunToken fences the completion to the run generation that produced it.
	// See [ExportJobState.RunToken].
	RunToken string `json:"RunToken,omitempty"`
}

// MarkAsFailedRequest is the entity input of the MarkAsFailed operation.
type MarkAsFailedRequest struct {
	// RunToken fences the failure to the run generation that produced it. See
	// [ExportJobState.RunToken].
	RunToken string `json:"RunToken,omitempty"`
	// Error is the message recorded on the job. It is optional.
	Error string `json:"Error,omitempty"`
}

// ExportJobOperationRequest is the input consumed by
// ExecuteExportJobOperationOrchestrator.
type ExportJobOperationRequest struct {
	EntityID      api.EntityID `json:"EntityId"`
	OperationName string       `json:"OperationName"`
	Input         any          `json:"Input,omitempty"`
}

// ExportJobRunRequest is the input consumed by ExportJobOrchestrator.
type ExportJobRunRequest struct {
	JobEntityID     api.EntityID `json:"JobEntityId"`
	ProcessedCycles int          `json:"ProcessedCycles"`
	// RunToken is the job generation this orchestration run belongs to. Every
	// entity mutation the run performs carries it, so a run left over from a
	// deleted-and-recreated job cannot alter the new one. See
	// [ExportJobState.RunToken].
	RunToken string `json:"RunToken,omitempty"`
	// ContinuedExecution marks an execution that resumed through ContinueAsNew
	// rather than a fresh start. Its zero value identifies the first execution,
	// which is the only one that treats a missing job as an error rather than
	// as a job that was deleted while it ran.
	ContinuedExecution bool `json:"ContinuedExecution,omitempty"`
}

// ListTerminalInstancesRequest is the input consumed by ListTerminalInstancesActivity.
// A zero CompletedTimeTo means the window has no upper bound.
type ListTerminalInstancesRequest struct {
	CompletedTimeFrom    time.Time                 `json:"CompletedTimeFrom"`
	CompletedTimeTo      *time.Time                `json:"CompletedTimeTo"`
	RuntimeStatus        []api.OrchestrationStatus `json:"RuntimeStatus,omitempty"`
	LastInstanceKey      string                    `json:"LastInstanceKey,omitempty"`
	MaxInstancesPerBatch int                       `json:"MaxInstancesPerBatch"`
}

// InstancePage is one page of terminal instance IDs plus the next cursor.
type InstancePage struct {
	InstanceIDs    []string          `json:"InstanceIds"`
	NextCheckpoint *ExportCheckpoint `json:"NextCheckpoint,omitempty"`
}

// ExportRequest is the input consumed by ExportInstanceHistoryActivity.
type ExportRequest struct {
	InstanceID  string            `json:"InstanceId"`
	Destination ExportDestination `json:"Destination"`
	Format      ExportFormat      `json:"Format"`
}

// ExportResult reports the outcome of exporting one instance. Per-instance
// failures are collected rather than thrown so a batch can report every failing
// instance at once.
type ExportResult struct {
	InstanceID string `json:"InstanceId"`
	Success    bool   `json:"Success"`
	Error      string `json:"Error,omitempty"`
	BlobPath   string `json:"BlobPath,omitempty"`
	EventCount int    `json:"EventCount,omitempty"`
}

// TerminalStatuses returns the orchestration runtime statuses an export job
// accepts, in the .NET declaration order.
func TerminalStatuses() []api.OrchestrationStatus {
	return []api.OrchestrationStatus{
		api.RUNTIME_STATUS_COMPLETED,
		api.RUNTIME_STATUS_FAILED,
		api.RUNTIME_STATUS_TERMINATED,
	}
}

func isTerminalStatus(status api.OrchestrationStatus) bool {
	switch status {
	case api.RUNTIME_STATUS_COMPLETED, api.RUNTIME_STATUS_FAILED, api.RUNTIME_STATUS_TERMINATED:
		return true
	default:
		return false
	}
}

// validateTerminalStatuses rejects a filter that names any status an export job
// cannot read. It is shared by client-side normalization and the listing
// activity so both reject the same set with the same message.
func validateTerminalStatuses(statuses []api.OrchestrationStatus) error {
	for _, status := range statuses {
		if !isTerminalStatus(status) {
			return &ValidationError{Message: terminalStatusesValidationMessage}
		}
	}
	return nil
}

func (state *ExportJobState) description(jobID string) *ExportJobDescription {
	description := &ExportJobDescription{
		JobID:                  jobID,
		Status:                 state.Status,
		Config:                 state.Config,
		OrchestratorInstanceID: state.OrchestratorInstanceID,
		ScannedInstances:       state.ScannedInstances,
		ExportedInstances:      state.ExportedInstances,
		LastError:              state.LastError,
		Checkpoint:             state.Checkpoint,
	}
	if state.CreatedAt != nil {
		description.CreatedAt = *state.CreatedAt
	}
	if state.LastModifiedAt != nil {
		description.LastModifiedAt = *state.LastModifiedAt
	}
	if state.LastCheckpointTime != nil {
		description.LastCheckpointTime = *state.LastCheckpointTime
	}
	return description
}

func (query ExportJobQuery) matches(description *ExportJobDescription) bool {
	if query.Status != nil && description.Status != *query.Status {
		return false
	}
	if !query.CreatedFrom.IsZero() && !description.CreatedAt.After(query.CreatedFrom) {
		return false
	}
	if !query.CreatedTo.IsZero() && !description.CreatedAt.Before(query.CreatedTo) {
		return false
	}
	return true
}
