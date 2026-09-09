package exporthistory

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/microsoft/durabletask-go/api"
	"github.com/microsoft/durabletask-go/task"
)

// HistorySource supplies the management reads an export job needs. It is
// implemented by [github.com/microsoft/durabletask-go/client.TaskHubGrpcClient]
// and by the Durable Task Scheduler client.
type HistorySource interface {
	// ListInstanceIDs returns one page of instance IDs matching the query.
	ListInstanceIDs(ctx context.Context, query api.InstanceIDQuery) (*api.InstanceIDQueryResult, error)
	// FetchOrchestrationMetadata returns the instance's current metadata.
	FetchOrchestrationMetadata(
		ctx context.Context,
		id api.InstanceID,
		opts ...api.FetchOrchestrationMetadataOptions,
	) (*api.OrchestrationMetadata, error)
	// GetOrchestrationHistory returns the instance's durable history.
	GetOrchestrationHistory(
		ctx context.Context,
		id api.InstanceID,
		query api.HistoryQuery,
	) (*api.OrchestrationHistory, error)
}

// exportRuntime carries the worker-side dependencies shared by the export
// activities.
type exportRuntime struct {
	source      HistorySource
	store       Store
	historyPage api.HistoryQuery
}

// listTerminalInstancesActivity returns one page of terminal instance IDs for
// the job's filter, starting from the job's durable checkpoint.
func (r *exportRuntime) listTerminalInstancesActivity(ctx task.ActivityContext) (any, error) {
	var input ListTerminalInstancesRequest
	if err := ctx.GetInput(&input); err != nil {
		return nil, err
	}
	if r.source == nil {
		return nil, errors.New("export history worker has no configured history source")
	}
	statuses := input.RuntimeStatus
	if len(statuses) == 0 {
		statuses = TerminalStatuses()
	}
	if err := validateTerminalStatuses(statuses); err != nil {
		return nil, err
	}
	pageSize := input.MaxInstancesPerBatch
	if pageSize <= 0 {
		pageSize = DefaultMaxInstancesPerBatch
	}
	var completedTimeTo time.Time
	if input.CompletedTimeTo != nil {
		completedTimeTo = *input.CompletedTimeTo
	}
	page, err := r.source.ListInstanceIDs(ctx.Context(), api.InstanceIDQuery{
		RuntimeStatus:     statuses,
		CompletedTimeFrom: input.CompletedTimeFrom,
		CompletedTimeTo:   completedTimeTo,
		PageSize:          pageSize,
		ContinuationToken: input.LastInstanceKey,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to list terminal orchestration instances: %w", err)
	}
	if page == nil {
		return nil, errors.New("failed to list terminal orchestration instances: no result")
	}
	result := InstancePage{InstanceIDs: make([]string, 0, len(page.InstanceIDs))}
	// A task hub reports the end of the stream by omitting the continuation
	// token. Leaving the checkpoint nil preserves the last opaque backend cursor;
	// a continuous job may re-scan that final page, but deterministic blob names
	// make the repeated writes idempotent.
	if page.ContinuationToken != "" {
		result.NextCheckpoint = &ExportCheckpoint{LastInstanceKey: page.ContinuationToken}
	}
	for _, id := range page.InstanceIDs {
		result.InstanceIDs = append(result.InstanceIDs, string(id))
	}
	return result, nil
}

// permanentExportError marks a per-instance condition that retrying cannot fix,
// such as an instance that does not exist or has not reached a terminal state.
// Transient failures are returned as activity errors instead, so the activity's
// retry policy can recover before the instance is recorded as failed.
type permanentExportError struct{ message string }

func (e *permanentExportError) Error() string { return e.message }

func permanentExportFailure(format string, args ...any) error {
	return &permanentExportError{message: fmt.Sprintf(format, args...)}
}

// exportInstanceHistoryActivity exports one instance's history to the
// destination.
//
// A condition that retrying cannot fix is returned as an unsuccessful
// [ExportResult] so the orchestration can collect every failing instance in a
// page. A transient failure is returned as an activity error so the activity's
// retry policy applies first; the orchestration collects it as a failure only
// after every attempt is exhausted. A malformed request always fails the
// activity.
func (r *exportRuntime) exportInstanceHistoryActivity(ctx task.ActivityContext) (any, error) {
	var input ExportRequest
	if err := ctx.GetInput(&input); err != nil {
		return nil, err
	}
	if input.InstanceID == "" {
		return nil, &ValidationError{Message: "export request instance ID is required"}
	}
	if err := input.Destination.Validate(); err != nil {
		return nil, err
	}
	if !input.Format.Kind.IsValid() {
		return nil, &ValidationError{
			Message: fmt.Sprintf("invalid export format kind %d", int(input.Format.Kind)),
		}
	}
	if r.source == nil {
		return nil, errors.New("export history worker has no configured history source")
	}
	if r.store == nil {
		return nil, errors.New("export history worker has no configured store")
	}

	result, err := r.exportInstance(ctx.Context(), input)
	if err != nil {
		var permanent *permanentExportError
		if errors.As(err, &permanent) {
			return ExportResult{InstanceID: input.InstanceID, Success: false, Error: permanent.Error()}, nil
		}
		return nil, err
	}
	return result, nil
}

func (r *exportRuntime) exportInstance(ctx context.Context, input ExportRequest) (ExportResult, error) {
	instanceID := api.InstanceID(input.InstanceID)
	metadata, err := r.source.FetchOrchestrationMetadata(ctx, instanceID)
	if err != nil {
		if errors.Is(err, api.ErrInstanceNotFound) {
			return ExportResult{}, permanentExportFailure("instance %s not found", input.InstanceID)
		}
		return ExportResult{}, fmt.Errorf("failed to read instance %s metadata: %w", input.InstanceID, err)
	}
	if metadata == nil {
		return ExportResult{}, permanentExportFailure("instance %s not found", input.InstanceID)
	}
	if !isTerminalStatus(metadata.RuntimeStatus) {
		return ExportResult{}, permanentExportFailure(
			"instance %s is not in a completed state", input.InstanceID)
	}
	if metadata.ExecutionID == "" {
		return ExportResult{}, fmt.Errorf("instance %s metadata is missing an execution ID", input.InstanceID)
	}

	query := r.historyPage
	query.ExecutionID = metadata.ExecutionID
	history, err := r.source.GetOrchestrationHistory(ctx, instanceID, query)
	if err != nil {
		return ExportResult{}, fmt.Errorf("failed to read instance %s history: %w", input.InstanceID, err)
	}
	if history == nil {
		return ExportResult{}, fmt.Errorf("instance %s returned no history", input.InstanceID)
	}
	if history.ExecutionID != metadata.ExecutionID {
		return ExportResult{}, fmt.Errorf("instance %s history execution ID %q does not match metadata execution %q",
			input.InstanceID, history.ExecutionID, metadata.ExecutionID)
	}

	content, contentType, err := serializeHistory(history.Events, input.Format)
	if err != nil {
		return ExportResult{}, err
	}

	completedAt := completionTimestamp(metadata)
	name := blobObjectName(completedAt, input.InstanceID, input.Format)
	path := input.Destination.BlobPath(name)
	object := ExportObject{
		Container:   input.Destination.Container,
		Name:        path,
		Content:     content,
		ContentType: contentType,
		Metadata: map[string]string{
			"instanceId":    input.InstanceID,
			"executionId":   history.ExecutionID,
			"schemaVersion": input.Format.SchemaVersion,
		},
	}
	if err := r.store.Write(ctx, object); err != nil {
		return ExportResult{}, err
	}
	return ExportResult{
		InstanceID: input.InstanceID,
		Success:    true,
		BlobPath:   path,
		EventCount: len(history.Events),
	}, nil
}

// completionTimestamp picks the instant that identifies a terminal instance.
// The service reports a dedicated completion time for most instances; fall back
// to the last update so the object name stays stable and collision-resistant.
func completionTimestamp(metadata *api.OrchestrationMetadata) time.Time {
	if !metadata.CompletedAt.IsZero() {
		return metadata.CompletedAt
	}
	return metadata.LastUpdatedAt
}

// blobObjectName derives a deterministic, collision-resistant object name from
// the instance's completion time and ID, so re-exporting the same instance
// overwrites its object instead of duplicating it.
func blobObjectName(completedAt time.Time, instanceID string, format ExportFormat) string {
	digest := sha256.Sum256([]byte(completedAt.UTC().Format(time.RFC3339Nano) + "|" + instanceID))
	return hex.EncodeToString(digest[:]) + "." + format.FileExtension()
}

// serializeHistory renders events in the requested format and returns the object
// body with the content type to store it under.
//
// A JSONL object is gzip-compressed and stored as an opaque gzip file: its name
// ends in .jsonl.gz and its content type is application/gzip, with no
// Content-Encoding. Declaring the compression as a content coding instead would
// make some clients transparently decompress the download while the object name
// still promises gzip bytes, so readers could not tell what they received.
func serializeHistory(
	events []*api.HistoryEvent,
	format ExportFormat,
) (content []byte, contentType string, err error) {
	if format.Kind == ExportFormatJSON {
		if events == nil {
			events = []*api.HistoryEvent{}
		}
		payload, err := json.Marshal(events)
		if err != nil {
			return nil, "", fmt.Errorf("failed to serialize orchestration history: %w", err)
		}
		return payload, format.ContentType(), nil
	}

	var builder strings.Builder
	for i, event := range events {
		if event == nil {
			continue
		}
		line, err := json.Marshal(event)
		if err != nil {
			return nil, "", fmt.Errorf("failed to serialize orchestration history event %d: %w", i, err)
		}
		builder.Write(line)
		builder.WriteByte('\n')
	}
	compressed, err := gzipContent([]byte(builder.String()))
	if err != nil {
		return nil, "", err
	}
	return compressed, format.ContentType(), nil
}
