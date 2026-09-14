package exporthistory

import (
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/microsoft/durabletask-go/api"
	"github.com/microsoft/durabletask-go/internal/historyconv"
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
	// StreamOrchestrationHistory delivers events serially, stopping when the
	// handler returns an error or ctx is canceled.
	StreamOrchestrationHistory(
		ctx context.Context,
		id api.InstanceID,
		query api.HistoryQuery,
		handler api.HistoryEventHandler,
	) error
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
		return nil, err
	}
	return result, nil
}

func (r *exportRuntime) exportInstance(ctx context.Context, input ExportRequest) (ExportResult, error) {
	instanceID := api.InstanceID(input.InstanceID)
	metadata, err := r.source.FetchOrchestrationMetadata(ctx, instanceID)
	if err != nil {
		if errors.Is(err, api.ErrInstanceNotFound) {
			return ExportResult{InstanceID: input.InstanceID, Error: fmt.Sprintf("instance %s not found", input.InstanceID)}, nil
		}
		return ExportResult{}, fmt.Errorf("failed to read instance %s metadata: %w", input.InstanceID, err)
	}
	if metadata == nil {
		return ExportResult{InstanceID: input.InstanceID, Error: fmt.Sprintf("instance %s not found", input.InstanceID)}, nil
	}
	if !isTerminalStatus(metadata.RuntimeStatus) {
		return ExportResult{
			InstanceID: input.InstanceID,
			Error:      fmt.Sprintf("instance %s is not in a completed state", input.InstanceID),
		}, nil
	}
	if metadata.ExecutionID == "" {
		return ExportResult{}, fmt.Errorf("instance %s metadata is missing an execution ID", input.InstanceID)
	}

	query := r.historyPage
	query.ExecutionID = metadata.ExecutionID
	completedAt := completionTimestamp(metadata)
	name := blobObjectName(completedAt, input.InstanceID, input.Format)
	path := input.Destination.BlobPath(name)
	object := ExportObject{
		Container:   input.Destination.Container,
		Name:        path,
		ContentType: input.Format.ContentType(),
		Metadata: map[string]string{
			"instanceId":    input.InstanceID,
			"executionId":   metadata.ExecutionID,
			"schemaVersion": input.Format.SchemaVersion,
		},
	}
	streamCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	reader, writer := io.Pipe()
	defer reader.Close() //nolint:errcheck // PipeReader.Close always returns nil.
	object.Content = reader
	type streamResult struct {
		count int
		err   error
	}
	done := make(chan streamResult, 1)
	go func() {
		count, streamErr := writeHistory(streamCtx, writer, input.Format, query, func(handler api.HistoryEventHandler) error {
			return r.source.StreamOrchestrationHistory(streamCtx, instanceID, query, handler)
		})
		if streamErr != nil {
			// Readers interpret EOF as success. Keep a source's EOF opaque on
			// the pipe; the original error is returned through done below.
			_ = writer.CloseWithError(errors.New("history export producer failed: " + streamErr.Error()))
			cancel()
		} else {
			_ = writer.Close()
		}
		done <- streamResult{count: count, err: streamErr}
	}()
	uploadErr := r.store.Write(streamCtx, object)
	// An uploader may fail without reading anything. Wake a blocked pipe write
	// AND a source blocked in its next receive, then join the producer.
	cancel()
	_ = reader.CloseWithError(io.ErrClosedPipe)
	streamed := <-done
	if err := errors.Join(streamed.err, uploadErr); err != nil {
		return ExportResult{}, fmt.Errorf("failed to export instance %s history: %w", input.InstanceID, err)
	}
	return ExportResult{
		InstanceID:  input.InstanceID,
		ExecutionID: metadata.ExecutionID,
		Success:     true,
		BlobPath:    path,
		EventCount:  streamed.count,
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

// writeHistory validates and serializes one event at a time. Only successful
// end-of-stream validation permits the JSON closing bracket or gzip trailer.
//
// A JSONL object is gzip-compressed and stored as an opaque gzip file: its name
// ends in .jsonl.gz and its content type is application/gzip, with no
// Content-Encoding. Declaring the compression as a content coding instead would
// make some clients transparently decompress the download while the object name
// still promises gzip bytes, so readers could not tell what they received.
func writeHistory(
	ctx context.Context,
	output io.Writer,
	format ExportFormat,
	query api.HistoryQuery,
	stream func(api.HistoryEventHandler) error,
) (int, error) {
	var compressed *gzip.Writer
	if format.Kind == ExportFormatJSON {
		if _, err := io.WriteString(output, "["); err != nil {
			return 0, err
		}
	} else {
		var err error
		compressed, err = gzip.NewWriterLevel(output, gzip.BestCompression)
		if err != nil {
			return 0, err
		}
		output = compressed
	}
	first := true
	_, count, err := historyconv.StreamValidated(query, stream, func(event *api.HistoryEvent) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		encoded, err := json.Marshal(event)
		if err != nil {
			return fmt.Errorf("failed to serialize orchestration history event: %w", err)
		}
		if format.Kind == ExportFormatJSON && !first {
			if _, err := io.WriteString(output, ","); err != nil {
				return err
			}
		}
		first = false
		if _, err := output.Write(encoded); err != nil {
			return err
		}
		if format.Kind == ExportFormatJSONL {
			_, err = io.WriteString(output, "\n")
		}
		return err
	})
	if err != nil {
		return count, err
	}
	if err := ctx.Err(); err != nil {
		return count, err
	}
	if compressed != nil {
		if err := compressed.Close(); err != nil {
			return count, fmt.Errorf("finish export compression: %w", err)
		}
	} else {
		_, err = io.WriteString(output, "]")
	}
	return count, err
}
