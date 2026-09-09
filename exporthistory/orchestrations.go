package exporthistory

import (
	"fmt"
	"strings"
	"time"

	"github.com/microsoft/durabletask-go/api"
	"github.com/microsoft/durabletask-go/task"
)

// Export orchestration tuning. These defaults mirror the .NET preview implementation.
const (
	// maxBatchRetryAttempts bounds how many times a page whose exports failed is
	// retried before the job fails.
	maxBatchRetryAttempts = 3
	// minBatchRetryBackoff is the delay before the first whole-page retry. Each
	// further retry doubles it, so the reachable schedule is 1 then 2 minutes:
	// the final attempt fails the page instead of waiting again.
	minBatchRetryBackoff = time.Minute
	// continueAsNewFrequency bounds how many pages one orchestration execution
	// processes before continuing as new, keeping history small.
	continueAsNewFrequency = 5
	// continuousIdleDelay is how long a continuous job waits after draining its
	// backlog before listing again.
	continuousIdleDelay = time.Minute
)

// exportActivityRetryPolicy retries a single instance export before the page is
// declared failed.
func exportActivityRetryPolicy() *task.RetryPolicy {
	return &task.RetryPolicy{
		MaxAttempts:          3,
		InitialRetryInterval: 15 * time.Second,
		BackoffCoefficient:   2,
		MaxRetryInterval:     time.Minute,
	}
}

// ExecuteExportJobOperationOrchestrator invokes one export job entity operation.
// It must remain unversioned because export clients explicitly target the
// unversioned system orchestrator.
func ExecuteExportJobOperationOrchestrator(ctx *task.OrchestrationContext) (any, error) {
	var request ExportJobOperationRequest
	if err := ctx.GetInput(&request); err != nil {
		return nil, err
	}
	if request.OperationName == "" {
		return nil, &ValidationError{
			JobID:   request.EntityID.Key,
			Message: "operation name is required",
		}
	}
	operation := ctx.CallEntity(
		request.EntityID,
		request.OperationName,
		task.WithEntityInput(request.Input),
	)
	// Non-JSON converters need the concrete result type, not *any.
	if strings.EqualFold(request.OperationName, deleteOperation) {
		var instanceID string
		if err := operation.Await(&instanceID); err != nil {
			return nil, err
		}
		return instanceID, nil
	}
	var result any
	if err := operation.Await(&result); err != nil {
		return nil, err
	}
	return result, nil
}

// ExportJobOrchestrator performs the export work for one job: it lists terminal
// instances matching the job's filter, exports each instance's history, and
// commits a checkpoint after every page.
//
// It must remain unversioned because the entity starts it explicitly
// unversioned so it stays reachable when application default versioning is on.
func ExportJobOrchestrator(ctx *task.OrchestrationContext) (any, error) {
	var input ExportJobRunRequest
	if err := ctx.GetInput(&input); err != nil {
		return nil, err
	}
	jobID := input.JobEntityID.Key
	logger := ctx.Logger()

	result, err := runExportJobOrchestration(ctx, input)
	if err == nil {
		return result, nil
	}
	logger.Error("export orchestrator failed", "jobId", jobID, "error", err)
	// Best-effort: a job already moved out of Active by a failed checkpoint
	// rejects MarkAsFailed, and that rejection must not mask the original error.
	// The run token keeps the failure from landing on a newer generation.
	markRequest := MarkAsFailedRequest{RunToken: input.RunToken, Error: err.Error()}
	if markErr := callEntityVoid(ctx, input.JobEntityID, markAsFailedOperation, markRequest); markErr != nil {
		logger.Warn("failed to mark export job as failed", "jobId", jobID, "error", markErr)
	}
	return nil, err
}

func runExportJobOrchestration(ctx *task.OrchestrationContext, input ExportJobRunRequest) (any, error) {
	jobID := input.JobEntityID.Key
	logger := ctx.Logger()

	// A missing job is an error only on the very first execution, where it means
	// the job was never readable. Once the run has continued as new the job may
	// legitimately have been deleted underneath it, so it stops quietly.
	firstExecution := !input.ContinuedExecution
	runToken := input.RunToken
	processedCycles := input.ProcessedCycles
	for {
		processedCycles++
		if processedCycles > continueAsNewFrequency {
			ctx.ContinueAsNew(
				ExportJobRunRequest{
					JobEntityID:        input.JobEntityID,
					RunToken:           runToken,
					ContinuedExecution: true,
				},
				task.WithContinueAsNewVersion(task.UnversionedTaskVersion),
			)
			return nil, nil
		}

		// Re-read the job each cycle: it may have been deleted, failed by a
		// checkpoint commit, or recreated with a different configuration.
		current, err := fetchExportJobState(ctx, input.JobEntityID)
		if err != nil {
			return nil, err
		}
		if current == nil || current.Config == nil {
			if firstExecution && processedCycles == 1 {
				return nil, fmt.Errorf("export job %q not found or has no configuration", jobID)
			}
			logger.Warn("export job is no longer available", "jobId", jobID)
			return nil, nil
		}
		// The job may have been deleted and recreated while this run was in
		// flight. The new generation owns the job, so this run stops without
		// touching it rather than checkpointing or completing it.
		if !runTokenMatches(current, runToken) {
			logger.Warn("export orchestrator stopping because its generation token does not match",
				"jobId", jobID, "runToken", runToken, "currentToken", current.RunToken)
			return nil, nil
		}
		if current.Status != ExportJobStatusActive {
			logger.Warn("export orchestrator stopping because the job is not active",
				"jobId", jobID, "status", current.Status.String())
			return nil, nil
		}
		config := *current.Config

		listRequest := ListTerminalInstancesRequest{
			CompletedTimeFrom:    config.Filter.CompletedTimeFrom,
			CompletedTimeTo:      optionalTime(config.Filter.CompletedTimeTo),
			RuntimeStatus:        config.Filter.RuntimeStatus,
			MaxInstancesPerBatch: config.MaxInstancesPerBatch,
		}
		if current.Checkpoint != nil {
			listRequest.LastInstanceKey = current.Checkpoint.LastInstanceKey
		}

		var page InstancePage
		if err := ctx.CallActivity(
			ListTerminalInstancesActivityName,
			task.WithActivityInput(listRequest),
		).Await(&page); err != nil {
			return nil, err
		}

		if len(page.InstanceIDs) == 0 {
			// A page can legitimately be empty while the task hub still has
			// more pages, for example when a filtered scan exhausts a page
			// without a match. Advancing the cursor keeps a batch job from
			// completing while instances remain, and keeps a continuous job
			// from re-reading the same empty page forever.
			if page.NextCheckpoint != nil {
				if err := commitBatchCheckpoint(ctx, input.JobEntityID, CommitCheckpointRequest{
					Checkpoint: page.NextCheckpoint,
					RunToken:   runToken,
				}); err != nil {
					return nil, err
				}
				continue
			}
		} else {
			batch, err := processBatchWithRetry(ctx, jobID, page.InstanceIDs, config, maxBatchRetryAttempts)
			if err != nil {
				return nil, err
			}
			if len(batch.failures) > 0 {
				// A failed page commits without a checkpoint so the cursor stays
				// put and the entity implicitly fails the job.
				if err := commitBatchCheckpoint(ctx, input.JobEntityID, CommitCheckpointRequest{
					Failures: batch.failures,
					RunToken: runToken,
				}); err != nil {
					return nil, err
				}
				return nil, fmt.Errorf(
					"export job %q batch export failed after %d retry attempts. Failure details: %s",
					jobID, maxBatchRetryAttempts, summarizeFailures(batch.failures))
			}

			if err := commitBatchCheckpoint(ctx, input.JobEntityID, CommitCheckpointRequest{
				ScannedInstances:  int64(len(page.InstanceIDs)),
				ExportedInstances: int64(batch.exportedCount),
				Checkpoint:        page.NextCheckpoint,
				RunToken:          runToken,
			}); err != nil {
				return nil, err
			}
		}

		if page.NextCheckpoint != nil {
			continue
		}
		// A page without a next checkpoint is the last one the task hub has.
		// A continuous job re-lists from the last committed cursor after idling,
		// which re-scans at most one page and overwrites deterministic blob names.
		if config.Mode == ExportModeContinuous {
			logger.Info("export job drained its backlog; waiting for new instances", "jobId", jobID)
			if err := ctx.CreateTimer(continuousIdleDelay).Await(nil); err != nil {
				return nil, err
			}
			continue
		}
		logger.Info("export job exported every matching instance", "jobId", jobID)
		break
	}

	completion := MarkAsCompletedRequest{RunToken: runToken}
	if err := callEntityVoid(ctx, input.JobEntityID, markAsCompletedOperation, completion); err != nil {
		return nil, err
	}
	logger.Info("export orchestrator completed", "jobId", jobID)
	return nil, nil
}

type batchExportResult struct {
	exportedCount int
	failures      []ExportFailure
}

func processBatchWithRetry(
	ctx *task.OrchestrationContext,
	jobID string,
	instanceIDs []string,
	config ExportJobConfiguration,
	maxAttempts int,
) (batchExportResult, error) {
	logger := ctx.Logger()
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		results, err := exportBatch(ctx, instanceIDs, config)
		if err != nil {
			return batchExportResult{}, err
		}
		failed := make([]ExportResult, 0, len(results))
		succeeded := 0
		for _, result := range results {
			if result.Success {
				succeeded++
				continue
			}
			failed = append(failed, result)
		}
		if len(failed) == 0 {
			logger.Info("export batch succeeded",
				"jobId", jobID, "attempt", attempt, "exported", succeeded)
			return batchExportResult{exportedCount: succeeded}, nil
		}
		logger.Warn("export batch failed",
			"jobId", jobID, "attempt", attempt, "failures", len(failed), "instances", len(instanceIDs))
		if attempt == maxAttempts {
			failures := make([]ExportFailure, 0, len(failed))
			for _, result := range failed {
				reason := result.Error
				if reason == "" {
					reason = "unknown error"
				}
				failures = append(failures, ExportFailure{
					InstanceID:   result.InstanceID,
					Reason:       reason,
					AttemptCount: attempt,
					LastAttempt:  ctx.CurrentTimeUtc,
				})
			}
			return batchExportResult{exportedCount: succeeded, failures: failures}, nil
		}
		if err := ctx.CreateTimer(batchRetryBackoff(attempt)).Await(nil); err != nil {
			return batchExportResult{}, err
		}
	}
	// Reached only when maxAttempts is not positive, which means the page was
	// never attempted. Reporting it as failed keeps the cursor on the page
	// instead of committing a checkpoint that would silently skip it.
	return batchExportResult{
		failures: []ExportFailure{{
			InstanceID:  strings.Join(instanceIDs, ","),
			Reason:      fmt.Sprintf("the export batch was never attempted: %d retry attempts configured", maxAttempts),
			LastAttempt: ctx.CurrentTimeUtc,
		}},
	}, nil
}

// batchRetryBackoff is the delay before retry number attempt+1, doubling from
// [minBatchRetryBackoff]. Only attempts 1 and 2 schedule a retry, because the
// final attempt fails the page, so the reachable schedule is 1 then 2 minutes.
func batchRetryBackoff(attempt int) time.Duration {
	backoff := minBatchRetryBackoff
	for i := 1; i < attempt; i++ {
		backoff *= 2
	}
	return backoff
}

// exportBatch fans out per-instance exports in deterministic windows bounded by
// MaxParallelExports and preserves the input order of the results.
func exportBatch(
	ctx *task.OrchestrationContext,
	instanceIDs []string,
	config ExportJobConfiguration,
) ([]ExportResult, error) {
	parallelism := config.MaxParallelExports
	if parallelism <= 0 {
		parallelism = DefaultMaxParallelExports
	}
	results := make([]ExportResult, 0, len(instanceIDs))
	retryPolicy := exportActivityRetryPolicy()
	for start := 0; start < len(instanceIDs); start += parallelism {
		end := min(start+parallelism, len(instanceIDs))
		window := instanceIDs[start:end]
		pending := make([]task.Task, 0, len(window))
		for _, instanceID := range window {
			pending = append(pending, ctx.CallActivity(
				ExportInstanceHistoryActivityName,
				task.WithActivityInput(ExportRequest{
					InstanceID:  instanceID,
					Destination: config.Destination,
					Format:      config.Format,
				}),
				task.WithActivityRetryPolicy(retryPolicy),
			))
		}
		for i, pendingTask := range pending {
			var result ExportResult
			if err := pendingTask.Await(&result); err != nil {
				// Every attempt of the activity failed, so record the instance as
				// failed instead of aborting the page: other instances in the same
				// window may still have succeeded and their failures matter too.
				result = ExportResult{InstanceID: window[i], Success: false, Error: err.Error()}
			}
			if result.InstanceID == "" {
				result.InstanceID = window[i]
			}
			results = append(results, result)
		}
	}
	return results, nil
}

func commitBatchCheckpoint(
	ctx *task.OrchestrationContext,
	entityID api.EntityID,
	request CommitCheckpointRequest,
) error {
	return callEntityVoid(ctx, entityID, commitCheckpointOperation, request)
}

func fetchExportJobState(ctx *task.OrchestrationContext, entityID api.EntityID) (*ExportJobState, error) {
	var state *ExportJobState
	if err := ctx.CallEntity(entityID, getOperation, task.WithEntityInput(nil)).Await(&state); err != nil {
		return nil, err
	}
	return state, nil
}

func callEntityVoid(
	ctx *task.OrchestrationContext,
	entityID api.EntityID,
	operation string,
	input any,
) error {
	return ctx.CallEntity(entityID, operation, task.WithEntityInput(input)).Await(nil)
}
