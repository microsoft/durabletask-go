package exporthistory

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/microsoft/durabletask-go/task"
)

// exportJobEntity implements the ExportJob system entity. It owns the job's
// lifecycle, configuration, checkpoint cursor, and progress counters.
func exportJobEntity(ctx *task.EntityContext) (any, error) {
	var state ExportJobState
	if rawState, ok := ctx.GetRawState(); ok && rawState != "" {
		if err := json.Unmarshal([]byte(rawState), &state); err != nil {
			return nil, fmt.Errorf("failed to deserialize export job state: %w", err)
		}
	}
	jobID := ctx.ID.Key

	switch {
	case strings.EqualFold(ctx.Operation, getOperation):
		// Get never mutates, so it must not rewrite state and resurrect a
		// deleted entity.
		if !ctx.HasState() {
			return nil, nil
		}
		return state, nil
	case strings.EqualFold(ctx.Operation, deleteOperation):
		instanceID := state.OrchestratorInstanceID
		ctx.DeleteState()
		return instanceID, nil
	case strings.EqualFold(ctx.Operation, createOperation):
		if !ctx.HasInput() {
			return nil, &ValidationError{JobID: jobID, Message: "export job creation options are required"}
		}
		var options JobCreationOptions
		if err := ctx.GetInput(&options); err != nil {
			return nil, &ValidationError{JobID: jobID, Message: "export job creation options could not be deserialized"}
		}
		if options.isZero() {
			return nil, &ValidationError{JobID: jobID, Message: "export job creation options are required"}
		}
		if err := createExportJob(ctx, &state, options); err != nil {
			return nil, err
		}
	case strings.EqualFold(ctx.Operation, runOperation):
		var request RunJobRequest
		if ctx.HasInput() {
			if err := ctx.GetInput(&request); err != nil {
				return nil, &ValidationError{JobID: jobID, Message: "run request could not be deserialized"}
			}
		}
		if dropsStaleMutation(ctx, &state, "a run signal", request.RunToken) {
			return nil, nil
		}
		if err := runExportJob(ctx, &state); err != nil {
			return nil, err
		}
	case strings.EqualFold(ctx.Operation, commitCheckpointOperation):
		// A checkpoint that arrives after the job was deleted must not
		// resurrect the entity with a configuration-less state, which would
		// leave a permanently Pending job behind. The orchestration is
		// terminated separately, so dropping the commit is the correct outcome.
		if !ctx.HasState() {
			ctx.Logger().Warn("dropping a checkpoint for a deleted export job", "jobId", jobID)
			return nil, nil
		}
		var request CommitCheckpointRequest
		if err := ctx.GetInput(&request); err != nil {
			return nil, &ValidationError{JobID: jobID, Message: "checkpoint commit request is required"}
		}
		if dropsStaleMutation(ctx, &state, "a checkpoint", request.RunToken) {
			return nil, nil
		}
		if err := commitCheckpoint(ctx, &state, request); err != nil {
			return nil, err
		}
	case strings.EqualFold(ctx.Operation, markAsCompletedOperation):
		var request MarkAsCompletedRequest
		if ctx.HasInput() {
			if err := ctx.GetInput(&request); err != nil {
				return nil, &ValidationError{JobID: jobID, Message: "completion request could not be deserialized"}
			}
		}
		if dropsStaleMutation(ctx, &state, "a completion", request.RunToken) {
			return nil, nil
		}
		if err := markExportJobCompleted(ctx, &state); err != nil {
			return nil, err
		}
	case strings.EqualFold(ctx.Operation, markAsFailedOperation):
		var request MarkAsFailedRequest
		if ctx.HasInput() {
			if err := ctx.GetInput(&request); err != nil {
				return nil, &ValidationError{JobID: jobID, Message: "failure request could not be deserialized"}
			}
		}
		if dropsStaleMutation(ctx, &state, "a failure", request.RunToken) {
			return nil, nil
		}
		if err := markExportJobFailed(ctx, &state, request.Error); err != nil {
			return nil, err
		}
	default:
		return nil, &ValidationError{
			JobID:   jobID,
			Message: fmt.Sprintf("export job does not support operation %q", ctx.Operation),
		}
	}

	payload, err := json.Marshal(state)
	if err != nil {
		return nil, fmt.Errorf("failed to serialize export job state: %w", err)
	}
	ctx.SetRawState(string(payload))
	return nil, nil
}

// newRunToken mints the fencing token of a new job generation.
//
// The token is random rather than derived, because the one property it must
// guarantee is that a generation never reuses a predecessor's token, and a
// delete clears the state a derived token could chain from. Randomness is safe
// here because an entity operation is not replayed: its state write and its
// emitted actions are produced by a single execution, so the token the job
// stores and the token its run signal carries always agree, and a redelivered
// Create is rejected by the lifecycle transition instead of minting a second
// token for a job that is already Active.
func newRunToken() string {
	return newCompactUUID()
}

// runTokenMatches reports whether a mutation carrying token belongs to the job's
// current run generation. Both tokens must be nonempty.
func runTokenMatches(state *ExportJobState, token string) bool {
	return token != "" && state.RunToken == token
}

// dropsStaleMutation reports whether a mutation carrying token belongs to a
// superseded run generation, logging the drop when it does. Every
// orchestration-originated mutation is fenced this way, so a run left over from
// a deleted-and-recreated job cannot alter the new generation. The mutation
// argument names the dropped signal in the log entry, for example "a run
// signal".
func dropsStaleMutation(ctx *task.EntityContext, state *ExportJobState, mutation, token string) bool {
	if runTokenMatches(state, token) {
		return false
	}
	ctx.Logger().Warn(
		"dropping "+mutation+" from a stale export job generation",
		"jobId", ctx.ID.Key,
		"requestToken", token,
	)
	return true
}

func createExportJob(ctx *task.EntityContext, state *ExportJobState, options JobCreationOptions) error {
	jobID := ctx.ID.Key
	if !isValidTransition(createOperation, state.Status, ExportJobStatusActive) {
		return &InvalidTransitionError{
			JobID:     jobID,
			From:      state.Status,
			To:        ExportJobStatusActive,
			Operation: createOperation,
		}
	}
	// The entity validates against its own clock, which is independent of the
	// client's, so the upper bound is allowed a bounded skew. Clients stay
	// strict, and [MaxCreationClockSkew] documents the tolerance.
	normalized, err := options.normalize(entityNow(ctx), MaxCreationClockSkew)
	if err != nil {
		return withJobID(err, jobID)
	}
	if normalized.JobID != jobID {
		return &ValidationError{
			JobID:   jobID,
			Message: fmt.Sprintf("creation job ID %q does not match entity key %q", normalized.JobID, jobID),
		}
	}
	config, err := normalized.configuration()
	if err != nil {
		return withJobID(err, jobID)
	}

	now := entityNow(ctx)
	createdAt := now
	if state.CreatedAt != nil {
		createdAt = *state.CreatedAt
	}
	// Recreating a terminal job resets progress and the cursor so the new run
	// starts from the beginning of its window, and mints a new run token so a
	// run left over from the previous generation cannot commit into it.
	runToken := newRunToken()
	*state = ExportJobState{
		Status:                 ExportJobStatusActive,
		Config:                 config,
		CreatedAt:              &createdAt,
		LastModifiedAt:         &now,
		OrchestratorInstanceID: OrchestratorInstanceIDPrefix + jobID + "-" + runToken,
		RunToken:               runToken,
	}

	ctx.Logger().Info("created export job", "jobId", jobID, "mode", config.Mode.String())
	return ctx.SignalEntity(ctx.ID, runOperation, RunJobRequest{RunToken: runToken})
}

func runExportJob(ctx *task.EntityContext, state *ExportJobState) error {
	jobID := ctx.ID.Key
	if state.Config == nil {
		return &ValidationError{JobID: jobID, Message: "export job configuration is missing"}
	}
	if state.Status != ExportJobStatusActive {
		return &ValidationError{JobID: jobID, Message: "export job must be in Active status to run"}
	}
	if state.OrchestratorInstanceID == "" {
		return &ValidationError{JobID: jobID, Message: "export job orchestration instance ID is missing"}
	}
	return startExportOrchestration(ctx, state)
}

// startExportOrchestration starts the job's dedicated orchestration. A failure
// to schedule is recorded on the job instead of failing the operation, matching
// the upstream behavior of leaving the entity readable with a diagnosable error.
func startExportOrchestration(ctx *task.EntityContext, state *ExportJobState) error {
	jobID := ctx.ID.Key
	request := ExportJobRunRequest{JobEntityID: ctx.ID, RunToken: state.RunToken}
	payload, err := ctx.SerializeInput(request)
	if err != nil {
		return fmt.Errorf("failed to serialize export job run request: %w", err)
	}
	now := entityNow(ctx)
	startErr := ctx.StartNewOrchestration(
		ExportJobOrchestratorName,
		task.WithEntityStartOrchestrationInstanceID(state.OrchestratorInstanceID),
		task.WithRawEntityStartOrchestrationInput(payload),
		// System orchestrations must not inherit an application default version.
		task.WithEntityStartOrchestrationVersion(task.UnversionedTaskVersion),
	)
	if startErr != nil {
		state.Status = ExportJobStatusFailed
		state.LastError = startErr.Error()
		state.LastModifiedAt = &now
		ctx.Logger().Error("failed to start export orchestration", "jobId", jobID, "error", startErr)
		return nil
	}
	state.LastModifiedAt = &now
	return nil
}

// commitCheckpoint records one batch's progress. A nil checkpoint keeps the
// cursor so the same page is retried; combined with failures it implicitly
// fails the job, which is how a batch that exhausted its retries stops the run.
func commitCheckpoint(ctx *task.EntityContext, state *ExportJobState, request CommitCheckpointRequest) error {
	if request.ScannedInstances < 0 || request.ExportedInstances < 0 {
		return &ValidationError{
			JobID:   ctx.ID.Key,
			Message: "checkpoint progress counts must not be negative",
		}
	}
	if request.Checkpoint != nil && len(request.Failures) > 0 {
		return &ValidationError{
			JobID:   ctx.ID.Key,
			Message: "checkpoint and failures cannot be committed together",
		}
	}
	state.ScannedInstances += request.ScannedInstances
	state.ExportedInstances += request.ExportedInstances
	if request.Checkpoint != nil {
		checkpoint := *request.Checkpoint
		state.Checkpoint = &checkpoint
	}
	now := entityNow(ctx)
	state.LastCheckpointTime = &now
	state.LastModifiedAt = &now

	if request.Checkpoint == nil && len(request.Failures) > 0 {
		// The implicit failure goes through the same transition helper as an
		// explicit MarkAsFailed, so the lifecycle rules stay in one place. A
		// job that already left Active keeps its terminal state; the progress
		// this commit carried is still recorded above.
		message := "Batch export failed after retries. Failures: " + summarizeFailures(request.Failures)
		if err := markExportJobFailed(ctx, state, message); err != nil {
			ctx.Logger().Warn("a failing checkpoint could not fail the export job",
				"jobId", ctx.ID.Key, "status", state.Status.String(), "error", err)
		}
	}
	return nil
}

func markExportJobCompleted(ctx *task.EntityContext, state *ExportJobState) error {
	jobID := ctx.ID.Key
	if !isValidTransition(markAsCompletedOperation, state.Status, ExportJobStatusCompleted) {
		return &InvalidTransitionError{
			JobID:     jobID,
			From:      state.Status,
			To:        ExportJobStatusCompleted,
			Operation: markAsCompletedOperation,
		}
	}
	now := entityNow(ctx)
	state.Status = ExportJobStatusCompleted
	state.LastError = ""
	state.LastModifiedAt = &now
	ctx.Logger().Info("export job completed", "jobId", jobID)
	return nil
}

func markExportJobFailed(ctx *task.EntityContext, state *ExportJobState, errorMessage string) error {
	jobID := ctx.ID.Key
	if !isValidTransition(markAsFailedOperation, state.Status, ExportJobStatusFailed) {
		return &InvalidTransitionError{
			JobID:     jobID,
			From:      state.Status,
			To:        ExportJobStatusFailed,
			Operation: markAsFailedOperation,
		}
	}
	now := entityNow(ctx)
	state.Status = ExportJobStatusFailed
	state.LastError = errorMessage
	state.LastModifiedAt = &now
	ctx.Logger().Warn("export job failed", "jobId", jobID, "error", errorMessage)
	return nil
}

// maxSummarizedFailures bounds the failure detail written to entity state and
// reported in the orchestration's terminal error, so a large failing batch
// cannot grow either without limit.
const maxSummarizedFailures = 10

// summarizeFailures renders a bounded, human-readable summary of the instances
// a batch could not export. It is the single summarizer the entity and the
// orchestration share, so both report the same detail under the same limit.
func summarizeFailures(failures []ExportFailure) string {
	if len(failures) == 0 {
		return "no failure details available"
	}
	var builder strings.Builder
	limit := min(len(failures), maxSummarizedFailures)
	for i := 0; i < limit; i++ {
		if i > 0 {
			builder.WriteString("; ")
		}
		fmt.Fprintf(&builder, "InstanceId: %s, Reason: %s", failures[i].InstanceID, failures[i].Reason)
	}
	if remaining := len(failures) - limit; remaining > 0 {
		fmt.Fprintf(&builder, " ... and %d more failures", remaining)
	}
	return builder.String()
}

// entityNow returns the timestamp the entity operation should treat as the
// current time. The service stamps each operation with a durable timestamp, so
// two workers processing the same job agree on the instant it recorded; the wall
// clock is only a fallback for transports that do not supply one.
func entityNow(ctx *task.EntityContext) time.Time {
	if now := ctx.CurrentTimeUTC(); !now.IsZero() {
		return now.UTC()
	}
	return time.Now().UTC()
}
