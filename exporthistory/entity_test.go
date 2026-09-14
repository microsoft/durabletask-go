package exporthistory

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/microsoft/durabletask-go/api"
	"github.com/microsoft/durabletask-go/internal/protos"
	"github.com/microsoft/durabletask-go/task"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

// entityHarness drives the ExportJob entity through the production entity
// executor, so operation dispatch, state persistence, and the emitted signal and
// start-orchestration actions are exercised exactly as the worker runs them.
type entityHarness struct {
	t         *testing.T
	executor  task.EntityExecutor
	converter api.DataConverter
	jobID     string
	state     *wrapperspb.StringValue
	actions   []*protos.OperationAction
}

func newEntityHarness(t *testing.T, jobID string, converters ...api.DataConverter) *entityHarness {
	t.Helper()
	converter := api.DefaultDataConverter()
	if len(converters) > 0 {
		converter = api.NormalizeDataConverter(converters[0])
	}
	registry := task.NewTaskRegistry()
	require.NoError(t, registry.AddEntityN(ExportJobEntityName, exportJobEntity))
	executor, ok := task.NewTaskExecutor(registry, task.WithDataConverter(converter)).(task.EntityExecutor)
	require.True(t, ok)
	return &entityHarness{t: t, executor: executor, converter: converter, jobID: jobID}
}

// call runs one operation and returns its serialized result. It fails the test
// when the operation reports a failure.
func (h *entityHarness) call(operation string, input any) string {
	h.t.Helper()
	result, err := h.tryCall(operation, input)
	require.NoError(h.t, err)
	return result
}

// tryCall runs one operation and returns the entity's failure as a Go error.
func (h *entityHarness) tryCall(operation string, input any) (string, error) {
	h.t.Helper()
	var rawInput *wrapperspb.StringValue
	if input != nil {
		payload, err := api.SerializeData(h.converter, input)
		require.NoError(h.t, err)
		rawInput = wrapperspb.String(payload)
	}
	return h.tryCallRaw(operation, rawInput)
}

func (h *entityHarness) tryCallRaw(operation string, input *wrapperspb.StringValue) (string, error) {
	h.t.Helper()
	request := &protos.EntityBatchRequest{
		InstanceId:  EntityID(h.jobID).String(),
		EntityState: h.state,
		Operations: []*protos.OperationRequest{{
			Operation: operation,
			RequestId: operation + "-request",
			Input:     input,
		}},
	}
	result, err := h.executor.ExecuteEntity(context.Background(), request)
	require.NoError(h.t, err)
	require.Len(h.t, result.Results, 1)
	h.state = result.EntityState
	h.actions = result.Actions

	if failure := result.Results[0].GetFailure(); failure != nil {
		return "", entityFailure{details: failure.GetFailureDetails()}
	}
	return result.Results[0].GetSuccess().GetResult().GetValue(), nil
}

// entityFailure adapts a protobuf failure into an error so tests can assert on
// the error type the entity produced.
type entityFailure struct{ details *protos.TaskFailureDetails }

func (e entityFailure) Error() string {
	if e.details == nil {
		return "entity operation failed"
	}
	return e.details.GetErrorType() + ": " + e.details.GetErrorMessage()
}

// assertEntityErrorType asserts the durable error type the entity reported.
func assertEntityErrorType(t *testing.T, err error, expected api.ErrorType) {
	t.Helper()
	var failure entityFailure
	require.ErrorAs(t, err, &failure)
	assert.Equal(t, string(expected), failure.details.GetErrorType())
}

func (h *entityHarness) jobState() *ExportJobState {
	h.t.Helper()
	if h.state == nil {
		return nil
	}
	var state ExportJobState
	require.NoError(h.t, json.Unmarshal([]byte(h.state.GetValue()), &state))
	return &state
}

func (h *entityHarness) hasState() bool { return h.state != nil }

func (h *entityHarness) runToken() string {
	h.t.Helper()
	state := h.jobState()
	require.NotNil(h.t, state)
	return state.RunToken
}

func batchOptions(jobID string) JobCreationOptions {
	options, err := JobCreationOptions{
		JobID:             jobID,
		Mode:              ExportModeBatch,
		CompletedTimeFrom: time.Now().UTC().Add(-24 * time.Hour),
		CompletedTimeTo:   time.Now().UTC().Add(-time.Minute),
		Destination:       &ExportDestination{Container: "test-container"},
	}.Normalize()
	if err != nil {
		panic(err)
	}
	return options
}

// TestEntityCreate ports the upstream ExportJobTests create scenarios.
func TestEntityCreate(t *testing.T) {
	t.Run("valid options activate the job and start the orchestration", func(t *testing.T) {
		harness := newEntityHarness(t, "job-1")
		harness.call(createOperation, batchOptions("job-1"))

		state := harness.jobState()
		require.NotNil(t, state)
		assert.Equal(t, ExportJobStatusActive, state.Status)
		require.NotNil(t, state.Config)
		assert.Equal(t, ExportModeBatch, state.Config.Mode)
		assert.Equal(t, "test-container", state.Config.Destination.Container)
		require.NotNil(t, state.CreatedAt)
		require.NotNil(t, state.LastModifiedAt)
		assert.Empty(t, state.LastError)
		assert.Zero(t, state.ScannedInstances)
		assert.Zero(t, state.ExportedInstances)
		assert.Nil(t, state.Checkpoint)
		require.NotEmpty(t, state.RunToken)
		assert.Equal(t, OrchestratorInstanceIDPrefix+"job-1-"+state.RunToken, state.OrchestratorInstanceID)

		// Create signals Run rather than starting the orchestration directly, so
		// the start action only appears after the signal is delivered.
		require.Len(t, harness.actions, 1)
		signal := harness.actions[0].GetSendSignal()
		require.NotNil(t, signal)
		assert.Equal(t, runOperation, signal.GetName())
		assert.Equal(t, EntityID("job-1").String(), signal.GetInstanceId())
	})

	t.Run("missing options are rejected", func(t *testing.T) {
		harness := newEntityHarness(t, "job-1")
		_, err := harness.tryCall(createOperation, nil)
		require.Error(t, err)
		assertEntityErrorType(t, err, validationErrorType)
		assert.Contains(t, err.Error(), "creation options are required")
		assert.False(t, harness.hasState())
	})

	t.Run("mismatched job ID is rejected", func(t *testing.T) {
		harness := newEntityHarness(t, "job-1")
		_, err := harness.tryCall(createOperation, batchOptions("other-job"))
		require.Error(t, err)
		assertEntityErrorType(t, err, validationErrorType)
		assert.Contains(t, err.Error(), "does not match entity key")
	})

	t.Run("invalid options are rejected inside the entity", func(t *testing.T) {
		harness := newEntityHarness(t, "job-1")
		_, err := harness.tryCall(createOperation, JobCreationOptions{
			JobID:       "job-1",
			Mode:        ExportModeBatch,
			Destination: &ExportDestination{Container: "test-container"},
		})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "CompletedTimeFrom is required")
	})

	t.Run("a destination is required", func(t *testing.T) {
		harness := newEntityHarness(t, "job-1")
		_, err := harness.tryCall(createOperation, JobCreationOptions{
			JobID:             "job-1",
			Mode:              ExportModeBatch,
			CompletedTimeFrom: time.Now().UTC().Add(-time.Hour),
			CompletedTimeTo:   time.Now().UTC().Add(-time.Minute),
		})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "export destination is required")
	})

	t.Run("recreating an active job is rejected", func(t *testing.T) {
		harness := newEntityHarness(t, "job-1")
		harness.call(createOperation, batchOptions("job-1"))
		_, err := harness.tryCall(createOperation, batchOptions("job-1"))
		require.Error(t, err)
		assertEntityErrorType(t, err, invalidTransitionErrorType)
		assert.Equal(t, ExportJobStatusActive, harness.jobState().Status)
	})

	t.Run("recreating a failed job resets progress and keeps CreatedAt", func(t *testing.T) {
		harness := newEntityHarness(t, "job-1")
		harness.call(createOperation, batchOptions("job-1"))
		harness.call(commitCheckpointOperation, CommitCheckpointRequest{
			ScannedInstances:  5,
			ExportedInstances: 5,
			Checkpoint:        &ExportCheckpoint{LastInstanceKey: "cursor"},
			RunToken:          harness.runToken(),
		})
		harness.call(markAsFailedOperation, MarkAsFailedRequest{
			RunToken: harness.runToken(),
			Error:    "test error",
		})
		originalCreatedAt := *harness.jobState().CreatedAt

		harness.call(createOperation, batchOptions("job-1"))
		state := harness.jobState()
		assert.Equal(t, ExportJobStatusActive, state.Status)
		assert.Zero(t, state.ScannedInstances)
		assert.Zero(t, state.ExportedInstances)
		assert.Nil(t, state.Checkpoint)
		assert.Nil(t, state.LastCheckpointTime)
		assert.Empty(t, state.LastError)
		require.NotNil(t, state.CreatedAt)
		assert.True(t, state.CreatedAt.Equal(originalCreatedAt))
	})

	t.Run("recreating a completed job is allowed", func(t *testing.T) {
		harness := newEntityHarness(t, "job-1")
		harness.call(createOperation, batchOptions("job-1"))
		harness.call(markAsCompletedOperation, MarkAsCompletedRequest{RunToken: harness.runToken()})
		harness.call(createOperation, batchOptions("job-1"))
		assert.Equal(t, ExportJobStatusActive, harness.jobState().Status)
	})
}

func TestEntityGet(t *testing.T) {
	t.Run("returns the current state", func(t *testing.T) {
		harness := newEntityHarness(t, "job-1")
		harness.call(createOperation, batchOptions("job-1"))
		result := harness.call(getOperation, nil)

		var state ExportJobState
		require.NoError(t, json.Unmarshal([]byte(result), &state))
		assert.Equal(t, ExportJobStatusActive, state.Status)
		require.NotNil(t, state.Config)
	})

	t.Run("does not resurrect a deleted entity", func(t *testing.T) {
		harness := newEntityHarness(t, "job-1")
		harness.call(createOperation, batchOptions("job-1"))
		harness.call(deleteOperation, nil)
		require.False(t, harness.hasState())

		result := harness.call(getOperation, nil)
		assert.Empty(t, result)
		assert.False(t, harness.hasState())
	})

	t.Run("reads emit no actions", func(t *testing.T) {
		harness := newEntityHarness(t, "job-1")
		harness.call(createOperation, batchOptions("job-1"))
		harness.call(getOperation, nil)
		assert.Empty(t, harness.actions)
	})
}

func TestEntityRun(t *testing.T) {
	t.Run("starts the orchestration with the ID persisted by Create", func(t *testing.T) {
		harness := newEntityHarness(t, "job-1")
		harness.call(createOperation, batchOptions("job-1"))
		instanceID := harness.jobState().OrchestratorInstanceID
		require.NotEmpty(t, instanceID)
		harness.call(runOperation, RunJobRequest{RunToken: harness.runToken()})

		require.Len(t, harness.actions, 1)
		start := harness.actions[0].GetStartNewOrchestration()
		require.NotNil(t, start)
		assert.Equal(t, ExportJobOrchestratorName, start.GetName())
		assert.Equal(t, instanceID, start.GetInstanceId())
		// System orchestrations are started explicitly unversioned.
		assert.Equal(t, task.UnversionedTaskVersion, start.GetVersion().GetValue())

		var request ExportJobRunRequest
		require.NoError(t, json.Unmarshal([]byte(start.GetInput().GetValue()), &request))
		assert.Equal(t, EntityID("job-1"), request.JobEntityID)
		assert.Zero(t, request.ProcessedCycles)

		assert.Equal(t, instanceID, harness.jobState().OrchestratorInstanceID)

		harness.call(runOperation, RunJobRequest{RunToken: harness.runToken()})
		require.Len(t, harness.actions, 1)
		assert.Equal(t, instanceID, harness.actions[0].GetStartNewOrchestration().GetInstanceId(),
			"a duplicate Run must target the same generation")
	})

	t.Run("rejects a job without configuration", func(t *testing.T) {
		harness := newEntityHarness(t, "job-1")
		harness.state = wrapperspb.String(`{"Status":1,"RunToken":"run-a"}`)
		_, err := harness.tryCall(runOperation, RunJobRequest{RunToken: "run-a"})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "configuration is missing")
	})

	t.Run("rejects a missing stored orchestration ID without deriving a replacement", func(t *testing.T) {
		harness := newEntityHarness(t, "job-1")
		harness.call(createOperation, batchOptions("job-1"))
		state := harness.jobState()
		state.OrchestratorInstanceID = ""
		payload, err := json.Marshal(state)
		require.NoError(t, err)
		harness.state = wrapperspb.String(string(payload))

		_, err = harness.tryCall(runOperation, RunJobRequest{RunToken: state.RunToken})
		require.ErrorContains(t, err, "orchestration instance ID is missing")
		assert.Empty(t, harness.actions)
	})

	t.Run("uses the stored ID rather than reconstructing it from the token", func(t *testing.T) {
		harness := newEntityHarness(t, "job-1")
		harness.call(createOperation, batchOptions("job-1"))
		state := harness.jobState()
		state.OrchestratorInstanceID = "persisted-generation-id"
		payload, err := json.Marshal(state)
		require.NoError(t, err)
		harness.state = wrapperspb.String(string(payload))

		harness.call(runOperation, RunJobRequest{RunToken: state.RunToken})
		require.Len(t, harness.actions, 1)
		assert.Equal(t, state.OrchestratorInstanceID, harness.actions[0].GetStartNewOrchestration().GetInstanceId())
	})

	t.Run("rejects a job that is not active", func(t *testing.T) {
		harness := newEntityHarness(t, "job-1")
		harness.call(createOperation, batchOptions("job-1"))
		harness.call(markAsCompletedOperation, MarkAsCompletedRequest{RunToken: harness.runToken()})
		_, err := harness.tryCall(runOperation, RunJobRequest{RunToken: harness.runToken()})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "must be in Active status to run")
	})
}

// TestEntityCommitCheckpoint covers durable checkpointing and the implicit
// failure a failed batch produces.
func TestEntityCommitCheckpoint(t *testing.T) {
	t.Run("advances the cursor and accumulates progress", func(t *testing.T) {
		harness := newEntityHarness(t, "job-1")
		harness.call(createOperation, batchOptions("job-1"))
		harness.call(commitCheckpointOperation, CommitCheckpointRequest{
			ScannedInstances:  100,
			ExportedInstances: 95,
			Checkpoint:        &ExportCheckpoint{LastInstanceKey: "last-key"},
			RunToken:          harness.runToken(),
		})
		state := harness.jobState()
		assert.Equal(t, int64(100), state.ScannedInstances)
		assert.Equal(t, int64(95), state.ExportedInstances)
		require.NotNil(t, state.Checkpoint)
		assert.Equal(t, "last-key", state.Checkpoint.LastInstanceKey)
		require.NotNil(t, state.LastCheckpointTime)
		assert.Equal(t, ExportJobStatusActive, state.Status)

		harness.call(commitCheckpointOperation, CommitCheckpointRequest{
			ScannedInstances:  10,
			ExportedInstances: 10,
			Checkpoint:        &ExportCheckpoint{LastInstanceKey: "next-key"},
			RunToken:          harness.runToken(),
		})
		state = harness.jobState()
		assert.Equal(t, int64(110), state.ScannedInstances)
		assert.Equal(t, int64(105), state.ExportedInstances)
		assert.Equal(t, "next-key", state.Checkpoint.LastInstanceKey)
	})

	t.Run("a nil checkpoint keeps the cursor", func(t *testing.T) {
		harness := newEntityHarness(t, "job-1")
		harness.call(createOperation, batchOptions("job-1"))
		harness.call(commitCheckpointOperation, CommitCheckpointRequest{
			ScannedInstances:  1,
			ExportedInstances: 1,
			Checkpoint:        &ExportCheckpoint{LastInstanceKey: "keep-me"},
			RunToken:          harness.runToken(),
		})
		harness.call(commitCheckpointOperation, CommitCheckpointRequest{RunToken: harness.runToken()})
		state := harness.jobState()
		require.NotNil(t, state.Checkpoint)
		assert.Equal(t, "keep-me", state.Checkpoint.LastInstanceKey)
		assert.Equal(t, ExportJobStatusActive, state.Status)
	})

	t.Run("failures without a checkpoint implicitly fail the job", func(t *testing.T) {
		harness := newEntityHarness(t, "job-1")
		harness.call(createOperation, batchOptions("job-1"))
		harness.call(commitCheckpointOperation, CommitCheckpointRequest{
			Failures: []ExportFailure{
				{InstanceID: "instance-1", Reason: "error1", AttemptCount: 1, LastAttempt: time.Now().UTC()},
				{InstanceID: "instance-2", Reason: "error2", AttemptCount: 2, LastAttempt: time.Now().UTC()},
			},
			RunToken: harness.runToken(),
		})
		state := harness.jobState()
		assert.Equal(t, ExportJobStatusFailed, state.Status)
		assert.Contains(t, state.LastError, "Batch export failed after retries")
		assert.Contains(t, state.LastError, "InstanceId: instance-1, Reason: error1")
		assert.Contains(t, state.LastError, "InstanceId: instance-2, Reason: error2")
	})

	t.Run("failures cannot be silently discarded by a checkpoint", func(t *testing.T) {
		harness := newEntityHarness(t, "job-1")
		harness.call(createOperation, batchOptions("job-1"))
		before := harness.jobState()
		_, err := harness.tryCall(commitCheckpointOperation, CommitCheckpointRequest{
			ScannedInstances: 1,
			Checkpoint:       &ExportCheckpoint{LastInstanceKey: "cursor"},
			Failures:         []ExportFailure{{InstanceID: "i", Reason: "r"}},
			RunToken:         harness.runToken(),
		})
		require.ErrorContains(t, err, "checkpoint and failures cannot be committed together")
		after := harness.jobState()
		assert.Equal(t, before.Status, after.Status)
		assert.Equal(t, before.ScannedInstances, after.ScannedInstances)
		assert.Equal(t, before.ExportedInstances, after.ExportedInstances)
		assert.Equal(t, before.Checkpoint, after.Checkpoint)
	})

	t.Run("a checkpoint for a deleted job does not resurrect it", func(t *testing.T) {
		harness := newEntityHarness(t, "job-1")
		harness.call(createOperation, batchOptions("job-1"))
		harness.call(deleteOperation, nil)
		require.False(t, harness.hasState())

		harness.call(commitCheckpointOperation, CommitCheckpointRequest{
			ScannedInstances:  5,
			ExportedInstances: 5,
			Checkpoint:        &ExportCheckpoint{LastInstanceKey: "cursor"},
		})
		assert.False(t, harness.hasState(), "a deleted export job must stay deleted")
	})

	t.Run("negative progress is rejected", func(t *testing.T) {
		harness := newEntityHarness(t, "job-1")
		harness.call(createOperation, batchOptions("job-1"))
		_, err := harness.tryCall(commitCheckpointOperation, CommitCheckpointRequest{
			ScannedInstances: -1,
			RunToken:         harness.runToken(),
		})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "must not be negative")
	})

	t.Run("the persisted failure summary is bounded", func(t *testing.T) {
		harness := newEntityHarness(t, "job-1")
		harness.call(createOperation, batchOptions("job-1"))
		failures := make([]ExportFailure, 0, 25)
		for i := 0; i < 25; i++ {
			failures = append(failures, ExportFailure{InstanceID: "instance", Reason: "boom"})
		}
		harness.call(commitCheckpointOperation, CommitCheckpointRequest{
			Failures: failures,
			RunToken: harness.runToken(),
		})
		assert.Contains(t, harness.jobState().LastError, "and 15 more failures")
	})
}

func TestEntityMarkAsCompletedAndFailed(t *testing.T) {
	t.Run("active jobs complete", func(t *testing.T) {
		harness := newEntityHarness(t, "job-1")
		harness.call(createOperation, batchOptions("job-1"))
		harness.call(markAsCompletedOperation, MarkAsCompletedRequest{RunToken: harness.runToken()})
		state := harness.jobState()
		assert.Equal(t, ExportJobStatusCompleted, state.Status)
		assert.Empty(t, state.LastError)
	})

	t.Run("completing a non-active job is an invalid transition", func(t *testing.T) {
		harness := newEntityHarness(t, "job-1")
		harness.call(createOperation, batchOptions("job-1"))
		harness.call(markAsFailedOperation, MarkAsFailedRequest{
			RunToken: harness.runToken(),
			Error:    "test error",
		})
		_, err := harness.tryCall(
			markAsCompletedOperation,
			MarkAsCompletedRequest{RunToken: harness.runToken()},
		)
		require.Error(t, err)
		assertEntityErrorType(t, err, invalidTransitionErrorType)
	})

	t.Run("active jobs fail with a message", func(t *testing.T) {
		harness := newEntityHarness(t, "job-1")
		harness.call(createOperation, batchOptions("job-1"))
		harness.call(markAsFailedOperation, MarkAsFailedRequest{
			RunToken: harness.runToken(),
			Error:    "Test error",
		})
		state := harness.jobState()
		assert.Equal(t, ExportJobStatusFailed, state.Status)
		assert.Equal(t, "Test error", state.LastError)
	})

	t.Run("failing without a message is allowed", func(t *testing.T) {
		harness := newEntityHarness(t, "job-1")
		harness.call(createOperation, batchOptions("job-1"))
		harness.call(markAsFailedOperation, MarkAsFailedRequest{RunToken: harness.runToken()})
		assert.Equal(t, ExportJobStatusFailed, harness.jobState().Status)
	})

	t.Run("failing a non-active job is an invalid transition", func(t *testing.T) {
		harness := newEntityHarness(t, "job-1")
		harness.call(createOperation, batchOptions("job-1"))
		harness.call(markAsCompletedOperation, MarkAsCompletedRequest{RunToken: harness.runToken()})
		_, err := harness.tryCall(markAsFailedOperation, MarkAsFailedRequest{
			RunToken: harness.runToken(),
			Error:    "boom",
		})
		require.Error(t, err)
		assertEntityErrorType(t, err, invalidTransitionErrorType)
	})
}

// TestEntityRunGenerationFencing covers run fencing on the entity side: every
// orchestration-originated mutation carrying a stale generation token is dropped
// so a run left over from a deleted-and-recreated job cannot alter the new one.
func TestEntityRunGenerationFencing(t *testing.T) {
	// newGeneration deletes and recreates the job, returning the token of the
	// previous generation and of the new one.
	newGeneration := func(t *testing.T) (*entityHarness, string, string) {
		t.Helper()
		harness := newEntityHarness(t, "job-1")
		harness.call(createOperation, batchOptions("job-1"))
		stale := harness.jobState().RunToken
		require.NotEmpty(t, stale)

		harness.call(deleteOperation, nil)
		harness.call(createOperation, batchOptions("job-1"))
		current := harness.jobState().RunToken
		require.NotEmpty(t, current)
		require.NotEqual(t, stale, current,
			"a delete-and-recreate must mint a new run generation")
		return harness, stale, current
	}

	t.Run("a stale checkpoint is dropped", func(t *testing.T) {
		harness, stale, current := newGeneration(t)
		harness.call(commitCheckpointOperation, CommitCheckpointRequest{
			ScannedInstances:  5,
			ExportedInstances: 5,
			Checkpoint:        &ExportCheckpoint{LastInstanceKey: "stale-cursor"},
			RunToken:          stale,
		})
		state := harness.jobState()
		assert.Zero(t, state.ScannedInstances)
		assert.Zero(t, state.ExportedInstances)
		assert.Nil(t, state.Checkpoint)
		assert.Equal(t, current, state.RunToken)

		// The current generation's checkpoint still applies.
		harness.call(commitCheckpointOperation, CommitCheckpointRequest{
			ScannedInstances:  2,
			ExportedInstances: 2,
			Checkpoint:        &ExportCheckpoint{LastInstanceKey: "fresh-cursor"},
			RunToken:          current,
		})
		state = harness.jobState()
		assert.Equal(t, int64(2), state.ScannedInstances)
		require.NotNil(t, state.Checkpoint)
		assert.Equal(t, "fresh-cursor", state.Checkpoint.LastInstanceKey)
	})

	t.Run("a stale implicit failure is dropped", func(t *testing.T) {
		harness, stale, _ := newGeneration(t)
		harness.call(commitCheckpointOperation, CommitCheckpointRequest{
			Failures: []ExportFailure{{InstanceID: "i1", Reason: "boom"}},
			RunToken: stale,
		})
		state := harness.jobState()
		assert.Equal(t, ExportJobStatusActive, state.Status)
		assert.Empty(t, state.LastError)
	})

	t.Run("a stale completion is dropped", func(t *testing.T) {
		harness, stale, current := newGeneration(t)
		harness.call(markAsCompletedOperation, MarkAsCompletedRequest{RunToken: stale})
		assert.Equal(t, ExportJobStatusActive, harness.jobState().Status)

		harness.call(markAsCompletedOperation, MarkAsCompletedRequest{RunToken: current})
		assert.Equal(t, ExportJobStatusCompleted, harness.jobState().Status)
	})

	t.Run("a stale failure is dropped", func(t *testing.T) {
		harness, stale, current := newGeneration(t)
		harness.call(markAsFailedOperation, MarkAsFailedRequest{RunToken: stale, Error: "stale boom"})
		state := harness.jobState()
		assert.Equal(t, ExportJobStatusActive, state.Status)
		assert.Empty(t, state.LastError)

		harness.call(markAsFailedOperation, MarkAsFailedRequest{RunToken: current, Error: "fresh boom"})
		state = harness.jobState()
		assert.Equal(t, ExportJobStatusFailed, state.Status)
		assert.Equal(t, "fresh boom", state.LastError)
	})

	t.Run("a stale run signal starts no orchestration", func(t *testing.T) {
		harness, stale, current := newGeneration(t)
		harness.call(runOperation, RunJobRequest{RunToken: stale})
		assert.Empty(t, harness.actions, "a stale run signal must not start an orchestration")

		harness.call(runOperation, RunJobRequest{RunToken: current})
		require.Len(t, harness.actions, 1)
		require.NotNil(t, harness.actions[0].GetStartNewOrchestration())
	})

	t.Run("missing request tokens reject every mutation", func(t *testing.T) {
		harness, _, _ := newGeneration(t)
		before := harness.state.GetValue()
		for _, request := range []struct {
			operation string
			input     any
		}{
			{runOperation, nil},
			{runOperation, RunJobRequest{}},
			{commitCheckpointOperation, CommitCheckpointRequest{ScannedInstances: 1}},
			{markAsCompletedOperation, nil},
			{markAsCompletedOperation, MarkAsCompletedRequest{}},
			{markAsFailedOperation, nil},
			{markAsFailedOperation, MarkAsFailedRequest{Error: "untokenized failure"}},
		} {
			harness.call(request.operation, request.input)
			assert.Equal(t, before, harness.state.GetValue(), request.operation)
			assert.Empty(t, harness.actions, request.operation)
		}
	})

	t.Run("missing state tokens reject every mutation", func(t *testing.T) {
		for _, token := range []string{"", "any-token"} {
			harness := newEntityHarness(t, "job-1")
			harness.call(createOperation, batchOptions("job-1"))
			state := harness.jobState()
			state.RunToken = ""
			payload, err := json.Marshal(state)
			require.NoError(t, err)
			harness.state = wrapperspb.String(string(payload))

			for operation, input := range map[string]any{
				runOperation:              RunJobRequest{RunToken: token},
				commitCheckpointOperation: CommitCheckpointRequest{ScannedInstances: 3, RunToken: token},
				markAsCompletedOperation:  MarkAsCompletedRequest{RunToken: token},
				markAsFailedOperation:     MarkAsFailedRequest{RunToken: token, Error: "stale failure"},
			} {
				harness.call(operation, input)
				assert.Equal(t, string(payload), harness.state.GetValue(), operation)
				assert.Empty(t, harness.actions, operation)
			}
		}
	})
}

func TestEntityDeletedJobDropsMutations(t *testing.T) {
	harness := newEntityHarness(t, "job-1")
	harness.call(createOperation, batchOptions("job-1"))
	token := harness.runToken()
	harness.call(deleteOperation, nil)
	for operation, input := range map[string]any{
		runOperation:              RunJobRequest{RunToken: token},
		commitCheckpointOperation: CommitCheckpointRequest{ScannedInstances: 3, RunToken: token},
		markAsCompletedOperation:  MarkAsCompletedRequest{RunToken: token},
		markAsFailedOperation:     MarkAsFailedRequest{RunToken: token, Error: "delayed failure"},
	} {
		harness.call(operation, input)
		assert.False(t, harness.hasState(), operation)
		assert.Empty(t, harness.actions, operation)
	}
}

func TestEntityRejectsMalformedMutationRequests(t *testing.T) {
	harness := newEntityHarness(t, "job-1")
	harness.call(createOperation, batchOptions("job-1"))
	token := harness.runToken()
	before := harness.state.GetValue()
	for operation, payload := range map[string]string{
		runOperation:              fmt.Sprintf(`{"RunToken":%q,"RunToken":42}`, token),
		commitCheckpointOperation: fmt.Sprintf(`{"RunToken":%q,"ScannedInstances":"bad"}`, token),
		markAsCompletedOperation:  fmt.Sprintf(`{"RunToken":%q,"RunToken":42}`, token),
		markAsFailedOperation:     fmt.Sprintf(`{"RunToken":%q,"Error":42}`, token),
	} {
		_, err := harness.tryCallRaw(operation, wrapperspb.String(payload))
		assertEntityErrorType(t, err, validationErrorType)
		assert.Equal(t, before, harness.state.GetValue(), operation)
		assert.Empty(t, harness.actions, operation)
	}
}

// TestEntityRunTokenTravelsToTheOrchestration keeps the generation the entity
// minted and the generation the orchestration runs under in sync.
func TestEntityRunTokenTravelsToTheOrchestration(t *testing.T) {
	harness := newEntityHarness(t, "job-1")
	harness.call(createOperation, batchOptions("job-1"))
	token := harness.jobState().RunToken
	require.NotEmpty(t, token)

	require.Len(t, harness.actions, 1)
	signal := harness.actions[0].GetSendSignal()
	require.NotNil(t, signal)
	var runRequest RunJobRequest
	require.NoError(t, json.Unmarshal([]byte(signal.GetInput().GetValue()), &runRequest))
	assert.Equal(t, token, runRequest.RunToken)

	harness.call(runOperation, runRequest)
	require.Len(t, harness.actions, 1)
	start := harness.actions[0].GetStartNewOrchestration()
	require.NotNil(t, start)
	var request ExportJobRunRequest
	require.NoError(t, json.Unmarshal([]byte(start.GetInput().GetValue()), &request))
	assert.Equal(t, token, request.RunToken)
	assert.False(t, request.ContinuedExecution)
}

// TestEntityCreationToleratesBoundedClockSkew pins the documented tolerance the
// entity applies to a batch window's upper bound. A client validates strictly
// against its own clock, so a worker running slightly behind must not reject a
// window that client accepted.
func TestEntityCreationToleratesBoundedClockSkew(t *testing.T) {
	windowEnd := time.Now().UTC().Add(MaxCreationClockSkew / 2)
	options := JobCreationOptions{
		JobID:             "job-1",
		Mode:              ExportModeBatch,
		CompletedTimeFrom: time.Now().UTC().Add(-time.Hour),
		CompletedTimeTo:   windowEnd,
		Destination:       &ExportDestination{Container: "test-container"},
	}

	// The client is strict: an upper bound ahead of its clock is rejected.
	require.ErrorIs(t, options.Validate(), ErrValidation)

	// The entity absorbs the skew so a job the client accepted on a slightly
	// faster clock still activates.
	harness := newEntityHarness(t, "job-1")
	harness.call(createOperation, options)
	assert.Equal(t, ExportJobStatusActive, harness.jobState().Status)

	t.Run("beyond the documented skew it is still rejected", func(t *testing.T) {
		beyond := options
		beyond.CompletedTimeTo = time.Now().UTC().Add(2 * MaxCreationClockSkew)
		rejected := newEntityHarness(t, "job-1")
		_, err := rejected.tryCall(createOperation, beyond)
		require.Error(t, err)
		assertEntityErrorType(t, err, validationErrorType)
		assert.Contains(t, err.Error(), "cannot be in the future")
	})
}

func TestEntityDelete(t *testing.T) {
	harness := newEntityHarness(t, "job-1")
	harness.call(createOperation, batchOptions("job-1"))
	harness.call(runOperation, RunJobRequest{RunToken: harness.runToken()})
	require.True(t, harness.hasState())
	instanceID := harness.jobState().OrchestratorInstanceID
	require.NotEmpty(t, instanceID)

	var deletedID string
	require.NoError(t, json.Unmarshal([]byte(harness.call(deleteOperation, nil)), &deletedID))
	assert.Equal(t, instanceID, deletedID)
	assert.False(t, harness.hasState())

	// Deleting an already-deleted job is a no-op rather than an error.
	require.NoError(t, json.Unmarshal([]byte(harness.call(deleteOperation, nil)), &deletedID))
	assert.Empty(t, deletedID)
	assert.False(t, harness.hasState())

	// A deleted job can be created again from scratch.
	harness.call(createOperation, batchOptions("job-1"))
	assert.Equal(t, ExportJobStatusActive, harness.jobState().Status)
	replacementID := harness.jobState().OrchestratorInstanceID
	require.NotEmpty(t, replacementID)
	assert.NotEqual(t, instanceID, replacementID)
	// A new Delete after recreation removes that new generation, not the old one.
	require.NoError(t, json.Unmarshal([]byte(harness.call(deleteOperation, nil)), &deletedID))
	assert.Equal(t, replacementID, deletedID)
	assert.False(t, harness.hasState())
}

func TestEntityDeleteBeforeRunDropsDelayedSignal(t *testing.T) {
	harness := newEntityHarness(t, "job-1")
	harness.call(createOperation, batchOptions("job-1"))
	var delayed RunJobRequest
	require.NoError(t, json.Unmarshal([]byte(harness.actions[0].GetSendSignal().GetInput().GetValue()), &delayed))
	harness.call(deleteOperation, nil)

	harness.call(runOperation, delayed)
	assert.False(t, harness.hasState())
	assert.Empty(t, harness.actions)

	harness.call(createOperation, batchOptions("job-1"))
	current := harness.state.GetValue()
	harness.call(runOperation, delayed)
	assert.Equal(t, current, harness.state.GetValue())
	assert.Empty(t, harness.actions)
}

func TestEntityUnknownOperation(t *testing.T) {
	harness := newEntityHarness(t, "job-1")
	_, err := harness.tryCall("NotAnOperation", nil)
	require.Error(t, err)
	assertEntityErrorType(t, err, validationErrorType)
	assert.Contains(t, err.Error(), `does not support operation "NotAnOperation"`)
}

// TestEntityOperationNamesAreCaseInsensitive keeps the entity reachable from
// SDKs that normalize operation names differently.
func TestEntityOperationNamesAreCaseInsensitive(t *testing.T) {
	harness := newEntityHarness(t, "job-1")
	harness.call("create", batchOptions("job-1"))
	assert.Equal(t, ExportJobStatusActive, harness.jobState().Status)
	harness.call("MARKASCOMPLETED", MarkAsCompletedRequest{RunToken: harness.runToken()})
	assert.Equal(t, ExportJobStatusCompleted, harness.jobState().Status)
	harness.call("DELETE", nil)
	assert.False(t, harness.hasState())
}

// TestEntityRejectsCorruptState surfaces unreadable state instead of silently
// starting a second export from a blank slate.
func TestEntityRejectsCorruptState(t *testing.T) {
	harness := newEntityHarness(t, "job-1")
	harness.state = wrapperspb.String("not json")
	_, err := harness.tryCall(getOperation, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to deserialize export job state")
}
