package exporthistory

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/microsoft/durabletask-go/api"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestExportModeAndStatusStrings(t *testing.T) {
	assert.Equal(t, "Batch", ExportModeBatch.String())
	assert.Equal(t, "Continuous", ExportModeContinuous.String())
	assert.Equal(t, "ExportMode(0)", ExportMode(0).String())
	assert.True(t, ExportModeBatch.IsValid())
	assert.True(t, ExportModeContinuous.IsValid())
	assert.False(t, ExportMode(0).IsValid())
	assert.False(t, ExportMode(3).IsValid())

	assert.Equal(t, "Pending", ExportJobStatusPending.String())
	assert.Equal(t, "Active", ExportJobStatusActive.String())
	assert.Equal(t, "Failed", ExportJobStatusFailed.String())
	assert.Equal(t, "Completed", ExportJobStatusCompleted.String())
	assert.Equal(t, "ExportJobStatus(9)", ExportJobStatus(9).String())
	assert.True(t, ExportJobStatusPending.IsValid())
	assert.True(t, ExportJobStatusActive.IsValid())
	assert.True(t, ExportJobStatusFailed.IsValid())
	assert.True(t, ExportJobStatusCompleted.IsValid())
	assert.False(t, ExportJobStatus(9).IsValid())

	// The persisted numeric values are part of the wire contract with .NET.
	assert.Equal(t, 0, int(ExportJobStatusPending))
	assert.Equal(t, 1, int(ExportJobStatusActive))
	assert.Equal(t, 2, int(ExportJobStatusFailed))
	assert.Equal(t, 3, int(ExportJobStatusCompleted))
	assert.Equal(t, 1, int(ExportModeBatch))
	assert.Equal(t, 2, int(ExportModeContinuous))
}

func TestExportFormatKindJSON(t *testing.T) {
	encoded, err := json.Marshal(ExportFormatJSONL)
	require.NoError(t, err)
	assert.JSONEq(t, `"Jsonl"`, string(encoded))

	encoded, err = json.Marshal(ExportFormatJSON)
	require.NoError(t, err)
	assert.JSONEq(t, `"Json"`, string(encoded))

	_, err = json.Marshal(ExportFormatKind(9))
	require.Error(t, err)

	for _, input := range []string{`"Jsonl"`, `"jsonl"`, `0`} {
		var kind ExportFormatKind
		require.NoError(t, json.Unmarshal([]byte(input), &kind), input)
		assert.Equal(t, ExportFormatJSONL, kind)
	}
	for _, input := range []string{`"Json"`, `"JSON"`, `1`} {
		var kind ExportFormatKind
		require.NoError(t, json.Unmarshal([]byte(input), &kind), input)
		assert.Equal(t, ExportFormatJSON, kind)
	}
	for _, input := range []string{`"csv"`, `7`, `{}`} {
		var kind ExportFormatKind
		require.Error(t, json.Unmarshal([]byte(input), &kind), input)
	}
}

func TestExportFormatDefaultsAndExtensions(t *testing.T) {
	format := DefaultExportFormat()
	assert.Equal(t, ExportFormatJSONL, format.Kind)
	assert.Equal(t, "1.0", format.SchemaVersion)
	assert.Equal(t, "jsonl.gz", format.FileExtension())
	assert.Equal(t, "json", ExportFormat{Kind: ExportFormatJSON}.FileExtension())
	// Unknown kinds fall back to the compressed default rather than producing an
	// extensionless object name.
	assert.Equal(t, "jsonl.gz", ExportFormat{Kind: ExportFormatKind(9)}.FileExtension())
}

// TestExportFormatContentType pins the unambiguous storage contract: a JSONL
// object is an opaque gzip file whose name and content type agree, so no reader
// has to guess whether it received raw or already-decompressed bytes.
func TestExportFormatContentType(t *testing.T) {
	assert.Equal(t, "application/gzip", DefaultExportFormat().ContentType())
	assert.Equal(t, "application/json", ExportFormat{Kind: ExportFormatJSON}.ContentType())
	assert.Equal(t, "application/gzip", ExportFormat{Kind: ExportFormatKind(9)}.ContentType())
}

func TestExportDestinationValidate(t *testing.T) {
	valid := []ExportDestination{
		{Container: "container"},
		{Container: "my-container", Prefix: "prefix"},
		{Container: "abc", Prefix: "a/b/c/"},
		{Container: "a1-b2-c3"},
	}
	for _, destination := range valid {
		require.NoError(t, destination.Validate(), "%+v", destination)
	}

	invalid := []struct {
		destination ExportDestination
		message     string
	}{
		{ExportDestination{}, "container is required"},
		{ExportDestination{Container: "   "}, "container is required"},
		{ExportDestination{Container: "ab"}, "not a valid Azure Blob container name"},
		{ExportDestination{Container: "UPPER"}, "not a valid Azure Blob container name"},
		{ExportDestination{Container: "-lead"}, "not a valid Azure Blob container name"},
		{ExportDestination{Container: "trail-"}, "not a valid Azure Blob container name"},
		{ExportDestination{Container: "double--dash"}, "not a valid Azure Blob container name"},
		{ExportDestination{Container: "under_score"}, "not a valid Azure Blob container name"},
		{ExportDestination{Container: "okc", Prefix: "/leading"}, "must not start with '/'"},
		{ExportDestination{Container: "okc", Prefix: "a//b"}, "must not contain '//'"},
		{ExportDestination{Container: "okc", Prefix: "../escape"}, "relative path segments"},
		{ExportDestination{Container: "okc", Prefix: "a/./b"}, "relative path segments"},
		{ExportDestination{Container: "okc", Prefix: "bad\npath"}, "unsupported character"},
	}
	for _, test := range invalid {
		err := test.destination.Validate()
		require.ErrorIs(t, err, ErrValidation, "%+v", test.destination)
		assert.Contains(t, err.Error(), test.message)
	}
}

func TestExportDestinationBlobPath(t *testing.T) {
	assert.Equal(t, "object", ExportDestination{Container: "c"}.BlobPath("object"))
	assert.Equal(t, "p/object", ExportDestination{Container: "c", Prefix: "p"}.BlobPath("object"))
	assert.Equal(t, "p/object", ExportDestination{Container: "c", Prefix: "p/"}.BlobPath("object"))
	assert.Equal(t, "a/b/object", ExportDestination{Container: "c", Prefix: "a/b//"}.BlobPath("object"))
}

func TestExportFilterJSONRoundTrip(t *testing.T) {
	from := time.Date(2024, time.January, 2, 3, 4, 5, 0, time.UTC)
	to := from.Add(time.Hour)

	bounded := ExportFilter{
		CompletedTimeFrom: from,
		CompletedTimeTo:   to,
		RuntimeStatus:     TerminalStatuses(),
	}
	encoded, err := json.Marshal(bounded)
	require.NoError(t, err)
	var decoded ExportFilter
	require.NoError(t, json.Unmarshal(encoded, &decoded))
	assert.True(t, decoded.CompletedTimeFrom.Equal(from))
	assert.True(t, decoded.CompletedTimeTo.Equal(to))
	assert.Equal(t, TerminalStatuses(), decoded.RuntimeStatus)

	open := ExportFilter{CompletedTimeFrom: from}
	encoded, err = json.Marshal(open)
	require.NoError(t, err)
	assert.Contains(t, string(encoded), `"CompletedTimeTo":null`)
	decoded = ExportFilter{}
	require.NoError(t, json.Unmarshal(encoded, &decoded))
	assert.True(t, decoded.CompletedTimeTo.IsZero())
}

func TestExportJobStateJSONRoundTrip(t *testing.T) {
	now := time.Date(2024, time.May, 5, 6, 7, 8, 0, time.UTC)
	state := ExportJobState{
		Status: ExportJobStatusActive,
		Config: &ExportJobConfiguration{
			Mode:                 ExportModeBatch,
			Filter:               ExportFilter{CompletedTimeFrom: now.Add(-time.Hour), CompletedTimeTo: now},
			Destination:          ExportDestination{Container: "container", Prefix: "batch-job/"},
			Format:               DefaultExportFormat(),
			MaxParallelExports:   DefaultMaxParallelExports,
			MaxInstancesPerBatch: DefaultMaxInstancesPerBatch,
		},
		Checkpoint:             &ExportCheckpoint{LastInstanceKey: "cursor"},
		CreatedAt:              &now,
		LastModifiedAt:         &now,
		LastCheckpointTime:     &now,
		LastError:              "boom",
		ScannedInstances:       12,
		ExportedInstances:      11,
		OrchestratorInstanceID: "ExportJob-job-run-a",
		RunToken:               "run-a",
	}
	encoded, err := json.Marshal(state)
	require.NoError(t, err)

	var decoded ExportJobState
	require.NoError(t, json.Unmarshal(encoded, &decoded))
	assert.Equal(t, state.Status, decoded.Status)
	require.NotNil(t, decoded.Config)
	assert.Equal(t, state.Config.Mode, decoded.Config.Mode)
	assert.Equal(t, state.Config.Destination, decoded.Config.Destination)
	assert.Equal(t, state.Config.MaxInstancesPerBatch, decoded.Config.MaxInstancesPerBatch)
	require.NotNil(t, decoded.Checkpoint)
	assert.Equal(t, "cursor", decoded.Checkpoint.LastInstanceKey)
	assert.Equal(t, int64(12), decoded.ScannedInstances)
	assert.Equal(t, int64(11), decoded.ExportedInstances)
	assert.Equal(t, "boom", decoded.LastError)
	assert.Equal(t, "ExportJob-job-run-a", decoded.OrchestratorInstanceID)
	assert.Equal(t, "run-a", decoded.RunToken)

	empty, err := json.Marshal(ExportJobState{})
	require.NoError(t, err)
	assert.JSONEq(t, `{"Status":0,"ScannedInstances":0,"ExportedInstances":0}`, string(empty))
}

func TestRunTokenMatchesRequiresNonemptyGeneration(t *testing.T) {
	for _, stateToken := range []string{"", "run-a", "run-b"} {
		for _, requestToken := range []string{"", "run-a", "run-b"} {
			assert.Equal(t, stateToken != "" && stateToken == requestToken,
				runTokenMatches(&ExportJobState{RunToken: stateToken}, requestToken),
				"state token %q, request token %q", stateToken, requestToken)
		}
	}
}

func TestMarkAsFailedRequestJSON(t *testing.T) {
	require.Error(t, json.Unmarshal([]byte(`"boom"`), new(MarkAsFailedRequest)))

	var fromObject MarkAsFailedRequest
	require.NoError(t, json.Unmarshal([]byte(`{"RunToken":"run-a","Error":"boom"}`), &fromObject))
	assert.Equal(t, "run-a", fromObject.RunToken)
	assert.Equal(t, "boom", fromObject.Error)

	var empty MarkAsFailedRequest
	require.NoError(t, json.Unmarshal([]byte(`{}`), &empty))
	assert.Empty(t, empty.Error)

	require.Error(t, json.Unmarshal([]byte(`[1,2]`), new(MarkAsFailedRequest)))

	encoded, err := json.Marshal(MarkAsFailedRequest{RunToken: "run-a", Error: "boom"})
	require.NoError(t, err)
	assert.JSONEq(t, `{"RunToken":"run-a","Error":"boom"}`, string(encoded))
}

func TestExportJobRunRequestRunFencingJSON(t *testing.T) {
	request := ExportJobRunRequest{
		JobEntityID:        EntityID("job-1"),
		RunToken:           "run-a",
		ContinuedExecution: true,
	}
	encoded, err := json.Marshal(request)
	require.NoError(t, err)
	assert.JSONEq(t, `{"JobEntityId":"@exportjob@job-1","ProcessedCycles":0,"RunToken":"run-a","ContinuedExecution":true}`, string(encoded))
	var decoded ExportJobRunRequest
	require.NoError(t, json.Unmarshal(encoded, &decoded))
	assert.Equal(t, request, decoded)
}

func TestExportJobStateDescription(t *testing.T) {
	now := time.Date(2024, time.June, 1, 0, 0, 0, 0, time.UTC)
	state := ExportJobState{
		Status:                 ExportJobStatusCompleted,
		CreatedAt:              &now,
		LastModifiedAt:         &now,
		LastCheckpointTime:     &now,
		ScannedInstances:       5,
		ExportedInstances:      4,
		LastError:              "err",
		OrchestratorInstanceID: "ExportJob-x-run-a",
		Checkpoint:             &ExportCheckpoint{LastInstanceKey: "k"},
	}
	description := state.description("x")
	assert.Equal(t, "x", description.JobID)
	assert.Equal(t, ExportJobStatusCompleted, description.Status)
	assert.Equal(t, now, description.CreatedAt)
	assert.Equal(t, now, description.LastModifiedAt)
	assert.Equal(t, now, description.LastCheckpointTime)
	assert.Equal(t, int64(5), description.ScannedInstances)
	assert.Equal(t, int64(4), description.ExportedInstances)
	assert.Equal(t, "err", description.LastError)
	assert.Equal(t, "ExportJob-x-run-a", description.OrchestratorInstanceID)
	require.NotNil(t, description.Checkpoint)

	// Unset timestamps stay zero rather than panicking on a nil dereference.
	bare := (&ExportJobState{}).description("y")
	assert.True(t, bare.CreatedAt.IsZero())
	assert.True(t, bare.LastModifiedAt.IsZero())
	assert.True(t, bare.LastCheckpointTime.IsZero())
}

func TestExportJobQueryMatches(t *testing.T) {
	created := time.Date(2024, time.July, 15, 12, 0, 0, 0, time.UTC)
	description := &ExportJobDescription{Status: ExportJobStatusActive, CreatedAt: created}

	active := ExportJobStatusActive
	completed := ExportJobStatusCompleted
	assert.True(t, ExportJobQuery{}.matches(description))
	assert.True(t, ExportJobQuery{Status: &active}.matches(description))
	assert.False(t, ExportJobQuery{Status: &completed}.matches(description))

	// Bounds are exclusive, matching .NET.
	assert.True(t, ExportJobQuery{CreatedFrom: created.Add(-time.Second)}.matches(description))
	assert.False(t, ExportJobQuery{CreatedFrom: created}.matches(description))
	assert.True(t, ExportJobQuery{CreatedTo: created.Add(time.Second)}.matches(description))
	assert.False(t, ExportJobQuery{CreatedTo: created}.matches(description))
}

func TestTerminalStatusHelpers(t *testing.T) {
	assert.Equal(t, []api.OrchestrationStatus{
		api.RUNTIME_STATUS_COMPLETED,
		api.RUNTIME_STATUS_FAILED,
		api.RUNTIME_STATUS_TERMINATED,
	}, TerminalStatuses())

	// The exported slice must not be a shared backing array.
	first := TerminalStatuses()
	first[0] = api.RUNTIME_STATUS_FAILED
	assert.Equal(t, api.RUNTIME_STATUS_COMPLETED, TerminalStatuses()[0])

	for _, status := range TerminalStatuses() {
		assert.True(t, isTerminalStatus(status))
	}
	for _, status := range []api.OrchestrationStatus{
		api.RUNTIME_STATUS_RUNNING,
		api.RUNTIME_STATUS_PENDING,
		api.RUNTIME_STATUS_SUSPENDED,
		api.RUNTIME_STATUS_CANCELED,
		api.RUNTIME_STATUS_CONTINUED_AS_NEW,
	} {
		assert.False(t, isTerminalStatus(status), status)
	}
}

func TestSystemIdentifiers(t *testing.T) {
	assert.Equal(t, "@exportjob@abc", EntityID("abc").String())
	assert.Equal(t, "exportjob", EntityID("abc").Name)
	assert.Equal(t, "abc", EntityID("abc").Key)
}

func TestCommitCheckpointRequestJSON(t *testing.T) {
	request := CommitCheckpointRequest{
		ScannedInstances:  10,
		ExportedInstances: 9,
		Checkpoint:        &ExportCheckpoint{LastInstanceKey: "cursor"},
		Failures: []ExportFailure{
			{InstanceID: "i1", Reason: "boom", AttemptCount: 3, LastAttempt: time.Unix(0, 0).UTC()},
		},
	}
	encoded, err := json.Marshal(request)
	require.NoError(t, err)
	var decoded CommitCheckpointRequest
	require.NoError(t, json.Unmarshal(encoded, &decoded))
	assert.Equal(t, request.ScannedInstances, decoded.ScannedInstances)
	assert.Equal(t, request.ExportedInstances, decoded.ExportedInstances)
	require.NotNil(t, decoded.Checkpoint)
	assert.Equal(t, "cursor", decoded.Checkpoint.LastInstanceKey)
	require.Len(t, decoded.Failures, 1)
	assert.Equal(t, "i1", decoded.Failures[0].InstanceID)
	assert.Equal(t, 3, decoded.Failures[0].AttemptCount)

	// A failed batch commits a nil checkpoint so the cursor stays put.
	failed, err := json.Marshal(CommitCheckpointRequest{Failures: request.Failures})
	require.NoError(t, err)
	assert.NotContains(t, string(failed), `"Checkpoint"`)
}
