package exporthistory

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/microsoft/durabletask-go/api"
	durabletaskclient "github.com/microsoft/durabletask-go/client"
	"github.com/microsoft/durabletask-go/internal/protos"
	"github.com/microsoft/durabletask-go/task"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/types/known/timestamppb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

// fakeSource serves scripted instance pages, metadata, and histories to the
// export activities.
type fakeSource struct {
	mu sync.Mutex

	pages     []api.InstanceIDQueryResult
	pageIndex int
	listQuery []api.InstanceIDQuery
	listErr   error

	metadata    map[string]*api.OrchestrationMetadata
	metadataErr map[string]error

	history      map[string]*api.OrchestrationHistory
	historyErr   map[string]error
	historyQuery api.HistoryQuery
}

func newFakeSource() *fakeSource {
	return &fakeSource{
		metadata:    map[string]*api.OrchestrationMetadata{},
		metadataErr: map[string]error{},
		history:     map[string]*api.OrchestrationHistory{},
		historyErr:  map[string]error{},
	}
}

func (s *fakeSource) ListInstanceIDs(
	_ context.Context,
	query api.InstanceIDQuery,
) (*api.InstanceIDQueryResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.listQuery = append(s.listQuery, query)
	if s.listErr != nil {
		return nil, s.listErr
	}
	if s.pageIndex >= len(s.pages) {
		return &api.InstanceIDQueryResult{}, nil
	}
	page := s.pages[s.pageIndex]
	s.pageIndex++
	return &page, nil
}

func (s *fakeSource) FetchOrchestrationMetadata(
	_ context.Context,
	id api.InstanceID,
	_ ...api.FetchOrchestrationMetadataOptions,
) (*api.OrchestrationMetadata, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err, ok := s.metadataErr[string(id)]; ok {
		return nil, err
	}
	metadata, ok := s.metadata[string(id)]
	if !ok {
		return nil, api.ErrInstanceNotFound
	}
	return metadata, nil
}

func (s *fakeSource) GetOrchestrationHistory(
	_ context.Context,
	id api.InstanceID,
	query api.HistoryQuery,
) (*api.OrchestrationHistory, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.historyQuery = query
	if err, ok := s.historyErr[string(id)]; ok {
		return nil, err
	}
	history, ok := s.history[string(id)]
	if !ok {
		return nil, api.ErrInstanceNotFound
	}
	return history, nil
}

func (s *fakeSource) addInstance(instanceID string, status api.OrchestrationStatus, events int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	completedAt := time.Date(2024, time.April, 1, 12, 0, 0, 0, time.UTC)
	s.metadata[instanceID] = &api.OrchestrationMetadata{
		InstanceID:    api.InstanceID(instanceID),
		ExecutionID:   instanceID + "-execution",
		Name:          "TestOrchestration",
		RuntimeStatus: status,
		CompletedAt:   completedAt,
		LastUpdatedAt: completedAt,
	}
	history := &api.OrchestrationHistory{
		InstanceID:  api.InstanceID(instanceID),
		ExecutionID: instanceID + "-execution",
	}
	for i := 0; i < events; i++ {
		history.Events = append(history.Events, &api.HistoryEvent{
			Type:      api.HistoryEventOrchestratorStarted,
			EventID:   int32(i),
			Timestamp: completedAt,
		})
	}
	s.history[instanceID] = history
}

var _ HistorySource = (*fakeSource)(nil)

func newTestRuntime(source HistorySource, store Store) *exportRuntime {
	query, err := api.NormalizeHistoryQuery(api.HistoryQuery{})
	if err != nil {
		panic(err)
	}
	return &exportRuntime{source: source, store: store, historyPage: query}
}

// activityContext is a minimal [task.ActivityContext] that carries a
// pre-serialized input, so activity bodies can be driven directly.
type activityContext struct {
	ctx   context.Context
	input []byte
	// decoded stands in for a worker configured with a custom
	// api.DataConverter, whose decoding does not go through encoding/json and so
	// can produce values the JSON decoder would have rejected.
	decoded any
}

func newActivityContext(t *testing.T, input any) *activityContext {
	t.Helper()
	payload, err := json.Marshal(input)
	require.NoError(t, err)
	return &activityContext{ctx: context.Background(), input: payload}
}

func newDecodedActivityContext(input any) *activityContext {
	return &activityContext{ctx: context.Background(), decoded: input}
}

func (c *activityContext) GetInput(target any) error {
	if c.decoded != nil {
		reflect.ValueOf(target).Elem().Set(reflect.ValueOf(c.decoded))
		return nil
	}
	return json.Unmarshal(c.input, target)
}

func (c *activityContext) Context() context.Context { return c.ctx }

var _ task.ActivityContext = (*activityContext)(nil)

func TestListTerminalInstancesActivity(t *testing.T) {
	t.Run("passes the filter and checkpoint to the management query", func(t *testing.T) {
		source := newFakeSource()
		source.pages = []api.InstanceIDQueryResult{{
			InstanceIDs:       []api.InstanceID{"a", "b"},
			ContinuationToken: "next-cursor",
		}}
		runtime := newTestRuntime(source, newMemoryStore())

		from := time.Date(2024, time.March, 1, 0, 0, 0, 0, time.UTC)
		to := from.Add(time.Hour)
		result, err := runtime.listTerminalInstancesActivity(newActivityContext(t, ListTerminalInstancesRequest{
			CompletedTimeFrom:    from,
			CompletedTimeTo:      &to,
			RuntimeStatus:        []api.OrchestrationStatus{api.RUNTIME_STATUS_COMPLETED},
			LastInstanceKey:      "cursor",
			MaxInstancesPerBatch: 7,
		}))
		require.NoError(t, err)

		page, ok := result.(InstancePage)
		require.True(t, ok)
		assert.Equal(t, []string{"a", "b"}, page.InstanceIDs)
		require.NotNil(t, page.NextCheckpoint)
		assert.Equal(t, "next-cursor", page.NextCheckpoint.LastInstanceKey)

		require.Len(t, source.listQuery, 1)
		query := source.listQuery[0]
		assert.Equal(t, []api.OrchestrationStatus{api.RUNTIME_STATUS_COMPLETED}, query.RuntimeStatus)
		assert.Equal(t, from, query.CompletedTimeFrom)
		assert.Equal(t, to, query.CompletedTimeTo)
		assert.Equal(t, 7, query.PageSize)
		assert.Equal(t, "cursor", query.ContinuationToken)
	})

	t.Run("omits the backend cursor at the end of the stream", func(t *testing.T) {
		source := newFakeSource()
		// A task hub reports the last page by omitting the continuation token.
		source.pages = []api.InstanceIDQueryResult{{InstanceIDs: []api.InstanceID{"a"}}}
		runtime := newTestRuntime(source, newMemoryStore())
		result, err := runtime.listTerminalInstancesActivity(newActivityContext(t, ListTerminalInstancesRequest{}))
		require.NoError(t, err)
		page := result.(InstancePage)
		assert.Equal(t, []string{"a"}, page.InstanceIDs)
		assert.Nil(t, page.NextCheckpoint)
	})

	t.Run("defaults the status filter and page size", func(t *testing.T) {
		source := newFakeSource()
		source.pages = []api.InstanceIDQueryResult{{}}
		runtime := newTestRuntime(source, newMemoryStore())
		_, err := runtime.listTerminalInstancesActivity(newActivityContext(t, ListTerminalInstancesRequest{}))
		require.NoError(t, err)
		require.Len(t, source.listQuery, 1)
		assert.Equal(t, TerminalStatuses(), source.listQuery[0].RuntimeStatus)
		assert.Equal(t, DefaultMaxInstancesPerBatch, source.listQuery[0].PageSize)
	})

	t.Run("rejects non-terminal statuses", func(t *testing.T) {
		runtime := newTestRuntime(newFakeSource(), newMemoryStore())
		_, err := runtime.listTerminalInstancesActivity(newActivityContext(t, ListTerminalInstancesRequest{
			RuntimeStatus: []api.OrchestrationStatus{api.RUNTIME_STATUS_RUNNING},
		}))
		require.ErrorIs(t, err, ErrValidation)
	})

	t.Run("surfaces management failures", func(t *testing.T) {
		failure := errors.New("list failed")
		source := newFakeSource()
		source.listErr = failure
		runtime := newTestRuntime(source, newMemoryStore())
		_, err := runtime.listTerminalInstancesActivity(newActivityContext(t, ListTerminalInstancesRequest{}))
		require.ErrorIs(t, err, failure)
	})

	t.Run("requires a configured source", func(t *testing.T) {
		runtime := &exportRuntime{store: newMemoryStore()}
		_, err := runtime.listTerminalInstancesActivity(newActivityContext(t, ListTerminalInstancesRequest{}))
		require.Error(t, err)
		assert.Contains(t, err.Error(), "no configured history source")
	})
}

func TestExportInstanceHistoryActivity(t *testing.T) {
	destination := ExportDestination{Container: "container", Prefix: "batch-job/"}

	t.Run("writes a gzip-compressed JSONL object", func(t *testing.T) {
		source := newFakeSource()
		source.addInstance("instance-1", api.RUNTIME_STATUS_COMPLETED, 3)
		store := newMemoryStore()
		runtime := newTestRuntime(source, store)

		result, err := runtime.exportInstanceHistoryActivity(newActivityContext(t, ExportRequest{
			InstanceID:  "instance-1",
			Destination: destination,
			Format:      DefaultExportFormat(),
		}))
		require.NoError(t, err)
		exported, ok := result.(ExportResult)
		require.True(t, ok)
		require.True(t, exported.Success, exported.Error)
		assert.Equal(t, "instance-1", exported.InstanceID)
		assert.Equal(t, 3, exported.EventCount)
		assert.True(t, strings.HasPrefix(exported.BlobPath, "batch-job/"))
		assert.True(t, strings.HasSuffix(exported.BlobPath, ".jsonl.gz"))

		objects := store.snapshot()
		require.Len(t, objects, 1)
		object := objects["container/"+exported.BlobPath]
		// A gzip-compressed JSONL object is stored as an opaque gzip file, so it
		// carries no content coding a reader could transparently apply.
		assert.Equal(t, "application/gzip", object.ContentType)
		assert.Equal(t, "instance-1", object.Metadata["instanceId"])
		assert.Equal(t, "instance-1-execution", object.Metadata["executionId"])
		assert.Equal(t, DefaultSchemaVersion, object.Metadata["schemaVersion"])

		decompressed := decompressGzip(t, object.Content)
		lines := strings.Split(strings.TrimRight(string(decompressed), "\n"), "\n")
		require.Len(t, lines, 3)
		for _, line := range lines {
			var event api.HistoryEvent
			require.NoError(t, json.Unmarshal([]byte(line), &event))
			assert.Equal(t, api.HistoryEventOrchestratorStarted, event.Type)
		}
	})

	t.Run("writes an uncompressed JSON array", func(t *testing.T) {
		source := newFakeSource()
		source.addInstance("instance-1", api.RUNTIME_STATUS_FAILED, 2)
		store := newMemoryStore()
		runtime := newTestRuntime(source, store)

		result, err := runtime.exportInstanceHistoryActivity(newActivityContext(t, ExportRequest{
			InstanceID:  "instance-1",
			Destination: destination,
			Format:      ExportFormat{Kind: ExportFormatJSON, SchemaVersion: "1.0"},
		}))
		require.NoError(t, err)
		exported := result.(ExportResult)
		require.True(t, exported.Success, exported.Error)
		assert.True(t, strings.HasSuffix(exported.BlobPath, ".json"))

		object := store.snapshot()["container/"+exported.BlobPath]
		assert.Equal(t, "application/json", object.ContentType)
		var events []api.HistoryEvent
		require.NoError(t, json.Unmarshal(object.Content, &events))
		require.Len(t, events, 2)
	})

	t.Run("object names are deterministic and instance-specific", func(t *testing.T) {
		source := newFakeSource()
		source.addInstance("instance-1", api.RUNTIME_STATUS_COMPLETED, 1)
		source.addInstance("instance-2", api.RUNTIME_STATUS_COMPLETED, 1)
		store := newMemoryStore()
		runtime := newTestRuntime(source, store)

		request := ExportRequest{
			InstanceID:  "instance-1",
			Destination: destination,
			Format:      DefaultExportFormat(),
		}
		first, err := runtime.exportInstanceHistoryActivity(newActivityContext(t, request))
		require.NoError(t, err)
		second, err := runtime.exportInstanceHistoryActivity(newActivityContext(t, request))
		require.NoError(t, err)
		assert.Equal(t, first.(ExportResult).BlobPath, second.(ExportResult).BlobPath)
		// Re-exporting overwrites rather than duplicating.
		assert.Equal(t, 1, store.count())
		assert.Equal(t, 2, store.writeCount())

		request.InstanceID = "instance-2"
		other, err := runtime.exportInstanceHistoryActivity(newActivityContext(t, request))
		require.NoError(t, err)
		assert.NotEqual(t, first.(ExportResult).BlobPath, other.(ExportResult).BlobPath)
		assert.Equal(t, 2, store.count())
	})

	t.Run("collects permanent per-instance failures without retrying", func(t *testing.T) {
		source := newFakeSource()
		source.addInstance("running", api.RUNTIME_STATUS_RUNNING, 1)
		source.metadata["nil-metadata"] = nil
		source.metadataErr["wrapped-missing"] = fmt.Errorf("lookup: %w", api.ErrInstanceNotFound)
		runtime := newTestRuntime(source, newMemoryStore())

		tests := []struct {
			instanceID string
			message    string
		}{
			{"missing", "instance missing not found"},
			{"nil-metadata", "instance nil-metadata not found"},
			{"wrapped-missing", "instance wrapped-missing not found"},
			{"running", "instance running is not in a completed state"},
		}
		for _, test := range tests {
			result, err := runtime.exportInstanceHistoryActivity(newActivityContext(t, ExportRequest{
				InstanceID:  test.instanceID,
				Destination: destination,
				Format:      DefaultExportFormat(),
			}))
			require.NoError(t, err, test.instanceID)
			exported := result.(ExportResult)
			assert.False(t, exported.Success, test.instanceID)
			assert.Equal(t, test.instanceID, exported.InstanceID)
			assert.Equal(t, test.message, exported.Error)
		}
	})

	// Transient failures must fail the activity so its retry policy applies;
	// the orchestration collects them only after every attempt is exhausted.
	t.Run("fails the activity for transient failures", func(t *testing.T) {
		source := newFakeSource()
		source.addInstance("history-error", api.RUNTIME_STATUS_COMPLETED, 1)
		source.historyErr["history-error"] = errors.New("history unavailable")
		source.metadataErr["metadata-error"] = errors.New("metadata unavailable")
		source.addInstance("store-error", api.RUNTIME_STATUS_COMPLETED, 1)
		store := newMemoryStore()
		store.failInstance("store-error", errors.New("upload rejected"))
		runtime := newTestRuntime(source, store)

		tests := []struct {
			instanceID string
			message    string
		}{
			{"history-error", "failed to read instance history-error history"},
			{"metadata-error", "failed to read instance metadata-error metadata"},
			{"store-error", "upload rejected"},
		}
		for _, test := range tests {
			result, err := runtime.exportInstanceHistoryActivity(newActivityContext(t, ExportRequest{
				InstanceID:  test.instanceID,
				Destination: destination,
				Format:      DefaultExportFormat(),
			}))
			require.Error(t, err, test.instanceID)
			assert.Nil(t, result, test.instanceID)
			assert.Contains(t, err.Error(), test.message)
		}
	})

	t.Run("rejects malformed requests", func(t *testing.T) {
		runtime := newTestRuntime(newFakeSource(), newMemoryStore())
		tests := []ExportRequest{
			{Destination: destination, Format: DefaultExportFormat()},
			{InstanceID: "i", Format: DefaultExportFormat()},
			{InstanceID: "i", Destination: ExportDestination{Container: "BAD"}, Format: DefaultExportFormat()},
		}
		for i, request := range tests {
			_, err := runtime.exportInstanceHistoryActivity(newActivityContext(t, request))
			require.ErrorIs(t, err, ErrValidation, "case %d", i)
		}

		// An out-of-range format kind cannot be produced by the JSON converter,
		// but a custom api.DataConverter can, so the activity still checks it.
		_, err := runtime.exportInstanceHistoryActivity(newDecodedActivityContext(ExportRequest{
			InstanceID:  "i",
			Destination: destination,
			Format:      ExportFormat{Kind: ExportFormatKind(9)},
		}))
		require.ErrorIs(t, err, ErrValidation)
	})

	t.Run("requires configured dependencies", func(t *testing.T) {
		request := newActivityContext(t, ExportRequest{
			InstanceID:  "i",
			Destination: destination,
			Format:      DefaultExportFormat(),
		})
		_, err := (&exportRuntime{store: newMemoryStore()}).exportInstanceHistoryActivity(request)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "no configured history source")

		_, err = (&exportRuntime{source: newFakeSource()}).exportInstanceHistoryActivity(request)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "no configured store")
	})

	t.Run("falls back to LastUpdatedAt when CompletedAt is unset", func(t *testing.T) {
		lastUpdated := time.Date(2024, time.February, 2, 2, 2, 2, 0, time.UTC)
		metadata := &api.OrchestrationMetadata{LastUpdatedAt: lastUpdated}
		assert.Equal(t, lastUpdated, completionTimestamp(metadata))
		metadata.CompletedAt = lastUpdated.Add(time.Hour)
		assert.Equal(t, lastUpdated.Add(time.Hour), completionTimestamp(metadata))
	})
}

func TestSerializeHistory(t *testing.T) {
	t.Run("empty JSONL history produces an empty gzip stream", func(t *testing.T) {
		content, contentType, err := serializeHistory(nil, DefaultExportFormat())
		require.NoError(t, err)
		assert.Equal(t, "application/gzip", contentType)
		decompressed := decompressGzip(t, content)
		assert.Empty(t, decompressed)
	})

	t.Run("empty JSON history produces an empty array", func(t *testing.T) {
		content, contentType, err := serializeHistory(nil, ExportFormat{Kind: ExportFormatJSON})
		require.NoError(t, err)
		assert.Equal(t, "application/json", contentType)
		assert.JSONEq(t, `[]`, string(content))
	})

	t.Run("nil events are skipped rather than serialized as null", func(t *testing.T) {
		events := []*api.HistoryEvent{
			{Type: api.HistoryEventExecutionStarted},
			nil,
			{Type: api.HistoryEventExecutionCompleted},
		}
		content, _, err := serializeHistory(events, DefaultExportFormat())
		require.NoError(t, err)
		decompressed := decompressGzip(t, content)
		lines := strings.Split(strings.TrimRight(string(decompressed), "\n"), "\n")
		require.Len(t, lines, 2)
		assert.NotContains(t, string(decompressed), "null")
	})
}

func TestBlobObjectName(t *testing.T) {
	completedAt := time.Date(2024, time.January, 1, 0, 0, 0, 0, time.UTC)
	name := blobObjectName(completedAt, "instance", DefaultExportFormat())
	assert.True(t, strings.HasSuffix(name, ".jsonl.gz"))
	assert.Len(t, strings.TrimSuffix(name, ".jsonl.gz"), 64)
	assert.Equal(t, name, blobObjectName(completedAt.In(time.FixedZone("x", 3600)), "instance", DefaultExportFormat()))
	assert.NotEqual(t, name, blobObjectName(completedAt.Add(time.Nanosecond), "instance", DefaultExportFormat()))
	assert.NotEqual(t, name, blobObjectName(completedAt, "other", DefaultExportFormat()))

	jsonName := blobObjectName(completedAt, "instance", ExportFormat{Kind: ExportFormatJSON})
	assert.True(t, strings.HasSuffix(jsonName, ".json"))
	assert.Equal(t,
		strings.TrimSuffix(name, ".jsonl.gz"),
		strings.TrimSuffix(jsonName, ".json"),
		"the digest must not depend on the format")
}

func TestExportActivityRetryPolicy(t *testing.T) {
	policy := exportActivityRetryPolicy()
	require.NoError(t, policy.Validate())
	assert.Equal(t, 3, policy.MaxAttempts)
	assert.Equal(t, 15*time.Second, policy.InitialRetryInterval)
	assert.Equal(t, 2.0, policy.BackoffCoefficient)
	assert.Equal(t, time.Minute, policy.MaxRetryInterval)
}

func TestRegisterValidation(t *testing.T) {
	source := newFakeSource()
	store := newMemoryStore()

	require.ErrorIs(t, Register(nil, WorkerOptions{Source: source, Store: store}), ErrValidation)
	require.ErrorIs(t, Register(task.NewTaskRegistry(), WorkerOptions{Store: store}), ErrValidation)
	require.ErrorIs(t, Register(task.NewTaskRegistry(), WorkerOptions{Source: source}), ErrValidation)

	err := Register(task.NewTaskRegistry(), WorkerOptions{
		Source:       source,
		Store:        store,
		HistoryQuery: api.HistoryQuery{MaxEvents: -1},
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid export history query")
}

// TestRegisterAddsEverySystemTask keeps the registry snapshot, and therefore the
// worker's derived work-item filters, complete.
func TestRegisterAddsEverySystemTask(t *testing.T) {
	registry := task.NewTaskRegistry()
	require.NoError(t, Register(registry, WorkerOptions{Source: newFakeSource(), Store: newMemoryStore()}))

	snapshot := registry.Snapshot()
	orchestrators := map[string]string{}
	for _, registration := range snapshot.Orchestrators {
		orchestrators[registration.Name] = registration.Version
	}
	assert.Contains(t, orchestrators, ExportJobOrchestratorName)
	assert.Contains(t, orchestrators, ExecuteExportJobOperationOrchestratorName)
	// System tasks stay unversioned so default versioning cannot hide them.
	assert.Equal(t, task.UnversionedTaskVersion, orchestrators[ExportJobOrchestratorName])
	assert.Equal(t, task.UnversionedTaskVersion, orchestrators[ExecuteExportJobOperationOrchestratorName])

	activities := map[string]string{}
	for _, registration := range snapshot.Activities {
		activities[registration.Name] = registration.Version
	}
	assert.Contains(t, activities, ListTerminalInstancesActivityName)
	assert.Contains(t, activities, ExportInstanceHistoryActivityName)
	assert.Equal(t, task.UnversionedTaskVersion, activities[ListTerminalInstancesActivityName])
	assert.Equal(t, task.UnversionedTaskVersion, activities[ExportInstanceHistoryActivityName])

	assert.Contains(t, snapshot.Entities, strings.ToLower(ExportJobEntityName))
}

func TestRegisterRejectsDuplicateRegistration(t *testing.T) {
	registry := task.NewTaskRegistry()
	options := WorkerOptions{Source: newFakeSource(), Store: newMemoryStore()}
	require.NoError(t, Register(registry, options))
	require.Error(t, Register(registry, options))
}

func TestSystemNamesMatchDotNet(t *testing.T) {
	// These names are part of the cross-SDK contract: a .NET client creating a
	// job schedules these exact orchestrator and entity names.
	assert.Equal(t, "ExportJob", ExportJobEntityName)
	assert.Equal(t, "ExportJobOrchestrator", ExportJobOrchestratorName)
	assert.Equal(t, "ExecuteExportJobOperationOrchestrator", ExecuteExportJobOperationOrchestratorName)
	assert.Equal(t, "ListTerminalInstancesActivity", ListTerminalInstancesActivityName)
	assert.Equal(t, "ExportInstanceHistoryActivity", ExportInstanceHistoryActivityName)
	assert.Equal(t, "ExportJob-", OrchestratorInstanceIDPrefix)

	assert.Equal(t, "Create", createOperation)
	assert.Equal(t, "Get", getOperation)
	assert.Equal(t, "Run", runOperation)
	assert.Equal(t, "CommitCheckpoint", commitCheckpointOperation)
	assert.Equal(t, "MarkAsCompleted", markAsCompletedOperation)
	assert.Equal(t, "MarkAsFailed", markAsFailedOperation)
	assert.Equal(t, "Delete", deleteOperation)
}

func TestExportRuntimeUsesTheConfiguredHistoryQuery(t *testing.T) {
	source := newFakeSource()
	source.addInstance("instance-1", api.RUNTIME_STATUS_COMPLETED, 1)
	source.addInstance("instance-2", api.RUNTIME_STATUS_COMPLETED, 1)
	query := api.HistoryQuery{ExecutionID: "shared-default", MaxEvents: 11, MaxBytes: 2048}
	runtime := &exportRuntime{
		source:      source,
		store:       newMemoryStore(),
		historyPage: query,
	}
	for _, instanceID := range []string{"instance-1", "instance-2"} {
		_, err := runtime.exportInstanceHistoryActivity(newActivityContext(t, ExportRequest{
			InstanceID:  instanceID,
			Destination: ExportDestination{Container: "container"},
			Format:      DefaultExportFormat(),
		}))
		require.NoError(t, err)
		assert.Equal(t, instanceID+"-execution", source.historyQuery.ExecutionID)
		assert.Equal(t, query.MaxEvents, source.historyQuery.MaxEvents)
		assert.Equal(t, query.MaxBytes, source.historyQuery.MaxBytes)
		assert.Equal(t, query, runtime.historyPage, "per-instance pinning must not mutate the shared query")
	}
}

func TestExportInstanceHistoryRejectsExecutionIdentity(t *testing.T) {
	for _, test := range []struct {
		name        string
		metadataID  string
		historyID   string
		readHistory bool
	}{
		{"different execution", "A", "B", true},
		{"missing history execution", "A", "", true},
		{"missing metadata execution", "", "A", false},
	} {
		t.Run(test.name, func(t *testing.T) {
			source := newFakeSource()
			source.addInstance("instance-1", api.RUNTIME_STATUS_COMPLETED, 1)
			source.metadata["instance-1"].ExecutionID = test.metadataID
			source.history["instance-1"].ExecutionID = test.historyID
			// Identity validation must run before attempting to serialize content.
			details := &api.FailureDetails{}
			details.InnerFailure = details
			source.history["instance-1"].Events = []*api.HistoryEvent{{
				TaskFailed: &api.HistoryTaskFailureEvent{FailureDetails: details},
			}}
			store := newMemoryStore()
			runtime := newTestRuntime(source, store)
			_, err := runtime.exportInstanceHistoryActivity(newActivityContext(t, ExportRequest{
				InstanceID:  "instance-1",
				Destination: ExportDestination{Container: "container"},
				Format:      DefaultExportFormat(),
			}))
			assert.ErrorContains(t, err, "execution")
			assert.Zero(t, store.writeCount())
			assert.Empty(t, store.snapshot())
			if test.readHistory {
				assert.Equal(t, test.metadataID, source.historyQuery.ExecutionID)
			} else {
				assert.Empty(t, source.historyQuery, "metadata without an identity must not trigger a history read")
			}
		})
	}
}

func TestExportInstanceHistoryPinsConcurrentReads(t *testing.T) {
	source := newFakeSource()
	for _, id := range []string{"instance-1", "instance-2"} {
		source.addInstance(id, api.RUNTIME_STATUS_COMPLETED, 1)
	}
	store := newMemoryStore()
	runtime := newTestRuntime(source, store)
	query := runtime.historyPage
	results := make(chan error, 2)
	for _, id := range []string{"instance-1", "instance-2"} {
		go func() {
			_, err := runtime.exportInstance(context.Background(), ExportRequest{
				InstanceID: id, Destination: ExportDestination{Container: "container"}, Format: DefaultExportFormat(),
			})
			results <- err
		}()
	}
	for range 2 {
		require.NoError(t, <-results)
	}
	require.Equal(t, query, runtime.historyPage)
	require.Equal(t, 2, store.writeCount())
}

type exportHistoryIdentityServer struct {
	protos.UnimplementedTaskHubSidecarServiceServer
	state    *protos.OrchestrationState
	events   []*protos.HistoryEvent
	requests chan *protos.StreamInstanceHistoryRequest
}

func (s *exportHistoryIdentityServer) GetInstance(context.Context, *protos.GetInstanceRequest) (*protos.GetInstanceResponse, error) {
	return &protos.GetInstanceResponse{Exists: true, OrchestrationState: s.state}, nil
}

func (s *exportHistoryIdentityServer) StreamInstanceHistory(
	request *protos.StreamInstanceHistoryRequest,
	stream protos.TaskHubSidecarService_StreamInstanceHistoryServer,
) error {
	s.requests <- request
	return stream.Send(&protos.HistoryChunk{Events: s.events})
}

func TestExportInstanceHistoryPinsTheClientCollector(t *testing.T) {
	for _, test := range []struct {
		name         string
		executionIDs []string
		wantError    bool
	}{
		{"matching execution", []string{"A"}, false},
		{"restarted execution", []string{"B"}, true},
		{"mixed executions", []string{"A", "B"}, true},
		{"missing execution", []string{""}, true},
		{"empty history", nil, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			serverImpl := &exportHistoryIdentityServer{
				state: &protos.OrchestrationState{
					InstanceId:          "instance-1",
					ExecutionId:         wrapperspb.String("A"),
					OrchestrationStatus: api.RUNTIME_STATUS_COMPLETED,
					CompletedTimestamp:  timestamppb.New(time.Date(2024, time.April, 1, 12, 0, 0, 0, time.UTC)),
				},
				requests: make(chan *protos.StreamInstanceHistoryRequest, 1),
			}
			for _, executionID := range test.executionIDs {
				serverImpl.events = append(serverImpl.events, &protos.HistoryEvent{
					EventType: &protos.HistoryEvent_ExecutionStarted{ExecutionStarted: &protos.ExecutionStartedEvent{
						OrchestrationInstance: &protos.OrchestrationInstance{
							InstanceId: "instance-1", ExecutionId: wrapperspb.String(executionID),
						},
					}},
				})
			}
			server := grpc.NewServer()
			protos.RegisterTaskHubSidecarServiceServer(server, serverImpl)
			listener := bufconn.Listen(1024 * 1024)
			go func() { _ = server.Serve(listener) }()
			t.Cleanup(func() {
				server.Stop()
				require.NoError(t, listener.Close())
			})
			connection, err := grpc.NewClient(
				"passthrough:///export-history-identity",
				grpc.WithTransportCredentials(insecure.NewCredentials()),
				grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
					return listener.DialContext(ctx)
				}),
			)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, connection.Close()) })

			store := newMemoryStore()
			runtime := newTestRuntime(durabletaskclient.NewTaskHubGrpcClient(connection, api.DefaultLogger()), store)
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			input := newActivityContext(t, ExportRequest{
				InstanceID: "instance-1", Destination: ExportDestination{Container: "container"}, Format: DefaultExportFormat(),
			})
			input.ctx = ctx
			result, err := runtime.exportInstanceHistoryActivity(input)
			if test.wantError {
				assert.ErrorContains(t, err, "execution")
				assert.Nil(t, result)
				assert.Zero(t, store.writeCount())
			} else {
				require.NoError(t, err)
				require.True(t, result.(ExportResult).Success)
				require.Equal(t, 1, store.writeCount())
				for _, object := range store.snapshot() {
					require.Equal(t, "A", object.Metadata["executionId"])
				}
			}
			select {
			case request := <-serverImpl.requests:
				assert.Equal(t, "A", request.GetExecutionId().GetValue())
			case <-ctx.Done():
				t.Fatal("history was not requested")
			}
			assert.Empty(t, runtime.historyPage.ExecutionID)
		})
	}
}

func TestExportJobRunRequestJSON(t *testing.T) {
	request := ExportJobRunRequest{
		JobEntityID:        EntityID("job-1"),
		ProcessedCycles:    3,
		RunToken:           "run-a",
		ContinuedExecution: true,
	}
	encoded, err := json.Marshal(request)
	require.NoError(t, err)
	assert.Contains(t, string(encoded), `"JobEntityId"`)
	var decoded ExportJobRunRequest
	require.NoError(t, json.Unmarshal(encoded, &decoded))
	assert.Equal(t, request, decoded)
}

func TestExportJobOperationRequestJSON(t *testing.T) {
	request := ExportJobOperationRequest{
		EntityID:      EntityID("job-1"),
		OperationName: createOperation,
		Input:         map[string]any{"Mode": float64(1)},
	}
	encoded, err := json.Marshal(request)
	require.NoError(t, err)
	assert.Contains(t, string(encoded), `"EntityId"`)
	assert.Contains(t, string(encoded), `"OperationName"`)
	var decoded ExportJobOperationRequest
	require.NoError(t, json.Unmarshal(encoded, &decoded))
	assert.Equal(t, request.EntityID, decoded.EntityID)
	assert.Equal(t, request.OperationName, decoded.OperationName)

	// A nil input is omitted rather than serialized as null.
	encoded, err = json.Marshal(ExportJobOperationRequest{
		EntityID:      EntityID("job-1"),
		OperationName: getOperation,
	})
	require.NoError(t, err)
	assert.NotContains(t, string(encoded), `"Input"`)
}

func TestInstancePageJSON(t *testing.T) {
	page := InstancePage{
		InstanceIDs:    []string{"a", "b"},
		NextCheckpoint: &ExportCheckpoint{LastInstanceKey: "cursor"},
	}
	encoded, err := json.Marshal(page)
	require.NoError(t, err)
	var decoded InstancePage
	require.NoError(t, json.Unmarshal(encoded, &decoded))
	assert.Equal(t, page.InstanceIDs, decoded.InstanceIDs)
	require.NotNil(t, decoded.NextCheckpoint)
	assert.Equal(t, "cursor", decoded.NextCheckpoint.LastInstanceKey)
}

func TestExportResultJSON(t *testing.T) {
	result := ExportResult{InstanceID: "i", Success: true, BlobPath: "p/o", EventCount: 4}
	encoded, err := json.Marshal(result)
	require.NoError(t, err)
	var decoded ExportResult
	require.NoError(t, json.Unmarshal(encoded, &decoded))
	assert.Equal(t, result, decoded)

	failed, err := json.Marshal(ExportResult{InstanceID: "i", Error: "boom"})
	require.NoError(t, err)
	assert.Contains(t, string(failed), `"Error":"boom"`)
	assert.NotContains(t, string(failed), `"BlobPath"`)
}

func TestFakeSourceHelpersProduceDistinctInstances(t *testing.T) {
	source := newFakeSource()
	for i := 0; i < 3; i++ {
		source.addInstance(fmt.Sprintf("instance-%d", i), api.RUNTIME_STATUS_COMPLETED, i+1)
	}
	for i := 0; i < 3; i++ {
		history, err := source.GetOrchestrationHistory(
			context.Background(),
			api.InstanceID(fmt.Sprintf("instance-%d", i)),
			api.HistoryQuery{},
		)
		require.NoError(t, err)
		assert.Len(t, history.Events, i+1)
	}
}
