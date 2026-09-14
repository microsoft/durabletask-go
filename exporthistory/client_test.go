package exporthistory

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/microsoft/durabletask-go/api"
	durabletaskclient "github.com/microsoft/durabletask-go/client"
	"github.com/microsoft/durabletask-go/internal/protos"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The export client and activities are written against the task hub client's
// public surface, so the concrete gRPC client must satisfy both contracts.
var (
	_ taskHubClient = (*durabletaskclient.TaskHubGrpcClient)(nil)
	_ HistorySource = (*durabletaskclient.TaskHubGrpcClient)(nil)
)

// fakeBackend records the calls the export client makes and replays scripted
// responses, standing in for a task hub.
type fakeBackend struct {
	mu sync.Mutex

	scheduled   []scheduledCall
	terminated  []api.InstanceID
	purged      []api.InstanceID
	waited      []api.InstanceID
	entityReads []api.EntityID

	scheduleErr    error
	scheduleResult api.InstanceID
	completion     *api.OrchestrationMetadata
	completionErr  error
	// completionErrByInstance overrides completionErr for one instance, so a
	// test can fail the orchestration cleanup without failing the entity
	// operation that runs first.
	completionErrByInstance map[api.InstanceID]error
	terminateErr            error
	purgeErr                error

	entity     *api.EntityMetadata
	entityErr  error
	queryPages *api.EntityQueryResults
	queryErr   error
	lastQuery  api.EntityQuery
}

type scheduledCall struct {
	name    string
	request ExportJobOperationRequest
	version string
}

func newFakeBackend() *fakeBackend {
	return &fakeBackend{
		scheduleResult: "test-instance",
		completion:     &api.OrchestrationMetadata{RuntimeStatus: api.RUNTIME_STATUS_COMPLETED},
	}
}

func (f *fakeBackend) ScheduleNewOrchestration(
	_ context.Context,
	orchestrator string,
	opts ...api.NewOrchestrationOptions,
) (api.InstanceID, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.scheduleErr != nil {
		return "", f.scheduleErr
	}
	// Replay the functional options through the real request type so the test
	// observes exactly the input and version the client sent.
	request := &protos.CreateInstanceRequest{}
	for _, configure := range opts {
		if err := configure(request, api.NormalizeDataConverter(nil)); err != nil {
			return "", err
		}
	}
	var decoded ExportJobOperationRequest
	if input := request.GetInput().GetValue(); input != "" {
		if err := json.Unmarshal([]byte(input), &decoded); err != nil {
			return "", err
		}
	}
	f.scheduled = append(f.scheduled, scheduledCall{
		name:    orchestrator,
		request: decoded,
		version: request.GetVersion().GetValue(),
	})
	return f.scheduleResult, nil
}

func (f *fakeBackend) WaitForOrchestrationCompletion(
	_ context.Context,
	id api.InstanceID,
	_ ...api.FetchOrchestrationMetadataOptions,
) (*api.OrchestrationMetadata, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.waited = append(f.waited, id)
	if err, ok := f.completionErrByInstance[id]; ok {
		return nil, err
	}
	if f.completionErr != nil {
		return nil, f.completionErr
	}
	return f.completion, nil
}

func (f *fakeBackend) TerminateOrchestration(
	_ context.Context,
	id api.InstanceID,
	_ ...api.TerminateOptions,
) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.terminated = append(f.terminated, id)
	return f.terminateErr
}

func (f *fakeBackend) PurgeOrchestrationState(
	_ context.Context,
	id api.InstanceID,
	_ ...api.PurgeOptions,
) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.purged = append(f.purged, id)
	return f.purgeErr
}

func (f *fakeBackend) GetEntity(
	_ context.Context,
	id api.EntityID,
	_ ...api.GetEntityOptions,
) (*api.EntityMetadata, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.entityReads = append(f.entityReads, id)
	if f.entityErr != nil {
		return nil, f.entityErr
	}
	return f.entity, nil
}

func (f *fakeBackend) QueryEntities(
	_ context.Context,
	query api.EntityQuery,
) (*api.EntityQueryResults, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.lastQuery = query
	if f.queryErr != nil {
		return nil, f.queryErr
	}
	return f.queryPages, nil
}

func entityMetadata(t *testing.T, jobID string, state ExportJobState) *api.EntityMetadata {
	t.Helper()
	payload, err := json.Marshal(state)
	require.NoError(t, err)
	return &api.EntityMetadata{
		InstanceID:      EntityID(jobID),
		StateIncluded:   true,
		HasState:        true,
		SerializedState: string(payload),
	}
}

func newTestClient(t *testing.T, hub taskHubClient, options ClientOptions) *Client {
	t.Helper()
	client, err := newClient(hub, options)
	require.NoError(t, err)
	return client
}

func TestNewClientValidation(t *testing.T) {
	_, err := NewClient(nil, ClientOptions{})
	require.ErrorIs(t, err, ErrValidation)

	_, err = newClient(nil, ClientOptions{})
	require.ErrorIs(t, err, ErrValidation)

	_, err = newClient(newFakeBackend(), ClientOptions{ContainerName: "Bad_Container"})
	require.ErrorIs(t, err, ErrValidation)
	assert.Contains(t, err.Error(), "not a valid Azure Blob container name")

	_, err = newClient(newFakeBackend(), ClientOptions{ContainerName: "container", Prefix: "/bad"})
	require.ErrorIs(t, err, ErrValidation)

	client, err := newClient(newFakeBackend(), ClientOptions{})
	require.NoError(t, err)
	assert.NotNil(t, client)
}

func TestJobClientValidation(t *testing.T) {
	client := newTestClient(t, newFakeBackend(), ClientOptions{ContainerName: "container"})
	for _, jobID := range []string{"", "  ", "bad@id"} {
		_, err := client.JobClient(jobID)
		require.ErrorIs(t, err, ErrValidation, "job ID %q", jobID)
	}
	job, err := client.JobClient("job-1")
	require.NoError(t, err)
	assert.Equal(t, "job-1", job.ID())

	var nilClient *Client
	_, err = nilClient.JobClient("job")
	require.ErrorIs(t, err, ErrValidation)

	var nilJob *JobClient
	assert.Empty(t, nilJob.ID())
	require.ErrorIs(t, nilJob.Create(context.Background(), JobCreationOptions{}), ErrValidation)
	_, describeErr := nilJob.Describe(context.Background())
	require.ErrorIs(t, describeErr, ErrValidation)
	require.ErrorIs(t, nilJob.Delete(context.Background()), ErrValidation)
}

func TestCreateJobSchedulesTheOperationOrchestrator(t *testing.T) {
	hub := newFakeBackend()
	client := newTestClient(t, hub, ClientOptions{ContainerName: "container"})

	job, err := client.CreateJob(context.Background(), JobCreationOptions{
		Mode:              ExportModeBatch,
		CompletedTimeFrom: time.Now().UTC().Add(-time.Hour),
		CompletedTimeTo:   time.Now().UTC().Add(-time.Minute),
	})
	require.NoError(t, err)
	require.NotNil(t, job)
	assert.NotEmpty(t, job.ID())

	require.Len(t, hub.scheduled, 1)
	call := hub.scheduled[0]
	assert.Equal(t, ExecuteExportJobOperationOrchestratorName, call.name)
	assert.Equal(t, createOperation, call.request.OperationName)
	assert.Equal(t, EntityID(job.ID()), call.request.EntityID)
	// System operations must not inherit an application default version.
	assert.Equal(t, "", call.version)
	require.Equal(t, []api.InstanceID{"test-instance"}, hub.waited)
}

func TestCreateJobResolvesTheDestination(t *testing.T) {
	tests := []struct {
		name           string
		clientOptions  ClientOptions
		destination    *ExportDestination
		mode           ExportMode
		expectedPrefix string
		expectedName   string
	}{
		{
			name:           "default prefix derives from mode and job ID",
			clientOptions:  ClientOptions{ContainerName: "container"},
			mode:           ExportModeBatch,
			expectedPrefix: "batch-job-1/",
			expectedName:   "container",
		},
		{
			name:           "continuous jobs get their own prefix",
			clientOptions:  ClientOptions{ContainerName: "container"},
			mode:           ExportModeContinuous,
			expectedPrefix: "continuous-job-1/",
			expectedName:   "container",
		},
		{
			name:           "client prefix wins over the derived default",
			clientOptions:  ClientOptions{ContainerName: "container", Prefix: "client-prefix/"},
			mode:           ExportModeBatch,
			expectedPrefix: "client-prefix/",
			expectedName:   "container",
		},
		{
			name:           "explicit destination wins over the client defaults",
			clientOptions:  ClientOptions{ContainerName: "container", Prefix: "client-prefix/"},
			destination:    &ExportDestination{Container: "other", Prefix: "explicit/"},
			mode:           ExportModeBatch,
			expectedPrefix: "explicit/",
			expectedName:   "other",
		},
		{
			name:           "an explicit container still gets the derived prefix",
			clientOptions:  ClientOptions{ContainerName: "container"},
			destination:    &ExportDestination{Container: "other"},
			mode:           ExportModeBatch,
			expectedPrefix: "batch-job-1/",
			expectedName:   "other",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			hub := newFakeBackend()
			client := newTestClient(t, hub, test.clientOptions)
			job, err := client.JobClient("job-1")
			require.NoError(t, err)

			options := JobCreationOptions{Mode: test.mode, Destination: test.destination}
			if test.mode == ExportModeBatch {
				options.CompletedTimeFrom = time.Now().UTC().Add(-time.Hour)
				options.CompletedTimeTo = time.Now().UTC().Add(-time.Minute)
			}
			require.NoError(t, job.Create(context.Background(), options))

			require.Len(t, hub.scheduled, 1)
			created, ok := hub.scheduled[0].request.Input.(map[string]any)
			require.True(t, ok)
			destination, ok := created["Destination"].(map[string]any)
			require.True(t, ok)
			assert.Equal(t, test.expectedName, destination["Container"])
			assert.Equal(t, test.expectedPrefix, destination["Prefix"])
		})
	}
}

func TestCreateJobRequiresAContainer(t *testing.T) {
	client := newTestClient(t, newFakeBackend(), ClientOptions{})
	job, err := client.JobClient("job-1")
	require.NoError(t, err)
	err = job.Create(context.Background(), JobCreationOptions{
		Mode:              ExportModeBatch,
		CompletedTimeFrom: time.Now().UTC().Add(-time.Hour),
		CompletedTimeTo:   time.Now().UTC().Add(-time.Minute),
	})
	require.ErrorIs(t, err, ErrValidation)
	assert.Contains(t, err.Error(), "ClientOptions.ContainerName")
}

func TestCreateJobRejectsMismatchedJobID(t *testing.T) {
	client := newTestClient(t, newFakeBackend(), ClientOptions{ContainerName: "container"})
	job, err := client.JobClient("job-1")
	require.NoError(t, err)
	err = job.Create(context.Background(), JobCreationOptions{
		JobID:             "other-job",
		Mode:              ExportModeBatch,
		CompletedTimeFrom: time.Now().UTC().Add(-time.Hour),
		CompletedTimeTo:   time.Now().UTC().Add(-time.Minute),
	})
	require.ErrorIs(t, err, ErrValidation)
	assert.Contains(t, err.Error(), "does not match export job client")
}

func TestCreateJobPropagatesValidationBeforeScheduling(t *testing.T) {
	hub := newFakeBackend()
	client := newTestClient(t, hub, ClientOptions{ContainerName: "container"})
	_, err := client.CreateJob(context.Background(), JobCreationOptions{Mode: ExportModeBatch})
	require.ErrorIs(t, err, ErrValidation)
	assert.Empty(t, hub.scheduled)
}

func TestCreateJobReportsOrchestrationFailure(t *testing.T) {
	hub := newFakeBackend()
	hub.completion = &api.OrchestrationMetadata{
		RuntimeStatus: api.RUNTIME_STATUS_FAILED,
		FailureDetails: &api.FailureDetails{
			ErrorType:  invalidTransitionErrorType,
			Properties: map[string]any{"from": float64(1), "to": float64(1), "operation": createOperation},
		},
	}
	client := newTestClient(t, hub, ClientOptions{ContainerName: "container"})
	_, err := client.CreateJob(context.Background(), JobCreationOptions{
		Mode:              ExportModeBatch,
		CompletedTimeFrom: time.Now().UTC().Add(-time.Hour),
		CompletedTimeTo:   time.Now().UTC().Add(-time.Minute),
	})
	require.ErrorIs(t, err, ErrJobInvalidTransition)

	hub.completion = &api.OrchestrationMetadata{
		RuntimeStatus:  api.RUNTIME_STATUS_FAILED,
		FailureDetails: &api.FailureDetails{ErrorType: "Contoso.Boom", ErrorMessage: "boom"},
	}
	_, err = client.CreateJob(context.Background(), JobCreationOptions{
		Mode:              ExportModeBatch,
		CompletedTimeFrom: time.Now().UTC().Add(-time.Hour),
		CompletedTimeTo:   time.Now().UTC().Add(-time.Minute),
	})
	require.ErrorIs(t, err, ErrJobOperationFailed)
	assert.Contains(t, err.Error(), "boom")
}

func TestCreateJobPropagatesTransportErrors(t *testing.T) {
	scheduleFailure := errors.New("schedule failed")
	hub := newFakeBackend()
	hub.scheduleErr = scheduleFailure
	client := newTestClient(t, hub, ClientOptions{ContainerName: "container"})
	_, err := client.CreateJob(context.Background(), JobCreationOptions{Mode: ExportModeContinuous})
	require.ErrorIs(t, err, scheduleFailure)

	waitFailure := errors.New("wait failed")
	hub = newFakeBackend()
	hub.completionErr = waitFailure
	client = newTestClient(t, hub, ClientOptions{ContainerName: "container"})
	_, err = client.CreateJob(context.Background(), JobCreationOptions{Mode: ExportModeContinuous})
	require.ErrorIs(t, err, waitFailure)
}

func TestGetJob(t *testing.T) {
	t.Run("returns the description when the job exists", func(t *testing.T) {
		now := time.Now().UTC().Truncate(time.Millisecond)
		hub := newFakeBackend()
		hub.entity = entityMetadata(t, "job-1", ExportJobState{
			Status:            ExportJobStatusActive,
			CreatedAt:         &now,
			LastModifiedAt:    &now,
			ScannedInstances:  3,
			ExportedInstances: 2,
			Config: &ExportJobConfiguration{
				Mode:        ExportModeBatch,
				Destination: ExportDestination{Container: "container"},
				Format:      DefaultExportFormat(),
			},
		})
		client := newTestClient(t, hub, ClientOptions{ContainerName: "container"})
		description, err := client.GetJob(context.Background(), "job-1")
		require.NoError(t, err)
		assert.Equal(t, "job-1", description.JobID)
		assert.Equal(t, ExportJobStatusActive, description.Status)
		assert.Equal(t, int64(3), description.ScannedInstances)
		assert.Equal(t, int64(2), description.ExportedInstances)
		require.NotNil(t, description.Config)
		assert.Equal(t, ExportModeBatch, description.Config.Mode)
	})

	t.Run("reports a typed not-found error", func(t *testing.T) {
		hub := newFakeBackend()
		client := newTestClient(t, hub, ClientOptions{ContainerName: "container"})
		_, err := client.GetJob(context.Background(), "job-1")
		require.ErrorIs(t, err, ErrJobNotFound)
		var notFound *NotFoundError
		require.ErrorAs(t, err, &notFound)
		assert.Equal(t, "job-1", notFound.JobID)
	})

	t.Run("maps a missing instance to not found", func(t *testing.T) {
		hub := newFakeBackend()
		client := newTestClient(t, hub, ClientOptions{ContainerName: "container"})
		_, err := client.GetJob(context.Background(), "job-1")
		require.ErrorIs(t, err, ErrJobNotFound)
	})

	t.Run("a deleted entity with empty state is not found", func(t *testing.T) {
		hub := newFakeBackend()
		hub.entity = &api.EntityMetadata{
			InstanceID:    EntityID("job-1"),
			StateIncluded: true,
		}
		client := newTestClient(t, hub, ClientOptions{ContainerName: "container"})
		_, err := client.GetJob(context.Background(), "job-1")
		require.ErrorIs(t, err, ErrJobNotFound)
	})

	t.Run("corrupt state surfaces a deserialization error", func(t *testing.T) {
		hub := newFakeBackend()
		hub.entity = &api.EntityMetadata{
			InstanceID:      EntityID("job-1"),
			StateIncluded:   true,
			HasState:        true,
			SerializedState: "not json",
		}
		client := newTestClient(t, hub, ClientOptions{ContainerName: "container"})
		_, err := client.GetJob(context.Background(), "job-1")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "failed to deserialize export job state")
	})

	t.Run("propagates transport errors", func(t *testing.T) {
		failure := errors.New("fetch failed")
		hub := newFakeBackend()
		hub.entityErr = failure
		client := newTestClient(t, hub, ClientOptions{ContainerName: "container"})
		_, err := client.GetJob(context.Background(), "job-1")
		require.ErrorIs(t, err, failure)
	})
}

func TestListJobs(t *testing.T) {
	older := time.Now().UTC().Add(-24 * time.Hour)
	newer := time.Now().UTC()
	pages := func(t *testing.T) *api.EntityQueryResults {
		t.Helper()
		return &api.EntityQueryResults{
			Entities: []*api.EntityMetadata{
				entityMetadata(t, "job-1", ExportJobState{Status: ExportJobStatusActive, CreatedAt: &older}),
				entityMetadata(t, "job-2", ExportJobState{Status: ExportJobStatusCompleted, CreatedAt: &newer}),
				{InstanceID: EntityID("job-3")},
			},
			ContinuationToken: "next",
		}
	}

	t.Run("returns every job when unfiltered", func(t *testing.T) {
		hub := newFakeBackend()
		hub.queryPages = pages(t)
		client := newTestClient(t, hub, ClientOptions{ContainerName: "container"})
		result, err := client.ListJobs(context.Background(), ExportJobQuery{})
		require.NoError(t, err)
		require.Len(t, result.Jobs, 2)
		assert.Equal(t, "job-1", result.Jobs[0].JobID)
		assert.Equal(t, "job-2", result.Jobs[1].JobID)
		assert.Equal(t, "next", result.ContinuationToken)
		assert.Equal(t, "@exportjob@", hub.lastQuery.InstanceIDStartsWith)
		assert.False(t, hub.lastQuery.ExcludeState)
		assert.Equal(t, int32(DefaultJobQueryPageSize), hub.lastQuery.PageSize)
	})

	t.Run("filters by status", func(t *testing.T) {
		hub := newFakeBackend()
		hub.queryPages = pages(t)
		client := newTestClient(t, hub, ClientOptions{ContainerName: "container"})
		active := ExportJobStatusActive
		result, err := client.ListJobs(context.Background(), ExportJobQuery{Status: &active})
		require.NoError(t, err)
		require.Len(t, result.Jobs, 1)
		assert.Equal(t, "job-1", result.Jobs[0].JobID)
	})

	t.Run("rejects an invalid status", func(t *testing.T) {
		hub := newFakeBackend()
		client := newTestClient(t, hub, ClientOptions{ContainerName: "container"})
		invalid := ExportJobStatus(99)
		_, err := client.ListJobs(context.Background(), ExportJobQuery{Status: &invalid})
		require.ErrorIs(t, err, ErrValidation)
		assert.Contains(t, err.Error(), "invalid export job status")
		assert.Zero(t, hub.lastQuery)
	})

	t.Run("filters by creation time", func(t *testing.T) {
		hub := newFakeBackend()
		hub.queryPages = pages(t)
		client := newTestClient(t, hub, ClientOptions{ContainerName: "container"})
		result, err := client.ListJobs(context.Background(), ExportJobQuery{
			CreatedFrom: older.Add(time.Hour),
		})
		require.NoError(t, err)
		require.Len(t, result.Jobs, 1)
		assert.Equal(t, "job-2", result.Jobs[0].JobID)
	})

	t.Run("applies the job ID prefix to the entity query", func(t *testing.T) {
		hub := newFakeBackend()
		hub.queryPages = &api.EntityQueryResults{}
		client := newTestClient(t, hub, ClientOptions{ContainerName: "container"})
		_, err := client.ListJobs(context.Background(), ExportJobQuery{
			JobIDPrefix:       "nightly-",
			PageSize:          25,
			ContinuationToken: "token",
		})
		require.NoError(t, err)
		assert.Equal(t, "@exportjob@nightly-", hub.lastQuery.InstanceIDStartsWith)
		assert.Equal(t, int32(25), hub.lastQuery.PageSize)
		assert.Equal(t, "token", hub.lastQuery.ContinuationToken)
	})

	t.Run("validates the page size and prefix", func(t *testing.T) {
		client := newTestClient(t, newFakeBackend(), ClientOptions{ContainerName: "container"})
		_, err := client.ListJobs(context.Background(), ExportJobQuery{PageSize: -1})
		require.ErrorIs(t, err, ErrValidation)
		_, err = client.ListJobs(context.Background(), ExportJobQuery{PageSize: api.MaxInstanceQueryPageSize + 1})
		require.ErrorIs(t, err, ErrValidation)
		_, err = client.ListJobs(context.Background(), ExportJobQuery{JobIDPrefix: "bad@prefix"})
		require.ErrorIs(t, err, ErrValidation)
	})

	t.Run("surfaces query errors", func(t *testing.T) {
		failure := errors.New("query failed")
		hub := newFakeBackend()
		hub.queryErr = failure
		client := newTestClient(t, hub, ClientOptions{ContainerName: "container"})
		_, err := client.ListJobs(context.Background(), ExportJobQuery{})
		require.ErrorIs(t, err, failure)

		hub = newFakeBackend()
		client = newTestClient(t, hub, ClientOptions{ContainerName: "container"})
		_, err = client.ListJobs(context.Background(), ExportJobQuery{})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "returned no result")
	})

	t.Run("rejects a nil client", func(t *testing.T) {
		var client *Client
		_, err := client.ListJobs(context.Background(), ExportJobQuery{})
		require.ErrorIs(t, err, ErrValidation)
		_, err = client.CreateJob(context.Background(), JobCreationOptions{})
		require.ErrorIs(t, err, ErrValidation)
	})
}

func TestDeleteJob(t *testing.T) {
	instanceID := api.InstanceID("ExportJob-job-1-run-removed")
	newBackend := func() *fakeBackend {
		hub := newFakeBackend()
		payload, err := json.Marshal(string(instanceID))
		require.NoError(t, err)
		hub.completion.SerializedOutput = string(payload)
		return hub
	}

	t.Run("deletes the entity then terminates and purges the orchestration", func(t *testing.T) {
		hub := newBackend()
		client := newTestClient(t, hub, ClientOptions{ContainerName: "container"})
		job, err := client.JobClient("job-1")
		require.NoError(t, err)
		require.NoError(t, job.Delete(context.Background()))

		require.Len(t, hub.scheduled, 1)
		assert.Equal(t, deleteOperation, hub.scheduled[0].request.OperationName)
		assert.Equal(t, []api.InstanceID{instanceID}, hub.terminated)
		assert.Equal(t, []api.InstanceID{instanceID}, hub.purged)
		assert.Equal(t, []api.InstanceID{"test-instance", instanceID}, hub.waited)
		assert.Empty(t, hub.entityReads)
	})

	t.Run("an absent job returns an empty string and needs no cleanup", func(t *testing.T) {
		hub := newBackend()
		hub.completion.SerializedOutput = `""`
		job, err := newTestClient(t, hub, ClientOptions{}).JobClient("job-1")
		require.NoError(t, err)
		require.NoError(t, job.Delete(context.Background()))
		assert.Empty(t, hub.terminated)
		assert.Empty(t, hub.purged)
		assert.Empty(t, hub.entityReads)
		assert.Equal(t, []api.InstanceID{"test-instance"}, hub.waited)
	})

	t.Run("tolerates a missing orchestration", func(t *testing.T) {
		for _, name := range []string{"terminate", "wait", "purge"} {
			hub := newBackend()
			switch name {
			case "terminate":
				hub.terminateErr = api.ErrInstanceNotFound
			case "wait":
				hub.completionErrByInstance = map[api.InstanceID]error{
					instanceID: api.ErrInstanceNotFound,
				}
			case "purge":
				hub.purgeErr = api.ErrInstanceNotFound
			}
			client := newTestClient(t, hub, ClientOptions{ContainerName: "container"})
			job, err := client.JobClient("job-1")
			require.NoError(t, err)
			require.NoError(t, job.Delete(context.Background()), name)
		}
	})

	t.Run("reports other cleanup failures", func(t *testing.T) {
		for _, step := range []string{"terminate", "wait", "purge"} {
			t.Run(step, func(t *testing.T) {
				failure := errors.New(step + " failed")
				hub := newBackend()
				switch step {
				case "terminate":
					hub.terminateErr = failure
				case "wait":
					hub.completionErrByInstance = map[api.InstanceID]error{instanceID: failure}
				case "purge":
					hub.purgeErr = failure
				}
				job, err := newTestClient(t, hub, ClientOptions{}).JobClient("job-1")
				require.NoError(t, err)
				require.ErrorIs(t, job.Delete(context.Background()), failure)
				assert.Empty(t, hub.entityReads)
				if step != "purge" {
					assert.Empty(t, hub.purged)
				}
			})
		}
	})

	t.Run("does not clean up when the entity delete fails", func(t *testing.T) {
		hub := newFakeBackend()
		hub.completion = &api.OrchestrationMetadata{RuntimeStatus: api.RUNTIME_STATUS_FAILED}
		client := newTestClient(t, hub, ClientOptions{ContainerName: "container"})
		job, err := client.JobClient("job-1")
		require.NoError(t, err)
		require.Error(t, job.Delete(context.Background()))
		assert.Empty(t, hub.terminated)
		assert.Empty(t, hub.purged)
	})

	t.Run("an unreadable delete result never triggers cleanup or Describe", func(t *testing.T) {
		for _, output := range []string{"", "not-json", `{"InstanceID":"wrong-shape"}`, `123`} {
			hub := newBackend()
			hub.completion.SerializedOutput = output
			hub.entity = entityMetadata(t, "job-1", ExportJobState{
				Status: ExportJobStatusActive, OrchestratorInstanceID: "replacement-run",
			})
			job, err := newTestClient(t, hub, ClientOptions{}).JobClient("job-1")
			require.NoError(t, err)
			err = job.Delete(context.Background())
			require.Error(t, err, "output %q", output)
			if output == "" {
				assert.Contains(t, err.Error(), "Delete returned no output")
			} else {
				assert.Contains(t, err.Error(), "failed to decode Delete result")
			}
			assert.Empty(t, hub.terminated)
			assert.Empty(t, hub.purged)
			assert.Empty(t, hub.entityReads)
		}
	})

	t.Run("missing operation metadata reports an error without cleanup", func(t *testing.T) {
		hub := newBackend()
		hub.completion = nil
		job, err := newTestClient(t, hub, ClientOptions{}).JobClient("job-1")
		require.NoError(t, err)
		require.ErrorIs(t, job.Delete(context.Background()), ErrJobOperationFailed)
		assert.Empty(t, hub.terminated)
		assert.Empty(t, hub.purged)
	})
}

func TestCreateDoesNotCleanUpPreviousRuns(t *testing.T) {
	options := JobCreationOptions{
		Mode:              ExportModeBatch,
		CompletedTimeFrom: time.Now().UTC().Add(-time.Hour),
		CompletedTimeTo:   time.Now().UTC().Add(-time.Minute),
	}
	for _, status := range []ExportJobStatus{ExportJobStatusCompleted, ExportJobStatusFailed, ExportJobStatusActive} {
		t.Run(status.String(), func(t *testing.T) {
			hub := newFakeBackend()
			hub.entity = entityMetadata(t, "job-1", ExportJobState{
				Status:                 status,
				OrchestratorInstanceID: "previous-run",
			})
			client := newTestClient(t, hub, ClientOptions{ContainerName: "container"})
			job, err := client.JobClient("job-1")
			require.NoError(t, err)
			require.NoError(t, job.Create(context.Background(), options))

			assert.Empty(t, hub.terminated)
			assert.Empty(t, hub.purged)
			assert.Empty(t, hub.entityReads)
			require.Len(t, hub.scheduled, 1)
			assert.Equal(t, createOperation, hub.scheduled[0].request.OperationName)
		})
	}

	t.Run("a job that does not exist needs no cleanup", func(t *testing.T) {
		hub := newFakeBackend()
		client := newTestClient(t, hub, ClientOptions{ContainerName: "container"})
		job, err := client.JobClient("job-1")
		require.NoError(t, err)
		require.NoError(t, job.Create(context.Background(), options))
		assert.Empty(t, hub.terminated)
		assert.Empty(t, hub.purged)
		require.Len(t, hub.scheduled, 1)
	})

	t.Run("creation does not depend on management reads or cleanup", func(t *testing.T) {
		hub := newFakeBackend()
		hub.entityErr = errors.New("read failed")
		hub.terminateErr = errors.New("terminate failed")
		hub.purgeErr = errors.New("purge failed")
		client := newTestClient(t, hub, ClientOptions{ContainerName: "container"})
		job, err := client.JobClient("job-1")
		require.NoError(t, err)
		require.NoError(t, job.Create(context.Background(), options))
		assert.Empty(t, hub.terminated)
		assert.Empty(t, hub.purged)
		assert.Empty(t, hub.entityReads)
		require.Len(t, hub.scheduled, 1)
	})
}

func TestDescribeUsesTheClientJobID(t *testing.T) {
	hub := newFakeBackend()
	// The service reports the lowercased entity key; the handle's own ID wins so
	// a caller always sees the ID it asked for.
	hub.entity = entityMetadata(t, "JOB-1", ExportJobState{Status: ExportJobStatusActive})
	client := newTestClient(t, hub, ClientOptions{ContainerName: "container"})
	job, err := client.JobClient("job-1")
	require.NoError(t, err)
	description, err := job.Describe(context.Background())
	require.NoError(t, err)
	assert.Equal(t, "job-1", description.JobID)
}

func TestDefaultPrefix(t *testing.T) {
	assert.Equal(t, "batch-job/", defaultPrefix(ExportModeBatch, "job"))
	assert.Equal(t, "continuous-job/", defaultPrefix(ExportModeContinuous, "job"))
	assert.True(t, strings.HasSuffix(defaultPrefix(ExportModeBatch, "job"), "/"))
}
