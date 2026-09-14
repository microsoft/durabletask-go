package exporthistory

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/gob"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/microsoft/durabletask-go/api"
	"github.com/microsoft/durabletask-go/internal/failure"
	"github.com/microsoft/durabletask-go/internal/protos"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

// lifecycleBackend executes client operations through both production executors.
// Hooks control the interleaving without a scheduler, network, or sleeps.
type lifecycleBackend struct {
	*fakeBackend
	entity         *entityHarness
	completions    map[api.InstanceID]*api.OrchestrationMetadata
	beforeCreate   func()
	afterOperation func(ExportJobOperationRequest, *api.OrchestrationMetadata)
}

func newLifecycleBackend(entity *entityHarness) *lifecycleBackend {
	return &lifecycleBackend{
		fakeBackend: newFakeBackend(),
		entity:      entity,
		completions: map[api.InstanceID]*api.OrchestrationMetadata{},
	}
}

func (b *lifecycleBackend) ScheduleNewOrchestration(
	_ context.Context,
	name string,
	options ...api.NewOrchestrationOptions,
) (api.InstanceID, error) {
	request := &protos.CreateInstanceRequest{}
	for _, option := range options {
		if err := option(request, b.entity.converter); err != nil {
			return "", err
		}
	}
	var operation ExportJobOperationRequest
	if err := b.entity.converter.Deserialize(request.GetInput().GetValue(), &operation); err != nil {
		return "", err
	}
	if strings.EqualFold(operation.OperationName, createOperation) && b.beforeCreate != nil {
		b.beforeCreate()
	}

	id := api.InstanceID(newCompactUUID())
	b.scheduled = append(b.scheduled, scheduledCall{name: name, request: operation, version: request.GetVersion().GetValue()})
	driver := newOrchestrationDriver(b.entity.t, newExportRegistry(b.entity.t), name, id, operation, b.entity.converter)
	driver.turn()
	called := driver.pendingEntityCall(operation.OperationName)
	output, err := b.entity.tryCallRaw(operation.OperationName, called.GetInput())
	if err != nil {
		var entityErr entityFailure
		require.ErrorAs(b.entity.t, err, &entityErr)
		driver.failEntityCallWithDetails(operation.OperationName, entityErr.details)
	} else {
		var rawOutput *wrapperspb.StringValue
		if output != "" {
			rawOutput = wrapperspb.String(output)
		}
		driver.completeEntityCallOutput(operation.OperationName, rawOutput)
	}
	driver.nextTurn()
	driver.turn()
	require.NotNil(b.entity.t, driver.completion)
	completion := driver.completion
	metadata := &api.OrchestrationMetadata{
		InstanceID:       id,
		RuntimeStatus:    completion.GetOrchestrationStatus(),
		SerializedOutput: completion.GetResult().GetValue(),
		FailureDetails:   failure.FromProto(completion.GetFailureDetails()),
		Converter:        b.entity.converter,
	}
	b.completions[id] = metadata
	if b.afterOperation != nil {
		b.afterOperation(operation, metadata)
	}
	return id, nil
}

func (b *lifecycleBackend) WaitForOrchestrationCompletion(
	ctx context.Context,
	id api.InstanceID,
	options ...api.FetchOrchestrationMetadataOptions,
) (*api.OrchestrationMetadata, error) {
	metadata, err := b.fakeBackend.WaitForOrchestrationCompletion(ctx, id, options...)
	if operation, ok := b.completions[id]; ok {
		return operation, nil
	}
	return metadata, err
}

func (b *lifecycleBackend) GetEntity(
	ctx context.Context,
	id api.EntityID,
	options ...api.GetEntityOptions,
) (*api.EntityMetadata, error) {
	_, err := b.fakeBackend.GetEntity(ctx, id, options...)
	if err != nil {
		return nil, err
	}
	var snapshot *api.EntityMetadata
	if state := b.entity.jobState(); state != nil {
		snapshot = entityMetadata(b.entity.t, b.entity.jobID, *state)
	}
	// Pause after taking the snapshot if a pre-Create read is reintroduced.
	// Otherwise ScheduleNewOrchestration pauses before submitting Create.
	if b.beforeCreate != nil {
		b.beforeCreate()
	}
	return snapshot, nil
}

func TestLifecycleConcurrentCreateDoesNotCleanUpTheWinner(t *testing.T) {
	for _, terminal := range []string{markAsCompletedOperation, markAsFailedOperation} {
		t.Run(terminal, func(t *testing.T) {
			entity := newEntityHarness(t, "job-1")
			entity.call(createOperation, batchOptions("job-1"))
			entity.call(runOperation, RunJobRequest{RunToken: entity.runToken()})
			previousID := entity.jobState().OrchestratorInstanceID
			entity.call(terminal, MarkAsFailedRequest{RunToken: entity.runToken()})

			hubA, hubB := newLifecycleBackend(entity), newLifecycleBackend(entity)
			jobA, err := newTestClient(t, hubA, ClientOptions{ContainerName: "container"}).JobClient("job-1")
			require.NoError(t, err)
			jobB, err := newTestClient(t, hubB, ClientOptions{ContainerName: "container"}).JobClient("job-1")
			require.NoError(t, err)

			paused, resume := make(chan struct{}), make(chan struct{})
			var pauseOnce, resumeOnce sync.Once
			hubB.beforeCreate = func() {
				pauseOnce.Do(func() {
					close(paused)
					<-resume
				})
			}
			defer resumeOnce.Do(func() { close(resume) })
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			done := make(chan error, 1)
			go func() {
				defer close(done)
				done <- jobB.Create(ctx, batchOptions("job-1"))
			}()
			select {
			case <-paused:
			case <-ctx.Done():
				t.Fatal("second Create did not reach its interleaving barrier")
			}

			require.NoError(t, jobA.Create(ctx, batchOptions("job-1")))
			entity.call(runOperation, RunJobRequest{RunToken: entity.runToken()})
			current := *entity.jobState()
			currentID := api.InstanceID(current.OrchestratorInstanceID)
			require.NotEmpty(t, currentID)
			resumeOnce.Do(func() { close(resume) })
			select {
			case err := <-done:
				require.ErrorIs(t, err, ErrJobInvalidTransition)
			case <-ctx.Done():
				t.Fatal("second Create did not finish")
			}

			assert.Equal(t, current, *entity.jobState())
			assert.NotEqual(t, previousID, string(currentID))
			assert.NotContains(t, hubB.terminated, currentID, "the losing Create must not terminate the winner")
			assert.NotContains(t, hubB.purged, currentID, "the losing Create must not purge the winner")
			assert.Empty(t, hubA.terminated)
			assert.Empty(t, hubA.purged)
			assert.Empty(t, hubB.terminated)
			assert.Empty(t, hubB.purged)
		})
	}
}

func TestLifecycleDeleteCleanupUsesOnlyTheCapturedGeneration(t *testing.T) {
	entity := newEntityHarness(t, "job-1")
	entity.call(createOperation, batchOptions("job-1"))
	entity.call(runOperation, RunJobRequest{RunToken: entity.runToken()})
	removedID := api.InstanceID(entity.jobState().OrchestratorInstanceID)

	deletingHub, creatingHub := newLifecycleBackend(entity), newLifecycleBackend(entity)
	deletingJob, err := newTestClient(t, deletingHub, ClientOptions{}).JobClient("job-1")
	require.NoError(t, err)
	creatingJob, err := newTestClient(t, creatingHub, ClientOptions{ContainerName: "container"}).JobClient("job-1")
	require.NoError(t, err)
	var replacement ExportJobState
	deletingHub.afterOperation = func(request ExportJobOperationRequest, metadata *api.OrchestrationMetadata) {
		require.Equal(t, deleteOperation, request.OperationName)
		require.False(t, entity.hasState())
		assert.Empty(t, deletingHub.terminated, "the entity completes before management cleanup begins")
		var capturedID string
		require.NoError(t, metadata.ReadOutput(&capturedID))
		assert.Equal(t, string(removedID), capturedID)

		require.NoError(t, creatingJob.Create(context.Background(), batchOptions("job-1")))
		entity.call(runOperation, RunJobRequest{RunToken: entity.runToken()})
		replacement = *entity.jobState()
	}
	require.NoError(t, deletingJob.Delete(context.Background()))
	assert.Equal(t, replacement, *entity.jobState())
	assert.NotEqual(t, string(removedID), replacement.OrchestratorInstanceID)
	assert.Equal(t, []api.InstanceID{removedID}, deletingHub.terminated)
	assert.Equal(t, []api.InstanceID{removedID}, deletingHub.purged)
	assert.Empty(t, deletingHub.entityReads, "Delete must not Describe the replacement")

	// Retrying cleanup of the captured ID after it is gone is harmless. A new
	// Delete operation, in contrast, would legitimately remove the replacement.
	deletingHub.terminateErr = api.ErrInstanceNotFound
	require.NoError(t, deletingJob.terminateAndPurgeOrchestration(context.Background(), removedID))
	assert.Equal(t, replacement, *entity.jobState())
	assert.Equal(t, []api.InstanceID{removedID, removedID}, deletingHub.terminated)
	assert.Equal(t, []api.InstanceID{removedID}, deletingHub.purged)
}

// Gob is non-JSON and type-sensitive: a concrete string cannot be decoded into
// *any. A prefixed JSON converter would not catch an untyped Await target.
type lifecycleGobConverter struct{}

func (lifecycleGobConverter) Serialize(value any) (string, error) {
	var buffer bytes.Buffer
	if err := gob.NewEncoder(&buffer).Encode(value); err != nil {
		return "", err
	}
	return base64.RawStdEncoding.EncodeToString(buffer.Bytes()), nil
}

func (lifecycleGobConverter) Deserialize(payload string, target any) error {
	data, err := base64.RawStdEncoding.DecodeString(payload)
	if err != nil {
		return err
	}
	return gob.NewDecoder(bytes.NewReader(data)).Decode(target)
}

func TestLifecycleDeleteResultRoundTripWithTypeSensitiveConverter(t *testing.T) {
	converter := lifecycleGobConverter{}
	encoded, err := converter.Serialize("a-run-id")
	require.NoError(t, err)
	var untyped any
	require.Error(t, converter.Deserialize(encoded, &untyped), "this converter must reject *any string results")

	entity := newEntityHarness(t, "job-1", converter)
	entity.call(createOperation, batchOptions("job-1"))
	entity.call(runOperation, RunJobRequest{RunToken: entity.runToken()})
	removedID := api.InstanceID(entity.jobState().OrchestratorInstanceID)
	require.NotEmpty(t, removedID)
	hub := newLifecycleBackend(entity)
	job, err := newTestClient(t, hub, ClientOptions{}).JobClient("job-1")
	require.NoError(t, err)
	require.NoError(t, job.Delete(context.Background()))
	assert.False(t, entity.hasState())
	assert.Equal(t, []api.InstanceID{removedID}, hub.terminated)
	assert.Equal(t, []api.InstanceID{removedID}, hub.purged)

	require.NoError(t, job.Delete(context.Background()), "an absent job still returns a typed empty string")
	assert.False(t, entity.hasState())
	assert.Equal(t, []api.InstanceID{removedID}, hub.terminated)
	assert.Equal(t, []api.InstanceID{removedID}, hub.purged)
	assert.Empty(t, hub.entityReads)
}

func TestLifecycleDeleteBeforeRunCapturesTheReservedID(t *testing.T) {
	entity := newEntityHarness(t, "job-1")
	entity.call(createOperation, batchOptions("job-1"))
	removedID := api.InstanceID(entity.jobState().OrchestratorInstanceID)
	require.NotEmpty(t, removedID, "Create must reserve the ID before Run is delivered")
	delayedRun := entity.actions[0].GetSendSignal().GetInput()
	hub := newLifecycleBackend(entity)
	hub.terminateErr = api.ErrInstanceNotFound
	job, err := newTestClient(t, hub, ClientOptions{}).JobClient("job-1")
	require.NoError(t, err)

	require.NoError(t, job.Delete(context.Background()))
	assert.Equal(t, []api.InstanceID{removedID}, hub.terminated)
	assert.Empty(t, hub.purged)
	_, err = entity.tryCallRaw(runOperation, delayedRun)
	require.NoError(t, err)
	assert.False(t, entity.hasState())
	assert.Empty(t, entity.actions)

	require.NoError(t, job.Delete(context.Background()))
	assert.Equal(t, []api.InstanceID{removedID}, hub.terminated)
}

func TestLifecycleDelayedStartCannotExportTheReplacement(t *testing.T) {
	entity := newEntityHarness(t, "job-1")
	entity.call(createOperation, batchOptions("job-1"))
	entity.call(runOperation, RunJobRequest{RunToken: entity.runToken()})
	delayedStart := entity.actions[0].GetStartNewOrchestration()
	require.NotNil(t, delayedStart)
	var delayedInput ExportJobRunRequest
	require.NoError(t, entity.converter.Deserialize(delayedStart.GetInput().GetValue(), &delayedInput))

	hub := newLifecycleBackend(entity)
	hub.terminateErr = api.ErrInstanceNotFound // the emitted Start has not arrived
	job, err := newTestClient(t, hub, ClientOptions{ContainerName: "container"}).JobClient("job-1")
	require.NoError(t, err)
	require.NoError(t, job.Delete(context.Background()))
	require.NoError(t, job.Create(context.Background(), batchOptions("job-1")))
	entity.call(runOperation, RunJobRequest{RunToken: entity.runToken()})
	replacement := *entity.jobState()
	assert.NotEqual(t, delayedStart.GetInstanceId(), replacement.OrchestratorInstanceID)

	driver := newOrchestrationDriver(t, newExportRegistry(t), delayedStart.GetName(),
		api.InstanceID(delayedStart.GetInstanceId()), delayedInput)
	driver.turn()
	output, err := entity.tryCallRaw(getOperation, driver.pendingEntityCall(getOperation).GetInput())
	require.NoError(t, err)
	driver.completeEntityCallOutput(getOperation, wrapperspb.String(output))
	driver.nextTurn()
	driver.turn()
	require.NotNil(t, driver.completion)
	assert.Equal(t, api.RUNTIME_STATUS_COMPLETED, driver.completion.GetOrchestrationStatus())
	assert.Empty(t, driver.pendingTasks, "a stale Start must never list or export the replacement's instances")
	assert.Empty(t, driver.pendingEntities, "a stale Start must not mutate the replacement")
	assert.Equal(t, replacement, *entity.jobState())
}

func TestLifecycleDeleteDecodeFailureNeverTargetsTheReplacement(t *testing.T) {
	entity := newEntityHarness(t, "job-1")
	entity.call(createOperation, batchOptions("job-1"))
	hub := newLifecycleBackend(entity)
	job, err := newTestClient(t, hub, ClientOptions{}).JobClient("job-1")
	require.NoError(t, err)
	hub.afterOperation = func(_ ExportJobOperationRequest, metadata *api.OrchestrationMetadata) {
		require.False(t, entity.hasState())
		entity.call(createOperation, batchOptions("job-1"))
		metadata.SerializedOutput = "invalid-output"
	}
	err = job.Delete(context.Background())
	require.Error(t, err)
	assert.False(t, errors.Is(err, ErrJobNotFound))
	assert.Empty(t, hub.terminated)
	assert.Empty(t, hub.purged)
	assert.Empty(t, hub.entityReads)
	assert.Equal(t, ExportJobStatusActive, entity.jobState().Status)
}
