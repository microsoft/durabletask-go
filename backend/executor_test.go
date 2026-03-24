package backend

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/microsoft/durabletask-go/api"
	"github.com/microsoft/durabletask-go/internal/helpers"
	"github.com/microsoft/durabletask-go/internal/protos"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type capturingBackend struct {
	created []*HistoryEvent
	added   []*HistoryEvent
	addedTo []api.InstanceID
}

func (*capturingBackend) CreateTaskHub(context.Context) error { return nil }
func (*capturingBackend) DeleteTaskHub(context.Context) error { return nil }
func (*capturingBackend) Start(context.Context) error         { return nil }
func (*capturingBackend) Stop(context.Context) error          { return nil }
func (b *capturingBackend) CreateOrchestrationInstance(_ context.Context, e *HistoryEvent, _ ...OrchestrationIdReusePolicyOptions) error {
	b.created = append(b.created, e)
	return nil
}
func (b *capturingBackend) AddNewOrchestrationEvent(_ context.Context, iid api.InstanceID, e *HistoryEvent) error {
	b.addedTo = append(b.addedTo, iid)
	b.added = append(b.added, e)
	return nil
}
func (*capturingBackend) GetOrchestrationWorkItem(context.Context) (*OrchestrationWorkItem, error) {
	return nil, nil
}
func (*capturingBackend) GetOrchestrationRuntimeState(context.Context, *OrchestrationWorkItem) (*OrchestrationRuntimeState, error) {
	return nil, nil
}
func (*capturingBackend) GetOrchestrationMetadata(context.Context, api.InstanceID) (*api.OrchestrationMetadata, error) {
	return nil, nil
}
func (*capturingBackend) CompleteOrchestrationWorkItem(context.Context, *OrchestrationWorkItem) error {
	return nil
}
func (*capturingBackend) AbandonOrchestrationWorkItem(context.Context, *OrchestrationWorkItem) error {
	return nil
}
func (*capturingBackend) GetActivityWorkItem(context.Context) (*ActivityWorkItem, error) {
	return nil, nil
}
func (*capturingBackend) CompleteActivityWorkItem(context.Context, *ActivityWorkItem) error {
	return nil
}
func (*capturingBackend) AbandonActivityWorkItem(context.Context, *ActivityWorkItem) error {
	return nil
}
func (*capturingBackend) PurgeOrchestrationState(context.Context, api.InstanceID) error { return nil }

func Test_GrpcExecutor_ExecuteEntity_RejectsConcurrentInstance(t *testing.T) {
	executor, _ := NewGrpcExecutor(nil, DefaultLogger())
	g := executor.(*grpcExecutor)

	req := &protos.EntityBatchRequest{InstanceId: "@counter@key"}
	g.pendingEntities.Store(req.InstanceId, &entityExecutionResult{complete: make(chan struct{})})

	_, err := g.ExecuteEntity(context.Background(), req)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "already pending")
}

func Test_GrpcExecutor_StartInstance_RejectsEntityInstanceID(t *testing.T) {
	executor, _ := NewGrpcExecutor(nil, DefaultLogger())
	g := executor.(*grpcExecutor)

	_, err := g.StartInstance(context.Background(), &protos.CreateInstanceRequest{
		Name:       "orchestrator",
		InstanceId: "@counter@key",
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "reserved entity format")
}

func Test_GrpcExecutor_SignalEntity_PreservesScheduledTimeAndRequestID(t *testing.T) {
	be := &capturingBackend{}
	executor, _ := NewGrpcExecutor(be, DefaultLogger())
	g := executor.(*grpcExecutor)

	scheduledTime := time.Now().Add(2 * time.Hour).UTC().Truncate(time.Millisecond)
	_, err := g.SignalEntity(context.Background(), &protos.SignalEntityRequest{
		InstanceId:    "@counter@key",
		Name:          "increment",
		RequestId:     "request-123",
		ScheduledTime: timestamppb.New(scheduledTime),
	})
	require.NoError(t, err)

	require.Len(t, be.created, 1)
	require.Len(t, be.added, 1)
	require.Equal(t, api.InstanceID("@counter@key"), be.addedTo[0])
	require.WithinDuration(t, scheduledTime, be.added[0].Timestamp.AsTime(), time.Millisecond)

	payload := be.added[0].GetEventRaised().GetInput().GetValue()
	var msg helpers.EntityRequestMessage
	require.NoError(t, json.Unmarshal([]byte(payload), &msg))
	require.Equal(t, "request-123", msg.ID)
	require.True(t, msg.IsSignal)
	require.Equal(t, "increment", msg.Operation)
}

func Test_GrpcExecutor_CompleteEntityTask_RemovesMetadataCorrelatedQueueEntry(t *testing.T) {
	executor, _ := NewGrpcExecutor(nil, DefaultLogger())
	g := executor.(*grpcExecutor)

	first := &entityExecutionResult{complete: make(chan struct{})}
	second := &entityExecutionResult{complete: make(chan struct{})}
	g.pendingEntities.Store("@counter@one", first)
	g.pendingEntities.Store("@counter@two", second)
	g.entityQueue.Enqueue("@counter@one")
	g.entityQueue.Enqueue("@counter@two")

	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("entity-instance-id", "@counter@one"))
	_, err := g.CompleteEntityTask(ctx, &protos.EntityBatchResult{})
	require.NoError(t, err)

	next, ok := g.entityQueue.Dequeue()
	require.True(t, ok)
	assert.Equal(t, "@counter@two", next)

	_, ok = g.entityQueue.Dequeue()
	assert.False(t, ok)
}
