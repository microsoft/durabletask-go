package task

import (
	"context"

	"github.com/microsoft/durabletask-go/api"
	"github.com/microsoft/durabletask-go/internal/protos"
)

// ExecutionResults carries the orchestrator actions produced by a single
// orchestrator execution turn. It is transport-neutral: the DTS worker sends
// [ExecutionResults.Response] back to the scheduler over gRPC.
type ExecutionResults struct {
	Response *protos.OrchestratorResponse
}

// Executor is the internal collaboration contract between the task runtime and
// the gRPC worker. It is exported only so those packages can share it; its
// internal protobuf parameters intentionally prevent application implementations.
// [NewTaskExecutor] is the implementation used by DTS workers.
type Executor interface {
	ExecuteOrchestrator(
		ctx context.Context,
		iid api.InstanceID,
		oldEvents []*protos.HistoryEvent,
		newEvents []*protos.HistoryEvent,
		entityParameters *protos.OrchestratorEntityParameters,
	) (*ExecutionResults, error)
	ExecuteActivity(context.Context, api.InstanceID, *protos.HistoryEvent) (*protos.HistoryEvent, error)
	Shutdown(ctx context.Context) error
}

// EntityExecutor is implemented by executors that process durable entity
// operation batches.
type EntityExecutor interface {
	ExecuteEntity(context.Context, *protos.EntityBatchRequest) (*protos.EntityBatchResult, error)
}
