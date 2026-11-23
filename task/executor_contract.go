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

// EntityExecutor processes normalized operation batches from V2 entity work
// items. EntityBatchRequest is an internal execution model, not a supported
// legacy work-item envelope.
type EntityExecutor interface {
	ExecuteEntity(context.Context, *protos.EntityBatchRequest) (*protos.EntityBatchResult, error)
}

// SessionExecutor is an optional extension of [Executor] for extended
// sessions. An extended session keeps an orchestration loaded in memory
// between turns, so the next turn only needs the new events instead of a
// replay of the full history. Like [Executor], it is an internal contract
// between the task runtime and the gRPC worker.
type SessionExecutor interface {
	// StartOrchestratorSession runs one turn exactly like
	// [Executor.ExecuteOrchestrator]. It also returns a session when the
	// orchestration can continue in memory. A nil session means that the
	// caller must complete the turn without a session.
	StartOrchestratorSession(
		ctx context.Context,
		iid api.InstanceID,
		oldEvents []*protos.HistoryEvent,
		newEvents []*protos.HistoryEvent,
		entityParameters *protos.OrchestratorEntityParameters,
	) (*ExecutionResults, OrchestratorSession, error)
}

// OrchestratorSession is an orchestration that stays loaded between turns.
// A session has one owner and is not safe for concurrent use.
//
// After each turn that returns a session, the owner must call Commit once
// the backend has accepted the turn's actions, and then either Resume or
// Close.
type OrchestratorSession interface {
	// Commit marks the actions that the backend has durably applied. They
	// are not sent again, in the same way that replayed history retires
	// them. Pass exactly the actions that were sent for the last turn.
	Commit(sent []*protos.OrchestratorAction)
	// Resume runs the next turn with a batch of new events. The batch must
	// start with an OrchestratorStarted event. historyBase is the number of
	// history events that the backend has stored for the current execution.
	// The returned session is nil when the orchestration can no longer stay
	// loaded; the session is then already closed. On error the session is
	// also closed and the caller must fall back to a full replay.
	Resume(
		ctx context.Context,
		batch []*protos.HistoryEvent,
		historyBase int,
	) (*ExecutionResults, OrchestratorSession, error)
	// Close unloads the orchestration. Deferred orchestrator code that runs
	// during the unload cannot schedule durable work. Close is safe to call
	// more than once.
	Close()
}
