package task

import (
	"context"
	"errors"
	"fmt"

	"github.com/microsoft/durabletask-go/internal/protos"
)

var _ SessionExecutor = (*taskExecutor)(nil)

type orchestratorSessionState int

const (
	sessionAwaitingCommit orchestratorSessionState = iota
	sessionAwaitingResume
	sessionClosed
)

var errOrchestratorSessionClosed = errors.New("orchestrator session is closed")

// orchestratorSession keeps one OrchestrationContext and its coroutines
// loaded between turns.
type orchestratorSession struct {
	executor *taskExecutor
	ctx      *OrchestrationContext
	state    orchestratorSessionState
	// closeErr explains why the session was closed by a misuse, if it was.
	closeErr error
}

func (s *orchestratorSession) Commit(sent []*protos.OrchestratorAction) {
	if s.state != sessionAwaitingCommit {
		s.closeWith(fmt.Errorf("orchestrator session commit is out of order"))
		return
	}
	s.ctx.commit(sent)
	s.state = sessionAwaitingResume
}

func (s *orchestratorSession) Resume(
	_ context.Context,
	batch []*protos.HistoryEvent,
	historyBase int,
) (*ExecutionResults, OrchestratorSession, error) {
	switch s.state {
	case sessionClosed:
		if s.closeErr != nil {
			return nil, nil, s.closeErr
		}
		return nil, nil, errOrchestratorSessionClosed
	case sessionAwaitingCommit:
		err := fmt.Errorf("orchestrator session resumed before the last turn was committed")
		s.closeWith(err)
		return nil, nil, err
	}
	if err := validateSessionBatch(batch, historyBase); err != nil {
		s.closeWith(err)
		return nil, nil, err
	}
	return s.finishTurn(s.ctx.resume(batch, historyBase))
}

func (s *orchestratorSession) Close() {
	s.closeWith(nil)
}

func (s *orchestratorSession) closeWith(err error) {
	if s.state == sessionClosed {
		return
	}
	s.state = sessionClosed
	s.closeErr = err
	s.ctx.unload()
}

// finishTurn builds the turn result and decides whether the orchestration can
// stay loaded. Only a turn that ends with live, waiting orchestrator code and
// no unprocessed events can continue with new events.
func (s *orchestratorSession) finishTurn(
	actions []*protos.OrchestratorAction,
) (*ExecutionResults, OrchestratorSession, error) {
	results := s.executor.orchestratorResults(s.ctx, actions)
	if !s.ctx.canContinueSession() {
		s.Close()
		return results, nil, nil
	}
	s.state = sessionAwaitingCommit
	return results, s, nil
}

// canContinueSession reports whether the orchestration can stay loaded after
// the current turn.
func (ctx *OrchestrationContext) canContinueSession() bool {
	scheduler := ctx.scheduler
	return scheduler != nil &&
		scheduler.root != nil &&
		!scheduler.isStopping() &&
		!ctx.isTerminated &&
		!ctx.hasCompletionAction() &&
		// The backend sends unprocessed events again in the next work item, so
		// a partial turn must not keep them in memory.
		!ctx.maxEventsPerTurnExceeded &&
		!ctx.maxHistoryEventsExceeded
}

func validateSessionBatch(batch []*protos.HistoryEvent, historyBase int) error {
	if historyBase < 0 {
		return fmt.Errorf("orchestrator session history base must not be negative: %d", historyBase)
	}
	if len(batch) == 0 || batch[0].GetOrchestratorStarted() == nil {
		return fmt.Errorf("orchestrator session batch must start with an OrchestratorStarted event")
	}
	for _, event := range batch {
		switch {
		case event == nil:
			return fmt.Errorf("orchestrator session batch must not contain nil events")
		case event.GetExecutionStarted() != nil:
			return fmt.Errorf("orchestrator session batch must not start a new execution")
		case event.GetExecutionRewound() != nil:
			return fmt.Errorf("orchestrator session batch must not contain a rewind request")
		}
	}
	return nil
}
