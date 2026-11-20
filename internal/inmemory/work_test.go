package inmemory

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/microsoft/durabletask-go/internal/helpers"
	"github.com/microsoft/durabletask-go/internal/protos"
)

func TestContinueAsNewDropsPreviousExecutionResults(t *testing.T) {
	s := NewServer()
	defer s.Close()
	inst := &instance{id: "id", name: "orchestrator", executionID: "old"}
	raised := helpers.NewEventRaisedEvent("event", nil)
	inst.pending = []*protos.HistoryEvent{
		helpers.NewTaskCompletedEvent(0, nil),
		raised,
		helpers.NewTimerFiredEvent(1, nil, nil),
	}
	carryover := helpers.NewEventRaisedEvent("carryover", nil)

	s.continueAsNewLocked(inst, &protos.CompleteOrchestrationAction{CarryoverEvents: []*protos.HistoryEvent{carryover}})

	require.Len(t, inst.pending, 3)
	assert.NotNil(t, inst.pending[0].GetExecutionStarted())
	assert.NotEqual(t, "old", inst.executionID)
	assert.Same(t, carryover, inst.pending[1])
	assert.Same(t, raised, inst.pending[2])
}
