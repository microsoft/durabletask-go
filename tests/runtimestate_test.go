package tests

import (
	"errors"
	"testing"
	"time"

	"github.com/microsoft/durabletask-go/api"
	"github.com/microsoft/durabletask-go/backend"
	"github.com/microsoft/durabletask-go/internal/helpers"
	"github.com/microsoft/durabletask-go/internal/protos"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

var defaultChunkingConfig = backend.ChunkingConfiguration{}

// Verifies runtime state created from an ExecutionStarted event
func Test_NewOrchestration(t *testing.T) {
	const iid = "abc"
	const expectedName = "myorchestration"
	createdAt := time.Now().UTC()

	e := &protos.HistoryEvent{
		EventId:   -1,
		Timestamp: timestamppb.New(createdAt),
		EventType: &protos.HistoryEvent_ExecutionStarted{
			ExecutionStarted: &protos.ExecutionStartedEvent{
				OrchestrationInstance: &protos.OrchestrationInstance{InstanceId: iid},
				Name:                  expectedName,
			},
		},
	}

	s := backend.NewOrchestrationRuntimeState(iid, []*protos.HistoryEvent{e})
	assert.Equal(t, api.InstanceID(iid), s.InstanceID())

	actualName, err := s.Name()
	if assert.NoError(t, err) {
		assert.Equal(t, expectedName, actualName)
	}

	actualTime, err := s.CreatedTime()
	if assert.NoError(t, err) {
		assert.WithinDuration(t, createdAt, actualTime, 0)
	}

	_, err = s.CompletedTime()
	if assert.Error(t, err) {
		assert.Equal(t, api.ErrNotCompleted, err)
	}

	assert.Equal(t, protos.OrchestrationStatus_ORCHESTRATION_STATUS_RUNNING, s.RuntimeStatus())

	oldEvents := s.OldEvents()
	if assert.Equal(t, 1, len(oldEvents)) {
		assert.Equal(t, e, oldEvents[0])
	}

	assert.Equal(t, 0, len(s.NewEvents()))
}

func Test_CompletedOrchestration(t *testing.T) {
	const iid = "abc"
	const expectedName = "myorchestration"
	createdAt := time.Now().UTC()
	completedAt := createdAt.Add(10 * time.Second)

	events := []*protos.HistoryEvent{{
		EventId:   -1,
		Timestamp: timestamppb.New(createdAt),
		EventType: &protos.HistoryEvent_ExecutionStarted{
			ExecutionStarted: &protos.ExecutionStartedEvent{
				OrchestrationInstance: &protos.OrchestrationInstance{InstanceId: iid},
				Name:                  expectedName,
			},
		},
	}, {
		EventId:   -1,
		Timestamp: timestamppb.New(completedAt),
		EventType: &protos.HistoryEvent_ExecutionCompleted{
			ExecutionCompleted: &protos.ExecutionCompletedEvent{
				OrchestrationStatus: protos.OrchestrationStatus_ORCHESTRATION_STATUS_COMPLETED,
			},
		},
	}}

	s := backend.NewOrchestrationRuntimeState(iid, events)
	assert.Equal(t, api.InstanceID(iid), s.InstanceID())

	actualName, err := s.Name()
	if assert.NoError(t, err) {
		assert.Equal(t, expectedName, actualName)
	}

	actualCreatedTime, err := s.CreatedTime()
	if assert.NoError(t, err) {
		assert.WithinDuration(t, createdAt, actualCreatedTime, 0)
	}

	actualCompletedTime, err := s.CompletedTime()
	if assert.NoError(t, err) {
		assert.WithinDuration(t, completedAt, actualCompletedTime, 0)
	}

	assert.Equal(t, protos.OrchestrationStatus_ORCHESTRATION_STATUS_COMPLETED, s.RuntimeStatus())

	assert.Equal(t, events, s.OldEvents())
	assert.Equal(t, 0, len(s.NewEvents()))
}

func Test_CompletedSubOrchestration(t *testing.T) {
	expectedOutput := "\"done!\""
	expectedTaskID := int32(3)

	// TODO: Loop through different completion status values
	status := protos.OrchestrationStatus_ORCHESTRATION_STATUS_COMPLETED

	parentInfo := helpers.NewParentInfo(expectedTaskID, "Parent", "parent_id")
	s := backend.NewOrchestrationRuntimeState("abc", []*protos.HistoryEvent{
		helpers.NewExecutionStartedEvent("Child", "child_id", nil, parentInfo, nil),
	})

	actions := []*protos.OrchestratorAction{
		helpers.NewCompleteOrchestrationAction(
			expectedTaskID,
			status,
			wrapperspb.String(expectedOutput),
			[]*protos.HistoryEvent{},
			nil),
	}
	s.AddActions(actions)

	changes, err := s.ProcessChanges(defaultChunkingConfig, nil, logger)
	require.NoError(t, err)
	assert.False(t, changes.ContinuedAsNew)
	assert.False(t, changes.IsPartial)

	require.Len(t, changes.NewEvents, 1)
	e := changes.NewEvents[0]
	assert.NotNil(t, e.Timestamp)

	ec := e.GetExecutionCompleted()
	require.NotNil(t, ec)
	assert.Equal(t, expectedTaskID, e.EventId)
	assert.Equal(t, status, ec.OrchestrationStatus)
	assert.Equal(t, expectedOutput, ec.Result.GetValue())
	assert.Nil(t, ec.FailureDetails)

	require.Len(t, changes.NewMessages, 1)
	m := changes.NewMessages[0]
	assert.NotNil(t, m.HistoryEvent.GetTimestamp())

	soc := m.HistoryEvent.GetSubOrchestrationInstanceCompleted()
	require.NotNil(t, soc)
	assert.Equal(t, expectedTaskID, soc.TaskScheduledId)
	assert.Equal(t, expectedOutput, soc.Result.GetValue())
}

func Test_RuntimeState_ContinueAsNew(t *testing.T) {
	iid := "abc"
	expectedName := "MyOrchestration"
	continueAsNewInput := "\"done!\""
	expectedTaskID := int32(3)
	eventName := "MyRaisedEvent"
	eventPayload := "MyEventPayload"

	state := backend.NewOrchestrationRuntimeState(api.InstanceID(iid), []*protos.HistoryEvent{
		helpers.NewExecutionStartedEvent(expectedName, iid, nil, nil, nil),
	})

	carryoverEvents := []*protos.HistoryEvent{helpers.NewEventRaisedEvent(eventName, wrapperspb.String(eventPayload))}
	actions := []*protos.OrchestratorAction{
		helpers.NewCompleteOrchestrationAction(
			expectedTaskID,
			protos.OrchestrationStatus_ORCHESTRATION_STATUS_CONTINUED_AS_NEW,
			wrapperspb.String(continueAsNewInput),
			carryoverEvents,
			nil),
	}
	state.AddActions(actions)

	_, err := state.ProcessChanges(defaultChunkingConfig, nil, logger)
	require.ErrorIs(t, err, backend.ErrContinuedAsNew)
}

func Test_CreateTimer(t *testing.T) {
	const iid = "abc"
	expectedFireAt := time.Now().UTC().Add(72 * time.Hour)

	s := backend.NewOrchestrationRuntimeState(iid, []*protos.HistoryEvent{
		helpers.NewExecutionStartedEvent("MyOrchestration", iid, nil, nil, nil),
	})

	var actions []*protos.OrchestratorAction
	timerCount := 3
	for i := 1; i <= timerCount; i++ {
		actions = append(actions, helpers.NewCreateTimerAction(int32(i), expectedFireAt))
	}
	s.AddActions(actions)

	changes, err := s.ProcessChanges(defaultChunkingConfig, nil, logger)
	require.NoError(t, err)

	if assert.Len(t, changes.NewEvents, timerCount) {
		for _, e := range changes.NewEvents {
			assert.NotNil(t, e.Timestamp)
			if timerCreated := e.GetTimerCreated(); assert.NotNil(t, timerCreated) {
				assert.WithinDuration(t, expectedFireAt, timerCreated.FireAt.AsTime(), 0)
			}
		}
	}
	if assert.Len(t, changes.NewTimers, timerCount) {
		for i, e := range changes.NewTimers {
			assert.NotNil(t, e.Timestamp)
			if timerFired := e.GetTimerFired(); assert.NotNil(t, timerFired) {
				expectedTimerID := int32(i + 1)
				assert.WithinDuration(t, expectedFireAt, timerFired.FireAt.AsTime(), 0)
				assert.Equal(t, expectedTimerID, timerFired.TimerId)
			}
		}
	}
}

func Test_ScheduleTask(t *testing.T) {
	const iid = "abc"
	expectedTaskID := int32(1)
	expectedName := "MyActivity"
	expectedInput := "{\"Foo\":5}"

	state := backend.NewOrchestrationRuntimeState(iid, []*protos.HistoryEvent{
		helpers.NewExecutionStartedEvent("MyOrchestration", iid, wrapperspb.String(expectedInput), nil, nil),
	})

	actions := []*protos.OrchestratorAction{
		helpers.NewScheduleTaskAction(expectedTaskID, expectedName, wrapperspb.String(expectedInput)),
	}
	state.AddActions(actions)

	tc := &protos.TraceContext{TraceParent: "trace", TraceState: wrapperspb.String("state")}
	changes, err := state.ProcessChanges(defaultChunkingConfig, tc, logger)
	require.NoError(t, err)

	if assert.Len(t, changes.NewEvents, 1) {
		e := changes.NewEvents[0]
		if taskScheduled := e.GetTaskScheduled(); assert.NotNil(t, taskScheduled) {
			assert.Equal(t, expectedTaskID, e.EventId)
			assert.Equal(t, expectedName, taskScheduled.Name)
			assert.Equal(t, expectedInput, taskScheduled.Input.GetValue())
			if assert.NotNil(t, taskScheduled.ParentTraceContext) {
				assert.Equal(t, "trace", taskScheduled.ParentTraceContext.TraceParent)
				assert.Equal(t, "state", taskScheduled.ParentTraceContext.TraceState.GetValue())
			}
		}
	}
	if assert.Len(t, changes.NewTasks, 1) {
		e := changes.NewTasks[0]
		if taskScheduled := e.GetTaskScheduled(); assert.NotNil(t, taskScheduled) {
			assert.Equal(t, expectedTaskID, e.EventId)
			assert.Equal(t, expectedName, taskScheduled.Name)
			assert.Equal(t, expectedInput, taskScheduled.Input.GetValue())
			if assert.NotNil(t, taskScheduled.ParentTraceContext) {
				assert.Equal(t, "trace", taskScheduled.ParentTraceContext.TraceParent)
				assert.Equal(t, "state", taskScheduled.ParentTraceContext.TraceState.GetValue())
			}
		}
	}
}

func Test_CreateSubOrchestration(t *testing.T) {
	iid := "abc"
	expectedTaskID := int32(4)
	expectedInstanceID := "xyz"
	expectedName := "MySubOrchestration"
	expectedInput := wrapperspb.String("{\"Foo\":5}")
	expectedTraceParent := "trace"
	expectedTraceState := "trace_state"

	state := backend.NewOrchestrationRuntimeState(api.InstanceID(iid), []*protos.HistoryEvent{
		helpers.NewExecutionStartedEvent("Parent", iid, nil, nil, nil),
	})

	actions := []*protos.OrchestratorAction{
		helpers.NewCreateSubOrchestrationAction(expectedTaskID, expectedName, expectedInstanceID, expectedInput),
	}
	state.AddActions(actions)

	tc := &protos.TraceContext{
		TraceParent: expectedTraceParent,
		TraceState:  wrapperspb.String(expectedTraceState),
	}
	changes, err := state.ProcessChanges(defaultChunkingConfig, tc, logger)
	require.NoError(t, err)

	if assert.Len(t, changes.NewEvents, 1) {
		e := changes.NewEvents[0]
		if orchCreated := e.GetSubOrchestrationInstanceCreated(); assert.NotNil(t, orchCreated) {
			assert.Equal(t, expectedTaskID, e.EventId)
			assert.Equal(t, expectedInstanceID, orchCreated.InstanceId)
			assert.Equal(t, expectedName, orchCreated.Name)
			assert.Equal(t, expectedInput.GetValue(), orchCreated.Input.GetValue())
			if assert.NotNil(t, orchCreated.ParentTraceContext) {
				assert.Equal(t, expectedTraceParent, orchCreated.ParentTraceContext.TraceParent)
				assert.Equal(t, expectedTraceState, orchCreated.ParentTraceContext.TraceState.GetValue())
			}
		}
	}
	if assert.Len(t, changes.NewMessages, 1) {
		msg := changes.NewMessages[0]
		if executionStarted := msg.HistoryEvent.GetExecutionStarted(); assert.NotNil(t, executionStarted) {
			assert.Equal(t, int32(-1), msg.HistoryEvent.EventId)
			assert.Equal(t, expectedInstanceID, executionStarted.OrchestrationInstance.InstanceId)
			assert.NotEmpty(t, executionStarted.OrchestrationInstance.ExecutionId)
			assert.Equal(t, expectedName, executionStarted.Name)
			assert.Equal(t, expectedInput.GetValue(), executionStarted.Input.GetValue())
			if assert.NotNil(t, executionStarted.ParentInstance) {
				assert.Equal(t, "Parent", executionStarted.ParentInstance.Name.GetValue())
				assert.Equal(t, expectedTaskID, executionStarted.ParentInstance.TaskScheduledId)
				if assert.NotNil(t, executionStarted.ParentInstance.OrchestrationInstance) {
					assert.Equal(t, iid, executionStarted.ParentInstance.OrchestrationInstance.InstanceId)
				}
			}
			if assert.NotNil(t, executionStarted.ParentTraceContext) {
				assert.Equal(t, expectedTraceParent, executionStarted.ParentTraceContext.TraceParent)
				assert.Equal(t, expectedTraceState, executionStarted.ParentTraceContext.TraceState.GetValue())
			}
		}
	}
}

func Test_SendEvent(t *testing.T) {
	expectedInstanceID := "xyz"
	expectedEventName := "MyEvent"
	expectedInput := "foo"

	s := backend.NewOrchestrationRuntimeState("abc", []*protos.HistoryEvent{
		helpers.NewExecutionStartedEvent("MyOrchestration", "abc", wrapperspb.String(expectedInput), nil, nil),
	})

	actions := []*protos.OrchestratorAction{
		helpers.NewSendEventAction(expectedInstanceID, expectedEventName, wrapperspb.String(expectedInput)),
	}
	s.AddActions(actions)

	changes, err := s.ProcessChanges(defaultChunkingConfig, nil, logger)
	require.NoError(t, err)

	if assert.Len(t, changes.NewEvents, 1) {
		e := changes.NewEvents[0]
		if sendEvent := e.GetEventSent(); assert.NotNil(t, sendEvent) {
			assert.Equal(t, expectedEventName, sendEvent.Name)
			assert.Equal(t, expectedInput, sendEvent.Input.GetValue())
			assert.Equal(t, expectedInstanceID, sendEvent.InstanceId)
		}
	}
	if assert.Len(t, changes.NewMessages, 1) {
		msg := changes.NewMessages[0]
		if sendEvent := msg.HistoryEvent.GetEventSent(); assert.NotNil(t, sendEvent) {
			assert.Equal(t, expectedEventName, sendEvent.Name)
			assert.Equal(t, expectedInput, sendEvent.Input.GetValue())
			assert.Equal(t, expectedInstanceID, sendEvent.InstanceId)
		}
	}
}

func Test_StateIsValid(t *testing.T) {
	s := backend.NewOrchestrationRuntimeState("abc", []*protos.HistoryEvent{})
	assert.True(t, s.IsValid())
	s = backend.NewOrchestrationRuntimeState("abc", []*protos.HistoryEvent{
		helpers.NewExecutionStartedEvent("MyOrchestration", "abc", nil, nil, nil),
	})
	assert.True(t, s.IsValid())
	s = backend.NewOrchestrationRuntimeState("abc", []*protos.HistoryEvent{
		helpers.NewTaskCompletedEvent(1, nil),
	})
	assert.False(t, s.IsValid())
}

func Test_DuplicateIncomingEvents(t *testing.T) {
	t.Run("ExecutionStarted", func(t *testing.T) {
		s := backend.NewOrchestrationRuntimeState("abc", []*protos.HistoryEvent{})
		err := s.AddEvent(helpers.NewExecutionStartedEvent("MyOrchestration", "abc", nil, nil, nil))
		require.NoError(t, err)
		err = s.AddEvent(helpers.NewExecutionStartedEvent("MyOrchestration", "abc", nil, nil, nil))
		require.ErrorIs(t, err, backend.ErrDuplicateEvent)
	})

	t.Run("ExecutionCompleted", func(t *testing.T) {
		s := backend.NewOrchestrationRuntimeState("abc", []*protos.HistoryEvent{})
		err := s.AddEvent(helpers.NewExecutionCompletedEvent(-1, protos.OrchestrationStatus_ORCHESTRATION_STATUS_COMPLETED, nil, nil))
		require.NoError(t, err)
		err = s.AddEvent(helpers.NewExecutionCompletedEvent(-1, protos.OrchestrationStatus_ORCHESTRATION_STATUS_COMPLETED, nil, nil))
		assert.ErrorIs(t, err, backend.ErrDuplicateEvent)
	})

	t.Run("TaskScheduled", func(t *testing.T) {
		s := backend.NewOrchestrationRuntimeState("abc", []*protos.HistoryEvent{})
		err := s.AddEvent(helpers.NewTaskScheduledEvent(1, "MyTask", nil, nil, nil))
		require.NoError(t, err)
		err = s.AddEvent(helpers.NewTaskScheduledEvent(1, "MyTask", nil, nil, nil))
		assert.ErrorIs(t, err, backend.ErrDuplicateEvent)
	})

	t.Run("TaskCompleted", func(t *testing.T) {
		s := backend.NewOrchestrationRuntimeState("abc", []*protos.HistoryEvent{})
		err := s.AddEvent(helpers.NewTaskCompletedEvent(1, nil))
		require.NoError(t, err)
		err = s.AddEvent(helpers.NewTaskCompletedEvent(1, nil))
		assert.ErrorIs(t, err, backend.ErrDuplicateEvent)
		err = s.AddEvent(helpers.NewTaskFailedEvent(1, nil))
		assert.ErrorIs(t, err, backend.ErrDuplicateEvent)
	})

	t.Run("TaskFailed", func(t *testing.T) {
		s := backend.NewOrchestrationRuntimeState("abc", []*protos.HistoryEvent{})
		err := s.AddEvent(helpers.NewTaskFailedEvent(1, nil))
		require.NoError(t, err)
		err = s.AddEvent(helpers.NewTaskFailedEvent(1, nil))
		assert.ErrorIs(t, err, backend.ErrDuplicateEvent)
		err = s.AddEvent(helpers.NewTaskCompletedEvent(1, nil))
		assert.ErrorIs(t, err, backend.ErrDuplicateEvent)
	})

	t.Run("TimerCreated", func(t *testing.T) {
		s := backend.NewOrchestrationRuntimeState("abc", []*protos.HistoryEvent{})
		err := s.AddEvent(helpers.NewTimerCreatedEvent(1, timestamppb.Now()))
		require.NoError(t, err)
		err = s.AddEvent(helpers.NewTimerCreatedEvent(1, timestamppb.Now()))
		assert.ErrorIs(t, err, backend.ErrDuplicateEvent)
	})

	t.Run("TimerFired", func(t *testing.T) {
		s := backend.NewOrchestrationRuntimeState("abc", []*protos.HistoryEvent{})
		err := s.AddEvent(helpers.NewTimerFiredEvent(1, timestamppb.Now()))
		require.NoError(t, err)
		err = s.AddEvent(helpers.NewTimerFiredEvent(1, timestamppb.Now()))
		assert.ErrorIs(t, err, backend.ErrDuplicateEvent)
	})

	t.Run("SubOrchestrationInstanceCreated", func(t *testing.T) {
		s := backend.NewOrchestrationRuntimeState("abc", []*protos.HistoryEvent{})
		err := s.AddEvent(helpers.NewSubOrchestrationCreatedEvent(1, "MyOrchestration", nil, nil, "xyz", nil))
		require.NoError(t, err)
		err = s.AddEvent(helpers.NewSubOrchestrationCreatedEvent(1, "MyOrchestration", nil, nil, "xyz", nil))
		assert.ErrorIs(t, err, backend.ErrDuplicateEvent)
	})

	t.Run("SubOrchestrationInstanceCompleted", func(t *testing.T) {
		s := backend.NewOrchestrationRuntimeState("abc", []*protos.HistoryEvent{})
		err := s.AddEvent(helpers.NewSubOrchestrationCompletedEvent(1, nil))
		require.NoError(t, err)
		err = s.AddEvent(helpers.NewSubOrchestrationCompletedEvent(1, nil))
		assert.ErrorIs(t, err, backend.ErrDuplicateEvent)
		err = s.AddEvent(helpers.NewSubOrchestrationFailedEvent(1, nil))
		assert.ErrorIs(t, err, backend.ErrDuplicateEvent)
	})

	t.Run("SubOrchestrationInstanceFailed", func(t *testing.T) {
		s := backend.NewOrchestrationRuntimeState("abc", []*protos.HistoryEvent{})
		err := s.AddEvent(helpers.NewSubOrchestrationFailedEvent(1, nil))
		require.NoError(t, err)
		err = s.AddEvent(helpers.NewSubOrchestrationFailedEvent(1, nil))
		assert.ErrorIs(t, err, backend.ErrDuplicateEvent)
		err = s.AddEvent(helpers.NewSubOrchestrationCompletedEvent(1, nil))
		assert.ErrorIs(t, err, backend.ErrDuplicateEvent)
	})
}

// Test_DuplicateOutgoingEvents verifies that duplicate outgoing events are ignored.
// This can happen if the orchestrator code schedules certain actions successfully as part of
// a chunk, but then fails before the final chunk is committed. When the orchestrator is replayed,
// it will attempt to schedule the same actions again, but the runtime state will already contain
// the outbound events in its history, so it can identify and de-dupe them.
func Test_DuplicateOutgoingEvents(t *testing.T) {
	t.Run("TimerCreated", func(t *testing.T) {
		now := timestamppb.Now()
		timerID := int32(1)
		s := backend.NewOrchestrationRuntimeState("abc", []*protos.HistoryEvent{helpers.NewTimerCreatedEvent(timerID, now)})
		s.AddActions([]*protos.OrchestratorAction{helpers.NewCreateTimerAction(timerID, now.AsTime())})
		changes, err := s.ProcessChanges(defaultChunkingConfig, nil, logger)
		require.NoError(t, err)
		assert.Empty(t, changes.NewEvents)
		assert.Empty(t, changes.NewTimers)
	})

	t.Run("TaskScheduled", func(t *testing.T) {
		taskID := int32(1)
		s := backend.NewOrchestrationRuntimeState("abc", []*protos.HistoryEvent{helpers.NewTaskScheduledEvent(taskID, "MyTask", nil, nil, nil)})
		s.AddActions([]*protos.OrchestratorAction{helpers.NewScheduleTaskAction(taskID, "MyTask", nil)})
		changes, err := s.ProcessChanges(defaultChunkingConfig, nil, logger)
		require.NoError(t, err)
		assert.Empty(t, changes.NewEvents)
		assert.Empty(t, changes.NewTasks)
	})

	t.Run("SubOrchestrationInstanceCreated", func(t *testing.T) {
		taskID := int32(1)
		s := backend.NewOrchestrationRuntimeState("abc", []*protos.HistoryEvent{helpers.NewSubOrchestrationCreatedEvent(taskID, "MyOrchestration", nil, nil, "xyz", nil)})
		s.AddActions([]*protos.OrchestratorAction{helpers.NewCreateSubOrchestrationAction(taskID, "MyOrchestration", "xyz", nil)})
		changes, err := s.ProcessChanges(defaultChunkingConfig, nil, logger)
		require.NoError(t, err)
		assert.Empty(t, changes.NewEvents)
		assert.Empty(t, changes.NewMessages)
	})
}

func Test_SetFailed(t *testing.T) {
	errFailure := errors.New("you got terminated")
	t.Run("Running", func(t *testing.T) {
		s := backend.NewOrchestrationRuntimeState("abc", []*protos.HistoryEvent{
			helpers.NewExecutionStartedEvent("MyOrchestration", "abc", nil, nil, nil),
		})
		assert.NotEqual(t, protos.OrchestrationStatus_ORCHESTRATION_STATUS_FAILED, s.RuntimeStatus())
		s.SetFailed(errFailure)
		s.ProcessChanges(defaultChunkingConfig, nil, logger)
		assert.Equal(t, protos.OrchestrationStatus_ORCHESTRATION_STATUS_FAILED, s.RuntimeStatus())
		failureDetails, err := s.FailureDetails()
		require.NoError(t, err)
		assert.Equal(t, errFailure.Error(), failureDetails.ErrorMessage)
	})

	t.Run("ContinuedAsNew", func(t *testing.T) {
		s := backend.NewOrchestrationRuntimeState("abc", []*protos.HistoryEvent{
			helpers.NewExecutionStartedEvent("MyOrchestration", "abc", nil, nil, nil),
		})
		s.AddEvent(helpers.NewExecutionCompletedEvent(-1, protos.OrchestrationStatus_ORCHESTRATION_STATUS_CONTINUED_AS_NEW, nil, nil))
		assert.NotEqual(t, protos.OrchestrationStatus_ORCHESTRATION_STATUS_FAILED, s.RuntimeStatus())
		s.SetFailed(errFailure)
		s.ProcessChanges(defaultChunkingConfig, nil, logger)
		assert.Equal(t, protos.OrchestrationStatus_ORCHESTRATION_STATUS_FAILED, s.RuntimeStatus())
		failureDetails, err := s.FailureDetails()
		require.NoError(t, err)
		assert.Equal(t, errFailure.Error(), failureDetails.ErrorMessage)
	})
}
