package tests

import (
	"testing"
	"time"

	"github.com/microsoft/durabletask-go/api"
	"github.com/microsoft/durabletask-go/backend"
	"github.com/microsoft/durabletask-go/internal/helpers"
	"github.com/microsoft/durabletask-go/internal/protos"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/types/known/timestamppb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

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
		helpers.NewCompleteOrchestrationAction(expectedTaskID, status, expectedOutput, []*protos.HistoryEvent{}, nil),
	}

	continuedAsNew, err := s.ApplyActions(actions, nil)
	if assert.NoError(t, err) && assert.False(t, continuedAsNew) {
		if assert.Len(t, s.NewEvents(), 1) {
			e := s.NewEvents()[0]
			assert.NotNil(t, e.Timestamp)
			if ec := e.GetExecutionCompleted(); assert.NotNil(t, ec) {
				assert.Equal(t, expectedTaskID, e.EventId)
				assert.Equal(t, status, ec.OrchestrationStatus)
				assert.Equal(t, expectedOutput, ec.Result.GetValue())
				assert.Nil(t, ec.FailureDetails)
			}
		}
		if assert.Len(t, s.PendingMessages(), 1) {
			e := s.PendingMessages()[0]
			assert.NotNil(t, e.HistoryEvent.Timestamp)
			if soc := e.HistoryEvent.GetSubOrchestrationInstanceCompleted(); assert.NotNil(t, soc) {
				assert.Equal(t, expectedTaskID, soc.TaskScheduledId)
				assert.Equal(t, expectedOutput, soc.Result.GetValue())
			}
		}
	}
}

func Test_ContinueAsNew(t *testing.T) {
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
			continueAsNewInput,
			carryoverEvents,
			nil),
	}

	continuedAsNew, err := state.ApplyActions(actions, nil)
	if assert.NoError(t, err) && assert.True(t, continuedAsNew) {
		if assert.Len(t, state.NewEvents(), 3) {
			assert.NotNil(t, state.NewEvents()[0].Timestamp)
			assert.NotNil(t, state.NewEvents()[0].GetOrchestratorStarted())
			assert.NotNil(t, state.NewEvents()[1].Timestamp)
			if ec := state.NewEvents()[1].GetExecutionStarted(); assert.NotNil(t, ec) {
				assert.Equal(t, protos.OrchestrationStatus_ORCHESTRATION_STATUS_RUNNING, state.RuntimeStatus())
				assert.Equal(t, string(state.InstanceID()), ec.OrchestrationInstance.InstanceId)
				if name, err := state.Name(); assert.NoError(t, err) {
					assert.Equal(t, expectedName, name)
					assert.Equal(t, expectedName, ec.Name)
				}
				if input, err := state.Input(); assert.NoError(t, err) {
					assert.Equal(t, continueAsNewInput, input)
				}
			}
			assert.NotNil(t, state.NewEvents()[2].Timestamp)
			if er := state.NewEvents()[2].GetEventRaised(); assert.NotNil(t, er) {
				assert.Equal(t, eventName, er.Name)
				assert.Equal(t, eventPayload, er.Input.GetValue())
			}
		}
		assert.Empty(t, state.PendingMessages())
		assert.Empty(t, state.PendingTasks())
		assert.Empty(t, state.PendingTimers())
	}
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

	continuedAsNew, err := s.ApplyActions(actions, nil)
	if assert.NoError(t, err) && assert.False(t, continuedAsNew) {
		if assert.Len(t, s.NewEvents(), timerCount) {
			for _, e := range s.NewEvents() {
				assert.NotNil(t, e.Timestamp)
				if timerCreated := e.GetTimerCreated(); assert.NotNil(t, timerCreated) {
					assert.WithinDuration(t, expectedFireAt, timerCreated.FireAt.AsTime(), 0)
				}
			}
		}
		if assert.Len(t, s.PendingTimers(), timerCount) {
			for i, e := range s.PendingTimers() {
				assert.NotNil(t, e.Timestamp)
				if timerFired := e.GetTimerFired(); assert.NotNil(t, timerFired) {
					expectedTimerID := int32(i + 1)
					assert.WithinDuration(t, expectedFireAt, timerFired.FireAt.AsTime(), 0)
					assert.Equal(t, expectedTimerID, timerFired.TimerId)
				}
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

	tc := &protos.TraceContext{TraceID: "trace", SpanID: "span", TraceState: wrapperspb.String("state")}
	continuedAsNew, err := state.ApplyActions(actions, tc)
	if assert.NoError(t, err) && assert.False(t, continuedAsNew) {
		if assert.Len(t, state.NewEvents(), 1) {
			e := state.NewEvents()[0]
			if taskScheduled := e.GetTaskScheduled(); assert.NotNil(t, taskScheduled) {
				assert.Equal(t, expectedTaskID, e.EventId)
				assert.Equal(t, expectedName, taskScheduled.Name)
				assert.Equal(t, expectedInput, taskScheduled.Input.GetValue())
				if assert.NotNil(t, taskScheduled.ParentTraceContext) {
					assert.Equal(t, "trace", taskScheduled.ParentTraceContext.TraceID)
					assert.Equal(t, "span", taskScheduled.ParentTraceContext.SpanID)
					assert.Equal(t, "state", taskScheduled.ParentTraceContext.TraceState.GetValue())
				}
			}
		}
		if assert.Len(t, state.PendingTasks(), 1) {
			e := state.PendingTasks()[0]
			if taskScheduled := e.GetTaskScheduled(); assert.NotNil(t, taskScheduled) {
				assert.Equal(t, expectedTaskID, e.EventId)
				assert.Equal(t, expectedName, taskScheduled.Name)
				assert.Equal(t, expectedInput, taskScheduled.Input.GetValue())
				if assert.NotNil(t, taskScheduled.ParentTraceContext) {
					assert.Equal(t, "trace", taskScheduled.ParentTraceContext.TraceID)
					assert.Equal(t, "span", taskScheduled.ParentTraceContext.SpanID)
					assert.Equal(t, "state", taskScheduled.ParentTraceContext.TraceState.GetValue())
				}
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
	expectedTraceID := "trace"
	expectedSpanID := "span"
	expectedTraceState := "trace_state"

	state := backend.NewOrchestrationRuntimeState(api.InstanceID(iid), []*protos.HistoryEvent{
		helpers.NewExecutionStartedEvent("Parent", iid, nil, nil, nil),
	})

	actions := []*protos.OrchestratorAction{
		helpers.NewCreateSubOrchestrationAction(expectedTaskID, expectedName, expectedInstanceID, expectedInput),
	}

	tc := &protos.TraceContext{
		TraceID:    expectedTraceID,
		SpanID:     expectedSpanID,
		TraceState: wrapperspb.String(expectedTraceState),
	}
	continuedAsNew, err := state.ApplyActions(actions, tc)
	if assert.NoError(t, err) && assert.False(t, continuedAsNew) {
		if assert.Len(t, state.NewEvents(), 1) {
			e := state.NewEvents()[0]
			if orchCreated := e.GetSubOrchestrationInstanceCreated(); assert.NotNil(t, orchCreated) {
				assert.Equal(t, expectedTaskID, e.EventId)
				assert.Equal(t, expectedInstanceID, orchCreated.InstanceId)
				assert.Equal(t, expectedName, orchCreated.Name)
				assert.Equal(t, expectedInput.GetValue(), orchCreated.Input.GetValue())
				if assert.NotNil(t, orchCreated.ParentTraceContext) {
					assert.Equal(t, expectedTraceID, orchCreated.ParentTraceContext.TraceID)
					assert.Equal(t, expectedSpanID, orchCreated.ParentTraceContext.SpanID)
					assert.Equal(t, expectedTraceState, orchCreated.ParentTraceContext.TraceState.GetValue())
				}
			}
		}
		if assert.Len(t, state.PendingMessages(), 1) {
			msg := state.PendingMessages()[0]
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
					assert.Equal(t, expectedTraceID, executionStarted.ParentTraceContext.TraceID)
					assert.Equal(t, expectedSpanID, executionStarted.ParentTraceContext.SpanID)
					assert.Equal(t, expectedTraceState, executionStarted.ParentTraceContext.TraceState.GetValue())
				}
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

	continuedAsNew, err := s.ApplyActions(actions, nil)
	if assert.NoError(t, err) && assert.False(t, continuedAsNew) {
		if assert.Len(t, s.NewEvents(), 1) {
			e := s.NewEvents()[0]
			if sendEvent := e.GetEventSent(); assert.NotNil(t, sendEvent) {
				assert.Equal(t, expectedEventName, sendEvent.Name)
				assert.Equal(t, expectedInput, sendEvent.Input.GetValue())
				assert.Equal(t, expectedInstanceID, sendEvent.InstanceId)
			}
		}
		if assert.Len(t, s.PendingMessages(), 1) {
			msg := s.PendingMessages()[0]
			if sendEvent := msg.HistoryEvent.GetEventSent(); assert.NotNil(t, sendEvent) {
				assert.Equal(t, expectedEventName, sendEvent.Name)
				assert.Equal(t, expectedInput, sendEvent.Input.GetValue())
				assert.Equal(t, expectedInstanceID, sendEvent.InstanceId)
			}
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

func Test_DuplicateEvents(t *testing.T) {
	s := backend.NewOrchestrationRuntimeState("abc", []*protos.HistoryEvent{})
	if err := s.AddEvent(helpers.NewExecutionStartedEvent("MyOrchestration", "abc", nil, nil, nil)); assert.NoError(t, err) {
		err = s.AddEvent(helpers.NewExecutionStartedEvent("MyOrchestration", "abc", nil, nil, nil))
		assert.ErrorIs(t, err, backend.ErrDuplicateEvent)
	} else {
		return
	}

	// TODO: Add other types of duplicate events (task completion, external events, sub-orchestration, etc.)

	if err := s.AddEvent(helpers.NewExecutionCompletedEvent(-1, protos.OrchestrationStatus_ORCHESTRATION_STATUS_COMPLETED, nil, nil)); assert.NoError(t, err) {
		err = s.AddEvent(helpers.NewExecutionCompletedEvent(-1, protos.OrchestrationStatus_ORCHESTRATION_STATUS_COMPLETED, nil, nil))
		assert.ErrorIs(t, err, backend.ErrDuplicateEvent)
	} else {
		return
	}
}
