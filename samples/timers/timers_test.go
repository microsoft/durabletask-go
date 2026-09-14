package main

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/microsoft/durabletask-go/api"
	"github.com/microsoft/durabletask-go/internal/helpers"
	"github.com/microsoft/durabletask-go/internal/historyconv"
	"github.com/microsoft/durabletask-go/internal/protos"
	"github.com/microsoft/durabletask-go/task"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

func TestTimerSampleAcceptsLatePhysicalDelivery(t *testing.T) {
	registry := task.NewTaskRegistry()
	require.NoError(t, registry.AddOrchestratorN("TimerOrchestrator", TimerOrchestrator))
	executor := task.NewTaskExecutor(registry, task.WithMaximumTimerInterval(maximumPhysicalTimerInterval))
	input, err := json.Marshal(timerInput{Delay: logicalTimerDelay})
	require.NoError(t, err)
	start := time.Date(2026, 9, 14, 12, 0, 0, 0, time.UTC)
	started := helpers.NewOrchestratorStartedEvent()
	started.Timestamp = timestamppb.New(start)
	history := []*protos.HistoryEvent{started,
		helpers.NewExecutionStartedEvent("TimerOrchestrator", "instance", wrapperspb.String(string(input)), nil, nil, nil)}
	first, err := executor.ExecuteOrchestrator(context.Background(), "instance", nil, history, nil)
	require.NoError(t, err)
	require.Len(t, first.Response.Actions, 1)
	timer := first.Response.Actions[0]
	require.Equal(t, start.Add(maximumPhysicalTimerInterval), timer.GetCreateTimer().GetFireAt().AsTime())
	history = append(history, helpers.NewTimerCreatedEvent(timer.Id, timer.GetCreateTimer().FireAt))

	late := helpers.NewOrchestratorStartedEvent()
	late.Timestamp = timestamppb.New(start.Add(6 * time.Second))
	events := []*protos.HistoryEvent{late, helpers.NewTimerFiredEvent(timer.Id, timer.GetCreateTimer().FireAt, nil)}
	second, err := executor.ExecuteOrchestrator(context.Background(), "instance", history, events, nil)
	require.NoError(t, err)
	require.Len(t, second.Response.Actions, 1)
	activity := second.Response.Actions[0]
	require.Equal(t, "RecordStableObservation", activity.GetScheduleTask().GetName())
	history = append(history, events...)
	history = append(history, helpers.NewTaskScheduledEvent(activity.Id, "RecordStableObservation", nil, activity.GetScheduleTask().Input, nil))
	events = []*protos.HistoryEvent{late, helpers.NewTaskCompletedEvent(activity.Id, activity.GetScheduleTask().Input)}
	last, err := executor.ExecuteOrchestrator(context.Background(), "instance", history, events, nil)
	require.NoError(t, err)
	require.Len(t, last.Response.Actions, 1)
	completion := last.Response.Actions[0].GetCompleteOrchestration()
	require.NotNil(t, completion)
	require.Equal(t, api.RUNTIME_STATUS_COMPLETED, completion.OrchestrationStatus)
	var output timerOutput
	require.NoError(t, json.Unmarshal([]byte(completion.GetResult().GetValue()), &output))
	require.Equal(t, start.Add(logicalTimerDelay), output.Deadline)
	require.Equal(t, start.Add(6*time.Second), output.FiredAt)

	converter := historyconv.New(nil)
	var records []*api.HistoryEvent
	for _, event := range append(history, events...) {
		record, err := converter.Convert(event)
		require.NoError(t, err)
		records = append(records, record)
	}
	require.NoError(t, verifyTimerHistory(records, output))
	output.FiredAt = output.Deadline.Add(-time.Second)
	require.ErrorContains(t, verifyTimerHistory(records, output), "before logical deadline")
}

func TestTimerHistoryRequiresMatchedRecords(t *testing.T) {
	deadline := time.Unix(10, 0).UTC()
	created := &api.HistoryEvent{Type: api.HistoryEventTimerCreated, EventID: 7,
		TimerCreated: &api.HistoryTimerEvent{FireAt: deadline}}
	fired := &api.HistoryEvent{Type: api.HistoryEventTimerFired,
		TimerFired: &api.HistoryTimerFiredEvent{TimerID: 7, FireAt: deadline}}
	output := timerOutput{Deadline: deadline, FiredAt: deadline}
	require.NoError(t, verifyTimerHistory([]*api.HistoryEvent{created, fired}, output))
	for _, events := range [][]*api.HistoryEvent{nil, {created}, {fired}, {created, fired, fired}} {
		require.Error(t, verifyTimerHistory(events, output))
	}
	fired.TimerFired.FireAt = deadline.Add(time.Second)
	require.Error(t, verifyTimerHistory([]*api.HistoryEvent{created, fired}, output))
}
