package exporthistory

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/microsoft/durabletask-go/api"
	"github.com/microsoft/durabletask-go/internal/contextprop"
	"github.com/microsoft/durabletask-go/internal/helpers"
	"github.com/microsoft/durabletask-go/internal/protos"
	"github.com/microsoft/durabletask-go/task"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

// entityMessage is an entity-directed message the scheduler would deliver to the
// target entity, paired with that target's instance ID.
type entityMessage struct {
	historyEvent     *protos.HistoryEvent
	targetInstanceID string
}

// orchestrationDriver replays an orchestration turn by turn against synthesized
// history, so timer-heavy paths such as batch retry backoff are covered without
// waiting on the wall clock. Each turn's actions are folded back into history the
// way the scheduler does, so replay stays faithful without a live service.
type orchestrationDriver struct {
	t          *testing.T
	executor   task.Executor
	converter  api.DataConverter
	instanceID api.InstanceID
	oldEvents  []*protos.HistoryEvent
	newEvents  []*protos.HistoryEvent

	pendingTasks  []*protos.HistoryEvent
	pendingTimers []*protos.HistoryEvent
	// createdTimers holds the TimerCreated history entries for the timers this
	// turn scheduled, which carry the requested delay; pendingTimers holds the
	// matching TimerFired messages the service would deliver back.
	createdTimers   []*protos.HistoryEvent
	pendingEntities []entityMessage
	completion      *protos.CompleteOrchestrationAction
	continuedAsNew  bool
}

func newOrchestrationDriver(
	t *testing.T,
	registry *task.TaskRegistry,
	name string,
	instanceID api.InstanceID,
	input any,
	converters ...api.DataConverter,
) *orchestrationDriver {
	t.Helper()
	converter := api.DefaultDataConverter()
	if len(converters) > 0 {
		converter = api.NormalizeDataConverter(converters[0])
	}
	payload, err := api.SerializeData(converter, input)
	require.NoError(t, err)
	return &orchestrationDriver{
		t:          t,
		executor:   task.NewTaskExecutor(registry, task.WithDataConverter(converter)),
		converter:  converter,
		instanceID: instanceID,
		newEvents: []*protos.HistoryEvent{
			helpers.NewOrchestratorStartedEvent(),
			helpers.NewExecutionStartedEvent(name, string(instanceID), wrapperspb.String(payload), nil, nil, nil),
		},
	}
}

// turn executes one orchestration episode and folds the resulting actions into
// history.
func (d *orchestrationDriver) turn() {
	d.t.Helper()
	results, err := d.executor.ExecuteOrchestrator(
		context.Background(),
		d.instanceID,
		d.oldEvents,
		d.newEvents,
		&protos.OrchestratorEntityParameters{EntityMessageReorderWindow: durationpb.New(0)},
	)
	require.NoError(d.t, err)

	history := append(append([]*protos.HistoryEvent{}, d.oldEvents...), d.newEvents...)
	d.oldEvents = history
	d.foldActions(results.Response.GetActions(), history)
}

// foldActions appends the history each action produces and records the messages
// the scheduler would deliver back. Export orchestrations only schedule
// activities, call entities, create timers, and complete, so any other action is
// a gap in this harness rather than something to silently drop.
func (d *orchestrationDriver) foldActions(
	actions []*protos.OrchestratorAction,
	history []*protos.HistoryEvent,
) {
	d.t.Helper()
	d.newEvents = nil
	d.pendingTasks = nil
	d.pendingTimers = nil
	d.pendingEntities = nil
	d.createdTimers = nil
	d.completion = nil
	d.continuedAsNew = false

	started := latestExecutionStarted(d.t, history)
	for _, action := range actions {
		switch {
		case action.GetScheduleTask() != nil:
			scheduled := action.GetScheduleTask()
			event := helpers.NewTaskScheduledEvent(
				action.GetId(), scheduled.GetName(), scheduled.GetVersion(), scheduled.GetInput(), nil)
			event.GetTaskScheduled().Tags = contextprop.Clone(scheduled.GetTags())
			d.newEvents = append(d.newEvents, event)
			d.pendingTasks = append(d.pendingTasks, event)

		case action.GetCreateTimer() != nil:
			fireAt := action.GetCreateTimer().GetFireAt()
			created := helpers.NewTimerCreatedEvent(action.GetId(), fireAt)
			d.newEvents = append(d.newEvents, created)
			d.createdTimers = append(d.createdTimers, created)
			d.pendingTimers = append(d.pendingTimers, helpers.NewTimerFiredEvent(action.GetId(), fireAt, nil))

		case action.GetSendEntityMessage().GetEntityOperationCalled() != nil:
			d.foldEntityCall(action, started)

		case action.GetCompleteOrchestration() != nil:
			complete := action.GetCompleteOrchestration()
			d.completion = complete
			if complete.GetOrchestrationStatus() == protos.OrchestrationStatus_ORCHESTRATION_STATUS_CONTINUED_AS_NEW {
				d.continuedAsNew = true
				d.restart(started, complete)
				return
			}
			d.newEvents = append(d.newEvents, &protos.HistoryEvent{
				EventId:   action.GetId(),
				Timestamp: timestamppb.Now(),
				EventType: &protos.HistoryEvent_ExecutionCompleted{
					ExecutionCompleted: &protos.ExecutionCompletedEvent{
						OrchestrationStatus: complete.GetOrchestrationStatus(),
						Result:              complete.GetResult(),
						FailureDetails:      complete.GetFailureDetails(),
					},
				},
			})

		default:
			require.FailNowf(d.t, "unsupported orchestrator action", "action %v", action)
		}
	}
}

// latestExecutionStarted returns the start event of the execution the next turn
// belongs to, which is the last one in history after any ContinueAsNew.
func latestExecutionStarted(t *testing.T, history []*protos.HistoryEvent) *protos.ExecutionStartedEvent {
	t.Helper()
	var started *protos.ExecutionStartedEvent
	for _, event := range history {
		if candidate := event.GetExecutionStarted(); candidate != nil {
			started = candidate
		}
	}
	require.NotNil(t, started, "history has no ExecutionStarted event")
	return started
}

// foldEntityCall mirrors how the scheduler splits an entity call: the caller's
// history keeps the target instance ID, while the message delivered to the entity
// drops it and carries the calling execution's identity instead.
func (d *orchestrationDriver) foldEntityCall(
	action *protos.OrchestratorAction,
	started *protos.ExecutionStartedEvent,
) {
	d.t.Helper()
	historyValue := proto.Clone(
		action.GetSendEntityMessage().GetEntityOperationCalled()).(*protos.EntityOperationCalledEvent)
	target := historyValue.GetTargetInstanceId().GetValue()
	require.NotEmpty(d.t, target, "entity call has no target instance ID")
	historyValue.ParentInstanceId = nil
	historyValue.ParentExecutionId = nil

	messageValue := proto.Clone(historyValue).(*protos.EntityOperationCalledEvent)
	messageValue.TargetInstanceId = nil
	messageValue.ParentInstanceId = wrapperspb.String(string(d.instanceID))
	messageValue.ParentExecutionId = started.GetOrchestrationInstance().GetExecutionId()

	timestamp := timestamppb.Now()
	d.newEvents = append(d.newEvents, &protos.HistoryEvent{
		EventId:   action.GetId(),
		Timestamp: timestamp,
		EventType: &protos.HistoryEvent_EntityOperationCalled{EntityOperationCalled: historyValue},
	})
	d.pendingEntities = append(d.pendingEntities, entityMessage{
		historyEvent: &protos.HistoryEvent{
			EventId:   -1,
			Timestamp: timestamp,
			EventType: &protos.HistoryEvent_EntityOperationCalled{EntityOperationCalled: messageValue},
		},
		targetInstanceID: target,
	})
}

// restart truncates history the way the scheduler does for ContinueAsNew: the new
// execution starts from a fresh start event carrying the previous execution's
// identity, the new input, and any carryover events. Work scheduled earlier in
// the same action batch does not survive the execution boundary.
func (d *orchestrationDriver) restart(
	started *protos.ExecutionStartedEvent,
	complete *protos.CompleteOrchestrationAction,
) {
	d.t.Helper()
	d.pendingTasks = nil
	d.pendingTimers = nil
	d.createdTimers = nil
	d.pendingEntities = nil

	version := started.GetVersion()
	if complete.GetNewVersion() != nil {
		version = complete.GetNewVersion()
	}
	startEvent := helpers.NewExecutionStartedEvent(
		started.GetName(),
		string(d.instanceID),
		complete.GetResult(),
		started.GetParentInstance(),
		started.GetParentTraceContext(),
		nil,
		version)
	startEvent.GetExecutionStarted().Tags = contextprop.Clone(started.GetTags())

	d.oldEvents = nil
	d.newEvents = append([]*protos.HistoryEvent{
		helpers.NewOrchestratorStartedEvent(),
		startEvent,
	}, complete.GetCarryoverEvents()...)
}

// nextTurn starts a new episode by appending the orchestrator-started marker
// that sets the deterministic current time.
func (d *orchestrationDriver) nextTurn() {
	d.newEvents = append(d.newEvents, helpers.NewOrchestratorStartedEvent())
}

// completeEntityCall answers the pending entity call whose operation matches,
// with the supplied result.
func (d *orchestrationDriver) completeEntityCall(operation string, result any) {
	d.t.Helper()
	output := (*wrapperspb.StringValue)(nil)
	if result != nil {
		payload, err := api.SerializeData(d.converter, result)
		require.NoError(d.t, err)
		output = wrapperspb.String(payload)
	}
	d.completeEntityCallOutput(operation, output)
}

func (d *orchestrationDriver) completeEntityCallOutput(operation string, output *wrapperspb.StringValue) {
	d.t.Helper()
	called := d.pendingEntityCall(operation)
	d.newEvents = append(d.newEvents, &protos.HistoryEvent{
		EventId:   -1,
		Timestamp: timestamppb.Now(),
		EventType: &protos.HistoryEvent_EntityOperationCompleted{
			EntityOperationCompleted: &protos.EntityOperationCompletedEvent{
				RequestId: called.RequestId,
				Output:    output,
			},
		},
	})
}

// failEntityCall answers the pending entity call whose operation matches, with a
// failure carrying errorType.
func (d *orchestrationDriver) failEntityCall(operation, errorType, message string) {
	d.t.Helper()
	d.failEntityCallWithDetails(operation, &protos.TaskFailureDetails{ErrorType: errorType, ErrorMessage: message})
}

func (d *orchestrationDriver) failEntityCallWithDetails(operation string, details *protos.TaskFailureDetails) {
	d.t.Helper()
	called := d.pendingEntityCall(operation)
	d.newEvents = append(d.newEvents, &protos.HistoryEvent{
		EventId:   -1,
		Timestamp: timestamppb.Now(),
		EventType: &protos.HistoryEvent_EntityOperationFailed{
			EntityOperationFailed: &protos.EntityOperationFailedEvent{
				RequestId:      called.RequestId,
				FailureDetails: details,
			},
		},
	})
}

func (d *orchestrationDriver) pendingEntityCall(operation string) *protos.EntityOperationCalledEvent {
	d.t.Helper()
	called, _ := d.pendingEntityMessage(operation)
	return called
}

func (d *orchestrationDriver) pendingEntityMessage(
	operation string,
) (*protos.EntityOperationCalledEvent, string) {
	d.t.Helper()
	for _, message := range d.pendingEntities {
		called := message.historyEvent.GetEntityOperationCalled()
		if called != nil && called.GetOperation() == operation {
			return called, message.targetInstanceID
		}
	}
	require.FailNowf(d.t, "no pending entity call", "operation %q", operation)
	return nil, ""
}

// entityCallInput returns the serialized input of the pending call for operation.
func (d *orchestrationDriver) entityCallInput(operation string, target any) {
	d.t.Helper()
	called := d.pendingEntityCall(operation)
	require.NoError(d.t, d.converter.Deserialize(called.GetInput().GetValue(), target))
}

// completeActivities answers every pending activity call using results keyed by
// the activity name, in scheduling order.
func (d *orchestrationDriver) completeActivities(results func(name string, index int) any) {
	d.t.Helper()
	counts := map[string]int{}
	for _, pending := range d.pendingTasks {
		scheduled := pending.GetTaskScheduled()
		require.NotNil(d.t, scheduled)
		name := scheduled.GetName()
		index := counts[name]
		counts[name]++
		payload, err := api.SerializeData(d.converter, results(name, index))
		require.NoError(d.t, err)
		d.newEvents = append(d.newEvents,
			helpers.NewTaskCompletedEvent(pending.GetEventId(), wrapperspb.String(payload)))
	}
}

// fireTimers delivers every pending durable timer and returns the delay each one
// was created with.
func (d *orchestrationDriver) fireTimers() []time.Duration {
	d.t.Helper()
	delays := make([]time.Duration, 0, len(d.pendingTimers))
	for _, created := range d.createdTimers {
		delays = append(delays,
			created.GetTimerCreated().GetFireAt().AsTime().Sub(created.GetTimestamp().AsTime()))
	}
	d.newEvents = append(d.newEvents, d.pendingTimers...)
	return delays
}

func (d *orchestrationDriver) activityInput(name string, target any) {
	d.t.Helper()
	for _, pending := range d.pendingTasks {
		scheduled := pending.GetTaskScheduled()
		if scheduled != nil && scheduled.GetName() == name {
			require.NoError(d.t, d.converter.Deserialize(scheduled.GetInput().GetValue(), target))
			return
		}
	}
	require.FailNowf(d.t, "no pending activity", "name %q", name)
}

// assertDelay compares a durable timer delay allowing for the small skew between
// the orchestrator's deterministic clock and the timestamp the driver stamps on
// the emitted TimerCreated event.
func assertDelay(t *testing.T, expected, actual time.Duration) {
	t.Helper()
	assert.InDelta(t, expected.Seconds(), actual.Seconds(), 1, "expected a %s timer", expected)
}

func newExportRegistry(t *testing.T) *task.TaskRegistry {
	t.Helper()
	registry := task.NewTaskRegistry()
	require.NoError(t, Register(registry, WorkerOptions{Source: newFakeSource(), Store: newMemoryStore()}))
	return registry
}

func activeJobState(t *testing.T, mode ExportMode, checkpoint string, runToken ...string) ExportJobState {
	t.Helper()
	options := batchOptions("job-1")
	if mode == ExportModeContinuous {
		var err error
		options, err = JobCreationOptions{
			JobID:       "job-1",
			Mode:        ExportModeContinuous,
			Destination: &ExportDestination{Container: "test-container"},
		}.Normalize()
		require.NoError(t, err)
	}
	config, err := options.configuration()
	require.NoError(t, err)
	state := ExportJobState{Status: ExportJobStatusActive, Config: config, RunToken: "run-a"}
	if checkpoint != "" {
		state.Checkpoint = &ExportCheckpoint{LastInstanceKey: checkpoint}
	}
	if len(runToken) > 0 {
		state.RunToken = runToken[0]
	}
	return state
}

func startExportOrchestrationDriver(t *testing.T, input ...ExportJobRunRequest) *orchestrationDriver {
	t.Helper()
	request := ExportJobRunRequest{JobEntityID: EntityID("job-1"), RunToken: "run-a"}
	if len(input) > 0 {
		request = input[0]
	}
	return newOrchestrationDriver(
		t,
		newExportRegistry(t),
		ExportJobOrchestratorName,
		api.InstanceID(OrchestratorInstanceIDPrefix+"job-1-"+request.RunToken),
		request,
	)
}

// TestExportJobOrchestratorBatchCompletes covers the happy path: one page of
// instances is exported, a checkpoint is committed, and the empty follow-up page
// completes the job.
func TestExportJobOrchestratorBatchCompletes(t *testing.T) {
	driver := startExportOrchestrationDriver(t)

	driver.turn()
	driver.completeEntityCall(getOperation, activeJobState(t, ExportModeBatch, ""))

	driver.nextTurn()
	driver.turn()
	var listRequest ListTerminalInstancesRequest
	driver.activityInput(ListTerminalInstancesActivityName, &listRequest)
	assert.Equal(t, DefaultMaxInstancesPerBatch, listRequest.MaxInstancesPerBatch)
	assert.Equal(t, TerminalStatuses(), listRequest.RuntimeStatus)
	assert.Empty(t, listRequest.LastInstanceKey)
	driver.completeActivities(func(string, int) any {
		return InstancePage{
			InstanceIDs:    []string{"i1", "i2"},
			NextCheckpoint: &ExportCheckpoint{LastInstanceKey: "cursor-1"},
		}
	})

	driver.nextTurn()
	driver.turn()
	require.Len(t, driver.pendingTasks, 2)
	var exportRequest ExportRequest
	driver.activityInput(ExportInstanceHistoryActivityName, &exportRequest)
	assert.Equal(t, "test-container", exportRequest.Destination.Container)
	assert.Equal(t, DefaultExportFormat(), exportRequest.Format)
	driver.completeActivities(func(_ string, index int) any {
		return ExportResult{InstanceID: []string{"i1", "i2"}[index], Success: true}
	})

	driver.nextTurn()
	driver.turn()
	var commit CommitCheckpointRequest
	driver.entityCallInput(commitCheckpointOperation, &commit)
	assert.Equal(t, int64(2), commit.ScannedInstances)
	assert.Equal(t, int64(2), commit.ExportedInstances)
	require.NotNil(t, commit.Checkpoint)
	assert.Equal(t, "cursor-1", commit.Checkpoint.LastInstanceKey)
	assert.Empty(t, commit.Failures)
	driver.completeEntityCall(commitCheckpointOperation, nil)

	// Second cycle: the job re-reads its state and the advanced cursor is used.
	driver.nextTurn()
	driver.turn()
	state := activeJobState(t, ExportModeBatch, "cursor-1")
	state.ScannedInstances, state.ExportedInstances = 2, 2
	driver.completeEntityCall(getOperation, state)

	driver.nextTurn()
	driver.turn()
	driver.activityInput(ListTerminalInstancesActivityName, &listRequest)
	assert.Equal(t, "cursor-1", listRequest.LastInstanceKey)
	driver.completeActivities(func(string, int) any { return InstancePage{} })

	driver.nextTurn()
	driver.turn()
	require.NotEmpty(t, driver.pendingEntities)
	driver.completeEntityCall(markAsCompletedOperation, nil)

	driver.nextTurn()
	driver.turn()
	require.NotNil(t, driver.completion)
	assert.Equal(t, protos.OrchestrationStatus_ORCHESTRATION_STATUS_COMPLETED, driver.completion.GetOrchestrationStatus())
}

// TestExportJobOrchestratorCompletesOnTheFinalPage covers a non-empty last page,
// which a task hub signals by omitting the continuation token. The job must
// commit its progress without resetting the cursor and then complete.
func TestExportJobOrchestratorCompletesOnTheFinalPage(t *testing.T) {
	driver := startExportOrchestrationDriver(t)
	driver.turn()
	driver.completeEntityCall(getOperation, activeJobState(t, ExportModeBatch, "cursor-1"))

	driver.nextTurn()
	driver.turn()
	// The final page carries instances but no next checkpoint.
	driver.completeActivities(func(string, int) any {
		return InstancePage{InstanceIDs: []string{"i1"}}
	})

	driver.nextTurn()
	driver.turn()
	driver.completeActivities(func(string, int) any {
		return ExportResult{InstanceID: "i1", Success: true}
	})

	driver.nextTurn()
	driver.turn()
	var commit CommitCheckpointRequest
	driver.entityCallInput(commitCheckpointOperation, &commit)
	assert.Equal(t, int64(1), commit.ScannedInstances)
	assert.Equal(t, int64(1), commit.ExportedInstances)
	assert.Nil(t, commit.Checkpoint, "the backend cursor must remain opaque")
	assert.Empty(t, commit.Failures)
	driver.completeEntityCall(commitCheckpointOperation, nil)

	// The job completes without listing again.
	driver.nextTurn()
	driver.turn()
	assert.Empty(t, driver.pendingTasks)
	driver.completeEntityCall(markAsCompletedOperation, nil)

	driver.nextTurn()
	driver.turn()
	require.NotNil(t, driver.completion)
	assert.Equal(t, protos.OrchestrationStatus_ORCHESTRATION_STATUS_COMPLETED, driver.completion.GetOrchestrationStatus())
}

// TestExportJobOrchestratorAdvancesPastAnEmptyPage covers a task hub that
// returns an empty page while more pages remain, which a filtered scan can
// produce. The cursor must advance so a batch job does not complete early and a
// continuous job does not re-read the same page forever.
func TestExportJobOrchestratorAdvancesPastAnEmptyPage(t *testing.T) {
	for _, mode := range []ExportMode{ExportModeBatch, ExportModeContinuous} {
		t.Run(mode.String(), func(t *testing.T) {
			driver := startExportOrchestrationDriver(t)
			driver.turn()
			driver.completeEntityCall(getOperation, activeJobState(t, mode, "cursor-1"))

			driver.nextTurn()
			driver.turn()
			var listRequest ListTerminalInstancesRequest
			driver.activityInput(ListTerminalInstancesActivityName, &listRequest)
			assert.Equal(t, "cursor-1", listRequest.LastInstanceKey)
			driver.completeActivities(func(string, int) any {
				return InstancePage{NextCheckpoint: &ExportCheckpoint{LastInstanceKey: "cursor-2"}}
			})

			driver.nextTurn()
			driver.turn()
			// The empty page still moves the durable cursor forward.
			var commit CommitCheckpointRequest
			driver.entityCallInput(commitCheckpointOperation, &commit)
			require.NotNil(t, commit.Checkpoint)
			assert.Equal(t, "cursor-2", commit.Checkpoint.LastInstanceKey)
			assert.Zero(t, commit.ScannedInstances)
			assert.Zero(t, commit.ExportedInstances)
			assert.Empty(t, commit.Failures)
			assert.Empty(t, driver.pendingTimers, "an empty page with more pages must not idle")
			assert.Nil(t, driver.completion, "an empty page with more pages must not complete the job")
			driver.completeEntityCall(commitCheckpointOperation, nil)

			// The next cycle resumes from the advanced cursor.
			driver.nextTurn()
			driver.turn()
			driver.completeEntityCall(getOperation, activeJobState(t, mode, "cursor-2"))
			driver.nextTurn()
			driver.turn()
			driver.activityInput(ListTerminalInstancesActivityName, &listRequest)
			assert.Equal(t, "cursor-2", listRequest.LastInstanceKey)
		})
	}
}

// TestExportJobOrchestratorContinuousIdlesOnTheFinalPage keeps a continuous job
// running after it drains a non-empty last page.
func TestExportJobOrchestratorContinuousIdlesOnTheFinalPage(t *testing.T) {
	driver := startExportOrchestrationDriver(t)
	driver.turn()
	driver.completeEntityCall(getOperation, activeJobState(t, ExportModeContinuous, "cursor-1"))

	driver.nextTurn()
	driver.turn()
	driver.completeActivities(func(string, int) any {
		return InstancePage{InstanceIDs: []string{"i1"}}
	})

	driver.nextTurn()
	driver.turn()
	driver.completeActivities(func(string, int) any {
		return ExportResult{InstanceID: "i1", Success: true}
	})

	driver.nextTurn()
	driver.turn()
	driver.completeEntityCall(commitCheckpointOperation, nil)

	driver.nextTurn()
	driver.turn()
	require.Len(t, driver.pendingTimers, 1)
	assertDelay(t, continuousIdleDelay, driver.fireTimers()[0])
	assert.Nil(t, driver.completion)
}

// TestExportJobOrchestratorContinuousIdles covers a continuous job waiting on a
// durable timer instead of completing when a page comes back empty.
func TestExportJobOrchestratorContinuousIdles(t *testing.T) {
	driver := startExportOrchestrationDriver(t)

	driver.turn()
	driver.completeEntityCall(getOperation, activeJobState(t, ExportModeContinuous, ""))

	driver.nextTurn()
	driver.turn()
	driver.completeActivities(func(string, int) any { return InstancePage{} })

	driver.nextTurn()
	driver.turn()
	// The job idles instead of completing or marking itself completed.
	assert.Nil(t, driver.completion)
	assert.Empty(t, driver.pendingEntities)
	delays := driver.fireTimers()
	require.Len(t, delays, 1)
	assertDelay(t, continuousIdleDelay, delays[0])

	// After the idle delay it re-reads the job and lists again.
	driver.nextTurn()
	driver.turn()
	driver.completeEntityCall(getOperation, activeJobState(t, ExportModeContinuous, ""))
	driver.nextTurn()
	driver.turn()
	require.Len(t, driver.pendingTasks, 1)
	assert.Equal(t, ListTerminalInstancesActivityName, driver.pendingTasks[0].GetTaskScheduled().GetName())
}

// TestExportJobOrchestratorRetriesAndFailsBatch covers whole-page retry with
// exponential backoff, the checkpoint-free commit that implicitly fails the job,
// and the terminal error's failure detail.
func TestExportJobOrchestratorRetriesAndFailsBatch(t *testing.T) {
	driver := startExportOrchestrationDriver(t)

	driver.turn()
	driver.completeEntityCall(getOperation, activeJobState(t, ExportModeBatch, ""))

	driver.nextTurn()
	driver.turn()
	driver.completeActivities(func(string, int) any {
		return InstancePage{
			InstanceIDs:    []string{"i1", "i2"},
			NextCheckpoint: &ExportCheckpoint{LastInstanceKey: "cursor-1"},
		}
	})

	backoffs := []time.Duration{}
	for attempt := 1; attempt <= maxBatchRetryAttempts; attempt++ {
		driver.nextTurn()
		driver.turn()
		require.Len(t, driver.pendingTasks, 2, "attempt %d", attempt)
		driver.completeActivities(func(_ string, index int) any {
			if index == 0 {
				return ExportResult{InstanceID: "i1", Success: true}
			}
			return ExportResult{InstanceID: "i2", Success: false, Error: "upload rejected"}
		})
		if attempt == maxBatchRetryAttempts {
			break
		}
		driver.nextTurn()
		driver.turn()
		backoffs = append(backoffs, driver.fireTimers()...)
	}
	require.Len(t, backoffs, 2)
	assertDelay(t, minBatchRetryBackoff, backoffs[0])
	assertDelay(t, 2*minBatchRetryBackoff, backoffs[1])

	driver.nextTurn()
	driver.turn()
	var commit CommitCheckpointRequest
	driver.entityCallInput(commitCheckpointOperation, &commit)
	// The cursor must not move so the same page can be retried after a fix.
	assert.Nil(t, commit.Checkpoint)
	assert.Zero(t, commit.ScannedInstances)
	assert.Zero(t, commit.ExportedInstances)
	require.Len(t, commit.Failures, 1)
	assert.Equal(t, "i2", commit.Failures[0].InstanceID)
	assert.Equal(t, "upload rejected", commit.Failures[0].Reason)
	assert.Equal(t, maxBatchRetryAttempts, commit.Failures[0].AttemptCount)
	assert.False(t, commit.Failures[0].LastAttempt.IsZero())
	driver.completeEntityCall(commitCheckpointOperation, nil)

	// The orchestration then tries to mark the job failed and fails itself.
	driver.nextTurn()
	driver.turn()
	var failure MarkAsFailedRequest
	driver.entityCallInput(markAsFailedOperation, &failure)
	assert.Contains(t, failure.Error, "batch export failed after 3 retry attempts")
	assert.Contains(t, failure.Error, "InstanceId: i2, Reason: upload rejected")
	driver.completeEntityCall(markAsFailedOperation, nil)

	driver.nextTurn()
	driver.turn()
	require.NotNil(t, driver.completion)
	assert.Equal(t, protos.OrchestrationStatus_ORCHESTRATION_STATUS_FAILED, driver.completion.GetOrchestrationStatus())
	assert.Contains(t, driver.completion.GetFailureDetails().GetErrorMessage(), "batch export failed")
}

// TestExportJobOrchestratorTreatsActivityFailureAsInstanceFailure keeps one
// instance whose export activity exhausted its own retries from aborting the
// page, so every failing instance in the page is reported.
func TestExportJobOrchestratorTreatsActivityFailureAsInstanceFailure(t *testing.T) {
	driver := startExportOrchestrationDriver(t)
	driver.turn()
	driver.completeEntityCall(getOperation, activeJobState(t, ExportModeBatch, ""))

	driver.nextTurn()
	driver.turn()
	driver.completeActivities(func(string, int) any {
		return InstancePage{
			InstanceIDs:    []string{"i1", "i2"},
			NextCheckpoint: &ExportCheckpoint{LastInstanceKey: "cursor-1"},
		}
	})

	driver.nextTurn()
	driver.turn()
	require.Len(t, driver.pendingTasks, 2)
	// The first instance succeeds; the second activity fails outright.
	success, err := json.Marshal(ExportResult{InstanceID: "i1", Success: true})
	require.NoError(t, err)
	driver.newEvents = append(driver.newEvents,
		helpers.NewTaskCompletedEvent(driver.pendingTasks[0].GetEventId(), wrapperspb.String(string(success))),
		helpers.NewTaskFailedEvent(driver.pendingTasks[1].GetEventId(), &protos.TaskFailureDetails{
			ErrorType:      "Contoso.Boom",
			ErrorMessage:   "activity exhausted its retries",
			IsNonRetriable: true,
		}))

	driver.nextTurn()
	driver.turn()
	// The page is retried rather than abandoned, which proves the failure was
	// collected instead of propagated.
	require.Len(t, driver.pendingTimers, 1)
}

// TestExportJobOrchestratorStopsWhenTheJobIsNoLongerActive covers a job deleted
// or failed underneath a running orchestration.
func TestExportJobOrchestratorStopsWhenTheJobIsNoLongerActive(t *testing.T) {
	tests := []struct {
		name  string
		state any
	}{
		{"failed by a checkpoint commit", ExportJobState{Status: ExportJobStatusFailed, Config: &ExportJobConfiguration{}, RunToken: "run-a"}},
		{"completed", ExportJobState{Status: ExportJobStatusCompleted, Config: &ExportJobConfiguration{}, RunToken: "run-a"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			driver := startExportOrchestrationDriver(t)
			driver.turn()
			driver.completeEntityCall(getOperation, test.state)

			driver.nextTurn()
			driver.turn()
			require.NotNil(t, driver.completion)
			assert.Equal(t,
				protos.OrchestrationStatus_ORCHESTRATION_STATUS_COMPLETED,
				driver.completion.GetOrchestrationStatus())
			// A stopped orchestration must not touch the job again.
			assert.Empty(t, driver.pendingEntities)
			assert.Empty(t, driver.pendingTasks)
		})
	}
}

// TestExportJobOrchestratorFailsWhenTheJobIsMissing covers a first cycle that
// cannot find the job at all.
func TestExportJobOrchestratorFailsWhenTheJobIsMissing(t *testing.T) {
	driver := startExportOrchestrationDriver(t)
	driver.turn()
	driver.completeEntityCall(getOperation, nil)

	driver.nextTurn()
	driver.turn()
	var failure MarkAsFailedRequest
	driver.entityCallInput(markAsFailedOperation, &failure)
	assert.Contains(t, failure.Error, "not found or has no configuration")
	driver.completeEntityCall(markAsFailedOperation, nil)

	driver.nextTurn()
	driver.turn()
	require.NotNil(t, driver.completion)
	assert.Equal(t, protos.OrchestrationStatus_ORCHESTRATION_STATUS_FAILED, driver.completion.GetOrchestrationStatus())
}

// TestExportJobOrchestratorMarkAsFailedRejectionDoesNotMaskTheCause covers the
// interplay between a checkpoint-driven implicit failure and the orchestration's
// own best-effort MarkAsFailed.
func TestExportJobOrchestratorMarkAsFailedRejectionDoesNotMaskTheCause(t *testing.T) {
	driver := startExportOrchestrationDriver(t)
	driver.turn()
	driver.completeEntityCall(getOperation, nil)

	driver.nextTurn()
	driver.turn()
	driver.failEntityCall(markAsFailedOperation, string(invalidTransitionErrorType), "job is already failed")

	driver.nextTurn()
	driver.turn()
	require.NotNil(t, driver.completion)
	assert.Equal(t, protos.OrchestrationStatus_ORCHESTRATION_STATUS_FAILED, driver.completion.GetOrchestrationStatus())
	assert.Contains(t, driver.completion.GetFailureDetails().GetErrorMessage(), "not found or has no configuration")
	assert.NotContains(t, driver.completion.GetFailureDetails().GetErrorMessage(), "already failed")
}

// TestExportJobOrchestratorContinuesAsNew keeps orchestration history bounded
// for long-running continuous jobs.
func TestExportJobOrchestratorContinuesAsNew(t *testing.T) {
	driver := startExportOrchestrationDriver(t, ExportJobRunRequest{
		JobEntityID: EntityID("job-1"),
		RunToken:    "run-a",
	})
	for cycle := 1; cycle <= continueAsNewFrequency; cycle++ {
		if cycle > 1 {
			driver.nextTurn()
		}
		driver.turn()
		driver.completeEntityCall(getOperation, activeJobState(t, ExportModeContinuous, "", "run-a"))

		driver.nextTurn()
		driver.turn()
		driver.completeActivities(func(string, int) any { return InstancePage{} })

		driver.nextTurn()
		driver.turn()
		require.Len(t, driver.pendingTimers, 1, "cycle %d", cycle)
		driver.fireTimers()
	}

	driver.nextTurn()
	driver.turn()
	assert.True(t, driver.continuedAsNew)
	require.NotNil(t, driver.completion)
	assert.Equal(t,
		protos.OrchestrationStatus_ORCHESTRATION_STATUS_CONTINUED_AS_NEW,
		driver.completion.GetOrchestrationStatus())

	var request ExportJobRunRequest
	require.NoError(t, json.Unmarshal([]byte(driver.completion.GetResult().GetValue()), &request))
	assert.Equal(t, EntityID("job-1"), request.JobEntityID)
	// The cycle counter resets so the next execution gets a full budget.
	assert.Zero(t, request.ProcessedCycles)
	// The run generation carries forward, so the continued execution keeps
	// fencing its mutations to the job it was started for.
	assert.Equal(t, "run-a", request.RunToken)
	// The continued execution is no longer the first one, so a job that
	// disappears underneath it is not an error.
	assert.True(t, request.ContinuedExecution)
}

// TestExportJobOrchestratorContinuedExecutionToleratesAMissingJob covers the
// explicit first-execution flag: after a ContinueAsNew the cycle counter resets,
// so a job deleted underneath the run must still stop quietly rather than fail.
func TestExportJobOrchestratorContinuedExecutionToleratesAMissingJob(t *testing.T) {
	driver := startExportOrchestrationDriver(t, ExportJobRunRequest{
		JobEntityID:        EntityID("job-1"),
		RunToken:           "run-a",
		ContinuedExecution: true,
	})
	driver.turn()
	// The very first cycle of this execution finds nothing, which for a
	// continued execution means the job was deleted while it ran.
	driver.completeEntityCall(getOperation, nil)

	driver.nextTurn()
	driver.turn()
	require.NotNil(t, driver.completion)
	assert.Equal(t,
		protos.OrchestrationStatus_ORCHESTRATION_STATUS_COMPLETED,
		driver.completion.GetOrchestrationStatus())
	// A deleted job must not be marked failed, which would resurrect it.
	assert.Empty(t, driver.pendingEntities)
}

// TestExportJobOrchestratorStopsWhenTheJobWasRecreated covers run fencing from
// the orchestration side: a run left over from a deleted-and-recreated job sees
// a different generation token and stops without touching the new job.
func TestExportJobOrchestratorStopsWhenTheJobWasRecreated(t *testing.T) {
	driver := startExportOrchestrationDriver(t, ExportJobRunRequest{
		JobEntityID: EntityID("job-1"),
		RunToken:    "run-a",
	})
	driver.turn()
	driver.completeEntityCall(getOperation, activeJobState(t, ExportModeBatch, "", "run-b"))

	driver.nextTurn()
	driver.turn()
	require.NotNil(t, driver.completion)
	assert.Equal(t,
		protos.OrchestrationStatus_ORCHESTRATION_STATUS_COMPLETED,
		driver.completion.GetOrchestrationStatus())
	// The stale run must neither list, checkpoint, complete, nor fail the new
	// generation of the job.
	assert.Empty(t, driver.pendingEntities)
	assert.Empty(t, driver.pendingTasks)
}

// TestExportJobOrchestratorFencesItsMutations proves every entity mutation the
// run performs carries the generation token it was started for.
func TestExportJobOrchestratorFencesItsMutations(t *testing.T) {
	driver := startExportOrchestrationDriver(t, ExportJobRunRequest{
		JobEntityID: EntityID("job-1"),
		RunToken:    "run-a",
	})
	driver.turn()
	driver.completeEntityCall(getOperation, activeJobState(t, ExportModeBatch, "", "run-a"))

	driver.nextTurn()
	driver.turn()
	driver.completeActivities(func(string, int) any {
		return InstancePage{InstanceIDs: []string{"i1"}}
	})

	driver.nextTurn()
	driver.turn()
	driver.completeActivities(func(string, int) any {
		return ExportResult{InstanceID: "i1", Success: true}
	})

	driver.nextTurn()
	driver.turn()
	var commit CommitCheckpointRequest
	driver.entityCallInput(commitCheckpointOperation, &commit)
	assert.Equal(t, "run-a", commit.RunToken)
	driver.completeEntityCall(commitCheckpointOperation, nil)

	driver.nextTurn()
	driver.turn()
	var completion MarkAsCompletedRequest
	driver.entityCallInput(markAsCompletedOperation, &completion)
	assert.Equal(t, "run-a", completion.RunToken)
}

func TestExportJobOrchestratorRequiresMatchingNonemptyTokens(t *testing.T) {
	for _, tokens := range []struct {
		name    string
		request string
		state   string
	}{
		{"missing request token", "", "run-a"},
		{"missing state token", "run-a", ""},
		{"both tokens missing", "", ""},
		{"different tokens", "run-a", "run-b"},
	} {
		t.Run(tokens.name, func(t *testing.T) {
			driver := startExportOrchestrationDriver(t, ExportJobRunRequest{
				JobEntityID: EntityID("job-1"),
				RunToken:    tokens.request,
			})
			driver.turn()
			driver.completeEntityCall(getOperation, activeJobState(t, ExportModeBatch, "", tokens.state))
			driver.nextTurn()
			driver.turn()
			require.NotNil(t, driver.completion)
			assert.Equal(t, protos.OrchestrationStatus_ORCHESTRATION_STATUS_COMPLETED,
				driver.completion.GetOrchestrationStatus())
			assert.Empty(t, driver.pendingEntities)
			assert.Empty(t, driver.pendingTasks)
			assert.Empty(t, driver.pendingTimers)
		})
	}
}

// TestExportJobOrchestratorFailureCarriesTheRunToken keeps a failing run from
// failing a newer generation of the same job.
func TestExportJobOrchestratorFailureCarriesTheRunToken(t *testing.T) {
	driver := startExportOrchestrationDriver(t, ExportJobRunRequest{
		JobEntityID: EntityID("job-1"),
		RunToken:    "run-a",
	})
	driver.turn()
	driver.completeEntityCall(getOperation, nil)

	driver.nextTurn()
	driver.turn()
	var failure MarkAsFailedRequest
	driver.entityCallInput(markAsFailedOperation, &failure)
	assert.Equal(t, "run-a", failure.RunToken)
	assert.Contains(t, failure.Error, "not found or has no configuration")
}

// TestExportJobOrchestratorFansOutWithinMaxParallelExports keeps a large page
// from scheduling unbounded concurrent activities.
func TestExportJobOrchestratorFansOutWithinMaxParallelExports(t *testing.T) {
	driver := startExportOrchestrationDriver(t)
	driver.turn()
	state := activeJobState(t, ExportModeBatch, "")
	state.Config.MaxParallelExports = 3
	driver.completeEntityCall(getOperation, state)

	instances := []string{"i1", "i2", "i3", "i4", "i5"}
	driver.nextTurn()
	driver.turn()
	driver.completeActivities(func(string, int) any {
		return InstancePage{InstanceIDs: instances, NextCheckpoint: &ExportCheckpoint{}}
	})

	driver.nextTurn()
	driver.turn()
	require.Len(t, driver.pendingTasks, 3)
	driver.completeActivities(func(_ string, index int) any {
		return ExportResult{InstanceID: instances[index], Success: true}
	})

	driver.nextTurn()
	driver.turn()
	require.Len(t, driver.pendingTasks, 2)
}

// TestExecuteExportJobOperationOrchestrator ports the upstream
// ExecuteExportJobOperationOrchestratorTests matrix.
func TestExecuteExportJobOperationOrchestrator(t *testing.T) {
	newDriver := func(t *testing.T, request ExportJobOperationRequest) *orchestrationDriver {
		return newOrchestrationDriver(
			t,
			newExportRegistry(t),
			ExecuteExportJobOperationOrchestratorName,
			"operation-instance",
			request,
		)
	}

	t.Run("forwards the operation and input to the entity", func(t *testing.T) {
		options := batchOptions("job-1")
		driver := newDriver(t, ExportJobOperationRequest{
			EntityID:      EntityID("job-1"),
			OperationName: createOperation,
			Input:         options,
		})
		driver.turn()
		_, target := driver.pendingEntityMessage(createOperation)
		assert.Equal(t, EntityID("job-1").String(), target)
		var forwarded JobCreationOptions
		driver.entityCallInput(createOperation, &forwarded)
		assert.Equal(t, options.JobID, forwarded.JobID)
		assert.Equal(t, options.Mode, forwarded.Mode)

		driver.completeEntityCall(createOperation, ExportJobState{Status: ExportJobStatusActive})
		driver.nextTurn()
		driver.turn()
		require.NotNil(t, driver.completion)
		assert.Equal(t,
			protos.OrchestrationStatus_ORCHESTRATION_STATUS_COMPLETED,
			driver.completion.GetOrchestrationStatus())
		assert.Contains(t, driver.completion.GetResult().GetValue(), `"Status":1`)
	})

	t.Run("forwards an operation without input", func(t *testing.T) {
		driver := newDriver(t, ExportJobOperationRequest{
			EntityID:      EntityID("job-1"),
			OperationName: getOperation,
		})
		driver.turn()
		called := driver.pendingEntityCall(getOperation)
		assert.Empty(t, called.GetInput().GetValue())

		driver.completeEntityCall(getOperation, nil)
		driver.nextTurn()
		driver.turn()
		require.NotNil(t, driver.completion)
		assert.Equal(t,
			protos.OrchestrationStatus_ORCHESTRATION_STATUS_COMPLETED,
			driver.completion.GetOrchestrationStatus())
	})

	t.Run("preserves typed delete results for case-insensitive operation names", func(t *testing.T) {
		converter := lifecycleGobConverter{}
		for _, operation := range []string{deleteOperation, "dElEtE"} {
			for _, instanceID := range []string{"ExportJob-job-1-run-a", ""} {
				driver := newOrchestrationDriver(t, newExportRegistry(t),
					ExecuteExportJobOperationOrchestratorName, "operation-instance",
					ExportJobOperationRequest{EntityID: EntityID("job-1"), OperationName: operation},
					converter)
				driver.turn()
				driver.completeEntityCall(operation, instanceID)
				driver.nextTurn()
				driver.turn()
				require.NotNil(t, driver.completion)
				require.Equal(t, api.RUNTIME_STATUS_COMPLETED, driver.completion.GetOrchestrationStatus())
				var result string
				require.NoError(t, converter.Deserialize(driver.completion.GetResult().GetValue(), &result))
				assert.Equal(t, instanceID, result)
			}
		}
	})

	t.Run("propagates entity failures", func(t *testing.T) {
		driver := newDriver(t, ExportJobOperationRequest{
			EntityID:      EntityID("job-1"),
			OperationName: createOperation,
			Input:         batchOptions("job-1"),
		})
		driver.turn()
		driver.failEntityCall(createOperation, string(invalidTransitionErrorType), "cannot recreate an active job")

		driver.nextTurn()
		driver.turn()
		require.NotNil(t, driver.completion)
		assert.Equal(t,
			protos.OrchestrationStatus_ORCHESTRATION_STATUS_FAILED,
			driver.completion.GetOrchestrationStatus())
		assert.Contains(t, driver.completion.GetFailureDetails().GetErrorMessage(), "cannot recreate an active job")
	})

	t.Run("requires an operation name", func(t *testing.T) {
		driver := newDriver(t, ExportJobOperationRequest{EntityID: EntityID("job-1")})
		driver.turn()
		require.NotNil(t, driver.completion)
		assert.Equal(t,
			protos.OrchestrationStatus_ORCHESTRATION_STATUS_FAILED,
			driver.completion.GetOrchestrationStatus())
		assert.Equal(t, string(validationErrorType), driver.completion.GetFailureDetails().GetErrorType())
	})
}
