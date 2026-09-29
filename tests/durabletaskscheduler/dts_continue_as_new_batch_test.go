package durabletaskscheduler_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/microsoft/durabletask-go/api"
	durabletaskclient "github.com/microsoft/durabletask-go/client"
	"github.com/microsoft/durabletask-go/durabletaskscheduler"
	"github.com/microsoft/durabletask-go/internal/protos"
	"github.com/microsoft/durabletask-go/task"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

// This experiment deliberately fails admission if DTS redelivers the timer
// separately instead of combining it with queued external events.
func TestDTSContinueAsNewSameNewEventsBatch(t *testing.T) {
	if os.Getenv("DTS_CAN_BATCH_TEST") != "1" {
		t.Skip("set DTS_CAN_BATCH_TEST=1 to run the service batching experiment")
	}
	require.True(t, os.Getenv("DTS_CONNECTION_STRING") != "" || os.Getenv("DTS_EMULATOR_ENDPOINT") != "",
		"the batching experiment requires an explicit DTS target")
	directory := os.Getenv(continueAsNewArtifactDirEnv)
	require.NotEmpty(t, directory, "the batching experiment requires an evidence directory")
	info, err := os.Stat(directory)
	require.NoError(t, err)
	require.True(t, info.IsDir())

	options := emulatorOptions(t)
	runID := uuid.NewString()
	instanceID := api.InstanceID("go-continue-as-new-batch-" + runID)
	name := "DTSContinueAsNewBatch-" + runID[:8]
	registry := task.NewTaskRegistry()
	require.NoError(t, registry.AddOrchestratorN(name, continueAsNewEventsOrchestrator))
	events := make([]raisedEvent, 0, 6)
	expected := make([]string, 0, 6)
	for index, eventName := range []string{"work", "other", "work", "other", "work", "work"} {
		event := raisedEvent{name: eventName, payload: fmt.Sprintf("%s-%d", runID, index+1)}
		events = append(events, event)
		expected = append(expected, event.name+"="+event.payload)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	logger := api.DefaultLogger()
	client, err := durabletaskscheduler.NewClient(ctx, options, logger)
	require.NoError(t, err)
	recorder := newWorkItemRecorder(string(instanceID))
	// This profile holds the successful RPC's return, not its invocation.
	recorder.releaseFirstCompletion()
	control := newContinueAsNewBatchControl(string(instanceID))
	outcome := "setup_failed"
	var workers []*durabletaskclient.TaskHubGrpcWorker
	t.Cleanup(func() {
		control.releaseCompletion()
		cleanupCtx, cancelCleanup := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancelCleanup()
		cleanupContinueAsNewInstance(t, cleanupCtx, client, instanceID)
		for _, worker := range workers {
			shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), 10*time.Second)
			shutdownErr := worker.Shutdown(shutdownCtx)
			cancelShutdown()
			if shutdownErr != nil {
				t.Errorf("failed to stop experiment worker: %v", shutdownErr)
			}
		}
		if closeErr := client.Close(); closeErr != nil {
			t.Errorf("failed to close experiment client: %v", closeErr)
		}
		recorder.writeEvidence(t)
		control.writeEvidence(t, outcome)
	})
	startWorker := func(label string, gate bool) *durabletaskclient.TaskHubGrpcWorker {
		workerOptions := *options
		workerOptions.UnaryInterceptors = append(slices.Clone(options.UnaryInterceptors), recorder.unaryInterceptor())
		if gate {
			workerOptions.UnaryInterceptors = append(workerOptions.UnaryInterceptors, control.interceptor())
		}
		workerOptions.StreamInterceptors = append(slices.Clone(options.StreamInterceptors), recorder.streamInterceptor(label))
		worker, workerErr := durabletaskscheduler.NewWorker(&workerOptions, registry, logger,
			durabletaskclient.WithAutoWorkItemFilters(),
			durabletaskclient.WithMaxConcurrentOrchestrationWorkItems(1),
			durabletaskclient.WithWorkerRPCTimeout(time.Minute),
			durabletaskclient.WithWorkerSilentDisconnectTimeout(time.Minute),
		)
		require.NoError(t, workerErr)
		// Cleanup still needs intake to process termination after a profile timeout.
		require.NoError(t, worker.Start(context.Background()))
		workers = append(workers, worker)
		control.record(label+"_started", "")
		return worker
	}

	workerA := startWorker("worker-a", true)
	_, err = client.ScheduleNewOrchestration(ctx, name, api.WithInstanceID(instanceID),
		api.WithInput(continueAsNewEventsInput{Expected: len(events)}))
	require.NoError(t, err)
	stepCtx, cancelStep := context.WithTimeout(ctx, 30*time.Second)
	defer cancelStep()
	var firstResponse *protos.OrchestratorResponse
	select {
	case firstResponse = <-control.acknowledged:
	case <-stepCtx.Done():
		t.Fatalf("INCONCLUSIVE: initial completion was not acknowledged: %v", stepCtx.Err())
	}
	require.Len(t, firstResponse.Actions, 1)
	require.NotNil(t, firstResponse.Actions[0].GetCreateTimer())
	timerID := firstResponse.Actions[0].GetId()
	initialItem, err := recorder.waitForWorkerItem(stepCtx, "worker-a", func(item *observedWorkItem) bool {
		return executionIDOf(item.history()) != ""
	})
	require.NoError(t, err)
	executionID := executionIDOf(initialItem.history())
	require.NotEmpty(t, executionID)

	timerItem, err := recorder.waitForWorkerItem(stepCtx, "worker-a", func(item *observedWorkItem) bool {
		return slices.ContainsFunc(item.Request.NewEvents, isTimerFired)
	})
	require.NoError(t, err, "INCONCLUSIVE: no unexecuted timer work item was observed")
	require.False(t, timerItem.Done)
	require.NotEmpty(t, timerItem.token)
	require.Equal(t, executionID, timerItem.Request.GetExecutionId().GetValue())
	for _, event := range timerItem.Request.NewEvents {
		if timer := event.GetTimerFired(); timer != nil {
			require.Equal(t, timerID, timer.GetTimerId())
			require.True(t, proto.Equal(firstResponse.Actions[0].GetCreateTimer().GetFireAt(), timer.GetFireAt()))
		}
	}
	control.expectAbandonment(timerItem.token)
	control.record("timer_observed_before_execution", executionID)

	for _, event := range events {
		raiseErr := client.RaiseEvent(stepCtx, instanceID, event.name, api.WithEventPayload(event.payload))
		require.NoError(t, raiseErr, "INCONCLUSIVE: a signal was not unambiguously accepted")
		control.record("event_acknowledged", event.name+"="+event.payload)
	}
	// B opens its work-item stream before A releases the outstanding lease.
	startWorker("worker-b", false)
	shutdown := make(chan error, 1)
	control.record("worker-a_shutdown_requested", "")
	go func() {
		shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancelShutdown()
		shutdown <- workerA.Shutdown(shutdownCtx)
	}()
	select {
	case <-control.abandoned:
	case <-stepCtx.Done():
		t.Fatalf("INCONCLUSIVE: the timer work item was not successfully abandoned: %v", stepCtx.Err())
	}
	require.NoError(t, control.gateFailure(), "INCONCLUSIVE: the completion-return barrier expired")
	control.releaseCompletion()
	select {
	case shutdownErr := <-shutdown:
		require.NoError(t, shutdownErr)
	case <-stepCtx.Done():
		t.Fatalf("INCONCLUSIVE: worker A did not finish draining: %v", stepCtx.Err())
	}

	redelivered, err := recorder.waitForWorkerItem(stepCtx, "worker-b", func(*observedWorkItem) bool { return true })
	require.NoError(t, err, "INCONCLUSIVE: worker B received no work item")
	if err := validateContinueAsNewBatch(redelivered.Request, executionID, timerID, events); err != nil {
		outcome = "batch_not_exercised"
		control.record("batch_not_exercised", err.Error())
		t.Fatalf("BATCH_NOT_EXERCISED: %v", err)
	}
	control.record("same_new_events_batch_observed", executionID)
	outcome = "completion_failed"
	metadata, err := client.WaitForOrchestrationCompletion(ctx, instanceID, api.WithFetchPayloads(true))
	require.NoError(t, err)
	require.Equal(t, api.RUNTIME_STATUS_COMPLETED, metadata.RuntimeStatus)
	admitted := recorder.requireRegressionCondition(t, events[0].payload)
	require.Equal(t, "worker-b", admitted.Worker)
	require.Equal(t, redelivered.token, admitted.token, "the admitted request must be the one that continued as new")
	require.Equal(t, executionID, executionIDOf(admitted.history()))
	if admitted.Request.GetRequiresHistoryStreaming() {
		require.NoError(t, validateUnprocessedBatchHistory(admitted.Streamed))
	}
	for _, event := range admitted.history() {
		require.Nil(t, event.GetExecutionSuspended())
		require.Nil(t, event.GetExecutionResumed())
	}
	require.True(t, slices.ContainsFunc(admitted.history(), func(event *protos.HistoryEvent) bool {
		return event.GetTimerCreated() != nil && event.GetEventId() == timerID
	}))
	finalExecution := recorder.lastExecutionID()
	require.NotEmpty(t, finalExecution)
	require.NotEqual(t, executionID, finalExecution)
	var ledger continueAsNewEventsLedger
	require.NoError(t, metadata.ReadOutput(&ledger))
	carryover := carryoverSummaries(t, admitted.Response)
	outcome = "payload_mismatch"
	require.Equal(t, expected, ledger.Received,
		"generation 1 must receive every accepted event exactly once and in order; carryover was %v", carryover)
	require.False(t, ledger.DeadlineHit)
	require.Equal(t, expected, carryover)
	outcome = "passed"
}

func validateContinueAsNewBatch(
	request *protos.OrchestratorRequest,
	executionID string,
	timerID int32,
	expected []raisedEvent,
) error {
	if request.GetExecutionId().GetValue() != executionID {
		return errors.New("redelivery changed the execution ID")
	}
	if err := validateUnprocessedBatchHistory(request.GetPastEvents()); err != nil {
		return err
	}
	timerSeen := false
	received := make([]raisedEvent, 0, len(expected))
	for _, event := range request.GetNewEvents() {
		switch {
		case event.GetExecutionSuspended() != nil || event.GetExecutionResumed() != nil:
			return errors.New("suspension was used")
		case event.GetTimerFired() != nil:
			if timerSeen || event.GetTimerFired().GetTimerId() != timerID {
				return errors.New("unexpected timer in the redelivered batch")
			}
			timerSeen = true
		case event.GetEventRaised() != nil:
			if !timerSeen {
				return errors.New("an external event precedes the timer in NewEvents")
			}
			var payload string
			if err := json.Unmarshal([]byte(event.GetEventRaised().GetInput().GetValue()), &payload); err != nil {
				return fmt.Errorf("invalid event payload: %w", err)
			}
			received = append(received, raisedEvent{name: event.GetEventRaised().GetName(), payload: payload})
		}
	}
	if !timerSeen || !slices.Equal(expected, received) {
		return fmt.Errorf("NewEvents must contain the timer followed by all %d accepted events; observed %s",
			len(expected), summarizeHistory(request.GetNewEvents()))
	}
	return nil
}

func validateUnprocessedBatchHistory(past []*protos.HistoryEvent) error {
	for _, event := range past {
		if event.GetEventRaised() != nil || event.GetTimerFired() != nil ||
			event.GetExecutionSuspended() != nil || event.GetExecutionResumed() != nil {
			return errors.New("timer or external events were already committed, or suspension was used")
		}
	}
	return nil
}

func (r *workItemRecorder) waitForWorkerItem(
	ctx context.Context,
	worker string,
	matches func(*observedWorkItem) bool,
) (*observedWorkItem, error) {
	for {
		r.mu.Lock()
		for _, item := range r.items {
			if item.Worker != worker || !matches(item) {
				continue
			}
			snapshot := *item
			snapshot.Request = proto.Clone(item.Request).(*protos.OrchestratorRequest)
			snapshot.Streamed = make([]*protos.HistoryEvent, len(item.Streamed))
			for index, event := range item.Streamed {
				snapshot.Streamed[index] = proto.Clone(event).(*protos.HistoryEvent)
			}
			if item.Response != nil {
				snapshot.Response = proto.Clone(item.Response).(*protos.OrchestratorResponse)
			}
			r.mu.Unlock()
			return &snapshot, nil
		}
		changed := r.changed
		r.mu.Unlock()
		select {
		case <-changed:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

type batchControlObservation struct {
	Phase  string    `json:"phase"`
	At     time.Time `json:"at"`
	Detail string    `json:"detail,omitempty"`
}

type continueAsNewBatchControl struct {
	instanceID   string
	holding      atomic.Bool
	acknowledged chan *protos.OrchestratorResponse
	release      chan struct{}
	releaseOnce  sync.Once
	abandoned    chan struct{}
	abandonOnce  sync.Once

	mu           sync.Mutex
	timerToken   string
	gateErr      error
	observations []batchControlObservation
}

func newContinueAsNewBatchControl(instanceID string) *continueAsNewBatchControl {
	return &continueAsNewBatchControl{
		instanceID:   instanceID,
		acknowledged: make(chan *protos.OrchestratorResponse, 1),
		release:      make(chan struct{}),
		abandoned:    make(chan struct{}),
	}
}

func (c *continueAsNewBatchControl) releaseCompletion() {
	c.releaseOnce.Do(func() { close(c.release) })
}

func (c *continueAsNewBatchControl) expectAbandonment(token string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.timerToken = token
}

func (c *continueAsNewBatchControl) gateFailure() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.gateErr
}

func (c *continueAsNewBatchControl) record(phase, detail string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.observations = append(c.observations, batchControlObservation{Phase: phase, At: time.Now().UTC(), Detail: detail})
}

func (c *continueAsNewBatchControl) interceptor() grpc.UnaryClientInterceptor {
	return func(
		ctx context.Context,
		method string,
		request any,
		reply any,
		connection *grpc.ClientConn,
		invoker grpc.UnaryInvoker,
		callOptions ...grpc.CallOption,
	) error {
		err := invoker(ctx, method, request, reply, connection, callOptions...)
		if response, ok := request.(*protos.OrchestratorResponse); ok &&
			method == protos.TaskHubSidecarService_CompleteOrchestratorTask_FullMethodName &&
			response.GetInstanceId() == c.instanceID && err == nil && c.holding.CompareAndSwap(false, true) {
			c.record("initial_completion_acknowledged", "")
			snapshot := proto.Clone(response).(*protos.OrchestratorResponse)
			snapshot.CompletionToken = ""
			c.acknowledged <- snapshot
			deadline := time.NewTimer(30 * time.Second)
			defer deadline.Stop()
			var gateErr error
			select {
			case <-c.release:
			case <-ctx.Done():
				gateErr = ctx.Err()
			case <-deadline.C:
				gateErr = errors.New("the 30-second completion-return barrier expired")
			}
			c.mu.Lock()
			c.gateErr = gateErr
			c.mu.Unlock()
			if gateErr != nil {
				c.record("completion_gate_failed", gateErr.Error())
			}
			c.record("initial_completion_return_released", "")
		}
		if abandoned, ok := request.(*protos.AbandonOrchestrationTaskRequest); ok &&
			method == protos.TaskHubSidecarService_AbandonTaskOrchestratorWorkItem_FullMethodName {
			c.mu.Lock()
			matches := c.timerToken != "" && abandoned.GetCompletionToken() == c.timerToken
			c.mu.Unlock()
			if matches {
				if err != nil {
					c.record("timer_abandonment_failed", err.Error())
				} else {
					c.record("timer_abandonment_acknowledged", "")
					c.abandonOnce.Do(func() { close(c.abandoned) })
				}
			}
		}
		return err
	}
}

func (c *continueAsNewBatchControl) writeEvidence(t *testing.T, outcome string) {
	t.Helper()
	c.mu.Lock()
	defer c.mu.Unlock()
	content, err := json.MarshalIndent(struct {
		Test         string                    `json:"test"`
		Instance     string                    `json:"instance"`
		Outcome      string                    `json:"outcome"`
		Observations []batchControlObservation `json:"observations"`
	}{t.Name(), c.instanceID, outcome, c.observations}, "", "  ")
	if err == nil {
		name := strings.ReplaceAll(t.Name(), "/", "_") + ".control.json"
		err = os.WriteFile(filepath.Join(os.Getenv(continueAsNewArtifactDirEnv), name), content, 0o600)
	}
	if err != nil {
		t.Errorf("failed to write batch control evidence: %v", err)
	}
}

func TestContinueAsNewBatchAdmission(t *testing.T) {
	const timerID int32 = 7
	expected := []raisedEvent{{name: "work", payload: "1"}, {name: "other", payload: "2"}}
	valid := &protos.OrchestratorRequest{
		ExecutionId: wrapperspb.String("original-execution"),
		PastEvents: []*protos.HistoryEvent{{
			EventId:   timerID,
			EventType: &protos.HistoryEvent_TimerCreated{TimerCreated: &protos.TimerCreatedEvent{}},
		}},
		NewEvents: []*protos.HistoryEvent{{
			EventType: &protos.HistoryEvent_TimerFired{TimerFired: &protos.TimerFiredEvent{TimerId: timerID}},
		}},
	}
	for _, event := range expected {
		raw, err := json.Marshal(event.payload)
		require.NoError(t, err)
		valid.NewEvents = append(valid.NewEvents, &protos.HistoryEvent{
			EventType: &protos.HistoryEvent_EventRaised{EventRaised: &protos.EventRaisedEvent{
				Name: event.name, Input: wrapperspb.String(string(raw)),
			}},
		})
	}
	tests := []struct {
		name   string
		mutate func(*protos.OrchestratorRequest)
	}{
		{"timer committed in past", func(request *protos.OrchestratorRequest) {
			request.PastEvents = append(request.PastEvents, request.NewEvents[0])
			request.NewEvents = request.NewEvents[1:]
		}},
		{"timer repeated in past", func(request *protos.OrchestratorRequest) {
			request.PastEvents = append(request.PastEvents, request.NewEvents[0])
		}},
		{"trailing event in past", func(request *protos.OrchestratorRequest) {
			request.PastEvents = append(request.PastEvents, request.NewEvents[2])
			request.NewEvents = request.NewEvents[:2]
		}},
		{"event before timer", func(request *protos.OrchestratorRequest) {
			request.NewEvents[0], request.NewEvents[1] = request.NewEvents[1], request.NewEvents[0]
		}},
		{"timer alone", func(request *protos.OrchestratorRequest) {
			request.NewEvents = request.NewEvents[:1]
		}},
		{"different execution", func(request *protos.OrchestratorRequest) {
			request.ExecutionId = wrapperspb.String("new-execution")
		}},
		{"different timer", func(request *protos.OrchestratorRequest) {
			request.NewEvents[0].GetTimerFired().TimerId++
		}},
		{"duplicate event", func(request *protos.OrchestratorRequest) {
			request.NewEvents = append(request.NewEvents, request.NewEvents[1])
		}},
		{"reordered events", func(request *protos.OrchestratorRequest) {
			request.NewEvents[1], request.NewEvents[2] = request.NewEvents[2], request.NewEvents[1]
		}},
		{"suspension", func(request *protos.OrchestratorRequest) {
			request.NewEvents = append(request.NewEvents, &protos.HistoryEvent{
				EventType: &protos.HistoryEvent_ExecutionSuspended{ExecutionSuspended: &protos.ExecutionSuspendedEvent{}},
			})
		}},
	}
	require.NoError(t, validateContinueAsNewBatch(valid, "original-execution", timerID, expected))
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			request := proto.Clone(valid).(*protos.OrchestratorRequest)
			test.mutate(request)
			require.Error(t, validateContinueAsNewBatch(request, "original-execution", timerID, expected))
		})
	}
}

func TestContinueAsNewBatchControl(t *testing.T) {
	for _, cancelGate := range []bool{false, true} {
		t.Run(fmt.Sprintf("cancel=%t", cancelGate), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			control := newContinueAsNewBatchControl("instance")
			defer control.releaseCompletion()
			response := &protos.OrchestratorResponse{InstanceId: "instance", CompletionToken: "initial-token"}
			invoked := false
			done := make(chan error, 1)
			go func() {
				done <- control.interceptor()(ctx, protos.TaskHubSidecarService_CompleteOrchestratorTask_FullMethodName,
					response, nil, nil, func(context.Context, string, any, any, *grpc.ClientConn, ...grpc.CallOption) error {
						invoked = true
						return nil
					})
			}()
			select {
			case acknowledged := <-control.acknowledged:
				require.True(t, invoked)
				require.Empty(t, acknowledged.CompletionToken)
				require.Equal(t, "initial-token", response.CompletionToken)
			case <-ctx.Done():
				t.Fatal("initial completion acknowledgement was not observed")
			}
			select {
			case <-done:
				t.Fatal("the initial call returned before the gate was released")
			default:
			}
			if cancelGate {
				cancel()
			} else {
				control.expectAbandonment("timer-token")
				for _, callErr := range []error{errors.New("transient RPC error"), nil} {
					err := control.interceptor()(ctx, protos.TaskHubSidecarService_AbandonTaskOrchestratorWorkItem_FullMethodName,
						&protos.AbandonOrchestrationTaskRequest{CompletionToken: "timer-token"}, nil, nil,
						func(context.Context, string, any, any, *grpc.ClientConn, ...grpc.CallOption) error { return callErr })
					require.ErrorIs(t, err, callErr)
					if callErr != nil {
						select {
						case <-control.abandoned:
							t.Fatal("a failed abandonment was reported as acknowledged")
						default:
						}
					}
				}
				select {
				case <-control.abandoned:
				case <-ctx.Done():
					t.Fatal("successful abandonment was not observed")
				}
				control.releaseCompletion()
			}
			require.NoError(t, <-done, "return the real successful completion result even if the test gate is canceled")
			if cancelGate {
				require.ErrorIs(t, control.gateFailure(), context.Canceled)
			} else {
				require.NoError(t, control.gateFailure())
			}
		})
	}
}
