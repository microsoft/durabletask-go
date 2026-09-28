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
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

const (
	continueAsNewTimer              = time.Second
	continueAsNewCollectionDeadline = 10 * time.Second
	continueAsNewArtifactDirEnv     = "DTS_CAN_ARTIFACT_DIR"
)

type continueAsNewEventsInput struct {
	Generation int
	Expected   int
}

type continueAsNewEventsLedger struct {
	Received    []string
	DeadlineHit bool
}

// continueAsNewEventsOrchestrator leaves a "work" wait pending in generation 0 and
// continues as new when its timer wins. Generation 1 records the events it
// receives until it has all of them or its durable deadline fires.
func continueAsNewEventsOrchestrator(ctx *task.OrchestrationContext) (any, error) {
	var input continueAsNewEventsInput
	if err := ctx.GetInput(&input); err != nil {
		return nil, err
	}
	if input.Generation == 0 {
		pending := ctx.WaitForSingleEvent("work", -1)
		timer := ctx.CreateTimer(continueAsNewTimer)
		if ctx.WhenAny(pending, timer) != timer {
			return nil, errors.New("a work event won before the timer")
		}
		ctx.ContinueAsNew(
			continueAsNewEventsInput{Generation: 1, Expected: input.Expected},
			task.WithKeepUnprocessedEvents(),
		)
		return nil, nil
	}

	work := task.NewEventChannel[string](ctx, "work")
	other := task.NewEventChannel[string](ctx, "other")
	deadline := ctx.CreateTimer(continueAsNewCollectionDeadline)
	var ledger continueAsNewEventsLedger
	for len(ledger.Received) < input.Expected && !ledger.DeadlineHit {
		ctx.Select(
			task.OnEvent(work, func(value string) { ledger.Received = append(ledger.Received, "work="+value) }),
			task.OnEvent(other, func(value string) { ledger.Received = append(ledger.Received, "other="+value) }),
			task.OnTask(deadline, func(task.Task) { ledger.DeadlineHit = true }),
		)
	}
	return ledger, nil
}

// TestDTSContinueAsNewKeepsEventsRaisedAfterTimerWins is the service-level
// regression for external events that follow a winning timer at a
// ContinueAsNew boundary while a WaitForSingleEvent loser is still pending.
//
// The service cannot be forced to put the timer and later events in one batch.
// Instead, the test suspends the instance before the timer exists. The timer
// and the events raised afterward are then buffered in history, and the resume
// work item replays them in order after generation 0 finalizes. The test proves
// that condition from unmodified work items before it checks the outcome.
func TestDTSContinueAsNewKeepsEventsRaisedAfterTimerWins(t *testing.T) {
	for _, restartWorker := range []bool{false, true} {
		name := "same-worker"
		if restartWorker {
			name = "fresh-worker-replay"
		}
		t.Run(name, func(t *testing.T) {
			runContinueAsNewEventsScenario(t, restartWorker)
		})
	}
}

func runContinueAsNewEventsScenario(t *testing.T, restartWorker bool) {
	options := emulatorOptions(t)
	runID := uuid.NewString()
	instanceID := api.InstanceID("go-continue-as-new-events-" + runID)
	orchestratorName := "DTSContinueAsNewEvents-" + runID[:8]
	registry := task.NewTaskRegistry()
	require.NoError(t, registry.AddOrchestratorN(orchestratorName, continueAsNewEventsOrchestrator))

	events := make([]raisedEvent, 0, 6)
	for index, name := range []string{"work", "other", "work", "other", "work", "work"} {
		events = append(events, raisedEvent{name: name, payload: fmt.Sprintf("%s-%d", runID, index+1)})
	}
	expected := make([]string, 0, len(events))
	for _, event := range events {
		expected = append(expected, event.name+"="+event.payload)
	}

	recorder := newWorkItemRecorder(string(instanceID))
	logger := api.DefaultLogger()
	client, err := durabletaskscheduler.NewClient(context.Background(), options, logger)
	require.NoError(t, err)

	var workers []*durabletaskclient.TaskHubGrpcWorker
	startWorker := func(label string) *durabletaskclient.TaskHubGrpcWorker {
		workerOptions := *options
		workerOptions.UnaryInterceptors = append(
			slices.Clone(options.UnaryInterceptors),
			recorder.unaryInterceptor(),
		)
		workerOptions.StreamInterceptors = append(
			slices.Clone(options.StreamInterceptors),
			recorder.streamInterceptor(label),
		)
		worker, workerErr := durabletaskscheduler.NewWorker(
			&workerOptions,
			registry,
			logger,
			durabletaskclient.WithAutoWorkItemFilters(),
		)
		require.NoError(t, workerErr)
		require.NoError(t, worker.Start(context.Background()))
		workers = append(workers, worker)
		return worker
	}
	stopWorker := func(worker *durabletaskclient.TaskHubGrpcWorker) error {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		return worker.Shutdown(shutdownCtx)
	}
	t.Cleanup(func() {
		// A held completion keeps a worker drain waiting, so release it first.
		recorder.releaseFirstCompletion()
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		cleanupContinueAsNewInstance(t, cleanupCtx, client, instanceID)
		for _, worker := range workers {
			if err := stopWorker(worker); err != nil {
				t.Errorf("failed to stop worker: %v", err)
			}
		}
		if err := client.Close(); err != nil {
			t.Errorf("failed to close client: %v", err)
		}
		recorder.writeEvidence(t)
	})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	worker := startWorker("worker-a")
	_, err = client.ScheduleNewOrchestration(
		ctx,
		orchestratorName,
		api.WithInstanceID(instanceID),
		api.WithInput(continueAsNewEventsInput{Expected: len(events)}),
	)
	require.NoError(t, err)

	// Suspending while the first completion is held orders the suspension
	// before the timer, which only exists once that completion commits.
	stepCtx, cancelStep := context.WithTimeout(ctx, 30*time.Second)
	defer cancelStep()
	require.NoError(t, recorder.waitForHeldCompletion(stepCtx))
	require.NoError(
		t,
		client.SuspendOrchestration(ctx, instanceID, "buffer the timer and trailing events"),
		"INCONCLUSIVE: the service rejected suspension while the first turn was in flight",
	)
	recorder.releaseFirstCompletion()
	require.NoError(t, recorder.waitForSuspendedTimer(stepCtx))

	for _, event := range events {
		require.NoError(t, client.RaiseEvent(ctx, instanceID, event.name, api.WithEventPayload(event.payload)))
	}
	if restartWorker {
		require.NoError(t, stopWorker(worker))
		startWorker("worker-b")
	}
	require.NoError(t, client.ResumeOrchestration(ctx, instanceID, "replay the buffered history"))

	metadata, err := client.WaitForOrchestrationCompletion(ctx, instanceID, api.WithFetchPayloads(true))
	require.NoError(t, err)
	require.Equal(t, api.RUNTIME_STATUS_COMPLETED, metadata.RuntimeStatus)

	admitted := recorder.requireRegressionCondition(t, events[0].payload)
	carryover := carryoverSummaries(t, admitted.Response)
	var ledger continueAsNewEventsLedger
	require.NoError(t, metadata.ReadOutput(&ledger))
	require.Equal(
		t,
		expected,
		ledger.Received,
		"generation 1 must receive every accepted event exactly once and in order; continue-as-new carryover was %v",
		carryover,
	)
	require.False(t, ledger.DeadlineHit)
	require.Equal(t, expected, carryover)

	firstExecution := executionIDOf(admitted.history())
	finalExecution := recorder.lastExecutionID()
	require.NotEmpty(t, firstExecution)
	require.NotEmpty(t, finalExecution)
	require.NotEqual(t, firstExecution, finalExecution, "ContinueAsNew must start a new execution")
	if restartWorker {
		require.Equal(t, "worker-b", admitted.Worker, "a fresh worker must replay generation 0")
	}
}

type raisedEvent struct {
	name    string
	payload string
}

// observedWorkItem is an unmodified copy of one orchestration work item and
// the completion the worker sent for it.
type observedWorkItem struct {
	Worker   string
	Request  *protos.OrchestratorRequest
	Streamed []*protos.HistoryEvent
	Response *protos.OrchestratorResponse
	Err      error
	Done     bool
	token    string
}

func (item *observedWorkItem) history() []*protos.HistoryEvent {
	past := item.Request.GetPastEvents()
	if item.Request.GetRequiresHistoryStreaming() {
		past = item.Streamed
	}
	return append(slices.Clone(past), item.Request.GetNewEvents()...)
}

// workItemRecorder observes one instance's worker traffic through standard
// gRPC interceptors. It forwards every message unchanged and can hold the
// instance's first completion until released.
type workItemRecorder struct {
	instanceID  string
	holding     atomic.Bool
	held        chan struct{}
	release     chan struct{}
	releaseOnce sync.Once

	mu      sync.Mutex
	changed chan struct{}
	items   []*observedWorkItem
}

func newWorkItemRecorder(instanceID string) *workItemRecorder {
	return &workItemRecorder{
		instanceID: instanceID,
		held:       make(chan struct{}),
		release:    make(chan struct{}),
		changed:    make(chan struct{}),
	}
}

func (r *workItemRecorder) releaseFirstCompletion() {
	r.releaseOnce.Do(func() { close(r.release) })
}

func (r *workItemRecorder) waitForHeldCompletion(ctx context.Context) error {
	select {
	case <-r.held:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("the first completion was not observed: %w", ctx.Err())
	}
}

func (r *workItemRecorder) unaryInterceptor() grpc.UnaryClientInterceptor {
	return func(
		ctx context.Context,
		method string,
		request any,
		reply any,
		connection *grpc.ClientConn,
		invoker grpc.UnaryInvoker,
		callOptions ...grpc.CallOption,
	) error {
		response, ok := request.(*protos.OrchestratorResponse)
		if method != protos.TaskHubSidecarService_CompleteOrchestratorTask_FullMethodName ||
			!ok || response.GetInstanceId() != r.instanceID {
			return invoker(ctx, method, request, reply, connection, callOptions...)
		}
		if r.holding.CompareAndSwap(false, true) {
			close(r.held)
			select {
			case <-r.release:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		err := invoker(ctx, method, request, reply, connection, callOptions...)
		r.recordCompletion(proto.Clone(response).(*protos.OrchestratorResponse), err)
		return err
	}
}

func (r *workItemRecorder) streamInterceptor(worker string) grpc.StreamClientInterceptor {
	return func(
		ctx context.Context,
		description *grpc.StreamDesc,
		connection *grpc.ClientConn,
		method string,
		streamer grpc.Streamer,
		callOptions ...grpc.CallOption,
	) (grpc.ClientStream, error) {
		stream, err := streamer(ctx, description, connection, method, callOptions...)
		if err != nil {
			return stream, err
		}
		if method == protos.TaskHubSidecarService_GetWorkItems_FullMethodName ||
			method == protos.TaskHubSidecarService_StreamInstanceHistory_FullMethodName {
			return &observedStream{ClientStream: stream, recorder: r, worker: worker}, nil
		}
		return stream, nil
	}
}

type observedStream struct {
	grpc.ClientStream
	recorder   *workItemRecorder
	worker     string
	historyFor string
}

func (s *observedStream) SendMsg(message any) error {
	if request, ok := message.(*protos.StreamInstanceHistoryRequest); ok {
		s.historyFor = request.GetInstanceId()
	}
	return s.ClientStream.SendMsg(message)
}

func (s *observedStream) RecvMsg(message any) error {
	if err := s.ClientStream.RecvMsg(message); err != nil {
		return err
	}
	switch received := message.(type) {
	case *protos.WorkItem:
		s.recorder.recordWorkItem(s.worker, received)
	case *protos.HistoryChunk:
		if s.historyFor == s.recorder.instanceID {
			s.recorder.recordHistoryChunk(received)
		}
	}
	return nil
}

func (r *workItemRecorder) recordWorkItem(worker string, item *protos.WorkItem) {
	request := item.GetOrchestratorRequest()
	if request.GetInstanceId() != r.instanceID {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.items = append(r.items, &observedWorkItem{
		Worker:  worker,
		Request: proto.Clone(request).(*protos.OrchestratorRequest),
		token:   item.GetCompletionToken(),
	})
	r.notifyLocked()
}

func (r *workItemRecorder) recordHistoryChunk(chunk *protos.HistoryChunk) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for index := len(r.items) - 1; index >= 0; index-- {
		item := r.items[index]
		if !item.Done && item.Request.GetRequiresHistoryStreaming() {
			for _, event := range chunk.GetEvents() {
				item.Streamed = append(item.Streamed, proto.Clone(event).(*protos.HistoryEvent))
			}
			return
		}
	}
}

func (r *workItemRecorder) recordCompletion(response *protos.OrchestratorResponse, err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, item := range r.items {
		// The worker retries transient completion failures with the same token,
		// so a later attempt replaces a failed one.
		if item.token == response.GetCompletionToken() && (!item.Done || item.Err != nil) {
			response.CompletionToken = ""
			item.Response, item.Err, item.Done = response, err, true
			r.notifyLocked()
			return
		}
	}
}

func (r *workItemRecorder) notifyLocked() {
	close(r.changed)
	r.changed = make(chan struct{})
}

// waitForSuspendedTimer waits until the timer is delivered and buffered while
// the instance is suspended, so generation 0 has not finalized yet.
func (r *workItemRecorder) waitForSuspendedTimer(ctx context.Context) error {
	for {
		r.mu.Lock()
		for _, item := range r.items {
			if !item.Done || item.Err != nil || !slices.ContainsFunc(item.Request.GetNewEvents(), isTimerFired) {
				continue
			}
			r.mu.Unlock()
			if completionOf(item.Response) != nil {
				return errors.New("INCONCLUSIVE: the timer was processed before the suspension took effect")
			}
			return nil
		}
		changed := r.changed
		r.mu.Unlock()
		select {
		case <-changed:
		case <-ctx.Done():
			return fmt.Errorf("INCONCLUSIVE: the service did not deliver the timer while the instance was suspended: %w", ctx.Err())
		}
	}
}

// requireRegressionCondition returns the work item that continued generation 0
// as new after proving, from its unmodified history, that it replayed the timer
// before a trailing work event while the work wait was still pending.
func (r *workItemRecorder) requireRegressionCondition(t *testing.T, firstWorkPayload string) *observedWorkItem {
	t.Helper()
	r.mu.Lock()
	defer r.mu.Unlock()
	var admitted *observedWorkItem
	for _, item := range r.items {
		if !item.Done || item.Err != nil ||
			completionOf(item.Response).GetOrchestrationStatus() !=
				protos.OrchestrationStatus_ORCHESTRATION_STATUS_CONTINUED_AS_NEW {
			continue
		}
		if admitted != nil {
			t.Fatalf("INCONCLUSIVE: more than one acknowledged continue-as-new completion was observed")
		}
		admitted = item
	}
	if admitted == nil {
		t.Fatalf("INCONCLUSIVE: no acknowledged continue-as-new completion was observed for %s", r.instanceID)
	}

	history := admitted.history()
	timerFired := slices.IndexFunc(history, isTimerFired)
	firstWork := slices.IndexFunc(history, func(event *protos.HistoryEvent) bool {
		return strings.EqualFold(event.GetEventRaised().GetName(), "work")
	})
	if timerFired < 0 || firstWork < timerFired {
		t.Fatalf(
			"INCONCLUSIVE: the continue-as-new work item did not replay the timer before a pending work event: %s",
			summarizeHistory(history),
		)
	}
	if got := raisedPayload(t, history[firstWork]); got != firstWorkPayload {
		t.Fatalf("INCONCLUSIVE: the first trailing work event was %q, want %q", got, firstWorkPayload)
	}
	return admitted
}

func (r *workItemRecorder) lastExecutionID() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	for index := len(r.items) - 1; index >= 0; index-- {
		if id := executionIDOf(r.items[index].history()); id != "" {
			return id
		}
	}
	return ""
}

// writeEvidence logs the observed timeline when the test fails and writes the
// unmodified observations when DTS_CAN_ARTIFACT_DIR is set. Completion tokens
// are never recorded.
func (r *workItemRecorder) writeEvidence(t *testing.T) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if t.Failed() {
		for index, item := range r.items {
			t.Logf("work item %d (%s): %s -> %s", index, item.Worker, summarizeHistory(item.history()), summarizeCompletion(item))
		}
	}
	directory := os.Getenv(continueAsNewArtifactDirEnv)
	if directory == "" {
		return
	}
	type evidence struct {
		Worker   string            `json:"worker"`
		Request  json.RawMessage   `json:"request"`
		Streamed []json.RawMessage `json:"streamedHistory,omitempty"`
		Response json.RawMessage   `json:"response,omitempty"`
		Error    string            `json:"error,omitempty"`
	}
	records := make([]evidence, 0, len(r.items))
	for _, item := range r.items {
		record := evidence{Worker: item.Worker, Request: protoJSON(item.Request)}
		for _, event := range item.Streamed {
			record.Streamed = append(record.Streamed, protoJSON(event))
		}
		if item.Response != nil {
			record.Response = protoJSON(item.Response)
		}
		if item.Err != nil {
			record.Error = item.Err.Error()
		}
		records = append(records, record)
	}
	content, err := json.MarshalIndent(map[string]any{
		"test":      t.Name(),
		"failed":    t.Failed(),
		"instance":  r.instanceID,
		"workItems": records,
	}, "", "  ")
	if err == nil {
		name := strings.NewReplacer("/", "_", " ", "_").Replace(t.Name()) + ".json"
		err = os.WriteFile(filepath.Join(directory, name), content, 0o600)
	}
	if err != nil {
		t.Errorf("failed to write continue-as-new evidence: %v", err)
	}
}

func cleanupContinueAsNewInstance(t *testing.T, ctx context.Context, client *durabletaskscheduler.Client, id api.InstanceID) {
	t.Helper()
	metadata, err := client.FetchOrchestrationMetadata(ctx, id)
	if errors.Is(err, api.ErrInstanceNotFound) {
		return
	}
	if err != nil {
		t.Errorf("failed to read %s before cleanup: %v", id, err)
		return
	}
	terminal := slices.Contains([]api.OrchestrationStatus{
		api.RUNTIME_STATUS_COMPLETED,
		api.RUNTIME_STATUS_FAILED,
		api.RUNTIME_STATUS_TERMINATED,
	}, metadata.RuntimeStatus)
	if !terminal {
		if err := client.TerminateOrchestration(ctx, id, api.WithRecursiveTerminate(false)); err != nil {
			t.Errorf("failed to terminate %s: %v", id, err)
			return
		}
		if _, err := client.WaitForOrchestrationCompletion(ctx, id); err != nil {
			t.Errorf("failed to wait for %s to terminate: %v", id, err)
			return
		}
	}
	if _, err := client.PurgeInstances(ctx, api.PurgeInstancesRequest{InstanceIDs: []api.InstanceID{id}}); err != nil {
		t.Errorf("failed to purge %s: %v", id, err)
	}
}

func completionOf(response *protos.OrchestratorResponse) *protos.CompleteOrchestrationAction {
	for _, action := range response.GetActions() {
		if completion := action.GetCompleteOrchestration(); completion != nil {
			return completion
		}
	}
	return nil
}

func carryoverSummaries(t *testing.T, response *protos.OrchestratorResponse) []string {
	t.Helper()
	var carryover []string
	for _, event := range completionOf(response).GetCarryoverEvents() {
		carryover = append(carryover, event.GetEventRaised().GetName()+"="+raisedPayload(t, event))
	}
	return carryover
}

func raisedPayload(t *testing.T, event *protos.HistoryEvent) string {
	t.Helper()
	var payload string
	require.NoError(t, json.Unmarshal([]byte(event.GetEventRaised().GetInput().GetValue()), &payload))
	return payload
}

func executionIDOf(history []*protos.HistoryEvent) string {
	for _, event := range history {
		if started := event.GetExecutionStarted(); started != nil {
			return started.GetOrchestrationInstance().GetExecutionId().GetValue()
		}
	}
	return ""
}

func isTimerFired(event *protos.HistoryEvent) bool {
	return event.GetTimerFired() != nil
}

func summarizeHistory(history []*protos.HistoryEvent) string {
	summaries := make([]string, 0, len(history))
	for _, event := range history {
		switch {
		case event.GetExecutionStarted() != nil:
			summaries = append(summaries, "ExecutionStarted("+executionIDOf([]*protos.HistoryEvent{event})+")")
		case event.GetTimerCreated() != nil:
			summaries = append(summaries, fmt.Sprintf("TimerCreated(%d)", event.GetEventId()))
		case event.GetTimerFired() != nil:
			summaries = append(summaries, fmt.Sprintf("TimerFired(%d)", event.GetTimerFired().GetTimerId()))
		case event.GetEventRaised() != nil:
			raised := event.GetEventRaised()
			summaries = append(summaries, "EventRaised("+raised.GetName()+"="+raised.GetInput().GetValue()+")")
		default:
			kind := fmt.Sprintf("%T", event.GetEventType())
			summaries = append(summaries, strings.TrimPrefix(kind, "*protos.HistoryEvent_"))
		}
	}
	return strings.Join(summaries, ", ")
}

func summarizeCompletion(item *observedWorkItem) string {
	switch {
	case !item.Done:
		return "not completed"
	case item.Err != nil:
		return "completion failed: " + item.Err.Error()
	}
	completion := completionOf(item.Response)
	if completion == nil {
		return fmt.Sprintf("%d action(s)", len(item.Response.GetActions()))
	}
	return fmt.Sprintf("%s with carryover [%s]", completion.GetOrchestrationStatus(), summarizeHistory(completion.GetCarryoverEvents()))
}

func protoJSON(message proto.Message) json.RawMessage {
	content, err := protojson.Marshal(message)
	if err != nil {
		return json.RawMessage(fmt.Sprintf("%q", err.Error()))
	}
	return content
}
