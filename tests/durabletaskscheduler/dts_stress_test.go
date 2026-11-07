//go:build stress && unix

// Stress and performance tests for extended sessions. They are not part of
// normal CI. Run them by hand against a session-enabled DTS endpoint:
//
//	DTS_EMULATOR_ENDPOINT=http://127.0.0.1:8080 \
//	DTS_STRESS_RESTART=/path/to/restart-backend.sh \
//	go test -tags stress -run 'TestDTSPerf|TestDTSStress' -timeout 6h -v ./tests/durabletaskscheduler
//
// Settings:
//   - DTS_STRESS_RESTART: a shell command that restarts the backend. The
//     harness then waits until the endpoint answers. The perf test restarts
//     the backend before every run. The backend-restart stress test needs it.
//   - DTS_STRESS_PERSISTENT=1: the backend keeps its state across restarts,
//     so the backend-restart test checks every instance and not only new ones.
//   - DTS_STRESS_SCALE: multiplies instance, step, and signal counts (default 1).
//   - DTS_STRESS_REPEAT: perf repetitions. The table shows the median (default 3).
//   - DTS_STRESS_SESSIONS=off: run the stress tests without extended sessions.
//   - DTS_STRESS_FAULT_RATE: the chance of a fault on each session stream in
//     the chaos test (default 0.1).
//   - DTS_STRESS_VERBOSE=1: print worker warnings, not only errors.
//
// Use 127.0.0.1 and not localhost. On some machines every localhost
// connection waits for an IPv6 attempt first.
//
// The CPU and memory numbers are for the whole test process, so they also
// include the client.

package durabletaskscheduler_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"math/rand/v2"
	"os"
	"os/exec"
	"runtime"
	"runtime/pprof"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/microsoft/durabletask-go/api"
	durabletaskclient "github.com/microsoft/durabletask-go/client"
	"github.com/microsoft/durabletask-go/durabletaskscheduler"
	"github.com/microsoft/durabletask-go/internal/protos"
	"github.com/microsoft/durabletask-go/task"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type stressFault int

const (
	noFault stressFault = iota
	faultCutBeforeAck
	faultDropAck
	faultCutMidBatch
	faultCutIdle
	faultDelayIdle
	stressFaultKinds
)

var stressFaultNames = [stressFaultKinds]string{
	"none", "cut before ack", "drop ack", "cut mid-batch", "cut while idle", "delay past idle timeout",
}

// stressProbe counts what the worker sends and receives, and can inject
// faults on session streams.
type stressProbe struct {
	faultRate float64
	idleDelay time.Duration

	historyBytes atomic.Int64
	workItems    atomic.Int64
	replays      atomic.Int64
	resumes      atomic.Int64
	acks         atomic.Int64
	unary        atomic.Int64
	abandons     atomic.Int64
	releases     atomic.Int64
	notFound     atomic.Int64
	open         atomic.Int64
	maxOpen      atomic.Int64
	faults       [stressFaultKinds]atomic.Int64

	mu        sync.Mutex
	rng       *rand.Rand
	turnStart map[string]time.Time
	latencies []time.Duration
}

func newStressProbe(faultRate float64, idleDelay time.Duration) *stressProbe {
	return &stressProbe{
		faultRate: faultRate,
		idleDelay: idleDelay,
		rng:       rand.New(rand.NewPCG(uint64(time.Now().UnixNano()), 1)),
		turnStart: make(map[string]time.Time),
	}
}

func (p *stressProbe) pickFault() stressFault {
	if p.faultRate <= 0 {
		return noFault
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.rng.Float64() >= p.faultRate {
		return noFault
	}
	return stressFault(1 + p.rng.IntN(int(stressFaultKinds)-1))
}

func (p *stressProbe) recordErr(err error) {
	if status.Code(err) == codes.NotFound {
		p.notFound.Add(1)
	}
}

// startTurn marks the time when the input of a turn is ready.
func (p *stressProbe) startTurn(token string) {
	p.mu.Lock()
	p.turnStart[token] = time.Now()
	p.mu.Unlock()
}

// finishTurn records the worker time of a turn when its completion is sent.
func (p *stressProbe) finishTurn(token string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if start, ok := p.turnStart[token]; ok {
		p.latencies = append(p.latencies, time.Since(start))
		delete(p.turnStart, token)
	}
}

func (p *stressProbe) faultCount() int64 {
	var total int64
	for i := range p.faults {
		total += p.faults[i].Load()
	}
	return total
}

func (p *stressProbe) streamInterceptor() grpc.StreamClientInterceptor {
	return func(
		ctx context.Context,
		desc *grpc.StreamDesc,
		cc *grpc.ClientConn,
		method string,
		streamer grpc.Streamer,
		opts ...grpc.CallOption,
	) (grpc.ClientStream, error) {
		switch method {
		case protos.TaskHubSidecarService_GetWorkItems_FullMethodName,
			protos.TaskHubSidecarService_StreamInstanceHistory_FullMethodName:
			stream, err := streamer(ctx, desc, cc, method, opts...)
			if err != nil {
				p.recordErr(err)
				return nil, err
			}
			return &probedReadStream{ClientStream: stream, probe: p}, nil
		case protos.TaskHubSidecarService_CompleteOrchestratorTaskWithExtendedSession_FullMethodName,
			protos.TaskHubSidecarService_CompleteEntityTaskWithExtendedSession_FullMethodName:
			streamCtx, cancel := context.WithCancel(ctx)
			stream, err := streamer(streamCtx, desc, cc, method, opts...)
			if err != nil {
				cancel()
				p.recordErr(err)
				return nil, err
			}
			open := p.open.Add(1)
			for current := p.maxOpen.Load(); open > current && !p.maxOpen.CompareAndSwap(current, open); current = p.maxOpen.Load() {
			}
			s := &probedSessionStream{ClientStream: stream, probe: p, ctx: streamCtx, cancel: cancel, fault: p.pickFault()}
			context.AfterFunc(streamCtx, s.finish)
			return s, nil
		default:
			return streamer(ctx, desc, cc, method, opts...)
		}
	}
}

func (p *stressProbe) unaryInterceptor() grpc.UnaryClientInterceptor {
	return func(
		ctx context.Context,
		method string,
		req, reply any,
		cc *grpc.ClientConn,
		invoker grpc.UnaryInvoker,
		opts ...grpc.CallOption,
	) error {
		switch method {
		case protos.TaskHubSidecarService_CompleteOrchestratorTask_FullMethodName:
			p.unary.Add(1)
			p.finishTurn(req.(*protos.OrchestratorResponse).GetCompletionToken())
		case protos.TaskHubSidecarService_CompleteEntityTask_FullMethodName:
			p.unary.Add(1)
			p.finishTurn(req.(*protos.EntityBatchResult).GetCompletionToken())
		case protos.TaskHubSidecarService_AbandonTaskOrchestratorWorkItem_FullMethodName,
			protos.TaskHubSidecarService_AbandonTaskEntityWorkItem_FullMethodName,
			protos.TaskHubSidecarService_AbandonTaskActivityWorkItem_FullMethodName:
			p.abandons.Add(1)
		case protos.TaskHubSidecarService_ReleaseTaskOrchestratorWorkItem_FullMethodName,
			protos.TaskHubSidecarService_ReleaseTaskEntityWorkItem_FullMethodName:
			p.releases.Add(1)
		}
		err := invoker(ctx, method, req, reply, cc, opts...)
		p.recordErr(err)
		return err
	}
}

type probedReadStream struct {
	grpc.ClientStream
	probe *stressProbe
}

func (s *probedReadStream) RecvMsg(m any) error {
	err := s.ClientStream.RecvMsg(m)
	if err != nil {
		s.probe.recordErr(err)
		return err
	}
	switch message := m.(type) {
	case *protos.HistoryChunk:
		s.probe.historyBytes.Add(int64(proto.Size(message)))
	case *protos.WorkItem:
		switch {
		case message.GetOrchestratorRequest() != nil:
			request := message.GetOrchestratorRequest()
			if len(request.GetPastEvents()) > 0 || request.GetRequiresHistoryStreaming() {
				s.probe.replays.Add(1)
			}
		case message.GetEntityRequestV2() != nil, message.GetEntityRequest() != nil:
		default:
			return nil
		}
		s.probe.workItems.Add(1)
		s.probe.historyBytes.Add(int64(proto.Size(message)))
		s.probe.startTurn(message.GetCompletionToken())
	}
	return nil
}

type probedSessionStream struct {
	grpc.ClientStream
	probe  *stressProbe
	ctx    context.Context
	cancel context.CancelFunc
	fault  stressFault
	once   sync.Once
	token  string
	batch  bool
	broken bool
	delay  bool
}

func (s *probedSessionStream) finish() {
	s.once.Do(func() { s.probe.open.Add(-1) })
}

// inject breaks the stream on both sides, so the service also sees it end.
func (s *probedSessionStream) inject() error {
	s.probe.faults[s.fault].Add(1)
	s.cancel()
	s.finish()
	return status.Error(codes.Unavailable, "injected: "+stressFaultNames[s.fault])
}

func (s *probedSessionStream) SendMsg(m any) error {
	if s.fault == faultCutBeforeAck {
		return s.inject()
	}
	switch request := m.(type) {
	case *protos.OrchestratorResponse:
		s.token = request.GetCompletionToken()
	case *protos.EntityBatchResult:
		s.token = request.GetCompletionToken()
	}
	s.probe.finishTurn(s.token)
	return s.ClientStream.SendMsg(m)
}

func (s *probedSessionStream) RecvMsg(m any) error {
	if s.broken {
		return s.inject()
	}
	if s.delay {
		s.delay = false
		s.probe.faults[faultDelayIdle].Add(1)
		select {
		case <-time.After(s.probe.idleDelay):
		case <-s.ctx.Done():
			s.finish()
			return status.FromContextError(s.ctx.Err()).Err()
		}
	}
	err := s.ClientStream.RecvMsg(m)
	if err != nil {
		if errors.Is(err, io.EOF) && s.batch {
			s.probe.resumes.Add(1)
			s.probe.startTurn(s.token)
		}
		s.probe.recordErr(err)
		s.cancel()
		s.finish()
		return err
	}
	response := m.(*protos.CompleteTaskWithExtendedSessionResponse)
	switch {
	case response.GetAck() != nil:
		if s.fault == faultDropAck {
			return s.inject()
		}
		s.probe.acks.Add(1)
		s.broken = s.fault == faultCutIdle
		s.delay = s.fault == faultDelayIdle
	case response.GetNewMessages() != nil:
		s.probe.historyBytes.Add(int64(proto.Size(response.GetNewMessages())))
		s.batch = true
		s.broken = s.fault == faultCutMidBatch
	}
	return nil
}

func envFloat(name string, fallback float64) float64 {
	if value, err := strconv.ParseFloat(os.Getenv(name), 64); err == nil {
		return value
	}
	return fallback
}

func scaled(n int) int {
	return max(1, int(math.Round(float64(n)*envFloat("DTS_STRESS_SCALE", 1))))
}

func stressLogger() api.Logger {
	level := slog.LevelError
	if os.Getenv("DTS_STRESS_VERBOSE") == "1" {
		level = slog.LevelWarn
	}
	return api.NewSlogLogger(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: level})))
}

// restartBackend runs DTS_STRESS_RESTART and waits until the endpoint
// answers. It does nothing when no restart command is set.
func restartBackend(t *testing.T) error {
	command := os.Getenv("DTS_STRESS_RESTART")
	if command == "" {
		return nil
	}
	if output, err := exec.Command("sh", "-c", command).CombinedOutput(); err != nil {
		return fmt.Errorf("restart command failed: %w: %s", err, output)
	}
	client, err := durabletaskscheduler.NewClient(context.Background(), emulatorOptions(t), stressLogger())
	if err != nil {
		return err
	}
	defer func() { _ = client.Close() }()
	for deadline := time.Now().Add(3 * time.Minute); time.Now().Before(deadline); time.Sleep(500 * time.Millisecond) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_, err = client.FetchOrchestrationMetadata(ctx, api.InstanceID("stress-ready-"+uuid.NewString()))
		cancel()
		if err == nil || errors.Is(err, api.ErrInstanceNotFound) {
			return nil
		}
	}
	return fmt.Errorf("the backend did not answer after the restart: %w", err)
}

// stressHarness owns one management client and a worker that can be
// replaced while the test runs.
type stressHarness struct {
	t             *testing.T
	registry      *task.TaskRegistry
	probe         *stressProbe
	workerOptions []durabletaskclient.TaskHubGrpcWorkerOption
	client        *durabletaskscheduler.Client

	mu       sync.Mutex
	worker   *durabletaskclient.TaskHubGrpcWorker
	restarts int
}

func newStressHarness(
	t *testing.T,
	registry *task.TaskRegistry,
	probe *stressProbe,
	sessions *durabletaskclient.ExtendedSessionOptions,
	workerOptions ...durabletaskclient.TaskHubGrpcWorkerOption,
) *stressHarness {
	t.Helper()
	client, err := durabletaskscheduler.NewClient(context.Background(), emulatorOptions(t), stressLogger())
	require.NoError(t, err)
	options := append([]durabletaskclient.TaskHubGrpcWorkerOption{
		durabletaskclient.WithAutoWorkItemFilters(),
		durabletaskclient.WithWorkerSilentDisconnectTimeout(15 * time.Second),
	}, workerOptions...)
	if sessions != nil {
		options = append(options, durabletaskclient.WithExtendedSessions(*sessions))
	}
	h := &stressHarness{t: t, registry: registry, probe: probe, workerOptions: options, client: client}
	require.NoError(t, h.startWorker())
	return h
}

func (h *stressHarness) startWorker() error {
	options := emulatorOptions(h.t)
	options.StreamInterceptors = append(slices.Clone(options.StreamInterceptors), h.probe.streamInterceptor())
	options.UnaryInterceptors = append(slices.Clone(options.UnaryInterceptors), h.probe.unaryInterceptor())
	worker, err := durabletaskscheduler.NewWorker(options, h.registry, stressLogger(), h.workerOptions...)
	if err != nil {
		return err
	}
	if err := worker.Start(context.Background()); err != nil {
		return err
	}
	h.mu.Lock()
	h.worker = worker
	h.mu.Unlock()
	return nil
}

func (h *stressHarness) stopWorker() {
	h.mu.Lock()
	worker := h.worker
	h.worker = nil
	h.mu.Unlock()
	if worker == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := worker.Shutdown(ctx); err != nil {
		h.t.Logf("worker shutdown: %v", err)
	}
}

// restartWorker replaces the worker. It is safe to call from any goroutine.
func (h *stressHarness) restartWorker() {
	h.stopWorker()
	assert.NoError(h.t, h.startWorker())
	h.mu.Lock()
	h.restarts++
	h.mu.Unlock()
}

func (h *stressHarness) close() {
	h.stopWorker()
	require.NoError(h.t, h.client.Close())
}

func stressRegistry(t *testing.T) *task.TaskRegistry {
	t.Helper()
	registry := task.NewTaskRegistry()
	require.NoError(t, registry.AddActivityN("PerfStep", func(ctx task.ActivityContext) (any, error) {
		var input int
		if err := ctx.GetInput(&input); err != nil {
			return nil, err
		}
		return input + 1, nil
	}))
	require.NoError(t, registry.AddOrchestratorN("PerfChain", func(ctx *task.OrchestrationContext) (any, error) {
		var steps int
		if err := ctx.GetInput(&steps); err != nil {
			return nil, err
		}
		value := 0
		for range steps {
			if err := ctx.CallActivity("PerfStep", task.WithActivityInput(value)).Await(&value); err != nil {
				return nil, err
			}
		}
		return value, nil
	}))
	require.NoError(t, registry.AddOrchestratorN("PerfEvents", func(ctx *task.OrchestrationContext) (any, error) {
		var events int
		if err := ctx.GetInput(&events); err != nil {
			return nil, err
		}
		sum := 0
		for i := range events {
			var value int
			if err := ctx.WaitForSingleEvent("e", -1).Await(&value); err != nil {
				return nil, err
			}
			sum += value
			if err := ctx.SetCustomStatusValue(i + 1); err != nil {
				return nil, err
			}
		}
		return sum, nil
	}))
	require.NoError(t, registry.AddOrchestratorN("PerfFanOut", func(ctx *task.OrchestrationContext) (any, error) {
		var width int
		if err := ctx.GetInput(&width); err != nil {
			return nil, err
		}
		return fanOut(ctx, 0, width)
	}))
	require.NoError(t, registry.AddEntityN("StressCounter", func(ctx *task.EntityContext) (any, error) {
		var value int
		if ctx.HasState() {
			if err := ctx.GetState(&value); err != nil {
				return nil, err
			}
		}
		if ctx.Operation != "add" {
			return nil, fmt.Errorf("unknown operation %q", ctx.Operation)
		}
		var amount int
		if err := ctx.GetInput(&amount); err != nil {
			return nil, err
		}
		value += amount
		return value, ctx.SetState(value)
	}))
	require.NoError(t, registry.AddOrchestratorN("StressMixed", stressMixed))
	return registry
}

func fanOut(ctx *task.OrchestrationContext, base, width int) (int, error) {
	tasks := make([]task.Task, width)
	for i := range tasks {
		tasks[i] = ctx.CallActivity("PerfStep", task.WithActivityInput(base+i))
	}
	if err := ctx.WhenAll(tasks...); err != nil {
		return 0, err
	}
	sum := 0
	for _, pending := range tasks {
		var value int
		if err := pending.Await(&value); err != nil {
			return 0, err
		}
		sum += value
	}
	return sum, nil
}

type stressInput struct {
	Index    int
	Entities int
	Prefix   string
}

const stressEventsPerInstance = 3

func stressEntityID(prefix string, index int) api.EntityID {
	return api.NewEntityID("StressCounter", fmt.Sprintf("%s-%d", prefix, index))
}

// stressMixed runs one of six shapes, chosen by the instance index.
// stressExpected returns its result.
func stressMixed(ctx *task.OrchestrationContext) (any, error) {
	var input stressInput
	if err := ctx.GetInput(&input); err != nil {
		return nil, err
	}
	i := input.Index
	switch i % 6 {
	case 0:
		value := i
		for range 3 + i%5 {
			if err := ctx.CallActivity("PerfStep", task.WithActivityInput(value)).Await(&value); err != nil {
				return nil, err
			}
		}
		return value, nil
	case 1:
		return fanOut(ctx, i, 10)
	case 2:
		sum := 0
		for range stressEventsPerInstance {
			var value int
			if err := ctx.WaitForSingleEvent("e", 30*time.Minute).Await(&value); err != nil {
				return nil, err
			}
			sum += value
		}
		return sum, nil
	case 3:
		sum := 0
		for range 2 {
			var value int
			if err := ctx.CallSubOrchestrator("PerfChain", task.WithSubOrchestratorInput(3)).Await(&value); err != nil {
				return nil, err
			}
			sum += value
		}
		return sum, nil
	case 4:
		if err := ctx.CreateTimer(50 * time.Millisecond).Await(nil); err != nil {
			return nil, err
		}
		var value int
		err := ctx.CallActivity("PerfStep", task.WithActivityInput(i)).Await(&value)
		return value, err
	default:
		entityID := stressEntityID(input.Prefix, i%input.Entities)
		if i%2 == 1 {
			unlock, err := ctx.LockEntities(entityID)
			if err != nil {
				return nil, err
			}
			defer unlock()
		}
		return 1, ctx.CallEntity(entityID, "add", task.WithEntityInput(1)).Await(nil)
	}
}

func stressExpected(i int) int {
	switch i % 6 {
	case 0:
		return i + 3 + i%5
	case 1:
		return 10*i + 45 + 10
	case 2:
		return stressEventsPerInstance * i
	case 3:
		return 6
	case 4:
		return i + 1
	default:
		return 1
	}
}

// ----------------------------------------------------------------------------
// Performance

type perfResult struct {
	wall         time.Duration
	units        int
	cpu          time.Duration
	totalAlloc   uint64
	peakHeap     uint64
	historyBytes int64
	workItems    int64
	replays      int64
	resumes      int64
	acks         int64
	unary        int64
	p50          time.Duration
	p95          time.Duration
	p99          time.Duration
}

type perfWorkload struct {
	name  string
	units string
	run   func(t *testing.T, ctx context.Context, client *durabletaskscheduler.Client) int
}

func processCPU() time.Duration {
	var usage syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &usage); err != nil {
		return 0
	}
	return time.Duration(usage.Utime.Nano() + usage.Stime.Nano())
}

func percentile(sorted []time.Duration, q float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	return sorted[min(len(sorted)-1, int(math.Ceil(q*float64(len(sorted))))-1)]
}

// waitCompleted is safe to call from any goroutine.
func waitCompleted(t *testing.T, ctx context.Context, client *durabletaskscheduler.Client, id api.InstanceID, want int) {
	metadata, err := client.WaitForOrchestrationCompletion(ctx, id, api.WithFetchPayloads(true))
	if !assert.NoError(t, err) {
		return
	}
	var got int
	if assert.Equal(t, protos.OrchestrationStatus_ORCHESTRATION_STATUS_COMPLETED, metadata.RuntimeStatus, "failure: %v", metadata.FailureDetails) &&
		assert.NoError(t, metadata.ReadOutput(&got)) {
		assert.Equal(t, want, got, "%s output", id)
	}
}

func perfWorkloads() []perfWorkload {
	return []perfWorkload{
		{name: "chain", units: "activities", run: func(t *testing.T, ctx context.Context, client *durabletaskscheduler.Client) int {
			steps := scaled(500)
			id, err := client.ScheduleNewOrchestration(ctx, "PerfChain", api.WithInput(steps))
			require.NoError(t, err)
			waitCompleted(t, ctx, client, id, steps)
			return steps
		}},
		{name: "events", units: "events", run: func(t *testing.T, ctx context.Context, client *durabletaskscheduler.Client) int {
			events := scaled(200)
			id, err := client.ScheduleNewOrchestration(ctx, "PerfEvents", api.WithInput(events))
			require.NoError(t, err)
			for i := range events {
				require.NoError(t, client.RaiseEvent(ctx, id, "e", api.WithEventPayload(i)))
				// Raise the next event only after this one was processed, so
				// every event is its own turn.
				for {
					metadata, err := client.FetchOrchestrationMetadata(ctx, id, api.WithFetchPayloads(true))
					require.NoError(t, err)
					var processed int
					if metadata.SerializedCustomStatus != "" {
						require.NoError(t, metadata.ReadCustomStatus(&processed))
					}
					if processed > i {
						break
					}
					time.Sleep(time.Millisecond)
				}
			}
			waitCompleted(t, ctx, client, id, events*(events-1)/2)
			return events
		}},
		{name: "fan-out", units: "activities", run: func(t *testing.T, ctx context.Context, client *durabletaskscheduler.Client) int {
			width := scaled(200)
			id, err := client.ScheduleNewOrchestration(ctx, "PerfFanOut", api.WithInput(width))
			require.NoError(t, err)
			waitCompleted(t, ctx, client, id, width*(width+1)/2)
			return width
		}},
		{name: "entity", units: "signals", run: func(t *testing.T, ctx context.Context, client *durabletaskscheduler.Client) int {
			signals := scaled(1000)
			entityID := stressEntityID("perf-"+uuid.NewString(), 0)
			for range signals {
				require.NoError(t, client.SignalEntity(ctx, entityID, "add", api.WithSignalInput(1)))
			}
			want := strconv.Itoa(signals)
			require.Eventually(t, func() bool {
				metadata, err := client.GetEntity(ctx, entityID)
				return err == nil && metadata != nil && metadata.SerializedState == want
			}, 10*time.Minute, 5*time.Millisecond)
			return signals
		}},
		{name: "short", units: "activities", run: func(t *testing.T, ctx context.Context, client *durabletaskscheduler.Client) int {
			instances := scaled(1000)
			ids := make([]api.InstanceID, instances)
			runParallel(instances, 32, func(i int) {
				id, err := client.ScheduleNewOrchestration(ctx, "PerfChain", api.WithInput(5))
				assert.NoError(t, err)
				ids[i] = id
			})
			require.False(t, t.Failed())
			runParallel(instances, 32, func(i int) { waitCompleted(t, ctx, client, ids[i], 5) })
			return 5 * instances
		}},
	}
}

func runParallel(n, workers int, fn func(int)) {
	var next atomic.Int64
	var wg sync.WaitGroup
	for range min(n, workers) {
		wg.Go(func() {
			for i := int(next.Add(1)) - 1; i < n; i = int(next.Add(1)) - 1 {
				fn(i)
			}
		})
	}
	wg.Wait()
}

func runPerf(t *testing.T, workload perfWorkload, sessions bool) perfResult {
	t.Helper()
	var sessionOptions *durabletaskclient.ExtendedSessionOptions
	if sessions {
		sessionOptions = &durabletaskclient.ExtendedSessionOptions{}
	}
	probe := newStressProbe(0, 0)
	h := newStressHarness(t, stressRegistry(t), probe, sessionOptions,
		durabletaskclient.WithMaxConcurrentOrchestrationWorkItems(16),
		durabletaskclient.WithMaxConcurrentActivityWorkItems(32),
		durabletaskclient.WithMaxConcurrentEntityWorkItems(16),
	)
	defer h.close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()

	runtime.GC()
	var before runtime.MemStats
	runtime.ReadMemStats(&before)
	var peakHeap atomic.Uint64
	stopSampler := make(chan struct{})
	sampled := make(chan struct{})
	go func() {
		defer close(sampled)
		ticker := time.NewTicker(20 * time.Millisecond)
		defer ticker.Stop()
		for {
			var stats runtime.MemStats
			runtime.ReadMemStats(&stats)
			if stats.HeapInuse > peakHeap.Load() {
				peakHeap.Store(stats.HeapInuse)
			}
			select {
			case <-stopSampler:
				return
			case <-ticker.C:
			}
		}
	}()
	cpuStart := processCPU()
	start := time.Now()
	units := workload.run(t, ctx, h.client)
	wall := time.Since(start)
	cpu := processCPU() - cpuStart
	close(stopSampler)
	<-sampled
	var after runtime.MemStats
	runtime.ReadMemStats(&after)

	probe.mu.Lock()
	latencies := slices.Clone(probe.latencies)
	probe.mu.Unlock()
	slices.Sort(latencies)
	if sessions {
		require.Positive(t, probe.acks.Load(), "the sessions-on run did not use sessions")
	} else {
		require.Zero(t, probe.acks.Load(), "the sessions-off run used sessions")
	}
	return perfResult{
		wall:         wall,
		units:        units,
		cpu:          cpu,
		totalAlloc:   after.TotalAlloc - before.TotalAlloc,
		peakHeap:     peakHeap.Load(),
		historyBytes: probe.historyBytes.Load(),
		workItems:    probe.workItems.Load(),
		replays:      probe.replays.Load(),
		resumes:      probe.resumes.Load(),
		acks:         probe.acks.Load(),
		unary:        probe.unary.Load(),
		p50:          percentile(latencies, 0.50),
		p95:          percentile(latencies, 0.95),
		p99:          percentile(latencies, 0.99),
	}
}

func medianOf[T int64 | uint64 | time.Duration](results []perfResult, field func(perfResult) T) T {
	values := make([]T, len(results))
	for i, result := range results {
		values[i] = field(result)
	}
	slices.Sort(values)
	return values[len(values)/2]
}

func formatPerfRows(workload perfWorkload, off, on []perfResult) string {
	var b strings.Builder
	row := func(metric string, offValue, onValue float64, format func(float64) string) {
		change := "n/a"
		if offValue != 0 {
			change = fmt.Sprintf("%+.1f%%", 100*(onValue-offValue)/offValue)
		}
		fmt.Fprintf(&b, "| %s | %s | %s | %s | %s |\n", workload.name, metric, format(offValue), format(onValue), change)
	}
	ms := func(v float64) string { return fmt.Sprintf("%.2f ms", v/float64(time.Millisecond)) }
	mib := func(v float64) string {
		if v < 1<<20 {
			return fmt.Sprintf("%.1f KiB", v/(1<<10))
		}
		return fmt.Sprintf("%.1f MiB", v/(1<<20))
	}
	count := func(v float64) string { return strconv.FormatFloat(v, 'f', 0, 64) }
	perSecond := func(v float64) string { return fmt.Sprintf("%.0f %s/s", v, workload.units) }
	durationRow := func(metric string, field func(perfResult) time.Duration) {
		row(metric, float64(medianOf(off, field)), float64(medianOf(on, field)), ms)
	}
	intRow := func(metric string, field func(perfResult) int64, format func(float64) string) {
		row(metric, float64(medianOf(off, field)), float64(medianOf(on, field)), format)
	}
	throughput := func(results []perfResult) float64 {
		wall := medianOf(results, func(r perfResult) time.Duration { return r.wall })
		return float64(results[0].units) / wall.Seconds()
	}
	durationRow("wall time", func(r perfResult) time.Duration { return r.wall })
	row("throughput", throughput(off), throughput(on), perSecond)
	durationRow("turn p50", func(r perfResult) time.Duration { return r.p50 })
	durationRow("turn p95", func(r perfResult) time.Duration { return r.p95 })
	durationRow("turn p99", func(r perfResult) time.Duration { return r.p99 })
	durationRow("process CPU", func(r perfResult) time.Duration { return r.cpu })
	row("allocated", float64(medianOf(off, func(r perfResult) uint64 { return r.totalAlloc })),
		float64(medianOf(on, func(r perfResult) uint64 { return r.totalAlloc })), mib)
	row("peak heap in use", float64(medianOf(off, func(r perfResult) uint64 { return r.peakHeap })),
		float64(medianOf(on, func(r perfResult) uint64 { return r.peakHeap })), mib)
	intRow("history received", func(r perfResult) int64 { return r.historyBytes }, mib)
	intRow("work items (loads)", func(r perfResult) int64 { return r.workItems }, count)
	intRow("replays", func(r perfResult) int64 { return r.replays }, count)
	intRow("session resumes", func(r perfResult) int64 { return r.resumes }, count)
	intRow("session turns (acks)", func(r perfResult) int64 { return r.acks }, count)
	intRow("unary completions", func(r perfResult) int64 { return r.unary }, count)
	return b.String()
}

// TestDTSPerf compares the same workloads with sessions off and on. With
// DTS_STRESS_RESTART set, every run starts on a fresh backend.
func TestDTSPerf(t *testing.T) {
	emulatorOptions(t)
	repeat := max(1, int(envFloat("DTS_STRESS_REPEAT", 3)))
	const header = "| workload | metric | sessions off | sessions on | change |\n|---|---|---|---|---|\n"
	var table strings.Builder
	for _, workload := range perfWorkloads() {
		t.Run(workload.name, func(t *testing.T) {
			results := map[bool][]perfResult{}
			for range repeat {
				for _, sessions := range []bool{false, true} {
					require.NoError(t, restartBackend(t))
					result := runPerf(t, workload, sessions)
					t.Logf("sessions=%v wall=%v acks=%d unary=%d replays=%d resumes=%d",
						sessions, result.wall, result.acks, result.unary, result.replays, result.resumes)
					results[sessions] = append(results[sessions], result)
				}
			}
			rows := formatPerfRows(workload, results[false], results[true])
			t.Logf("median of %d runs:\n%s%s", repeat, header, rows)
			table.WriteString(rows)
		})
	}
	t.Logf("median of %d runs:\n%s%s", repeat, header, table.String())
}

// ----------------------------------------------------------------------------
// Stress

type stressConfig struct {
	instances             int
	entities              int
	signalsPerEntity      int
	orchestrations        int
	activities            int
	entityConcurrency     int
	completionConnections int
	faultRate             float64
	workerRestarts        int
	backendRestart        bool
}

type stressBatch struct {
	prefix   string
	ids      []api.InstanceID
	entities int
	expected map[int]int
}

func stressSessionsOn() bool {
	return os.Getenv("DTS_STRESS_SESSIONS") != "off"
}

// scheduleStressBatch starts the instances, raises their events, and sends
// the entity signals.
func scheduleStressBatch(t *testing.T, ctx context.Context, client *durabletaskscheduler.Client, config stressConfig) *stressBatch {
	t.Helper()
	batch := &stressBatch{
		prefix:   "stress-" + uuid.NewString()[:8],
		ids:      make([]api.InstanceID, config.instances),
		entities: config.entities,
		expected: make(map[int]int),
	}
	for i := range config.instances {
		if i%6 == 5 {
			batch.expected[i%config.entities]++
		}
	}
	for e := range config.entities {
		batch.expected[e] += config.signalsPerEntity
	}
	runParallel(config.instances, 32, func(i int) {
		id := api.InstanceID(fmt.Sprintf("%s-%d", batch.prefix, i))
		_, err := client.ScheduleNewOrchestration(ctx, "StressMixed", api.WithInstanceID(id),
			api.WithInput(stressInput{Index: i, Entities: config.entities, Prefix: batch.prefix}))
		assert.NoError(t, err)
		batch.ids[i] = id
		if i%6 == 2 {
			for range stressEventsPerInstance {
				assert.NoError(t, client.RaiseEvent(ctx, id, "e", api.WithEventPayload(i)))
			}
		}
	})
	runParallel(config.entities*config.signalsPerEntity, 32, func(n int) {
		assert.NoError(t, client.SignalEntity(ctx, stressEntityID(batch.prefix, n%config.entities), "add", api.WithSignalInput(1)))
	})
	require.False(t, t.Failed(), "scheduling failed")
	return batch
}

// verifyStressBatch waits for every instance and entity and returns the
// problems it found. completed counts the finished instances.
func verifyStressBatch(ctx context.Context, client *durabletaskscheduler.Client, batch *stressBatch, completed *atomic.Int64) []string {
	var mu sync.Mutex
	var problems []string
	report := func(format string, args ...any) {
		mu.Lock()
		problems = append(problems, fmt.Sprintf(format, args...))
		mu.Unlock()
	}
	runParallel(len(batch.ids), 32, func(i int) {
		for {
			metadata, err := client.WaitForOrchestrationCompletion(ctx, batch.ids[i], api.WithFetchPayloads(true))
			if err != nil {
				if ctx.Err() != nil {
					status := "unknown"
					if latest, fetchErr := client.FetchOrchestrationMetadata(context.Background(), batch.ids[i]); fetchErr == nil && latest != nil {
						status = latest.RuntimeStatus.String()
					}
					report("%s: not finished (%s): %v", batch.ids[i], status, err)
					return
				}
				// The backend may be restarting.
				time.Sleep(time.Second)
				continue
			}
			completed.Add(1)
			var got int
			if metadata.RuntimeStatus != protos.OrchestrationStatus_ORCHESTRATION_STATUS_COMPLETED {
				report("%s: status %s: %v", batch.ids[i], metadata.RuntimeStatus, metadata.FailureDetails)
			} else if err := metadata.ReadOutput(&got); err != nil || got != stressExpected(i) {
				report("%s: output %d, want %d (%v)", batch.ids[i], got, stressExpected(i), err)
			}
			return
		}
	})
	// Signals can still be running after the last instance completes.
	entityDeadline := time.Now().Add(3 * time.Minute)
	for e := range batch.entities {
		entityID := stressEntityID(batch.prefix, e)
		want := strconv.Itoa(batch.expected[e])
		got := ""
		for ctx.Err() == nil && time.Now().Before(entityDeadline) {
			metadata, err := client.GetEntity(ctx, entityID)
			if err == nil && metadata != nil {
				got = metadata.SerializedState
			}
			if got == want {
				break
			}
			time.Sleep(50 * time.Millisecond)
		}
		if got != want {
			report("%s: state %q, want %s", entityID, got, want)
		}
	}
	return problems
}

func runStress(t *testing.T, config stressConfig) {
	sessionsOn := stressSessionsOn()
	if config.backendRestart && os.Getenv("DTS_STRESS_RESTART") == "" {
		t.Skip("set DTS_STRESS_RESTART to run the backend-restart test")
	}
	baseline := runtime.NumGoroutine()
	idleTimeout := 3 * time.Second
	var sessions *durabletaskclient.ExtendedSessionOptions
	if sessionsOn {
		sessions = &durabletaskclient.ExtendedSessionOptions{IdleTimeout: idleTimeout}
	}
	probe := newStressProbe(config.faultRate, idleTimeout+time.Second)
	workerOptions := []durabletaskclient.TaskHubGrpcWorkerOption{
		durabletaskclient.WithMaxConcurrentOrchestrationWorkItems(config.orchestrations),
		durabletaskclient.WithMaxConcurrentActivityWorkItems(config.activities),
		durabletaskclient.WithMaxConcurrentEntityWorkItems(config.entityConcurrency),
	}
	if config.completionConnections > 0 {
		workerOptions = append(workerOptions, durabletaskclient.WithWorkerCompletionConnections(config.completionConnections))
	}
	h := newStressHarness(t, stressRegistry(t), probe, sessions, workerOptions...)
	closed := false
	defer func() {
		if !closed {
			h.close()
		}
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()

	start := time.Now()
	batch := scheduleStressBatch(t, ctx, h.client, config)
	t.Logf("scheduled %d instances and %d signals in %v", config.instances, config.entities*config.signalsPerEntity, time.Since(start))

	var completed atomic.Int64
	done := make(chan struct{})
	var background sync.WaitGroup
	// waitProgress waits until a share of the instances completed. It
	// returns false when the run ends first.
	waitProgress := func(share float64) bool {
		for float64(completed.Load()) < share*float64(config.instances) {
			select {
			case <-done:
				return false
			case <-time.After(10 * time.Millisecond):
			}
		}
		return true
	}
	if config.workerRestarts > 0 {
		background.Go(func() {
			for n := 1; n <= config.workerRestarts; n++ {
				if !waitProgress(float64(n) / float64(config.workerRestarts+1)) {
					return
				}
				h.restartWorker()
			}
		})
	}
	persistent := os.Getenv("DTS_STRESS_PERSISTENT") == "1"
	if config.backendRestart {
		background.Go(func() {
			if !waitProgress(0.3) {
				return
			}
			t.Logf("restarting the backend after %d of %d instances", completed.Load(), config.instances)
			assert.NoError(t, restartBackend(t))
			t.Logf("the backend is back after %v", time.Since(start))
		})
	}
	var problems []string
	if config.backendRestart && !persistent {
		// All state is lost on restart. Check only that new work completes.
		for completed.Load() < int64(config.instances)*3/10 {
			_, err := h.client.WaitForOrchestrationCompletion(ctx, batch.ids[completed.Load()])
			if err == nil {
				completed.Add(1)
			}
			require.NoError(t, ctx.Err())
		}
		background.Wait()
		fresh := config
		fresh.instances, fresh.entities, fresh.signalsPerEntity = scaled(120), scaled(12), 5
		freshBatch := scheduleStressBatch(t, ctx, h.client, fresh)
		problems = verifyStressBatch(ctx, h.client, freshBatch, new(atomic.Int64))
	} else {
		problems = verifyStressBatch(ctx, h.client, batch, &completed)
	}
	close(done)
	background.Wait()
	elapsed := time.Since(start)

	t.Logf("finished in %v: worker restarts=%d acks=%d unary=%d replays=%d resumes=%d releases=%d abandons=%d notFound=%d maxOpenStreams=%d",
		elapsed, h.restarts, probe.acks.Load(), probe.unary.Load(), probe.replays.Load(), probe.resumes.Load(),
		probe.releases.Load(), probe.abandons.Load(), probe.notFound.Load(), probe.maxOpen.Load())
	for kind := faultCutBeforeAck; kind < stressFaultKinds; kind++ {
		if n := probe.faults[kind].Load(); n > 0 {
			t.Logf("injected %q: %d", stressFaultNames[kind], n)
		}
	}
	for _, problem := range problems[:min(len(problems), 20)] {
		t.Error(problem)
	}
	require.Empty(t, problems, "%d problems", len(problems))

	if sessionsOn {
		require.Positive(t, probe.acks.Load(), "sessions were not used")
		connections := config.completionConnections
		if connections == 0 {
			connections = 3
		}
		require.LessOrEqual(t, probe.maxOpen.Load(), int64(50*connections), "a connection exceeded its session stream budget")
		// Idle sessions end after the idle timeout and return their streams.
		require.Eventually(t, func() bool { return probe.open.Load() == 0 }, idleTimeout+30*time.Second, 50*time.Millisecond,
			"%d session streams are still open", probe.open.Load())
	} else {
		require.Zero(t, probe.acks.Load(), "sessions were used while off")
	}
	h.close()
	closed = true
	require.Zero(t, probe.open.Load(), "session streams are open after shutdown")
	// Poll here: assert.Eventually runs its condition in another goroutine.
	for deadline := time.Now().Add(20 * time.Second); runtime.NumGoroutine() > baseline && time.Now().Before(deadline); {
		time.Sleep(100 * time.Millisecond)
	}
	if runtime.NumGoroutine() > baseline {
		var stacks strings.Builder
		_ = pprof.Lookup("goroutine").WriteTo(&stacks, 1)
		t.Fatalf("goroutines: %d after shutdown, %d before:\n%s", runtime.NumGoroutine(), baseline, stacks.String())
	}
	if probe.faultCount() == 0 && h.restarts == 0 && !config.backendRestart {
		assert.Zero(t, probe.abandons.Load(), "unexpected abandons")
		assert.Zero(t, probe.notFound.Load(), "unexpected NotFound errors")
	}
}

// TestDTSStress checks correctness under load and faults. Every subtest
// checks results, entity values, open session streams, and goroutines.
func TestDTSStress(t *testing.T) {
	emulatorOptions(t)
	instances, entities := scaled(2000), scaled(200)
	t.Run("load", func(t *testing.T) {
		runStress(t, stressConfig{
			instances: instances, entities: entities, signalsPerEntity: 10,
			orchestrations: 16, activities: 32, entityConcurrency: 16,
		})
	})
	t.Run("one-completion-connection", func(t *testing.T) {
		runStress(t, stressConfig{
			instances: instances, entities: entities, signalsPerEntity: 10,
			orchestrations: 160, activities: 64, entityConcurrency: 120, completionConnections: 1,
		})
	})
	t.Run("chaos", func(t *testing.T) {
		runStress(t, stressConfig{
			instances: instances, entities: entities, signalsPerEntity: 10,
			orchestrations: 32, activities: 32, entityConcurrency: 16,
			faultRate: envFloat("DTS_STRESS_FAULT_RATE", 0.1), workerRestarts: 3,
		})
	})
	t.Run("backend-restart", func(t *testing.T) {
		runStress(t, stressConfig{
			instances: instances, entities: entities, signalsPerEntity: 10,
			orchestrations: 32, activities: 32, entityConcurrency: 16,
			faultRate: envFloat("DTS_STRESS_FAULT_RATE", 0.1) / 2, backendRestart: true,
		})
	})
}
