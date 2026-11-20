// Package durabletasktest runs orchestrations, activities, and entities in process for tests.
//
// A [Host] starts an in-memory Durable Task service, a worker for the
// orchestrators, activities, and entities in a [task.TaskRegistry], and a client
// connected to the same service. Communication uses an in-memory gRPC
// transport, so no network ports or external services are required.
//
//	registry := task.NewTaskRegistry()
//	registry.AddOrchestratorN("Greeting", greeting)
//	registry.AddActivityN("SayHello", sayHello)
//
//	host := durabletasktest.New(t, registry)
//	id, err := host.Client().ScheduleNewOrchestration(ctx, "Greeting")
//	...
//	metadata, err := host.Client().WaitForOrchestrationCompletion(ctx, id)
//
// Orchestrations run through the same worker, executor, and client code paths
// that production workers use. To isolate an orchestrator from external
// dependencies, register stub activities in the test's registry.
//
// Timers use the Go runtime clock. Go 1.25+ tests can use testing/synctest to
// advance timers without waiting in real time.
// Failed orchestrations can be rewound with the same history-safety limitations
// as the production worker. Rewind enqueues recovery and preserves successful
// work; outstanding timers and activities are recovered. Activities can run
// again if their results are unavailable. Purged children without retained
// results are recreated without their previous history; conflicting child IDs
// prevent rewind.
// Durable entities are supported, including entity critical sections:
// signals and calls to a locked entity from outside its critical section wait
// until the lock is released.
package durabletasktest

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	"github.com/microsoft/durabletask-go/api"
	"github.com/microsoft/durabletask-go/client"
	"github.com/microsoft/durabletask-go/internal/inmemory"
	"github.com/microsoft/durabletask-go/task"
)

const bufferSize = 64 << 10

const defaultCloseTimeout = 5 * time.Second

// Option configures a [Host].
type Option func(*options)

type options struct {
	logger        api.Logger
	workerOptions []client.TaskHubGrpcWorkerOption
	clientOptions []client.TaskHubGrpcClientOption
}

// WithLogger sets the logger used by the worker and client. By default, the
// host discards log output.
func WithLogger(logger api.Logger) Option {
	return func(o *options) { o.logger = logger }
}

// WithWorkerOptions adds options for the host's worker, such as a data
// converter or versioning settings.
func WithWorkerOptions(opts ...client.TaskHubGrpcWorkerOption) Option {
	return func(o *options) { o.workerOptions = append(o.workerOptions, opts...) }
}

// WithClientOptions adds options for the host's client, such as a data converter.
func WithClientOptions(opts ...client.TaskHubGrpcClientOption) Option {
	return func(o *options) { o.clientOptions = append(o.clientOptions, opts...) }
}

// Host is an in-process Durable Task service with a running worker and a client.
type Host struct {
	backend    *inmemory.Server
	server     *grpc.Server
	listener   *bufconn.Listener
	clientConn *grpc.ClientConn
	client     *client.TaskHubGrpcClient
	worker     *client.TaskHubGrpcWorker
}

// New starts a [Host] for a test and closes it when the test and its subtests finish.
// It stops the test with [testing.TB.Fatal] if the host can't start.
func New(tb testing.TB, registry *task.TaskRegistry, opts ...Option) *Host {
	tb.Helper()
	host, err := NewHost(tb.Context(), registry, opts...)
	if err != nil {
		tb.Fatalf("failed to start durable task test host: %v", err)
	}
	tb.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), defaultCloseTimeout)
		defer cancel()
		if err := host.Close(ctx); err != nil {
			tb.Errorf("failed to close durable task test host: %v", err)
		}
	})
	return host
}

// NewHost starts an in-memory service and a worker for the registry. Call
// [Host.Close] to stop it. ctx bounds startup only.
func NewHost(ctx context.Context, registry *task.TaskRegistry, opts ...Option) (*Host, error) {
	if registry == nil {
		return nil, errors.New("task registry is required")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	o := options{logger: api.NewSlogLogger(slog.New(slog.DiscardHandler))}
	for _, configure := range opts {
		configure(&o)
	}

	h := &Host{
		backend:  inmemory.NewServer(),
		server:   grpc.NewServer(),
		listener: bufconn.Listen(bufferSize),
	}
	h.backend.Register(h.server)
	go func() { _ = h.server.Serve(h.listener) }()

	var err error
	h.clientConn, err = h.dial()
	if err != nil {
		h.stop()
		return nil, err
	}
	h.client = client.NewTaskHubGrpcClient(h.clientConn, o.logger, o.clientOptions...)

	factory := func(context.Context) (grpc.ClientConnInterface, io.Closer, error) {
		conn, err := h.dial()
		if err != nil {
			return nil, nil, err
		}
		return conn, conn, nil
	}
	h.worker, err = client.NewTaskHubGrpcWorkerWithConnectionFactory(factory, registry, o.logger, o.workerOptions...)
	if err != nil {
		h.stop()
		return nil, fmt.Errorf("failed to create worker: %w", err)
	}
	startupCtx, finishStartup := newStartupContext(ctx)
	startErr := h.worker.Start(startupCtx)
	startupErr := finishStartup()
	if startErr != nil {
		h.stop()
		if startupErr != nil {
			startErr = startupErr
		}
		return nil, fmt.Errorf("failed to start worker: %w", startErr)
	}
	if startupErr != nil {
		closeCtx, cancel := context.WithTimeout(context.Background(), defaultCloseTimeout)
		defer cancel()
		_ = h.worker.Shutdown(closeCtx)
		h.stop()
		return nil, fmt.Errorf("failed to start worker: %w", startupErr)
	}
	return h, nil
}

type startupContext struct {
	context.Context
	done chan struct{}

	mu       sync.Mutex
	err      error
	starting bool
}

func newStartupContext(parent context.Context) (*startupContext, func() error) {
	ctx := &startupContext{
		Context:  context.WithoutCancel(parent),
		done:     make(chan struct{}),
		starting: true,
	}
	stopParentCancel := context.AfterFunc(parent, func() {
		ctx.mu.Lock()
		defer ctx.mu.Unlock()
		if ctx.starting {
			ctx.cancelLocked(parent.Err())
		}
	})
	finish := func() error {
		ctx.mu.Lock()
		defer ctx.mu.Unlock()
		stopParentCancel()
		if err := parent.Err(); err != nil {
			ctx.cancelLocked(err)
		}
		ctx.starting = false
		return ctx.err
	}
	return ctx, finish
}

func (c *startupContext) Deadline() (time.Time, bool) {
	return time.Time{}, false
}

func (c *startupContext) Done() <-chan struct{} {
	return c.done
}

func (c *startupContext) Err() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.err
}

func (c *startupContext) cancelLocked(err error) {
	if c.err != nil {
		return
	}
	if err == nil {
		err = context.Canceled
	}
	c.err = err
	close(c.done)
}

func (h *Host) dial() (*grpc.ClientConn, error) {
	conn, err := grpc.NewClient(
		"passthrough:///durabletasktest",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return h.listener.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create in-memory connection: %w", err)
	}
	return conn, nil
}

// Client returns a client connected to the host's in-memory service.
func (h *Host) Client() *client.TaskHubGrpcClient {
	return h.client
}

// Close stops the worker and the in-memory service. All orchestration state is discarded.
// If ctx ends before in-flight work items finish, they are canceled.
func (h *Host) Close(ctx context.Context) error {
	err := h.worker.Shutdown(ctx)
	h.stop()
	return err
}

func (h *Host) stop() {
	h.backend.Close()
	if h.clientConn != nil {
		_ = h.clientConn.Close()
	}
	h.server.Stop()
	_ = h.listener.Close()
}
