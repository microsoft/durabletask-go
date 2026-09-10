package exporthistory

import (
	"context"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/microsoft/durabletask-go/api"
	durabletaskclient "github.com/microsoft/durabletask-go/client"
	"github.com/microsoft/durabletask-go/internal/protos"
	"github.com/microsoft/durabletask-go/task"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/types/known/emptypb"
)

// filterCapturingServer answers just enough of the DTS gRPC protocol for a
// worker to complete its handshake, and records the work-item filters the
// worker advertises.
type filterCapturingServer struct {
	protos.UnimplementedTaskHubSidecarServiceServer

	mu        sync.Mutex
	filters   *protos.WorkItemFilters
	requested chan struct{}
	once      sync.Once
}

func newFilterCapturingServer() *filterCapturingServer {
	return &filterCapturingServer{requested: make(chan struct{})}
}

func (s *filterCapturingServer) Hello(context.Context, *emptypb.Empty) (*emptypb.Empty, error) {
	return &emptypb.Empty{}, nil
}

func (s *filterCapturingServer) GetWorkItems(
	request *protos.GetWorkItemsRequest,
	stream protos.TaskHubSidecarService_GetWorkItemsServer,
) error {
	s.mu.Lock()
	s.filters = request.GetWorkItemFilters()
	s.mu.Unlock()
	s.once.Do(func() { close(s.requested) })
	<-stream.Context().Done()
	return stream.Context().Err()
}

// advertised blocks until the worker has sent its first GetWorkItems request
// and returns the filters it carried.
func (s *filterCapturingServer) advertised(t *testing.T) *protos.WorkItemFilters {
	t.Helper()
	select {
	case <-s.requested:
	case <-time.After(10 * time.Second):
		t.Fatal("the worker never requested work items")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.filters
}

// startFilterCapturingWorker registers the export history system tasks on a
// worker configured with workerOptions and returns the filters it advertises.
func startFilterCapturingWorker(
	t *testing.T,
	workerOptions ...durabletaskclient.TaskHubGrpcWorkerOption,
) *protos.WorkItemFilters {
	t.Helper()

	registry := task.NewTaskRegistry()
	require.NoError(t, registry.AddOrchestratorNVersion(
		"Application",
		"2.0",
		func(*task.OrchestrationContext) (any, error) { return nil, nil },
	))
	require.NoError(t, Register(registry, WorkerOptions{Source: newFakeSource(), Store: newMemoryStore()}))

	serverImpl := newFilterCapturingServer()
	server := grpc.NewServer()
	protos.RegisterTaskHubSidecarServiceServer(server, serverImpl)
	listener := bufconn.Listen(1024 * 1024)
	go func() { _ = server.Serve(listener) }()

	connection, err := grpc.NewClient(
		"passthrough:///export-history-filters",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return listener.Dial()
		}),
	)
	require.NoError(t, err)

	worker, err := durabletaskclient.NewTaskHubGrpcWorker(
		connection,
		registry,
		api.DefaultLogger(),
		workerOptions...,
	)
	require.NoError(t, err)
	require.NoError(t, worker.Start(context.Background()))
	t.Cleanup(func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		require.NoError(t, worker.Shutdown(shutdownCtx))
		require.NoError(t, connection.Close())
		server.Stop()
		require.NoError(t, listener.Close())
	})

	return serverImpl.advertised(t)
}

func orchestrationFilterVersions(filters *protos.WorkItemFilters) map[string][]string {
	versions := map[string][]string{}
	for _, filter := range filters.GetOrchestrations() {
		versions[filter.GetName()] = filter.GetVersions()
	}
	return versions
}

func activityFilterVersions(filters *protos.WorkItemFilters) map[string][]string {
	versions := map[string][]string{}
	for _, filter := range filters.GetActivities() {
		versions[filter.GetName()] = filter.GetVersions()
	}
	return versions
}

// TestWithExportHistoryKeepsSystemTasksRoutableUnderStrictVersioning pins the
// interaction between application default versioning and the unversioned export
// history system tasks. With [WithExportHistory] the worker's derived work-item
// filters keep advertising them unversioned, so the service dispatches export
// work; without it the strict worker version is advertised for them instead and
// the service never dispatches an unversioned export work item.
func TestWithExportHistoryKeepsSystemTasksRoutableUnderStrictVersioning(t *testing.T) {
	strictVersioning := durabletaskclient.WithTaskVersioning(task.VersioningOptions{
		Version:         "2.0",
		MatchStrategy:   task.VersionMatchStrict,
		FailureStrategy: task.VersionFailureReject,
	})

	t.Run("routable with the export history worker option", func(t *testing.T) {
		filters := startFilterCapturingWorker(
			t, strictVersioning, durabletaskclient.WithAutoWorkItemFilters(), WithExportHistory())

		orchestrations := orchestrationFilterVersions(filters)
		require.Equal(t, []string{""}, orchestrations[ExportJobOrchestratorName])
		require.Equal(t, []string{""}, orchestrations[ExecuteExportJobOperationOrchestratorName])
		// The application's own orchestrator still demands the worker version.
		require.Equal(t, []string{"2.0"}, orchestrations["Application"])

		activities := activityFilterVersions(filters)
		require.Equal(t, []string{""}, activities[ListTerminalInstancesActivityName])
		require.Equal(t, []string{""}, activities[ExportInstanceHistoryActivityName])

		entities := make([]string, 0, len(filters.GetEntities()))
		for _, filter := range filters.GetEntities() {
			entities = append(entities, filter.GetName())
		}
		require.Contains(t, entities, strings.ToLower(ExportJobEntityName))
	})

	t.Run("unroutable without it", func(t *testing.T) {
		filters := startFilterCapturingWorker(
			t, strictVersioning, durabletaskclient.WithAutoWorkItemFilters())

		orchestrations := orchestrationFilterVersions(filters)
		require.Equal(t, []string{"2.0"}, orchestrations[ExportJobOrchestratorName])
		require.Equal(t, []string{"2.0"}, orchestrations[ExecuteExportJobOperationOrchestratorName])

		activities := activityFilterVersions(filters)
		require.Equal(t, []string{"2.0"}, activities[ListTerminalInstancesActivityName])
		require.Equal(t, []string{"2.0"}, activities[ExportInstanceHistoryActivityName])
	})
}
