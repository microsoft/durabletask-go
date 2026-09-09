package client

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/microsoft/durabletask-go/api"
	"github.com/microsoft/durabletask-go/internal/grpcerrors"
	"github.com/microsoft/durabletask-go/internal/protos"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// managementServer is a fake task hub service that answers only the management
// RPCs these tests drive over the wire. It stores nothing: the assertions are
// about the gRPC round trip performed by TaskHubGrpcClient.
type managementServer struct {
	protos.UnimplementedTaskHubSidecarServiceServer

	mu      sync.Mutex
	created bool
	deleted bool
}

func (s *managementServer) CreateTaskHub(
	context.Context,
	*protos.CreateTaskHubRequest,
) (*protos.CreateTaskHubResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.created = true
	return &protos.CreateTaskHubResponse{}, nil
}

func (s *managementServer) DeleteTaskHub(
	context.Context,
	*protos.DeleteTaskHubRequest,
) (*protos.DeleteTaskHubResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.deleted = true
	return &protos.DeleteTaskHubResponse{}, nil
}

func (*managementServer) QueryInstances(
	_ context.Context,
	req *protos.QueryInstancesRequest,
) (*protos.QueryInstancesResponse, error) {
	if req.GetQuery().GetMaxInstanceCount() <= 0 {
		return nil, status.Error(codes.InvalidArgument, "page size must be positive")
	}
	return &protos.QueryInstancesResponse{}, nil
}

func (*managementServer) ListInstanceIds(
	_ context.Context,
	req *protos.ListInstanceIdsRequest,
) (*protos.ListInstanceIdsResponse, error) {
	if req.GetPageSize() <= 0 {
		return nil, status.Error(codes.InvalidArgument, "page size must be positive")
	}
	return &protos.ListInstanceIdsResponse{}, nil
}

func (s *managementServer) lifecycleCalls() (bool, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.created, s.deleted
}

// lifecycleErrorServer fails both lifecycle RPCs with the durable error reasons
// a task hub service attaches, so the client-side sentinel mapping is observed.
type lifecycleErrorServer struct {
	protos.UnimplementedTaskHubSidecarServiceServer
}

func (*lifecycleErrorServer) CreateTaskHub(
	context.Context,
	*protos.CreateTaskHubRequest,
) (*protos.CreateTaskHubResponse, error) {
	return nil, grpcerrors.New(
		codes.AlreadyExists,
		ErrTaskHubExists.Error(),
		grpcerrors.ReasonTaskHubExists,
	)
}

func (*lifecycleErrorServer) DeleteTaskHub(
	context.Context,
	*protos.DeleteTaskHubRequest,
) (*protos.DeleteTaskHubResponse, error) {
	return nil, grpcerrors.New(
		codes.NotFound,
		ErrTaskHubNotFound.Error(),
		grpcerrors.ReasonTaskHubNotFound,
	)
}

// bareLifecycleErrorServer omits the durable error reason detail so the
// status-code-only fallback in clientRPCError is exercised too.
type bareLifecycleErrorServer struct {
	protos.UnimplementedTaskHubSidecarServiceServer
}

func (*bareLifecycleErrorServer) CreateTaskHub(
	context.Context,
	*protos.CreateTaskHubRequest,
) (*protos.CreateTaskHubResponse, error) {
	return nil, status.Error(codes.AlreadyExists, "task hub already exists")
}

func (*bareLifecycleErrorServer) DeleteTaskHub(
	context.Context,
	*protos.DeleteTaskHubRequest,
) (*protos.DeleteTaskHubResponse, error) {
	return nil, status.Error(codes.NotFound, "task hub not found")
}

func TestTaskHubGrpcManagementOverBufconn(t *testing.T) {
	server := &managementServer{}
	client := startQueryClient(t, server)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	require.NoError(t, client.CreateTaskHub(ctx))
	query, err := client.QueryInstances(ctx, api.OrchestrationQuery{PageSize: 10})
	require.NoError(t, err)
	require.Empty(t, query.Orchestrations)
	ids, err := client.ListInstanceIDs(ctx, api.InstanceIDQuery{PageSize: 10})
	require.NoError(t, err)
	require.Empty(t, ids.InstanceIDs)
	require.NoError(t, client.DeleteTaskHub(ctx))

	created, deleted := server.lifecycleCalls()
	require.True(t, created)
	require.True(t, deleted)
}

func TestTaskHubLifecycleErrorsRoundTripOverGRPC(t *testing.T) {
	for _, test := range []struct {
		name   string
		server protos.TaskHubSidecarServiceServer
	}{
		{name: "with-durable-error-reason", server: &lifecycleErrorServer{}},
		{name: "status-code-only", server: &bareLifecycleErrorServer{}},
	} {
		t.Run(test.name, func(t *testing.T) {
			client := startQueryClient(t, test.server)

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			err := client.CreateTaskHub(ctx)
			require.True(t, errors.Is(err, ErrTaskHubExists), "CreateTaskHub() error = %v", err)
			err = client.DeleteTaskHub(ctx)
			require.True(t, errors.Is(err, ErrTaskHubNotFound), "DeleteTaskHub() error = %v", err)
		})
	}
}
