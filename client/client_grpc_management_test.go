package client

import (
	"context"
	"errors"
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

	queryErr error
	rewind   func(context.Context, *protos.RewindInstanceRequest) (*protos.RewindInstanceResponse, error)
	purge    func(context.Context, *protos.PurgeInstancesRequest) (*protos.PurgeInstancesResponse, error)
}

func (s *managementServer) RewindInstance(ctx context.Context, req *protos.RewindInstanceRequest) (*protos.RewindInstanceResponse, error) {
	if s.rewind == nil {
		return nil, status.Error(codes.Unimplemented, "rewind is not implemented")
	}
	return s.rewind(ctx, req)
}

func (s *managementServer) PurgeInstances(ctx context.Context, req *protos.PurgeInstancesRequest) (*protos.PurgeInstancesResponse, error) {
	if s.purge == nil {
		return nil, status.Error(codes.Unimplemented, "purge is not implemented")
	}
	return s.purge(ctx, req)
}

func (s *managementServer) QueryInstances(
	_ context.Context,
	req *protos.QueryInstancesRequest,
) (*protos.QueryInstancesResponse, error) {
	if s.queryErr != nil {
		return nil, s.queryErr
	}
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

func TestTaskHubGrpcManagementOverBufconn(t *testing.T) {
	server := &managementServer{}
	client := startQueryClient(t, server)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	query, err := client.QueryInstances(ctx, api.OrchestrationQuery{PageSize: 10})
	require.NoError(t, err)
	require.Empty(t, query.Orchestrations)
	ids, err := client.ListInstanceIDs(ctx, api.InstanceIDQuery{PageSize: 10})
	require.NoError(t, err)
	require.Empty(t, ids.InstanceIDs)
}

func TestTaskHubGrpcManagementErrorsRoundTrip(t *testing.T) {
	for _, test := range []struct {
		name string
		err  error
		want error
	}{
		{
			name: "missing-task-hub",
			err:  grpcerrors.New(codes.NotFound, ErrTaskHubNotFound.Error(), grpcerrors.ReasonTaskHubNotFound),
			want: ErrTaskHubNotFound,
		},
		{
			name: "missing-instance",
			err:  status.Error(codes.NotFound, "instance not found"),
			want: api.ErrInstanceNotFound,
		},
		{
			name: "unsupported-feature",
			err:  status.Error(codes.Unimplemented, "query is not implemented"),
			want: api.ErrFeatureNotSupported,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			client := startQueryClient(t, &managementServer{
				queryErr: test.err,
				purge: func(context.Context, *protos.PurgeInstancesRequest) (*protos.PurgeInstancesResponse, error) {
					return nil, test.err
				},
			})

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			_, err := client.QueryInstances(ctx, api.OrchestrationQuery{PageSize: 10})
			require.ErrorIs(t, err, test.want)
			require.Equal(t, status.Code(test.err), status.Code(err))

			err = client.PurgeOrchestrationState(ctx, "instance")
			require.ErrorIs(t, err, test.want)
			require.Equal(t, status.Code(test.err), status.Code(err))
		})
	}
}

func TestPurgeOrchestrationStateRequest(t *testing.T) {
	for _, test := range []struct {
		name      string
		options   []api.PurgeOptions
		recursive bool
	}{
		{name: "default"},
		{name: "recursive", options: []api.PurgeOptions{api.WithRecursivePurge(true)}, recursive: true},
		{name: "nonrecursive", options: []api.PurgeOptions{api.WithRecursivePurge(false)}},
	} {
		t.Run(test.name, func(t *testing.T) {
			requests := make(chan *protos.PurgeInstancesRequest, 1)
			client := startQueryClient(t, &managementServer{
				purge: func(_ context.Context, req *protos.PurgeInstancesRequest) (*protos.PurgeInstancesResponse, error) {
					requests <- req
					return &protos.PurgeInstancesResponse{DeletedInstanceCount: 1}, nil
				},
			})
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			require.NoError(t, client.PurgeOrchestrationState(ctx, "completed-instance", test.options...))
			req := <-requests
			require.IsType(t, &protos.PurgeInstancesRequest_InstanceId{}, req.Request)
			require.Equal(t, "completed-instance", req.GetInstanceId())
			require.Equal(t, test.recursive, req.GetRecursive())
			require.True(t, req.GetIsOrchestration(), "single-instance purge must identify an orchestration")
		})
	}
}

func TestPurgeOrchestrationStateMissingInstance(t *testing.T) {
	client := startQueryClient(t, &managementServer{
		purge: func(context.Context, *protos.PurgeInstancesRequest) (*protos.PurgeInstancesResponse, error) {
			return &protos.PurgeInstancesResponse{}, nil
		},
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	require.ErrorIs(t, client.PurgeOrchestrationState(ctx, "missing-instance"), api.ErrInstanceNotFound)
}

func TestPurgeOrchestrationStateOptionError(t *testing.T) {
	client := &TaskHubGrpcClient{}
	optionErr := errors.New("invalid purge option")
	err := client.PurgeOrchestrationState(context.Background(), "instance", func(*protos.PurgeInstancesRequest) error {
		return optionErr
	})
	require.ErrorIs(t, err, api.ErrInvalidArgument)
	require.ErrorIs(t, err, optionErr)
}
