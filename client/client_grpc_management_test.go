package client

import (
	"context"
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
			client := startQueryClient(t, &managementServer{queryErr: test.err})

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			_, err := client.QueryInstances(ctx, api.OrchestrationQuery{PageSize: 10})
			require.ErrorIs(t, err, test.want)
			require.Equal(t, status.Code(test.err), status.Code(err))
		})
	}
}
