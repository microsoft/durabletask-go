package client

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/microsoft/durabletask-go/api"
	"github.com/microsoft/durabletask-go/internal/protos"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

func TestRewindInstanceRequest(t *testing.T) {
	for _, test := range []struct {
		name   string
		reason *wrapperspb.StringValue
	}{
		{"omitted reason", nil},
		{"empty reason", wrapperspb.String("")},
		{"reason", wrapperspb.String("dependency repaired")},
	} {
		t.Run(test.name, func(t *testing.T) {
			requests := make(chan *protos.RewindInstanceRequest, 1)
			client := startQueryClient(t, &managementServer{
				rewind: func(_ context.Context, req *protos.RewindInstanceRequest) (*protos.RewindInstanceResponse, error) {
					requests <- req
					return &protos.RewindInstanceResponse{}, nil
				},
			})
			var options []api.RewindOptions
			if test.reason != nil {
				options = append(options, api.WithRewindReason(test.reason.Value))
			}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			// The fake service implements no metadata/wait RPC. Rewind must only enqueue.
			require.NoError(t, client.RewindInstance(ctx, "failed-instance", options...))
			req := <-requests
			require.Equal(t, "failed-instance", req.InstanceId)
			require.Equal(t, test.reason, req.Reason)
		})
	}
}

func TestRewindInstanceValidation(t *testing.T) {
	// Invalid requests must not reach the transport.
	client := &TaskHubGrpcClient{}
	for _, id := range []api.InstanceID{"", "@counter@key", "@", "@not-an-orchestration"} {
		require.ErrorIs(t, client.RewindInstance(context.Background(), id), api.ErrInvalidArgument)
	}
	optionErr := errors.New("invalid rewind option")
	err := client.RewindInstance(context.Background(), "instance", func(*protos.RewindInstanceRequest) error {
		return optionErr
	})
	require.ErrorIs(t, err, api.ErrInvalidArgument)
	require.ErrorIs(t, err, optionErr)
}

func TestRewindInstanceRPCErrors(t *testing.T) {
	for _, test := range []struct {
		code codes.Code
		want error
	}{
		{codes.InvalidArgument, api.ErrInvalidArgument},
		{codes.NotFound, api.ErrInstanceNotFound},
		{codes.FailedPrecondition, api.ErrInvalidState},
		{codes.Unimplemented, api.ErrFeatureNotSupported},
		{codes.Canceled, context.Canceled},
		{codes.DeadlineExceeded, context.DeadlineExceeded},
	} {
		t.Run(test.code.String(), func(t *testing.T) {
			client := startQueryClient(t, &managementServer{
				rewind: func(context.Context, *protos.RewindInstanceRequest) (*protos.RewindInstanceResponse, error) {
					return nil, status.Error(test.code, "rewind rejected")
				},
			})
			err := client.RewindInstance(context.Background(), "instance")
			require.ErrorIs(t, err, test.want)
			require.Equal(t, test.code, status.Code(err))
		})
	}
}

func TestRewindInstanceCancellation(t *testing.T) {
	entered := make(chan struct{})
	client := startQueryClient(t, &managementServer{
		rewind: func(ctx context.Context, _ *protos.RewindInstanceRequest) (*protos.RewindInstanceResponse, error) {
			close(entered)
			<-ctx.Done()
			return nil, status.FromContextError(ctx.Err()).Err()
		},
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	result := make(chan error, 1)
	go func() { result <- client.RewindInstance(ctx, "instance") }()
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal("rewind did not reach the service")
	}
	cancel()
	err := <-result
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, codes.Canceled, status.Code(err))
}

func TestRewindInstanceAlreadyCanceled(t *testing.T) {
	client := startQueryClient(t, &managementServer{})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.ErrorIs(t, client.RewindInstance(ctx, "instance"), context.Canceled)
}
