package durabletaskscheduler

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/microsoft/durabletask-go/api"
	"github.com/microsoft/durabletask-go/internal/protos"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

type retrySafetyServer struct {
	protos.UnimplementedTaskHubSidecarServiceServer
	reads          atomic.Int32
	acceptedEvents atomic.Int32
}

func (*retrySafetyServer) Hello(context.Context, *emptypb.Empty) (*emptypb.Empty, error) {
	return &emptypb.Empty{}, nil
}

func (s *retrySafetyServer) GetInstance(context.Context, *protos.GetInstanceRequest) (*protos.GetInstanceResponse, error) {
	if s.reads.Add(1) <= 3 {
		return nil, status.Error(codes.Unavailable, "temporary read failure")
	}
	return &protos.GetInstanceResponse{Exists: false}, nil
}

func (s *retrySafetyServer) RaiseEvent(context.Context, *protos.RaiseEventRequest) (*protos.RaiseEventResponse, error) {
	s.acceptedEvents.Add(1)
	// Returning without response headers reproduces a trailers-only failure
	// after the server has already accepted the non-idempotent operation.
	return nil, status.Error(codes.Unavailable, "event accepted but acknowledgement failed")
}

func TestClientRetriesReadsWithoutDuplicatingAcceptedEvents(t *testing.T) {
	server := new(retrySafetyServer)
	listener, stop := startBufconnServer(t, server)
	defer stop()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	client, err := NewClient(ctx, insecureBufconnOptions(t, listener), api.DefaultLogger())
	require.NoError(t, err)
	defer func() { require.NoError(t, client.Close()) }()

	_, err = client.FetchOrchestrationMetadata(ctx, "instance")
	require.ErrorIs(t, err, api.ErrInstanceNotFound)
	require.EqualValues(t, 4, server.reads.Load())

	err = client.RaiseEvent(ctx, "instance", "event", api.WithEventPayload("once"))
	require.Equal(t, codes.Unavailable, status.Code(err))
	require.ErrorContains(t, err, "event accepted but acknowledgement failed")
	require.EqualValues(t, 1, server.acceptedEvents.Load())
}
