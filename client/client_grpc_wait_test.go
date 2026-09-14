package client

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/microsoft/durabletask-go/api"
	"github.com/microsoft/durabletask-go/internal/protos"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type missingWaitSchedulerClient struct {
	protos.TaskHubSidecarServiceClient
	startCalls      atomic.Int32
	completionCalls atomic.Int32
}

func (c *missingWaitSchedulerClient) WaitForInstanceStart(
	context.Context,
	*protos.GetInstanceRequest,
	...grpc.CallOption,
) (*protos.GetInstanceResponse, error) {
	c.startCalls.Add(1)
	return nil, status.Error(codes.NotFound, "missing instance")
}

func (c *missingWaitSchedulerClient) WaitForInstanceCompletion(
	context.Context,
	*protos.GetInstanceRequest,
	...grpc.CallOption,
) (*protos.GetInstanceResponse, error) {
	c.completionCalls.Add(1)
	return nil, status.Error(codes.NotFound, "missing instance")
}

func TestWaitForMissingInstanceReturnsImmediately(t *testing.T) {
	scheduler := new(missingWaitSchedulerClient)
	client := &TaskHubGrpcClient{client: scheduler}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	_, err := client.WaitForOrchestrationStart(ctx, "missing")
	require.ErrorIs(t, err, api.ErrInstanceNotFound)
	require.False(t, errors.Is(err, context.DeadlineExceeded))
	require.EqualValues(t, 1, scheduler.startCalls.Load())

	_, err = client.WaitForOrchestrationCompletion(ctx, "missing")
	require.ErrorIs(t, err, api.ErrInstanceNotFound)
	require.False(t, errors.Is(err, context.DeadlineExceeded))
	require.EqualValues(t, 1, scheduler.completionCalls.Load())
}
