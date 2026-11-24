package inmemory

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/microsoft/durabletask-go/internal/protos"
)

func TestSuspendAndResumeBeforeScheduledStart(t *testing.T) {
	s := NewServer()
	defer s.Close()
	ctx := t.Context()

	_, err := s.StartInstance(ctx, &protos.CreateInstanceRequest{
		InstanceId:              "id",
		Name:                    "orchestrator",
		ScheduledStartTimestamp: timestamppb.New(time.Now().Add(time.Hour)),
	})
	require.NoError(t, err)
	getReq := &protos.GetInstanceRequest{InstanceId: "id"}
	waitForStart := func() error {
		waitCtx, cancel := context.WithTimeout(ctx, 20*time.Millisecond)
		defer cancel()
		_, err := s.WaitForInstanceStart(waitCtx, getReq)
		return err
	}

	_, err = s.SuspendInstance(ctx, &protos.SuspendRequest{InstanceId: "id"})
	require.NoError(t, err)
	assert.Equal(t, codes.DeadlineExceeded, status.Code(waitForStart()), "a suspended instance has not started")

	_, err = s.ResumeInstance(ctx, &protos.ResumeRequest{InstanceId: "id"})
	require.NoError(t, err)
	resp, err := s.GetInstance(ctx, getReq)
	require.NoError(t, err)
	assert.Equal(t, protos.OrchestrationStatus_ORCHESTRATION_STATUS_PENDING, resp.GetOrchestrationState().GetOrchestrationStatus())
	assert.Equal(t, codes.DeadlineExceeded, status.Code(waitForStart()))

	_, err = s.SuspendInstance(ctx, &protos.SuspendRequest{InstanceId: "id"})
	require.NoError(t, err)
	_, err = s.TerminateInstance(ctx, &protos.TerminateRequest{InstanceId: "id"})
	require.NoError(t, err)
	resp, err = s.WaitForInstanceStart(ctx, getReq)
	require.NoError(t, err)
	assert.Equal(t, protos.OrchestrationStatus_ORCHESTRATION_STATUS_TERMINATED, resp.GetOrchestrationState().GetOrchestrationStatus())
}
