package client

import (
	"context"
	"testing"

	"github.com/microsoft/durabletask-go/api"
	"github.com/microsoft/durabletask-go/internal/protos"
	"github.com/stretchr/testify/require"
)

func TestGrpcClientDefaultVersionAndExplicitUnversionedOverride(t *testing.T) {
	scheduler := new(largePayloadSchedulerClient)
	client := &TaskHubGrpcClient{
		client:         scheduler,
		defaultVersion: "v2",
	}

	_, err := client.ScheduleNewOrchestration(context.Background(), "orchestration")
	require.NoError(t, err)
	require.Equal(t, "v2", scheduler.start.GetVersion().GetValue())

	_, err = client.ScheduleNewOrchestration(context.Background(), "orchestration", api.WithVersion(""))
	require.NoError(t, err)
	require.NotNil(t, scheduler.start.Version)
	require.Empty(t, scheduler.start.GetVersion().GetValue())
}

func TestOrchestrationIDReusePolicyUsesCurrentDedupeSemantics(t *testing.T) {
	tests := []struct {
		name        string
		statuses    []api.OrchestrationStatus
		wantPolicy  bool
		replaceable []protos.OrchestrationStatus
	}{
		{
			name:       "nil uses service default",
			statuses:   nil,
			wantPolicy: false,
		},
		{
			name:       "empty allows every reusable status",
			statuses:   []api.OrchestrationStatus{},
			wantPolicy: true,
			replaceable: []protos.OrchestrationStatus{
				protos.OrchestrationStatus_ORCHESTRATION_STATUS_RUNNING,
				protos.OrchestrationStatus_ORCHESTRATION_STATUS_COMPLETED,
				protos.OrchestrationStatus_ORCHESTRATION_STATUS_FAILED,
				protos.OrchestrationStatus_ORCHESTRATION_STATUS_CANCELED,
				protos.OrchestrationStatus_ORCHESTRATION_STATUS_TERMINATED,
				protos.OrchestrationStatus_ORCHESTRATION_STATUS_PENDING,
				protos.OrchestrationStatus_ORCHESTRATION_STATUS_SUSPENDED,
			},
		},
		{
			name:       "dedupe statuses are removed from replaceable statuses",
			statuses:   []api.OrchestrationStatus{api.RUNTIME_STATUS_RUNNING, api.RUNTIME_STATUS_PENDING},
			wantPolicy: true,
			replaceable: []protos.OrchestrationStatus{
				protos.OrchestrationStatus_ORCHESTRATION_STATUS_COMPLETED,
				protos.OrchestrationStatus_ORCHESTRATION_STATUS_FAILED,
				protos.OrchestrationStatus_ORCHESTRATION_STATUS_CANCELED,
				protos.OrchestrationStatus_ORCHESTRATION_STATUS_TERMINATED,
				protos.OrchestrationStatus_ORCHESTRATION_STATUS_SUSPENDED,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := &protos.CreateInstanceRequest{}
			configure := api.WithOrchestrationIDReusePolicy(&api.OrchestrationIDReusePolicy{
				DedupeStatuses: tt.statuses,
			})
			require.NoError(t, configure(req, api.DefaultDataConverter()))
			if tt.wantPolicy {
				require.NotNil(t, req.OrchestrationIdReusePolicy)
				require.Equal(t, tt.replaceable, req.OrchestrationIdReusePolicy.ReplaceableStatus)
			} else {
				require.Nil(t, req.OrchestrationIdReusePolicy)
			}
		})
	}
}
