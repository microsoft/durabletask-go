package api

import (
	"testing"

	"github.com/microsoft/durabletask-go/internal/protos"
	"github.com/stretchr/testify/require"
)

func Test_API_WithInstanceID_RejectsEntityFormat(t *testing.T) {
	req := &protos.CreateInstanceRequest{}

	err := WithInstanceID(InstanceID("@counter@key"))(req, DefaultDataConverter())
	require.Error(t, err)
}

func Test_API_WithInstanceID_AllowsNormalValue(t *testing.T) {
	req := &protos.CreateInstanceRequest{}

	err := WithInstanceID(InstanceID("my-instance"))(req, DefaultDataConverter())
	require.NoError(t, err)
	require.Equal(t, "my-instance", req.InstanceId)
}

func Test_API_WithOrchestrationIDReusePolicy_RejectsInvalidStatus(t *testing.T) {
	req := &protos.CreateInstanceRequest{}
	err := WithOrchestrationIDReusePolicy(&OrchestrationIDReusePolicy{
		DedupeStatuses: []OrchestrationStatus{OrchestrationStatus(99)},
	})(req, DefaultDataConverter())
	require.ErrorContains(t, err, "invalid orchestration dedupe status")
}

func Test_API_WithOrchestrationIDReusePolicy_DeduplicatesStatuses(t *testing.T) {
	req := &protos.CreateInstanceRequest{}
	err := WithOrchestrationIDReusePolicy(&OrchestrationIDReusePolicy{
		DedupeStatuses: []OrchestrationStatus{
			RUNTIME_STATUS_RUNNING,
			RUNTIME_STATUS_RUNNING,
		},
	})(req, DefaultDataConverter())
	require.NoError(t, err)
	require.NotContains(
		t,
		req.OrchestrationIdReusePolicy.ReplaceableStatus,
		protos.OrchestrationStatus_ORCHESTRATION_STATUS_RUNNING,
	)
	require.Len(t, req.OrchestrationIdReusePolicy.ReplaceableStatus, len(reusableOrchestrationStatuses)-1)
}

func Test_API_WithOrchestrationIDReusePolicy_AllStatusesReplaceNothing(t *testing.T) {
	req := &protos.CreateInstanceRequest{}
	err := WithOrchestrationIDReusePolicy(&OrchestrationIDReusePolicy{
		DedupeStatuses: append([]OrchestrationStatus(nil), reusableOrchestrationStatuses[:]...),
	})(req, DefaultDataConverter())
	require.NoError(t, err)
	require.NotNil(t, req.OrchestrationIdReusePolicy)
	require.Empty(t, req.OrchestrationIdReusePolicy.ReplaceableStatus)
}

func Test_API_ReusableOrchestrationStatusesTrackStableProtoStatuses(t *testing.T) {
	require.Len(t, reusableOrchestrationStatuses, len(protos.OrchestrationStatus_name)-1)
	for status := range protos.OrchestrationStatus_name {
		value := OrchestrationStatus(status)
		if value == RUNTIME_STATUS_CONTINUED_AS_NEW {
			require.False(t, isReusableOrchestrationStatus(value))
			continue
		}
		require.True(t, isReusableOrchestrationStatus(value), value.String())
	}
}

// WithRecursiveTerminate and WithRecursivePurge are the only way callers reach
// the DTS-side recursive flags, so both directions must survive on the wire.
func Test_API_WithRecursiveTerminate_SetsWireFlag(t *testing.T) {
	for _, recursive := range []bool{true, false} {
		req := &protos.TerminateRequest{Recursive: !recursive}
		require.NoError(t, WithRecursiveTerminate(recursive)(req, DefaultDataConverter()))
		require.Equal(t, recursive, req.Recursive)
	}
}

func Test_API_WithRecursivePurge_SetsWireFlag(t *testing.T) {
	for _, recursive := range []bool{true, false} {
		req := &protos.PurgeInstancesRequest{Recursive: !recursive}
		require.NoError(t, WithRecursivePurge(recursive)(req))
		require.Equal(t, recursive, req.Recursive)
	}
}

// IsComplete decides when DTS client wait loops stop polling, so every terminal
// status must be treated as complete and every non-terminal one as running.
func Test_API_OrchestrationMetadata_RunningAndCompleteStatuses(t *testing.T) {
	complete := []protos.OrchestrationStatus{
		protos.OrchestrationStatus_ORCHESTRATION_STATUS_COMPLETED,
		protos.OrchestrationStatus_ORCHESTRATION_STATUS_FAILED,
		protos.OrchestrationStatus_ORCHESTRATION_STATUS_TERMINATED,
		protos.OrchestrationStatus_ORCHESTRATION_STATUS_CANCELED,
	}
	running := []protos.OrchestrationStatus{
		protos.OrchestrationStatus_ORCHESTRATION_STATUS_RUNNING,
		protos.OrchestrationStatus_ORCHESTRATION_STATUS_PENDING,
		protos.OrchestrationStatus_ORCHESTRATION_STATUS_CONTINUED_AS_NEW,
		protos.OrchestrationStatus_ORCHESTRATION_STATUS_SUSPENDED,
	}

	for _, status := range complete {
		metadata := &OrchestrationMetadata{RuntimeStatus: status}
		require.Truef(t, metadata.IsComplete(), "status %v should be complete", status)
		require.Falsef(t, metadata.IsRunning(), "status %v should not be running", status)
	}
	for _, status := range running {
		metadata := &OrchestrationMetadata{RuntimeStatus: status}
		require.Falsef(t, metadata.IsComplete(), "status %v should not be complete", status)
		require.Truef(t, metadata.IsRunning(), "status %v should be running", status)
	}
}
