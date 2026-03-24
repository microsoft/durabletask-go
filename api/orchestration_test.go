package api

import (
	"testing"

	"github.com/microsoft/durabletask-go/internal/protos"
	"github.com/stretchr/testify/require"
)

func Test_API_WithInstanceID_RejectsEntityFormat(t *testing.T) {
	req := &protos.CreateInstanceRequest{}

	err := WithInstanceID(InstanceID("@counter@key"))(req)
	require.Error(t, err)
}

func Test_API_WithInstanceID_AllowsNormalValue(t *testing.T) {
	req := &protos.CreateInstanceRequest{}

	err := WithInstanceID(InstanceID("my-instance"))(req)
	require.NoError(t, err)
	require.Equal(t, "my-instance", req.InstanceId)
}
