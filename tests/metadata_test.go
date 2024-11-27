package tests

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/dapr/durabletask-go/api"
	"github.com/dapr/durabletask-go/api/protos"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

func Test_OrchestrationMetadata_Serialization(t *testing.T) {
	metadata := api.NewOrchestrationMetadata(
		api.InstanceID("abc123"),
		"MyOrchestration",
		protos.OrchestrationStatus_ORCHESTRATION_STATUS_TERMINATED,
		time.Now().UTC(),
		time.Now().UTC().Add(1*time.Minute),
		"\"World\"",
		"\"Hello, World!\"",
		"",
		&protos.TaskFailureDetails{
			ErrorType:    "MyError",
			ErrorMessage: "Kah-BOOOOM!!!",
			StackTrace:   wrapperspb.String("stack trace"),
			InnerFailure: &protos.TaskFailureDetails{
				ErrorType:    "InnerError",
				ErrorMessage: "Fuse lit",
			},
		})

	if bytes, err := json.Marshal(metadata); assert.NoError(t, err) {
		metadata2 := new(api.OrchestrationMetadata)
		if err := json.Unmarshal(bytes, metadata2); assert.NoError(t, err) {
			assert.Equal(t, metadata.InstanceID, metadata2.InstanceID)
			assert.Equal(t, metadata.Name, metadata2.Name)
			assert.Equal(t, metadata.RuntimeStatus, metadata2.RuntimeStatus)
			assert.Equal(t, metadata.CreatedAt, metadata2.CreatedAt)
			assert.Equal(t, metadata.LastUpdatedAt, metadata2.LastUpdatedAt)
			assert.Equal(t, metadata.SerializedInput, metadata2.SerializedInput)
			assert.Equal(t, metadata.SerializedOutput, metadata2.SerializedOutput)
			assert.Equal(t, metadata.SerializedCustomStatus, metadata2.SerializedCustomStatus)
			if assert.NotNil(t, metadata2.FailureDetails) {
				assert.Equal(t, metadata.FailureDetails.ErrorType, metadata2.FailureDetails.ErrorType)
				assert.Equal(t, metadata.FailureDetails.ErrorMessage, metadata2.FailureDetails.ErrorMessage)
				assert.Equal(t, metadata.FailureDetails.StackTrace.GetValue(), metadata2.FailureDetails.StackTrace.GetValue())
				if assert.NotNil(t, metadata2.FailureDetails.InnerFailure) {
					assert.Equal(t, metadata.FailureDetails.InnerFailure.ErrorType, metadata2.FailureDetails.InnerFailure.ErrorType)
					assert.Equal(t, metadata.FailureDetails.InnerFailure.ErrorMessage, metadata2.FailureDetails.InnerFailure.ErrorMessage)
					assert.Nil(t, metadata2.FailureDetails.InnerFailure.StackTrace)
					assert.Nil(t, metadata2.FailureDetails.InnerFailure.InnerFailure)
				}
			}
		}
	}
}
