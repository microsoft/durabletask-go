package tests

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/microsoft/durabletask-go/api"
	"github.com/microsoft/durabletask-go/internal/protos"
	"github.com/stretchr/testify/assert"
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
		&api.FailureDetails{
			ErrorType:      "MyError",
			ErrorMessage:   "Kah-BOOOOM!!!",
			StackTrace:     "stack trace",
			IsNonRetriable: true,
			Properties:     map[string]any{"attempt": float64(2)},
			InnerFailure: &api.FailureDetails{
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
				assert.Equal(t, metadata.FailureDetails.StackTrace, metadata2.FailureDetails.StackTrace)
				assert.Equal(t, metadata.FailureDetails.IsNonRetriable, metadata2.FailureDetails.IsNonRetriable)
				assert.Equal(t, metadata.FailureDetails.Properties, metadata2.FailureDetails.Properties)
				if assert.NotNil(t, metadata2.FailureDetails.InnerFailure) {
					assert.Equal(t, metadata.FailureDetails.InnerFailure.ErrorType, metadata2.FailureDetails.InnerFailure.ErrorType)
					assert.Equal(t, metadata.FailureDetails.InnerFailure.ErrorMessage, metadata2.FailureDetails.InnerFailure.ErrorMessage)
					assert.Empty(t, metadata2.FailureDetails.InnerFailure.StackTrace)
					assert.Nil(t, metadata2.FailureDetails.InnerFailure.InnerFailure)
				}
			}
		}
	}
}
