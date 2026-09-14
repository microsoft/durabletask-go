package dtssample

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/microsoft/durabletask-go/api"
	"github.com/microsoft/durabletask-go/durabletaskscheduler"
)

// NewInstanceID gives each sample run its own orchestration namespace.
func NewInstanceID(sample string) api.InstanceID {
	return api.InstanceID("sample-" + sample + "-" + uuid.NewString())
}

// RequireCompleted rejects unsuccessful terminal states as well as missing results.
func RequireCompleted(metadata *api.OrchestrationMetadata) error {
	if metadata == nil {
		return errors.New("sample received no orchestration metadata")
	}
	if metadata.RuntimeStatus != api.RUNTIME_STATUS_COMPLETED {
		return fmt.Errorf("orchestration %s ended in %s: %v",
			metadata.InstanceID, metadata.RuntimeStatus, metadata.FailureDetails)
	}
	return nil
}

// Cleanup terminates and purges only the supplied, sample-owned instance IDs and
// their children. Call it before shutting down the worker. Entity state, schedules,
// export jobs, and storage objects need their own sample-specific cleanup.
func Cleanup(client *durabletaskscheduler.Client, instanceIDs ...api.InstanceID) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	var failures []error
	for _, id := range instanceIDs {
		metadata, err := client.FetchOrchestrationMetadata(ctx, id, api.WithFetchPayloads(false))
		if errors.Is(err, api.ErrInstanceNotFound) {
			continue
		}
		if err == nil && metadata == nil {
			err = errors.New("missing orchestration metadata")
		}
		if err == nil && !metadata.IsComplete() {
			err = client.TerminateOrchestration(ctx, id)
			if err == nil {
				_, err = client.WaitForOrchestrationCompletion(ctx, id)
			}
		}
		if err == nil {
			err = client.PurgeOrchestrationState(ctx, id, api.WithRecursivePurge(true))
		}
		if err != nil && !errors.Is(err, api.ErrInstanceNotFound) {
			failures = append(failures, fmt.Errorf("clean up %s: %w", id, err))
		}
	}
	return errors.Join(failures...)
}
