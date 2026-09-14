package durabletaskscheduler_test

import (
	"context"
	"crypto/sha256"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/microsoft/durabletask-go/api"
	durabletaskclient "github.com/microsoft/durabletask-go/client"
	"github.com/microsoft/durabletask-go/payload"
	"github.com/microsoft/durabletask-go/task"
	"github.com/stretchr/testify/require"
)

func TestDTSContinueAsNewLargeCarryover(t *testing.T) {
	for _, storeKind := range []string{"memory", "file", "blob"} {
		t.Run(storeKind, func(t *testing.T) {
			options := emulatorOptions(t)
			switch storeKind {
			case "memory":
				store := payload.NewMemoryStore()
				options.LargePayloads = &api.LargePayloadOptions{Store: store, Resolver: store}
			case "file":
				store, err := payload.NewFileStore(t.TempDir())
				require.NoError(t, err)
				options.LargePayloads = &api.LargePayloadOptions{Store: store, Resolver: store}
			case "blob":
				connectionString := os.Getenv("AZURITE_CONNECTION_STRING")
				if connectionString == "" {
					t.Skip("set AZURITE_CONNECTION_STRING to run Blob carryover")
				}
				store, err := payload.NewAzureBlobStore(payload.AzureBlobStoreOptions{
					ConnectionString:  connectionString,
					Container:         "carryover" + strings.ReplaceAll(uuid.NewString(), "-", ""),
					AllowInsecureHTTP: true,
				})
				require.NoError(t, err)
				options.LargePayloads = &api.LargePayloadOptions{Store: store, Resolver: store}
			}

			name := "DTSReviewCarryover-" + uuid.NewString()
			registry := task.NewTaskRegistry()
			require.NoError(t, registry.AddOrchestratorN(name, func(ctx *task.OrchestrationContext) (any, error) {
				var continued bool
				if err := ctx.GetInput(&continued); err != nil {
					return nil, err
				}
				if !continued {
					if err := ctx.WaitForSingleEvent("continue", time.Minute).Await(nil); err != nil {
						return nil, err
					}
					ctx.ContinueAsNew(true, task.WithKeepUnprocessedEvents())
					return nil, nil
				}
				var content string
				if err := ctx.WaitForSingleEvent("large", time.Minute).Await(&content); err != nil {
					return nil, err
				}
				return fmt.Sprintf("%x", sha256.Sum256([]byte(content))), nil
			}))
			client, _ := startEmulatorWithOptions(t, options, registry, durabletaskclient.WithAutoWorkItemFilters())
			ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
			defer cancel()
			id := uniqueInstanceID("go-review-carryover")
			t.Cleanup(func() { cleanupRewindInstances(t, client, []api.InstanceID{id}) })
			_, err := client.ScheduleNewOrchestration(ctx, name, api.WithInstanceID(id), api.WithInput(false))
			require.NoError(t, err)
			started, err := client.WaitForOrchestrationStart(ctx, id)
			require.NoError(t, err)
			require.NotEmpty(t, started.ExecutionID)

			content := strings.Repeat("x", 5*1024*1024)
			require.NoError(t, client.RaiseEvent(ctx, id, "large", api.WithEventPayload(content)))
			require.NoError(t, client.RaiseEvent(ctx, id, "continue"))
			completed, err := client.WaitForOrchestrationCompletion(ctx, id, api.WithFetchPayloads(true))
			require.NoError(t, err)
			require.Equal(t, api.RUNTIME_STATUS_COMPLETED, completed.RuntimeStatus, "%+v", completed.FailureDetails)
			require.NotEqual(t, started.ExecutionID, completed.ExecutionID)
			var digest string
			require.NoError(t, completed.ReadOutput(&digest))
			require.Equal(t, fmt.Sprintf("%x", sha256.Sum256([]byte(content))), digest)
		})
	}
}

func TestDTSStrictMixedVersionRoutes(t *testing.T) {
	options := emulatorOptions(t)
	options.Versioning = &task.VersioningOptions{
		Version:        "1.0",
		DefaultVersion: "1.0",
		MatchStrategy:  task.VersionMatchStrict,
	}
	name := "DTSReviewMixed-" + uuid.NewString()
	activityName := name + "-activity"
	registry := task.NewTaskRegistry()
	for _, version := range []string{"", "1.0"} {
		require.NoError(t, registry.AddActivityNVersion(activityName, version, func(task.ActivityContext) (any, error) {
			return "activity:" + version, nil
		}))
		require.NoError(t, registry.AddOrchestratorNVersion(name, version, func(ctx *task.OrchestrationContext) (any, error) {
			var result string
			if err := ctx.CallActivity(activityName).Await(&result); err != nil {
				return nil, err
			}
			return "orchestration:" + version + "/" + result, nil
		}))
	}
	client, _ := startEmulatorWithOptions(t, options, registry,
		durabletaskclient.WithAutoWorkItemFilters(),
		durabletaskclient.WithUnversionedOrchestratorNames(name),
		durabletaskclient.WithUnversionedActivityNames(activityName))
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	for _, version := range []string{"", "1.0"} {
		id := uniqueInstanceID("go-review-routing")
		t.Cleanup(func() { cleanupRewindInstances(t, client, []api.InstanceID{id}) })
		_, err := client.ScheduleNewOrchestration(ctx, name, api.WithInstanceID(id), api.WithVersion(version))
		require.NoError(t, err)
		completed, err := client.WaitForOrchestrationCompletion(ctx, id, api.WithFetchPayloads(true))
		require.NoError(t, err)
		require.Equal(t, api.RUNTIME_STATUS_COMPLETED, completed.RuntimeStatus, "%+v", completed.FailureDetails)
		require.Equal(t, version, completed.Version)
		var result string
		require.NoError(t, completed.ReadOutput(&result))
		require.Equal(t, "orchestration:"+version+"/activity:"+version, result)
	}
}
