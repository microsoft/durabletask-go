package durabletaskscheduler_test

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/microsoft/durabletask-go/api"
	durabletaskclient "github.com/microsoft/durabletask-go/client"
	"github.com/microsoft/durabletask-go/durabletaskscheduler"
	"github.com/microsoft/durabletask-go/exporthistory"
	"github.com/microsoft/durabletask-go/task"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// exportHistoryAzuriteStore builds an export store against a throwaway Azurite
// container. It skips when Azurite is not configured.
func exportHistoryAzuriteStore(t *testing.T) (*exporthistory.AzureBlobHistoryStore, string, *azblob.Client) {
	t.Helper()
	connectionString := os.Getenv("AZURITE_CONNECTION_STRING")
	if connectionString == "" {
		t.Skip("set AZURITE_CONNECTION_STRING to run the export history end-to-end test")
	}
	var suffix [8]byte
	_, err := rand.Read(suffix[:])
	require.NoError(t, err)
	container := "dtsexport" + hex.EncodeToString(suffix[:])

	store, err := exporthistory.NewAzureBlobHistoryStore(exporthistory.AzureBlobHistoryStoreOptions{
		ConnectionString:  connectionString,
		ContainerName:     container,
		AllowInsecureHTTP: true,
	})
	require.NoError(t, err)

	// A second client reads the exported objects back without reaching into the
	// store's internals.
	reader, err := azblob.NewClientFromConnectionString(connectionString, nil)
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = reader.DeleteContainer(context.Background(), container, nil)
	})
	return store, container, reader
}

// TestDTSExportHistoryEndToEnd exports real orchestration histories from a live
// Durable Task Scheduler task hub into Azurite, covering the entity, the export
// orchestration, both activities, the durable checkpoint, and the exported
// object layout.
//
// It requires both a Durable Task Scheduler endpoint and Azurite; it skips when
// either is missing.
func TestDTSExportHistoryEndToEnd(t *testing.T) {
	store, container, blobReader := exportHistoryAzuriteStore(t)

	testCtx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	registry := task.NewTaskRegistry()
	require.NoError(t, registry.AddActivityN("DTSExportEcho", func(ctx task.ActivityContext) (any, error) {
		var input string
		if err := ctx.GetInput(&input); err != nil {
			return nil, err
		}
		return "echo:" + input, nil
	}))
	require.NoError(t, registry.AddOrchestratorN("DTSExportSubject", func(ctx *task.OrchestrationContext) (any, error) {
		var input string
		if err := ctx.GetInput(&input); err != nil {
			return nil, err
		}
		var echoed string
		if err := ctx.CallActivity("DTSExportEcho", task.WithActivityInput(input)).Await(&echoed); err != nil {
			return nil, err
		}
		return echoed, nil
	}))

	// The export activities read management APIs through the same task hub the
	// worker is connected to, so the client is created before the worker.
	options := emulatorOptions(t)
	managementClient, worker := startExportHistoryDTSWorker(t, registry, store, options)

	prefix := fmt.Sprintf("dts-export-%d/", time.Now().UTC().UnixNano())
	exportClient, err := exporthistory.NewClient(managementClient.TaskHubGrpcClient, exporthistory.ClientOptions{
		ContainerName: container,
		Prefix:        prefix,
	})
	require.NoError(t, err)

	// The task hub is shared with every other test in this package, so the
	// export window starts now and excludes instances those tests already
	// completed. Otherwise the job would export the whole hub.
	from := time.Now().UTC()
	subjects := make([]api.InstanceID, 0, 3)
	for i := 0; i < 3; i++ {
		id, err := managementClient.ScheduleNewOrchestration(
			testCtx, "DTSExportSubject", api.WithInput(fmt.Sprintf("subject-%d", i)))
		require.NoError(t, err)
		metadata, err := managementClient.WaitForOrchestrationCompletion(testCtx, id)
		require.NoError(t, err)
		require.Equal(t, api.RUNTIME_STATUS_COMPLETED, metadata.RuntimeStatus)
		subjects = append(subjects, id)
	}

	// The service's instance-ID index can lag orchestration completion, and a
	// batch job that lists an empty first page legitimately completes with
	// nothing exported. Wait until the subjects are listable so the test
	// exercises the export path rather than that race.
	listable, err := waitForListableInstances(testCtx, managementClient, from, subjects, 90*time.Second)
	require.NoError(t, err)
	if !listable {
		t.Skip("DTS emulator did not expose the completed subjects through ListInstanceIds")
	}

	jobID := strings.TrimSuffix(prefix, "/")
	job, err := exportClient.CreateJob(testCtx, exporthistory.JobCreationOptions{
		JobID:                jobID,
		Mode:                 exporthistory.ExportModeBatch,
		CompletedTimeFrom:    from,
		CompletedTimeTo:      time.Now().UTC(),
		MaxInstancesPerBatch: 2,
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cleanupCancel()
		if err := job.Delete(cleanupCtx); err != nil {
			t.Logf("failed to delete export job: %v", err)
		}
	})

	var description *exporthistory.ExportJobDescription
	require.Eventually(t, func() bool {
		description, err = job.Describe(testCtx)
		return err == nil && description.Status == exporthistory.ExportJobStatusCompleted
	}, 4*time.Minute, time.Second, "export job did not complete against the live service")

	assert.Empty(t, description.LastError)
	assert.Equal(t, description.ScannedInstances, description.ExportedInstances)
	assert.GreaterOrEqual(t, description.ScannedInstances, int64(len(subjects)))
	require.NotNil(t, description.Config)
	assert.Equal(t, container, description.Config.Destination.Container)
	assert.Equal(t, prefix, description.Config.Destination.Prefix)

	// Every subject's history landed in the destination as gzip-compressed JSONL.
	exported := listExportedInstances(t, blobReader, container, prefix)
	for _, id := range subjects {
		assert.Contains(t, exported, string(id), "instance %s was not exported", id)
	}
	for instanceID, events := range exported {
		require.NotEmpty(t, events, "instance %s exported no events", instanceID)
		var sawExecutionStarted, sawExecutionCompleted bool
		for _, event := range events {
			switch event.Type {
			case api.HistoryEventExecutionStarted:
				sawExecutionStarted = true
			case api.HistoryEventExecutionCompleted:
				sawExecutionCompleted = true
			}
		}
		assert.True(t, sawExecutionStarted, "instance %s is missing ExecutionStarted", instanceID)
		assert.True(t, sawExecutionCompleted, "instance %s is missing ExecutionCompleted", instanceID)
	}

	// Listing and per-job reads agree with the job's own description.
	listed, err := exportClient.ListJobs(testCtx, exporthistory.ExportJobQuery{JobIDPrefix: jobID})
	require.NoError(t, err)
	require.Len(t, listed.Jobs, 1)
	assert.Equal(t, exporthistory.ExportJobStatusCompleted, listed.Jobs[0].Status)

	require.NotEmpty(t, description.OrchestratorInstanceID)
	firstRunID := api.InstanceID(description.OrchestratorInstanceID)
	firstRun, err := managementClient.WaitForOrchestrationCompletion(testCtx, firstRunID)
	require.NoError(t, err)
	require.Equal(t, api.RUNTIME_STATUS_COMPLETED, firstRun.RuntimeStatus)
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cleanupCancel()
		// Recreation retains older histories; Delete cleans only the current run.
		if err := managementClient.PurgeOrchestrationState(cleanupCtx, firstRunID); err != nil {
			t.Logf("failed to purge the previous export run: %v", err)
		}
	})

	// A completed job can be recreated in place, which resets its progress and
	// starts a second run that must genuinely execute rather than leaving the
	// job Active but idle.
	require.NoError(t, job.Create(testCtx, exporthistory.JobCreationOptions{
		JobID:             jobID,
		Mode:              exporthistory.ExportModeBatch,
		CompletedTimeFrom: from,
		CompletedTimeTo:   time.Now().UTC(),
	}))
	recreated, err := job.Describe(testCtx)
	require.NoError(t, err)
	require.NotEmpty(t, recreated.OrchestratorInstanceID)
	assert.NotEqual(t, description.OrchestratorInstanceID, recreated.OrchestratorInstanceID)
	assert.Equal(t, description.CreatedAt, recreated.CreatedAt)

	retainedRun, err := managementClient.FetchOrchestrationMetadata(testCtx, firstRunID)
	require.NoError(t, err, "recreating a job must retain the previous generation's orchestration history")
	assert.Equal(t, api.RUNTIME_STATUS_COMPLETED, retainedRun.RuntimeStatus)

	var second *exporthistory.ExportJobDescription
	require.Eventually(t, func() bool {
		second, err = job.Describe(testCtx)
		return err == nil &&
			second.Status == exporthistory.ExportJobStatusCompleted &&
			second.ScannedInstances >= int64(len(subjects))
	}, 4*time.Minute, time.Second, "the recreated export job did not run again against the live service")
	assert.Empty(t, second.LastError)
	assert.Equal(t, recreated.OrchestratorInstanceID, second.OrchestratorInstanceID)
	assert.Equal(t, second.ScannedInstances, second.ExportedInstances)
	assert.True(t, second.LastModifiedAt.After(description.LastModifiedAt),
		"the recreated run must have made progress after the first one finished")
	require.True(t, worker.Running())
}

// TestDTSExportHistoryJobNotFound covers the typed not-found error against the
// live service without needing Azurite.
func TestDTSExportHistoryJobNotFound(t *testing.T) {
	options := emulatorOptions(t)
	testCtx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	managementClient, err := durabletaskscheduler.NewClient(testCtx, options, api.DefaultLogger())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, managementClient.Close()) })
	exportClient, err := exporthistory.NewClient(managementClient.TaskHubGrpcClient, exporthistory.ClientOptions{
		ContainerName: "history-exports",
	})
	require.NoError(t, err)

	var suffix [8]byte
	_, err = rand.Read(suffix[:])
	require.NoError(t, err)
	_, err = exportClient.GetJob(testCtx, "missing-"+hex.EncodeToString(suffix[:]))
	require.ErrorIs(t, err, exporthistory.ErrJobNotFound)
}

// waitForListableInstances blocks until every instance in want is visible to the
// management instance-ID query the export job uses.
func waitForListableInstances(
	ctx context.Context,
	client *durabletaskscheduler.Client,
	from time.Time,
	want []api.InstanceID,
	timeout time.Duration,
) (bool, error) {
	deadline := time.Now().Add(timeout)
	for {
		seen := map[api.InstanceID]bool{}
		token := ""
		for page := 0; page < 100; page++ {
			if ctx.Err() != nil {
				return false, ctx.Err()
			}
			if time.Now().After(deadline) {
				return false, nil
			}
			result, err := client.ListInstanceIDs(ctx, api.InstanceIDQuery{
				RuntimeStatus:     exporthistory.TerminalStatuses(),
				CompletedTimeFrom: from,
				PageSize:          100,
				ContinuationToken: token,
			})
			if err != nil {
				return false, err
			}
			if len(result.InstanceIDs) == 0 {
				break
			}
			for _, id := range result.InstanceIDs {
				seen[id] = true
			}
			if result.ContinuationToken == "" {
				break
			}
			token = result.ContinuationToken
		}
		allVisible := true
		for _, id := range want {
			if !seen[id] {
				allVisible = false
				break
			}
		}
		if allVisible {
			return true, nil
		}
		if ctx.Err() != nil || time.Now().After(deadline) {
			return false, ctx.Err()
		}
		timer := time.NewTimer(time.Second)
		select {
		case <-ctx.Done():
			timer.Stop()
			return false, ctx.Err()
		case <-timer.C:
		}
	}
}

// listExportedInstances reads every exported object under prefix and returns the
// decoded history keyed by instance ID.
func listExportedInstances(
	t *testing.T,
	client *azblob.Client,
	container string,
	prefix string,
) map[string][]api.HistoryEvent {
	t.Helper()
	ctx := context.Background()
	exported := map[string][]api.HistoryEvent{}
	pager := client.NewListBlobsFlatPager(container, &azblob.ListBlobsFlatOptions{
		Prefix:  &prefix,
		Include: azblob.ListBlobsInclude{Metadata: true},
	})
	for pager.More() {
		page, err := pager.NextPage(ctx)
		require.NoError(t, err)
		for _, item := range page.Segment.BlobItems {
			name := *item.Name
			assert.True(t, strings.HasSuffix(name, ".jsonl.gz"), name)
			instanceID := ""
			for key, value := range item.Metadata {
				if strings.EqualFold(key, "instanceIdBase64") && value != nil {
					decoded, err := base64.RawURLEncoding.DecodeString(*value)
					require.NoError(t, err, "invalid instanceIdBase64 metadata")
					instanceID = string(decoded)
				}
			}
			require.NotEmpty(t, instanceID, "exported object %s has no instanceIdBase64 metadata", name)
			// The object is an opaque gzip file, so nothing transparently
			// decompresses it and the download is always the gzip stream.
			assert.Equal(t, "application/gzip", derefBlobString(item.Properties.ContentType), name)
			assert.Empty(t, derefBlobString(item.Properties.ContentEncoding), name)

			response, err := client.DownloadStream(ctx, container, name, nil)
			require.NoError(t, err)
			payload, err := io.ReadAll(response.Body)
			require.NoError(t, err)
			require.NoError(t, response.Body.Close())
			exported[instanceID] = decodeExportedJSONL(t, payload)
		}
	}
	return exported
}

// derefBlobString reads an optional blob property.
func derefBlobString(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}

// decodeExportedJSONL decodes an exported object. The export never declares a
// content coding, so the downloaded bytes are always the gzip stream the object
// name promises and no raw-versus-decompressed guessing is needed.
func decodeExportedJSONL(t *testing.T, payload []byte) []api.HistoryEvent {
	t.Helper()
	reader, err := gzip.NewReader(bytes.NewReader(payload))
	require.NoError(t, err)
	decompressed, err := io.ReadAll(reader)
	require.NoError(t, err)
	require.NoError(t, reader.Close())

	events := []api.HistoryEvent{}
	for _, line := range strings.Split(strings.TrimRight(string(decompressed), "\n"), "\n") {
		if line == "" {
			continue
		}
		var event api.HistoryEvent
		require.NoError(t, json.Unmarshal([]byte(line), &event))
		events = append(events, event)
	}
	return events
}

// startExportHistoryDTSWorker starts a Durable Task Scheduler worker that runs
// both the application tasks in registry and the export history system tasks.
// The export activities read management APIs through the same client the caller
// uses, which is how a production worker is wired.
func startExportHistoryDTSWorker(
	t *testing.T,
	registry *task.TaskRegistry,
	store exporthistory.Store,
	options *durabletaskscheduler.Options,
) (*durabletaskscheduler.Client, *durabletaskclient.TaskHubGrpcWorker) {
	t.Helper()
	logger := api.DefaultLogger()
	managementClient, err := durabletaskscheduler.NewClient(context.Background(), options, logger)
	require.NoError(t, err)

	require.NoError(t, exporthistory.Register(registry, exporthistory.WorkerOptions{
		Source: managementClient.TaskHubGrpcClient,
		Store:  store,
	}))

	worker, err := durabletaskscheduler.NewWorker(options, registry, logger,
		durabletaskclient.WithMaxConcurrentOrchestrationWorkItems(4),
		durabletaskclient.WithMaxConcurrentActivityWorkItems(8),
		durabletaskclient.WithMaxConcurrentEntityWorkItems(4),
		durabletaskclient.WithWorkerSilentDisconnectTimeout(15*time.Second),
		exporthistory.WithExportHistory(),
	)
	require.NoError(t, err)
	require.NoError(t, worker.Start(context.Background()))

	t.Cleanup(func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := worker.Shutdown(shutdownCtx); err != nil {
			t.Logf("failed to shut down export history worker: %v", err)
		}
		require.NoError(t, managementClient.Close())
	})
	return managementClient, worker
}
