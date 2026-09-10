// Command exporthistory exercises the public exporthistory package against
// known source orchestrations and verifies the downloaded Azure Blob output.
package main

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/bloberror"
	"github.com/microsoft/durabletask-go/api"
	durabletaskclient "github.com/microsoft/durabletask-go/client"
	"github.com/microsoft/durabletask-go/durabletaskscheduler"
	"github.com/microsoft/durabletask-go/exporthistory"
	"github.com/microsoft/durabletask-go/samples/internal/dtssample"
	"github.com/microsoft/durabletask-go/task"
)

type scenario struct {
	name     string
	mode     exporthistory.ExportMode
	format   exporthistory.ExportFormatKind
	recreate bool
}

type sourceInput struct {
	Scenario string `json:"scenario"`
	Index    int    `json:"index"`
}

type sourceOutput struct {
	Value string `json:"value"`
}

type sourceExecution struct {
	id          api.InstanceID
	executionID string
	output      string
}

type sampleApp struct {
	client *durabletaskscheduler.Client
	worker *durabletaskclient.TaskHubGrpcWorker
}

type storageSettings struct {
	connectionString  string
	container         string
	deleteContainer   bool
	allowInsecureHTTP bool
}

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
	fmt.Println("SAMPLE_OK exporthistory")
}

func run() (err error) {
	scenarioName := flag.String("scenario", "all", "scenario to run: all, batch-jsonl, batch-json, continuous-jsonl, continuous-json")
	flag.Parse()
	scenarios, err := selectScenarios(*scenarioName)
	if err != nil {
		return err
	}
	settings, err := readStorageSettings()
	if err != nil {
		return err
	}
	blobClient, err := azblob.NewClientFromConnectionString(settings.connectionString, nil)
	if err != nil {
		return fmt.Errorf("create Azure Blob reader: %w", err)
	}
	store, err := exporthistory.NewAzureBlobHistoryStore(exporthistory.AzureBlobHistoryStoreOptions{
		ConnectionString:  settings.connectionString,
		ContainerName:     settings.container,
		AllowInsecureHTTP: settings.allowInsecureHTTP,
	})
	if err != nil {
		return fmt.Errorf("create Azure Blob history store: %w", err)
	}
	var ownedPrefixes []string
	cleanupStorage := func() error {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		if settings.deleteContainer {
			_, cleanupErr := blobClient.DeleteContainer(cleanupCtx, settings.container, nil)
			if bloberror.HasCode(cleanupErr, bloberror.ContainerNotFound) {
				return nil
			}
			return cleanupErr
		}
		var failures []error
		for _, prefix := range ownedPrefixes {
			failures = append(failures, deleteBlobsWithPrefix(cleanupCtx, blobClient, settings.container, prefix))
		}
		return errors.Join(failures...)
	}

	options, err := dtssample.Options()
	if err != nil {
		return err
	}
	if err := requireIsolatedTaskHub(options); err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	app, err := startExportHistoryApp(ctx, options, store)
	if err != nil {
		err = errors.Join(err, cleanupStorage())
		return err
	}
	var ownedIDs []api.InstanceID
	var retainedExportRuns []api.InstanceID
	defer func() {
		err = errors.Join(
			err,
			dtssample.Cleanup(app.client, retainedExportRuns...),
			dtssample.Cleanup(app.client, ownedIDs...),
			cleanupStorage(),
			app.shutdown(),
		)
	}()

	for _, scenario := range scenarios {
		result, scenarioErr := runScenario(ctx, app.client, blobClient, settings.container, scenario)
		if scenarioErr != nil {
			return scenarioErr
		}
		ownedIDs = append(ownedIDs, result.sourceIDs...)
		retainedExportRuns = append(retainedExportRuns, result.retainedRuns...)
		ownedPrefixes = append(ownedPrefixes, result.prefix)
		fmt.Printf("verified %s with %d exported histories\n", scenario.name, result.exported)
	}
	return nil
}

type scenarioResult struct {
	sourceIDs    []api.InstanceID
	retainedRuns []api.InstanceID
	prefix       string
	exported     int
}

func runScenario(
	ctx context.Context,
	client *durabletaskscheduler.Client,
	blobClient *azblob.Client,
	container string,
	scenario scenario,
) (result scenarioResult, err error) {
	var sources []sourceExecution
	var retainedRuns []api.InstanceID
	var prefix string
	defer func() {
		if err != nil {
			cleanupCtx, cancel := context.WithTimeout(context.Background(), time.Minute)
			defer cancel()
			err = errors.Join(
				err,
				dtssample.Cleanup(client, sourceIDs(sources)...),
				dtssample.Cleanup(client, retainedRuns...),
				deleteBlobsWithPrefix(cleanupCtx, blobClient, container, prefix),
			)
		}
	}()
	from := time.Now().UTC().Add(-time.Second)
	sources, err = runKnownSources(ctx, client, scenario.name, 3)
	if err != nil {
		return scenarioResult{}, err
	}
	if err := waitForListableInstances(ctx, client, from, sourceIDs(sources), 90*time.Second); err != nil {
		return scenarioResult{}, err
	}
	format := exporthistory.ExportFormat{Kind: scenario.format}
	suffix, err := compactID()
	if err != nil {
		return scenarioResult{}, err
	}
	jobID := strings.ReplaceAll("sample-"+scenario.name+"-"+suffix, "_", "-")
	prefix = "samples/exporthistory/" + jobID + "/"
	exportClient, err := exporthistory.NewClient(client.TaskHubGrpcClient, exporthistory.ClientOptions{
		ContainerName: container,
		Prefix:        prefix,
	})
	if err != nil {
		return scenarioResult{}, err
	}
	job, err := exportClient.JobClient(jobID)
	if err != nil {
		return scenarioResult{}, err
	}
	deleteJob := true
	defer func() {
		if deleteJob {
			cleanupCtx, cancel := context.WithTimeout(context.Background(), time.Minute)
			defer cancel()
			if cleanupErr := job.Delete(cleanupCtx); cleanupErr != nil {
				err = errors.Join(err, fmt.Errorf("cleanup export job %s: %w", jobID, cleanupErr))
			}
		}
	}()
	if _, err := exportClient.CreateJob(ctx, exporthistory.JobCreationOptions{
		JobID:                jobID,
		Mode:                 scenario.mode,
		CompletedTimeFrom:    from,
		CompletedTimeTo:      completedTimeTo(scenario.mode),
		Format:               &format,
		MaxInstancesPerBatch: 2,
	}); err != nil {
		return scenarioResult{}, err
	}

	description, err := waitForExport(ctx, job, scenario.mode, len(sources))
	if err != nil {
		return scenarioResult{}, err
	}
	if err := verifyJobLifecycle(ctx, exportClient, jobID, description, scenario.mode); err != nil {
		return scenarioResult{}, err
	}
	exported, err := waitForDownloadedHistories(ctx, blobClient, container, prefix, scenario.format, sources)
	if err != nil {
		return scenarioResult{}, err
	}
	exportedCount := len(exported)

	if scenario.mode == exporthistory.ExportModeContinuous {
		lateSources, err := runKnownSources(ctx, client, scenario.name+"-late", 1)
		sources = append(sources, lateSources...)
		if err != nil {
			return scenarioResult{}, err
		}
		if err := waitForListableInstances(ctx, client, from, sourceIDs(lateSources), 90*time.Second); err != nil {
			return scenarioResult{}, err
		}
		lateDescription, err := waitForExport(ctx, job, scenario.mode, len(sources))
		if err != nil {
			return scenarioResult{}, err
		}
		if lateDescription.Status != exporthistory.ExportJobStatusActive {
			return scenarioResult{}, fmt.Errorf("continuous job %s status=%s after late export, want Active",
				jobID, lateDescription.Status)
		}
		if lateDescription.OrchestratorInstanceID != description.OrchestratorInstanceID {
			return scenarioResult{}, fmt.Errorf("continuous job %s changed run ID from %s to %s",
				jobID, description.OrchestratorInstanceID, lateDescription.OrchestratorInstanceID)
		}
		if err := verifyJobLifecycle(ctx, exportClient, jobID, lateDescription, scenario.mode); err != nil {
			return scenarioResult{}, err
		}
		exported, err = waitForDownloadedHistories(ctx, blobClient, container, prefix, scenario.format, sources)
		if err != nil {
			return scenarioResult{}, err
		}
		exportedCount = len(exported)
		description = lateDescription
	}

	result = scenarioResult{sourceIDs: sourceIDs(sources), prefix: prefix, exported: exportedCount}

	if scenario.recreate {
		firstRunID := api.InstanceID(description.OrchestratorInstanceID)
		if firstRunID == "" {
			return scenarioResult{}, errors.New("completed export job did not report its run orchestration ID")
		}
		retainedRuns = append(retainedRuns, firstRunID)
		result.retainedRuns = append(result.retainedRuns, firstRunID)
		if err := job.Create(ctx, exporthistory.JobCreationOptions{
			JobID:                jobID,
			Mode:                 scenario.mode,
			CompletedTimeFrom:    from,
			CompletedTimeTo:      completedTimeTo(scenario.mode),
			Format:               &format,
			MaxInstancesPerBatch: 2,
		}); err != nil {
			return scenarioResult{}, fmt.Errorf("recreate job %s: %w", jobID, err)
		}
		recreated, err := waitForExport(ctx, job, scenario.mode, len(sources))
		if err != nil {
			return scenarioResult{}, err
		}
		if recreated.OrchestratorInstanceID == description.OrchestratorInstanceID {
			return scenarioResult{}, errors.New("recreated job reused the previous run ID")
		}
		retained, err := client.FetchOrchestrationMetadata(ctx, firstRunID)
		if err != nil {
			return scenarioResult{}, fmt.Errorf("previous export generation was not retained: %w", err)
		}
		if retained.RuntimeStatus != api.RUNTIME_STATUS_COMPLETED {
			return scenarioResult{}, fmt.Errorf("previous export generation status=%s, want COMPLETED", retained.RuntimeStatus)
		}
		if _, err := waitForDownloadedHistories(ctx, blobClient, container, prefix, scenario.format, sources); err != nil {
			return scenarioResult{}, err
		}
	}

	if err := job.Delete(ctx); err != nil {
		return scenarioResult{}, fmt.Errorf("delete job %s: %w", jobID, err)
	}
	if _, err := exportClient.GetJob(ctx, jobID); !errors.Is(err, exporthistory.ErrJobNotFound) {
		if err != nil {
			return scenarioResult{}, fmt.Errorf("read deleted job %s: %w", jobID, err)
		}
		return scenarioResult{}, fmt.Errorf("deleted job %s remains readable", jobID)
	}
	deleteJob = false
	return result, nil
}

func startExportHistoryApp(
	ctx context.Context,
	options *durabletaskscheduler.Options,
	store exporthistory.Store,
) (*sampleApp, error) {
	registry := task.NewTaskRegistry()
	if err := registry.AddOrchestratorN("ExportHistoryKnownSource", exportHistoryKnownSource); err != nil {
		return nil, err
	}
	if err := registry.AddActivityN("ExportHistoryEcho", exportHistoryEcho); err != nil {
		return nil, err
	}
	logger := api.DefaultLogger()
	client, err := durabletaskscheduler.NewClient(ctx, options, logger)
	if err != nil {
		return nil, fmt.Errorf("create DTS client: %w", err)
	}
	if err := exporthistory.Register(registry, exporthistory.WorkerOptions{
		Source: client.TaskHubGrpcClient,
		Store:  store,
	}); err != nil {
		return nil, errors.Join(err, client.Close())
	}
	worker, err := durabletaskscheduler.NewWorker(options, registry, logger,
		durabletaskclient.WithAutoWorkItemFilters(),
		durabletaskclient.WithMaxConcurrentOrchestrationWorkItems(4),
		durabletaskclient.WithMaxConcurrentActivityWorkItems(8),
		exporthistory.WithExportHistory(),
	)
	if err != nil {
		return nil, errors.Join(fmt.Errorf("create DTS worker: %w", err), client.Close())
	}
	if err := worker.Start(ctx); err != nil {
		return nil, errors.Join(fmt.Errorf("start DTS worker: %w", err), client.Close())
	}
	return &sampleApp{client: client, worker: worker}, nil
}

func (a *sampleApp) shutdown() error {
	if a == nil {
		return nil
	}
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	return errors.Join(a.worker.Shutdown(shutdownCtx), a.client.Close())
}

func runKnownSources(
	ctx context.Context,
	client *durabletaskscheduler.Client,
	scenario string,
	count int,
) ([]sourceExecution, error) {
	sources := make([]sourceExecution, 0, count)
	for i := 0; i < count; i++ {
		id := dtssample.NewInstanceID("exporthistory-" + scenario)
		sources = append(sources, sourceExecution{id: id})
		if _, err := client.ScheduleNewOrchestration(ctx, "ExportHistoryKnownSource",
			api.WithInstanceID(id),
			api.WithInput(sourceInput{Scenario: scenario, Index: i}),
			api.WithTags(map[string]string{"sample": "exporthistory", "scenario": scenario})); err != nil {
			return sources, err
		}
		metadata, err := client.WaitForOrchestrationCompletion(ctx, id, api.WithFetchPayloads(true))
		if err != nil {
			return sources, err
		}
		if err := dtssample.RequireCompleted(metadata); err != nil {
			return sources, err
		}
		var output sourceOutput
		if err := metadata.ReadOutput(&output); err != nil {
			return sources, err
		}
		want := fmt.Sprintf("history:%s:%d", scenario, i)
		if output.Value != want {
			return sources, fmt.Errorf("%s output=%q, want %q", id, output.Value, want)
		}
		sources[len(sources)-1] = sourceExecution{id: id, executionID: metadata.ExecutionID, output: output.Value}
	}
	return sources, nil
}

func exportHistoryKnownSource(ctx *task.OrchestrationContext) (any, error) {
	var input sourceInput
	if err := ctx.GetInput(&input); err != nil {
		return nil, err
	}
	if err := ctx.CreateTimer(25 * time.Millisecond).Await(nil); err != nil {
		return nil, err
	}
	var output sourceOutput
	if err := ctx.CallActivity("ExportHistoryEcho", task.WithActivityInput(input)).Await(&output); err != nil {
		return nil, err
	}
	return output, nil
}

func exportHistoryEcho(ctx task.ActivityContext) (any, error) {
	var input sourceInput
	if err := ctx.GetInput(&input); err != nil {
		return nil, err
	}
	return sourceOutput{Value: fmt.Sprintf("history:%s:%d", input.Scenario, input.Index)}, nil
}

func waitForExport(
	ctx context.Context,
	job *exporthistory.JobClient,
	mode exporthistory.ExportMode,
	wantExported int,
) (*exporthistory.ExportJobDescription, error) {
	for {
		description, err := job.Describe(ctx)
		if err != nil {
			return nil, err
		}
		if description.Status == exporthistory.ExportJobStatusFailed {
			return nil, fmt.Errorf("export job %s failed: %s", description.JobID, description.LastError)
		}
		if description.ExportedInstances >= int64(wantExported) && description.ScannedInstances > 0 {
			if mode == exporthistory.ExportModeContinuous || description.Status == exporthistory.ExportJobStatusCompleted {
				return description, nil
			}
		}
		if mode == exporthistory.ExportModeBatch && description.Status == exporthistory.ExportJobStatusCompleted {
			return nil, fmt.Errorf("batch job completed after scanning %d/exporting %d, want at least %d",
				description.ScannedInstances, description.ExportedInstances, wantExported)
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(time.Second):
		}
	}
}

func verifyJobLifecycle(
	ctx context.Context,
	client *exporthistory.Client,
	jobID string,
	description *exporthistory.ExportJobDescription,
	mode exporthistory.ExportMode,
) error {
	got, err := client.GetJob(ctx, jobID)
	if err != nil {
		return err
	}
	if got.JobID != jobID || got.Status != description.Status {
		return fmt.Errorf("GetJob returned %+v, want job %s status %s", got, jobID, description.Status)
	}
	listed, err := client.ListJobs(ctx, exporthistory.ExportJobQuery{JobIDPrefix: jobID, PageSize: 10})
	if err != nil {
		return err
	}
	if len(listed.Jobs) != 1 || listed.Jobs[0].JobID != jobID {
		return fmt.Errorf("ListJobs returned %d matches for %s", len(listed.Jobs), jobID)
	}
	if description.Config == nil || description.Config.Mode != mode {
		return fmt.Errorf("job %s description missing mode %s", jobID, mode)
	}
	return nil
}

func waitForDownloadedHistories(
	ctx context.Context,
	client *azblob.Client,
	container string,
	prefix string,
	format exporthistory.ExportFormatKind,
	sources []sourceExecution,
) (map[string][]api.HistoryEvent, error) {
	var lastErr error
	for {
		exported, err := downloadHistories(ctx, client, container, prefix, format)
		if err == nil {
			err = verifyHistories(exported, sources)
		}
		if err == nil {
			return exported, nil
		}
		lastErr = err
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("downloaded histories under %s never matched sources: %w; last error: %w", prefix, ctx.Err(), lastErr)
		case <-time.After(time.Second):
		}
	}
}

func downloadHistories(
	ctx context.Context,
	client *azblob.Client,
	container string,
	prefix string,
	format exporthistory.ExportFormatKind,
) (map[string][]api.HistoryEvent, error) {
	exported := map[string][]api.HistoryEvent{}
	pager := client.NewListBlobsFlatPager(container, &azblob.ListBlobsFlatOptions{
		Prefix:  &prefix,
		Include: azblob.ListBlobsInclude{Metadata: true},
	})
	for pager.More() {
		page, err := pager.NextPage(ctx)
		if err != nil {
			return nil, err
		}
		for _, item := range page.Segment.BlobItems {
			if item.Name == nil {
				continue
			}
			instanceID := metadataValue(item.Metadata, "instanceId")
			if instanceID == "" {
				return nil, fmt.Errorf("blob %s has no instanceId metadata", *item.Name)
			}
			response, err := client.DownloadStream(ctx, container, *item.Name, nil)
			if err != nil {
				return nil, err
			}
			body, readErr := io.ReadAll(response.Body)
			closeErr := response.Body.Close()
			if readErr != nil || closeErr != nil {
				return nil, errors.Join(readErr, closeErr)
			}
			events, err := decodeHistoryObject(body, format)
			if err != nil {
				return nil, fmt.Errorf("decode %s: %w", *item.Name, err)
			}
			exported[instanceID] = events
		}
	}
	return exported, nil
}

func verifyHistories(exported map[string][]api.HistoryEvent, sources []sourceExecution) error {
	for _, source := range sources {
		events, ok := exported[string(source.id)]
		if !ok {
			return fmt.Errorf("source %s was not exported", source.id)
		}
		var sawStarted, sawCompleted, sawActivity bool
		for _, event := range events {
			switch event.Type {
			case api.HistoryEventExecutionStarted:
				if event.ExecutionStarted != nil && event.ExecutionStarted.ExecutionID == source.executionID {
					sawStarted = true
				}
			case api.HistoryEventExecutionCompleted:
				if event.ExecutionCompleted != nil {
					var output sourceOutput
					if err := json.Unmarshal([]byte(event.ExecutionCompleted.SerializedResult), &output); err != nil {
						return fmt.Errorf("decode completed output for %s: %w", source.id, err)
					}
					sawCompleted = output.Value == source.output
				}
			case api.HistoryEventTaskScheduled:
				if event.TaskScheduled != nil && event.TaskScheduled.Name == "ExportHistoryEcho" {
					sawActivity = true
				}
			}
		}
		if !sawStarted || !sawCompleted || !sawActivity {
			return fmt.Errorf("history for %s missing started=%v completed=%v activity=%v", source.id, sawStarted, sawCompleted, sawActivity)
		}
	}
	return nil
}

func decodeHistoryObject(body []byte, format exporthistory.ExportFormatKind) ([]api.HistoryEvent, error) {
	if format == exporthistory.ExportFormatJSON {
		var events []api.HistoryEvent
		if err := json.Unmarshal(body, &events); err != nil {
			return nil, err
		}
		return events, nil
	}
	reader, err := gzip.NewReader(bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	decompressed, readErr := io.ReadAll(reader)
	closeErr := reader.Close()
	if readErr != nil || closeErr != nil {
		return nil, errors.Join(readErr, closeErr)
	}
	var events []api.HistoryEvent
	for _, line := range strings.Split(strings.TrimRight(string(decompressed), "\n"), "\n") {
		if line == "" {
			continue
		}
		var event api.HistoryEvent
		if err := json.Unmarshal([]byte(line), &event); err != nil {
			return nil, err
		}
		events = append(events, event)
	}
	return events, nil
}

func waitForListableInstances(
	ctx context.Context,
	client *durabletaskscheduler.Client,
	from time.Time,
	want []api.InstanceID,
	timeout time.Duration,
) error {
	deadline := time.Now().Add(timeout)
	for {
		seen := map[api.InstanceID]bool{}
		token := ""
		for pages := 0; pages < 100; pages++ {
			result, err := client.ListInstanceIDs(ctx, api.InstanceIDQuery{
				RuntimeStatus:     exporthistory.TerminalStatuses(),
				CompletedTimeFrom: from,
				PageSize:          100,
				ContinuationToken: token,
			})
			if err != nil {
				return err
			}
			for _, id := range result.InstanceIDs {
				seen[id] = true
			}
			if result.ContinuationToken == "" {
				break
			}
			token = result.ContinuationToken
		}
		missing := missingIDs(seen, want)
		if len(missing) == 0 {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("DTS ListInstanceIDs did not expose source IDs %v within %s", missing, timeout)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Second):
		}
	}
}

func readStorageSettings() (storageSettings, error) {
	connectionString := os.Getenv("AZURE_STORAGE_CONNECTION_STRING")
	if connectionString == "" {
		connectionString = os.Getenv("EXPORT_STORAGE_CONNECTION_STRING")
	}
	if connectionString == "" {
		return storageSettings{}, errors.New("AZURE_STORAGE_CONNECTION_STRING is required; EXPORT_STORAGE_CONNECTION_STRING is accepted for compatibility")
	}
	allowHTTP, err := allowInsecureStorage(connectionString)
	if err != nil {
		return storageSettings{}, err
	}
	container := strings.TrimSpace(os.Getenv("EXPORT_CONTAINER"))
	deleteContainer := false
	if container == "" {
		container, err = randomContainerName("dtgoexport")
		if err != nil {
			return storageSettings{}, err
		}
		deleteContainer = true
	}
	return storageSettings{connectionString: connectionString, container: container, deleteContainer: deleteContainer, allowInsecureHTTP: allowHTTP}, nil
}

func requireIsolatedTaskHub(options *durabletaskscheduler.Options) error {
	if os.Getenv("DTS_SAMPLE_ISOLATED_TASKHUB") == "1" {
		return nil
	}
	return fmt.Errorf("exporthistory requires an isolated task hub because export windows cannot filter by instance ID prefix; set DTS_SAMPLE_ISOLATED_TASKHUB=1 for a private hub (current hub %q)", options.TaskHubName)
}

func allowInsecureStorage(connectionString string) (bool, error) {
	if !strings.Contains(strings.ToLower(connectionString), "http://") &&
		!strings.Contains(strings.ToLower(connectionString), "usedevelopmentstorage=true") {
		return false, nil
	}
	if os.Getenv("DTS_SAMPLE_ALLOW_INSECURE_STORAGE") != "1" {
		return false, errors.New("plain HTTP storage endpoints require DTS_SAMPLE_ALLOW_INSECURE_STORAGE=1 and loopback Azurite endpoints")
	}
	if strings.Contains(strings.ToLower(connectionString), "usedevelopmentstorage=true") {
		return true, nil
	}
	endpoints := connectionStringEndpoints(connectionString)
	if len(endpoints) == 0 {
		return false, errors.New("plaintext storage connection strings must include explicit loopback service endpoints")
	}
	for _, endpoint := range endpoints {
		if strings.HasPrefix(strings.ToLower(endpoint), "http://") && !isLoopbackEndpoint(endpoint) {
			return false, fmt.Errorf("refusing non-loopback plaintext storage endpoint %q", endpoint)
		}
	}
	return true, nil
}

func selectScenarios(name string) ([]scenario, error) {
	all := []scenario{
		{name: "batch-jsonl", mode: exporthistory.ExportModeBatch, format: exporthistory.ExportFormatJSONL, recreate: true},
		{name: "batch-json", mode: exporthistory.ExportModeBatch, format: exporthistory.ExportFormatJSON},
		{name: "continuous-jsonl", mode: exporthistory.ExportModeContinuous, format: exporthistory.ExportFormatJSONL},
		{name: "continuous-json", mode: exporthistory.ExportModeContinuous, format: exporthistory.ExportFormatJSON},
	}
	if name == "all" {
		return all, nil
	}
	for _, candidate := range all {
		if candidate.name == name {
			return []scenario{candidate}, nil
		}
	}
	return nil, fmt.Errorf("unknown scenario %q", name)
}

func completedTimeTo(mode exporthistory.ExportMode) time.Time {
	if mode == exporthistory.ExportModeBatch {
		return time.Now().UTC()
	}
	return time.Time{}
}

func sourceIDs(sources []sourceExecution) []api.InstanceID {
	ids := make([]api.InstanceID, 0, len(sources))
	for _, source := range sources {
		ids = append(ids, source.id)
	}
	return ids
}

func missingIDs(seen map[api.InstanceID]bool, want []api.InstanceID) []api.InstanceID {
	var missing []api.InstanceID
	for _, id := range want {
		if !seen[id] {
			missing = append(missing, id)
		}
	}
	return missing
}

func randomContainerName(prefix string) (string, error) {
	var suffix [8]byte
	if _, err := rand.Read(suffix[:]); err != nil {
		return "", err
	}
	return prefix + hex.EncodeToString(suffix[:]), nil
}

func compactID() (string, error) {
	var suffix [8]byte
	if _, err := rand.Read(suffix[:]); err != nil {
		return "", fmt.Errorf("generate job suffix: %w", err)
	}
	return hex.EncodeToString(suffix[:]), nil
}

func metadataValue(metadata map[string]*string, key string) string {
	for name, value := range metadata {
		if strings.EqualFold(name, key) && value != nil {
			return *value
		}
	}
	return ""
}

func deleteBlobsWithPrefix(ctx context.Context, client *azblob.Client, container, prefix string) error {
	if prefix == "" {
		return nil
	}
	var failures []error
	pager := client.NewListBlobsFlatPager(container, &azblob.ListBlobsFlatOptions{Prefix: &prefix})
	for pager.More() {
		page, err := pager.NextPage(ctx)
		if bloberror.HasCode(err, bloberror.ContainerNotFound) {
			return nil
		}
		if err != nil {
			return err
		}
		for _, item := range page.Segment.BlobItems {
			if item.Name == nil {
				continue
			}
			if _, err := client.DeleteBlob(ctx, container, *item.Name, nil); err != nil &&
				!bloberror.HasCode(err, bloberror.BlobNotFound, bloberror.ContainerNotFound) {
				failures = append(failures, fmt.Errorf("delete blob %s: %w", *item.Name, err))
			}
		}
	}
	return errors.Join(failures...)
}

func connectionStringEndpoints(connectionString string) []string {
	var endpoints []string
	for _, part := range strings.Split(connectionString, ";") {
		key, value, ok := strings.Cut(part, "=")
		if ok && strings.HasSuffix(strings.ToLower(strings.TrimSpace(key)), "endpoint") {
			endpoints = append(endpoints, strings.TrimSpace(value))
		}
	}
	return endpoints
}

func isLoopbackEndpoint(endpoint string) bool {
	parsed, err := url.Parse(endpoint)
	if err != nil {
		return false
	}
	host := parsed.Hostname()
	if strings.EqualFold(host, "localhost") {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}
