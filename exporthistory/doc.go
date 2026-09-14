// Package exporthistory exports terminal orchestration histories to Azure Blob
// Storage using durable entities, orchestrations, and activities.
//
// # Maturity: preview
//
// This package is a preview API. Its exported types, function signatures, the
// serialized shape of the ExportJob entity state, and the names of the system
// entity, orchestrators, and activities may change in a future release without a
// major version bump. Do not depend on it for production workloads that cannot
// tolerate a breaking change. Drain and delete export jobs before upgrading;
// legacy export-job state is not supported or migrated. Do not mix .NET or
// older Go export-history workers with these system tasks in the same task hub.
//
// # Model
//
// An export job is a durable entity keyed by job ID. Creating a job transitions
// the entity to [ExportJobStatusActive], mints a run token, and reserves an
// orchestration ID derived from the job ID and token before signaling Run.
// [ExportJobDescription.OrchestratorInstanceID] identifies that generation.
// Only the current generation can mutate job state; older orchestrations and
// already-scheduled activities can briefly remain in flight. The orchestration
// repeatedly lists terminal orchestration instances that match the job's filter,
// exports each instance's history, and commits a checkpoint back to the entity.
// A batch job completes when the task hub reports no further pages; an empty page
// with a continuation cursor advances the cursor and keeps going. A continuous
// job idles and lists again.
//
// Recreation assigns a fresh orchestration ID and retains previous generations'
// orchestration histories. Delete atomically captures the current ID and removes
// entity state, then the client terminates and purges only that captured
// generation. A concurrent recreation is not affected by that cleanup. Every
// run and orchestration-originated mutation requires a matching nonempty token.
//
// # Usage
//
// Workers register the system tasks and advertise them to the service:
//
//	registry := task.NewTaskRegistry()
//	store, err := exporthistory.NewAzureBlobHistoryStore(exporthistory.AzureBlobHistoryStoreOptions{
//		ConnectionString: connectionString,
//		ContainerName:    "history-exports",
//	})
//	if err != nil {
//		return err
//	}
//	if err := exporthistory.Register(registry, exporthistory.WorkerOptions{
//		Source: taskHubClient,
//		Store:  store,
//	}); err != nil {
//		return err
//	}
//	worker, err := durabletaskscheduler.NewWorker(
//		options, registry, logger, exporthistory.WithExportHistory())
//
// Clients create and inspect jobs:
//
//	exportClient, err := exporthistory.NewClient(taskHubClient, exporthistory.ClientOptions{
//		ContainerName: "history-exports",
//	})
//	if err != nil {
//		return err
//	}
//	job, err := exportClient.CreateJob(ctx, exporthistory.JobCreationOptions{
//		Mode:              exporthistory.ExportModeBatch,
//		CompletedTimeFrom: from,
//		CompletedTimeTo:   to,
//	})
//	if err != nil {
//		return err
//	}
//	description, err := job.Describe(ctx)
//
// # Limitations
//
// Export requires a task hub that implements the instance-ID listing and history
// streaming management APIs. Extended sessions are not supported.
package exporthistory
