package exporthistory

import (
	"fmt"

	"github.com/microsoft/durabletask-go/api"
	durabletaskclient "github.com/microsoft/durabletask-go/client"
	"github.com/microsoft/durabletask-go/task"
)

// WorkerOptions configures the export history system tasks a worker runs.
type WorkerOptions struct {
	// Source supplies the management reads the export activities perform. It is
	// required and is normally the same task hub client the worker connects with.
	Source HistorySource
	// Store persists exported objects. It is required.
	Store Store
	// HistoryQuery bounds each instance-history read. Zero values select the
	// api package defaults.
	HistoryQuery api.HistoryQuery
}

// Register adds the export history system entity, orchestrators, and activities
// to registry.
//
// Every system task is registered unversioned so it stays reachable when an
// application enables default versioning; pair it with [WithExportHistory] so
// the worker's derived work-item filters keep accepting them under strict
// version matching.
func Register(registry *task.TaskRegistry, options WorkerOptions) error {
	if registry == nil {
		return &ValidationError{Message: "task registry is required"}
	}
	if options.Source == nil {
		return &ValidationError{Message: "export history worker requires a history source"}
	}
	if options.Store == nil {
		return &ValidationError{Message: "export history worker requires a store"}
	}
	historyQuery, err := api.NormalizeHistoryQuery(options.HistoryQuery)
	if err != nil {
		return fmt.Errorf("invalid export history query: %w", err)
	}
	runtime := &exportRuntime{
		source:      options.Source,
		store:       options.Store,
		historyPage: historyQuery,
	}

	if err := registry.AddEntityN(ExportJobEntityName, exportJobEntity); err != nil {
		return err
	}
	if err := registry.AddOrchestratorN(
		ExecuteExportJobOperationOrchestratorName,
		ExecuteExportJobOperationOrchestrator,
	); err != nil {
		return err
	}
	if err := registry.AddOrchestratorN(ExportJobOrchestratorName, ExportJobOrchestrator); err != nil {
		return err
	}
	if err := registry.AddActivityN(
		ListTerminalInstancesActivityName,
		runtime.listTerminalInstancesActivity,
	); err != nil {
		return err
	}
	return registry.AddActivityN(
		ExportInstanceHistoryActivityName,
		runtime.exportInstanceHistoryActivity,
	)
}

// WithExportHistory marks the export history system orchestrators and
// activities as unversioned so they remain routable when the worker runs with
// strict version matching, and so registry-derived work-item filters advertise
// them with the version they were registered under.
//
// The export orchestrations are started unversioned, and an activity inherits
// its caller's version, so both kinds must be allow-listed. Use it together
// with [Register]; on its own it changes no behavior.
func WithExportHistory() durabletaskclient.TaskHubGrpcWorkerOption {
	return durabletaskclient.CombineTaskHubGrpcWorkerOptions(
		durabletaskclient.WithUnversionedOrchestratorNames(
			ExecuteExportJobOperationOrchestratorName,
			ExportJobOrchestratorName,
		),
		durabletaskclient.WithUnversionedActivityNames(
			ListTerminalInstancesActivityName,
			ExportInstanceHistoryActivityName,
		),
	)
}
