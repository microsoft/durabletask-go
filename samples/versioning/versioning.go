// Command versioning demonstrates version-aware orchestration and activity
// routing across two worker registries.
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/microsoft/durabletask-go/api"
	durabletaskclient "github.com/microsoft/durabletask-go/client"
	"github.com/microsoft/durabletask-go/durabletaskscheduler"
	"github.com/microsoft/durabletask-go/samples/internal/dtssample"
	"github.com/microsoft/durabletask-go/task"
)

const (
	versionGreetingName      = "SampleVersioningGreeting"
	versionActivityName      = "SampleVersioningActivity"
	versionMigrationName     = "SampleVersioningMigration"
	versionOlderName         = "SampleVersioningOlderAccepted"
	versionOlderActivityName = "SampleVersioningOlderActivity"
	versionUnversionedName   = "SampleVersioningUnversioned"
	versionStrictOnlyName    = "SampleVersioningStrictOnly"
	unversionedActivityName  = "SampleVersioningUnversionedActivity"
)

type versionOutput struct {
	Input                string `json:"input"`
	Worker               string `json:"worker"`
	OrchestrationVersion string `json:"orchestrationVersion"`
	ActivityWorker       string `json:"activityWorker"`
	ActivityVersion      string `json:"activityVersion"`
	ActivityObservedFrom string `json:"activityObservedFrom"`
}

type migrationState struct {
	Steps []versionOutput `json:"steps"`
}

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
	fmt.Println("SAMPLE_OK versioning")
}

func run() (err error) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	baseOptions, err := dtssample.Options()
	if err != nil {
		return err
	}
	v1Options := cloneVersionOptions(baseOptions, &task.VersioningOptions{
		Version:         "1.0",
		DefaultVersion:  "1.0",
		MatchStrategy:   task.VersionMatchStrict,
		FailureStrategy: task.VersionFailureFail,
	})
	v1Registry := task.NewTaskRegistry()
	if err := registerVersionOne(v1Registry); err != nil {
		return err
	}
	app, err := dtssample.StartWithOptions(
		ctx,
		v1Options,
		v1Registry,
		durabletaskclient.WithWorkItemFilters(&durabletaskclient.WorkItemFilters{
			Orchestrations: []durabletaskclient.WorkItemFilter{
				{Name: versionGreetingName, Versions: []string{"1.0"}},
				{Name: versionMigrationName, Versions: []string{"1.0"}},
				{Name: versionStrictOnlyName, Versions: []string{"2.0"}},
				{Name: versionUnversionedName, Versions: []string{task.UnversionedTaskVersion}},
			},
			Activities: []durabletaskclient.WorkItemFilter{
				{Name: versionActivityName, Versions: []string{"1.0"}},
				{Name: unversionedActivityName, Versions: []string{task.UnversionedTaskVersion}},
			},
			RejectAllEntities: true,
		}),
		durabletaskclient.WithTaskExecutorOptions(
			task.WithUnversionedOrchestratorNames(versionUnversionedName),
			task.WithUnversionedActivityNames(unversionedActivityName),
		),
	)
	if err != nil {
		return err
	}
	var ownedIDs []api.InstanceID
	var extraWorkers []*durabletaskclient.TaskHubGrpcWorker
	defer func() {
		cleanupErr := dtssample.Cleanup(app.Client, ownedIDs...)
		for _, worker := range extraWorkers {
			cleanupErr = errors.Join(cleanupErr, shutdownWorker(worker))
		}
		err = errors.Join(err, cleanupErr, app.Shutdown())
	}()

	v2Options := cloneVersionOptions(baseOptions, &task.VersioningOptions{
		Version:         "2.0",
		DefaultVersion:  "2.0",
		MatchStrategy:   task.VersionMatchStrict,
		FailureStrategy: task.VersionFailureFail,
	})
	v2Registry := task.NewTaskRegistry()
	if err := registerVersionTwo(v2Registry); err != nil {
		return err
	}
	v2Worker, err := startVersionWorker(ctx, v2Options, v2Registry)
	if err != nil {
		return err
	}
	extraWorkers = append(extraWorkers, v2Worker)

	checks := []struct {
		name string
		run  func(context.Context, *durabletaskscheduler.Client, *[]api.InstanceID) error
	}{
		{"default-version-routing", verifyDefaultVersionRouting},
		{"explicit-version-routing", verifyExplicitVersionRouting},
		{"unversioned-fallback", verifyUnversionedFallback},
		{"strict-negative-routing", verifyStrictNegativeRouting},
		{"continue-as-new-migration", verifyContinueAsNewMigration},
	}
	for _, check := range checks {
		if err := check.run(ctx, app.Client, &ownedIDs); err != nil {
			return fmt.Errorf("%s: %w", check.name, err)
		}
		fmt.Printf("verified %s\n", check.name)
	}

	olderOptions := cloneVersionOptions(baseOptions, &task.VersioningOptions{
		Version:         "2.0",
		DefaultVersion:  "2.0",
		MatchStrategy:   task.VersionMatchCurrentOrOlder,
		FailureStrategy: task.VersionFailureFail,
	})
	olderRegistry := task.NewTaskRegistry()
	if err := olderRegistry.AddOrchestratorNVersion(versionOlderName, "1.0", versionedGreetingWithActivity("worker-v2-current-or-older", versionOlderActivityName)); err != nil {
		return err
	}
	if err := olderRegistry.AddActivityNVersion(versionOlderActivityName, "1.0", versionedActivity("worker-v2-current-or-older")); err != nil {
		return err
	}
	olderWorker, err := startVersionWorker(ctx, olderOptions, olderRegistry)
	if err != nil {
		return err
	}
	extraWorkers = append(extraWorkers, olderWorker)
	if err := verifyCurrentOrOlderRouting(ctx, app.Client, &ownedIDs); err != nil {
		return fmt.Errorf("current-or-older-routing: %w", err)
	}
	fmt.Println("verified current-or-older-routing")
	return nil
}

func registerVersionOne(registry *task.TaskRegistry) error {
	if err := registry.AddOrchestratorNVersion(versionGreetingName, "1.0", versionedGreeting("worker-v1")); err != nil {
		return err
	}
	if err := registry.AddActivityNVersion(versionActivityName, "1.0", versionedActivity("worker-v1")); err != nil {
		return err
	}
	if err := registry.AddOrchestratorNVersion(versionMigrationName, "1.0", migrationV1); err != nil {
		return err
	}
	if err := registry.AddOrchestratorN(versionUnversionedName, unversionedOrchestrator); err != nil {
		return err
	}
	if err := registry.AddActivityN(unversionedActivityName, unversionedActivity); err != nil {
		return err
	}
	return registry.AddOrchestratorNVersion(versionStrictOnlyName, "1.0", versionedGreeting("worker-v1"))
}

func registerVersionTwo(registry *task.TaskRegistry) error {
	if err := registry.AddOrchestratorNVersion(versionGreetingName, "2.0", versionedGreeting("worker-v2")); err != nil {
		return err
	}
	if err := registry.AddActivityNVersion(versionActivityName, "2.0", versionedActivity("worker-v2")); err != nil {
		return err
	}
	return registry.AddOrchestratorNVersion(versionMigrationName, "2.0", migrationV2)
}

func verifyDefaultVersionRouting(ctx context.Context, client *durabletaskscheduler.Client, ownedIDs *[]api.InstanceID) error {
	id := dtssample.NewInstanceID("versioning-default")
	*ownedIDs = append(*ownedIDs, id)
	output, metadata, err := runVersionGreeting(ctx, client, id, nil, "default")
	if err != nil {
		return err
	}
	if metadata.Version != "1.0" || output.Worker != "worker-v1" || output.ActivityVersion != "1.0" {
		return fmt.Errorf("default routing output=%#v metadata version=%q", output, metadata.Version)
	}
	return nil
}

func verifyExplicitVersionRouting(ctx context.Context, client *durabletaskscheduler.Client, ownedIDs *[]api.InstanceID) error {
	for _, test := range []struct {
		version string
		worker  string
	}{
		{"1.0", "worker-v1"},
		{"2.0", "worker-v2"},
	} {
		id := dtssample.NewInstanceID("versioning-explicit-" + test.version)
		*ownedIDs = append(*ownedIDs, id)
		version := test.version
		output, metadata, err := runVersionGreeting(ctx, client, id, func() api.NewOrchestrationOptions {
			return api.WithVersion(version)
		}, "explicit-"+test.version)
		if err != nil {
			return err
		}
		if metadata.Version != test.version || output.Worker != test.worker || output.ActivityVersion != test.version {
			return fmt.Errorf("explicit %s routed to output=%#v metadata=%q", test.version, output, metadata.Version)
		}
	}
	return nil
}

func verifyUnversionedFallback(ctx context.Context, client *durabletaskscheduler.Client, ownedIDs *[]api.InstanceID) error {
	id := dtssample.NewInstanceID("versioning-unversioned")
	*ownedIDs = append(*ownedIDs, id)
	if _, err := client.ScheduleNewOrchestration(
		ctx,
		versionUnversionedName,
		api.WithInstanceID(id),
		api.WithVersion(task.UnversionedTaskVersion),
		api.WithInput("fallback"),
		api.WithTags(map[string]string{"sample": "versioning", "scenario": "unversioned"}),
	); err != nil {
		return err
	}
	metadata, err := client.WaitForOrchestrationCompletion(ctx, id, api.WithFetchPayloads(true))
	if err != nil {
		return err
	}
	if err := dtssample.RequireCompleted(metadata); err != nil {
		return err
	}
	var output versionOutput
	if err := metadata.ReadOutput(&output); err != nil {
		return err
	}
	if metadata.Version != "" || output.Worker != "worker-v1-unversioned" || output.ActivityVersion != "" {
		return fmt.Errorf("unversioned fallback output=%#v metadata=%q", output, metadata.Version)
	}
	return nil
}

func verifyStrictNegativeRouting(ctx context.Context, client *durabletaskscheduler.Client, ownedIDs *[]api.InstanceID) error {
	id := dtssample.NewInstanceID("versioning-strict-negative")
	*ownedIDs = append(*ownedIDs, id)
	if _, err := client.ScheduleNewOrchestration(
		ctx,
		versionStrictOnlyName,
		api.WithInstanceID(id),
		api.WithVersion("2.0"),
		api.WithTags(map[string]string{"sample": "versioning", "scenario": "strict-negative"}),
	); err != nil {
		return err
	}
	metadata, err := client.WaitForOrchestrationCompletion(ctx, id, api.WithFetchPayloads(true))
	if err != nil {
		return err
	}
	if metadata.RuntimeStatus != api.RUNTIME_STATUS_FAILED ||
		metadata.FailureDetails == nil ||
		!metadata.FailureDetails.Matches(api.ErrVersionMismatch) {
		return fmt.Errorf("strict mismatch metadata=%+v", metadata)
	}
	return nil
}

func verifyContinueAsNewMigration(ctx context.Context, client *durabletaskscheduler.Client, ownedIDs *[]api.InstanceID) error {
	id := dtssample.NewInstanceID("versioning-migration")
	*ownedIDs = append(*ownedIDs, id)
	if _, err := client.ScheduleNewOrchestration(
		ctx,
		versionMigrationName,
		api.WithInstanceID(id),
		api.WithVersion("1.0"),
		api.WithTags(map[string]string{"sample": "versioning", "scenario": "migration"}),
	); err != nil {
		return err
	}
	metadata, err := client.WaitForOrchestrationCompletion(ctx, id, api.WithFetchPayloads(true))
	if err != nil {
		return err
	}
	if err := dtssample.RequireCompleted(metadata); err != nil {
		return err
	}
	if metadata.Version != "2.0" {
		return fmt.Errorf("migration completed as version %q, want 2.0", metadata.Version)
	}
	var output migrationState
	if err := metadata.ReadOutput(&output); err != nil {
		return err
	}
	if len(output.Steps) != 2 ||
		output.Steps[0].Worker != "worker-v1" ||
		output.Steps[0].ActivityVersion != "1.0" ||
		output.Steps[1].Worker != "worker-v2" ||
		output.Steps[1].ActivityVersion != "2.0" {
		return fmt.Errorf("migration output = %#v", output)
	}
	return nil
}

func verifyCurrentOrOlderRouting(ctx context.Context, client *durabletaskscheduler.Client, ownedIDs *[]api.InstanceID) error {
	id := dtssample.NewInstanceID("versioning-current-or-older")
	*ownedIDs = append(*ownedIDs, id)
	if _, err := client.ScheduleNewOrchestration(
		ctx,
		versionOlderName,
		api.WithInstanceID(id),
		api.WithVersion("1.0"),
		api.WithInput("older"),
		api.WithTags(map[string]string{"sample": "versioning", "scenario": "current-or-older"}),
	); err != nil {
		return err
	}
	metadata, err := client.WaitForOrchestrationCompletion(ctx, id, api.WithFetchPayloads(true))
	if err != nil {
		return err
	}
	if err := dtssample.RequireCompleted(metadata); err != nil {
		return err
	}
	var output versionOutput
	if err := metadata.ReadOutput(&output); err != nil {
		return err
	}
	if metadata.Version != "1.0" || output.Worker != "worker-v2-current-or-older" || output.ActivityVersion != "1.0" {
		return fmt.Errorf("current-or-older output=%#v metadata=%q", output, metadata.Version)
	}
	return nil
}

func runVersionGreeting(
	ctx context.Context,
	client *durabletaskscheduler.Client,
	id api.InstanceID,
	version func() api.NewOrchestrationOptions,
	input string,
) (versionOutput, *api.OrchestrationMetadata, error) {
	options := []api.NewOrchestrationOptions{
		api.WithInstanceID(id),
		api.WithInput(input),
		api.WithTags(map[string]string{"sample": "versioning"}),
	}
	if version != nil {
		options = append(options, version())
	}
	if _, err := client.ScheduleNewOrchestration(ctx, versionGreetingName, options...); err != nil {
		return versionOutput{}, nil, err
	}
	metadata, err := client.WaitForOrchestrationCompletion(ctx, id, api.WithFetchPayloads(true))
	if err != nil {
		return versionOutput{}, nil, err
	}
	if err := dtssample.RequireCompleted(metadata); err != nil {
		return versionOutput{}, nil, err
	}
	var output versionOutput
	if err := metadata.ReadOutput(&output); err != nil {
		return versionOutput{}, nil, err
	}
	return output, metadata, nil
}

func versionedGreeting(worker string) task.Orchestrator {
	return versionedGreetingWithActivity(worker, versionActivityName)
}

func versionedGreetingWithActivity(worker string, activityName string) task.Orchestrator {
	return func(ctx *task.OrchestrationContext) (any, error) {
		var input string
		if err := ctx.GetInput(&input); err != nil {
			return nil, err
		}
		var activity versionOutput
		if err := ctx.CallActivity(activityName, task.WithActivityInput(input)).Await(&activity); err != nil {
			return nil, err
		}
		return versionOutput{
			Input:                input,
			Worker:               worker,
			OrchestrationVersion: ctx.Version,
			ActivityWorker:       activity.ActivityWorker,
			ActivityVersion:      activity.ActivityVersion,
			ActivityObservedFrom: activity.ActivityObservedFrom,
		}, nil
	}
}

func versionedActivity(worker string) task.Activity {
	return func(ctx task.ActivityContext) (any, error) {
		var input string
		if err := ctx.GetInput(&input); err != nil {
			return nil, err
		}
		activityInfo, _ := api.ActivityContextInfoFromContext(ctx.Context())
		orchestrationInfo, _ := api.OrchestrationContextInfoFromContext(ctx.Context())
		return versionOutput{
			Input:                input,
			ActivityWorker:       worker,
			ActivityVersion:      activityInfo.Version,
			ActivityObservedFrom: orchestrationInfo.Version,
		}, nil
	}
}

func migrationV1(ctx *task.OrchestrationContext) (any, error) {
	var activity versionOutput
	if err := ctx.CallActivity(versionActivityName, task.WithActivityInput("migration-v1")).Await(&activity); err != nil {
		return nil, err
	}
	state := migrationState{Steps: []versionOutput{{
		Input:                "migration-v1",
		Worker:               "worker-v1",
		OrchestrationVersion: ctx.Version,
		ActivityWorker:       activity.ActivityWorker,
		ActivityVersion:      activity.ActivityVersion,
		ActivityObservedFrom: activity.ActivityObservedFrom,
	}}}
	ctx.ContinueAsNew(state, task.WithContinueAsNewVersion("2.0"))
	return nil, nil
}

func migrationV2(ctx *task.OrchestrationContext) (any, error) {
	var state migrationState
	if err := ctx.GetInput(&state); err != nil {
		return nil, err
	}
	var activity versionOutput
	if err := ctx.CallActivity(versionActivityName, task.WithActivityInput("migration-v2")).Await(&activity); err != nil {
		return nil, err
	}
	state.Steps = append(state.Steps, versionOutput{
		Input:                "migration-v2",
		Worker:               "worker-v2",
		OrchestrationVersion: ctx.Version,
		ActivityWorker:       activity.ActivityWorker,
		ActivityVersion:      activity.ActivityVersion,
		ActivityObservedFrom: activity.ActivityObservedFrom,
	})
	return state, nil
}

func unversionedOrchestrator(ctx *task.OrchestrationContext) (any, error) {
	var input string
	if err := ctx.GetInput(&input); err != nil {
		return nil, err
	}
	var activity versionOutput
	if err := ctx.CallActivity(
		unversionedActivityName,
		task.WithActivityInput(input),
		task.WithActivityVersion(task.UnversionedTaskVersion),
	).Await(&activity); err != nil {
		return nil, err
	}
	return versionOutput{
		Input:                input,
		Worker:               "worker-v1-unversioned",
		OrchestrationVersion: ctx.Version,
		ActivityWorker:       activity.ActivityWorker,
		ActivityVersion:      activity.ActivityVersion,
		ActivityObservedFrom: activity.ActivityObservedFrom,
	}, nil
}

func unversionedActivity(ctx task.ActivityContext) (any, error) {
	var input string
	if err := ctx.GetInput(&input); err != nil {
		return nil, err
	}
	activityInfo, _ := api.ActivityContextInfoFromContext(ctx.Context())
	orchestrationInfo, _ := api.OrchestrationContextInfoFromContext(ctx.Context())
	return versionOutput{
		Input:                input,
		ActivityWorker:       "worker-v1-unversioned",
		ActivityVersion:      activityInfo.Version,
		ActivityObservedFrom: orchestrationInfo.Version,
	}, nil
}

func cloneVersionOptions(
	base *durabletaskscheduler.Options,
	versioning *task.VersioningOptions,
) *durabletaskscheduler.Options {
	copy := *base
	copy.Versioning = versioning
	return &copy
}

func startVersionWorker(
	ctx context.Context,
	options *durabletaskscheduler.Options,
	registry *task.TaskRegistry,
) (*durabletaskclient.TaskHubGrpcWorker, error) {
	worker, err := durabletaskscheduler.NewWorker(
		options,
		registry,
		api.DefaultLogger(),
		durabletaskclient.WithAutoWorkItemFilters(),
	)
	if err != nil {
		return nil, err
	}
	if err := worker.Start(ctx); err != nil {
		return nil, err
	}
	return worker, nil
}

func shutdownWorker(worker *durabletaskclient.TaskHubGrpcWorker) error {
	if worker == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	return worker.Shutdown(ctx)
}
