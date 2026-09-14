// Command replayanalysis pairs a replay-safe orchestration with instructions
// for checking intentionally unsafe fixtures using cmd/orchestratorvet.
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/microsoft/durabletask-go/api"
	"github.com/microsoft/durabletask-go/samples/internal/dtssample"
	"github.com/microsoft/durabletask-go/task"
)

type replayOutput struct {
	StartedAt   time.Time `json:"startedAt"`
	DurableID   string    `json:"durableId"`
	ActivityOut string    `json:"activityOut"`
}

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
	fmt.Println("SAMPLE_OK replayanalysis")
}

func run() (err error) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	registry := task.NewTaskRegistry()
	if err := registry.AddOrchestratorN("ReplayAnalysisSafe", replayAnalysisSafe); err != nil {
		return err
	}
	if err := registry.AddActivityN("ReplayAnalysisActivity", replayAnalysisActivity); err != nil {
		return err
	}
	app, err := dtssample.Start(ctx, registry)
	if err != nil {
		return err
	}
	var ownedIDs []api.InstanceID
	defer func() { err = errors.Join(err, dtssample.Cleanup(app.Client, ownedIDs...), app.Shutdown()) }()

	instanceID := dtssample.NewInstanceID("replayanalysis")
	ownedIDs = append(ownedIDs, instanceID)
	if _, err := app.Client.ScheduleNewOrchestration(ctx, "ReplayAnalysisSafe",
		api.WithInstanceID(instanceID),
		api.WithInput("safe-counterpart"),
		api.WithTags(map[string]string{"sample": "replayanalysis"})); err != nil {
		return err
	}
	metadata, err := app.Client.WaitForOrchestrationCompletion(ctx, instanceID, api.WithFetchPayloads(true))
	if err != nil {
		return err
	}
	if err := dtssample.RequireCompleted(metadata); err != nil {
		return err
	}
	var output replayOutput
	if err := metadata.ReadOutput(&output); err != nil {
		return err
	}
	if err := validateReplayOutput(output); err != nil {
		return err
	}
	return nil
}

func replayAnalysisSafe(ctx *task.OrchestrationContext) (any, error) {
	var input string
	if err := ctx.GetInput(&input); err != nil {
		return nil, err
	}
	startedAt := ctx.CurrentTimeUtc
	durableID := ctx.NewGuid()
	if err := ctx.CreateTimer(50 * time.Millisecond).Await(nil); err != nil {
		return nil, err
	}
	var activityOut string
	if err := ctx.CallActivity("ReplayAnalysisActivity", task.WithActivityInput(input+"|"+durableID)).Await(&activityOut); err != nil {
		return nil, err
	}
	return replayOutput{StartedAt: startedAt, DurableID: durableID, ActivityOut: activityOut}, nil
}

func replayAnalysisActivity(ctx task.ActivityContext) (any, error) {
	var input string
	if err := ctx.GetInput(&input); err != nil {
		return nil, err
	}
	return "activity:" + input, nil
}

func validateReplayOutput(output replayOutput) error {
	if output.StartedAt.IsZero() {
		return errors.New("orchestration did not return durable current time")
	}
	if output.DurableID == "" {
		return errors.New("orchestration did not return deterministic GUID")
	}
	if output.ActivityOut != "activity:safe-counterpart|"+output.DurableID {
		return fmt.Errorf("activity output=%q does not contain the returned deterministic ID", output.ActivityOut)
	}
	return nil
}
