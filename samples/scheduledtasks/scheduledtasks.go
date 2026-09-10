// Command scheduledtasks demonstrates recurring scheduled-task management and
// validation using the public Go scheduler surface.
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/microsoft/durabletask-go/api"
	durabletaskclient "github.com/microsoft/durabletask-go/client"
	"github.com/microsoft/durabletask-go/durabletaskscheduler"
	"github.com/microsoft/durabletask-go/samples/internal/dtssample"
	"github.com/microsoft/durabletask-go/task"
)

const (
	scheduledTargetName   = "SampleScheduledTasksTarget"
	scheduledActivityName = "SampleScheduledTasksAttempt"
)

var scheduledAttempts sync.Map

type scheduledInput struct {
	Run       string `json:"run"`
	Phase     string `json:"phase"`
	FailFirst bool   `json:"failFirst"`
}

type scheduledOutput struct {
	Run           string            `json:"run"`
	Phase         string            `json:"phase"`
	Attempt       int32             `json:"attempt"`
	ContextFields map[string]string `json:"contextFields"`
}

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
	fmt.Println("SAMPLE_OK scheduledtasks")
}

func run() (err error) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	createdFrom := time.Now().UTC().Add(-time.Second)
	runID := string(dtssample.NewInstanceID("scheduledtasks"))
	scheduleID := runID + "-schedule"

	registry := task.NewTaskRegistry()
	if err := registry.AddOrchestratorN(scheduledTargetName, scheduledTargetOrchestrator); err != nil {
		return err
	}
	if err := registry.AddActivityN(scheduledActivityName, scheduledAttemptActivity); err != nil {
		return err
	}
	if err := durabletaskscheduler.RegisterScheduledTasks(registry); err != nil {
		return err
	}
	app, err := dtssample.Start(ctx, registry, durabletaskscheduler.WithScheduledTasks(), durabletaskclient.WithAutoWorkItemFilters())
	if err != nil {
		return err
	}
	var targetIDs []api.InstanceID
	var handle *durabletaskscheduler.ScheduleClient
	defer func() {
		var cleanupErr error
		if handle != nil {
			deleteCtx, stopDelete := context.WithTimeout(context.Background(), 15*time.Second)
			cleanupErr = errors.Join(cleanupErr, handle.Delete(deleteCtx))
			stopDelete()
		}
		queryCtx, stopQuery := context.WithTimeout(context.Background(), 15*time.Second)
		_, observed, queryErr := waitForObservedTargets(queryCtx, app.Client, createdFrom, runID)
		targetIDs = appendMissingIDs(targetIDs, observed...)
		stopQuery()
		cleanupErr = errors.Join(cleanupErr, queryErr)
		cleanupErr = errors.Join(cleanupErr, dtssample.Cleanup(app.Client, targetIDs...))
		err = errors.Join(err, cleanupErr, app.Shutdown())
	}()

	schedules := app.Client.ScheduledTasks()
	handle, err = schedules.Create(ctx, durabletaskscheduler.ScheduleCreationOptions{
		ScheduleID:              scheduleID,
		OrchestrationName:       scheduledTargetName,
		TypedOrchestrationInput: scheduledInput{Run: runID, Phase: "initial", FailFirst: true},
		Interval:                time.Hour,
		StartAt:                 time.Now().UTC().Add(-time.Second),
		StartImmediatelyIfLate:  true,
		Tags:                    map[string]string{"sample": "scheduledtasks", "run": runID, "phase": "initial"},
		ContextFields:           api.ContextFields{"tenant": "initial"},
		RetryPolicy: &durabletaskscheduler.ScheduleRetryPolicy{
			MaxAttempts:          2,
			InitialRetryInterval: time.Second,
			BackoffCoefficient:   1,
			MaxRetryInterval:     time.Second,
			RetryTimeout:         15 * time.Second,
		},
	})
	if err != nil {
		return err
	}
	description, err := schedules.Get(ctx, scheduleID)
	if err != nil {
		return err
	}
	if description == nil || description.Status != durabletaskscheduler.ScheduleStatusActive {
		return fmt.Errorf("created schedule description = %+v, want active", description)
	}
	if description.Tags["phase"] != "initial" || description.ContextFields["tenant"] != "initial" {
		return fmt.Errorf("created schedule tags/context = %#v / %#v", description.Tags, description.ContextFields)
	}
	list, err := schedules.List(ctx, durabletaskscheduler.ScheduleQuery{
		ScheduleIDPrefix: scheduleID,
		PageSize:         10,
	})
	if err != nil {
		return err
	}
	if len(list.Schedules) != 1 || list.Schedules[0].ScheduleID != scheduleID {
		return fmt.Errorf("schedule list returned %+v, want %s", list, scheduleID)
	}

	initial, observed, err := waitForScheduledCompletion(ctx, app.Client, createdFrom, runID, "initial")
	targetIDs = appendMissingIDs(targetIDs, observed...)
	if err != nil {
		return err
	}
	var initialOutput scheduledOutput
	if err := initial.ReadOutput(&initialOutput); err != nil {
		return err
	}
	if initialOutput.Phase != "initial" || initialOutput.Attempt != 2 ||
		initialOutput.ContextFields["tenant"] != "initial" {
		return fmt.Errorf("initial scheduled output = %#v", initialOutput)
	}

	if err := handle.Pause(ctx); err != nil {
		return err
	}
	paused, err := handle.Describe(ctx)
	if err != nil {
		return err
	}
	if paused.Status != durabletaskscheduler.ScheduleStatusPaused {
		return fmt.Errorf("paused schedule status=%s, want paused", paused.Status)
	}
	updatedStart := time.Now().UTC().Add(-time.Second)
	updatedInterval := 2 * time.Second
	startImmediately := true
	if err := handle.Update(ctx, durabletaskscheduler.ScheduleUpdateOptions{
		TypedOrchestrationInput: scheduledInput{Run: runID, Phase: "updated"},
		StartAt:                 &updatedStart,
		Interval:                &updatedInterval,
		StartImmediatelyIfLate:  &startImmediately,
		Tags:                    map[string]string{"sample": "scheduledtasks", "run": runID, "phase": "updated"},
		ContextFields:           api.ContextFields{"tenant": "updated"},
	}); err != nil {
		return err
	}
	updated, err := handle.Describe(ctx)
	if err != nil {
		return err
	}
	var updatedInput scheduledInput
	if err := updated.ReadInput(&updatedInput); err != nil {
		return err
	}
	if updated.Status != durabletaskscheduler.ScheduleStatusPaused ||
		updatedInput.Phase != "updated" ||
		updated.Tags["phase"] != "updated" ||
		updated.ContextFields["tenant"] != "updated" {
		return fmt.Errorf("updated paused schedule = %+v input=%#v", updated, updatedInput)
	}
	for until := time.Now().Add(2 * updatedInterval); time.Now().Before(until); {
		observed, ids, err := waitForObservedTargets(ctx, app.Client, createdFrom, runID)
		targetIDs = appendMissingIDs(targetIDs, ids...)
		if err != nil {
			return err
		}
		for _, target := range observed {
			if target.Tags["phase"] == "updated" {
				return errors.New("updated target started while its schedule was paused")
			}
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(200 * time.Millisecond):
		}
	}
	if err := handle.Resume(ctx); err != nil {
		return err
	}
	resumed, err := handle.Describe(ctx)
	if err != nil {
		return err
	}
	if resumed.Status != durabletaskscheduler.ScheduleStatusActive {
		return fmt.Errorf("resumed schedule status=%s, want active", resumed.Status)
	}
	second, observed, err := waitForScheduledCompletion(ctx, app.Client, createdFrom, runID, "updated")
	targetIDs = appendMissingIDs(targetIDs, observed...)
	if err != nil {
		return err
	}
	var secondOutput scheduledOutput
	if err := second.ReadOutput(&secondOutput); err != nil {
		return err
	}
	if secondOutput.Phase != "updated" ||
		secondOutput.ContextFields["tenant"] != "updated" ||
		secondOutput.Run != runID {
		return fmt.Errorf("updated scheduled output = %#v", secondOutput)
	}

	if err := handle.Delete(ctx); err != nil {
		return err
	}
	handle = nil
	deleted, err := schedules.Get(ctx, scheduleID)
	if err != nil {
		return err
	}
	if deleted != nil {
		return fmt.Errorf("schedule %s still exists after delete: %+v", scheduleID, deleted)
	}
	_, observed, err = waitForObservedTargets(ctx, app.Client, createdFrom, runID)
	targetIDs = appendMissingIDs(targetIDs, observed...)
	return err
}

func scheduledTargetOrchestrator(ctx *task.OrchestrationContext) (any, error) {
	var input scheduledInput
	if err := ctx.GetInput(&input); err != nil {
		return nil, err
	}
	var output scheduledOutput
	if err := ctx.CallActivity(scheduledActivityName, task.WithActivityInput(input)).Await(&output); err != nil {
		return nil, err
	}
	output.ContextFields = map[string]string(api.ContextFieldsFromContext(ctx.Context()))
	return output, nil
}

func scheduledAttemptActivity(ctx task.ActivityContext) (any, error) {
	var input scheduledInput
	if err := ctx.GetInput(&input); err != nil {
		return nil, err
	}
	key := input.Run + "/" + input.Phase
	counterValue, _ := scheduledAttempts.LoadOrStore(key, new(atomic.Int32))
	attempt := counterValue.(*atomic.Int32).Add(1)
	if input.FailFirst && attempt == 1 {
		return nil, fmt.Errorf("deterministic first activity failure for %s", key)
	}
	return scheduledOutput{
		Run:     input.Run,
		Phase:   input.Phase,
		Attempt: attempt,
	}, nil
}

func waitForScheduledCompletion(
	ctx context.Context,
	client *durabletaskscheduler.Client,
	createdFrom time.Time,
	runID string,
	phase string,
) (*api.OrchestrationMetadata, []api.InstanceID, error) {
	deadline := time.Now().Add(45 * time.Second)
	for {
		observed, ids, err := waitForObservedTargets(ctx, client, createdFrom, runID)
		if err != nil {
			return nil, ids, err
		}
		for _, metadata := range observed {
			if metadata.Name == scheduledTargetName &&
				metadata.RuntimeStatus == api.RUNTIME_STATUS_COMPLETED &&
				metadata.Tags["phase"] == phase {
				full, err := client.FetchOrchestrationMetadata(ctx, metadata.InstanceID, api.WithFetchPayloads(true))
				if err != nil {
					return nil, ids, err
				}
				if full.SerializedOutput == "" {
					return nil, ids, fmt.Errorf(
						"completed scheduled target %s returned no output from FetchOrchestrationMetadata(..., api.WithFetchPayloads(true))",
						metadata.InstanceID,
					)
				}
				return full, ids, nil
			}
		}
		if time.Now().After(deadline) {
			return nil, ids, fmt.Errorf("scheduled target phase %q did not complete; observed %d target(s)", phase, len(observed))
		}
		select {
		case <-ctx.Done():
			return nil, ids, ctx.Err()
		case <-time.After(200 * time.Millisecond):
		}
	}
}

func waitForObservedTargets(
	ctx context.Context,
	client *durabletaskscheduler.Client,
	createdFrom time.Time,
	runID string,
) ([]*api.OrchestrationMetadata, []api.InstanceID, error) {
	query, err := client.QueryInstances(ctx, api.OrchestrationQuery{
		CreatedTimeFrom:       createdFrom,
		Tags:                  map[string]string{"sample": "scheduledtasks", "run": runID},
		PageSize:              100,
		FetchInputsAndOutputs: true,
	})
	if err != nil {
		return nil, nil, err
	}
	ids := make([]api.InstanceID, 0, len(query.Orchestrations))
	targets := make([]*api.OrchestrationMetadata, 0, len(query.Orchestrations))
	for _, metadata := range query.Orchestrations {
		if metadata.Name != scheduledTargetName {
			continue
		}
		targets = append(targets, metadata)
		ids = append(ids, metadata.InstanceID)
	}
	return targets, ids, nil
}

func appendMissingIDs(base []api.InstanceID, additions ...api.InstanceID) []api.InstanceID {
	for _, id := range additions {
		if id != "" && !slices.Contains(base, id) {
			base = append(base, id)
		}
	}
	return base
}
