// Command timers demonstrates durable timers, scheduled start, long-timer
// splitting, replay-stable orchestration time, and deterministic GUIDs.
//
//	export DTS_CONNECTION_STRING="Endpoint=http://localhost:8080;TaskHub=default;Authentication=None"
//	go run ./samples/timers
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

const (
	maximumPhysicalTimerInterval = 2 * time.Second
	logicalTimerDelay            = 5 * time.Second
	expectedPhysicalTimers       = 3
)

type timerInput struct {
	Delay time.Duration `json:"delay"`
}

type stableObservation struct {
	StartedAt time.Time `json:"startedAt"`
	Deadline  time.Time `json:"deadline"`
	GUID      string    `json:"guid"`
}

type timerOutput struct {
	StartedAt  time.Time         `json:"startedAt"`
	Deadline   time.Time         `json:"deadline"`
	FiredAt    time.Time         `json:"firedAt"`
	FirstGUID  string            `json:"firstGuid"`
	SecondGUID string            `json:"secondGuid"`
	Recorded   stableObservation `json:"recorded"`
}

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
	fmt.Println("SAMPLE_OK timers")
}

func run() (err error) {
	options, err := dtssample.Options()
	if err != nil {
		return err
	}
	options.MaximumTimerInterval = maximumPhysicalTimerInterval

	registry := task.NewTaskRegistry()
	if err := registry.AddOrchestratorN("TimerOrchestrator", TimerOrchestrator); err != nil {
		return fmt.Errorf("failed to register orchestrator: %w", err)
	}
	if err := registry.AddActivityN("RecordStableObservation", RecordStableObservation); err != nil {
		return fmt.Errorf("failed to register activity: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	app, err := dtssample.StartWithOptions(ctx, options, registry)
	if err != nil {
		return err
	}
	var ownedIDs []api.InstanceID
	defer func() { err = errors.Join(err, app.Shutdown()) }()
	defer func() { err = errors.Join(err, dtssample.Cleanup(app.Client, ownedIDs...)) }()

	scheduledStart := time.Now().UTC().Add(2 * time.Second)
	id := dtssample.NewInstanceID("timers")
	ownedIDs = append(ownedIDs, id)
	if _, err := app.Client.ScheduleNewOrchestration(
		ctx,
		"TimerOrchestrator",
		api.WithInstanceID(id),
		api.WithInput(timerInput{Delay: logicalTimerDelay}),
		api.WithStartTime(scheduledStart),
	); err != nil {
		return fmt.Errorf("failed to schedule timer orchestration: %w", err)
	}

	metadata, err := app.Client.WaitForOrchestrationCompletion(ctx, id, api.WithFetchPayloads(true))
	if err != nil {
		return fmt.Errorf("failed to wait for timer orchestration completion: %w", err)
	}
	if err := dtssample.RequireCompleted(metadata); err != nil {
		return err
	}
	var output timerOutput
	if err := metadata.ReadOutput(&output); err != nil {
		return fmt.Errorf("failed to decode timer output: %w", err)
	}
	if metadata.ScheduledStartAt.IsZero() {
		return errors.New("metadata did not include the scheduled start time")
	}
	if scheduledStart.Sub(metadata.ScheduledStartAt).Abs() > time.Second {
		return fmt.Errorf("scheduled start = %s, want near %s", metadata.ScheduledStartAt, scheduledStart)
	}
	if output.StartedAt.Before(metadata.ScheduledStartAt.Add(-time.Second)) {
		return fmt.Errorf("orchestration started at %s before scheduled start %s", output.StartedAt, metadata.ScheduledStartAt)
	}
	if !output.Deadline.Equal(output.StartedAt.Add(logicalTimerDelay)) {
		return fmt.Errorf("deadline = %s, want %s", output.Deadline, output.StartedAt.Add(logicalTimerDelay))
	}
	if output.FiredAt.Before(output.Deadline) {
		return fmt.Errorf("timer fired at %s before logical deadline %s", output.FiredAt, output.Deadline)
	}
	if output.FirstGUID == "" || output.SecondGUID == "" || output.FirstGUID == output.SecondGUID {
		return fmt.Errorf("unexpected deterministic GUIDs: first=%q second=%q", output.FirstGUID, output.SecondGUID)
	}
	if output.Recorded != (stableObservation{StartedAt: output.StartedAt, Deadline: output.Deadline, GUID: output.FirstGUID}) {
		return fmt.Errorf("recorded stable values = %#v, want orchestration values", output.Recorded)
	}

	history, err := app.Client.GetOrchestrationHistory(ctx, id, api.HistoryQuery{ExecutionID: metadata.ExecutionID})
	if err != nil {
		return fmt.Errorf("failed to read timer orchestration history: %w", err)
	}
	timerCreated, timerFired := 0, 0
	var lastTimerFireAt time.Time
	for _, event := range history.Events {
		switch event.Type {
		case api.HistoryEventTimerCreated:
			timerCreated++
		case api.HistoryEventTimerFired:
			timerFired++
			lastTimerFireAt = event.TimerFired.FireAt
		}
	}
	if timerCreated != expectedPhysicalTimers || timerFired != expectedPhysicalTimers {
		return fmt.Errorf("physical timers created/fired = %d/%d, want %d/%d",
			timerCreated, timerFired, expectedPhysicalTimers, expectedPhysicalTimers)
	}
	if !lastTimerFireAt.Equal(output.Deadline) {
		return fmt.Errorf("last physical timer fire = %s, want logical deadline %s", lastTimerFireAt, output.Deadline)
	}
	return nil
}

func TimerOrchestrator(ctx *task.OrchestrationContext) (any, error) {
	var input timerInput
	if err := ctx.GetInput(&input); err != nil {
		return nil, err
	}
	startedAt := ctx.CurrentTimeUtc
	firstGUID := ctx.NewGuid()
	deadline := startedAt.Add(input.Delay)
	if err := ctx.CreateTimer(input.Delay).Await(nil); err != nil {
		return nil, err
	}
	firedAt := ctx.CurrentTimeUtc
	secondGUID := ctx.NewGuid()
	observation := stableObservation{StartedAt: startedAt, Deadline: deadline, GUID: firstGUID}
	var recorded stableObservation
	if err := ctx.CallActivity("RecordStableObservation", task.WithActivityInput(observation)).Await(&recorded); err != nil {
		return nil, err
	}
	return timerOutput{
		StartedAt:  startedAt,
		Deadline:   deadline,
		FiredAt:    firedAt,
		FirstGUID:  firstGUID,
		SecondGUID: secondGUID,
		Recorded:   recorded,
	}, nil
}

func RecordStableObservation(ctx task.ActivityContext) (any, error) {
	var observation stableObservation
	if err := ctx.GetInput(&observation); err != nil {
		return nil, err
	}
	return observation, nil
}
