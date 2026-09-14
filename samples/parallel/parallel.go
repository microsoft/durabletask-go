// Command parallel demonstrates deterministic fan-out/fan-in with WhenAll and
// a timer-controlled race with WhenAny.
//
//	export DTS_CONNECTION_STRING="Endpoint=http://localhost:8080;TaskHub=default;Authentication=None"
//	go run ./samples/parallel
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"reflect"
	"time"

	"github.com/microsoft/durabletask-go/api"
	"github.com/microsoft/durabletask-go/samples/internal/dtssample"
	"github.com/microsoft/durabletask-go/task"
)

type parallelInput struct {
	Devices []deviceUpdate `json:"devices"`
}

type deviceUpdate struct {
	DeviceID       string `json:"deviceId"`
	TargetVersion  int    `json:"targetVersion"`
	ExpectedToFail bool   `json:"expectedToFail,omitempty"`
}

type deviceResult struct {
	DeviceID       string `json:"deviceId"`
	AppliedVersion int    `json:"appliedVersion"`
	Status         string `json:"status"`
}

type parallelSummary struct {
	RaceWinner string         `json:"raceWinner"`
	Total      int            `json:"total"`
	Updated    []string       `json:"updated"`
	Failed     []string       `json:"failed"`
	Results    []deviceResult `json:"results"`
}

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
	fmt.Println("SAMPLE_OK parallel")
}

func run() (err error) {
	registry := task.NewTaskRegistry()
	if err := registry.AddOrchestratorN("UpdateDevicesOrchestrator", UpdateDevicesOrchestrator); err != nil {
		return fmt.Errorf("failed to register orchestrator: %w", err)
	}
	if err := registry.AddActivityN("UpdateDevice", UpdateDevice); err != nil {
		return fmt.Errorf("failed to register activity: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	app, err := dtssample.Start(ctx, registry)
	if err != nil {
		return err
	}
	var ownedIDs []api.InstanceID
	defer func() { err = errors.Join(err, app.Shutdown()) }()
	defer func() { err = errors.Join(err, dtssample.Cleanup(app.Client, ownedIDs...)) }()

	id := dtssample.NewInstanceID("parallel")
	ownedIDs = append(ownedIDs, id)
	input := parallelInput{Devices: []deviceUpdate{
		{DeviceID: "thermostat-01", TargetVersion: 7},
		{DeviceID: "door-lock-02", TargetVersion: 7, ExpectedToFail: true},
		{DeviceID: "light-03", TargetVersion: 7},
		{DeviceID: "sensor-04", TargetVersion: 7},
	}}
	if _, err := app.Client.ScheduleNewOrchestration(
		ctx,
		"UpdateDevicesOrchestrator",
		api.WithInstanceID(id),
		api.WithInput(input),
	); err != nil {
		return fmt.Errorf("failed to schedule parallel orchestration: %w", err)
	}

	metadata, err := app.Client.WaitForOrchestrationCompletion(ctx, id, api.WithFetchPayloads(true))
	if err != nil {
		return fmt.Errorf("failed to wait for parallel orchestration: %w", err)
	}
	if err := dtssample.RequireCompleted(metadata); err != nil {
		return err
	}
	var output parallelSummary
	if err := metadata.ReadOutput(&output); err != nil {
		return fmt.Errorf("failed to decode parallel output: %w", err)
	}
	expected := parallelSummary{
		RaceWinner: "fast-timer",
		Total:      4,
		Updated:    []string{"thermostat-01", "light-03", "sensor-04"},
		Failed:     []string{"door-lock-02"},
		Results: []deviceResult{
			{DeviceID: "thermostat-01", AppliedVersion: 7, Status: "updated"},
			{DeviceID: "door-lock-02", AppliedVersion: 0, Status: "blocked-by-policy"},
			{DeviceID: "light-03", AppliedVersion: 7, Status: "updated"},
			{DeviceID: "sensor-04", AppliedVersion: 7, Status: "updated"},
		},
	}
	if !reflect.DeepEqual(output, expected) {
		return fmt.Errorf("parallel output = %#v, want %#v", output, expected)
	}
	return nil
}

// UpdateDevicesOrchestrator runs all device updates together, waits for every
// activity with WhenAll, and uses durable timers to make the WhenAny winner
// independent from worker wall-clock scheduling.
func UpdateDevicesOrchestrator(ctx *task.OrchestrationContext) (any, error) {
	var input parallelInput
	if err := ctx.GetInput(&input); err != nil {
		return nil, err
	}

	fastTimer := ctx.CreateTimer(time.Second)
	slowTimerCtx, cancelSlowTimer := ctx.WithCancel()
	slowTimer := slowTimerCtx.CreateTimer(3 * time.Second)
	raceWinner := "slow-timer"
	if ctx.WhenAny(fastTimer, slowTimer) == fastTimer {
		raceWinner = "fast-timer"
		cancelSlowTimer()
	}

	tasks := make([]task.Task, len(input.Devices))
	for i, device := range input.Devices {
		tasks[i] = ctx.CallActivity("UpdateDevice", task.WithActivityInput(device))
	}
	if err := ctx.WhenAll(tasks...); err != nil {
		return nil, err
	}

	summary := parallelSummary{
		RaceWinner: raceWinner,
		Total:      len(input.Devices),
		Results:    make([]deviceResult, len(tasks)),
	}
	for i, task := range tasks {
		var result deviceResult
		if err := task.Await(&result); err != nil {
			return nil, err
		}
		summary.Results[i] = result
		if result.Status == "updated" {
			summary.Updated = append(summary.Updated, result.DeviceID)
		} else {
			summary.Failed = append(summary.Failed, result.DeviceID)
		}
	}
	return summary, nil
}

// UpdateDevice is deterministic: the expected failure is part of the activity
// input rather than a random or host-clock result.
func UpdateDevice(ctx task.ActivityContext) (any, error) {
	var input deviceUpdate
	if err := ctx.GetInput(&input); err != nil {
		return nil, err
	}
	if input.ExpectedToFail {
		return deviceResult{DeviceID: input.DeviceID, Status: "blocked-by-policy"}, nil
	}
	return deviceResult{
		DeviceID:       input.DeviceID,
		AppliedVersion: input.TargetVersion,
		Status:         "updated",
	}, nil
}
