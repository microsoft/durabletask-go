// Command rewind repairs a failed activity without repeating successful work.
//
//	export DTS_CONNECTION_STRING="Endpoint=http://localhost:8080;TaskHub=default;Authentication=None"
//	go run ./samples/rewind
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/microsoft/durabletask-go/api"
	"github.com/microsoft/durabletask-go/samples/internal/dtssample"
	"github.com/microsoft/durabletask-go/task"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
	fmt.Println("SAMPLE_OK rewind")
}

func run() (err error) {
	name := "RewindSample_" + uuid.NewString()
	var repaired atomic.Bool
	var successfulCalls, failedCalls atomic.Int32
	registry := task.NewTaskRegistry()
	if err := registry.AddActivityN(name+"Good", func(task.ActivityContext) (any, error) {
		successfulCalls.Add(1)
		return "kept", nil
	}); err != nil {
		return err
	}
	if err := registry.AddActivityN(name+"Flaky", func(task.ActivityContext) (any, error) {
		failedCalls.Add(1)
		if !repaired.Load() {
			return nil, errors.New("dependency unavailable")
		}
		return "recovered", nil
	}); err != nil {
		return err
	}
	if err := registry.AddOrchestratorN(name, func(ctx *task.OrchestrationContext) (any, error) {
		var good, recovered string
		if err := ctx.CallActivity(name + "Good").Await(&good); err != nil {
			return nil, err
		}
		if err := ctx.CallActivity(name + "Flaky").Await(&recovered); err != nil {
			return nil, err
		}
		return good + ":" + recovered, nil
	}); err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	// Registry-derived filters and unique task names isolate this sample's work.
	app, err := dtssample.Start(ctx, registry)
	if err != nil {
		return err
	}
	id := api.InstanceID(name)
	defer func() { err = errors.Join(err, dtssample.Cleanup(app.Client, id), app.Shutdown()) }()
	if _, err := app.Client.ScheduleNewOrchestration(ctx, name, api.WithInstanceID(id)); err != nil {
		return err
	}
	failed, err := app.Client.WaitForOrchestrationCompletion(ctx, id)
	if err != nil {
		return err
	}
	if failed.RuntimeStatus != api.RUNTIME_STATUS_FAILED {
		return fmt.Errorf("expected initial failure, got %s", failed.RuntimeStatus)
	}

	repaired.Store(true)
	if err := app.Client.RewindInstance(ctx, id, api.WithRewindReason("dependency repaired")); err != nil {
		return err
	}
	fmt.Printf("Rewind enqueued for %s (previous execution %s)\n", id, failed.ExecutionID)

	// A normal completion wait can still return the previous Failed execution.
	// Require the replacement execution to complete successfully instead.
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()
	for {
		current, err := app.Client.FetchOrchestrationMetadata(ctx, id, api.WithFetchPayloads(true))
		if err != nil {
			return err
		}
		if current.ExecutionID != failed.ExecutionID && current.RuntimeStatus == api.RUNTIME_STATUS_COMPLETED {
			if current.SerializedOutput != `"kept:recovered"` || successfulCalls.Load() != 1 || failedCalls.Load() != 2 {
				return fmt.Errorf("unexpected recovery: output=%s successful calls=%d failed calls=%d",
					current.SerializedOutput, successfulCalls.Load(), failedCalls.Load())
			}
			fmt.Printf("Recovered execution %s: %s; successful activity ran once, failed activity ran twice\n",
				current.ExecutionID, current.SerializedOutput)
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("recovery not observed after rewind was enqueued: %w", ctx.Err())
		case <-ticker.C:
		}
	}
}
