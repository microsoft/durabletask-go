// Command coroutines demonstrates orchestration coroutines, durable wait
// groups, Select, and child cancellation scopes.
//
//	export DTS_CONNECTION_STRING="Endpoint=http://localhost:8080;TaskHub=default;Authentication=None"
//	go run ./samples/coroutines
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

type coroutineInput struct {
	Values         []int         `json:"values"`
	ApprovalEvent  string        `json:"approvalEvent"`
	ApprovalWindow time.Duration `json:"approvalWindow"`
}

type coroutineOutput struct {
	Selected string `json:"selected"`
	Doubled  []int  `json:"doubled"`
	Sum      int    `json:"sum"`
}

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
	fmt.Println("SAMPLE_OK coroutines")
}

func run() (err error) {
	registry := task.NewTaskRegistry()
	if err := registry.AddOrchestratorN("CoroutineFanInOrchestrator", CoroutineFanInOrchestrator); err != nil {
		return fmt.Errorf("failed to register orchestrator: %w", err)
	}
	if err := registry.AddActivityN("DoubleValue", DoubleValue); err != nil {
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

	id := dtssample.NewInstanceID("coroutines")
	ownedIDs = append(ownedIDs, id)
	input := coroutineInput{
		Values:         []int{2, 4, 6},
		ApprovalEvent:  "approved",
		ApprovalWindow: 30 * time.Second,
	}
	if _, err := app.Client.ScheduleNewOrchestration(
		ctx,
		"CoroutineFanInOrchestrator",
		api.WithInstanceID(id),
		api.WithInput(input),
	); err != nil {
		return fmt.Errorf("failed to schedule coroutine orchestration: %w", err)
	}
	if _, err := app.Client.WaitForOrchestrationStart(ctx, id); err != nil {
		return fmt.Errorf("failed to wait for coroutine orchestration start: %w", err)
	}
	if err := app.Client.RaiseEvent(ctx, id, "Approval", api.WithEventPayload(input.ApprovalEvent)); err != nil {
		return fmt.Errorf("failed to raise approval event: %w", err)
	}

	metadata, err := app.Client.WaitForOrchestrationCompletion(ctx, id, api.WithFetchPayloads(true))
	if err != nil {
		return fmt.Errorf("failed to wait for coroutine orchestration completion: %w", err)
	}
	if err := dtssample.RequireCompleted(metadata); err != nil {
		return err
	}
	var output coroutineOutput
	if err := metadata.ReadOutput(&output); err != nil {
		return fmt.Errorf("failed to decode coroutine output: %w", err)
	}
	expected := coroutineOutput{
		Selected: "event:approved",
		Doubled:  []int{4, 8, 12},
		Sum:      24,
	}
	if !reflect.DeepEqual(output, expected) {
		return fmt.Errorf("coroutine output = %#v, want %#v", output, expected)
	}
	return nil
}

func CoroutineFanInOrchestrator(ctx *task.OrchestrationContext) (any, error) {
	var input coroutineInput
	if err := ctx.GetInput(&input); err != nil {
		return nil, err
	}

	doubled := make([]int, len(input.Values))
	waitGroup := ctx.NewWaitGroup()
	waitGroup.Add(len(input.Values))
	for i, value := range input.Values {
		i, value := i, value
		ctx.Go(func(ctx *task.OrchestrationContext) {
			defer waitGroup.Done()
			var result int
			if err := ctx.CallActivity("DoubleValue", task.WithActivityInput(value)).Await(&result); err != nil {
				panic(err)
			}
			doubled[i] = result
		})
	}

	timerCtx, cancelTimer := ctx.WithCancel()
	timer := timerCtx.CreateTimer(input.ApprovalWindow)
	approvals := task.NewEventChannel[string](ctx, "Approval")
	selected := "timeout"
	ctx.Select(
		task.OnEvent(approvals, func(value string) {
			selected = "event:" + value
			cancelTimer()
		}),
		task.OnTask(timer, func(task.Task) {
			selected = "timeout"
		}),
	)

	waitGroup.Wait(ctx)
	sum := 0
	for _, value := range doubled {
		sum += value
	}
	return coroutineOutput{Selected: selected, Doubled: doubled, Sum: sum}, nil
}

func DoubleValue(ctx task.ActivityContext) (any, error) {
	var value int
	if err := ctx.GetInput(&value); err != nil {
		return nil, err
	}
	return value * 2, nil
}
