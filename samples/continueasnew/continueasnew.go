// Command continueasnew demonstrates bounded ContinueAsNew patterns with
// explicit checkpoints, MaxHistoryEvents compaction, and MaxEventsPerTurn.
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/microsoft/durabletask-go/api"
	durabletaskclient "github.com/microsoft/durabletask-go/client"
	"github.com/microsoft/durabletask-go/durabletaskscheduler"
	"github.com/microsoft/durabletask-go/samples/internal/dtssample"
	"github.com/microsoft/durabletask-go/task"
)

const (
	carryoverName   = "SampleContinueCarryover"
	compactionName  = "SampleContinueHistoryCompaction"
	eventBudgetName = "SampleContinueEventBudget"
)

type continueCarryState struct {
	Want       int      `json:"want"`
	Items      []string `json:"items"`
	Generation int      `json:"generation"`
}

type continueCompactionState struct {
	WantItems               int      `json:"wantItems"`
	Items                   []string `json:"items"`
	Compactions             int      `json:"compactions"`
	LastHistoryLength       int      `json:"lastHistoryLength"`
	LastUnprocessedEventCnt int      `json:"lastUnprocessedEventCount"`
}

type continueBudgetInput struct {
	Count int `json:"count"`
}

type continueBudgetOutput struct {
	Items []int `json:"items"`
}

func main() {
	if err := run(os.Args[1:]); err != nil {
		log.Fatal(err)
	}
	fmt.Println("SAMPLE_OK continueasnew")
}

func run(args []string) error {
	scenarios := []struct {
		name string
		run  func(context.Context) error
	}{
		{"checkpoint", runCheckpointScenario},
		{"history-limit", runHistoryLimitScenario},
		{"event-budget", runEventBudgetScenario},
	}
	selected := map[string]bool{}
	if len(args) == 0 {
		selected["all"] = true
	} else {
		for _, arg := range args {
			name := strings.ToLower(strings.TrimSpace(arg))
			switch name {
			case "all", "checkpoint", "history-limit", "event-budget":
				selected[name] = true
			default:
				return fmt.Errorf("unknown scenario %q; use all, checkpoint, history-limit, or event-budget", arg)
			}
		}
	}
	for _, scenario := range scenarios {
		if !selected["all"] && !selected[scenario.name] {
			continue
		}
		ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
		err := scenario.run(ctx)
		cancel()
		if err != nil {
			return fmt.Errorf("%s scenario: %w", scenario.name, err)
		}
		fmt.Printf("verified %s\n", scenario.name)
	}
	return nil
}

func runCheckpointScenario(ctx context.Context) (err error) {
	registry := task.NewTaskRegistry()
	if err := registry.AddOrchestratorN(carryoverName, carryoverOrchestrator); err != nil {
		return err
	}
	app, err := startContinueApp(ctx, registry)
	if err != nil {
		return err
	}
	instanceID := dtssample.NewInstanceID("continueasnew-checkpoint")
	ownedIDs := []api.InstanceID{instanceID}
	defer func() { err = errors.Join(err, dtssample.Cleanup(app.Client, ownedIDs...), app.Shutdown()) }()

	if _, err := app.Client.ScheduleNewOrchestration(
		ctx,
		carryoverName,
		api.WithInstanceID(instanceID),
		api.WithInput(continueCarryState{Want: 3}),
		api.WithTags(map[string]string{"sample": "continueasnew", "scenario": "checkpoint"}),
	); err != nil {
		return err
	}
	if _, err := app.Client.WaitForOrchestrationStart(ctx, instanceID); err != nil {
		return err
	}
	if err := app.Client.SuspendOrchestration(ctx, instanceID, "queue events before checkpoint"); err != nil {
		return err
	}
	for _, item := range []string{"one", "two", "three"} {
		if err := app.Client.RaiseEvent(ctx, instanceID, "item", api.WithEventPayload(item)); err != nil {
			return err
		}
	}
	if err := app.Client.ResumeOrchestration(ctx, instanceID, "drain carried events"); err != nil {
		return err
	}
	metadata, err := app.Client.WaitForOrchestrationCompletion(ctx, instanceID, api.WithFetchPayloads(true))
	if err != nil {
		return err
	}
	if err := dtssample.RequireCompleted(metadata); err != nil {
		return err
	}
	var output continueCarryState
	if err := metadata.ReadOutput(&output); err != nil {
		return err
	}
	if output.Generation != 3 || !sameStringSet(output.Items, []string{"one", "two", "three"}) {
		return fmt.Errorf("checkpoint output = %#v", output)
	}
	if metadata.ExecutionID == "" {
		return errors.New("checkpoint completion did not include the latest execution ID")
	}
	return nil
}

func runHistoryLimitScenario(ctx context.Context) (err error) {
	registry := task.NewTaskRegistry()
	if err := registry.AddOrchestratorN(compactionName, compactionOrchestrator); err != nil {
		return err
	}
	app, err := startContinueApp(ctx, registry, task.WithOrchestrationOptions(task.OrchestrationOptions{
		MaxHistoryEvents: 8,
		OnHistoryLimitExceeded: func(info task.HistoryLimitInfo) (any, error) {
			var state continueCompactionState
			if err := info.GetInput(&state); err != nil {
				return nil, err
			}
			state.Compactions++
			state.LastHistoryLength = info.HistoryLength
			state.LastUnprocessedEventCnt = info.UnprocessedEventCount
			return state, nil
		},
	}))
	if err != nil {
		return err
	}
	instanceID := dtssample.NewInstanceID("continueasnew-history")
	ownedIDs := []api.InstanceID{instanceID}
	defer func() { err = errors.Join(err, dtssample.Cleanup(app.Client, ownedIDs...), app.Shutdown()) }()

	if _, err := app.Client.ScheduleNewOrchestration(
		ctx,
		compactionName,
		api.WithInstanceID(instanceID),
		api.WithInput(continueCompactionState{WantItems: 3}),
		api.WithTags(map[string]string{"sample": "continueasnew", "scenario": "history-limit"}),
	); err != nil {
		return err
	}
	started, err := app.Client.WaitForOrchestrationStart(ctx, instanceID, api.WithFetchPayloads(true))
	if err != nil {
		return err
	}
	if err := app.Client.SuspendOrchestration(ctx, instanceID, "build bounded carryover set"); err != nil {
		return err
	}
	for _, item := range []string{"red", "green", "blue"} {
		if err := app.Client.RaiseEvent(ctx, instanceID, "work", api.WithEventPayload(item)); err != nil {
			return err
		}
	}
	if err := app.Client.ResumeOrchestration(ctx, instanceID, "allow history-limit handler"); err != nil {
		return err
	}
	if err := waitForExecutionChange(ctx, app.Client, instanceID, started.ExecutionID); err != nil {
		return err
	}
	if err := app.Client.RaiseEvent(ctx, instanceID, "release", api.WithEventPayload(3)); err != nil {
		return err
	}
	metadata, err := app.Client.WaitForOrchestrationCompletion(ctx, instanceID, api.WithFetchPayloads(true))
	if err != nil {
		return err
	}
	if err := dtssample.RequireCompleted(metadata); err != nil {
		return err
	}
	var output continueCompactionState
	if err := metadata.ReadOutput(&output); err != nil {
		return err
	}
	if output.Compactions == 0 || output.LastHistoryLength <= output.LastUnprocessedEventCnt {
		return fmt.Errorf("history-limit handler did not report a real compaction: %#v", output)
	}
	if !sameStringSet(output.Items, []string{"red", "green", "blue"}) {
		return fmt.Errorf("compacted business items were lost or duplicated: %#v", output.Items)
	}
	history, err := app.Client.GetOrchestrationHistory(ctx, instanceID, api.HistoryQuery{ExecutionID: metadata.ExecutionID})
	if err != nil {
		return err
	}
	if len(history.Events) > 16 {
		return fmt.Errorf("latest execution history has %d events after compaction; expected a bounded current generation", len(history.Events))
	}
	return nil
}

func runEventBudgetScenario(ctx context.Context) (err error) {
	registry := task.NewTaskRegistry()
	if err := registry.AddOrchestratorN(eventBudgetName, eventBudgetOrchestrator); err != nil {
		return err
	}
	options, err := dtssample.Options()
	if err != nil {
		return err
	}
	client, err := durabletaskscheduler.NewClient(ctx, options, api.DefaultLogger())
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, client.Close()) }()
	instanceID := dtssample.NewInstanceID("continueasnew-event-budget")
	var mu sync.Mutex
	var processed []int
	worker, err := durabletaskscheduler.NewWorker(options, registry, api.DefaultLogger(),
		durabletaskclient.WithAutoWorkItemFilters(),
		durabletaskclient.WithTaskExecutorOptions(
			task.WithOrchestrationOptions(task.OrchestrationOptions{MaxEventsPerTurn: 1}),
			task.WithMetricsHooks(task.MetricsHooks{History: func(metric task.HistoryMetric) {
				if metric.InstanceID == instanceID {
					mu.Lock()
					processed = append(processed, metric.ProcessedEvents)
					mu.Unlock()
				}
			}}),
		),
	)
	if err != nil {
		return err
	}
	defer func() {
		stopCtx, stop := context.WithTimeout(context.Background(), 30*time.Second)
		defer stop()
		err = errors.Join(err, worker.Shutdown(stopCtx))
	}()
	defer func() { err = errors.Join(err, dtssample.Cleanup(client, instanceID)) }()

	if _, err := client.ScheduleNewOrchestration(
		ctx,
		eventBudgetName,
		api.WithInstanceID(instanceID),
		api.WithInput(continueBudgetInput{Count: 5}),
		api.WithTags(map[string]string{"sample": "continueasnew", "scenario": "event-budget"}),
	); err != nil {
		return err
	}
	// Queue the input before starting the worker, rather than relying on live arrival timing.
	for index := range 5 {
		if err := client.RaiseEvent(ctx, instanceID, "item", api.WithEventPayload(index)); err != nil {
			return err
		}
	}
	if err := worker.Start(ctx); err != nil {
		return err
	}
	metadata, err := client.WaitForOrchestrationCompletion(ctx, instanceID, api.WithFetchPayloads(true))
	if err != nil {
		return err
	}
	if err := dtssample.RequireCompleted(metadata); err != nil {
		return err
	}
	var output continueBudgetOutput
	if err := metadata.ReadOutput(&output); err != nil {
		return err
	}
	slices.Sort(output.Items)
	if !slices.Equal(output.Items, []int{0, 1, 2, 3, 4}) {
		return fmt.Errorf("MaxEventsPerTurn output = %#v", output.Items)
	}
	mu.Lock()
	counts := slices.Clone(processed)
	mu.Unlock()
	total := 0
	for _, count := range counts {
		if count > 1 {
			return fmt.Errorf("turn exceeded MaxEventsPerTurn=1: %v", counts)
		}
		total += count
	}
	if total < 5 {
		return fmt.Errorf("missing turn-level evidence for the five inputs: %v", counts)
	}
	fmt.Printf("verified per-turn event counts %v\n", counts)
	return nil
}

func startContinueApp(
	ctx context.Context,
	registry *task.TaskRegistry,
	executorOptions ...task.TaskExecutorOption,
) (*dtssample.App, error) {
	var workerOptions []durabletaskclient.TaskHubGrpcWorkerOption
	if len(executorOptions) > 0 {
		workerOptions = append(workerOptions, durabletaskclient.WithTaskExecutorOptions(executorOptions...))
	}
	return dtssample.Start(ctx, registry, workerOptions...)
}

func carryoverOrchestrator(ctx *task.OrchestrationContext) (any, error) {
	var state continueCarryState
	if err := ctx.GetInput(&state); err != nil {
		return nil, err
	}
	for len(state.Items) < state.Want {
		var item string
		if err := ctx.WaitForSingleEvent("item", time.Minute).Await(&item); err != nil {
			return nil, err
		}
		state.Items = append(state.Items, item)
		state.Generation++
		if len(state.Items) < state.Want {
			ctx.ContinueAsNew(state, task.WithKeepUnprocessedEvents())
			return nil, nil
		}
	}
	return state, nil
}

func compactionOrchestrator(ctx *task.OrchestrationContext) (any, error) {
	var state continueCompactionState
	if err := ctx.GetInput(&state); err != nil {
		return nil, err
	}
	var count int
	if err := ctx.WaitForSingleEvent("release", time.Minute).Await(&count); err != nil {
		return nil, err
	}
	for len(state.Items) < count {
		var item string
		if err := ctx.WaitForSingleEvent("work", time.Minute).Await(&item); err != nil {
			return nil, err
		}
		state.Items = append(state.Items, item)
	}
	return state, nil
}

func eventBudgetOrchestrator(ctx *task.OrchestrationContext) (any, error) {
	var input continueBudgetInput
	if err := ctx.GetInput(&input); err != nil {
		return nil, err
	}
	output := continueBudgetOutput{Items: make([]int, 0, input.Count)}
	for len(output.Items) < input.Count {
		var item int
		if err := ctx.WaitForSingleEvent("item", time.Minute).Await(&item); err != nil {
			return nil, err
		}
		output.Items = append(output.Items, item)
	}
	return output, nil
}

func waitForExecutionChange(
	ctx context.Context,
	client *durabletaskscheduler.Client,
	instanceID api.InstanceID,
	originalExecutionID string,
) error {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		current, err := client.FetchOrchestrationMetadata(ctx, instanceID, api.WithFetchPayloads(true))
		if err == nil && current.ExecutionID != "" && current.ExecutionID != originalExecutionID {
			return nil
		}
		if err != nil && !errors.Is(err, api.ErrInstanceNotFound) {
			return err
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("MaxHistoryEvents did not compact to a new execution before release was sent: %w", ctx.Err())
		case <-ticker.C:
		}
	}
}

func sameStringSet(left, right []string) bool {
	leftCopy := append([]string(nil), left...)
	rightCopy := append([]string(nil), right...)
	slices.Sort(leftCopy)
	slices.Sort(rightCopy)
	return slices.Equal(leftCopy, rightCopy)
}
