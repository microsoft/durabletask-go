// Command history reads real DTS orchestration history through buffered and streaming APIs.
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"slices"
	"time"

	"github.com/microsoft/durabletask-go/api"
	"github.com/microsoft/durabletask-go/durabletaskscheduler"
	"github.com/microsoft/durabletask-go/samples/internal/dtssample"
	"github.com/microsoft/durabletask-go/task"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
	fmt.Println("SAMPLE_OK history")
}

func run() (err error) {
	registry := task.NewTaskRegistry()
	if err := registry.AddOrchestratorN("SampleHistoryWorkflow", historyWorkflow); err != nil {
		return err
	}
	if err := registry.AddActivityN("SampleHistoryEcho", historyEchoActivity); err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	app, err := dtssample.Start(ctx, registry)
	if err != nil {
		return err
	}
	id := dtssample.NewInstanceID("history")
	defer func() { err = errors.Join(err, dtssample.Cleanup(app.Client, id), app.Shutdown()) }()

	input := historyPayload{Label: "history", Count: 2}
	if _, err := app.Client.ScheduleNewOrchestration(ctx, "SampleHistoryWorkflow",
		api.WithInstanceID(id), api.WithInput(input)); err != nil {
		return err
	}
	metadata, err := app.Client.WaitForOrchestrationCompletion(ctx, id, api.WithFetchPayloads(true))
	if err != nil {
		return err
	}
	if err := dtssample.RequireCompleted(metadata); err != nil {
		return err
	}
	if metadata.ExecutionID == "" {
		return errors.New("completed metadata did not include an execution ID")
	}
	var output historyOutput
	if err := metadata.ReadOutput(&output); err != nil {
		return err
	}
	if output.Activity != "history:2" || output.FinalCount != 3 {
		return fmt.Errorf("history workflow output = %+v", output)
	}

	query := api.HistoryQuery{ExecutionID: metadata.ExecutionID, MaxEvents: 100, MaxBytes: 1 << 20}
	history, err := app.Client.GetOrchestrationHistory(ctx, id, query)
	if err != nil {
		return err
	}
	if err := verifyBufferedHistory(history, input, output, metadata.ExecutionID); err != nil {
		return err
	}
	if err := verifyStreamedHistory(ctx, app.Client, id, query, history); err != nil {
		return err
	}
	if err := verifyHistoryCaps(ctx, app.Client, id, metadata.ExecutionID); err != nil {
		return err
	}
	return nil
}

func historyWorkflow(ctx *task.OrchestrationContext) (any, error) {
	var input historyPayload
	if err := ctx.GetInput(&input); err != nil {
		return nil, err
	}
	activityInput := historyPayload{Label: input.Label, Count: input.Count}
	var activityResult string
	if err := ctx.CallActivity("SampleHistoryEcho", task.WithActivityInput(activityInput)).Await(&activityResult); err != nil {
		return nil, err
	}
	if err := ctx.CreateTimer(100 * time.Millisecond).Await(nil); err != nil {
		return nil, err
	}
	return historyOutput{Activity: activityResult, FinalCount: input.Count + 1}, nil
}

func historyEchoActivity(ctx task.ActivityContext) (any, error) {
	var input historyPayload
	if err := ctx.GetInput(&input); err != nil {
		return nil, err
	}
	return fmt.Sprintf("%s:%d", input.Label, input.Count), nil
}

type historyPayload struct {
	Label string `json:"label"`
	Count int    `json:"count"`
}

type historyOutput struct {
	Activity   string `json:"activity"`
	FinalCount int    `json:"finalCount"`
}

func verifyBufferedHistory(history *api.OrchestrationHistory, input historyPayload, output historyOutput, executionID string) error {
	if history == nil {
		return errors.New("buffered history was nil")
	}
	if history.ExecutionID != executionID {
		return fmt.Errorf("history execution ID = %q, want %q", history.ExecutionID, executionID)
	}
	if len(history.Events) < 6 {
		return fmt.Errorf("history contained %d events, want a real completed workflow history", len(history.Events))
	}
	if err := verifyEventOrder(history.Events); err != nil {
		return err
	}
	started := firstEvent(history.Events, api.HistoryEventExecutionStarted)
	if started == nil || started.ExecutionStarted == nil {
		return errors.New("history missing ExecutionStarted")
	}
	var startedInput historyPayload
	if err := started.ReadInput(&startedInput); err != nil {
		return err
	}
	if startedInput != input {
		return fmt.Errorf("ExecutionStarted input = %+v", startedInput)
	}

	scheduled := firstTaskScheduled(history.Events, "SampleHistoryEcho")
	if scheduled == nil {
		return errors.New("history missing SampleHistoryEcho schedule")
	}
	var scheduledInput historyPayload
	if err := scheduled.ReadInput(&scheduledInput); err != nil {
		return err
	}
	if scheduledInput != input {
		return fmt.Errorf("TaskScheduled input = %+v", scheduledInput)
	}

	completedTask := firstEvent(history.Events, api.HistoryEventTaskCompleted)
	if completedTask == nil {
		return errors.New("history missing TaskCompleted")
	}
	var activityResult string
	if err := completedTask.ReadResult(&activityResult); err != nil {
		return err
	}
	if activityResult != output.Activity {
		return fmt.Errorf("TaskCompleted result = %q", activityResult)
	}
	completed := firstEvent(history.Events, api.HistoryEventExecutionCompleted)
	if completed == nil || completed.ExecutionCompleted == nil {
		return errors.New("history missing ExecutionCompleted")
	}
	var completedOutput historyOutput
	if err := completed.ReadResult(&completedOutput); err != nil {
		return err
	}
	if completedOutput != output {
		return fmt.Errorf("ExecutionCompleted output = %+v", completedOutput)
	}
	fmt.Printf("verified buffered history with %d events pinned to execution %s\n", len(history.Events), executionID)
	return nil
}

func verifyStreamedHistory(
	ctx context.Context,
	client *durabletaskscheduler.Client,
	id api.InstanceID,
	query api.HistoryQuery,
	buffered *api.OrchestrationHistory,
) error {
	var streamed []*api.HistoryEvent
	if err := client.StreamOrchestrationHistory(ctx, id, query, func(event *api.HistoryEvent) error {
		streamed = append(streamed, event)
		return nil
	}); err != nil {
		return err
	}
	if len(streamed) != len(buffered.Events) {
		return fmt.Errorf("streamed %d events, buffered %d", len(streamed), len(buffered.Events))
	}
	for i := range streamed {
		if streamed[i].Type != buffered.Events[i].Type || streamed[i].EventID != buffered.Events[i].EventID {
			return fmt.Errorf("streamed event %d = (%s,%d), buffered (%s,%d)",
				i, streamed[i].Type, streamed[i].EventID, buffered.Events[i].Type, buffered.Events[i].EventID)
		}
	}
	fmt.Println("verified streamed history identity and order match buffered history")
	return nil
}

func verifyHistoryCaps(ctx context.Context, client *durabletaskscheduler.Client, id api.InstanceID, executionID string) error {
	_, err := client.GetOrchestrationHistory(ctx, id, api.HistoryQuery{ExecutionID: executionID, MaxEvents: 1})
	if !errors.Is(err, api.ErrHistoryLimitExceeded) {
		return fmt.Errorf("MaxEvents cap error = %v, want %v", err, api.ErrHistoryLimitExceeded)
	}
	_, err = client.GetOrchestrationHistory(ctx, id, api.HistoryQuery{ExecutionID: executionID, MaxBytes: 64})
	if !errors.Is(err, api.ErrHistoryLimitExceeded) {
		return fmt.Errorf("MaxBytes cap error = %v, want %v", err, api.ErrHistoryLimitExceeded)
	}
	fmt.Println("verified buffered history event and byte caps")
	return nil
}

func verifyEventOrder(events []*api.HistoryEvent) error {
	positions := map[api.HistoryEventType]int{}
	for i, event := range events {
		if _, ok := positions[event.Type]; !ok {
			positions[event.Type] = i
		}
	}
	required := []api.HistoryEventType{
		api.HistoryEventExecutionStarted,
		api.HistoryEventTaskScheduled,
		api.HistoryEventTaskCompleted,
		api.HistoryEventTimerCreated,
		api.HistoryEventTimerFired,
		api.HistoryEventExecutionCompleted,
	}
	for _, eventType := range required {
		if _, ok := positions[eventType]; !ok {
			return fmt.Errorf("history missing %s", eventType)
		}
	}
	for i := 1; i < len(required); i++ {
		if positions[required[i-1]] > positions[required[i]] {
			return fmt.Errorf("%s appeared after %s", required[i-1], required[i])
		}
	}
	return nil
}

func firstEvent(events []*api.HistoryEvent, eventType api.HistoryEventType) *api.HistoryEvent {
	index := slices.IndexFunc(events, func(event *api.HistoryEvent) bool {
		return event != nil && event.Type == eventType
	})
	if index < 0 {
		return nil
	}
	return events[index]
}

func firstTaskScheduled(events []*api.HistoryEvent, name string) *api.HistoryEvent {
	index := slices.IndexFunc(events, func(event *api.HistoryEvent) bool {
		return event != nil && event.Type == api.HistoryEventTaskScheduled &&
			event.TaskScheduled != nil && event.TaskScheduled.Name == name
	})
	if index < 0 {
		return nil
	}
	return events[index]
}
