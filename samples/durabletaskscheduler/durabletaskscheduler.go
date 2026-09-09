// Command durabletaskscheduler demonstrates the Durable Task Scheduler surface:
// versioned registrations, tagged scheduling, orchestration history, and
// recurring scheduled tasks.
//
//	export DTS_CONNECTION_STRING="Endpoint=http://localhost:8080;TaskHub=default;Authentication=None"
//	go run ./samples/durabletaskscheduler
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
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
}

func run() error {
	options, err := dtssample.Options()
	if err != nil {
		return err
	}
	options.Versioning = &task.VersioningOptions{
		DefaultVersion: "1.0",
		MatchStrategy:  task.VersionMatchNone,
	}

	registry := task.NewTaskRegistry()
	if err := registry.AddOrchestratorNVersion("ActivitySequence", "1.0", activitySequence); err != nil {
		return err
	}
	if err := registry.AddActivityNVersion("SayHello", "1.0", sayHello); err != nil {
		return err
	}
	if err := durabletaskscheduler.RegisterScheduledTasksWithDefaultVersion(registry, options.Versioning.DefaultVersion); err != nil {
		return err
	}

	// Connect a client and worker to the Durable Task Scheduler task hub
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	app, err := dtssample.StartWithOptions(ctx, options, registry, durabletaskscheduler.WithScheduledTasks())
	if err != nil {
		return err
	}
	defer func() {
		if err := app.Shutdown(); err != nil {
			log.Printf("Failed to shut down: %v", err)
		}
	}()
	schedulerClient := app.Client

	instanceID, err := schedulerClient.ScheduleNewOrchestration(
		ctx,
		"ActivitySequence",
		api.WithTags(map[string]string{"sample": "durable-task-scheduler"}),
	)
	if err != nil {
		return err
	}
	metadata, err := schedulerClient.WaitForOrchestrationCompletion(ctx, instanceID)
	if err != nil {
		return err
	}
	output, err := json.MarshalIndent(metadata, "", "  ")
	if err != nil {
		return err
	}
	fmt.Println(string(output))
	query, err := schedulerClient.QueryInstances(ctx, api.OrchestrationQuery{
		Tags: map[string]string{"sample": "durable-task-scheduler"},
	})
	if err != nil {
		return err
	}
	fmt.Printf("matched %d tagged orchestration(s)\n", len(query.Orchestrations))

	history, err := schedulerClient.GetOrchestrationHistory(ctx, instanceID, api.HistoryQuery{
		ExecutionID: metadata.ExecutionID,
	})
	if err != nil {
		return err
	}
	fmt.Printf("history contains %d event(s)\n", len(history.Events))

	scheduleID := "sample-hourly"
	schedule, err := schedulerClient.ScheduledTasks().Create(ctx, durabletaskscheduler.ScheduleCreationOptions{
		ScheduleID:        scheduleID,
		OrchestrationName: "ActivitySequence",
		Interval:          time.Hour,
		StartAt:           time.Now().UTC().Add(time.Hour),
		Tags:              map[string]string{"sample": "scheduled-task"},
	})
	if err != nil {
		return err
	}
	description, err := schedule.Describe(ctx)
	if err != nil {
		return err
	}
	fmt.Printf("schedule %s is %s; next run: %s\n", description.ScheduleID, description.Status, description.NextRunAt)
	if err := schedule.Delete(ctx); err != nil {
		return err
	}
	return nil
}

func activitySequence(ctx *task.OrchestrationContext) (any, error) {
	var results []string
	for _, city := range []string{"Tokyo", "London", "Seattle"} {
		var result string
		if err := ctx.CallActivity("SayHello", task.WithActivityInput(city)).Await(&result); err != nil {
			return nil, err
		}
		results = append(results, result)
	}
	return results, nil
}

func sayHello(ctx task.ActivityContext) (any, error) {
	var city string
	if err := ctx.GetInput(&city); err != nil {
		return nil, err
	}
	return "Hello, " + city + "!", nil
}
