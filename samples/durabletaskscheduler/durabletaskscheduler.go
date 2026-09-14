// Command durabletaskscheduler demonstrates the basic Durable Task Scheduler
// client and worker flow with typed orchestration input and output.
//
//	export DTS_CONNECTION_STRING="Endpoint=http://localhost:8080;TaskHub=default;Authentication=None"
//	go run ./samples/durabletaskscheduler
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

type itineraryInput struct {
	Cities []string `json:"cities"`
}

type greeting struct {
	City    string `json:"city"`
	Message string `json:"message"`
}

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
	fmt.Println("SAMPLE_OK durabletaskscheduler")
}

func run() (err error) {
	registry := task.NewTaskRegistry()
	if err := registry.AddOrchestratorN("ActivitySequence", activitySequence); err != nil {
		return err
	}
	if err := registry.AddActivityN("SayHello", sayHello); err != nil {
		return err
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

	id := dtssample.NewInstanceID("durabletaskscheduler")
	ownedIDs = append(ownedIDs, id)
	input := itineraryInput{Cities: []string{"Tokyo", "London", "Seattle"}}
	if _, err := app.Client.ScheduleNewOrchestration(
		ctx,
		"ActivitySequence",
		api.WithInstanceID(id),
		api.WithInput(input),
	); err != nil {
		return fmt.Errorf("failed to schedule activity sequence: %w", err)
	}

	metadata, err := app.Client.WaitForOrchestrationCompletion(ctx, id, api.WithFetchPayloads(true))
	if err != nil {
		return fmt.Errorf("failed to wait for activity sequence: %w", err)
	}
	if err := dtssample.RequireCompleted(metadata); err != nil {
		return err
	}
	var output []greeting
	if err := metadata.ReadOutput(&output); err != nil {
		return fmt.Errorf("failed to decode activity sequence output: %w", err)
	}
	expected := []greeting{
		{City: "Tokyo", Message: "Hello, Tokyo!"},
		{City: "London", Message: "Hello, London!"},
		{City: "Seattle", Message: "Hello, Seattle!"},
	}
	if !reflect.DeepEqual(output, expected) {
		return fmt.Errorf("activity sequence output = %#v, want %#v", output, expected)
	}
	return nil
}

func activitySequence(ctx *task.OrchestrationContext) (any, error) {
	var input itineraryInput
	if err := ctx.GetInput(&input); err != nil {
		return nil, err
	}
	results := make([]greeting, 0, len(input.Cities))
	for _, city := range input.Cities {
		var result greeting
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
	return greeting{City: city, Message: "Hello, " + city + "!"}, nil
}
