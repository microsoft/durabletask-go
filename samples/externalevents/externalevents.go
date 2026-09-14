// Command externalevents demonstrates single external events, repeated typed
// event channels, cross-instance durable SendEvent, and an expected timeout.
//
//	export DTS_CONNECTION_STRING="Endpoint=http://localhost:8080;TaskHub=default;Authentication=None"
//	printf 'Taylor\n' | go run ./samples/externalevents
package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"reflect"
	"time"

	"github.com/microsoft/durabletask-go/api"
	"github.com/microsoft/durabletask-go/samples/internal/dtssample"
	"github.com/microsoft/durabletask-go/task"
)

const (
	sampleHostTimeout = 5 * time.Minute
	nameEventTimeout  = 30 * time.Second
)

type checkpointEvent struct {
	Index int    `json:"index"`
	Label string `json:"label"`
}

type signalEvent struct {
	From    string `json:"from"`
	Message string `json:"message"`
}

type forwardInput struct {
	Target api.InstanceID `json:"target"`
	Signal signalEvent    `json:"signal"`
}

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
	fmt.Println("SAMPLE_OK externalevents")
}

func run() (err error) {
	registry := task.NewTaskRegistry()
	for name, orchestrator := range map[string]func(*task.OrchestrationContext) (any, error){
		"ExternalEventOrchestrator":        ExternalEventOrchestrator,
		"RepeatedEventChannelOrch":         RepeatedEventChannelOrchestrator,
		"CrossInstanceReceiverOrch":        CrossInstanceReceiverOrchestrator,
		"CrossInstanceSenderOrch":          CrossInstanceSenderOrchestrator,
		"ExternalEventTimeoutOrchestrator": ExternalEventTimeoutOrchestrator,
	} {
		if err := registry.AddOrchestratorN(name, orchestrator); err != nil {
			return fmt.Errorf("failed to register %s: %w", name, err)
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), sampleHostTimeout)
	defer cancel()
	app, err := dtssample.Start(ctx, registry)
	if err != nil {
		return err
	}
	var ownedIDs []api.InstanceID
	defer func() { err = errors.Join(err, app.Shutdown()) }()
	defer func() { err = errors.Join(err, dtssample.Cleanup(app.Client, ownedIDs...)) }()

	if err := runSingleNameEventScenario(ctx, app.Client, &ownedIDs); err != nil {
		return err
	}
	if err := runRepeatedEventChannelScenario(ctx, app.Client, &ownedIDs); err != nil {
		return err
	}
	if err := runCrossInstanceSendEventScenario(ctx, app.Client, &ownedIDs); err != nil {
		return err
	}
	if err := runExpectedTimeoutScenario(ctx, app.Client, &ownedIDs); err != nil {
		return err
	}
	return nil
}

func runSingleNameEventScenario(
	ctx context.Context,
	client interface {
		ScheduleNewOrchestration(context.Context, string, ...api.NewOrchestrationOptions) (api.InstanceID, error)
		WaitForOrchestrationStart(context.Context, api.InstanceID, ...api.FetchOrchestrationMetadataOptions) (*api.OrchestrationMetadata, error)
		WaitForOrchestrationCompletion(context.Context, api.InstanceID, ...api.FetchOrchestrationMetadataOptions) (*api.OrchestrationMetadata, error)
		RaiseEvent(context.Context, api.InstanceID, string, ...api.RaiseEventOptions) error
	},
	ownedIDs *[]api.InstanceID,
) error {
	id := dtssample.NewInstanceID("externalevents-name")
	*ownedIDs = append(*ownedIDs, id)
	if _, err := client.ScheduleNewOrchestration(ctx, "ExternalEventOrchestrator", api.WithInstanceID(id)); err != nil {
		return fmt.Errorf("failed to schedule name event orchestration: %w", err)
	}
	if _, err := client.WaitForOrchestrationStart(ctx, id); err != nil {
		return fmt.Errorf("failed to wait for name event orchestration start: %w", err)
	}

	waitCtx, cancelWait := context.WithCancelCause(ctx)
	defer cancelWait(nil)
	go readAndRaiseEvent(waitCtx, cancelWait, os.Stdin, func(ctx context.Context, name string) error {
		return client.RaiseEvent(ctx, id, "Name", api.WithEventPayload(name))
	})

	metadata, err := client.WaitForOrchestrationCompletion(waitCtx, id, api.WithFetchPayloads(true))
	if err != nil {
		if cause := context.Cause(waitCtx); cause != nil {
			err = cause
		}
		return fmt.Errorf("failed to wait for name event orchestration completion: %w", err)
	}
	if err := dtssample.RequireCompleted(metadata); err != nil {
		return err
	}
	var output string
	if err := metadata.ReadOutput(&output); err != nil {
		return fmt.Errorf("failed to decode name event output: %w", err)
	}
	if output != "Hello, Taylor!" {
		return fmt.Errorf("name event output = %q, want %q", output, "Hello, Taylor!")
	}
	return nil
}

func runRepeatedEventChannelScenario(
	ctx context.Context,
	client interface {
		ScheduleNewOrchestration(context.Context, string, ...api.NewOrchestrationOptions) (api.InstanceID, error)
		WaitForOrchestrationStart(context.Context, api.InstanceID, ...api.FetchOrchestrationMetadataOptions) (*api.OrchestrationMetadata, error)
		WaitForOrchestrationCompletion(context.Context, api.InstanceID, ...api.FetchOrchestrationMetadataOptions) (*api.OrchestrationMetadata, error)
		RaiseEvent(context.Context, api.InstanceID, string, ...api.RaiseEventOptions) error
	},
	ownedIDs *[]api.InstanceID,
) error {
	id := dtssample.NewInstanceID("externalevents-repeated")
	*ownedIDs = append(*ownedIDs, id)
	if _, err := client.ScheduleNewOrchestration(
		ctx,
		"RepeatedEventChannelOrch",
		api.WithInstanceID(id),
		api.WithInput(3),
	); err != nil {
		return fmt.Errorf("failed to schedule repeated event orchestration: %w", err)
	}
	if _, err := client.WaitForOrchestrationStart(ctx, id); err != nil {
		return fmt.Errorf("failed to wait for repeated event orchestration start: %w", err)
	}
	events := []checkpointEvent{
		{Index: 1, Label: "received"},
		{Index: 2, Label: "validated"},
		{Index: 3, Label: "approved"},
	}
	for _, event := range events {
		if err := client.RaiseEvent(ctx, id, "Checkpoint", api.WithEventPayload(event)); err != nil {
			return fmt.Errorf("failed to raise checkpoint %d: %w", event.Index, err)
		}
	}
	metadata, err := client.WaitForOrchestrationCompletion(ctx, id, api.WithFetchPayloads(true))
	if err != nil {
		return fmt.Errorf("failed to wait for repeated event orchestration completion: %w", err)
	}
	if err := dtssample.RequireCompleted(metadata); err != nil {
		return err
	}
	var output []string
	if err := metadata.ReadOutput(&output); err != nil {
		return fmt.Errorf("failed to decode repeated event output: %w", err)
	}
	expected := []string{"01:received", "02:validated", "03:approved"}
	if !reflect.DeepEqual(output, expected) {
		return fmt.Errorf("repeated event output = %#v, want %#v", output, expected)
	}
	return nil
}

func runCrossInstanceSendEventScenario(
	ctx context.Context,
	client interface {
		ScheduleNewOrchestration(context.Context, string, ...api.NewOrchestrationOptions) (api.InstanceID, error)
		WaitForOrchestrationStart(context.Context, api.InstanceID, ...api.FetchOrchestrationMetadataOptions) (*api.OrchestrationMetadata, error)
		WaitForOrchestrationCompletion(context.Context, api.InstanceID, ...api.FetchOrchestrationMetadataOptions) (*api.OrchestrationMetadata, error)
	},
	ownedIDs *[]api.InstanceID,
) error {
	receiverID := dtssample.NewInstanceID("externalevents-receiver")
	senderID := dtssample.NewInstanceID("externalevents-sender")
	*ownedIDs = append(*ownedIDs, receiverID, senderID)
	if _, err := client.ScheduleNewOrchestration(
		ctx,
		"CrossInstanceReceiverOrch",
		api.WithInstanceID(receiverID),
	); err != nil {
		return fmt.Errorf("failed to schedule receiver orchestration: %w", err)
	}
	if _, err := client.WaitForOrchestrationStart(ctx, receiverID); err != nil {
		return fmt.Errorf("failed to wait for receiver orchestration start: %w", err)
	}
	signal := signalEvent{From: "sender", Message: "approved"}
	if _, err := client.ScheduleNewOrchestration(
		ctx,
		"CrossInstanceSenderOrch",
		api.WithInstanceID(senderID),
		api.WithInput(forwardInput{Target: receiverID, Signal: signal}),
	); err != nil {
		return fmt.Errorf("failed to schedule sender orchestration: %w", err)
	}
	sender, err := client.WaitForOrchestrationCompletion(ctx, senderID, api.WithFetchPayloads(true))
	if err != nil {
		return fmt.Errorf("failed to wait for sender orchestration completion: %w", err)
	}
	if err := dtssample.RequireCompleted(sender); err != nil {
		return err
	}
	var senderOutput string
	if err := sender.ReadOutput(&senderOutput); err != nil {
		return fmt.Errorf("failed to decode sender output: %w", err)
	}
	if senderOutput != "sent" {
		return fmt.Errorf("sender output = %q, want sent", senderOutput)
	}
	receiver, err := client.WaitForOrchestrationCompletion(ctx, receiverID, api.WithFetchPayloads(true))
	if err != nil {
		return fmt.Errorf("failed to wait for receiver orchestration completion: %w", err)
	}
	if err := dtssample.RequireCompleted(receiver); err != nil {
		return err
	}
	var receiverOutput signalEvent
	if err := receiver.ReadOutput(&receiverOutput); err != nil {
		return fmt.Errorf("failed to decode receiver output: %w", err)
	}
	if receiverOutput != signal {
		return fmt.Errorf("receiver output = %#v, want %#v", receiverOutput, signal)
	}
	return nil
}

func runExpectedTimeoutScenario(
	ctx context.Context,
	client interface {
		ScheduleNewOrchestration(context.Context, string, ...api.NewOrchestrationOptions) (api.InstanceID, error)
		WaitForOrchestrationCompletion(context.Context, api.InstanceID, ...api.FetchOrchestrationMetadataOptions) (*api.OrchestrationMetadata, error)
	},
	ownedIDs *[]api.InstanceID,
) error {
	id := dtssample.NewInstanceID("externalevents-timeout")
	*ownedIDs = append(*ownedIDs, id)
	if _, err := client.ScheduleNewOrchestration(
		ctx,
		"ExternalEventTimeoutOrchestrator",
		api.WithInstanceID(id),
	); err != nil {
		return fmt.Errorf("failed to schedule timeout orchestration: %w", err)
	}
	metadata, err := client.WaitForOrchestrationCompletion(ctx, id, api.WithFetchPayloads(true))
	if err != nil {
		return fmt.Errorf("failed to wait for timeout orchestration completion: %w", err)
	}
	if err := dtssample.RequireCompleted(metadata); err != nil {
		return err
	}
	var output string
	if err := metadata.ReadOutput(&output); err != nil {
		return fmt.Errorf("failed to decode timeout output: %w", err)
	}
	if output != "timeout-observed" {
		return fmt.Errorf("timeout output = %q, want timeout-observed", output)
	}
	return nil
}

func readAndRaiseEvent(
	ctx context.Context,
	cancel context.CancelCauseFunc,
	input io.Reader,
	raise func(context.Context, string) error,
) {
	var name string
	if _, err := fmt.Fscanln(input, &name); err != nil {
		cancel(fmt.Errorf("failed to read input: %w", err))
		return
	}
	if err := raise(ctx, name); err != nil {
		cancel(fmt.Errorf("failed to raise event: %w", err))
	}
}

// ExternalEventOrchestrator blocks for 30 seconds or until a Name event arrives.
func ExternalEventOrchestrator(ctx *task.OrchestrationContext) (any, error) {
	var nameInput string
	if err := ctx.WaitForSingleEvent("Name", nameEventTimeout).Await(&nameInput); err != nil {
		return nil, err
	}
	return fmt.Sprintf("Hello, %s!", nameInput), nil
}

func RepeatedEventChannelOrchestrator(ctx *task.OrchestrationContext) (any, error) {
	var count int
	if err := ctx.GetInput(&count); err != nil {
		return nil, err
	}
	channel := task.NewEventChannel[checkpointEvent](ctx, "Checkpoint")
	results := make([]string, 0, count)
	for range count {
		event, err := channel.ReceiveErr(ctx)
		if err != nil {
			return nil, err
		}
		results = append(results, fmt.Sprintf("%02d:%s", event.Index, event.Label))
	}
	return results, nil
}

func CrossInstanceReceiverOrchestrator(ctx *task.OrchestrationContext) (any, error) {
	return task.NewEventChannel[signalEvent](ctx, "Forwarded").Receive(ctx), nil
}

func CrossInstanceSenderOrchestrator(ctx *task.OrchestrationContext) (any, error) {
	var input forwardInput
	if err := ctx.GetInput(&input); err != nil {
		return nil, err
	}
	if err := ctx.SendEvent(input.Target, "Forwarded", input.Signal); err != nil {
		return nil, err
	}
	if err := ctx.CreateTimer(100 * time.Millisecond).Await(nil); err != nil {
		return nil, err
	}
	return "sent", nil
}

func ExternalEventTimeoutOrchestrator(ctx *task.OrchestrationContext) (any, error) {
	if err := ctx.WaitForSingleEvent("Never", time.Second).Await(nil); !errors.Is(err, task.ErrTaskCanceled) {
		if err == nil {
			return nil, errors.New("timeout event completed without timing out")
		}
		return nil, err
	}
	return "timeout-observed", nil
}
