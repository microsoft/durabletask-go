// Command dataconverter demonstrates a visible custom api.DataConverter on DTS.
package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/microsoft/durabletask-go/api"
	"github.com/microsoft/durabletask-go/durabletaskscheduler"
	"github.com/microsoft/durabletask-go/samples/internal/dtssample"
	"github.com/microsoft/durabletask-go/task"
)

const (
	converterPrefix = "sample-dc:"
	converterEntity = "sampledataconverterentity"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
	fmt.Println("SAMPLE_OK dataconverter")
}

func run() (err error) {
	converter := prefixedJSONConverter{}
	options, err := dtssample.Options()
	if err != nil {
		return err
	}
	options.DataConverter = converter

	registry := task.NewTaskRegistry()
	if err := registry.AddOrchestratorN("SampleDataConverterTyped", typedConverterWorkflow); err != nil {
		return err
	}
	if err := registry.AddOrchestratorN("SampleDataConverterRawBypass", rawBypassWorkflow); err != nil {
		return err
	}
	if err := registry.AddActivityN("SampleDataConverterEcho", converterEchoActivity); err != nil {
		return err
	}
	if err := registry.AddEntityN(converterEntity, task.NewEntityFor[converterCounter]()); err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	app, err := dtssample.StartWithOptions(ctx, options, registry)
	if err != nil {
		return err
	}
	typedID := dtssample.NewInstanceID("dataconverter-typed")
	rawID := dtssample.NewInstanceID("dataconverter-raw")
	entityID := api.NewEntityID(converterEntity, string(dtssample.NewInstanceID("dataconverter-entity")))
	defer func() {
		err = errors.Join(err, deleteConverterEntity(app.Client, entityID), dtssample.Cleanup(app.Client, typedID, rawID), app.Shutdown())
	}()

	if err := verifyTypedRoundTrip(ctx, app.Client, typedID, entityID); err != nil {
		return err
	}
	if err := verifyRawBypass(ctx, app.Client, rawID); err != nil {
		return err
	}
	return nil
}

func verifyTypedRoundTrip(ctx context.Context, client *durabletaskscheduler.Client, id api.InstanceID, entityID api.EntityID) error {
	input := samplePayload{Text: "typed-input", Count: 1}
	if _, err := client.ScheduleNewOrchestration(ctx, "SampleDataConverterTyped",
		api.WithInstanceID(id), api.WithInput(typedWorkflowInput{Payload: input, EntityID: entityID})); err != nil {
		return err
	}
	if _, err := client.WaitForOrchestrationStart(ctx, id, api.WithFetchPayloads(true)); err != nil {
		return err
	}
	if err := client.RaiseEvent(ctx, id, "payload", api.WithEventPayload(samplePayload{Text: "event", Count: 4})); err != nil {
		return err
	}
	metadata, err := client.WaitForOrchestrationCompletion(ctx, id, api.WithFetchPayloads(true))
	if err != nil {
		return err
	}
	if err := dtssample.RequireCompleted(metadata); err != nil {
		return err
	}
	for name, payload := range map[string]string{
		"typed input metadata":  metadata.SerializedInput,
		"typed output metadata": metadata.SerializedOutput,
		"typed custom status":   metadata.SerializedCustomStatus,
	} {
		if err := assertEncoded(name, payload); err != nil {
			return err
		}
	}

	var status samplePayload
	if err := metadata.ReadCustomStatus(&status); err != nil {
		return err
	}
	if status != (samplePayload{Text: "custom-status", Count: 2}) {
		return fmt.Errorf("custom status = %+v", status)
	}
	var output typedWorkflowOutput
	if err := metadata.ReadOutput(&output); err != nil {
		return err
	}
	if output.Input != input ||
		output.Activity != (samplePayload{Text: "activity:typed-input", Count: 2}) ||
		output.Event != (samplePayload{Text: "event", Count: 4}) ||
		output.EntityTotal != 4 {
		return fmt.Errorf("unexpected typed converter output: %+v", output)
	}
	entity, err := client.GetEntity(ctx, entityID)
	if err != nil {
		return err
	}
	if entity == nil || !entity.HasState {
		return errors.New("converter entity state was not persisted")
	}
	if err := assertEncoded("entity state", entity.SerializedState); err != nil {
		return err
	}
	var state converterCounter
	if err := entity.ReadState(&state); err != nil {
		return err
	}
	if state.Total != 4 {
		return fmt.Errorf("converter entity total = %d, want 4", state.Total)
	}
	fmt.Println("verified custom converter on typed input, result, event, status, and entity state")
	return nil
}

func verifyRawBypass(ctx context.Context, client *durabletaskscheduler.Client, id api.InstanceID) error {
	rawInput := `{"text":"legacy-json","count":5}`
	if _, err := client.ScheduleNewOrchestration(ctx, "SampleDataConverterRawBypass",
		api.WithInstanceID(id), api.WithRawInput(rawInput)); err != nil {
		return err
	}
	metadata, err := client.WaitForOrchestrationCompletion(ctx, id, api.WithFetchPayloads(true))
	if err != nil {
		return err
	}
	if err := dtssample.RequireCompleted(metadata); err != nil {
		return err
	}
	if metadata.SerializedInput != rawInput {
		return fmt.Errorf("raw input was changed: %q", metadata.SerializedInput)
	}
	if metadata.SerializedCustomStatus != "raw-status:not-converted" {
		return fmt.Errorf("raw custom status = %q", metadata.SerializedCustomStatus)
	}
	if err := assertEncoded("raw bypass output", metadata.SerializedOutput); err != nil {
		return err
	}
	var output samplePayload
	if err := metadata.ReadOutput(&output); err != nil {
		return err
	}
	if output != (samplePayload{Text: "raw:legacy-json", Count: 6}) {
		return fmt.Errorf("raw bypass output = %+v", output)
	}
	fmt.Println("verified intentional raw input/status bypass and JSON compatibility fallback")
	return nil
}

func typedConverterWorkflow(ctx *task.OrchestrationContext) (any, error) {
	var input typedWorkflowInput
	if err := ctx.GetInput(&input); err != nil {
		return nil, err
	}
	if err := ctx.SetCustomStatusValue(samplePayload{Text: "custom-status", Count: 2}); err != nil {
		return nil, err
	}
	var activity samplePayload
	if err := ctx.CallActivity("SampleDataConverterEcho", task.WithActivityInput(input.Payload)).Await(&activity); err != nil {
		return nil, err
	}
	var event samplePayload
	if err := ctx.WaitForSingleEvent("payload", 20*time.Second).Await(&event); err != nil {
		return nil, err
	}
	var entityTotal int
	if err := ctx.CallEntity(input.EntityID, "Add", task.WithEntityInput(event.Count)).Await(&entityTotal); err != nil {
		return nil, err
	}
	return typedWorkflowOutput{Input: input.Payload, Activity: activity, Event: event, EntityTotal: entityTotal}, nil
}

func rawBypassWorkflow(ctx *task.OrchestrationContext) (any, error) {
	var input samplePayload
	if err := ctx.GetInput(&input); err != nil {
		return nil, err
	}
	ctx.SetRawCustomStatus("raw-status:not-converted")
	return samplePayload{Text: "raw:" + input.Text, Count: input.Count + 1}, nil
}

func converterEchoActivity(ctx task.ActivityContext) (any, error) {
	var input samplePayload
	if err := ctx.GetInput(&input); err != nil {
		return nil, err
	}
	return samplePayload{Text: "activity:" + input.Text, Count: input.Count + 1}, nil
}

type typedWorkflowInput struct {
	Payload  samplePayload `json:"payload"`
	EntityID api.EntityID  `json:"entityId"`
}

type typedWorkflowOutput struct {
	Input       samplePayload `json:"input"`
	Activity    samplePayload `json:"activity"`
	Event       samplePayload `json:"event"`
	EntityTotal int           `json:"entityTotal"`
}

type samplePayload struct {
	Text  string `json:"text"`
	Count int    `json:"count"`
}

type converterCounter struct {
	Total int `json:"total"`
}

func (c *converterCounter) Add(amount int) int {
	c.Total += amount
	return c.Total
}

func deleteConverterEntity(client *durabletaskscheduler.Client, id api.EntityID) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	if err := client.SignalEntity(ctx, id, "delete"); err != nil {
		return err
	}
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		metadata, err := client.GetEntity(ctx, id)
		if err != nil {
			return err
		}
		if metadata == nil || !metadata.HasState {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

type prefixedJSONConverter struct{}

func (prefixedJSONConverter) Serialize(value any) (string, error) {
	data, err := json.Marshal(value)
	if err != nil {
		return "", err
	}
	return converterPrefix + base64.StdEncoding.EncodeToString(data), nil
}

func (prefixedJSONConverter) Deserialize(payload string, target any) error {
	if target == nil {
		return nil
	}
	if strings.HasPrefix(payload, converterPrefix) {
		encoded := strings.TrimPrefix(payload, converterPrefix)
		data, err := base64.StdEncoding.DecodeString(encoded)
		if err != nil {
			return fmt.Errorf("failed to decode sample converter payload: %w", err)
		}
		return json.Unmarshal(data, target)
	}
	return json.Unmarshal([]byte(payload), target)
}

func assertEncoded(name, payload string) error {
	if !strings.HasPrefix(payload, converterPrefix) {
		return fmt.Errorf("%s was not encoded with %q: %q", name, converterPrefix, payload)
	}
	return nil
}
