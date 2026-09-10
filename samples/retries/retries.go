// Command retries demonstrates deterministic activity retry policies against DTS.
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/microsoft/durabletask-go/api"
	"github.com/microsoft/durabletask-go/samples/internal/dtssample"
	"github.com/microsoft/durabletask-go/task"
)

const (
	transientFailureType   api.ErrorType = "SampleTransientFailure"
	handlerStopFailureType api.ErrorType = "SampleHandlerStoppedFailure"
	nonRetriableType       api.ErrorType = "SampleNonRetriableFailure"
)

var attempts = &attemptStore{counts: make(map[string]int)}

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
	fmt.Println("SAMPLE_OK retries")
}

func run() (err error) {
	registry := task.NewTaskRegistry()
	if err := registry.AddOrchestratorN("SampleRetryRecovery", retryRecoveryOrchestrator); err != nil {
		return err
	}
	if err := registry.AddOrchestratorN("SampleRetryHandlerStops", retryHandlerStopsOrchestrator); err != nil {
		return err
	}
	if err := registry.AddOrchestratorN("SampleRetryNonRetriable", retryNonRetriableOrchestrator); err != nil {
		return err
	}
	if err := registry.AddActivityN("SampleControlledFailure", controlledFailureActivity); err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	app, err := dtssample.Start(ctx, registry)
	if err != nil {
		return err
	}
	recoveryID := dtssample.NewInstanceID("retries-recovery")
	handlerID := dtssample.NewInstanceID("retries-handler")
	nonRetriableID := dtssample.NewInstanceID("retries-non-retriable")
	ids := []api.InstanceID{recoveryID, handlerID, nonRetriableID}
	defer func() { err = errors.Join(err, dtssample.Cleanup(app.Client, ids...), app.Shutdown()) }()

	if err := verifyRecovery(ctx, app.Client, recoveryID); err != nil {
		return err
	}
	if err := verifyHandlerStop(ctx, app.Client, handlerID); err != nil {
		return err
	}
	if err := verifyNonRetriable(ctx, app.Client, nonRetriableID); err != nil {
		return err
	}
	return nil
}

func verifyRecovery(ctx context.Context, client interface {
	ScheduleNewOrchestration(context.Context, string, ...api.NewOrchestrationOptions) (api.InstanceID, error)
	WaitForOrchestrationCompletion(context.Context, api.InstanceID, ...api.FetchOrchestrationMetadataOptions) (*api.OrchestrationMetadata, error)
}, id api.InstanceID) error {
	input := failurePlan{Key: string(id), FailuresBeforeSuccess: 2}
	if _, err := client.ScheduleNewOrchestration(ctx, "SampleRetryRecovery",
		api.WithInstanceID(id), api.WithInput(input)); err != nil {
		return err
	}
	metadata, err := client.WaitForOrchestrationCompletion(ctx, id, api.WithFetchPayloads(true))
	if err != nil {
		return err
	}
	if err := dtssample.RequireCompleted(metadata); err != nil {
		return err
	}
	var output retryResult
	if err := metadata.ReadOutput(&output); err != nil {
		return err
	}
	if output.Key != input.Key || output.Attempts != 3 || output.Value != "recovered" {
		return fmt.Errorf("retry recovery result = %+v, want key %q attempts 3 recovered", output, input.Key)
	}
	fmt.Printf("verified deterministic recovery after %d attempts\n", output.Attempts)
	return nil
}

func verifyHandlerStop(ctx context.Context, client interface {
	ScheduleNewOrchestration(context.Context, string, ...api.NewOrchestrationOptions) (api.InstanceID, error)
	WaitForOrchestrationCompletion(context.Context, api.InstanceID, ...api.FetchOrchestrationMetadataOptions) (*api.OrchestrationMetadata, error)
}, id api.InstanceID) error {
	input := failurePlan{Key: string(id), Mode: "handler-stop"}
	if _, err := client.ScheduleNewOrchestration(ctx, "SampleRetryHandlerStops",
		api.WithInstanceID(id), api.WithInput(input)); err != nil {
		return err
	}
	metadata, err := client.WaitForOrchestrationCompletion(ctx, id, api.WithFetchPayloads(true))
	if err != nil {
		return err
	}
	if metadata.RuntimeStatus != api.RUNTIME_STATUS_FAILED {
		return fmt.Errorf("handler-stop status = %s, want FAILED", metadata.RuntimeStatus)
	}
	if !failureChainContains(metadata.FailureDetails, handlerStopFailureType) {
		return fmt.Errorf("handler-stop failure chain missing %s: %v", handlerStopFailureType, metadata.FailureDetails)
	}
	if got := attempts.Count(input.Key); got != 1 {
		return fmt.Errorf("handler-stop attempts = %d, want 1", got)
	}
	fmt.Println("verified retry handler stops on a typed durable failure")
	return nil
}

func verifyNonRetriable(ctx context.Context, client interface {
	ScheduleNewOrchestration(context.Context, string, ...api.NewOrchestrationOptions) (api.InstanceID, error)
	WaitForOrchestrationCompletion(context.Context, api.InstanceID, ...api.FetchOrchestrationMetadataOptions) (*api.OrchestrationMetadata, error)
}, id api.InstanceID) error {
	input := failurePlan{Key: string(id), Mode: "non-retriable"}
	if _, err := client.ScheduleNewOrchestration(ctx, "SampleRetryNonRetriable",
		api.WithInstanceID(id), api.WithInput(input)); err != nil {
		return err
	}
	metadata, err := client.WaitForOrchestrationCompletion(ctx, id, api.WithFetchPayloads(true))
	if err != nil {
		return err
	}
	if metadata.RuntimeStatus != api.RUNTIME_STATUS_FAILED {
		return fmt.Errorf("non-retriable status = %s, want FAILED", metadata.RuntimeStatus)
	}
	details := findFailure(metadata.FailureDetails, nonRetriableType)
	if details == nil {
		return fmt.Errorf("non-retriable failure chain missing %s: %v", nonRetriableType, metadata.FailureDetails)
	}
	if !details.IsNonRetriable {
		return fmt.Errorf("%s was not marked non-retriable", nonRetriableType)
	}
	if details.Properties["category"] != "validation" || details.Properties["sample"] != "retries" {
		return fmt.Errorf("unexpected custom failure properties: %+v", details.Properties)
	}
	if got := attempts.Count(input.Key); got != 1 {
		return fmt.Errorf("non-retriable attempts = %d, want 1", got)
	}
	fmt.Println("verified non-retriable typed failure and custom properties")
	return nil
}

func retryRecoveryOrchestrator(ctx *task.OrchestrationContext) (any, error) {
	var input failurePlan
	if err := ctx.GetInput(&input); err != nil {
		return nil, err
	}
	var output retryResult
	if err := ctx.CallActivity("SampleControlledFailure",
		task.WithActivityInput(input),
		task.WithActivityRetryPolicy(sampleRetryPolicy()),
	).Await(&output); err != nil {
		return nil, err
	}
	return output, nil
}

func retryHandlerStopsOrchestrator(ctx *task.OrchestrationContext) (any, error) {
	var input failurePlan
	if err := ctx.GetInput(&input); err != nil {
		return nil, err
	}
	return nil, ctx.CallActivity("SampleControlledFailure",
		task.WithActivityInput(input),
		task.WithActivityRetryPolicy(sampleRetryPolicy()),
	).Await(nil)
}

func retryNonRetriableOrchestrator(ctx *task.OrchestrationContext) (any, error) {
	var input failurePlan
	if err := ctx.GetInput(&input); err != nil {
		return nil, err
	}
	return nil, ctx.CallActivity("SampleControlledFailure",
		task.WithActivityInput(input),
		task.WithActivityRetryPolicy(sampleRetryPolicy()),
	).Await(nil)
}

func sampleRetryPolicy() *task.RetryPolicy {
	return &task.RetryPolicy{
		MaxAttempts:          5,
		InitialRetryInterval: 100 * time.Millisecond,
		BackoffCoefficient:   1,
		MaxRetryInterval:     100 * time.Millisecond,
		RetryTimeout:         10 * time.Second,
		Handle: func(ctx task.RetryContext) bool {
			return ctx.LastFailure.IsCausedBy(transientFailureType)
		},
	}
}

type failurePlan struct {
	Key                   string `json:"key"`
	Mode                  string `json:"mode,omitempty"`
	FailuresBeforeSuccess int    `json:"failuresBeforeSuccess,omitempty"`
}

type retryResult struct {
	Key      string `json:"key"`
	Attempts int    `json:"attempts"`
	Value    string `json:"value"`
}

func controlledFailureActivity(ctx task.ActivityContext) (any, error) {
	var input failurePlan
	if err := ctx.GetInput(&input); err != nil {
		return nil, err
	}
	attempt := attempts.Increment(input.Key)
	switch input.Mode {
	case "handler-stop":
		return nil, handlerStoppedError{key: input.Key}
	case "non-retriable":
		return nil, nonRetriableSampleError{key: input.Key}
	default:
		if attempt <= input.FailuresBeforeSuccess {
			return nil, fmt.Errorf("controlled attempt %d: %w", attempt, transientSampleError{key: input.Key, attempt: attempt})
		}
		return retryResult{Key: input.Key, Attempts: attempt, Value: "recovered"}, nil
	}
}

type attemptStore struct {
	mu     sync.Mutex
	counts map[string]int
}

func (s *attemptStore) Increment(key string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.counts[key]++
	return s.counts[key]
}

func (s *attemptStore) Count(key string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.counts[key]
}

type transientSampleError struct {
	key     string
	attempt int
}

func (e transientSampleError) Error() string {
	return fmt.Sprintf("transient failure for %s on attempt %d", e.key, e.attempt)
}

func (e transientSampleError) DurableTaskErrorType() api.ErrorType {
	return transientFailureType
}

func (e transientSampleError) DurableTaskErrorProperties() map[string]any {
	return map[string]any{"key": e.key, "attempt": e.attempt}
}

type handlerStoppedError struct {
	key string
}

func (e handlerStoppedError) Error() string {
	return "handler-selected permanent failure for " + e.key
}

func (e handlerStoppedError) DurableTaskErrorType() api.ErrorType {
	return handlerStopFailureType
}

func (e handlerStoppedError) DurableTaskErrorProperties() map[string]any {
	return map[string]any{"category": "handler-stop", "key": e.key}
}

type nonRetriableSampleError struct {
	key string
}

func (e nonRetriableSampleError) Error() string {
	return "non-retriable validation failure for " + e.key
}

func (e nonRetriableSampleError) DurableTaskErrorType() api.ErrorType {
	return nonRetriableType
}

func (e nonRetriableSampleError) DurableTaskErrorProperties() map[string]any {
	return map[string]any{"category": "validation", "sample": "retries", "key": e.key}
}

func (e nonRetriableSampleError) NonRetriable() bool {
	return true
}

func failureChainContains(details *api.FailureDetails, errorType api.ErrorType) bool {
	return findFailure(details, errorType) != nil
}

func findFailure(details *api.FailureDetails, errorType api.ErrorType) *api.FailureDetails {
	for current := details; current != nil; current = current.InnerFailure {
		if current.ErrorType == errorType {
			return current
		}
	}
	return nil
}
