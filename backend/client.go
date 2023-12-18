package backend

import (
	"context"
	"fmt"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/google/uuid"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/microsoft/durabletask-go/api"
	"github.com/microsoft/durabletask-go/internal/helpers"
	"github.com/microsoft/durabletask-go/internal/protos"
)

type TaskHubClient interface {
	ScheduleNewOrchestration(ctx context.Context, orchestrator interface{}, opts ...api.NewOrchestrationOptions) (api.InstanceID, error)
	FetchOrchestrationMetadata(ctx context.Context, id api.InstanceID) (*api.OrchestrationMetadata, error)
	WaitForOrchestrationStart(ctx context.Context, id api.InstanceID) (*api.OrchestrationMetadata, error)
	WaitForOrchestrationCompletion(ctx context.Context, id api.InstanceID) (*api.OrchestrationMetadata, error)
	TerminateOrchestration(ctx context.Context, id api.InstanceID, opts ...api.TerminateOptions) error
	RaiseEvent(ctx context.Context, id api.InstanceID, eventName string, opts ...api.RaiseEventOptions) error
	SuspendOrchestration(ctx context.Context, id api.InstanceID, reason string) error
	ResumeOrchestration(ctx context.Context, id api.InstanceID, reason string) error
	PurgeOrchestrationState(ctx context.Context, id api.InstanceID) error
}

type backendClient struct {
	be Backend
}

func NewTaskHubClient(be Backend) TaskHubClient {
	return &backendClient{
		be: be,
	}
}

func (c *backendClient) ScheduleNewOrchestration(ctx context.Context, orchestrator interface{}, opts ...api.NewOrchestrationOptions) (api.InstanceID, error) {
	name := helpers.GetTaskFunctionName(orchestrator)
	req := &protos.CreateInstanceRequest{Name: name}
	for _, configure := range opts {
		if err := configure(req); err != nil {
			return api.EmptyInstanceID, fmt.Errorf("failed to configure create instance request: %w", err)
		}
	}
	if req.InstanceId == "" {
		u, err := uuid.NewRandom()
		if err != nil {
			return api.EmptyInstanceID, fmt.Errorf("failed to generate instance ID: %w", err)
		}
		req.InstanceId = u.String()
	}

	var span trace.Span
	ctx, span = helpers.StartNewCreateOrchestrationSpan(ctx, req.Name, req.Version.GetValue(), req.InstanceId)
	defer span.End()

	tc := helpers.TraceContextFromSpan(span)
	e := helpers.NewExecutionStartedEvent(req.Name, req.InstanceId, req.Input, nil, tc)
	if err := c.be.CreateOrchestrationInstance(ctx, e, WithOrchestrationIdReusePolicy(req.OrchestrationIdReusePolicy)); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return api.EmptyInstanceID, fmt.Errorf("failed to start orchestration: %w", err)
	}
	return api.InstanceID(req.InstanceId), nil
}

// FetchOrchestrationMetadata fetches metadata for the specified orchestration from the configured task hub.
//
// ErrInstanceNotFound is returned when the specified orchestration doesn't exist.
func (c *backendClient) FetchOrchestrationMetadata(ctx context.Context, id api.InstanceID) (*api.OrchestrationMetadata, error) {
	metadata, err := c.be.GetOrchestrationMetadata(ctx, id)
	if err != nil {
		return nil, fmt.Errorf("Failed to fetch orchestration metadata: %w", err)
	}
	return metadata, nil
}

// WaitForOrchestrationStart waits for an orchestration to start running and returns an [OrchestrationMetadata] object that contains
// metadata about the started instance.
//
// ErrInstanceNotFound is returned when the specified orchestration doesn't exist.
func (c *backendClient) WaitForOrchestrationStart(ctx context.Context, id api.InstanceID) (*api.OrchestrationMetadata, error) {
	return c.waitForOrchestrationCondition(ctx, id, func(metadata *api.OrchestrationMetadata) bool {
		return metadata.RuntimeStatus != protos.OrchestrationStatus_ORCHESTRATION_STATUS_PENDING
	})
}

// WaitForOrchestrationCompletion waits for an orchestration to complete and returns an [OrchestrationMetadata] object that contains
// metadata about the completed instance.
//
// ErrInstanceNotFound is returned when the specified orchestration doesn't exist.
func (c *backendClient) WaitForOrchestrationCompletion(ctx context.Context, id api.InstanceID) (*api.OrchestrationMetadata, error) {
	return c.waitForOrchestrationCondition(ctx, id, func(metadata *api.OrchestrationMetadata) bool {
		return metadata.IsComplete()
	})
}

func (c *backendClient) waitForOrchestrationCondition(ctx context.Context, id api.InstanceID, condition func(metadata *api.OrchestrationMetadata) bool) (*api.OrchestrationMetadata, error) {
	b := backoff.ExponentialBackOff{
		InitialInterval:     100 * time.Millisecond,
		MaxInterval:         10 * time.Second,
		Multiplier:          1.5,
		RandomizationFactor: 0.05,
		Stop:                backoff.Stop,
		Clock:               backoff.SystemClock,
	}
	b.Reset()

	for {
		t := time.NewTimer(b.NextBackOff())
		select {
		case <-ctx.Done():
			if !t.Stop() {
				<-t.C
			}
			return nil, ctx.Err()
		case <-t.C:
			metadata, err := c.FetchOrchestrationMetadata(ctx, id)
			if err != nil {
				return nil, err
			}
			if metadata != nil && condition(metadata) {
				return metadata, nil
			}
		}
	}
}

// TerminateOrchestration enqueues a message to terminate a running orchestration, causing it to stop receiving new events and
// go directly into the TERMINATED state. This operation is asynchronous. An orchestration worker must
// dequeue the termination event before the orchestration will be terminated.
func (c *backendClient) TerminateOrchestration(ctx context.Context, id api.InstanceID, opts ...api.TerminateOptions) error {
	req := &protos.TerminateRequest{InstanceId: string(id), Recursive: true}
	for _, configure := range opts {
		if err := configure(req); err != nil {
			return fmt.Errorf("failed to configure termination request: %w", err)
		}
	}

	e := helpers.NewExecutionTerminatedEvent(req.Output, req.Recursive)
	if err := c.be.AddNewOrchestrationEvent(ctx, id, e); err != nil {
		return fmt.Errorf("failed to add terminate event: %w", err)
	}
	return nil
}

// RaiseEvent implements TaskHubClient and sends an asynchronous event notification to a waiting orchestration.
//
// In order to handle the event, the target orchestration instance must be waiting for an event named [eventName]
// using the [WaitForSingleEvent] method of the orchestration context parameter. If the target orchestration instance
// is not yet waiting for an event named [eventName], then the event will be bufferred in memory until a task
// subscribing to that event name is created.
//
// Raised events for a completed or non-existent orchestration instance will be silently discarded.
func (c *backendClient) RaiseEvent(ctx context.Context, id api.InstanceID, eventName string, opts ...api.RaiseEventOptions) error {
	req := &protos.RaiseEventRequest{InstanceId: string(id), Name: eventName}
	for _, configure := range opts {
		if err := configure(req); err != nil {
			return fmt.Errorf("failed to configure raise event request: %w", err)
		}
	}

	e := helpers.NewEventRaisedEvent(req.Name, req.Input)
	if err := c.be.AddNewOrchestrationEvent(ctx, id, e); err != nil {
		return fmt.Errorf("failed to raise event: %w", err)
	}
	return nil
}

// SuspendOrchestration suspends an orchestration instance, halting processing of its events until a "resume" operation resumes it.
//
// Note that suspended orchestrations are still considered to be "running" even though they will not process events.
func (c *backendClient) SuspendOrchestration(ctx context.Context, id api.InstanceID, reason string) error {
	e := helpers.NewSuspendOrchestrationEvent(reason)
	if err := c.be.AddNewOrchestrationEvent(ctx, id, e); err != nil {
		return fmt.Errorf("failed to suspend orchestration: %w", err)
	}
	return nil
}

// ResumeOrchestration resumes an orchestration instance that was previously suspended.
func (c *backendClient) ResumeOrchestration(ctx context.Context, id api.InstanceID, reason string) error {
	e := helpers.NewResumeOrchestrationEvent(reason)
	if err := c.be.AddNewOrchestrationEvent(ctx, id, e); err != nil {
		return fmt.Errorf("failed to resume orchestration: %w", err)
	}
	return nil
}

// PurgeOrchestrationState deletes the state of the specified orchestration instance.
//
// [api.ErrInstanceNotFound] is returned if the specified orchestration instance doesn't exist.
// [api.ErrNotCompleted] is returned if the specified orchestration instance is still running.
func (c *backendClient) PurgeOrchestrationState(ctx context.Context, id api.InstanceID) error {
	if err := c.be.PurgeOrchestrationState(ctx, id); err != nil {
		return fmt.Errorf("failed to purge orchestration state: %w", err)
	}
	return nil
}
