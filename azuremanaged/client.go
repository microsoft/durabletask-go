package azuremanaged

import (
	"context"
	"crypto/tls"
	"fmt"

	"github.com/microsoft/durabletask-go/api"
	"github.com/microsoft/durabletask-go/backend"
	"github.com/microsoft/durabletask-go/client"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

// Client schedules and manages orchestration instances on an Azure-managed Durable Task
// Scheduler (DTS) task hub. Use NewWorker to run orchestrator and activity code; a Client
// only issues management operations and cannot process work items.
type Client struct {
	inner *client.TaskHubGrpcClient
}

// NewClient creates a Client connected to an Azure-managed DTS task hub using the supplied
// Options.
func NewClient(opts *Options) (*Client, error) {
	conn, logger, err := newConnection(opts, "")
	if err != nil {
		return nil, err
	}
	return &Client{inner: client.NewTaskHubGrpcClient(conn, logger)}, nil
}

// NewClientFromConnectionString creates a Client from a DTS connection string of the form
// "Endpoint=<address>;Authentication=<type>;TaskHub=<name>".
func NewClientFromConnectionString(connectionString string) (*Client, error) {
	opts, err := optionsFromConnectionString(connectionString)
	if err != nil {
		return nil, err
	}
	return NewClient(opts)
}

// ScheduleNewOrchestration schedules a new orchestration instance for execution.
func (c *Client) ScheduleNewOrchestration(ctx context.Context, orchestrator string, opts ...api.NewOrchestrationOptions) (api.InstanceID, error) {
	return c.inner.ScheduleNewOrchestration(ctx, orchestrator, opts...)
}

// FetchOrchestrationMetadata fetches the current metadata for an orchestration instance.
func (c *Client) FetchOrchestrationMetadata(ctx context.Context, id api.InstanceID, opts ...api.FetchOrchestrationMetadataOptions) (*api.OrchestrationMetadata, error) {
	return c.inner.FetchOrchestrationMetadata(ctx, id, opts...)
}

// WaitForOrchestrationStart blocks until the orchestration instance leaves the pending state.
func (c *Client) WaitForOrchestrationStart(ctx context.Context, id api.InstanceID, opts ...api.FetchOrchestrationMetadataOptions) (*api.OrchestrationMetadata, error) {
	return c.inner.WaitForOrchestrationStart(ctx, id, opts...)
}

// WaitForOrchestrationCompletion blocks until the orchestration instance reaches a terminal state.
func (c *Client) WaitForOrchestrationCompletion(ctx context.Context, id api.InstanceID, opts ...api.FetchOrchestrationMetadataOptions) (*api.OrchestrationMetadata, error) {
	return c.inner.WaitForOrchestrationCompletion(ctx, id, opts...)
}

// TerminateOrchestration forcibly terminates an orchestration instance.
func (c *Client) TerminateOrchestration(ctx context.Context, id api.InstanceID, opts ...api.TerminateOptions) error {
	return c.inner.TerminateOrchestration(ctx, id, opts...)
}

// RaiseEvent raises an external event for a waiting orchestration instance.
func (c *Client) RaiseEvent(ctx context.Context, id api.InstanceID, eventName string, opts ...api.RaiseEventOptions) error {
	return c.inner.RaiseEvent(ctx, id, eventName, opts...)
}

// SuspendOrchestration suspends a running orchestration instance.
func (c *Client) SuspendOrchestration(ctx context.Context, id api.InstanceID, reason string) error {
	return c.inner.SuspendOrchestration(ctx, id, reason)
}

// ResumeOrchestration resumes a previously suspended orchestration instance.
func (c *Client) ResumeOrchestration(ctx context.Context, id api.InstanceID, reason string) error {
	return c.inner.ResumeOrchestration(ctx, id, reason)
}

// PurgeOrchestrationState purges the state of an orchestration instance.
func (c *Client) PurgeOrchestrationState(ctx context.Context, id api.InstanceID, opts ...api.PurgeOptions) error {
	return c.inner.PurgeOrchestrationState(ctx, id, opts...)
}

// newConnection validates options and dials a gRPC connection configured with the DTS auth
// interceptors. A non-empty workerID marks the connection as a worker.
func newConnection(opts *Options, workerID string) (*grpc.ClientConn, backend.Logger, error) {
	if err := opts.validate(); err != nil {
		return nil, nil, err
	}

	interceptor := &authInterceptor{
		taskHub:   opts.TaskHub,
		userAgent: userAgent(),
		workerID:  workerID,
	}
	if opts.Credential != nil {
		interceptor.tokenManager = newAccessTokenManager(opts.Credential, []string{opts.resourceID() + "/.default"})
	}

	var transportCreds credentials.TransportCredentials
	if opts.useInsecureChannel() {
		transportCreds = insecure.NewCredentials()
	} else {
		transportCreds = credentials.NewTLS(&tls.Config{MinVersion: tls.VersionTLS12})
	}

	conn, err := grpc.Dial(
		normalizeEndpoint(opts.Endpoint),
		grpc.WithTransportCredentials(transportCreds),
		grpc.WithUnaryInterceptor(interceptor.unaryClientInterceptor()),
		grpc.WithStreamInterceptor(interceptor.streamClientInterceptor()),
	)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to connect to durable task scheduler: %w", err)
	}
	return conn, opts.logger(), nil
}
