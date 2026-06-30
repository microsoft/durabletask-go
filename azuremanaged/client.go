package azuremanaged

import (
	"crypto/tls"
	"fmt"

	"github.com/microsoft/durabletask-go/backend"
	"github.com/microsoft/durabletask-go/client"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

// NewClient creates a client.TaskHubGrpcClient connected to an Azure-managed DTS task hub
// using the supplied Options.
func NewClient(opts *Options) (*client.TaskHubGrpcClient, error) {
	conn, logger, err := newConnection(opts, "")
	if err != nil {
		return nil, err
	}
	return client.NewTaskHubGrpcClient(conn, logger), nil
}

// NewClientFromConnectionString creates a client from a DTS connection string of the form
// "Endpoint=<address>;Authentication=<type>;TaskHub=<name>".
func NewClientFromConnectionString(connectionString string) (*client.TaskHubGrpcClient, error) {
	opts, err := optionsFromConnectionString(connectionString)
	if err != nil {
		return nil, err
	}
	return NewClient(opts)
}

// NewWorker creates a client.TaskHubGrpcClient configured to run as a DTS worker. The
// returned value carries a unique worker id and is ready for StartWorkItemListener.
func NewWorker(opts *Options) (*client.TaskHubGrpcClient, error) {
	conn, logger, err := newConnection(opts, generateWorkerID())
	if err != nil {
		return nil, err
	}
	return client.NewTaskHubGrpcClient(conn, logger), nil
}

// NewWorkerFromConnectionString creates a DTS worker from a connection string.
func NewWorkerFromConnectionString(connectionString string) (*client.TaskHubGrpcClient, error) {
	opts, err := optionsFromConnectionString(connectionString)
	if err != nil {
		return nil, err
	}
	return NewWorker(opts)
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
