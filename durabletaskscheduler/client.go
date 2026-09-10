package durabletaskscheduler

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/microsoft/durabletask-go/api"
	durabletaskclient "github.com/microsoft/durabletask-go/client"
	"github.com/microsoft/durabletask-go/internal/protos"
	"google.golang.org/protobuf/types/known/emptypb"
)

// Client owns the management connection it creates.
type Client struct {
	*durabletaskclient.TaskHubGrpcClient

	connection *recreatingClientConn
	closeOnce  sync.Once
	closeErr   error
	converter  api.DataConverter
}

// NewClient creates an independently owned management connection and validates
// it with a deadline-bound Hello call.
// A nil logger uses [api.DefaultLogger].
func NewClient(ctx context.Context, options *Options, logger api.Logger) (*Client, error) {
	if logger == nil {
		logger = api.DefaultLogger()
	}
	prepared, err := prepareOptions(options)
	if err != nil {
		return nil, err
	}
	factory := func(ctx context.Context, _ *clientTransport) (*clientTransport, error) {
		connection, err := connect(&prepared, clientRole, "")
		if err != nil {
			return nil, err
		}
		helloCtx, cancel := context.WithTimeout(ctx, prepared.HelloTimeout)
		_, err = protos.NewTaskHubSidecarServiceClient(connection).Hello(helloCtx, &emptypb.Empty{})
		cancel()
		if err != nil {
			_ = connection.Close()
			return nil, fmt.Errorf("DTS client Hello failed: %w", err)
		}
		return &clientTransport{connection: connection, closer: connection}, nil
	}
	initial, err := factory(ctx, nil)
	if err != nil {
		return nil, err
	}
	connection := newRecreatingClientConn(
		initial,
		factory,
		prepared.ChannelRecreateFailureThreshold,
		prepared.ChannelRecreateMinInterval,
		logger,
	)
	clientOptions := []durabletaskclient.TaskHubGrpcClientOption{
		durabletaskclient.WithLargePayloads(prepared.LargePayloads),
		durabletaskclient.WithDataConverter(prepared.DataConverter),
	}
	if prepared.Versioning != nil && prepared.Versioning.DefaultVersion != "" {
		clientOptions = append(clientOptions, durabletaskclient.WithDefaultVersion(prepared.Versioning.DefaultVersion))
	}
	return &Client{
		TaskHubGrpcClient: durabletaskclient.NewTaskHubGrpcClient(connection, logger, clientOptions...),
		connection:        connection,
		converter:         api.NormalizeDataConverter(prepared.DataConverter),
	}, nil
}

func (c *Client) Close() error {
	if c == nil {
		return nil
	}
	c.closeOnce.Do(func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		stopErr := c.StopWorkItemListener(shutdownCtx)
		cancel()
		c.closeErr = errors.Join(stopErr, c.connection.Close())
	})
	return c.closeErr
}
