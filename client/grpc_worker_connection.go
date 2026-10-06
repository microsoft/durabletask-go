package client

import (
	"context"
	"errors"
	"fmt"
	"io"
	"reflect"
	"slices"
	"sync"
	"sync/atomic"

	"github.com/microsoft/durabletask-go/internal/protos"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/status"
)

const maxWorkerCompletionConnections = 8

func validateWorkerCompletionBudget(n int) error {
	if n < 1 || n > maxWorkerCompletionConnections {
		return fmt.Errorf("worker completion connections must be between 1 and %d", maxWorkerCompletionConnections)
	}
	return nil
}

func isNilWorkerTransport(value any) bool {
	if value == nil {
		return true
	}
	v := reflect.ValueOf(value)
	switch v.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return v.IsNil()
	default:
		return false
	}
}

func sameWorkerTransport(a, b any) bool {
	return a != nil && reflect.ValueOf(a).Comparable() && a == b
}

func validateWorkerCompletionTransports(intake grpc.ClientConnInterface, completions []grpc.ClientConnInterface) error {
	for i, connection := range completions {
		if isNilWorkerTransport(connection) {
			return fmt.Errorf("worker completion transport %d is nil", i)
		}
		if sameWorkerTransport(connection, intake) {
			return fmt.Errorf("worker completion transports must be separate from intake")
		}
		for _, previous := range completions[:i] {
			if sameWorkerTransport(connection, previous) {
				return fmt.Errorf("worker completion transports must be distinct")
			}
		}
	}
	return nil
}

// workerCompletionTransport belongs to one stream generation. Its caller must
// drain that generation's work before closing it.
type workerCompletionTransport struct {
	intake      grpc.ClientConnInterface
	completions []grpc.ClientConnInterface
	closers     []io.Closer
	next        atomic.Uint64
	closeOnce   sync.Once
	closeErr    error
}

func newWorkerCompletionTransport(
	ctx context.Context,
	factory TaskHubGrpcWorkerConnectionFactory,
	additional int,
) (*workerCompletionTransport, error) {
	if err := validateWorkerCompletionBudget(additional); err != nil {
		return nil, err
	}
	group := &workerCompletionTransport{}
	for i := 0; i <= additional; i++ {
		cc, closer, err := factory(ctx)
		duplicateCloser := false
		if !isNilWorkerTransport(closer) {
			for _, previous := range group.closers {
				duplicateCloser = duplicateCloser || sameWorkerTransport(closer, previous)
			}
			if !duplicateCloser {
				group.closers = append(group.closers, closer)
			}
		}
		switch {
		case err != nil:
			err = fmt.Errorf("worker connection %d: %w", i, err)
		case isNilWorkerTransport(cc):
			err = fmt.Errorf("gRPC worker connection factory returned a nil connection")
		case isNilWorkerTransport(closer):
			err = fmt.Errorf("completion connections require an owning factory with a closer for every connection")
		case ctx.Err() != nil:
			err = ctx.Err()
		}
		if err == nil {
			err = validateWorkerCompletionTransports(group.intake, append(slices.Clone(group.completions), cc))
		}
		if err == nil && duplicateCloser {
			err = fmt.Errorf("owning worker factories must return distinct ownership closers")
		}
		if err == nil {
			if connection, ok := cc.(*grpc.ClientConn); ok {
				err = waitWorkerConnectionReady(ctx, connection)
			}
		}
		if err != nil {
			return nil, errors.Join(err, group.Close())
		}
		if i == 0 {
			group.intake = cc
		} else {
			group.completions = append(group.completions, cc)
		}
	}
	return group, nil
}

func waitWorkerConnectionReady(ctx context.Context, connection *grpc.ClientConn) error {
	connection.Connect()
	for {
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("worker connection did not become ready: %w", status.FromContextError(err).Err())
		}
		state := connection.GetState()
		if state == connectivity.Ready {
			return nil
		}
		connection.WaitForStateChange(ctx, state)
	}
}

func (c *workerCompletionTransport) Invoke(
	ctx context.Context,
	method string,
	args, reply any,
	opts ...grpc.CallOption,
) error {
	connection := c.intake
	if isWorkerCompletionMethod(method) {
		index := (c.next.Add(1) - 1) % uint64(len(c.completions))
		connection = c.completions[index]
	}
	return connection.Invoke(ctx, method, args, reply, opts...)
}

func isWorkerCompletionMethod(method string) bool {
	switch method {
	case protos.TaskHubSidecarService_CompleteActivityTask_FullMethodName,
		protos.TaskHubSidecarService_CompleteOrchestratorTask_FullMethodName,
		protos.TaskHubSidecarService_CompleteEntityTask_FullMethodName,
		protos.TaskHubSidecarService_AbandonTaskActivityWorkItem_FullMethodName,
		protos.TaskHubSidecarService_AbandonTaskOrchestratorWorkItem_FullMethodName,
		protos.TaskHubSidecarService_AbandonTaskEntityWorkItem_FullMethodName:
		return true
	default:
		return false
	}
}

func (c *workerCompletionTransport) NewStream(
	ctx context.Context,
	desc *grpc.StreamDesc,
	method string,
	opts ...grpc.CallOption,
) (grpc.ClientStream, error) {
	return c.intake.NewStream(ctx, desc, method, opts...)
}

func (c *workerCompletionTransport) Close() error {
	c.closeOnce.Do(func() {
		for i := len(c.closers) - 1; i >= 0; i-- {
			if err := c.closers[i].Close(); err != nil {
				c.closeErr = errors.Join(c.closeErr, fmt.Errorf("close worker connection %d: %w", i, err))
			}
		}
	})
	return c.closeErr
}
