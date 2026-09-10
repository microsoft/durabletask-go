package durabletaskscheduler

import (
	"context"
	"errors"
	"io"
	"sync"
	"time"

	"github.com/microsoft/durabletask-go/api"
	"github.com/microsoft/durabletask-go/internal/protos"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const clientChannelRecreateTimeout = 30 * time.Second

type clientTransport struct {
	connection grpc.ClientConnInterface
	closer     io.Closer
	inFlight   int
	retired    bool
	closed     bool
}

type clientTransportFactory func(context.Context, *clientTransport) (*clientTransport, error)

type recreatingClientConn struct {
	mu                  sync.Mutex
	current             *clientTransport
	transports          map[*clientTransport]struct{}
	factory             clientTransportFactory
	failureThreshold    int
	minRecreateInterval time.Duration
	consecutiveFailures int
	lastRecreateAttempt time.Time
	recreateInFlight    bool
	recreateContext     context.Context
	cancelRecreate      context.CancelFunc
	recreateWait        sync.WaitGroup
	logger              api.Logger
	closed              bool
	closeOnce           sync.Once
	closeErr            error
}

func newRecreatingClientConn(
	initial *clientTransport,
	factory clientTransportFactory,
	failureThreshold int,
	minRecreateInterval time.Duration,
	logger api.Logger,
) *recreatingClientConn {
	recreateContext, cancelRecreate := context.WithCancel(context.Background())
	return &recreatingClientConn{
		current:             initial,
		transports:          map[*clientTransport]struct{}{initial: {}},
		factory:             factory,
		failureThreshold:    failureThreshold,
		minRecreateInterval: minRecreateInterval,
		recreateContext:     recreateContext,
		cancelRecreate:      cancelRecreate,
		logger:              logger,
	}
}

func (c *recreatingClientConn) Invoke(
	ctx context.Context,
	method string,
	args any,
	reply any,
	opts ...grpc.CallOption,
) error {
	transport, err := c.acquire()
	if err != nil {
		return err
	}
	err = transport.connection.Invoke(ctx, method, args, reply, opts...)
	c.release(transport)
	c.recordOutcome(ctx, method, transport, err)
	return err
}

func (c *recreatingClientConn) NewStream(
	ctx context.Context,
	desc *grpc.StreamDesc,
	method string,
	opts ...grpc.CallOption,
) (grpc.ClientStream, error) {
	transport, err := c.acquire()
	if err != nil {
		return nil, err
	}
	var finishOnce sync.Once
	finish := func(callErr error) {
		finishOnce.Do(func() {
			c.release(transport)
			c.recordOutcome(ctx, method, transport, callErr)
		})
	}
	opts = append(opts, grpc.OnFinish(finish))
	stream, err := transport.connection.NewStream(ctx, desc, method, opts...)
	if err != nil {
		finish(err)
	}
	return stream, err
}

func (c *recreatingClientConn) Close() error {
	if c == nil {
		return nil
	}
	c.closeOnce.Do(func() {
		c.mu.Lock()
		c.closed = true
		transports := c.takeAllTransportsLocked()
		c.mu.Unlock()

		c.cancelRecreate()
		c.closeErr = closeClientTransports(transports)
		c.recreateWait.Wait()
	})
	return c.closeErr
}

func (c *recreatingClientConn) acquire() (*clientTransport, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed || c.current == nil {
		return nil, status.Error(codes.Unavailable, "DTS management client is closed")
	}
	c.current.inFlight++
	return c.current, nil
}

func (c *recreatingClientConn) release(transport *clientTransport) {
	var closer io.Closer
	c.mu.Lock()
	transport.inFlight--
	if transport.inFlight < 0 {
		c.mu.Unlock()
		panic("DTS client transport lease released more than once")
	}
	if transport.retired && transport.inFlight == 0 && !transport.closed {
		transport.closed = true
		delete(c.transports, transport)
		closer = transport.closer
	}
	c.mu.Unlock()
	if closer != nil {
		if err := closer.Close(); err != nil {
			c.logger.Warnf("failed to close retired DTS management channel: %v", err)
		}
	}
}

func (c *recreatingClientConn) recordOutcome(
	ctx context.Context,
	method string,
	transport *clientTransport,
	err error,
) {
	c.mu.Lock()
	if c.closed || transport != c.current {
		c.mu.Unlock()
		return
	}
	if err == nil {
		c.consecutiveFailures = 0
		c.mu.Unlock()
		return
	}
	if isNeutralChannelOutcome(ctx, method, err) {
		c.mu.Unlock()
		return
	}
	if !countsTowardChannelRecreation(err) {
		c.consecutiveFailures = 0
		c.mu.Unlock()
		return
	}

	c.consecutiveFailures++
	failureCount := c.consecutiveFailures
	if c.failureThreshold <= 0 ||
		failureCount < c.failureThreshold ||
		c.recreateInFlight ||
		!c.recreateIntervalElapsedLocked() {
		c.mu.Unlock()
		return
	}
	c.recreateInFlight = true
	previous := c.current
	c.recreateWait.Add(1)
	c.mu.Unlock()

	c.logger.Warnf(
		"recreating DTS management channel after %d consecutive transport failures",
		failureCount,
	)
	go c.recreate(previous)
}

func (c *recreatingClientConn) recreate(previous *clientTransport) {
	defer c.recreateWait.Done()
	ctx, cancel := context.WithTimeout(c.recreateContext, clientChannelRecreateTimeout)
	replacement, err := c.factory(ctx, previous)
	cancel()

	var closer io.Closer
	c.mu.Lock()
	c.recreateInFlight = false
	c.lastRecreateAttempt = time.Now()
	switch {
	case err != nil:
		c.mu.Unlock()
		if c.recreateContext.Err() == nil {
			c.logger.Errorf("failed to recreate DTS management channel: %v", err)
		}
		return
	case replacement == nil || replacement.connection == nil:
		c.mu.Unlock()
		c.logger.Errorf("failed to recreate DTS management channel: factory returned no connection")
		return
	case c.closed:
		if replacement != previous && !replacement.closed {
			replacement.closed = true
			closer = replacement.closer
		}
		c.mu.Unlock()
		if closer != nil {
			_ = closer.Close()
		}
		return
	case replacement == previous:
		c.consecutiveFailures = 0
		c.mu.Unlock()
		return
	default:
		c.current = replacement
		c.transports[replacement] = struct{}{}
		c.consecutiveFailures = 0
		previous.retired = true
		if previous.inFlight == 0 && !previous.closed {
			previous.closed = true
			delete(c.transports, previous)
			closer = previous.closer
		}
		c.mu.Unlock()
		if closer != nil {
			if closeErr := closer.Close(); closeErr != nil {
				c.logger.Warnf("failed to close replaced DTS management channel: %v", closeErr)
			}
		}
	}
}

func (c *recreatingClientConn) recreateIntervalElapsedLocked() bool {
	return c.lastRecreateAttempt.IsZero() ||
		time.Since(c.lastRecreateAttempt) >= c.minRecreateInterval
}

func (c *recreatingClientConn) takeAllTransportsLocked() []*clientTransport {
	transports := make([]*clientTransport, 0, len(c.transports))
	for transport := range c.transports {
		if transport.closed {
			continue
		}
		transport.closed = true
		transport.retired = true
		transports = append(transports, transport)
	}
	clear(c.transports)
	c.current = nil
	return transports
}

func closeClientTransports(transports []*clientTransport) error {
	var errs []error
	for _, transport := range transports {
		if transport.closer != nil {
			errs = append(errs, transport.closer.Close())
		}
	}
	return errors.Join(errs...)
}

func isNeutralChannelOutcome(ctx context.Context, method string, err error) bool {
	if ctx.Err() != nil {
		return true
	}
	return status.Code(err) == codes.DeadlineExceeded &&
		(method == protos.TaskHubSidecarService_WaitForInstanceStart_FullMethodName ||
			method == protos.TaskHubSidecarService_WaitForInstanceCompletion_FullMethodName)
}

func countsTowardChannelRecreation(err error) bool {
	code := status.Code(err)
	return code == codes.Unavailable || code == codes.DeadlineExceeded
}
