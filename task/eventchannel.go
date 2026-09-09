package task

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/microsoft/durabletask-go/internal/protos"
)

type bufferedEvent struct {
	event *protos.HistoryEvent
	order uint64
}

// EventChannel receives a named external event repeatedly while preserving
// durable event order. Use ReceiveErr or TryReceiveErr when payload errors
// should be handled by the orchestrator.
type EventChannel[T any] struct {
	ctx  *OrchestrationContext
	name string
	key  string
}

// NewEventChannel returns the durable channel for a named external event.
// Repeated calls using the same event name and value type return the same channel.
func NewEventChannel[T any](ctx *OrchestrationContext, name string) *EventChannel[T] {
	if ctx == nil {
		panic("event channel requires an orchestration context")
	}
	engine := ctx.engineContext()
	key := strings.ToUpper(name)
	if existing, ok := engine.eventChannels[key]; ok {
		channel, ok := existing.(*EventChannel[T])
		if !ok {
			panic(fmt.Sprintf(
				"event channel %q was already created with a different value type",
				name,
			))
		}
		return channel
	}
	channel := &EventChannel[T]{ctx: engine, name: name, key: key}
	engine.eventChannels[key] = channel
	return channel
}

// Receive waits for and consumes the next event value.
func (c *EventChannel[T]) Receive(ctx *OrchestrationContext) T {
	value, err := c.ReceiveErr(ctx)
	if err != nil {
		panic(err)
	}
	return value
}

// ReceiveErr waits for and consumes the next event value, returning payload
// decoding and cancellation errors to the orchestrator.
func (c *EventChannel[T]) ReceiveErr(ctx *OrchestrationContext) (T, error) {
	if ctx.engineContext() != c.ctx {
		panic("event channel used with a different orchestration context")
	}
	var value T
	if ctx.scope.isCanceled() {
		return value, ErrTaskCanceled
	}
	if value, ok, err := c.TryReceiveErr(); ok || err != nil {
		return value, err
	}
	if err := ctx.WaitForSingleEvent(c.name, -1).Await(&value); err != nil {
		return value, err
	}
	return value, nil
}

// TryReceive consumes a buffered event without blocking.
func (c *EventChannel[T]) TryReceive() (T, bool) {
	value, ok, err := c.TryReceiveErr()
	if err != nil {
		panic(err)
	}
	return value, ok
}

// TryReceiveErr consumes a buffered event without blocking and returns payload
// decoding errors.
func (c *EventChannel[T]) TryReceiveErr() (T, bool, error) {
	var value T
	buffered, ok := c.ctx.takeBufferedEvent(c.key)
	if !ok {
		return value, false, nil
	}
	raw := []byte(buffered.event.GetEventRaised().GetInput().GetValue())
	if err := unmarshalData(c.ctx.converter, raw, &value); err != nil {
		return value, true, fmt.Errorf("failed to decode event %q as %s: %w", c.name, reflect.TypeFor[T](), err)
	}
	return value, true, nil
}

func (c *EventChannel[T]) peek() (*bufferedEvent, bool) {
	return c.ctx.peekBufferedEvent(c.key)
}

type eventSelectCase[T any] struct {
	channel *EventChannel[T]
	handler func(T)
}

// OnEvent creates a Select case that receives from a durable event channel.
func OnEvent[T any](channel *EventChannel[T], handler func(T)) SelectCase {
	if channel == nil {
		panic("event Select case requires a channel")
	}
	return &eventSelectCase[T]{channel: channel, handler: handler}
}

func (c *eventSelectCase[T]) owner() *OrchestrationContext {
	return c.channel.ctx
}

func (c *eventSelectCase[T]) ready() (bool, uint64) {
	event, ok := c.channel.peek()
	if !ok {
		return false, 0
	}
	return true, event.order
}

func (c *eventSelectCase[T]) subscribe(coroutine *coroutine) {
	c.channel.ctx.addEventWaiter(c.channel.key, coroutine)
}

func (c *eventSelectCase[T]) unsubscribe(coroutine *coroutine) {
	c.channel.ctx.removeEventWaiter(c.channel.key, coroutine)
}

func (c *eventSelectCase[T]) invoke() {
	value, ok := c.channel.TryReceive()
	if !ok {
		panic("selected event channel no longer has a buffered value")
	}
	if c.handler != nil {
		c.handler(value)
	}
}
