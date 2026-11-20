package task

import (
	"errors"
	"slices"
)

// ErrOrchestrationDeadlock identifies channel waits that cannot make progress:
// no coroutine is runnable and no durable task or external event wait can wake
// the orchestration. The execution fails rather than remaining silently stuck.
var ErrOrchestrationDeadlock = errors.New("orchestration deadlock: channel operations are waiting with no pending durable work or external event receiver")

// Channel exchanges values between coroutines in one orchestration. It is
// reconstructed on replay, not a transport between orchestration instances.
// Values are passed by assignment, like Go channels, without serialization.
// Blocking operations yield to the durable scheduler; never use native Go
// channels for orchestration coordination.
type Channel[T any] struct {
	ctx       *OrchestrationContext
	capacity  int
	buffer    []channelValue[T]
	senders   []*channelOperation[T]
	receivers []*channelOperation[T]
}

type channelValue[T any] struct {
	value T
	order uint64
}

type channelOperation[T any] struct {
	selection *selection
	candidate *channelSelectCase[T]
	matches   []*channelMatch[T]
	order     uint64
	ready     bool
}

// Each potential rendezvous has one globally unique readiness order shared by
// both endpoints. This prevents cyclic preferences between competing selects.
type channelMatch[T any] struct {
	sender   *channelOperation[T]
	receiver *channelOperation[T]
	order    uint64
}

// NewChannel creates a fresh orchestration-local channel. Zero capacity
// requires a sender/receiver rendezvous; positive capacity provides a bounded
// FIFO buffer. Channels do not support close: use completion messages, known
// result counts, or cancellation to finish receivers.
func NewChannel[T any](ctx *OrchestrationContext, capacity int) *Channel[T] {
	if ctx == nil {
		panic("channel requires an orchestration context")
	}
	if capacity < 0 {
		panic("channel capacity must not be negative")
	}
	engine := ctx.effectContext()
	if engine.scheduler == nil {
		panic("channel created outside orchestrator execution")
	}
	return &Channel[T]{ctx: engine, capacity: capacity}
}

// Send waits until value can be sent. It panics on cancellation.
func (c *Channel[T]) Send(ctx *OrchestrationContext, value T) {
	if err := c.SendErr(ctx, value); err != nil {
		panic(err)
	}
}

// SendErr waits until value can be sent, returning ErrTaskCanceled if the
// waiting scope is canceled before the send commits.
func (c *Channel[T]) SendErr(ctx *OrchestrationContext, value T) error {
	_, err := ctx.selectCaseErr([]SelectCase{OnSend(c, value, nil)})
	return err
}

// Receive waits for the next value. It panics on cancellation.
func (c *Channel[T]) Receive(ctx *OrchestrationContext) T {
	value, err := c.ReceiveErr(ctx)
	if err != nil {
		panic(err)
	}
	return value
}

// ReceiveErr waits for the next value, returning ErrTaskCanceled if the
// waiting scope is canceled before the receive commits.
func (c *Channel[T]) ReceiveErr(ctx *OrchestrationContext) (T, error) {
	var value T
	_, err := ctx.selectCaseErr([]SelectCase{OnReceive(c, func(received T) {
		value = received
	})})
	return value, err
}

type channelSelectCase[T any] struct {
	channel    *Channel[T]
	send       bool
	value      T
	onSend     func()
	onReceive  func(T)
	operations map[*selection]*channelOperation[T]
}

// OnReceive creates a Select case that consumes one channel value only if it
// wins. The handler runs in the selecting coroutine.
func OnReceive[T any](channel *Channel[T], handler func(T)) SelectCase {
	if channel == nil {
		panic("receive Select case requires a channel")
	}
	return &channelSelectCase[T]{channel: channel, onReceive: handler}
}

// OnSend creates a Select case that sends value only if it wins. The handler
// runs in the selecting coroutine after the send commits.
func OnSend[T any](channel *Channel[T], value T, handler func()) SelectCase {
	if channel == nil {
		panic("send Select case requires a channel")
	}
	return &channelSelectCase[T]{channel: channel, send: true, value: value, onSend: handler}
}

func (c *channelSelectCase[T]) owner() *OrchestrationContext {
	return c.channel.ctx
}

func (c *channelSelectCase[T]) subscribe(s *selection) {
	if c.operations == nil {
		c.operations = make(map[*selection]*channelOperation[T])
	}
	op := &channelOperation[T]{selection: s, candidate: c}
	c.operations[s] = op
	channel := c.channel
	peers := channel.senders
	if c.send {
		peers = channel.receivers
		channel.senders = append(channel.senders, op)
	} else {
		channel.receivers = append(channel.receivers, op)
	}
	if channel.capacity == 0 {
		for _, peer := range peers {
			if peer.selection == s || !peer.selection.pending() {
				continue
			}
			match := &channelMatch[T]{order: channel.ctx.scheduler.nextCompletionID()}
			if c.send {
				match.sender, match.receiver = op, peer
			} else {
				match.sender, match.receiver = peer, op
			}
			op.matches = append(op.matches, match)
			peer.matches = append(peer.matches, match)
		}
	}
	channel.notify()
}

func (c *channelSelectCase[T]) unsubscribe(s *selection) {
	op := c.operations[s]
	if op == nil {
		return
	}
	delete(c.operations, s)
	channel := c.channel
	if c.send {
		channel.senders = slices.DeleteFunc(channel.senders, func(other *channelOperation[T]) bool {
			return other == op
		})
	} else {
		channel.receivers = slices.DeleteFunc(channel.receivers, func(other *channelOperation[T]) bool {
			return other == op
		})
	}
	for _, match := range op.matches {
		peer := match.sender
		if c.send {
			peer = match.receiver
		}
		peer.matches = slices.DeleteFunc(peer.matches, func(other *channelMatch[T]) bool {
			return other == match
		})
	}
	op.matches = nil
	channel.notify()
}

func (op *channelOperation[T]) firstMatch() *channelMatch[T] {
	for _, match := range op.matches {
		if match.sender.selection.pending() && match.receiver.selection.pending() {
			return match
		}
	}
	return nil
}

func (c *channelSelectCase[T]) ready(s *selection) (bool, uint64) {
	op := c.operations[s]
	if op == nil {
		return false, 0
	}
	if c.channel.capacity == 0 {
		if match := op.firstMatch(); match != nil {
			return true, match.order
		}
		return false, 0
	}
	if c.send {
		return op.ready, op.order
	}
	if len(c.channel.buffer) != 0 {
		return true, c.channel.buffer[0].order
	}
	return false, 0
}

func (c *channelSelectCase[T]) commit(s *selection) bool {
	channel := c.channel
	if channel.capacity == 0 {
		match := c.operations[s].firstMatch()
		if match == nil {
			return false
		}
		peer := match.sender
		if c.send {
			peer = match.receiver
		}
		// Do not steal a waiting peer from an earlier ready task, event, or
		// rendezvous. Let that coroutine resolve its preferred case first.
		if peer.selection.preferred() != peer.candidate {
			channel.ctx.scheduler.makeRunnable(peer.selection.coroutine)
			return false
		}
		// A case can have multiple peers; both must prefer this exact match.
		if peer.firstMatch() != match {
			channel.ctx.scheduler.makeRunnable(peer.selection.coroutine)
			return false
		}
		value := match.sender.candidate.value
		sendHandler := match.sender.candidate.onSend
		receiveHandler := match.receiver.candidate.onReceive
		match.sender.selection.finish(match.sender.candidate, sendHandler)
		match.receiver.selection.finish(match.receiver.candidate, func() {
			if receiveHandler != nil {
				receiveHandler(value)
			}
		})
		return true
	}
	if c.send {
		channel.buffer = append(channel.buffer, channelValue[T]{
			value: c.value,
			order: channel.ctx.scheduler.nextCompletionID(),
		})
		s.finish(c, c.onSend)
	} else {
		value := channel.buffer[0].value
		channel.buffer = slices.Delete(channel.buffer, 0, 1)
		s.finish(c, func() {
			if c.onReceive != nil {
				c.onReceive(value)
			}
		})
	}
	channel.notify()
	return true
}

func (c *Channel[T]) notify() {
	slots := c.capacity - len(c.buffer)
	for _, op := range c.senders {
		if !op.selection.pending() {
			continue
		}
		if c.capacity > 0 {
			ready := slots > 0
			if ready {
				slots--
			}
			if ready && !op.ready {
				op.order = c.ctx.scheduler.nextCompletionID()
			}
			op.ready = ready
		}
		c.ctx.scheduler.makeRunnable(op.selection.coroutine)
	}
	for _, op := range c.receivers {
		if op.selection.pending() {
			c.ctx.scheduler.makeRunnable(op.selection.coroutine)
		}
	}
}

func (s *coroutineScheduler) hasChannelDeadlock() bool {
	channelWait := false
	for _, c := range s.all {
		if c.selection == nil || !c.selection.pending() {
			continue
		}
		for _, candidate := range c.selection.cases {
			if _, ok := candidate.(interface{ channelCase() }); ok {
				channelWait = true
			}
		}
	}
	if !channelWait {
		return false
	}
	// Be conservative: even work not directly awaited by a coroutine may
	// complete a compound task and wake a branch.
	for _, task := range s.ctx.pendingTasks {
		if !task.isCompleted {
			return false
		}
	}
	for _, task := range s.ctx.pendingEntityTasks {
		if !task.isCompleted {
			return false
		}
	}
	for _, tasks := range s.ctx.pendingExternalEventTasks {
		for item := tasks.Front(); item != nil; item = item.Next() {
			if !item.Value.(*completableTask).isCompleted {
				return false
			}
		}
	}
	return len(s.ctx.eventWaiters) == 0
}

func (*channelSelectCase[T]) channelCase() {}
