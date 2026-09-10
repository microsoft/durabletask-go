package largepayload

import (
	"context"
	"sync"

	"github.com/microsoft/durabletask-go/api"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

// maxConcurrentPayloadOperations caps the simultaneous payload store requests
// issued for a single message, matching the .NET Azure Blob interceptor. It
// overlaps independent payload I/O without emitting an unbounded burst.
const maxConcurrentPayloadOperations = 8

// runBounded runs operations concurrently, capped at
// maxConcurrentPayloadOperations, while preserving everything a caller could
// observe from a sequential loop over the same operations:
//
//   - An empty operation list does no work and never reports cancellation.
//   - The failure of the earliest operation in message order is returned even
//     when a later operation fails first or cancellation follows.
//   - No operation is dispatched after a failure is recorded. An operation
//     already dispatched may begin afterwards, but every dispatched operation
//     finishes before runBounded returns, so no operation mutates its target
//     after the caller regains control.
//   - Cancellation is reported only when it actually stopped an operation from
//     starting, so already-completed work is never turned into an error.
func runBounded(ctx context.Context, operations []func(context.Context) error) error {
	return runBoundedWithHook(ctx, operations, nil)
}

// runBoundedWithHook is runBounded with an ordering point for tests.
// failureRecorded, which is nil in production, runs after an operation's
// failure has been recorded and before its concurrency slot is released.
func runBoundedWithHook(
	ctx context.Context,
	operations []func(context.Context) error,
	failureRecorded func(),
) error {
	if len(operations) == 0 {
		return nil
	}
	// A single operation cannot overlap with anything, so it runs inline. The
	// explicit ctx.Err check keeps the cancellation contract identical to the
	// concurrent path, which reports cancellation before dispatching.
	if len(operations) == 1 {
		if err := ctx.Err(); err != nil {
			return err
		}
		return operations[0](ctx)
	}

	limit := min(len(operations), maxConcurrentPayloadOperations)
	var (
		semaphore     = make(chan struct{}, limit)
		workers       sync.WaitGroup
		mu            sync.Mutex
		failure       error
		failedOrdinal = len(operations)
		cancelStopped bool
	)

	// canDispatch reports whether the operation may start. It is the dispatch
	// linearization point: a failure recorded afterwards never revokes a claim,
	// and no operation can claim dispatch once a failure has been recorded.
	canDispatch := func() bool {
		mu.Lock()
		defer mu.Unlock()
		if failure != nil {
			return false
		}
		if ctx.Err() != nil {
			cancelStopped = true
			return false
		}
		return true
	}

	for ordinal, operation := range operations {
		if !canDispatch() {
			break
		}
		// Acquiring a slot is deliberately not cancellable: every dispatched
		// operation observes ctx itself, so a slot always frees up, and an
		// unwind here would abandon operations that are already tracked.
		semaphore <- struct{}{}
		if !canDispatch() {
			<-semaphore
			break
		}
		workers.Add(1)
		go func() {
			defer workers.Done()
			defer func() { <-semaphore }()
			err := operation(ctx)
			if err == nil {
				return
			}
			mu.Lock()
			if ordinal < failedOrdinal {
				failedOrdinal, failure = ordinal, err
			}
			mu.Unlock()
			if failureRecorded != nil {
				failureRecorded()
			}
		}()
	}
	workers.Wait()

	mu.Lock()
	err, cancelled := failure, cancelStopped
	mu.Unlock()
	if err != nil {
		return err
	}
	if cancelled {
		return ctx.Err()
	}
	return nil
}

type payloadTransform func(
	context.Context,
	*api.LargePayloadOptions,
	*wrapperspb.StringValue,
) (*wrapperspb.StringValue, error)

// transformPlan collects the payload fields of one message so they can be
// transformed with bounded concurrency in a stable, message-defined order.
type transformPlan struct {
	options    *api.LargePayloadOptions
	transform  payloadTransform
	operations []func(context.Context) error
	// queued records the fields already planned. A message may reach the same
	// field twice, for example when one *protos.HistoryEvent appears in both
	// the past and new event lists of an orchestrator request. Transforming
	// such a field twice would externalize an already externalized token, so
	// each distinct field is planned exactly once.
	queued map[**wrapperspb.StringValue]struct{}
}

func newTransformPlan(options *api.LargePayloadOptions, transform payloadTransform) *transformPlan {
	return &transformPlan{options: options, transform: transform}
}

// add queues target for transformation. Fields without a payload contribute no
// operation, so a message with nothing to transform stays zero-work. The field
// value is snapshotted now rather than read when the operation runs, so a
// concurrently transformed sibling field can never be observed mid-update.
func (p *transformPlan) add(target **wrapperspb.StringValue) {
	if target == nil || *target == nil {
		return
	}
	// With no options, ordinary payloads are already final. Reserved references
	// must still run through the transform so missing configuration is rejected.
	if p.options == nil && !api.IsLargePayloadReference((*target).GetValue()) {
		return
	}
	if _, duplicate := p.queued[target]; duplicate {
		return
	}
	if p.queued == nil {
		p.queued = make(map[**wrapperspb.StringValue]struct{})
	}
	p.queued[target] = struct{}{}
	transform, options, value := p.transform, p.options, *target
	p.operations = append(p.operations, func(ctx context.Context) error {
		transformed, err := transform(ctx, options, value)
		if err != nil {
			return err
		}
		// Distinct fields are written by distinct operations, so concurrent
		// writes never target the same memory.
		*target = transformed
		return nil
	})
}

func (p *transformPlan) run(ctx context.Context) error {
	return runBounded(ctx, p.operations)
}
