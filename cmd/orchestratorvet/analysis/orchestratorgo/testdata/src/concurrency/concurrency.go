// Package concurrency exercises the raw goroutine, synchronization, and channel
// checks, plus the durable primitives that are allowed to replace them.
package concurrency

import (
	"sync"
	"sync/atomic"

	"github.com/microsoft/durabletask-go/task"
)

func rawGoroutines(ctx *task.OrchestrationContext) (any, error) {
	go func() {}() // want `raw go statement is not deterministic in an orchestrator`
	go helper()    // want `raw go statement is not deterministic in an orchestrator`
	return nil, nil
}

func helper() {}

func syncPrimitives(ctx *task.OrchestrationContext) (any, error) {
	var mutex sync.Mutex
	mutex.Lock()   // want `\(sync\.Mutex\)\.Lock is not replay safe in an orchestrator`
	mutex.Unlock() // want `\(sync\.Mutex\)\.Unlock is not replay safe in an orchestrator`

	var group sync.WaitGroup
	group.Add(1) // want `\(sync\.WaitGroup\)\.Add is not replay safe in an orchestrator`
	group.Done() // want `\(sync\.WaitGroup\)\.Done is not replay safe in an orchestrator`
	group.Wait() // want `\(sync\.WaitGroup\)\.Wait is not replay safe in an orchestrator`

	var once sync.Once
	once.Do(func() {}) // want `\(sync\.Once\)\.Do is not replay safe in an orchestrator`

	memo := sync.OnceValue(func() int { return 1 }) // want `sync\.OnceValue is not replay safe in an orchestrator`
	_ = memo()

	var counter atomic.Int64
	counter.Add(1) // want `\(atomic\.Int64\)\.Add is not replay safe in an orchestrator`
	var plain int64
	atomic.AddInt64(&plain, 1) // want `sync/atomic\.AddInt64 is not replay safe in an orchestrator`
	return nil, nil
}

// embeddedMutex proves the check keys on the declaring type, not the syntax.
type embeddedMutex struct {
	sync.Mutex
	value int
}

func embeddedSync(ctx *task.OrchestrationContext) (any, error) {
	state := &embeddedMutex{}
	state.Lock() // want `\(sync\.Mutex\)\.Lock is not replay safe in an orchestrator`
	state.value++
	state.Unlock() // want `\(sync\.Mutex\)\.Unlock is not replay safe in an orchestrator`
	return state.value, nil
}

func channelOperations(ctx *task.OrchestrationContext) (any, error) {
	results := make(chan int, 1) // want `creating a channel is not deterministic in an orchestrator`
	results <- 1                 // want `channel send is not deterministic in an orchestrator`
	value := <-results           // want `channel receive is not deterministic in an orchestrator`
	close(results)               // want `closing a channel is not deterministic in an orchestrator`

	stream := make(chan int)   // want `creating a channel is not deterministic in an orchestrator`
	for item := range stream { // want `ranging over a channel is not deterministic in an orchestrator`
		value += item
	}
	select { // want `select statement is not deterministic in an orchestrator`
	case received := <-stream: // want `channel receive is not deterministic in an orchestrator`
		value += received
	default:
	}
	return value, nil
}

// durableConcurrency uses only the orchestration scheduler primitives.
func durableConcurrency(ctx *task.OrchestrationContext) (any, error) {
	group := ctx.NewWaitGroup()
	group.Add(1)
	ctx.Go(func(child *task.OrchestrationContext) {
		defer group.Done()
		_ = child.CallActivity("work")
	})
	group.Wait(ctx)

	events := task.NewEventChannel[int](ctx, "tick")
	value, err := events.ReceiveErr(ctx)
	if err != nil {
		return nil, err
	}
	pending := ctx.CallActivity("work")
	ctx.Select(
		task.OnTask(pending, func(task.Task) {}),
		task.OnEvent(events, func(int) {}),
	)
	return value, nil
}

// goInsideCoroutine proves a raw go statement is still reported when it is
// written directly inside an orchestration coroutine.
func goInsideCoroutine(ctx *task.OrchestrationContext) (any, error) {
	ctx.Go(func(*task.OrchestrationContext) {
		go func() {}() // want `raw go statement is not deterministic in an orchestrator`
	})
	return nil, nil
}

func register() {
	registry := task.NewTaskRegistry()
	_ = registry.AddOrchestrator(rawGoroutines)
	_ = registry.AddOrchestrator(syncPrimitives)
	_ = registry.AddOrchestrator(embeddedSync)
	_ = registry.AddOrchestrator(channelOperations)
	_ = registry.AddOrchestrator(durableConcurrency)
	_ = registry.AddOrchestrator(goInsideCoroutine)

	// An orchestrator registered as an inline literal under an explicit name is
	// a root just like a named declaration, so its body is analyzed.
	_ = registry.AddOrchestratorN("inline", func(*task.OrchestrationContext) (any, error) {
		go func() {}() // want `raw go statement is not deterministic in an orchestrator`
		return nil, nil
	})
}
