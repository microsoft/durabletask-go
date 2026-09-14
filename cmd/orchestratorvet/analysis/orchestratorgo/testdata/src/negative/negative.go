// Package negative collects orchestrators that must never produce a diagnostic.
// Every construct here is either deterministic, routed through a durable task
// API, or outside the reachability of a registered orchestrator.
package negative

import (
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/microsoft/durabletask-go/task"
)

// durableSequence uses activities, timers, events, and entities.
func durableSequence(ctx *task.OrchestrationContext) (any, error) {
	var input string
	if err := ctx.GetInput(&input); err != nil {
		return nil, err
	}
	var greeting string
	if err := ctx.CallActivity("sayHello", task.WithActivityInput(input)).Await(&greeting); err != nil {
		return nil, err
	}
	if err := ctx.CreateTimer(30 * time.Second).Await(nil); err != nil {
		return nil, err
	}
	if err := ctx.WaitForSingleEvent("approval", time.Hour).Await(nil); err != nil {
		return nil, err
	}
	unlock, err := ctx.LockEntities(task.EntityID{Name: "counter", Key: input})
	if err != nil {
		return nil, err
	}
	defer unlock()
	if err := ctx.CallEntity(task.EntityID{Name: "counter", Key: input}, "add").Await(nil); err != nil {
		return nil, err
	}
	return greeting, nil
}

// deterministicComputation only uses pure standard library helpers.
func deterministicComputation(ctx *task.OrchestrationContext) (any, error) {
	values := []int{3, 1, 2}
	sort.Ints(values)
	sort.Slice(values, func(i, j int) bool { return values[i] < values[j] })

	names := strings.Split("c,a,b", ",")
	sort.Strings(names)
	joined := strings.Join(names, "-")

	parsed, err := strconv.Atoi("42")
	if err != nil {
		return nil, fmt.Errorf("parse: %w", err)
	}
	encoded, err := json.Marshal(map[string]int{"value": parsed})
	if err != nil {
		return nil, err
	}
	if errors.Is(err, strconv.ErrSyntax) {
		return nil, err
	}
	return joined + string(encoded), nil
}

// durableFanOut uses the orchestration scheduler instead of goroutines.
func durableFanOut(ctx *task.OrchestrationContext) (any, error) {
	group := ctx.NewWaitGroup()
	pending := make([]task.Task, 0, 3)
	for i := 0; i < 3; i++ {
		pending = append(pending, ctx.CallActivity("sayHello", task.WithActivityInput(i)))
	}
	for _, item := range pending {
		group.Add(1)
		current := item
		ctx.Go(func(child *task.OrchestrationContext) {
			defer group.Done()
			_ = current.Await(nil)
		})
	}
	group.Wait(ctx)
	return len(pending), nil
}

// durableSelection waits on durable cases rather than a Go select.
func durableSelection(ctx *task.OrchestrationContext) (any, error) {
	work := ctx.CallActivity("sayHello")
	timeout := ctx.CreateTimer(time.Minute)
	winner := ""
	ctx.Select(
		task.OnTask(work, func(task.Task) { winner = "work" }),
		task.OnTask(timeout, func(task.Task) { winner = "timeout" }),
	)
	events := task.NewEventChannel[string](ctx, "signal")
	ctx.Select(task.OnEvent(events, func(string) { winner = "event" }))
	value, err := events.ReceiveErr(ctx)
	if err != nil {
		return nil, err
	}
	return winner + value, nil
}

// replaySafeLogging uses the orchestration logger.
func replaySafeLogging(ctx *task.OrchestrationContext) (any, error) {
	ctx.Logger().Info("progress", "instance", ctx.ID, "replaying", ctx.IsReplaying)
	ctx.SetCustomStatus(fmt.Sprintf("instance %s at %s", ctx.ID, ctx.CurrentTimeUtc))
	return nil, nil
}

// activityBody holds hazards that are never analyzed because activities are
// not replayed. Activities are delivered at least once and must be idempotent.
func activityBody(ctx task.ActivityContext) (any, error) {
	go func() {}()
	return time.Now(), nil
}

func register() {
	registry := task.NewTaskRegistry()
	_ = registry.AddOrchestrator(durableSequence)
	_ = registry.AddOrchestrator(deterministicComputation)
	_ = registry.AddOrchestrator(durableFanOut)
	_ = registry.AddOrchestrator(durableSelection)
	_ = registry.AddOrchestrator(replaySafeLogging)
	_ = registry.AddActivityN("sayHello", activityBody)
}
