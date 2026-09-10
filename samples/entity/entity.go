// Command entity demonstrates durable entity state, calls, signals, factories, and locks.
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/microsoft/durabletask-go/api"
	"github.com/microsoft/durabletask-go/durabletaskscheduler"
	"github.com/microsoft/durabletask-go/samples/internal/dtssample"
	"github.com/microsoft/durabletask-go/task"
)

const (
	counterEntityName          = "samplecounter"
	reflectedCounterEntityName = "samplereflectedcounter"
	factoryCounterEntityName   = "samplefactorycounter"
	bankAccountEntityName      = "samplebankaccount"
)

var factoryStats = &factoryRecorder{}

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
	fmt.Println("SAMPLE_OK entity")
}

func run() (err error) {
	registry := task.NewTaskRegistry()
	if err := registry.AddEntityN(counterEntityName, CounterEntity); err != nil {
		return err
	}
	if err := registry.AddEntityN(reflectedCounterEntityName, task.NewEntityFor[ReflectedCounter]()); err != nil {
		return err
	}
	if err := registry.AddEntityFactoryN(factoryCounterEntityName, task.NewEntityObjectFactory[FactoryCounterState, *FactoryCounter](
		func(ctx task.EntityFactoryContext) (*FactoryCounter, error) {
			factoryStats.Created(ctx.ID)
			return &FactoryCounter{recorder: factoryStats, id: ctx.ID}, nil
		},
		task.WithEntityStateInitializer(func(*task.EntityContext) (FactoryCounterState, error) {
			return FactoryCounterState{Value: 100}, nil
		}),
	)); err != nil {
		return err
	}
	if err := registry.AddEntityFactoryN(bankAccountEntityName, task.NewEntityObjectFactory[BankAccountState, *BankAccount](
		func(task.EntityFactoryContext) (*BankAccount, error) { return new(BankAccount), nil },
	)); err != nil {
		return err
	}
	if err := registry.AddOrchestratorN("SampleEntityCounterCalls", counterCallsOrchestrator); err != nil {
		return err
	}
	if err := registry.AddOrchestratorN("SampleEntityTransfer", transferOrchestrator); err != nil {
		return err
	}
	if err := registry.AddOrchestratorN("SampleEntityStartedWorkflow", entityStartedWorkflow); err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	app, err := dtssample.Start(ctx, registry)
	if err != nil {
		return err
	}

	runID := string(dtssample.NewInstanceID("entity"))
	counterID := api.NewEntityID(counterEntityName, runID+"-counter")
	forwardedCounterID := api.NewEntityID(counterEntityName, runID+"-forwarded")
	reflectedID := api.NewEntityID(reflectedCounterEntityName, runID+"-reflected")
	factoryID := api.NewEntityID(factoryCounterEntityName, runID+"-factory")
	checkingID := api.NewEntityID(bankAccountEntityName, runID+"-checking")
	savingsID := api.NewEntityID(bankAccountEntityName, runID+"-savings")
	entities := []api.EntityID{counterID, forwardedCounterID, reflectedID, factoryID, checkingID, savingsID}

	counterWorkflowID := dtssample.NewInstanceID("entity-counter-calls")
	transferID := dtssample.NewInstanceID("entity-transfer")
	startedID := dtssample.NewInstanceID("entity-started")
	ids := []api.InstanceID{counterWorkflowID, transferID, startedID}
	defer func() {
		err = errors.Join(err, deleteEntities(app.Client, entities...), dtssample.Cleanup(app.Client, ids...), app.Shutdown())
	}()

	if err := verifyRawCounterSignalsAndQueries(ctx, app.Client, counterID); err != nil {
		return err
	}
	if err := verifyEntityToEntitySignal(ctx, app.Client, counterID, forwardedCounterID); err != nil {
		return err
	}
	if err := verifyCounterCallWorkflow(ctx, app.Client, counterWorkflowID, counterID); err != nil {
		return err
	}
	if err := verifyReflectedCounter(ctx, app.Client, reflectedID); err != nil {
		return err
	}
	if err := verifyFactoryCounter(ctx, app.Client, factoryID); err != nil {
		return err
	}
	if err := verifyLockedTransfer(ctx, app.Client, transferID, checkingID, savingsID); err != nil {
		return err
	}
	if err := verifyEntityStartedWorkflow(ctx, app.Client, counterID, startedID); err != nil {
		return err
	}
	return nil
}

func verifyRawCounterSignalsAndQueries(ctx context.Context, client *durabletaskscheduler.Client, counterID api.EntityID) error {
	if err := client.SignalEntity(ctx, counterID, "add", api.WithSignalInput(10)); err != nil {
		return err
	}
	if err := client.SignalEntity(ctx, counterID, "add", api.WithSignalInput(5)); err != nil {
		return err
	}
	if _, err := waitForEntityState[int](ctx, client, counterID, 15); err != nil {
		return err
	}
	if err := client.SignalEntity(ctx, counterID, "add", api.WithSignalInput(4),
		api.WithSignalScheduledTime(time.Now().UTC().Add(300*time.Millisecond))); err != nil {
		return err
	}
	if _, err := waitForEntityState[int](ctx, client, counterID, 19); err != nil {
		return err
	}
	query, err := client.QueryEntities(ctx, api.EntityQuery{InstanceIDStartsWith: counterID.String(), PageSize: 10})
	if err != nil {
		return err
	}
	if len(query.Entities) != 1 || query.Entities[0].InstanceID.String() != counterID.String() {
		return fmt.Errorf("counter query returned %d entities, want exactly %s", len(query.Entities), counterID)
	}
	fmt.Println("verified raw counter state, signals, scheduled signals, and query")
	return nil
}

func verifyEntityToEntitySignal(ctx context.Context, client *durabletaskscheduler.Client, source, target api.EntityID) error {
	if err := client.SignalEntity(ctx, source, "forwardAdd", api.WithSignalInput(forwardAdd{Target: target, Amount: 7})); err != nil {
		return err
	}
	if _, err := waitForEntityState[int](ctx, client, target, 7); err != nil {
		return err
	}
	fmt.Println("verified entity-to-entity signal")
	return nil
}

func verifyCounterCallWorkflow(ctx context.Context, client *durabletaskscheduler.Client, id api.InstanceID, counterID api.EntityID) error {
	if _, err := client.ScheduleNewOrchestration(ctx, "SampleEntityCounterCalls",
		api.WithInstanceID(id), api.WithInput(counterWorkflowInput{Counter: counterID})); err != nil {
		return err
	}
	metadata, err := client.WaitForOrchestrationCompletion(ctx, id, api.WithFetchPayloads(true))
	if err != nil {
		return err
	}
	if err := dtssample.RequireCompleted(metadata); err != nil {
		return err
	}
	var output counterWorkflowOutput
	if err := metadata.ReadOutput(&output); err != nil {
		return err
	}
	if output.AfterCall != 22 || output.AfterSignal != 28 {
		return fmt.Errorf("counter workflow output = %+v, want call 22 and signal 28", output)
	}
	fmt.Println("verified orchestrator entity calls and signals")
	return nil
}

func verifyReflectedCounter(ctx context.Context, client *durabletaskscheduler.Client, id api.EntityID) error {
	if err := client.SignalEntity(ctx, id, "Add"); err != nil {
		return err
	}
	if _, err := waitForEntityState[ReflectedCounter](ctx, client, id, ReflectedCounter{Value: 1}); err != nil {
		return err
	}
	if err := client.SignalEntity(ctx, id, "Add", api.WithSignalInput(9)); err != nil {
		return err
	}
	if _, err := waitForEntityState[ReflectedCounter](ctx, client, id, ReflectedCounter{Value: 10}); err != nil {
		return err
	}
	fmt.Println("verified reflected entity model and optional input")
	return nil
}

func verifyFactoryCounter(ctx context.Context, client *durabletaskscheduler.Client, id api.EntityID) error {
	beforeCreated, beforeClosed := factoryStats.Counts()
	if err := client.SignalEntity(ctx, id, "Add"); err != nil {
		return err
	}
	if _, err := waitForEntityState[FactoryCounterState](ctx, client, id, FactoryCounterState{Value: 101}); err != nil {
		return err
	}
	created, closed := factoryStats.Counts()
	if created <= beforeCreated || closed <= beforeClosed {
		return fmt.Errorf("factory cleanup not observed; before=(%d,%d) after=(%d,%d)", beforeCreated, beforeClosed, created, closed)
	}
	fmt.Println("verified factory capture, initialized state, optional input, and batch cleanup")
	return nil
}

func verifyLockedTransfer(ctx context.Context, client *durabletaskscheduler.Client, id api.InstanceID, checking, savings api.EntityID) error {
	if err := client.SignalEntity(ctx, checking, "Deposit", api.WithSignalInput(1000)); err != nil {
		return err
	}
	if err := client.SignalEntity(ctx, savings, "Deposit", api.WithSignalInput(100)); err != nil {
		return err
	}
	if _, err := waitForEntityState[BankAccountState](ctx, client, checking, BankAccountState{Balance: 1000}); err != nil {
		return err
	}
	if _, err := waitForEntityState[BankAccountState](ctx, client, savings, BankAccountState{Balance: 100}); err != nil {
		return err
	}
	input := transferInput{From: checking, To: savings, Amount: 300}
	if _, err := client.ScheduleNewOrchestration(ctx, "SampleEntityTransfer",
		api.WithInstanceID(id), api.WithInput(input)); err != nil {
		return err
	}
	metadata, err := client.WaitForOrchestrationCompletion(ctx, id, api.WithFetchPayloads(true))
	if err != nil {
		return err
	}
	if err := dtssample.RequireCompleted(metadata); err != nil {
		return err
	}
	var output transferOutput
	if err := metadata.ReadOutput(&output); err != nil {
		return err
	}
	if output.From != 700 || output.To != 400 || output.Total != 1100 {
		return fmt.Errorf("transfer output = %+v, want balances 700/400 and invariant 1100", output)
	}
	fmt.Println("verified locked transfer balance invariant")
	return nil
}

func verifyEntityStartedWorkflow(ctx context.Context, client *durabletaskscheduler.Client, entityID api.EntityID, startedID api.InstanceID) error {
	if err := client.SignalEntity(ctx, entityID, "startWorkflow",
		api.WithSignalInput(startWorkflowInput{InstanceID: string(startedID), Message: "from-entity"})); err != nil {
		return err
	}
	metadata, err := waitForOrchestrationCreatedAndCompleted(ctx, client, startedID)
	if err != nil {
		return err
	}
	if err := dtssample.RequireCompleted(metadata); err != nil {
		return err
	}
	var output string
	if err := metadata.ReadOutput(&output); err != nil {
		return err
	}
	if output != "started:from-entity" {
		return fmt.Errorf("entity-started workflow output = %q", output)
	}
	fmt.Println("verified entity-started orchestration")
	return nil
}

func waitForOrchestrationCreatedAndCompleted(
	ctx context.Context,
	client *durabletaskscheduler.Client,
	id api.InstanceID,
) (*api.OrchestrationMetadata, error) {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		_, err := client.FetchOrchestrationMetadata(ctx, id, api.WithFetchPayloads(false))
		if err == nil {
			return client.WaitForOrchestrationCompletion(ctx, id, api.WithFetchPayloads(true))
		}
		if !errors.Is(err, api.ErrInstanceNotFound) {
			return nil, err
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-ticker.C:
		}
	}
}

func CounterEntity(ctx *task.EntityContext) (any, error) {
	var count int
	if ctx.HasState() {
		if err := ctx.GetState(&count); err != nil {
			return nil, err
		}
	}
	switch ctx.Operation {
	case "add":
		var amount int
		if err := ctx.GetInput(&amount); err != nil {
			return nil, err
		}
		count += amount
	case "get":
	case "reset":
		count = 0
	case "delete":
		ctx.DeleteState()
		return nil, nil
	case "forwardAdd":
		var input forwardAdd
		if err := ctx.GetInput(&input); err != nil {
			return nil, err
		}
		if err := ctx.SignalEntity(input.Target, "add", input.Amount); err != nil {
			return nil, err
		}
	case "startWorkflow":
		var input startWorkflowInput
		if err := ctx.GetInput(&input); err != nil {
			return nil, err
		}
		if err := ctx.StartNewOrchestration("SampleEntityStartedWorkflow",
			task.WithEntityStartOrchestrationInstanceID(input.InstanceID),
			task.WithEntityStartOrchestrationInput(input.Message)); err != nil {
			return nil, err
		}
	default:
		return nil, fmt.Errorf("unknown counter operation %q", ctx.Operation)
	}
	if err := ctx.SetState(count); err != nil {
		return nil, err
	}
	return count, nil
}

type forwardAdd struct {
	Target api.EntityID `json:"target"`
	Amount int          `json:"amount"`
}

type startWorkflowInput struct {
	InstanceID string `json:"instanceId"`
	Message    string `json:"message"`
}

type ReflectedCounter struct {
	Value int `json:"value"`
}

func (c *ReflectedCounter) Add(input task.OptionalEntityInput[int]) (int, error) {
	c.Value += input.Or(1)
	return c.Value, nil
}

type FactoryCounterState struct {
	Value int `json:"value"`
}

type FactoryCounter struct {
	task.EntityObjectBase[FactoryCounterState]
	recorder *factoryRecorder
	id       api.EntityID
}

func (c *FactoryCounter) Add(input task.OptionalEntityInput[int]) (int, error) {
	c.State().Value += input.Or(1)
	return c.State().Value, nil
}

func (c *FactoryCounter) CloseEntityBatch(context.Context) error {
	c.recorder.Closed(c.id)
	return nil
}

type factoryRecorder struct {
	mu      sync.Mutex
	created map[string]int
	closed  map[string]int
}

func (r *factoryRecorder) Created(id api.EntityID) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.created == nil {
		r.created = make(map[string]int)
	}
	r.created[id.String()]++
}

func (r *factoryRecorder) Closed(id api.EntityID) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed == nil {
		r.closed = make(map[string]int)
	}
	r.closed[id.String()]++
}

func (r *factoryRecorder) Counts() (created, closed int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, count := range r.created {
		created += count
	}
	for _, count := range r.closed {
		closed += count
	}
	return created, closed
}

type BankAccountState struct {
	Balance int `json:"balance"`
}

type BankAccount struct {
	task.EntityObjectBase[BankAccountState]
}

func (a *BankAccount) Deposit(amount int) (int, error) {
	a.State().Balance += amount
	return a.State().Balance, nil
}

func (a *BankAccount) Withdraw(amount int) (int, error) {
	if amount > a.State().Balance {
		return 0, fmt.Errorf("insufficient funds: balance=%d withdrawal=%d", a.State().Balance, amount)
	}
	a.State().Balance -= amount
	return a.State().Balance, nil
}

func counterCallsOrchestrator(ctx *task.OrchestrationContext) (any, error) {
	var input counterWorkflowInput
	if err := ctx.GetInput(&input); err != nil {
		return nil, err
	}
	var afterCall int
	if err := ctx.CallEntity(input.Counter, "add", task.WithEntityInput(3)).Await(&afterCall); err != nil {
		return nil, err
	}
	if err := ctx.SignalEntity(input.Counter, "add", task.WithSignalEntityInput(6)); err != nil {
		return nil, err
	}
	if err := ctx.CreateTimer(250 * time.Millisecond).Await(nil); err != nil {
		return nil, err
	}
	var afterSignal int
	if err := ctx.CallEntity(input.Counter, "get").Await(&afterSignal); err != nil {
		return nil, err
	}
	return counterWorkflowOutput{AfterCall: afterCall, AfterSignal: afterSignal}, nil
}

type counterWorkflowInput struct {
	Counter api.EntityID `json:"counter"`
}

type counterWorkflowOutput struct {
	AfterCall   int `json:"afterCall"`
	AfterSignal int `json:"afterSignal"`
}

func transferOrchestrator(ctx *task.OrchestrationContext) (any, error) {
	var input transferInput
	if err := ctx.GetInput(&input); err != nil {
		return nil, err
	}
	unlock, err := ctx.LockEntities(input.From, input.To)
	if err != nil {
		return nil, err
	}
	defer unlock()

	var fromBalance int
	if err := ctx.CallEntity(input.From, "Withdraw", task.WithEntityInput(input.Amount)).Await(&fromBalance); err != nil {
		return nil, err
	}
	var toBalance int
	if err := ctx.CallEntity(input.To, "Deposit", task.WithEntityInput(input.Amount)).Await(&toBalance); err != nil {
		return nil, err
	}
	return transferOutput{From: fromBalance, To: toBalance, Total: fromBalance + toBalance}, nil
}

type transferInput struct {
	From   api.EntityID `json:"from"`
	To     api.EntityID `json:"to"`
	Amount int          `json:"amount"`
}

type transferOutput struct {
	From  int `json:"from"`
	To    int `json:"to"`
	Total int `json:"total"`
}

func entityStartedWorkflow(ctx *task.OrchestrationContext) (any, error) {
	var message string
	if err := ctx.GetInput(&message); err != nil {
		return nil, err
	}
	return "started:" + message, nil
}

func waitForEntityState[T comparable](
	ctx context.Context,
	client *durabletaskscheduler.Client,
	entityID api.EntityID,
	expected T,
) (*api.EntityMetadata, error) {
	return waitForEntity(ctx, client, entityID, func(metadata *api.EntityMetadata) (bool, error) {
		if metadata == nil || !metadata.HasState {
			return false, nil
		}
		var actual T
		if err := metadata.ReadState(&actual); err != nil {
			return false, err
		}
		return actual == expected, nil
	})
}

func deleteEntities(client *durabletaskscheduler.Client, entityIDs ...api.EntityID) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	var failures []error
	for _, id := range entityIDs {
		if err := client.SignalEntity(ctx, id, "delete"); err != nil {
			failures = append(failures, fmt.Errorf("delete %s: %w", id, err))
			continue
		}
		if _, err := waitForEntity(ctx, client, id, func(metadata *api.EntityMetadata) (bool, error) {
			return metadata == nil || !metadata.HasState, nil
		}); err != nil {
			failures = append(failures, fmt.Errorf("wait for %s delete: %w", id, err))
		}
	}
	return errors.Join(failures...)
}

func waitForEntity(
	ctx context.Context,
	client *durabletaskscheduler.Client,
	entityID api.EntityID,
	ready func(*api.EntityMetadata) (bool, error),
) (*api.EntityMetadata, error) {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		metadata, err := client.GetEntity(ctx, entityID)
		if err != nil {
			return nil, err
		}
		ok, err := ready(metadata)
		if err != nil {
			return nil, err
		}
		if ok {
			return metadata, nil
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-ticker.C:
		}
	}
}
