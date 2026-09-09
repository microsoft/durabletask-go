// This sample demonstrates how to use durable entities with the Durable Task Go SDK.
// It shows two patterns:
//
//  1. A raw entity function (Counter) with manual operation dispatch
//
//  2. An auto-dispatch entity (BankAccount) where operations map to methods on a struct
//
//     export DTS_CONNECTION_STRING="Endpoint=http://localhost:8080;TaskHub=default;Authentication=None"
//     go run ./samples/entity
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/google/uuid"

	"github.com/microsoft/durabletask-go/api"
	"github.com/microsoft/durabletask-go/durabletaskscheduler"
	"github.com/microsoft/durabletask-go/samples/internal/dtssample"
	"github.com/microsoft/durabletask-go/task"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	r := task.NewTaskRegistry()

	// Pattern 1: Register a raw entity function with manual dispatch
	if err := r.AddEntityN("counter", CounterEntity); err != nil {
		return fmt.Errorf("failed to register counter entity: %w", err)
	}

	// Pattern 2: Register a persistent entity object with separate durable state.
	bankAccountFactory := task.NewEntityObjectFactory[BankAccountState, *BankAccount](
		func(task.EntityFactoryContext) (*BankAccount, error) {
			return new(BankAccount), nil
		},
	)
	if err := r.AddEntityFactoryN("bankaccount", bankAccountFactory); err != nil {
		return fmt.Errorf("failed to register bank account entity: %w", err)
	}
	if err := r.AddOrchestratorN("transfer", TransferOrchestrator); err != nil {
		return fmt.Errorf("failed to register transfer orchestrator: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	app, err := dtssample.Start(ctx, r)
	if err != nil {
		return err
	}
	client := app.Client
	defer func() {
		if err := app.Shutdown(); err != nil {
			log.Printf("Failed to shut down: %v", err)
		}
	}()

	// Entity state is durable, so each run uses fresh keys rather than
	// accumulating on top of a previous run's balances.
	run := uuid.NewString()

	// --- Demo 1: Counter entity (raw function) ---
	fmt.Println("=== Counter Entity Demo ===")
	counterID := api.NewEntityID("counter", "myCounter-"+run)

	// Signal the entity to perform operations
	if err := client.SignalEntity(ctx, counterID, "add", api.WithSignalInput(10)); err != nil {
		return fmt.Errorf("failed to signal entity: %w", err)
	}
	if err := client.SignalEntity(ctx, counterID, "add", api.WithSignalInput(5)); err != nil {
		return fmt.Errorf("failed to signal entity: %w", err)
	}
	if err := client.SignalEntity(ctx, counterID, "add", api.WithSignalInput(-3)); err != nil {
		return fmt.Errorf("failed to signal entity: %w", err)
	}

	// Query the entity state
	meta, err := waitForEntityState(ctx, client, counterID, "12")
	if err != nil {
		return fmt.Errorf("failed to fetch entity: %w", err)
	}
	fmt.Printf("Counter state: %s\n", meta.SerializedState) // Expected: 12

	// --- Demo 2: BankAccount entity (auto-dispatch) ---
	fmt.Println("\n=== Bank Account Entity Demo ===")
	accountID := api.NewEntityID("bankaccount", "checking-"+run)

	if err := client.SignalEntity(ctx, accountID, "Deposit", api.WithSignalInput(1000)); err != nil {
		return fmt.Errorf("failed to signal entity: %w", err)
	}
	if err := client.SignalEntity(ctx, accountID, "Deposit", api.WithSignalInput(500)); err != nil {
		return fmt.Errorf("failed to signal entity: %w", err)
	}
	if err := client.SignalEntity(ctx, accountID, "Withdraw", api.WithSignalInput(200)); err != nil {
		return fmt.Errorf("failed to signal entity: %w", err)
	}

	meta, err = waitForEntityState(ctx, client, accountID, `{"balance":1300}`)
	if err != nil {
		return fmt.Errorf("failed to fetch entity: %w", err)
	}
	fmt.Printf("Bank account state: %s\n", meta.SerializedState) // Expected: {"balance":1300}

	savingsID := api.NewEntityID("bankaccount", "savings-"+run)
	if err := client.SignalEntity(ctx, savingsID, "Deposit", api.WithSignalInput(100)); err != nil {
		return fmt.Errorf("failed to initialize savings account: %w", err)
	}
	if _, err := waitForEntityState(ctx, client, savingsID, `{"balance":100}`); err != nil {
		return fmt.Errorf("failed to initialize savings account: %w", err)
	}
	transferID, err := client.ScheduleNewOrchestration(
		ctx,
		"transfer",
		api.WithInput(TransferInput{From: accountID, To: savingsID, Amount: 300}),
	)
	if err != nil {
		return fmt.Errorf("failed to schedule transfer: %w", err)
	}
	transfer, err := client.WaitForOrchestrationCompletion(ctx, transferID)
	if err != nil {
		return fmt.Errorf("transfer failed: %w", err)
	}
	fmt.Printf("Transfer result: %s\n", transfer.SerializedOutput)

	fmt.Println("\nDone!")
	return nil
}

// --- Pattern 1: Raw entity function ---

// CounterEntity is a simple counter entity that supports "add", "get", and "reset" operations.
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
		// just return current value
	case "reset":
		count = 0
	default:
		return nil, fmt.Errorf("unknown operation: %s", ctx.Operation)
	}

	if err := ctx.SetState(count); err != nil {
		return nil, err
	}
	return count, nil
}

// --- Pattern 2: Auto-dispatch entity ---

// BankAccount is a struct-based entity. Public methods are automatically
// dispatched by operation name (case-insensitive).
type BankAccountState struct {
	Balance int `json:"balance"`
}

type BankAccount struct {
	task.EntityObjectBase[BankAccountState]
}

func (a *BankAccount) Deposit(amount int) (any, error) {
	a.State().Balance += amount
	return a.State().Balance, nil
}

func (a *BankAccount) Withdraw(amount int) (any, error) {
	if amount > a.State().Balance {
		return nil, fmt.Errorf(
			"insufficient funds: balance=%d, withdrawal=%d",
			a.State().Balance,
			amount,
		)
	}
	a.State().Balance -= amount
	return a.State().Balance, nil
}

func (a *BankAccount) Get() (any, error) {
	return a.State().Balance, nil
}

type TransferInput struct {
	From   api.EntityID `json:"from"`
	To     api.EntityID `json:"to"`
	Amount int          `json:"amount"`
}

func TransferOrchestrator(ctx *task.OrchestrationContext) (any, error) {
	var input TransferInput
	if err := ctx.GetInput(&input); err != nil {
		return nil, err
	}
	unlock, err := ctx.LockEntities(input.From, input.To)
	if err != nil {
		return nil, err
	}
	defer unlock()

	var fromBalance int
	if err := ctx.CallEntity(
		input.From,
		"Withdraw",
		task.WithEntityInput(input.Amount),
	).Await(&fromBalance); err != nil {
		return nil, err
	}
	var toBalance int
	if err := ctx.CallEntity(
		input.To,
		"Deposit",
		task.WithEntityInput(input.Amount),
	).Await(&toBalance); err != nil {
		return nil, err
	}
	return map[string]int{"from": fromBalance, "to": toBalance}, nil
}

func waitForEntityState(
	ctx context.Context,
	client *durabletaskscheduler.Client,
	entityID api.EntityID,
	expected string,
) (*api.EntityMetadata, error) {
	timeout := time.NewTimer(30 * time.Second)
	defer timeout.Stop()
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		metadata, err := client.GetEntity(ctx, entityID)
		// The entity does not exist until the service has processed its first
		// signal, so a not-found result means "not ready yet", not a failure.
		if err != nil {
			return nil, err
		}
		if metadata != nil && metadata.SerializedState == expected {
			return metadata, nil
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-timeout.C:
			return nil, fmt.Errorf("timed out waiting for %s state %s", entityID.String(), expected)
		case <-ticker.C:
		}
	}
}
