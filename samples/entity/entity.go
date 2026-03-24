// This sample demonstrates how to use durable entities with the Durable Task Go SDK.
// It shows two patterns:
//  1. A raw entity function (Counter) with manual operation dispatch
//  2. An auto-dispatch entity (BankAccount) where operations map to methods on a struct
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/microsoft/durabletask-go/api"
	"github.com/microsoft/durabletask-go/backend"
	"github.com/microsoft/durabletask-go/backend/sqlite"
	"github.com/microsoft/durabletask-go/task"
)

func main() {
	r := task.NewTaskRegistry()

	// Pattern 1: Register a raw entity function with manual dispatch
	if err := r.AddEntityN("counter", CounterEntity); err != nil {
		log.Fatalf("Failed to register counter entity: %v", err)
	}

	// Pattern 2: Register an auto-dispatch entity backed by a struct
	if err := r.AddEntityN("bankaccount", task.NewEntityFor[BankAccount]()); err != nil {
		log.Fatalf("Failed to register bank account entity: %v", err)
	}

	ctx := context.Background()
	client, worker, err := Init(ctx, r)
	if err != nil {
		log.Fatalf("Failed to initialize: %v", err)
	}
	defer func() {
		if err := worker.Shutdown(ctx); err != nil {
			log.Printf("Failed to shutdown: %v", err)
		}
	}()

	// --- Demo 1: Counter entity (raw function) ---
	fmt.Println("=== Counter Entity Demo ===")
	counterID := api.NewEntityID("counter", "myCounter")

	// Signal the entity to perform operations
	if err := client.SignalEntity(ctx, counterID, "add", api.WithSignalInput(10)); err != nil {
		log.Printf("Failed to signal entity: %v", err) //nolint:gocritic // sample code, keeping simple
		return
	}
	if err := client.SignalEntity(ctx, counterID, "add", api.WithSignalInput(5)); err != nil {
		log.Printf("Failed to signal entity: %v", err)
		return
	}
	if err := client.SignalEntity(ctx, counterID, "add", api.WithSignalInput(-3)); err != nil {
		log.Printf("Failed to signal entity: %v", err)
		return
	}

	// Wait for processing
	time.Sleep(3 * time.Second)

	// Query the entity state
	meta, err := client.FetchEntityMetadata(ctx, counterID, true)
	if err != nil {
		log.Printf("Failed to fetch entity: %v", err)
		return
	}
	fmt.Printf("Counter state: %s\n", meta.SerializedState) // Expected: 12

	// --- Demo 2: BankAccount entity (auto-dispatch) ---
	fmt.Println("\n=== Bank Account Entity Demo ===")
	accountID := api.NewEntityID("bankaccount", "checking-001")

	if err := client.SignalEntity(ctx, accountID, "Deposit", api.WithSignalInput(1000)); err != nil {
		log.Printf("Failed to signal entity: %v", err)
		return
	}
	if err := client.SignalEntity(ctx, accountID, "Deposit", api.WithSignalInput(500)); err != nil {
		log.Printf("Failed to signal entity: %v", err)
		return
	}
	if err := client.SignalEntity(ctx, accountID, "Withdraw", api.WithSignalInput(200)); err != nil {
		log.Printf("Failed to signal entity: %v", err)
		return
	}

	time.Sleep(3 * time.Second)

	meta, err = client.FetchEntityMetadata(ctx, accountID, true)
	if err != nil {
		log.Printf("Failed to fetch entity: %v", err)
		return
	}
	fmt.Printf("Bank account state: %s\n", meta.SerializedState) // Expected: {"balance":1300}

	fmt.Println("\nDone!")
}

// Init creates and initializes an in-memory client and worker pair.
func Init(ctx context.Context, r *task.TaskRegistry) (backend.EntityTaskHubClient, backend.TaskHubWorker, error) {
	logger := backend.DefaultLogger()
	be := sqlite.NewSqliteBackend(sqlite.NewSqliteOptions(""), logger)
	executor := task.NewTaskExecutor(r)
	orchestrationWorker := backend.NewOrchestrationWorker(be, executor, logger)
	activityWorker := backend.NewActivityTaskWorker(be, executor, logger)
	taskHubWorker := backend.NewTaskHubWorker(be, orchestrationWorker, activityWorker, logger)
	if err := taskHubWorker.Start(ctx); err != nil {
		return nil, nil, err
	}
	taskHubClient := backend.NewTaskHubClient(be)
	return taskHubClient.(backend.EntityTaskHubClient), taskHubWorker, nil
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
type BankAccount struct {
	Balance int `json:"balance"`
}

func (a *BankAccount) Deposit(amount int) (any, error) {
	a.Balance += amount
	return a.Balance, nil
}

func (a *BankAccount) Withdraw(amount int) (any, error) {
	if amount > a.Balance {
		return nil, fmt.Errorf("insufficient funds: balance=%d, withdrawal=%d", a.Balance, amount)
	}
	a.Balance -= amount
	return a.Balance, nil
}

func (a *BankAccount) Get() (any, error) {
	return a.Balance, nil
}
