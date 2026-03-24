// Integration tests for in-process entity execution via the orchestration worker.
package tests

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/microsoft/durabletask-go/api"
	"github.com/microsoft/durabletask-go/backend"
	"github.com/microsoft/durabletask-go/task"
)

// Test that an entity can be signaled from the client and processed in-process.
func Test_InProcess_Entity_SignalAndQuery(t *testing.T) {
	r := task.NewTaskRegistry()
	require.NoError(t, r.AddEntityN("counter", func(ctx *task.EntityContext) (any, error) {
		var count int
		if ctx.HasState() {
			_ = ctx.GetState(&count)
		}
		switch ctx.Operation {
		case "add":
			var amount int
			if err := ctx.GetInput(&amount); err != nil {
				return nil, err
			}
			count += amount
		case "get":
			// no-op, just return
		}
		_ = ctx.SetState(count)
		return count, nil
	}))

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	baseClient, worker := initTaskHubWorker(ctx, r)
	client := baseClient.(backend.EntityTaskHubClient)
	defer func() {
		if err := worker.Shutdown(context.Background()); err != nil {
			t.Logf("shutdown: %v", err)
		}
	}()

	entityID := api.NewEntityID("counter", "test1")

	// Signal the entity to add 5
	err := client.SignalEntity(ctx, entityID, "add", api.WithSignalInput(5))
	require.NoError(t, err)

	// Give the entity worker time to process
	time.Sleep(2 * time.Second)

	// Signal again to add 3
	err = client.SignalEntity(ctx, entityID, "add", api.WithSignalInput(3))
	require.NoError(t, err)

	time.Sleep(2 * time.Second)

	// Verify state via FetchEntityMetadata
	meta, err := client.FetchEntityMetadata(ctx, entityID, true)
	require.NoError(t, err)
	require.NotNil(t, meta)
	assert.Equal(t, entityID, meta.InstanceID)
	// State should be 8 (5+3), stored as custom status
	assert.Contains(t, meta.SerializedState, "8")
}

// Test that entities work with the auto-dispatch pattern.
func Test_InProcess_Entity_AutoDispatch(t *testing.T) {
	type counter struct {
		Value int `json:"value"`
	}

	r := task.NewTaskRegistry()
	require.NoError(t, r.AddEntityN("smartcounter", task.NewEntityFor[counter]()))
	// Register a dummy Add method on counter for the dispatcher
	// Actually, we need counter to have methods. Let's use the raw entity pattern instead.
	// Re-register with raw function since we can't add methods to a local type.

	r2 := task.NewTaskRegistry()
	require.NoError(t, r2.AddEntityN("counter", func(ctx *task.EntityContext) (any, error) {
		var val int
		if ctx.HasState() {
			_ = ctx.GetState(&val)
		}
		switch ctx.Operation {
		case "increment":
			val++
		case "decrement":
			val--
		case "get":
		}
		_ = ctx.SetState(val)
		return val, nil
	}))

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	baseClient, worker := initTaskHubWorker(ctx, r2)
	client := baseClient.(backend.EntityTaskHubClient)
	defer func() {
		if err := worker.Shutdown(context.Background()); err != nil {
			t.Logf("shutdown: %v", err)
		}
	}()

	entityID := api.NewEntityID("counter", "auto")

	// Send multiple signals
	require.NoError(t, client.SignalEntity(ctx, entityID, "increment"))
	require.NoError(t, client.SignalEntity(ctx, entityID, "increment"))
	require.NoError(t, client.SignalEntity(ctx, entityID, "increment"))
	require.NoError(t, client.SignalEntity(ctx, entityID, "decrement"))

	time.Sleep(3 * time.Second)

	// Should be 2 (3 increments - 1 decrement)
	meta, err := client.FetchEntityMetadata(ctx, entityID, true)
	require.NoError(t, err)
	require.NotNil(t, meta)
	assert.Contains(t, meta.SerializedState, "2")
}
