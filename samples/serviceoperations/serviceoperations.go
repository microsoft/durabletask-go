// Command serviceoperations verifies hub-wide maintenance in a disposable task hub.
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/microsoft/durabletask-go/api"
	"github.com/microsoft/durabletask-go/durabletaskscheduler"
	"github.com/microsoft/durabletask-go/samples/internal/dtssample"
	"github.com/microsoft/durabletask-go/task"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
	fmt.Println("SAMPLE_OK serviceoperations")
}

func run() (err error) {
	options, err := dtssample.Options()
	if err != nil {
		return err
	}
	if os.Getenv("DTS_SAMPLE_ALLOW_HUB_MAINTENANCE") != "1" ||
		!strings.HasPrefix(options.TaskHubName, "sample-") {
		return errors.New("administration requires DTS_SAMPLE_ALLOW_HUB_MAINTENANCE=1 and a disposable task hub whose name starts with sample-; never use a shared hub")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	registry := task.NewTaskRegistry()
	if err := registry.AddOrchestratorN("SampleAdminEcho", adminEcho); err != nil {
		return err
	}
	if err := registry.AddEntityN("SampleAdminEntity", adminEntity); err != nil {
		return err
	}
	app, err := dtssample.StartWithOptions(ctx, options, registry)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, app.Shutdown()) }()

	var failures []error
	for _, check := range []struct {
		name string
		run  func(context.Context) error
	}{
		{"filtered purge", func(ctx context.Context) error { return verifyFilteredPurge(ctx, app.Client) }},
		{"entity maintenance", func(ctx context.Context) error { return verifyEntityMaintenance(ctx, app.Client) }},
	} {
		checkCtx, stop := context.WithTimeout(ctx, 40*time.Second)
		checkErr := check.run(checkCtx)
		stop()
		if checkErr != nil {
			failures = append(failures, fmt.Errorf("%s: %w", check.name, checkErr))
		} else {
			fmt.Printf("verified %s\n", check.name)
		}
	}
	return errors.Join(failures...)
}

func adminEcho(ctx *task.OrchestrationContext) (any, error) {
	var input string
	if err := ctx.GetInput(&input); err != nil {
		return nil, err
	}
	return input, nil
}

func adminEntity(ctx *task.EntityContext) (any, error) {
	switch ctx.Operation {
	case "set":
		return nil, ctx.SetState(1)
	case "delete":
		ctx.DeleteState()
		return nil, nil
	default:
		return nil, fmt.Errorf("unknown operation %q", ctx.Operation)
	}
}

func completeEcho(ctx context.Context, client *durabletaskscheduler.Client, id api.InstanceID) (*api.OrchestrationMetadata, error) {
	if _, err := client.ScheduleNewOrchestration(ctx, "SampleAdminEcho",
		api.WithInstanceID(id), api.WithInput("echo")); err != nil {
		return nil, err
	}
	metadata, err := client.WaitForOrchestrationCompletion(ctx, id, api.WithFetchPayloads(true))
	if err != nil {
		return nil, err
	}
	if err := dtssample.RequireCompleted(metadata); err != nil {
		return nil, err
	}
	var output string
	if err := metadata.ReadOutput(&output); err != nil {
		return nil, err
	}
	if output != "echo" {
		return nil, fmt.Errorf("echo output=%q, want echo", output)
	}
	return metadata, nil
}

func verifyFilteredPurge(ctx context.Context, client *durabletaskscheduler.Client) (err error) {
	var ids []api.InstanceID
	defer func() { err = errors.Join(err, dtssample.Cleanup(client, ids...)) }()
	var first, last time.Time
	for range 3 {
		id := dtssample.NewInstanceID("filtered-purge")
		ids = append(ids, id)
		metadata, err := completeEcho(ctx, client, id)
		if err != nil {
			return err
		}
		if first.IsZero() || metadata.CreatedAt.Before(first) {
			first = metadata.CreatedAt
		}
		if metadata.CreatedAt.After(last) {
			last = metadata.CreatedAt
		}
	}
	result, err := client.PurgeInstances(ctx, api.PurgeInstancesRequest{
		Filter: &api.PurgeInstanceFilter{
			CreatedTimeFrom: first.Add(-time.Second), CreatedTimeTo: last.Add(time.Second),
			RuntimeStatus: []api.OrchestrationStatus{api.RUNTIME_STATUS_COMPLETED},
		},
	})
	if err != nil {
		return err
	}
	if !result.IsComplete || result.DeletedInstanceCount < len(ids) {
		return fmt.Errorf("filtered purge did not remove the subjects: %+v", result)
	}
	for _, id := range ids {
		if _, err := client.FetchOrchestrationMetadata(ctx, id); !errors.Is(err, api.ErrInstanceNotFound) {
			return fmt.Errorf("purged instance %s remains readable or lookup failed: %v", id, err)
		}
	}
	return nil
}

func verifyEntityMaintenance(ctx context.Context, client *durabletaskscheduler.Client) (err error) {
	id := api.NewEntityID("SampleAdminEntity", string(dtssample.NewInstanceID("entity-maintenance")))
	live := api.NewEntityID("SampleAdminEntity", string(dtssample.NewInstanceID("entity-preserved")))
	needsDelete := true
	defer func() {
		cleanupCtx, stop := context.WithTimeout(context.Background(), 15*time.Second)
		defer stop()
		for _, target := range []api.EntityID{live, id} {
			if target == id && !needsDelete {
				continue
			}
			cleanupErr := client.SignalEntity(cleanupCtx, target, "delete")
			if cleanupErr == nil {
				_, cleanupErr = waitForEntity(cleanupCtx, client, target, func(m *api.EntityMetadata) bool {
					return m == nil || !m.HasState
				})
			}
			err = errors.Join(err, cleanupErr)
		}
	}()
	if err := client.SignalEntity(ctx, live, "set"); err != nil {
		return err
	}
	if _, err := waitForEntity(ctx, client, live, func(m *api.EntityMetadata) bool {
		return m != nil && m.HasState && m.SerializedState == "1"
	}); err != nil {
		return err
	}
	if err := client.SignalEntity(ctx, id, "set"); err != nil {
		return err
	}
	if _, err := waitForEntity(ctx, client, id, func(m *api.EntityMetadata) bool {
		return m != nil && m.HasState && m.SerializedState == "1"
	}); err != nil {
		return err
	}
	if err := client.SignalEntity(ctx, id, "delete"); err != nil {
		return err
	}
	_, err = waitForEntity(ctx, client, id, func(m *api.EntityMetadata) bool {
		return m == nil || !m.HasState
	})
	if err != nil {
		return err
	}
	needsDelete = false
	query := api.EntityQuery{InstanceIDStartsWith: id.String(), IncludeTransient: true}
	before, err := client.QueryEntities(ctx, query)
	if err != nil {
		return err
	}
	removed := int32(0)
	for {
		cleaned, err := client.CleanEntityStorage(ctx)
		if err != nil {
			return err
		}
		removed += cleaned.EmptyEntitiesRemoved
		after, err := client.QueryEntities(ctx, query)
		if err != nil {
			return err
		}
		if len(after.Entities) == 0 && (len(before.Entities) == 0 || removed > 0) {
			preserved, err := client.GetEntity(ctx, live)
			if err != nil {
				return err
			}
			if preserved == nil || !preserved.HasState || preserved.SerializedState != "1" {
				return errors.New("entity maintenance changed the nonempty entity")
			}
			if len(before.Entities) == 0 {
				fmt.Println("empty entity was already removed by the service; verified maintenance idempotency and preservation of live state")
			} else {
				fmt.Printf("maintenance removed retained empty state; reported removals=%d\n", removed)
			}
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("maintenance left %d transient records and reported %d removals: %w",
				len(after.Entities), removed, ctx.Err())
		case <-time.After(200 * time.Millisecond):
		}
	}
}

func waitForEntity(ctx context.Context, client *durabletaskscheduler.Client, id api.EntityID, ready func(*api.EntityMetadata) bool) (*api.EntityMetadata, error) {
	for {
		metadata, err := client.GetEntity(ctx, id)
		if err != nil {
			return nil, err
		}
		if ready(metadata) {
			return metadata, nil
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
	}
}
