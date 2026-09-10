// Command management demonstrates instance-management operations scoped to
// sample-owned orchestration IDs.
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"slices"
	"time"

	"github.com/microsoft/durabletask-go/api"
	"github.com/microsoft/durabletask-go/durabletaskscheduler"
	"github.com/microsoft/durabletask-go/samples/internal/dtssample"
	"github.com/microsoft/durabletask-go/task"
)

const (
	managementCompleteName = "SampleManagementComplete"
	managementProgressName = "SampleManagementProgress"
	managementWaitName     = "SampleManagementWait"
	managementTimerName    = "SampleManagementTimer"
)

type progressStatus struct {
	Step  int    `json:"step"`
	State string `json:"state"`
}

type waitInput struct {
	Count int `json:"count"`
}

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
	fmt.Println("SAMPLE_OK management")
}

func run() (err error) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	registry := task.NewTaskRegistry()
	if err := registry.AddOrchestratorN(managementCompleteName, managementCompleteOrchestrator); err != nil {
		return err
	}
	if err := registry.AddOrchestratorN(managementProgressName, managementProgressOrchestrator); err != nil {
		return err
	}
	if err := registry.AddOrchestratorN(managementWaitName, managementWaitOrchestrator); err != nil {
		return err
	}
	if err := registry.AddOrchestratorN(managementTimerName, managementTimerOrchestrator); err != nil {
		return err
	}
	app, err := dtssample.Start(ctx, registry)
	if err != nil {
		return err
	}
	var ownedIDs []api.InstanceID
	defer func() {
		err = errors.Join(err, dtssample.Cleanup(app.Client, ownedIDs...), app.Shutdown())
	}()

	checks := []struct {
		name string
		run  func(context.Context, *durabletaskscheduler.Client, *[]api.InstanceID) error
	}{
		{"start-wait-custom-status", verifyStartWaitAndCustomStatus},
		{"query-pages-and-id-list", verifyQueryPagesAndIDList},
		{"suspend-resume", verifySuspendResume},
		{"terminate", verifyTerminate},
		{"restart", verifyRestart},
		{"status-based-id-reuse", verifyStatusBasedIDReuse},
		{"single-and-batch-purge", verifySingleAndBatchPurge},
	}
	for _, check := range checks {
		if err := check.run(ctx, app.Client, &ownedIDs); err != nil {
			return fmt.Errorf("%s: %w", check.name, err)
		}
		fmt.Printf("verified %s\n", check.name)
	}
	return nil
}

func verifyStartWaitAndCustomStatus(ctx context.Context, client *durabletaskscheduler.Client, ownedIDs *[]api.InstanceID) error {
	id := dtssample.NewInstanceID("management-progress")
	*ownedIDs = append(*ownedIDs, id)
	if _, err := client.ScheduleNewOrchestration(
		ctx,
		managementProgressName,
		api.WithInstanceID(id),
		api.WithTags(map[string]string{"sample": "management", "scenario": "progress"}),
	); err != nil {
		return err
	}
	if err := waitForCustomStatus(ctx, client, id, progressStatus{Step: 1, State: "waiting"}); err != nil {
		return err
	}
	if err := client.RaiseEvent(ctx, id, "finish", api.WithEventPayload("done")); err != nil {
		return err
	}
	metadata, err := client.WaitForOrchestrationCompletion(ctx, id, api.WithFetchPayloads(true))
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
	if output != "done" {
		return fmt.Errorf("progress output=%q, want done", output)
	}
	return nil
}

func verifyQueryPagesAndIDList(ctx context.Context, client *durabletaskscheduler.Client, ownedIDs *[]api.InstanceID) error {
	prefix := string(dtssample.NewInstanceID("management-query"))
	ids := make([]api.InstanceID, 0, 3)
	for index := range 3 {
		id := api.InstanceID(fmt.Sprintf("%s-%d", prefix, index))
		ids = append(ids, id)
		*ownedIDs = append(*ownedIDs, id)
		if _, err := client.ScheduleNewOrchestration(
			ctx,
			managementCompleteName,
			api.WithInstanceID(id),
			api.WithInput(fmt.Sprintf("query-%d", index)),
			api.WithTags(map[string]string{"sample": "management", "query-page": fmt.Sprintf("%d", index%2)}),
		); err != nil {
			return err
		}
		metadata, err := client.WaitForOrchestrationCompletion(ctx, id, api.WithFetchPayloads(true))
		if err != nil {
			return err
		}
		if err := dtssample.RequireCompleted(metadata); err != nil {
			return err
		}
	}

	first, err := client.QueryInstances(ctx, api.OrchestrationQuery{
		InstanceIDPrefix:      prefix,
		Tags:                  map[string]string{"sample": "management", "query-page": "0"},
		PageSize:              1,
		FetchInputsAndOutputs: true,
	})
	if err != nil {
		return err
	}
	if len(first.Orchestrations) != 1 || first.ContinuationToken == "" {
		return fmt.Errorf("first query page = %+v; expected one result and a continuation token", first)
	}
	second, err := client.QueryInstances(ctx, api.OrchestrationQuery{
		InstanceIDPrefix:      prefix,
		Tags:                  map[string]string{"sample": "management", "query-page": "0"},
		PageSize:              1,
		ContinuationToken:     first.ContinuationToken,
		FetchInputsAndOutputs: true,
	})
	if err != nil {
		return err
	}
	if len(second.Orchestrations) != 1 || second.Orchestrations[0].InstanceID == first.Orchestrations[0].InstanceID {
		return fmt.Errorf("second query page = %+v; expected a different matching instance", second)
	}

	listed, err := collectCompletedInstanceIDs(ctx, client, 20)
	if err != nil {
		return err
	}
	for _, id := range ids {
		if !listed[id] {
			return fmt.Errorf("ListInstanceIDs did not return owned completed instance %s; this target needs service fallback instead of treating omission as pass", id)
		}
	}
	return nil
}

func verifySuspendResume(ctx context.Context, client *durabletaskscheduler.Client, ownedIDs *[]api.InstanceID) error {
	id := dtssample.NewInstanceID("management-suspend")
	*ownedIDs = append(*ownedIDs, id)
	if _, err := client.ScheduleNewOrchestration(
		ctx,
		managementWaitName,
		api.WithInstanceID(id),
		api.WithInput(waitInput{Count: 2}),
		api.WithTags(map[string]string{"sample": "management", "scenario": "suspend-resume"}),
	); err != nil {
		return err
	}
	if _, err := client.WaitForOrchestrationStart(ctx, id); err != nil {
		return err
	}
	if err := client.SuspendOrchestration(ctx, id, "management sample"); err != nil {
		return err
	}
	for _, value := range []string{"a", "b"} {
		if err := client.RaiseEvent(ctx, id, "value", api.WithEventPayload(value)); err != nil {
			return err
		}
	}
	waitCtx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	_, waitErr := client.WaitForOrchestrationCompletion(waitCtx, id)
	cancel()
	if !errors.Is(waitErr, context.DeadlineExceeded) {
		return fmt.Errorf("suspended instance completed unexpectedly: %v", waitErr)
	}
	suspended, err := client.FetchOrchestrationMetadata(ctx, id)
	if err != nil {
		return err
	}
	if suspended.RuntimeStatus != api.RUNTIME_STATUS_SUSPENDED {
		return fmt.Errorf("suspended status=%s, want SUSPENDED", suspended.RuntimeStatus)
	}
	if err := client.ResumeOrchestration(ctx, id, "management sample"); err != nil {
		return err
	}
	completed, err := client.WaitForOrchestrationCompletion(ctx, id, api.WithFetchPayloads(true))
	if err != nil {
		return err
	}
	if err := dtssample.RequireCompleted(completed); err != nil {
		return err
	}
	var values []string
	if err := completed.ReadOutput(&values); err != nil {
		return err
	}
	if !slices.Equal(values, []string{"a", "b"}) {
		return fmt.Errorf("suspend/resume output=%#v", values)
	}
	return nil
}

func verifyTerminate(ctx context.Context, client *durabletaskscheduler.Client, ownedIDs *[]api.InstanceID) error {
	id := dtssample.NewInstanceID("management-terminate")
	*ownedIDs = append(*ownedIDs, id)
	if _, err := client.ScheduleNewOrchestration(
		ctx,
		managementTimerName,
		api.WithInstanceID(id),
		api.WithTags(map[string]string{"sample": "management", "scenario": "terminate"}),
	); err != nil {
		return err
	}
	if _, err := client.WaitForOrchestrationStart(ctx, id); err != nil {
		return err
	}
	if err := client.TerminateOrchestration(ctx, id, api.WithOutput("terminated by management sample")); err != nil {
		return err
	}
	terminated, err := client.WaitForOrchestrationCompletion(ctx, id, api.WithFetchPayloads(true))
	if err != nil {
		return err
	}
	if terminated.RuntimeStatus != api.RUNTIME_STATUS_TERMINATED {
		return fmt.Errorf("terminate status=%s, want TERMINATED", terminated.RuntimeStatus)
	}
	var output string
	if err := terminated.ReadOutput(&output); err != nil {
		return err
	}
	if output != "terminated by management sample" {
		return fmt.Errorf("terminate output=%q", output)
	}
	return nil
}

func verifyRestart(ctx context.Context, client *durabletaskscheduler.Client, ownedIDs *[]api.InstanceID) error {
	id := dtssample.NewInstanceID("management-restart")
	*ownedIDs = append(*ownedIDs, id)
	if _, err := client.ScheduleNewOrchestration(
		ctx,
		managementCompleteName,
		api.WithInstanceID(id),
		api.WithInput("restart-payload"),
		api.WithTags(map[string]string{"sample": "management", "scenario": "restart"}),
	); err != nil {
		return err
	}
	original, err := client.WaitForOrchestrationCompletion(ctx, id, api.WithFetchPayloads(true))
	if err != nil {
		return err
	}
	if err := dtssample.RequireCompleted(original); err != nil {
		return err
	}
	restartedID, err := client.RestartInstance(ctx, id, api.WithRestartNewInstanceID(true))
	if errors.Is(err, api.ErrFeatureNotSupported) {
		return fmt.Errorf("RestartInstance is not supported by this target; run the management sample against a scheduler that implements restart")
	}
	if err != nil {
		return err
	}
	*ownedIDs = append(*ownedIDs, restartedID)
	restarted, err := client.WaitForOrchestrationCompletion(ctx, restartedID, api.WithFetchPayloads(true))
	if err != nil {
		return err
	}
	if err := dtssample.RequireCompleted(restarted); err != nil {
		return err
	}
	var output string
	if err := restarted.ReadOutput(&output); err != nil {
		return err
	}
	if output != "restart-payload" {
		return fmt.Errorf("RestartInstance returned output %q, want restart-payload; missing restart payloads are a service blocker, not a sample pass", output)
	}
	return nil
}

func verifyStatusBasedIDReuse(ctx context.Context, client *durabletaskscheduler.Client, ownedIDs *[]api.InstanceID) error {
	id := dtssample.NewInstanceID("management-id-reuse")
	*ownedIDs = append(*ownedIDs, id)
	if _, err := client.ScheduleNewOrchestration(
		ctx,
		managementCompleteName,
		api.WithInstanceID(id),
		api.WithInput("first"),
		api.WithTags(map[string]string{"sample": "management", "scenario": "id-reuse"}),
	); err != nil {
		return err
	}
	first, err := client.WaitForOrchestrationCompletion(ctx, id, api.WithFetchPayloads(true))
	if err != nil {
		return err
	}
	if err := dtssample.RequireCompleted(first); err != nil {
		return err
	}
	if _, err := client.ScheduleNewOrchestration(
		ctx,
		managementCompleteName,
		api.WithInstanceID(id),
		api.WithInput("duplicate"),
		api.WithOrchestrationIDReusePolicy(&api.OrchestrationIDReusePolicy{
			DedupeStatuses: []api.OrchestrationStatus{api.RUNTIME_STATUS_COMPLETED},
		}),
	); !errors.Is(err, api.ErrDuplicateInstance) {
		return fmt.Errorf("completed-status duplicate scheduling error=%v, want ErrDuplicateInstance", err)
	}
	if _, err := client.ScheduleNewOrchestration(
		ctx,
		managementCompleteName,
		api.WithInstanceID(id),
		api.WithInput("second"),
		api.WithOrchestrationIDReusePolicy(&api.OrchestrationIDReusePolicy{
			DedupeStatuses: []api.OrchestrationStatus{api.RUNTIME_STATUS_RUNNING, api.RUNTIME_STATUS_PENDING},
		}),
		api.WithTags(map[string]string{"sample": "management", "scenario": "id-reuse"}),
	); err != nil {
		return err
	}
	second, err := client.WaitForOrchestrationCompletion(ctx, id, api.WithFetchPayloads(true))
	if err != nil {
		return err
	}
	if err := dtssample.RequireCompleted(second); err != nil {
		return err
	}
	var output string
	if err := second.ReadOutput(&output); err != nil {
		return err
	}
	if output != "second" || second.ExecutionID == first.ExecutionID {
		return fmt.Errorf("ID reuse output=%q execution=%q first=%q", output, second.ExecutionID, first.ExecutionID)
	}
	return nil
}

func verifySingleAndBatchPurge(ctx context.Context, client *durabletaskscheduler.Client, ownedIDs *[]api.InstanceID) error {
	singleID := dtssample.NewInstanceID("management-single-purge")
	batchA := dtssample.NewInstanceID("management-batch-purge")
	batchB := dtssample.NewInstanceID("management-batch-purge")
	ids := []api.InstanceID{singleID, batchA, batchB}
	*ownedIDs = append(*ownedIDs, ids...)
	for _, id := range ids {
		if _, err := client.ScheduleNewOrchestration(ctx, managementCompleteName, api.WithInstanceID(id), api.WithInput(string(id))); err != nil {
			return err
		}
		if metadata, err := client.WaitForOrchestrationCompletion(ctx, id); err != nil {
			return err
		} else if err := dtssample.RequireCompleted(metadata); err != nil {
			return err
		}
	}
	single, err := client.PurgeInstances(ctx, api.PurgeInstancesRequest{
		InstanceIDs: []api.InstanceID{singleID},
		Recursive:   true,
	})
	if errors.Is(err, api.ErrFeatureNotSupported) {
		return fmt.Errorf("ID-scoped PurgeInstances is not supported by this target")
	}
	if err != nil {
		return err
	}
	if !single.IsComplete {
		return fmt.Errorf("single purge did not complete: %+v", single)
	}
	batch, err := client.PurgeInstances(ctx, api.PurgeInstancesRequest{
		InstanceIDs: []api.InstanceID{batchA, batchB},
		Recursive:   true,
	})
	if err != nil {
		return err
	}
	if !batch.IsComplete {
		return fmt.Errorf("batch purge did not complete: %+v", batch)
	}
	for _, id := range ids {
		if _, err := client.FetchOrchestrationMetadata(ctx, id); !errors.Is(err, api.ErrInstanceNotFound) {
			return fmt.Errorf("purged instance %s remains readable or lookup failed: %v", id, err)
		}
	}
	return nil
}

func managementCompleteOrchestrator(ctx *task.OrchestrationContext) (any, error) {
	var input string
	if err := ctx.GetInput(&input); err != nil {
		return nil, err
	}
	return input, nil
}

func managementProgressOrchestrator(ctx *task.OrchestrationContext) (any, error) {
	if err := ctx.SetCustomStatusValue(progressStatus{Step: 1, State: "waiting"}); err != nil {
		return nil, err
	}
	var value string
	if err := ctx.WaitForSingleEvent("finish", time.Minute).Await(&value); err != nil {
		return nil, err
	}
	if err := ctx.SetCustomStatusValue(progressStatus{Step: 2, State: "complete"}); err != nil {
		return nil, err
	}
	return value, nil
}

func managementWaitOrchestrator(ctx *task.OrchestrationContext) (any, error) {
	var input waitInput
	if err := ctx.GetInput(&input); err != nil {
		return nil, err
	}
	values := make([]string, 0, input.Count)
	for len(values) < input.Count {
		var value string
		if err := ctx.WaitForSingleEvent("value", time.Minute).Await(&value); err != nil {
			return nil, err
		}
		values = append(values, value)
	}
	return values, nil
}

func managementTimerOrchestrator(ctx *task.OrchestrationContext) (any, error) {
	if err := ctx.CreateTimer(time.Hour).Await(nil); err != nil {
		return nil, err
	}
	return "timer fired", nil
}

func waitForCustomStatus(
	ctx context.Context,
	client *durabletaskscheduler.Client,
	id api.InstanceID,
	want progressStatus,
) error {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		metadata, err := client.FetchOrchestrationMetadata(ctx, id, api.WithFetchPayloads(true))
		if err == nil && metadata != nil {
			var status progressStatus
			if readErr := metadata.ReadCustomStatus(&status); readErr == nil && status == want {
				return nil
			}
		} else if err != nil && !errors.Is(err, api.ErrInstanceNotFound) {
			return err
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("custom status %v was not observed for %s: %w", want, id, ctx.Err())
		case <-ticker.C:
		}
	}
}

func collectCompletedInstanceIDs(
	ctx context.Context,
	client *durabletaskscheduler.Client,
	maxPages int,
) (map[api.InstanceID]bool, error) {
	listed := map[api.InstanceID]bool{}
	token := ""
	for range maxPages {
		result, err := client.ListInstanceIDs(ctx, api.InstanceIDQuery{
			RuntimeStatus:     []api.OrchestrationStatus{api.RUNTIME_STATUS_COMPLETED},
			PageSize:          100,
			ContinuationToken: token,
		})
		if errors.Is(err, api.ErrFeatureNotSupported) {
			return nil, fmt.Errorf("ListInstanceIDs is not supported by this target; run against a scheduler that implements ID listing")
		}
		if err != nil {
			return nil, err
		}
		for _, id := range result.InstanceIDs {
			listed[id] = true
		}
		token = result.ContinuationToken
		if token == "" {
			break
		}
	}
	return listed, nil
}
