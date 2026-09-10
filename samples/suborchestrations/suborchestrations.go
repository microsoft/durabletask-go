// Command suborchestrations demonstrates parent/child orchestration composition
// with explicit child instance IDs, result aggregation, propagated tags/context,
// and child-failure propagation.
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"maps"
	"slices"
	"strings"
	"time"

	"github.com/microsoft/durabletask-go/api"
	"github.com/microsoft/durabletask-go/durabletaskscheduler"
	"github.com/microsoft/durabletask-go/samples/internal/dtssample"
	"github.com/microsoft/durabletask-go/task"
)

const (
	subParentName   = "SampleSubOrchestrationsParent"
	subChildName    = "SampleSubOrchestrationsChild"
	subActivityName = "SampleSubOrchestrationsFormat"
)

type subParentInput struct {
	Children       []string `json:"children"`
	IncludeFailure bool     `json:"includeFailure"`
}

type subChildInput struct {
	Name   string `json:"name"`
	Fail   bool   `json:"fail"`
	Tenant string `json:"tenant,omitempty"`
}

type subChildResult struct {
	Name          string            `json:"name"`
	Message       string            `json:"message"`
	ContextFields map[string]string `json:"contextFields"`
}

type subParentOutput struct {
	CombinedResults []string            `json:"combinedResults"`
	ChildIDs        []api.InstanceID    `json:"childIds"`
	ChildContexts   []map[string]string `json:"childContexts"`
	Children        []subChildResult    `json:"children"`
}

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
	fmt.Println("SAMPLE_OK suborchestrations")
}

func run() (err error) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	registry := task.NewTaskRegistry()
	if err := registry.AddOrchestratorN(subParentName, subParentOrchestrator); err != nil {
		return err
	}
	if err := registry.AddOrchestratorN(subChildName, subChildOrchestrator); err != nil {
		return err
	}
	if err := registry.AddActivityN(subActivityName, subFormatActivity); err != nil {
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

	parentID := dtssample.NewInstanceID("suborchestrations-parent")
	childIDs := plannedSubChildIDs(parentID, 2)
	ownedIDs = append(ownedIDs, parentID)
	ownedIDs = append(ownedIDs, childIDs...)
	if err := verifySuccessfulParent(ctx, app.Client, parentID, childIDs); err != nil {
		return err
	}

	failureParentID := dtssample.NewInstanceID("suborchestrations-failure")
	failureChildIDs := plannedSubChildIDs(failureParentID, 3)
	ownedIDs = append(ownedIDs, failureParentID)
	ownedIDs = append(ownedIDs, failureChildIDs...)
	if err := verifyChildFailurePropagation(ctx, app.Client, failureParentID); err != nil {
		return err
	}
	return nil
}

func verifySuccessfulParent(ctx context.Context, client *durabletaskscheduler.Client, parentID api.InstanceID, childIDs []api.InstanceID) error {
	_, err := client.ScheduleNewOrchestration(
		ctx,
		subParentName,
		api.WithInstanceID(parentID),
		api.WithInput(subParentInput{Children: []string{"alpha", "bravo"}}),
		api.WithTags(map[string]string{"sample": "suborchestrations", "scenario": "success"}),
		api.WithContextFields(api.ContextFields{"tenant": "sample", "request": "sub-orchestration"}),
	)
	if err != nil {
		return err
	}
	metadata, err := client.WaitForOrchestrationCompletion(ctx, parentID, api.WithFetchPayloads(true))
	if err != nil {
		return err
	}
	if err := dtssample.RequireCompleted(metadata); err != nil {
		return err
	}
	var output subParentOutput
	if err := metadata.ReadOutput(&output); err != nil {
		return err
	}
	if !slices.Equal(output.CombinedResults, []string{"child alpha for sample", "child bravo for sample"}) {
		return fmt.Errorf("combined child results = %#v", output.CombinedResults)
	}
	if !slices.Equal(output.ChildIDs, childIDs) {
		return fmt.Errorf("child IDs = %#v, want %#v", output.ChildIDs, childIDs)
	}
	parentHistory, err := client.GetOrchestrationHistory(ctx, parentID, api.HistoryQuery{ExecutionID: metadata.ExecutionID})
	if err != nil {
		return err
	}
	if countHistoryEvents(parentHistory, api.HistoryEventSubOrchestrationInstanceCreated) != len(childIDs) ||
		countHistoryEvents(parentHistory, api.HistoryEventSubOrchestrationInstanceCompleted) != len(childIDs) {
		return fmt.Errorf("parent history does not contain expected child create/complete events")
	}
	for _, childID := range childIDs {
		child, err := client.FetchOrchestrationMetadata(ctx, childID, api.WithFetchPayloads(true))
		if err != nil {
			return err
		}
		if err := dtssample.RequireCompleted(child); err != nil {
			return err
		}
		history, err := client.GetOrchestrationHistory(ctx, childID, api.HistoryQuery{ExecutionID: child.ExecutionID})
		if err != nil {
			return err
		}
		started := executionStarted(history)
		if started == nil || started.Parent == nil || started.Parent.InstanceID != parentID {
			return fmt.Errorf("child %s did not record parent %s in history", childID, parentID)
		}
		if started.ContextFields["tenant"] != "sample" || started.ContextFields["child-scope"] != "explicit" {
			return fmt.Errorf("child %s context fields = %#v", childID, started.ContextFields)
		}
	}
	fmt.Printf("verified parent %s and %d children\n", parentID, len(childIDs))
	return nil
}

func verifyChildFailurePropagation(ctx context.Context, client *durabletaskscheduler.Client, parentID api.InstanceID) error {
	_, err := client.ScheduleNewOrchestration(
		ctx,
		subParentName,
		api.WithInstanceID(parentID),
		api.WithInput(subParentInput{Children: []string{"ok-a", "ok-b"}, IncludeFailure: true}),
		api.WithTags(map[string]string{"sample": "suborchestrations", "scenario": "failure"}),
	)
	if err != nil {
		return err
	}
	metadata, err := client.WaitForOrchestrationCompletion(ctx, parentID, api.WithFetchPayloads(true))
	if err != nil {
		return err
	}
	if metadata.RuntimeStatus != api.RUNTIME_STATUS_FAILED {
		return fmt.Errorf("failure propagation status = %s, want FAILED", metadata.RuntimeStatus)
	}
	if metadata.FailureDetails == nil ||
		!strings.Contains(metadata.FailureDetails.ErrorMessage, "child forced-error failed as requested") {
		return fmt.Errorf("parent %s did not preserve the expected child failure: %v", parentID, metadata.FailureDetails)
	}
	history, err := client.GetOrchestrationHistory(ctx, parentID, api.HistoryQuery{ExecutionID: metadata.ExecutionID})
	if err != nil {
		return err
	}
	if countHistoryEvents(history, api.HistoryEventSubOrchestrationInstanceFailed) != 1 {
		return fmt.Errorf("parent history did not record exactly one failed child")
	}
	fmt.Printf("verified child failure propagation for %s\n", parentID)
	return nil
}

func subParentOrchestrator(ctx *task.OrchestrationContext) (any, error) {
	var input subParentInput
	if err := ctx.GetInput(&input); err != nil {
		return nil, err
	}
	childNames := append([]string(nil), input.Children...)
	if input.IncludeFailure {
		childNames = append(childNames, "forced-error")
	}
	output := subParentOutput{
		ChildIDs:      make([]api.InstanceID, 0, len(childNames)),
		ChildContexts: make([]map[string]string, 0, len(childNames)),
		Children:      make([]subChildResult, 0, len(childNames)),
	}
	parentFields := api.ContextFieldsFromContext(ctx.Context())
	for index, name := range childNames {
		childID := api.InstanceID(fmt.Sprintf("%s-child-%02d", ctx.ID, index))
		output.ChildIDs = append(output.ChildIDs, childID)
		childFields := api.ContextFields{"child-scope": "explicit"}
		maps.Copy(childFields, parentFields)
		var child subChildResult
		err := ctx.CallSubOrchestrator(
			subChildName,
			task.WithSubOrchestrationInstanceID(string(childID)),
			task.WithSubOrchestratorInput(subChildInput{Name: name, Fail: name == "forced-error"}),
			task.WithSubOrchestrationTags(map[string]string{"sample": "suborchestrations", "child": name}),
			task.WithSubOrchestrationContextFields(childFields),
		).Await(&child)
		if err != nil {
			return nil, err
		}
		output.Children = append(output.Children, child)
		output.CombinedResults = append(output.CombinedResults, child.Message)
		output.ChildContexts = append(output.ChildContexts, maps.Clone(child.ContextFields))
	}
	return output, nil
}

func subChildOrchestrator(ctx *task.OrchestrationContext) (any, error) {
	var input subChildInput
	if err := ctx.GetInput(&input); err != nil {
		return nil, err
	}
	if input.Fail {
		return nil, fmt.Errorf("child %s failed as requested", input.Name)
	}
	fields := api.ContextFieldsFromContext(ctx.Context())
	input.Tenant = fields["tenant"]
	var result subChildResult
	if err := ctx.CallActivity(subActivityName, task.WithActivityInput(input)).Await(&result); err != nil {
		return nil, err
	}
	result.ContextFields = maps.Clone(fields)
	return result, nil
}

func subFormatActivity(ctx task.ActivityContext) (any, error) {
	var input subChildInput
	if err := ctx.GetInput(&input); err != nil {
		return nil, err
	}
	return subChildResult{
		Name:    input.Name,
		Message: fmt.Sprintf("child %s for %s", input.Name, input.Tenant),
	}, nil
}

func plannedSubChildIDs(parentID api.InstanceID, count int) []api.InstanceID {
	ids := make([]api.InstanceID, 0, count)
	for index := range count {
		ids = append(ids, api.InstanceID(fmt.Sprintf("%s-child-%02d", parentID, index)))
	}
	return ids
}

func countHistoryEvents(history *api.OrchestrationHistory, eventType api.HistoryEventType) int {
	count := 0
	if history == nil {
		return count
	}
	for _, event := range history.Events {
		if event.Type == eventType {
			count++
		}
	}
	return count
}

func executionStarted(history *api.OrchestrationHistory) *api.HistoryExecutionStartedEvent {
	if history == nil {
		return nil
	}
	for _, event := range history.Events {
		if event.ExecutionStarted != nil {
			return event.ExecutionStarted
		}
	}
	return nil
}
