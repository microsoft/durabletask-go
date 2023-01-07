package samples

import (
	"context"
	"fmt"
	"time"

	"github.com/microsoft/durabletask-go/task"
)

func RunExternalEventsSample() {
	r := task.NewTaskRegistry()
	r.AddOrchestrator(ExternalEventOrchestrator)

	ctx := context.Background()
	client, worker := Init(ctx, r)
	defer worker.Shutdown(ctx)

	// Start the orchestration
	id, err := client.ScheduleNewOrchestration(ctx, ExternalEventOrchestrator)
	if err != nil {
		panic(err)
	}
	metadata, err := client.WaitForOrchestrationStart(ctx, id)
	if err != nil {
		panic(err)
	}

	// Prompt the user for their name and send that to the orchestrator
	go func() {
		fmt.Println("Enter your first name: ")
		var nameInput string
		fmt.Scanln(&nameInput)
		if err = client.RaiseEvent(ctx, id, "Name", nameInput); err != nil {
			panic(err)
		}
	}()

	// After the orchestration receives the event, it should complete on its own
	metadata, err = client.WaitForOrchestrationCompletion(ctx, id)
	if err != nil {
		panic(err)
	}
	if metadata.FailureDetails != nil {
		fmt.Println("orchestration failed:", metadata.FailureDetails.ErrorMessage)
	} else {
		fmt.Println("orchestration completed:", metadata.SerializedOutput)
	}
}

// ExternalEventOrchestrator is an orchestrator function that blocks for 30 seconds or
// until a "Name" event is sent to it.
func ExternalEventOrchestrator(ctx *task.OrchestrationContext) (any, error) {
	var nameInput string
	if err := ctx.WaitForSingleEvent("Name", 30*time.Second).Await(&nameInput); err != nil {
		// Timeout expired
		return nil, err
	}

	return fmt.Sprintf("Hello, %s!", nameInput), nil
}
