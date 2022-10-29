package samples

import (
	"context"
	"fmt"

	"github.com/microsoft/durabletask-go/task"
)

func RunSequenceSample() {
	r := task.NewTaskRegistry()
	r.AddOrchestrator(ActivitySequenceOrchestrator)
	r.AddActivity(SayHelloActivity)

	ctx := context.Background()
	client, worker := Init(ctx, r)
	defer worker.Shutdown(ctx)

	id, err := client.ScheduleNewOrchestration(ctx, ActivitySequenceOrchestrator)
	if err != nil {
		panic(err)
	}
	metadata, err := client.WaitForOrchestrationCompletion(ctx, id)
	if err != nil {
		panic(err)
	}
	fmt.Printf("orchestration completed: %v\n", metadata)
}

func ActivitySequenceOrchestrator(ctx *task.OrchestrationContext) (any, error) {
	var helloTokyo string
	if err := ctx.CallActivity(SayHelloActivity, "Tokyo").Await(&helloTokyo); err != nil {
		return nil, err
	}
	var helloLondon string
	if err := ctx.CallActivity(SayHelloActivity, "London").Await(&helloLondon); err != nil {
		return nil, err
	}
	var helloSeattle string
	if err := ctx.CallActivity(SayHelloActivity, "Seattle").Await(&helloSeattle); err != nil {
		return nil, err
	}
	return []string{helloTokyo, helloLondon, helloSeattle}, nil
}

func SayHelloActivity(ctx task.ActivityContext) (any, error) {
	var input string
	if err := ctx.GetInput(&input); err != nil {
		return "", err
	}
	return fmt.Sprintf("Hello, %s!", input), nil
}
