package azuremanaged_test

import (
	"context"
	"fmt"

	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/microsoft/durabletask-go/azuremanaged"
	"github.com/microsoft/durabletask-go/task"
)

// ExampleNewClientFromConnectionString shows how to connect to an Azure-managed Durable Task
// Scheduler (DTS) task hub using a connection string.
func ExampleNewClientFromConnectionString() {
	// A DTS connection string has the form:
	//   Endpoint=<address>;Authentication=<type>;TaskHub=<name>
	client, err := azuremanaged.NewClientFromConnectionString(
		"Endpoint=myscheduler.westus2.durabletask.io;Authentication=DefaultAzure;TaskHub=myhub")
	if err != nil {
		panic(err)
	}

	id, err := client.ScheduleNewOrchestration(context.Background(), "MyOrchestrator")
	if err != nil {
		panic(err)
	}
	fmt.Println(id)
}

// ExampleNewClient shows how to configure a DTS client explicitly with Options and an Azure
// credential, and ExampleNewClient also registers a worker for the same task hub.
func ExampleNewClient() {
	credential, err := azidentity.NewDefaultAzureCredential(nil)
	if err != nil {
		panic(err)
	}

	opts := &azuremanaged.Options{
		Endpoint:   "myscheduler.westus2.durabletask.io",
		TaskHub:    "myhub",
		Credential: credential,
	}

	// The worker registers orchestrator/activity functions and listens for work.
	worker, err := azuremanaged.NewWorker(opts)
	if err != nil {
		panic(err)
	}
	r := task.NewTaskRegistry()
	_ = r.AddOrchestratorN("MyOrchestrator", func(ctx *task.OrchestrationContext) (any, error) {
		var out string
		err := ctx.CallActivity("SayHello", task.WithActivityInput("world")).Await(&out)
		return out, err
	})
	_ = r.AddActivityN("SayHello", func(ctx task.ActivityContext) (any, error) {
		var name string
		if err := ctx.GetInput(&name); err != nil {
			return nil, err
		}
		return "Hello, " + name + "!", nil
	})
	if err := worker.StartWorkItemListener(context.Background(), r); err != nil {
		panic(err)
	}

	// The client schedules and manages orchestration instances.
	client, err := azuremanaged.NewClient(opts)
	if err != nil {
		panic(err)
	}
	id, err := client.ScheduleNewOrchestration(context.Background(), "MyOrchestrator")
	if err != nil {
		panic(err)
	}
	metadata, err := client.WaitForOrchestrationCompletion(context.Background(), id)
	if err != nil {
		panic(err)
	}
	fmt.Println(metadata.RuntimeStatus)
}
