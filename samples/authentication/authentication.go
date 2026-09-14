// Command authentication runs real DTS work using connection-string and
// programmatic Azure Identity credentials.
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/url"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/microsoft/durabletask-go/api"
	"github.com/microsoft/durabletask-go/durabletaskscheduler"
	"github.com/microsoft/durabletask-go/samples/internal/dtssample"
	"github.com/microsoft/durabletask-go/task"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
	fmt.Println("SAMPLE_OK authentication")
}

func run() error {
	options, err := dtssample.Options()
	if err != nil {
		return err
	}
	endpoint, err := url.Parse(options.EndpointAddress)
	if err != nil {
		return err
	}
	if endpoint.Scheme != "https" || options.Authentication == durabletaskscheduler.AuthenticationNone {
		return errors.New("authentication requires an HTTPS DTS endpoint and Azure Identity; an emulator run does not validate authentication")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	if err := runAuthenticated(ctx, options, "connection-string"); err != nil {
		return err
	}

	credential, err := azidentity.NewDefaultAzureCredential(&azidentity.DefaultAzureCredentialOptions{
		TenantID: options.TenantID,
	})
	if err != nil {
		return err
	}
	programmatic := durabletaskscheduler.NewOptionsWithCredential(
		options.EndpointAddress, options.TaskHubName, credential,
	)
	programmatic.ResourceID = options.ResourceID
	return runAuthenticated(ctx, programmatic, "programmatic-credential")
}

func runAuthenticated(ctx context.Context, options *durabletaskscheduler.Options, mode string) (err error) {
	registry := task.NewTaskRegistry()
	if err := registry.AddOrchestratorN("SampleAuthentication", authenticatedWorkflow); err != nil {
		return err
	}
	if err := registry.AddActivityN("SampleAuthenticationEcho", authenticatedEcho); err != nil {
		return err
	}
	app, err := dtssample.StartWithOptions(ctx, options, registry)
	if err != nil {
		return fmt.Errorf("%s connection: %w", mode, err)
	}
	defer func() { err = errors.Join(err, app.Shutdown()) }()
	id := dtssample.NewInstanceID("authentication")
	defer func() { err = errors.Join(err, dtssample.Cleanup(app.Client, id)) }()
	if _, err := app.Client.ScheduleNewOrchestration(ctx, "SampleAuthentication",
		api.WithInstanceID(id), api.WithInput(mode)); err != nil {
		return err
	}
	metadata, err := app.Client.WaitForOrchestrationCompletion(ctx, id, api.WithFetchPayloads(true))
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
	if output != "authenticated:"+mode {
		return fmt.Errorf("%s output=%q, want authenticated:%s", mode, output, mode)
	}
	fmt.Printf("%s: authenticated client and worker completed %s\n", mode, id)
	return nil
}

func authenticatedWorkflow(ctx *task.OrchestrationContext) (any, error) {
	var input string
	if err := ctx.GetInput(&input); err != nil {
		return nil, err
	}
	var output string
	if err := ctx.CallActivity("SampleAuthenticationEcho", task.WithActivityInput(input)).Await(&output); err != nil {
		return nil, err
	}
	return output, nil
}

func authenticatedEcho(ctx task.ActivityContext) (any, error) {
	var input string
	if err := ctx.GetInput(&input); err != nil {
		return nil, err
	}
	return "authenticated:" + input, nil
}
