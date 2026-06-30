# Azure-managed Durable Task Scheduler (DTS) for Go

This module connects the Durable Task Framework for Go to an **Azure-managed Durable Task
Scheduler (DTS)** task hub. It wraps the core `client.TaskHubGrpcClient` with the gRPC
metadata and authentication that DTS requires:

- the `taskhub` header identifying the target task hub,
- an `x-user-agent` header,
- a `workerid` header (for workers), and
- an `Authorization: Bearer <token>` header, with the token obtained and refreshed from an
  [`azcore.TokenCredential`](https://pkg.go.dev/github.com/Azure/azure-sdk-for-go/sdk/azcore#TokenCredential).

It is a separate Go module so that the core engine does not take a dependency on the Azure SDK.

This module exposes two distinct types, mirroring the other DTS SDKs: a `Client` that
schedules and manages orchestration instances, and a `Worker` that executes orchestrator and
activity functions. Neither can perform the other's role.

## Install

```bash
go get github.com/microsoft/durabletask-go/azuremanaged
```

## Connecting with a connection string

A DTS connection string has the form `Endpoint=<address>;Authentication=<type>;TaskHub=<name>`:

```go
client, err := azuremanaged.NewClientFromConnectionString(
    "Endpoint=myscheduler.westus2.durabletask.io;Authentication=DefaultAzure;TaskHub=myhub")

worker, err := azuremanaged.NewWorkerFromConnectionString(
    "Endpoint=myscheduler.westus2.durabletask.io;Authentication=DefaultAzure;TaskHub=myhub")
```

### Connection string properties

| Property | Required | Notes |
| --- | --- | --- |
| `Endpoint` | yes | DTS endpoint address. A scheme is optional; port defaults to `443`. |
| `Authentication` | yes | One of the authentication types below. |
| `TaskHub` | yes | Task hub name. |
| `ClientID` | no | Client ID for `ManagedIdentity` / `WorkloadIdentity`. |
| `TenantId` | no | Tenant ID for `WorkloadIdentity`. |
| `TokenFilePath` | no | Token file path for `WorkloadIdentity`. |
| `AdditionallyAllowedTenants` | no | Comma-separated tenants for `WorkloadIdentity`. |

### Authentication types

`DefaultAzure`, `ManagedIdentity`, `WorkloadIdentity`, `Environment`, `AzureCLI`,
`AzureDeveloperCLI`, and `None`. Values are case-insensitive and ignore spaces.

`None` attaches no bearer token and uses an insecure (non-TLS) channel — only appropriate for
the local emulator.

> Note: the .NET connection string also accepts `AzurePowerShell`, `VisualStudio`,
> `VisualStudioCode`, and `InteractiveBrowser`. The Go `azidentity` package does not provide
> equivalents for all of these, so they are not currently supported here.

## Connecting with explicit options

```go
credential, err := azidentity.NewDefaultAzureCredential(nil)
// handle err

opts := &azuremanaged.Options{
    Endpoint:   "myscheduler.westus2.durabletask.io",
    TaskHub:    "myhub",
    Credential: credential,
}

client, err := azuremanaged.NewClient(opts)
worker, err := azuremanaged.NewWorker(opts)
```

A `Worker` registers orchestrator/activity functions through a `task.TaskRegistry` and starts
processing work items with `Start`, which returns once the listener is connected and continues
on a background goroutine until the context is canceled:

```go
r := task.NewTaskRegistry()
// r.AddOrchestratorN(...) / r.AddActivityN(...)
if err := worker.Start(context.Background(), r); err != nil {
    // handle err
}
```

A `Client` schedules and manages instances:

```go
id, err := client.ScheduleNewOrchestration(context.Background(), "MyOrchestrator")
// ... WaitForOrchestrationCompletion, TerminateOrchestration, RaiseEvent, etc.
```

When `Credential` is `nil`, an insecure channel is used (local/emulator only). The token scope
is `<ResourceID>/.default`, where `ResourceID` defaults to `https://durabletask.io`.

## Local emulator

Point `Endpoint` at the emulator's gRPC address and use `Authentication=None`:

```go
client, err := azuremanaged.NewClientFromConnectionString(
    "Endpoint=localhost:8080;Authentication=None;TaskHub=default")
```

See [`example_test.go`](./example_test.go) for full client and worker examples.
