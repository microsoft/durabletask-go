# Durable Task SDK for Go

[![Build](https://github.com/microsoft/durabletask-go/actions/workflows/pr-validation.yml/badge.svg)](https://github.com/microsoft/durabletask-go/actions/workflows/pr-validation.yml)

This SDK lets you write reliable business logic in Go. You write the logic as normal Go code. The SDK calls this logic an *orchestration*.

You write the orchestrations, activities, and entities. [Azure Durable Task Scheduler](https://learn.microsoft.com/azure/azure-functions/durable/durable-task-scheduler/durable-task-scheduler) (DTS) keeps the durable state. DTS also dispatches the work and recovers the work after a failure.

DTS is the only supported runtime. This SDK does not include a storage backend.

The SDK gives you two connections:

- A **management client** starts orchestrations and reads their state.
- A **worker** runs your registered orchestrators, activities, and entities.

## Features

The features are in three groups. Group 1 has the most important features. Start with group 1.

### Group 1: Basic features

You need these features for almost all applications.

| Feature | Description |
| --- | --- |
| Orchestrations | Write a workflow as a Go function. DTS saves the progress after each step. |
| Activities | Call a function that does work with side effects, for example an HTTP request. |
| Retries and failure handling | Retry a failed task with a policy. Read the cause from typed errors. |
| Orchestration management | Start, query, wait for, terminate, suspend, resume, and purge an orchestration. |

### Group 2: Common patterns

Most applications use one or more of these patterns.

| Feature | Description |
| --- | --- |
| Fan-out and fan-in | Start many activities in parallel. Then wait for all of the results. |
| Durable timers | Wait for a time period. The timer stays correct after a restart. |
| External events | Stop the orchestration until an external system sends an event. |
| Sub-orchestrations | Call one orchestration from a different orchestration. |
| Durable entities | Keep addressable state. An entity does its operations one at a time. |

### Group 3: Advanced features

Use these features for large systems or for special conditions.

| Feature | Description |
| --- | --- |
| Task versions | Register more than one implementation under the same logical name. |
| Data converters | Replace the default JSON serialization of your payloads. |
| Large payloads | Keep large payloads in Azure Blob Storage and send a reference token. |
| Tags and trace context | Attach user tags to actions. Send the caller trace context to DTS. |
| Distributed tracing | Emit OpenTelemetry spans from your process. DTS emits the durable spans. |
| Replay-hazard analysis | Find nondeterministic orchestrator code with `cmd/orchestratorvet`. |
| History export (preview) | Copy terminal orchestration histories to Azure Blob Storage. |

## Quick start

This project needs Go 1.25 or later.

1. Start the [DTS emulator](https://learn.microsoft.com/azure/azure-functions/durable/durable-task-scheduler/quickstart-durable-task-scheduler). Use any OCI runtime:

   ```bash
   docker run -d -p 8080:8080 -p 8082:8082 \
     -e DTS_TASK_HUB_NAMES=default \
     mcr.microsoft.com/dts/dts-emulator:latest
   ```

   The dashboard runs on port `8082`.

2. Set the connection string:

   ```bash
   export DTS_CONNECTION_STRING="Endpoint=http://localhost:8080;TaskHub=default;Authentication=None"
   ```

3. Run a sample:

   ```bash
   go run ./samples/durabletaskscheduler
   ```

## Connection to DTS

The [`durabletaskscheduler`](./durabletaskscheduler) package is the integration surface. The package validates your connection string and supplies Azure token credentials. It also owns the management connection and the worker connection separately. The worker stream recovers after a network failure.

```go
options, err := durabletaskscheduler.NewOptionsFromConnectionString(
    os.Getenv("DTS_CONNECTION_STRING"))
if err != nil {
    return err
}

registry := task.NewTaskRegistry()
if err := registry.AddOrchestratorN("ActivitySequence", ActivitySequence); err != nil {
    return err
}
if err := registry.AddActivityN("SayHello", SayHello); err != nil {
    return err
}

logger := api.DefaultLogger()

// The management client schedules orchestrations and reads their state.
client, err := durabletaskscheduler.NewClient(ctx, options, logger)
if err != nil {
    return err
}
defer client.Close()

// The worker executes the registered orchestrators and activities.
worker, err := durabletaskscheduler.NewWorker(
    options, registry, logger, durabletaskclient.WithAutoWorkItemFilters())
if err != nil {
    return err
}
if err := worker.Start(ctx); err != nil {
    return err
}
defer worker.Shutdown(ctx)

id, err := client.ScheduleNewOrchestration(ctx, "ActivitySequence")
if err != nil {
    return err
}
metadata, err := client.WaitForOrchestrationCompletion(ctx, id)
```

In this example, `durabletaskclient` is the import alias for the [`client`](./client) package.

Management clients also read the orchestration history and the recurring interval schedules. These APIs do not expose the generated protobuf messages.

For more information, read the [DTS transport guide and feature matrix](./durabletaskscheduler/README.md).

## Samples

Each sample connects to the task hub in `DTS_CONNECTION_STRING`. Set that variable first. Then run `go run ./samples/<name>`.

| Sample | Description |
| --- | --- |
| [durabletaskscheduler](./samples/durabletaskscheduler) | Connect to DTS and call activities in sequence. |
| [parallel](./samples/parallel) | Run activities in parallel. |
| [externalevents](./samples/externalevents) | Wait for an event from an external system. |
| [retries](./samples/retries) | Retry an activity after a failure. |
| [entity](./samples/entity) | Use durable entities. |
| [exporthistory](./samples/exporthistory) | Export orchestration histories to Azure Blob Storage. |
| [distributedtracing](./samples/distributedtracing) | Send trace data to an OpenTelemetry collector. |

Two samples need more steps:

- `distributedtracing` is a separate Go module. Run it with `cd samples/distributedtracing && go run .`.
- `exporthistory` also needs `EXPORT_STORAGE_CONNECTION_STRING`. It accepts the optional variable `EXPORT_CONTAINER`.

Most samples use the shared helper [`samples/internal/dtssample`](./samples/internal/dtssample). The helper reads `DTS_CONNECTION_STRING`, opens the client, and starts the worker. The `exporthistory` sample does not use the helper. That sample needs the client before it registers the export system tasks.

## Orchestrations in Go

### Pattern 1: Activity sequence

An activity sequence is the most simple pattern. The orchestrator calls the activities one after the other.

```go
// ActivitySequenceOrchestrator makes three activity calls in sequence.
// It returns the results as an array.
func ActivitySequenceOrchestrator(ctx *task.OrchestrationContext) (any, error) {
	var helloTokyo string
	if err := ctx.CallActivity("SayHello", task.WithActivityInput("Tokyo")).Await(&helloTokyo); err != nil {
		return nil, err
	}
	var helloLondon string
	if err := ctx.CallActivity("SayHello", task.WithActivityInput("London")).Await(&helloLondon); err != nil {
		return nil, err
	}
	var helloSeattle string
	if err := ctx.CallActivity("SayHello", task.WithActivityInput("Seattle")).Await(&helloSeattle); err != nil {
		return nil, err
	}
	return []string{helloTokyo, helloLondon, helloSeattle}, nil
}

// SayHelloActivity returns a greeting. An orchestrator function can call it.
func SayHelloActivity(ctx task.ActivityContext) (any, error) {
	var input string
	if err := ctx.GetInput(&input); err != nil {
		return "", err
	}
	return fmt.Sprintf("Hello, %s!", input), nil
}
```

Full sample: [samples/durabletaskscheduler](./samples/durabletaskscheduler).

### Pattern 2: Fan-out and fan-in

In this pattern, the orchestrator starts many activities in parallel. Then it waits for all of the results. This example does not show the `GetDevicesToUpdate` and `UpdateDevice` activities.

```go
// UpdateDevicesOrchestrator runs activities in parallel.
func UpdateDevicesOrchestrator(ctx *task.OrchestrationContext) (any, error) {
	// Get a dynamic list of devices to update.
	var devices []string
	if err := ctx.CallActivity("GetDevicesToUpdate").Await(&devices); err != nil {
		return nil, err
	}

	// Start a dynamic number of tasks in parallel. Do not wait for the results now.
	tasks := make([]task.Task, len(devices))
	for i, id := range devices {
		tasks[i] = ctx.CallActivity("UpdateDevice", task.WithActivityInput(id))
	}

	// All tasks are started. Wait for the results and calculate the success rate.
	successCount := 0
	for _, task := range tasks {
		var succeeded bool
		if err := task.Await(&succeeded); err == nil && succeeded {
			successCount++
		}
	}

	return float32(successCount) / float32(len(devices)), nil
}
```

Full sample: [samples/parallel](./samples/parallel).

### Pattern 3: External events

An orchestration can wait for input from an external system. For example, an approval workflow waits for a signal from a user. Use the `WaitForSingleEvent` method to stop the orchestration until the event comes.

Set a timeout value to limit the wait time. Use `-1` for an infinite timeout.

```go
// ExternalEventOrchestrator waits 30 seconds for a "Name" event.
func ExternalEventOrchestrator(ctx *task.OrchestrationContext) (any, error) {
	var nameInput string
	if err := ctx.WaitForSingleEvent("Name", 30*time.Second).Await(&nameInput); err != nil {
		// The timeout expired.
		return nil, err
	}

	return fmt.Sprintf("Hello, %s!", nameInput), nil
}
```

Use the `RaiseEvent` method of the client to send an event.

```go
id, _ := client.ScheduleNewOrchestration(ctx, "ExternalEventOrchestrator")

// Ask the user for a name and send the name to the orchestrator.
go func() {
	fmt.Println("Enter your first name: ")
	var nameInput string
	fmt.Scanln(&nameInput)

	client.RaiseEvent(ctx, id, "Name", api.WithEventPayload(nameInput))
}()
```

DTS keeps each event in the orchestration state. The orchestration reads the event when it calls `WaitForSingleEvent` with the same event name.

Two rules control the order:

- If more than one live wait uses the same event name, the newest wait gets the next event. This rule is last in, first out. It agrees with the Durable Task .NET replay contract.
- If an event arrives before any wait, DTS keeps the event. The orchestration then reads these events in arrival order. This rule is first in, first out.

Full sample: [samples/externalevents](./samples/externalevents).

### Pattern 4: Retries and failure handling

An activity or a sub-orchestration that fails returns a `*task.TaskFailedError`. An entity call that fails returns a `*task.EntityOperationFailedError`. Both errors contain an `api.FailureDetails` value. This value holds the error type, the message, the stack trace, the inner failure, the non-retriable marker, and the custom properties.

```go
err := ctx.CallActivity("ChargeCard").Await(nil)
var failed *task.TaskFailedError
if errors.As(err, &failed) {
	fmt.Printf("%s failed: %s\n", failed.TaskName, failed.FailureDetails)
}
```

Attach a retry policy to call a failed task again:

```go
policy := &task.RetryPolicy{
	MaxAttempts:          3,
	InitialRetryInterval: time.Second,
	Handle: func(retry task.RetryContext) bool {
		return !retry.LastFailure.IsCausedBy(api.ErrorTypeActivityTaskNotFound)
	},
}
```

Obey these rules when you write a retry handler:

- The handler runs again during replay. Do not do I/O in the handler. Do not read the wall clock in the handler.
- The retry options copy the policy when you make the option. Validation does not change your policy value.
- If the next backoff goes past `RetryTimeout`, the SDK stops the retries. The SDK makes this decision from the failure event, so the result is stable across replay and redelivery.
- Some failures are non-retriable and skip your handler. Missing activity registrations, missing entity registrations, and version mismatches are non-retriable.

`OrchestrationContext.Context()` is separate from the host context of the worker. It holds only the persisted orchestration identity and context fields. It never holds host values, deadlines, or cancellation. Use durable timers and task cancellation scopes to control the orchestration. Use `ctx.Logger()` for replay-safe logging. Use the activity context for host cancellation and for outbound I/O.

Full sample: [samples/retries](./samples/retries).

### Orchestration management

Use a `TaskRegistry` to register your orchestrator, activity, and entity functions. Then use the client from `durabletaskscheduler.NewClient` to control the orchestrations.

```go
r := task.NewTaskRegistry()
r.AddOrchestratorN("ActivitySequence", ActivitySequenceOrchestrator)
r.AddActivityN("SayHello", SayHelloActivity)

id, err := client.ScheduleNewOrchestration(ctx, "ActivitySequence")
if err != nil {
  panic(err)
}

metadata, err := client.WaitForOrchestrationCompletion(ctx, id)
if err != nil {
  panic(err)
}

fmt.Printf("orchestration completed: %v\n", metadata)
```

This example does not show the client and worker setup. Read [Connection to DTS](#connection-to-dts) for that code.

The client also does these operations, if the connected service supplies them:

- Query the instances with a limit. List the instance IDs.
- Restart an orchestration.
- Rewind a failed orchestration without repeating successful activities.
- Purge in a batch or with a filter.
- Terminate an orchestration.
- Read the tags and the worker capabilities.

Provision and delete task hubs through the Azure control plane or Azure CLI.
The SDK does not expose task-hub lifecycle or skip-graceful-termination
operations.

`RewindInstance(ctx, id, api.WithRewindReason("dependency repaired"))` only
enqueues recovery. Observe a **new execution ID** and its final status separately;
an immediate completion wait can still return the previous failed execution.
The worker replaces failed history using the same protocol as the Python SDK,
and DTS recursively rewinds failed children. Activity retry-policy histories
containing retry timers are not supported by the pinned Python rewind algorithm
or this implementation. See the [rewind sample](./samples/rewind) and
[rewind behavior and limitations](./durabletaskscheduler/README.md#rewind).

To read a long history, use `StreamOrchestrationHistory`. This method reads the history one part at a time. If you buffer the history instead, the SDK applies a validated event cap.

## Durable entities

A durable entity is an addressable object that holds state. An entity does its operations one at a time.

```go
type Counter struct {
	task.EntityObjectBase[CounterState]
}

type CounterState struct {
	Value int `json:"value"`
}

func (counter *Counter) Add(amount int) {
	counter.State().Value += amount
}

func (counter *Counter) Get() int {
	return counter.State().Value
}

counterFactory := task.NewEntityObjectFactory[CounterState, *Counter](
	func(task.EntityFactoryContext) (*Counter, error) {
		return new(Counter), nil
	},
)
registry.AddEntityFactoryN("counter", counterFactory)
counter := api.NewEntityID("counter", "orders")

client.SignalEntity(ctx, counter, "Add", api.WithSignalInput(1))

registry.AddOrchestratorN("read-counter", func(ctx *task.OrchestrationContext) (any, error) {
	var value int
	err := ctx.CallEntity(counter, "Get").Await(&value)
	return value, err
})
```

The SDK supports raw entity functions, state-struct dispatch, and persistent entity objects with separate durable state. Entity factories run once per operation batch, may run concurrently across batches, and can capture dependencies. A factory can return batch cleanup, and an entity object can implement `task.EntityBatchCloser`. Shared functions registered with `AddEntityN` must be thread-safe. Entity IDs use the compact JSON form `"@name@key"`. Reflected operations can use `task.OptionalEntityInput[T]` when input is optional.

The SDK also supports scheduled signals, orchestration calls, entity-to-entity signals, queries, cleanup, and ordered critical sections across more than one entity.

Entity names and operation names are matched case-insensitively using the same invariant rule as the .NET SDK, so a name resolves to the same entity in both SDKs.

The DTS worker accepts the legacy `EntityBatchRequest` work item and the current `EntityRequestV2` work item.

Full sample: [samples/entity](./samples/entity).

## Task versions

An orchestrator or an activity can have more than one implementation under the same logical name. The registry identity contains the name and the version. The registry ignores letter case.

```go
registry.AddOrchestratorNVersion("orders", "v1", ordersV1)
registry.AddOrchestratorNVersion("orders", "v2", ordersV2)
registry.AddActivityNVersion("charge", "v2", chargeV2)
```

Dispatch obeys these rules:

- An exact match of the name and the version has priority.
- A versioned request uses an unversioned registration only if the logical name has no versioned registrations.
- An activity uses the version of its parent orchestration. To select a different version, supply `task.WithActivityVersion`. To request the unversioned activity, supply `""`.
- A sub-orchestration uses `VersioningOptions.DefaultVersion`. You can override this value.
- `task.WithContinueAsNewVersion` moves the next execution to a different version. This change happens at a deterministic ContinueAsNew boundary. To go back to an unversioned registration, use `task.UnversionedTaskVersion`.

Set `durabletaskscheduler.Options.Versioning` to configure the management client and the worker together. The reject and fail strategies for a version mismatch stay available for rolling deployments.

## Data converters

`api.DataConverter` controls how the SDK serializes your payloads. The default converter is `api.JSONDataConverter`. It keeps the existing `encoding/json` wire format.

Set `durabletaskscheduler.Options.DataConverter` one time. The SDK gives the converter to the management client and to the worker.

The converter applies to the typed payloads. These payloads include the orchestration, activity, entity, event, status, management, metadata, and ContinueAsNew values.

The converter does not apply to these values:

- The `WithRaw*` options.
- The serialized metadata fields.
- The failure metadata.
- The large-payload reference descriptors.

The SDK does not save the identity of the converter. A new converter must continue to decode the payloads from your earlier deployments.

## Tags, trace context, and turn limits

Use `api.WithTags`, `task.WithActivityTags`, and `task.WithSubOrchestrationTags` to attach user tags. An activity and a sub-orchestration inherit the tags of the parent orchestration. A tag on the action has priority over an inherited tag. The completion actions carry the current tags, so ContinueAsNew keeps them.

The client sends the sampled caller trace context when it schedules an orchestration or signals an entity. The worker adds separate action trace contexts for the service-owned activity and sub-orchestration spans. The worker does not emit duplicate local Durable Task spans. A legacy entity operation request also sends its operation trace context to the entity actions. The current DTS V2 entity request does not carry that source field.

Use `task.OrchestrationOptions.MaxEventsPerTurn` to limit the new events in one turn. If the worker uses only part of a batch, it sets `numEventsProcessed`. DTS then keeps the remaining events for the next replay. This count obeys the DTS work-item rules. The orchestration control markers do not count against the limit.

To control instance-ID reuse, configure `api.OrchestrationIDReusePolicy.DedupeStatuses`:

| Value | Result |
| --- | --- |
| A status in the list | The new start is rejected. |
| A status that is not in the list | DTS can terminate the instance and replace it. |
| A nil slice | The service default applies. |
| An empty non-nil slice | Replacement is permitted for every reusable status. |

## Large payloads

To externalize large payloads, set `durabletaskscheduler.Options.LargePayloads` to an `*api.LargePayloadOptions` value. This value holds an `api.LargePayloadStore` that saves the bytes. It also holds an `api.LargePayloadResolver` that reads the bytes back. The SDK applies the option to the management client and to the worker.

The [`payload`](./payload) package includes Azure Blob Storage support. It emits the same self-describing `blob:v2` tokens as the .NET SDK.

## Distributed tracing

The SDK sends the W3C trace context of a sampled caller when it schedules an orchestration. DTS owns the spans for the orchestrations, activities, timers, and sub-orchestrations. Your application code can use standard [OpenTelemetry](https://opentelemetry.io/) instrumentation. Use it for caller spans, custom activity spans, and outbound dependencies.

When DTS supplies an activity trace parent, the SDK restores it on
`ActivityContext.Context()` without creating another durable-operation span.
Application instrumentation can therefore attach outbound requests to the
service-owned trace.

This example sends the traces of your process to an [OpenTelemetry Collector](https://opentelemetry.io/docs/collector/) over OTLP/HTTP. Configure the DTS telemetry separately for the service-owned spans.

```go
func ConfigureOTLPTracing(ctx context.Context) (*trace.TracerProvider, error) {
	exp, err := otlptracehttp.New(
		ctx,
		otlptracehttp.WithEndpoint("localhost:4318"),
		otlptracehttp.WithInsecure(),
	)
	if err != nil {
		return nil, err
	}

	// NOTE: Do not use the simple span processor in production.
	//       Use the batch span processor in production.
	processor := trace.NewSimpleSpanProcessor(exp)
	// processor := trace.NewBatchSpanProcessor(exp)

	tp := trace.NewTracerProvider(
		trace.WithSpanProcessor(processor),
		trace.WithSampler(trace.AlwaysSample()),
		trace.WithResource(resource.NewWithAttributes(
			"durabletask.io",
			attribute.KeyValue{Key: "service.name", Value: attribute.StringValue("sample-app")},
		)),
	)
	otel.SetTracerProvider(tp)
	return tp, nil
}
```

The [distributed tracing sample](./samples/distributedtracing) starts a caller span before `ScheduleNewOrchestration`. DTS then joins its service-side spans to the same trace. The sample also instruments an HTTP request from an activity.

## History export (preview)

> [!WARNING]
> This package is a preview. The exported API can change without a major version. The saved shape of the `ExportJob` entity state and the names of the system tasks can also change. Extended sessions are not supported.

The [`exporthistory`](./exporthistory) package copies terminal orchestration histories to Azure Blob Storage. The output format is gzip-compressed JSONL.

The package uses a durable entity for the job state. It also uses an operation orchestrator, an export orchestrator, and two activities. One activity lists the instances. The other activity exports them.

```go
store, err := exporthistory.NewAzureBlobHistoryStore(exporthistory.AzureBlobHistoryStoreOptions{
    ConnectionString: storageConnectionString,
    ContainerName:    "history-exports",
})
err = exporthistory.Register(registry, exporthistory.WorkerOptions{
    Source: taskHubClient, // supplies ListInstanceIDs and orchestration history
    Store:  store,
})
worker, err := durabletaskscheduler.NewWorker(options, registry, logger,
    durabletaskclient.WithAutoWorkItemFilters(),
    exporthistory.WithExportHistory(),
)

exportClient, err := exporthistory.NewClient(taskHubClient, exporthistory.ClientOptions{
    ContainerName: "history-exports",
})
job, err := exportClient.CreateJob(ctx, exporthistory.JobCreationOptions{
    Mode:              exporthistory.ExportModeBatch,
    CompletedTimeFrom: from,
    CompletedTimeTo:   to,
})
description, err := job.Describe(ctx)
```

The package supports batch jobs and continuous jobs. It also supports durable checkpoints, terminal-status filters, and a per-batch instance limit. It collects the failures. It returns typed errors for validation, not-found, and invalid-transition conditions.

Each successful `Create` reserves a new generation-specific orchestration ID, exposed as `description.OrchestratorInstanceID`. Only the current generation can change job state, though older work can briefly remain in flight. Recreation retains previous generations' orchestration histories. `Delete` captures the ID of the generation it removes and cleans only that ID, so concurrent recreation is safe. Legacy export jobs and mixed .NET/older Go export-history workers are not supported.

Each exported object is an opaque gzip file. The name ends with `.jsonl.gz`. The content type is `application/gzip`. The object has no `Content-Encoding` header. Every reader therefore gets the bytes that the object name promises.

For more information, read the [export history guide](./exporthistory/README.md). See also the [sample](./samples/exporthistory).

## Development

### Clone the repository

```bash
git clone https://github.com/microsoft/durabletask-go
```

The protocol buffer definitions are in [`vendored/durabletask-protobuf/protos`](./vendored/durabletask-protobuf/protos). To get a new version from the upstream [microsoft/durabletask-protobuf](https://github.com/microsoft/durabletask-protobuf) repository, read [`vendored/durabletask-protobuf/README.md`](./vendored/durabletask-protobuf/README.md).

### Build the project

This project needs Go 1.25 or later. This project is a library. To build all of the packages, run this command in the project root:

```bash
go build ./...
```

### Generate the protobuf code

Run this command after you change the `.proto` file:

```bash
# NOTE: The .proto file must define: option go_package = "/internal/protos"
protoc --go_out=. --go-grpc_out=. -I vendored/durabletask-protobuf/protos orchestrator_service.proto
```

### Run the tests

The package tests are beside the code that they exercise. The `./tests` directory holds [black box tests](https://en.wikipedia.org/wiki/Black-box_testing). These tests find accidental changes to the public API. The `./tests` directory holds deterministic runtime tests that use hand-built histories. The `./tests/durabletaskscheduler` directory holds the end-to-end tests for a live scheduler.

Run the full suite with these commands:

```bash
go test ./... -count=1 -coverpkg=./api,./task,./client,./durabletaskscheduler,./exporthistory,./payload,./internal/contextprop,./internal/failure,./internal/grpcerrors,./internal/helpers,./internal/historyconv,./internal/largepayload,./internal/tagcodec
(cd cmd/orchestratorvet && go test ./...)
(cd samples/distributedtracing && go test ./...)
```

The DTS tests and the Azurite tests skip themselves if their environment variables are empty. Read [Run the services locally](#run-the-services-locally) for these variables. PR validation runs the full suite against both services. It then runs the suite again with the race detector on the most recent supported Go version.

### Test doubles

This repository has no generated mock package. DTS is the only supported runtime, so the useful fakes are the generated gRPC surfaces. The tests use small hand-written fakes:

- The client tests embed `protos.UnimplementedTaskHubSidecarServiceServer`. They serve it over [`bufconn`](https://pkg.go.dev/google.golang.org/grpc/test/bufconn). Other client tests embed `protos.TaskHubSidecarServiceClient` and override only the RPCs under test. `TaskHubSidecarService` is the generated name of the DTS gRPC service. The name does not imply a sidecar deployment.
- The worker tests and the executor tests implement `task.Executor` and `task.EntityExecutor`.
- The orchestration tests supply hand-built histories to `task.NewTaskExecutor`.

### Check the orchestrators for replay hazards

DTS replays your orchestrator code from the history on each turn. Your orchestrator must therefore be deterministic and free of side effects.

`cmd/orchestratorvet` is a driver for the `orchestratorgo` analyzer. The driver is compatible with [`go vet`](https://pkg.go.dev/cmd/vet). The analyzer examines the orchestrators that a package registers with `task.TaskRegistry`.

```bash
cd cmd/orchestratorvet
go build -o ../../bin/orchestratorvet .
cd ../..
go vet -vettool=$PWD/bin/orchestratorvet ./...
```

The analyzer starts at each `AddOrchestrator`, `AddOrchestratorN`, `AddOrchestratorVersion`, and `AddOrchestratorNVersion` call that it can resolve. It then follows the call graph. The graph includes named functions and methods in the same package. It also includes resolvable function variables and nested function literals.

The analyzer reports these hazards:

- Wall-clock reads and host timers.
- Nondeterministic identifier sources and random sources.
- Unsafe parallelism and synchronization.
- Direct filesystem, network, process, and environment I/O.
- Replay-unsafe logging.
- Unbounded loops that it proves do not make progress.
- Task names that a complete registration set proves are missing.
- Registration forms that `task.TaskRegistry` rejects or that give an unstable name.

The analyzer supplies an `analysis.SuggestedFix` for `time.Now()` and for `go func() { ... }()`. The fixes change them to `ctx.CurrentTimeUtc` and `ctx.Go`. Both `gopls` and `go vet -fix` can apply these fixes. The other diagnostics have no fix, because no single rewrite is always correct.

The analyzer reports only the hazards that it proves. It stays silent for all other code. You can therefore enable it on an existing codebase without many diagnostics.

The analyzer ignores the files that end in `_test.go`. Tests frequently register invalid or nondeterministic orchestrators on purpose. To include these files, use the `-orchestratorgo.test-files` flag.

The analyzer does not examine activity bodies or entity bodies. It also ignores code that no registered orchestrator reaches.

For the full list of checks, the suggested fixes, the false-positive guards, and the limitations, read [`cmd/orchestratorvet/README.md`](cmd/orchestratorvet/README.md).

### Run the services locally

The DTS emulator supplies a local task hub. Use it for development and for the end-to-end tests. The blob payload tests and the history-export tests need Azurite. Start both services with any OCI runtime:

```bash
docker run -d -p 8080:8080 -p 8082:8082 \
  -e DTS_TASK_HUB_NAMES=default \
  mcr.microsoft.com/dts/dts-emulator:latest
docker run -d -p 10000:10000 mcr.microsoft.com/azure-storage/azurite:3.37.0
```

The end-to-end tests read these environment variables. The tests skip themselves if the variables are empty.

| Variable | Description |
| --- | --- |
| `DTS_CONNECTION_STRING` | A full connection string. This variable has priority over the two variables below. |
| `DTS_EMULATOR_ENDPOINT` | The gRPC endpoint of the emulator, for example `http://127.0.0.1:8080`. |
| `DTS_TASK_HUB` | The task hub name for `DTS_EMULATOR_ENDPOINT`. The default value is `default`. |
| `AZURITE_CONNECTION_STRING` | An Azure Storage connection string for the blob payload tests and the history-export tests. |

For `AZURITE_CONNECTION_STRING`, use the development credentials from the [Azurite documentation](https://learn.microsoft.com/azure/storage/common/storage-use-azurite#well-known-storage-account-and-key).

```bash
DTS_EMULATOR_ENDPOINT="http://127.0.0.1:8080" \
DTS_TASK_HUB="default" \
AZURITE_CONNECTION_STRING="DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=<azurite-account-key>;BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;" \
go test ./... -count=1
```

## Contributions

This project accepts contributions and suggestions. Most contributions need a Contributor License Agreement (CLA). The CLA declares that you have the right to give us the rights to your contribution. For more information, go to <https://cla.opensource.microsoft.com>.

When you send a pull request, a CLA bot examines the pull request. The bot tells you if you must supply a CLA. Obey the instructions from the bot. You supply the CLA one time only for all repositories that use our CLA.

This project uses the [Microsoft Open Source Code of Conduct](https://opensource.microsoft.com/codeofconduct/). For more information, read the [Code of Conduct FAQ](https://opensource.microsoft.com/codeofconduct/faq/). You can also send questions to [opencode@microsoft.com](mailto:opencode@microsoft.com).

## Trademarks

This project can contain trademarks or logos for projects, products, or services. You must obey [Microsoft's Trademark and Brand Guidelines](https://www.microsoft.com/legal/intellectualproperty/trademarks/usage/general) when you use Microsoft trademarks or logos. Do not use Microsoft trademarks or logos in a changed version of this project if the use causes confusion. Do not imply Microsoft sponsorship. Third-party trademarks and logos obey the policies of those third parties.
