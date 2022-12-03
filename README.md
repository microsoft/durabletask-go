# Durable Task Framework for Go

[![Build](https://github.com/microsoft/durabletask-go/actions/workflows/pr-validation.yml/badge.svg)](https://github.com/microsoft/durabletask-go/actions/workflows/pr-validation.yml)

The Durable Task Framework is a lightweight, embeddable engine for writing durable, fault-tolerant business logic (*orchestrations*) as ordinary code. The engine itself is written in Go and intended to be embedded into other Go-based processes. It exposes a gRPC endpoint to support writing durable flows in any language. There are currently SDKs that consume this gRPC endpoint for [.NET](https://github.com/microsoft/durabletask-dotnet) and [Java](https://github.com/microsoft/durabletask-java), with more to come. It's also possible to write orchestrations directly in Go and run them in the local process.

This project is largely a Go clone of the [.NET-based Durable Task Framework](https://github.com/Azure/durabletask), which is used by various cloud service teams at Microsoft for building reliable control planes and managing infrastructure. It also takes inspiration from the [Go Workflows](https://github.com/cschleiden/go-workflows) project, which itself is a Go project that borrows heavily from both the Durable Task Framework and [Temporal](https://github.com/temporalio/temporal). The main difference is that the Durable Task engine is designed to be used in sidecar architectures.

The Durable Task engine is also intended to be used as the basis for the [Dapr embedded workflow engine](https://github.com/dapr/dapr/issues/4576).

> This project is a work-in-progress and should not be used for production workloads. The public API surface is also not yet stable. The project itself is also in the very early stages and is missing some of the basics, such as contribution guidelines, etc.

## Storage providers

This project includes a [sqlite](https://sqlite.org/) storage provider for persisting app state to disk.

```go
// Persists state to a file named test.sqlite3. Use "" for in-memory storage.
options := sqlite.NewSqliteOptions("test.sqlite3")
be := sqlite.NewSqliteBackend(options, backend.DefaultLogger())
```

Additional storage providers can be created by extending the `Backend` interface.

## Creating the standalone gRPC sidecar

See the `main.go` file for an example of how to create a standalone gRPC sidecar that embeds the Durable Task engine. In short, you must create an `Backend` (for storage), an `Executor` (for executing user code), and host them as a `TaskHubWorker`.

The following code creates a `TaskHub` worker with sqlite `Backend` and a gRPC `Executor` implementations.

```go
// Use the default logger or provide your own
logger := backend.DefaultLogger()

// Configure the sqlite backend that will store the runtime state
sqliteOptions := sqlite.NewSqliteOptions(sqliteFilePath)
be := sqlite.NewSqliteBackend(sqliteOptions, logger)

// Create a gRPC server that the language SDKs will connect to
grpcServer := grpc.NewServer()
executor := backend.NewGrpcExecutor(grpcServer, be, logger)

// Construct and start the task hub worker object, which polls the backend for new work
orchestrationWorker := backend.NewOrchestrationWorker(be, executor, logger)
activityWorker := backend.NewActivityTaskWorker(be, executor, logger)
taskHubWorker := backend.NewTaskHubWorker(be, orchestrationWorker, activityWorker, logger)
taskHubWorker.Start(context.Background())

// Start listening.
lis, _ := net.Listen("tcp", "localhost:4001")
fmt.Printf("server listening at %v\n", lis.Addr())
grpcServer.Serve(lis)
```

Note that the Durable Task gRPC service implementation is designed to serve one client at a time, just like with any sidecar architecture. Scale out is achieved by adding new pod replicas that contain both the app process and the sidecar (connected to a common database).

### Language SDKs for gRPC

The Durable Task Framework for Go currently supports writing orchestrations in the following languages:

| Language/Stack | Package | Project Home | Samples |
| - | - | - | - |
| .NET | [![NuGet](https://img.shields.io/nuget/v/Microsoft.DurableTask.Client.svg?style=flat)](https://www.nuget.org/packages/Microsoft.DurableTask.Client/) | [GitHub](https://github.com/microsoft/durabletask-dotnet) | [Samples](https://github.com/microsoft/durabletask-dotnet/tree/main/samples) |
| Java | [![Maven Central](https://img.shields.io/maven-central/v/com.microsoft/durabletask-client?label=durabletask-client)](https://search.maven.org/artifact/com.microsoft/durabletask-client) | [GitHub](https://github.com/microsoft/durabletask-java) | [Samples](https://github.com/microsoft/durabletask-java/tree/main/samples/src/main/java/io/durabletask/samples) |

More language SDKs are planned to be added in the future. In particular, SDKs for Python and JavaScript/TypeScript. Anyone can theoretically create an SDK using a language that supports gRPC. However, there is not yet a guide for how to do this, so developers would need to reference existing SDK code as a reference. Starting with the Java implementation is recommended. The gRPC API is defined [here](https://github.com/microsoft/durabletask-protobuf).

## Embedded orchestrations

It's also possible to create orchestrations in Go and run them in the local process. You can find code samples in the [samples](./samples/) directory. The full set of Durable Task features is not yet available as part of the Go SDK, but will be added over time.

### Activity sequence example

Activity sequences like the following are the simplest and most common pattern used in the Durable Task Framework.

```go
// ActivitySequenceOrchestrator makes three activity calls in sequence and results the results
// as an array.
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

// SayHelloActivity can be called by an orchestrator function and will return a friendly greeting.
func SayHelloActivity(ctx task.ActivityContext) (any, error) {
	var input string
	if err := ctx.GetInput(&input); err != nil {
		return "", err
	}
	return fmt.Sprintf("Hello, %s!", input), nil
}
```

You can find the full sample [here](./samples/sequence.go).

### Fan-out / fan-in execution example

The next most common pattern is "fan-out / fan-in" where multiple activities are run in parallel, as shown in the snippet below (note that the `GetDevicesToUpdate` and `UpdateDevice` activity definitions are left out of the snippet below for brevity):

```go
// UpdateDevicesOrchestrator is an orchestrator that runs activities in parallel
func UpdateDevicesOrchestrator(ctx *task.OrchestrationContext) (any, error) {
	// Get a dynamic list of devices to perform updates on
	var devices []string
	if err := ctx.CallActivity(GetDevicesToUpdate, nil).Await(&devices); err != nil {
		return nil, err
	}

	// Start a dynamic number of tasks in parallel, not waiting for any to complete (yet)
	tasks := make([]task.Task, 0, len(devices))
	for _, id := range devices {
		tasks = append(tasks, ctx.CallActivity(UpdateDevice, id))
	}

	// Now that all are started, wait for them to complete and then return the success rate
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

The full sample can be found [here](./samples/parallel.go).

### Managing local orchestrations

The following code snippet provides an example of how you can configure and run orchestrations. The `TaskRegistry` type allows you to register orchestrator and activity functions, and the `TaskHubClient` allows you to start, query, terminate, and wait for orchestrations to complete.

The code snippet below demonstrates how to register and start a new instance of the `ActivitySequenceOrchestrator` orchestrator and wait for it to complete. The initialization of the client and worker are left out for brevity.

```go
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
```

Each sample linked above has a full implementation you can use as a reference.

## Distributed tracing support

The Durable Task Framework for Go supports publishing distributed traces to any configured [Open Telemetry](https://opentelemetry.io/)-compatible exporter. Simply use [`otel.SetTracerProvider(tp)`](https://pkg.go.dev/go.opentelemetry.io/otel#SetTracerProvider) to register a global `TracerProvider` as part of your application startup and the task hub worker will automatically use it to emit OLTP trace spans.

The following example code shows how you can configure distributed trace collection with [Zipkin](https://zipkin.io/), a popular open source distributed tracing system. The example assumes Zipkin is running locally, as shown in the code.

```go
func ConfigureZipkinTracing() *trace.TracerProvider {
	// Inspired by this sample: https://github.com/open-telemetry/opentelemetry-go/blob/main/example/zipkin/main.go
	exp, err := zipkin.New("http://localhost:9411/api/v2/spans")
	if err != nil {
		log.Fatal(err)
	}

	// NOTE: The simple span processor is not recommended for production.
	//       Instead, the batch span processor should be used for production.
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
	return tp
}
```

You can find this code in the [distributedtracing.go](./samples/distributedtracing.go) sample. The following is a screenshot showing the trace for the sample's orchestration, which calls an activity, creates a 2-second durable timer, and uses another activity to make an HTTP request to bing.com:

![image](https://user-images.githubusercontent.com/2704139/205171291-8d12d6fe-5d4f-40c7-9a48-2586a4c4af49.png)

Note that each orchestration is represented as a single span with activities, timers, and sub-orchestrations as child spans. The generated spans contain a variety of attributes that include information such as orchestration instance IDs, task names, task IDs, etc.

## Cloning this repository

This repository contains submodules. Be sure to clone it with the option to include submodules. Otherwise you will not be able to generate the protobuf code.

```bash
git clone --recurse-submodules https://github.com/microsoft/durabletask-go 
```

## Building the project

This project requires go v1.18.x or greater. You can build a standalone executable by simply running `go build` at the project root.

### Generating protobuf

Use the following command to regenerate the protobuf from the submodule. Use this whenever updating the submodule reference.

```bash
# NOTE: assumes the .proto file defines: option go_package = "/internal/protos"
protoc --go_out=. --go-grpc_out=. -I submodules/durabletask-protobuf/protos orchestrator_service.proto
```

### Generating mocks for testing

Test mocks were generated using [mockery](https://github.com/vektra/mockery). Use the following command at the project root to regenerate the mocks.

```bash
mockery --dir ./backend --name="^Backend|^Executor|^TaskWorker" --output ./tests/mocks --with-expecter
```

## Running tests

All automated tests are under `./tests`. A separate test package hierarchy was chosen intentionally to prioritize [black box testing](https://en.wikipedia.org/wiki/Black-box_testing). This strategy also makes it easier to catch accidental breaking API changes.

Run tests with the following command.

```bash
go test ./tests -coverpkg ./api,./task,./backend/...,./internal/helpers
```

## Running integration tests

You can run pre-built container images to run full integration tests against the durable task host over gRPC.

### .NET Durable Task client SDK tests

Use the following docker command to run tests against a running worker.

```bash
docker run -e GRPC_HOST="host.docker.internal" cgillum/durabletask-dotnet-tester:0.5.0-beta
```

Note that the test assumes the gRPC server can be reached over `localhost` on port `4001` on the host machine. These values can be overridden with the following environment variables:

* `GRPC_HOST`: Use this to change from the default `127.0.0.1` to some other value, for example `host.docker.internal`.
* `GRPC_PORT`: Set this environment variable to change the default port from `4001` to something else.

If successful, you should see output that looks like the following:

```
Test run for /root/out/bin/Debug/Microsoft.DurableTask.Tests/net6.0/Microsoft.DurableTask.Tests.dll (.NETCoreApp,Version=v6.0)
Microsoft (R) Test Execution Command Line Tool Version 17.3.1 (x64)
Copyright (c) Microsoft Corporation.  All rights reserved.

Starting test execution, please wait...
A total of 1 test files matched the specified pattern.
[xUnit.net 00:00:00.00] xUnit.net VSTest Adapter v2.4.3+1b45f5407b (64-bit .NET 6.0.10)
[xUnit.net 00:00:00.82]   Discovering: Microsoft.DurableTask.Tests
[xUnit.net 00:00:00.90]   Discovered:  Microsoft.DurableTask.Tests
[xUnit.net 00:00:00.90]   Starting:    Microsoft.DurableTask.Tests
  Passed Microsoft.DurableTask.Tests.OrchestrationPatterns.ExternalEvents(eventCount: 100) [6 s]
  Passed Microsoft.DurableTask.Tests.OrchestrationPatterns.ExternalEvents(eventCount: 1) [309 ms]
  Passed Microsoft.DurableTask.Tests.OrchestrationPatterns.LongTimer [8 s]
  Passed Microsoft.DurableTask.Tests.OrchestrationPatterns.SubOrchestration [1 s]
  ...
  Passed Microsoft.DurableTask.Tests.OrchestrationPatterns.ActivityFanOut [914 ms]
[xUnit.net 00:01:01.04]   Finished:    Microsoft.DurableTask.Tests
  Passed Microsoft.DurableTask.Tests.OrchestrationPatterns.SingleActivity_Async [365 ms]

Test Run Successful.
Total tests: 33
     Passed: 33
 Total time: 1.0290 Minutes
```

## Running locally

You can run the engine locally by pressing `F5` in [Visual Studio Code](https://code.visualstudio.com/) (the recommended editor). You can also simply run `go run main.go` to start a local Durable Task gRPC server that listens on port 4001.

```bash
go run main.go --port 4001 --db ./test.sqlite3
```

The following is the expected output:

```
2022/09/14 17:26:50 backend started: sqlite::./test.sqlite3
2022/09/14 17:26:50 server listening at 127.0.0.1:4001
2022/09/14 17:26:50 orchestration-processor: waiting for new work items...
2022/09/14 17:26:50 activity-processor: waiting for new work items...
```

At this point you can use one of the [language SDKs](#language-sdks) mentioned earlier in a separate process to implement and execute durable orchestrations. Those SDKs will connect to port `4001` by default to interact with the Durable Task engine.

## Contributing

This project welcomes contributions and suggestions.  Most contributions require you to agree to a
Contributor License Agreement (CLA) declaring that you have the right to, and actually do, grant us
the rights to use your contribution. For details, visit https://cla.opensource.microsoft.com.

When you submit a pull request, a CLA bot will automatically determine whether you need to provide
a CLA and decorate the PR appropriately (e.g., status check, comment). Simply follow the instructions
provided by the bot. You will only need to do this once across all repos using our CLA.

This project has adopted the [Microsoft Open Source Code of Conduct](https://opensource.microsoft.com/codeofconduct/).
For more information see the [Code of Conduct FAQ](https://opensource.microsoft.com/codeofconduct/faq/) or
contact [opencode@microsoft.com](mailto:opencode@microsoft.com) with any additional questions or comments.

## Trademarks

This project may contain trademarks or logos for projects, products, or services. Authorized use of Microsoft trademarks or logos is subject to and must follow [Microsoft's Trademark & Brand Guidelines](https://www.microsoft.com/legal/intellectualproperty/trademarks/usage/general).
Use of Microsoft trademarks or logos in modified versions of this project must not cause confusion or imply Microsoft sponsorship.
Any use of third-party trademarks or logos are subject to those third-party's policies.
