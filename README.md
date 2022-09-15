# Durable Task Framework for Go

[![Build](https://github.com/cgillum/durabletask-go/actions/workflows/pr-validation.yml/badge.svg)](https://github.com/cgillum/durabletask-go/actions/workflows/pr-validation.yml)

The Durable Task Framework is a lightweight, embeddable engine for writing durable, fault-tolerant business logic (*orchestrations*) as ordinary code. The engine itself is written in Go and intended to be embedded into other Go-based processes. It exposes a gRPC endpoint to support writing durable flows in any language. There are currently SDKs that consume this gRPC endpoint for [.NET](https://github.com/microsoft/durabletask-dotnet) and [Java](https://github.com/microsoft/durabletask-java), with more to come.

This project is largely a Go clone of the [.NET-based Durable Task Framework](https://github.com/Azure/durabletask), which is used by various cloud service teams at Microsoft for building reliable control planes and managing infrastructure. It also takes inspiration from the [Go Workflows](https://github.com/cschleiden/go-workflows) project, which itself is a Go project that borrows heavily from both the Durable Task Framework and [Temporal](https://github.com/temporalio/temporal). The main difference is that this engine is designed to be used in sidecar architectures.

This engine is also intended to be used as the basis for the [Dapr embedded workflow engine](https://github.com/dapr/dapr/issues/4576).

> This project is a work-in-progress and should not be used for production workloads. The public API surface is also not yet stable. The project itself is also in the very early stages and is missing some of the basics, such as contribution guidelines, etc.

## Language SDKs

The Durable Task Framework for Go currently supports writing orchestrations in the following languages:

| Language/Stack | Package | Project Home | Samples |
| - | - | - | - |
| .NET | [![NuGet](https://img.shields.io/nuget/v/Microsoft.DurableTask.Client.svg?style=flat)](https://www.nuget.org/packages/Microsoft.DurableTask.Client/) | [GitHub](https://github.com/microsoft/durabletask-dotnet) | [Samples](https://github.com/microsoft/durabletask-dotnet/tree/main/samples) |
| Java | [![Maven Central](https://img.shields.io/maven-central/v/com.microsoft/durabletask-client?label=durabletask-client)](https://search.maven.org/artifact/com.microsoft/durabletask-client) | [GitHub](https://github.com/microsoft/durabletask-java) | [Samples](https://github.com/microsoft/durabletask-java/tree/main/samples/src/main/java/io/durabletask/samples) |

More language SDKs are planned to be added in the future. In particular, SDKs for Python and JavaScript/TypeScript. Anyone can theoretically create an SDK using a language that supports gRPC. However, there is not yet a guide for how to do this, so developers would need to reference existing SDK code as a reference. Starting with the Java implementation is recommended. The gRPC API is defined [here](https://github.com/microsoft/durabletask-protobuf).

Support for Go is also planned, but using a local interface rather than gRPC.

## Storage providers

This project includes a [sqlite](https://sqlite.org/) storage provider for persisting app state to disk.

```go
// Persists state to a file named test.sqlite3. Use "" for in-memory storage.
options := sqlite.NewSqliteOptions("test.sqlite3")
be := sqlite.NewSqliteBackend(options)
```

Additional storage providers can be created by extending the `Backend` interface.

## Creating the standalone gRPC sidecar

See the `main.go` file for an example of how to create a standalone gRPC sidecar that embeds the Durable Task engine. In short, you must create an `Backend` (for storage), an `Executor` (for executing user code), and host them as a `TaskHubWorker`.

The following code creates a `TaskHub` worker with sqlite `Backend` and a gRPC `Executor` implementations.

```go
// Create a gRPC server that the language SDKs will connect to
grpcServer := grpc.NewServer()
grpcExecutorFunc := func(be backend.Backend) backend.Executor {
    return backend.NewGrpcExecutor(grpcServer, be)
}
configureFunc := func(opts *sqlite.SqliteOptions) {
    opts.FilePath = "taskhub.sqlite3"
}

// The sqlite package has functions for initializing the worker.
// The database will be created automatically if it doesn't already exist.
worker := sqlite.NewTaskHubWorker(grpcExecutorFunc, configureFunc)
worker.Start(context.Background())

// Start listening.
lis, _ := net.Listen("tcp", "localhost:4001")
log.Printf("server listening at %v", lis.Addr())
grpcServer.Serve(lis)
```

Note that the Durable Task gRPC service implementation is designed to serve one client at a time, just like with any sidecar architecture. Scale out is achieved by adding new pod replicas that contain both the app process and the sidecar (connected to a common database).

## Building the project

This project requires go v1.18.x or greater. You can build a standalone executable by simply running `go build` at the project root.

### Generating protobuf

Use the following command to generate the protobuf from the submodule.

```bash
# NOTE: assumes the .proto file defines: option go_package = "/internal/protos"
protoc --go_out=. --go-grpc_out=. submodules/durabletask-protobuf/protos/orchestrator_service.proto
```

### Generating mocks for testing

Test mocks were generated using [mockery](https://github.com/vektra/mockery). Use the following command at the project root to regenerate the mocks.

```bash
mockery --dir ./backend --name="^Backend|^Executor|^TaskWorker" --output ./tests/mocks --with-expecter
```

## Running tests

All tests in the package under `./tests`. This was done to avoid circular package reference problems. Tests external to the package (i.e., [black box testing](https://en.wikipedia.org/wiki/Black-box_testing)) also makes it easier to catch accidental breaking API changes.

Run tests with the following command.

```bash
go test ./tests -coverpkg ./api,./backend/...
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
