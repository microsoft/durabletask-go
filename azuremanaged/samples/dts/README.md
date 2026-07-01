# Azure-managed DTS sample

A minimal end-to-end sample that connects to an **Azure-managed Durable Task Scheduler (DTS)**
task hub, starts a worker, schedules a "hello cities" orchestration, and prints the result.

## Configure

Set **either** a full connection string:

```bash
export DTS_CONNECTION_STRING="Endpoint=<scheduler>.durabletask.io;Authentication=DefaultAzure;TaskHub=<hub>"
```

**or** an endpoint plus task hub (authenticates with `DefaultAzureCredential`):

```bash
export DTS_ENDPOINT="<scheduler>.durabletask.io"
export DTS_TASKHUB="<hub>"
```

Make sure your identity has access to the scheduler (e.g. `az login`, a managed identity, or
environment credentials that `DefaultAzureCredential` can resolve).

For the local emulator, use `Authentication=None`:

```bash
export DTS_CONNECTION_STRING="Endpoint=localhost:8080;Authentication=None;TaskHub=default"
```

## Run

From the `azuremanaged` module directory:

```bash
go run ./samples/dts
```

You should see the scheduled instance id followed by the completed orchestration metadata,
including the greetings for Tokyo, London, and Seattle.
