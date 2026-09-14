# Large payloads sample

Demonstrates Durable Task Scheduler large-payload externalization and hydration
with the public `payload.AzureBlobStore` and `durabletaskscheduler` APIs.

## Features

- Forces a 64-byte externalization threshold and sends a payload larger than it.
- Uses Azure Blob `blob:v2` references and optional gzip compression.
- Verifies the orchestration output hash and downloads the actual blob objects.
- Creates a unique owned container every run and deletes it during cleanup.

## Prerequisites

- `DTS_CONNECTION_STRING` for an isolated DTS task hub.
- `AZURE_STORAGE_CONNECTION_STRING`; `EXPORT_STORAGE_CONNECTION_STRING` is also
  accepted for compatibility.
- For local Azurite over plaintext HTTP, set `DTS_SAMPLE_ALLOW_INSECURE_STORAGE=1`.
  The sample rejects non-loopback plaintext endpoints.

Do not commit emulator keys. Local automation should generate the Azurite
account key at runtime.

## Run

```bash
# from the repository root
go run ./samples/largepayloads
```

Optional variants:

```bash
LARGEPAYLOADS_DISABLE_GZIP=1 go run ./samples/largepayloads
```

## Expected outcome

The process exits with status 0 and prints:

```text
SAMPLE_OK largepayloads
```

## Cleanup

The orchestration is recursively terminated/purged before shutdown. The unique
owned container is deleted. The sample does not accept an existing container, so
it never deletes another writer's blobs.

## Required validation variants

Integration validation should run the default gzip-enabled command and the
`LARGEPAYLOADS_DISABLE_GZIP=1` variant against DTS and Azurite.
