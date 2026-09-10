# Azure authentication

This sample runs and verifies an orchestration and activity twice: once with the
connection string's identity configuration and once with a programmatically
supplied `DefaultAzureCredential`. Both the management client and worker must
successfully perform real data-plane work. An emulator cannot validate this sample.

## Run

Use an existing Azure DTS task hub and an identity authorized to run workers,
start/query orchestrations, and terminate/purge the sample's instances.
For local development, sign in with `az login` and select the correct tenant.

```bash
export DTS_CONNECTION_STRING='Endpoint=https://<scheduler-host>;TaskHub=<hub>;Authentication=DefaultAzure'
go run ./samples/authentication
```

For managed or workload identity, run on the corresponding Azure host and
configure the standard `AZURE_*` identity environment variables. The programmatic
half always uses the DefaultAzureCredential chain; configuring a different
connection-string mode does not change that chain. See the
[DTS authentication reference](../../durabletaskscheduler/README.md#configuration)
for the supported credential modes and their settings.

No account keys or access tokens belong in source code or logs.

## Expected result and cleanup

Both modes report their completed, uniquely named instance and the process ends
with `SAMPLE_OK authentication`. It checks the exact activity result, not merely
whether a connection or Hello request succeeded.

The sample terminates/purges only its own instance IDs and descendants, then
stops its workers and closes its clients. It never resets or deletes the task hub.
Missing credentials, missing data-plane permissions, incorrect results, and
cleanup failures produce a nonzero exit.
