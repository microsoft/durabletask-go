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

## Azure Government

Configure all three independently: the scheduler endpoint, the token audience,
and the credential authority. This connection string runs both halves of the
sample against your government scheduler:

```bash
export DTS_CONNECTION_STRING='Endpoint=https://<government-scheduler-host>;TaskHub=<hub>;Authentication=DefaultAzure;ResourceId=https://durabletask.azure.us;AuthorityHost=https://login.microsoftonline.us/'
go run ./samples/authentication
```

The programmatic half copies `ResourceID` onto the scheduler options and forwards
`AuthorityHost` to `DefaultAzureCredential` through
`azcore.ClientOptions.Cloud.ActiveDirectoryAuthorityHost`. Omitting
`AuthorityHost` preserves Azure Identity defaults, including `AZURE_AUTHORITY_HOST`.
For CLI-backed local development, first configure
`az cloud set --name AzureUSGovernment` and then `az login`; setting an authority on
`DefaultAzureCredential` does not configure developer tools.

Omitting `ResourceId`, or using `ResourceId=`, selects the government audience
when `REGION_NAME` starts with `usgov` or `usdod` (case-insensitively). This is an
intentional default change; all other regions retain `https://durabletask.io`.
An explicit audience wins in either direction. It is a token audience URI,
not an ARM resource path, and never changes the authority or endpoint.

For `Authentication=ManagedIdentity`, omit `AuthorityHost`: managed identities
use their hosting environment's identity endpoint. For `AzureCLI` or
`AzurePowerShell`, configure the tool's cloud separately rather than supplying
`AuthorityHost`. See the [SDK reference](../../durabletaskscheduler/README.md#azure-government-example)
for caller-supplied credential configuration.

## Expected result and cleanup

Both modes report their completed, uniquely named instance and the process ends
with `SAMPLE_OK authentication`. It checks the exact activity result, not merely
whether a connection or Hello request succeeded.

The sample terminates/purges only its own instance IDs and descendants, then
stops its workers and closes its clients. It never resets or deletes the task hub.
Missing credentials, missing data-plane permissions, incorrect results, and
cleanup failures produce a nonzero exit.

The SDK's automated audience tests use recording credentials and local TLS gRPC
servers; they do not establish live Azure Government authentication. Running this
sample with a real, authorized government identity is a separate live check.
