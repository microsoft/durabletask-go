---
name: durabletask-go Feature Parity Analysis
description: >
  Analyzes feature parity between durabletask-go and sibling Durable Task SDKs
  using the shared protobuf spec as ground truth. Use when investigating a parity
  gap or validating cross-SDK compatibility.
triggers:
  - parity investigation
  - cross-SDK compatibility check
  - proto field gap analysis
  - when Feature Parity Specialist agent runs
---

# Skill: durabletask-go Feature Parity Analysis

Use this skill to systematically analyze and prove parity gaps between durabletask-go and other Durable Task SDKs. Parity is defined by the shared proto spec, not by feature names or documentation.

---

## When to Load This Skill

- When a reported issue mentions that "feature X works in .NET but not Go."
- When implementing a feature that exists in another SDK.
- When reviewing a PR that claims to add parity with another SDK.
- When the Feature Parity Specialist agent is active.

---

## Ground Truth: The Proto Specification

```bash
# Read the proto file (the authoritative source)
cat submodules/durabletask-protobuf/protos/orchestrator_service.proto
```

Every RPC, message, and field in this file represents a cross-SDK capability.
If the Go SDK does not populate a field that is populated by the .NET SDK, that is a gap.

---

## Step 1: Identify the Proto Field or RPC

For the feature under investigation, find its proto representation:

```bash
# Search the proto file for the relevant message or RPC
grep -n "YourFeatureName\|relevant_field" \
  submodules/durabletask-protobuf/protos/orchestrator_service.proto
```

Record:
- The message name (e.g., `CreateInstanceRequest`)
- The field name and number (e.g., `OrchestrationIdReusePolicy idReusePolicy = 5`)
- The RPC that uses it (e.g., `CreateInstance`)

---

## Step 2: Find the Go SDK Implementation

```bash
# Search for the proto field or message in the Go codebase
grep -rn "IdReusePolicy\|idReusePolicy" --include="*.go" . \
  --exclude-dir=internal/protos  # exclude generated code — look at usage sites

# Check whether the field is populated in the relevant request builder
grep -n "CreateInstanceRequest" client/client_grpc.go
```

Record:
- Whether the field is populated.
- If yes: what value is used and where.
- If no: what code path would need to populate it.

---

## Step 3: Determine the Gap Severity

| Severity | Condition |
|---|---|
| **Critical** | Feature is in the proto spec, referenced by all other SDKs, and completely absent from Go SDK's wire output |
| **High** | Feature is partially implemented (field set but wrong value in some cases) |
| **Medium** | Feature is implemented but not exposed via the public Go API (`With*` option missing) |
| **Low** | Feature is implemented and exposed but undocumented |

---

## Step 4: Verify Against Reference SDK (Optional)

If you have access to the reference SDK's source:

For the .NET SDK (`microsoft/durabletask-dotnet`):
- Look for the client method corresponding to the Go method (e.g., `ScheduleNewOrchestrationAsync`).
- Find where the proto request is constructed.
- Verify which fields are set.

For the Python SDK (`microsoft/durabletask-python`):
- Look for the equivalent gRPC call.
- Verify proto field population.

---

## Step 5: Document the Gap

```markdown
## Parity Gap: <Feature Name>

**Proto**: `<message>.<field>` (field number N in `orchestrator_service.proto`)
**RPC**: `<RPCName>`

**Go SDK**: Does NOT populate `<field>` in `<file>:<line>`
**Reference SDK**: Populates `<field>` with `<value>` in `<reference location>`

**Impact**: <What breaks when the field is missing>
**Severity**: Critical / High / Medium / Low

**Proposed fix**:
1. Add `With<FieldName>(value Type)` option function in `api/orchestration.go`
2. Apply the option in `client/client_grpc.go:<line>`
3. Add test in `tests/orchestrations_test.go` that verifies the field is populated

**Wire compatibility**: This fix does NOT break existing behavior because...
```

---

## Step 6: Verify the Fix

After implementing the gap fix:

```bash
# Verify the field is now populated in the request
# Use debug logging or a test that inspects the wire format
go test ./tests/... -run TestParity<FeatureName> -v
```

---

## Known Proto Fields and Their Go SDK Status

| Feature | Proto field | Go SDK status |
|---|---|---|
| Orchestration ID reuse | `CreateInstanceRequest.idReusePolicy` | ✅ Implemented (`WithOrchestrationIdReusePolicy`) |
| Scheduled start | `CreateInstanceRequest.scheduledStartTimestamp` | ✅ Implemented (`WithStartTime`) |
| Cascade terminate | `TerminateRequest.recursive` | ✅ Implemented (`WithRecursiveTerminate`) |
| Cascade purge | `PurgeInstancesRequest.recursive` | ✅ Implemented (`WithRecursivePurge`) |
| Custom status | `ExecuteOrchestratorResult.customStatus` | ✅ Implemented |
| Suspend | `SuspendRequest` | ✅ Implemented |
| Resume | `ResumeRequest` | ✅ Implemented |
| Version (orchestrator) | `CreateInstanceRequest.version` | ⚠️ In proto, verify Go population |
| Version (activity) | `ScheduleTaskAction.version` | ⚠️ In proto, verify Go population |

---

## Exit Criteria

This skill is complete when:
- [ ] The proto field or RPC is identified with file + line reference.
- [ ] The Go SDK's handling of the field is verified (present or absent).
- [ ] The gap severity is classified.
- [ ] A proposed fix is documented (if gap exists).
- [ ] The fix is verified with a test (if fix was implemented).
