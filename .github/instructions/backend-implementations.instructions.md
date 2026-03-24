---
applyTo: "backend/sqlite/**,backend/postgres/**"
---

# Backend Implementation Instructions — durabletask-go

These instructions apply to `backend/sqlite/` and `backend/postgres/`.

---

## Structural Parity Rule

The SQLite and PostgreSQL backends implement the same `backend.Backend` interface.
They are structurally parallel: if you add, fix, or change something in one, check whether the same applies to the other.

The primary structural differences are:
| Aspect | SQLite | PostgreSQL |
|--------|--------|------------|
| Driver | `modernc.org/sqlite` (pure Go, CGO-free) | `jackc/pgx/v5` |
| Schema types | `INTEGER`, `BLOB`, `DATETIME`, `CURRENT_TIMESTAMP` | `SERIAL`, `BYTEA`, `TIMESTAMP`, `NOW()` |
| In-memory mode | Supported (`""` path → in-memory) | Not supported |
| Test gating | Default (always runs) | `POSTGRES_ENABLED=true` env var |

---

## Work Item Locking

Both backends implement optimistic locking for work items:
- `LockedBy` stores the worker's identifier.
- `LockExpiration` (or equivalent) marks when the lock expires.
- `GetOrchestrationWorkItem` must atomically claim an item by setting `LockedBy` in the same DB transaction.
- `AbandonOrchestrationWorkItem` must increment `RetryCount` so `GetAbandonDelay()` computes the correct backoff.
- `ErrWorkItemLockLost` must be returned if a work item's lock has been stolen or expired before completion.

---

## Schema Changes

Schema is defined in:
- `backend/sqlite/schema.sql`
- `backend/postgres/schema.sql`

Both backends apply the schema at startup via `CreateTaskHub`. If you add a column:
1. Add it to both schema files.
2. Handle the migration case (table already exists, column does not) — both backends create the schema only if the table does not exist, so forward-compatible defaults are required.
3. Document the migration path in the PR.

---

## History Event Serialization

History events stored in the `History` table are serialized with `backend.MarshalHistoryEvent` (i.e., `proto.Marshal`).
Do not store JSON in `EventPayload` columns — they are typed `BLOB`/`BYTEA` for a reason.

---

## Abandon Delay

`OrchestrationWorkItem.GetAbandonDelay()` computes a linear backoff in seconds from `RetryCount`:
- `RetryCount 0` → 0 delay
- For `RetryCount > 0`, the delay is `RetryCount` seconds, capped at 5 minutes (300 seconds) once `RetryCount` exceeds 100.

Backends must increment `RetryCount` on abandon (not just clear the lock). Failure to do so breaks backoff.
