# Admin: recovery and break-glass clients

[Documentation](../README.md) › [Guides](README.md) › **Admin operations**

Separate role SDKs: `AdminClient` (routine admin + recovery) and
`BreakGlassClient` (emergency JIT). Do not mix credentials between public
`/v1` and private `/admin/v1`, and do not put admin / break-glass tokens in
producer/consumer pods.

## Install (`workhold-admin`)

```bash
pip install workhold-admin
pip install "workhold-admin[async]"
```

Sync: `ObserverClient`, `AdminClient`, `BreakGlassClient`.
Async: `AsyncObserverClient`, `AsyncAdminClient`, `AsyncBreakGlassClient` from
`workhold_admin.async_client`.
Bounded cursor pagination (`max_pages` / `max_items` required), retry helpers,
instrumentation redaction and capability guards:
[06-client-sdk-ergonomics.md](06-client-sdk-ergonomics.md).

## Separate base URLs and tokens

| Client | Listener | Credential |
| --- | --- | --- |
| `ProducerClient` / `ConsumerClient` | public `/v1` | PRODUCER / WORKER |
| `ObserverClient` / `AdminClient` | private `/admin/v1` | OBSERVER / ADMIN |
| `BreakGlassClient` | private `/admin/v1` | short-lived `BREAK_GLASS` JIT |

```python
from _workhold_client_core.transport import HttpJsonTransport
from workhold_admin import AdminClient, BreakGlassClient

public = HttpJsonTransport("https://queue.example")
admin = HttpJsonTransport("https://queue-admin.example")

routine = AdminClient(
    public,
    bearer_token=admin_token,  # ADMIN only
    admin_transport=admin,
)

# Deployment-issued JIT — SDK never mints or refreshes this token.
emergency = BreakGlassClient(admin, bearer_token=break_glass_jit_token)
```

Optimistic config: `set_queue_state` / `activate_queue_policy` require
`expected_config_version`. On conflict, reread the queue and retry —
the SDK does not perform a hidden read-modify-write.

**Do not change queue / task tables directly in PostgreSQL.** The control plane
and audit live in the HTTP API; direct SQL bypasses fencing, idempotency, and retention.

`ObserverClient.get_stats()` / `AdminClient.get_stats()` take **no query params**
(`GET /admin/v1/stats`). Do not pass `time_from` / `time_to` / `cursor`.

## Dead-letter replay (at-least-once)

`AdminClient.replay_dead_letter` creates a **new** ready task with lineage
`source_task_id`. Source terminal history is **not** mutated. A repeat with the
same admin `Idempotency-Key` can return `replayed=true` — this is **at-least-once**,
not exactly-once. External side effects can repeat.

## Bulk preview → execute

Preview and execute are **separate** calls. Execute accepts only a
`BulkPreviewResult` from its own preview; the confirmation token cannot be filled in
by hand at random. Filters are bounded; unbounded SQL/payload search is forbidden.

```python
preview = routine.preview_bulk_replay("orders", filters={"from": "...", "to": "..."})
# human review of candidate_count / sample_task_ids
result = routine.execute_bulk_replay(
    "orders",
    preview=preview,
    idempotency_key="...",
    reason="...",
    filters={"from": "...", "to": "..."},
)
```

The same applies to `preview_bulk_cancel` / `execute_bulk_cancel` (cancel does not create
spawn/events).

## Break-glass

Only `BreakGlassClient` plus a JIT with `expires_at` and `allowed_operations`.
An ordinary `ADMIN` / `OBSERVER` / producer / worker does **not** authorize emergency
ops. Each call requires the ack triad: `reason`, `incident_reference`,
`risk_acknowledged=True`.

Break-glass does **not**:

- mint / impersonate worker `claim_token`;
- mutate terminal history in place;
- promise exactly-once recovery;
- issue credentials through the workhold API (minting is deployment-only).

Allowlist and audit details: [07-admin-tools.md](../05-operations/07-admin-tools.md).

---

← [Admin queues](04-admin-queues.md) · [Guides](README.md) →
