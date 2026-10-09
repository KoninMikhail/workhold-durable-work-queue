# Formats (draft, not a contract)

> **Superseded proposal — do not implement.** This draft is not a contract. It predates
> [product boundary](../01-concepts/07-product-boundary.md), [state
> machines](../04-architecture/01-state-machine.md), and
> [ADR 003](../04-architecture/adr/003-separate-spawns-and-events.md).
> It incorrectly uses `outbox` for spawned tasks and does not support named
> queues. The future normative format will be designed in Phase 3/5.

Logical records of the original variant. **They are not a contract.** HTTP projection:
[05-http-api.md](05-http-api.md).

The queue does not read or validate the contents of `payload`. Domain fields (`file_id`, `minio_path`, `job_kind`, `parse_result`, and any others) live **only inside** `payload` when the application needs them.

Scope: the producer enqueues a task; the consumer claims it and completes it. A broker envelope and an inbox are not this document. Table draft: [04-storage.md](04-storage.md).

The structure comes from the `jobs` / `outbox` tables in an earlier `parsers-queue-service` design. Record roles and queue metadata are carried over, not parser columns.

## Roles

| Role | What it writes | What it reads |
| --- | --- | --- |
| Producer | enqueue: `payload` + `Idempotency-Key` | the created or repeated **task** |
| Consumer | heartbeat; complete: a list of **outbox item** | **claim** (lease + task) |
| Queue | persist, lease, atomic complete | — |

A repeated enqueue with the same `Idempotency-Key` returns the same **task**. A repeated complete on the same claim does not create new outbox records.

## Payload

| Field | Type | Who sets it |
| --- | --- | --- |
| `payload` | JSON | the application |

The queue stores and returns `payload` as-is. There are no field names inside it. There are no reserved queue keys.

If a parser needs `file_id` and `minio_path`, it puts them in its own `payload`. Another application does not need those keys, and the queue does not require them.

## v1 → this model

| v1 (`jobs` / `outbox`) | Here |
| --- | --- |
| `jobs.minio_path`, `file_id`, `job_kind`, `processing_params` | no separate fields → `task.payload` |
| `outbox.parse_result`, domain `outbox.payload` | no separate fields → `outbox item.payload` |
| `jobs.id` | `task.id` |
| `jobs.status`: `pending` / `processing` / `completed` | `available` / `leased` / `completed` |
| `jobs.created_at` | `task.created_at` |
| `jobs.attempt_count` | `task.delivery_count` (how many times the task was claimed; observation only) |
| claim without a separate entity (`processing` + `claimed_by`) | **claim**: `id` + `expires_at` |
| idempotency `(job_kind, file_id)` | header `Idempotency-Key` |
| `outbox.job_id` | not in the format; in storage — `outbox.source_task_id` |
| `jobs.run_at`, `error_message`, `cancel_requested`, `failed` / `cancelled` | not in this draft |
| `outbox.status`, `published_at`, backoff, reaper | not in the format; in storage — `pending` / `published` without backoff |

## Enqueue (producer → queue)

The producer must pass `Idempotency-Key` (a string, 1–256).

```json
{ "payload": {} }
```

`payload` is required. The value is any JSON the application treats as the task body.

The response is a **task**. A new key creates one. A repeat of the same key returns the same **task**.

## Task

What the queue treats as a task. The producer receives it after enqueue. The consumer receives it inside a **claim**.

| Field | Type | Meaning |
| --- | --- | --- |
| `id` | UUID | Task identifier |
| `status` | `available` \| `leased` \| `completed` | State in the queue |
| `payload` | JSON | Application body, without interpretation |
| `delivery_count` | integer ≥ 0 | How many times the task was claimed. Not an idempotency key |
| `created_at` | date-time | When the task appeared |

```json
{
  "id": "018f2c1a-7b3e-7c00-8000-000000000001",
  "status": "available",
  "payload": {},
  "delivery_count": 0,
  "created_at": "2026-09-18T12:00:00Z"
}
```

There are no other fields in **task**.

## Claim (queue → consumer)

| Field | Type | Meaning |
| --- | --- | --- |
| `lease.id` | UUID | Lease identifier. Heartbeat and complete use the same id |
| `lease.expires_at` | date-time | Until this instant the task stays `leased` |
| `task` | **task** | Task body and metadata |

```json
{
  "lease": {
    "id": "018f2c1a-7b3e-7c00-8000-0000000000aa",
    "expires_at": "2026-09-18T12:05:00Z"
  },
  "task": {
    "id": "018f2c1a-7b3e-7c00-8000-000000000001",
    "status": "leased",
    "payload": {},
    "delivery_count": 1,
    "created_at": "2026-09-18T12:00:00Z"
  }
}
```

The consumer may request `lease_seconds` on claim and on heartbeat. The queue may shorten it. The response fields stay the same: `id`, `expires_at`.

When there is no free task, there is no body (HTTP `204`).

If there is no complete before `expires_at`, the lease is released and the task becomes `available` again. The format has no separate nack/release.

## Outbox item (consumer → queue)

One outgoing record on complete. In storage it is a row of the `outbox` table ([04-storage.md](04-storage.md)). In this draft each such record becomes a new **task** in the same queue, in the same commit.

```json
{ "payload": {} }
```

There is one field: `payload`. There is no `file_id`, `minio_path`, `parse_result`, callback address, or event type.

The list may be empty: the task is closed and there are no following tasks.

## Complete (consumer → queue)

```json
{
  "outbox": [
    { "payload": {} }
  ]
}
```

Response:

| Field | Type | Meaning |
| --- | --- | --- |
| `task` | **task** | Closed task, `status=completed` |
| `outbox` | **task**[] | New tasks from the outbox records, in the same commit |

```json
{
  "task": {
    "id": "018f2c1a-7b3e-7c00-8000-000000000001",
    "status": "completed",
    "payload": {},
    "delivery_count": 1,
    "created_at": "2026-09-18T12:00:00Z"
  },
  "outbox": [
    {
      "id": "018f2c1a-7b3e-7c00-8000-000000000002",
      "status": "available",
      "payload": {},
      "delivery_count": 0,
      "created_at": "2026-09-18T12:04:00Z"
    }
  ]
}
```

The queue does not execute side effects from `payload`. It only persists the close and the new tasks atomically.

## Out of scope

- Broker envelope (Kafka / RabbitMQ / other)
- Physical tables beyond the draft [04-storage.md](04-storage.md); TTL, reaper as in v1
- Where the inbox is stored
- How to join the queue atomically with the application's business database
- v1 fields: `file_id`, `minio_path`, `job_kind`, `parse_result`, `processing_params`, `cancel_requested`, `claimed_by`

Next: [04-storage.md](04-storage.md), [HTTP API](05-http-api.md), [outbox as a pattern](../01-concepts/10-transactional-outbox.md).
