# Storage (draft, not a contract)

> **Superseded proposal — do not implement.** This schema is not a contract. It assumes one
> queue, mixes spawned tasks with outbox events, and does not store a full
> attempt history. The accepted invariants are in
> [state-machine.md](../04-architecture/01-state-machine.md) and
> [concurrency.md](../04-architecture/02-concurrency.md). The physical schema will
> be designed separately.

Original physical-schema draft for [03-formats.md](03-formats.md). **It is not a
contract.** There are no migrations. The DBMS is PostgreSQL (see
[tech-stack](../_ai/tech-stack.md)).

The queue is the sole owner of the DDL. The application does not access the tables: only HTTP from [05-http-api.md](05-http-api.md).

One logical queue per instance. There are no domain columns: the entire application body is `payload` JSONB.

Two tables: `tasks` (the work queue) and `outbox` (the delivery outbox: intent to deliver after complete). They sit side by side in the same queue store.

## Table `tasks`

Enqueue, claim, heartbeat. Closing a task happens here as well; outgoing records go to `outbox`.

| Column | Type | Null | Meaning |
| --- | --- | --- | --- |
| `id` | UUID | no | PK |
| `payload` | JSONB | no | Opaque body. The queue does not read keys |
| `status` | VARCHAR(16) | no | `available` \| `leased` \| `completed` |
| `delivery_count` | INTEGER | no | default `0`. +1 on every successful claim |
| `created_at` | timestamptz | no | default `now()` |
| `idempotency_key` | VARCHAR(256) | yes | Producer key. NULL on tasks spawned from the outbox |
| `claim_id` | UUID | yes | Current (or last) lease |
| `lease_expires_at` | timestamptz | yes | Lease end |

There are no columns `file_id`, `minio_path`, `job_kind`, `parse_result`, `processing_params`, `run_at`, `error_message`, `cancel_requested`, `claimed_by`. The link “which task spawned this one” lives in `outbox`, not here.

### Invariants (CHECK)

- `status IN ('available', 'leased', 'completed')`
- `delivery_count >= 0`
- `status = 'available'` → `claim_id IS NULL AND lease_expires_at IS NULL`
- `status = 'leased'` → `claim_id IS NOT NULL AND lease_expires_at IS NOT NULL`
- `status = 'completed'` → `claim_id IS NOT NULL`

### Indexes

| Name | Definition | Why |
| --- | --- | --- |
| `pk_tasks` | PRIMARY KEY (`id`) | |
| `uq_tasks_idempotency_key` | UNIQUE (`idempotency_key`) WHERE `idempotency_key IS NOT NULL` | Enqueue repeat |
| `uq_tasks_claim_id` | UNIQUE (`claim_id`) WHERE `claim_id IS NOT NULL` | heartbeat / complete by `claim_id` |
| `idx_tasks_available_created_at` | (`created_at`) WHERE `status = 'available'` | Claim of free rows, FIFO |
| `idx_tasks_leased_expires_at` | (`lease_expires_at`) WHERE `status = 'leased'` | Claim of expired leases |

`now()` is not placed in an index predicate.

## Table `outbox`

Adjacent table. A row is one intent to deliver, written in the same transaction as `tasks.status = completed`.

In this draft, “deliver” means enqueue a new task on this same queue. A relay into a broker is not specified. v1 callback fields (`parse_result`, backoff, `failed`) are not carried over.

| Column | Type | Null | Meaning |
| --- | --- | --- | --- |
| `id` | UUID | no | PK |
| `created_at` | timestamptz | no | default `now()` |
| `payload` | JSONB | no | Opaque body of the next task |
| `status` | VARCHAR(16) | no | `pending` \| `published` |
| `source_task_id` | UUID | no | FK → `tasks.id`. Which task was closed |
| `source_claim_id` | UUID | no | The complete claim. A complete repeat looks up by this column |
| `result_task_id` | UUID | yes | FK → `tasks.id`. The task created from this row |
| `published_at` | timestamptz | yes | When `result_task_id` appeared |

FKs have no `ON DELETE CASCADE`: completed tasks are not deleted in this draft. `source_task_id` and `result_task_id` are `ON DELETE RESTRICT`.

### Invariants (CHECK)

- `status IN ('pending', 'published')`
- `status = 'published'` → `result_task_id IS NOT NULL AND published_at IS NOT NULL`
- `status = 'pending'` → `result_task_id IS NULL AND published_at IS NULL`

### Indexes

| Name | Definition | Why |
| --- | --- | --- |
| `pk_outbox` | PRIMARY KEY (`id`) | |
| `idx_outbox_source_claim_id` | (`source_claim_id`) | Complete repeat |
| `idx_outbox_source_task_id` | (`source_task_id`) | Link to the closed task |
| `uq_outbox_result_task_id` | UNIQUE (`result_task_id`) WHERE `result_task_id IS NOT NULL` | One outbox row → one task |
| `idx_outbox_pending_created_at` | (`created_at`) WHERE `status = 'pending'` | If a deferred relay appears later |

## Operations

Claim mechanics match v1: one `UPDATE … FROM (SELECT … FOR UPDATE SKIP LOCKED) AS picked`. Exact SQL is not fixed here.

### Enqueue

`INSERT` into `tasks`: `status = 'available'`, `delivery_count = 0`, `idempotency_key` filled in. The outbox is not touched.

A conflict on `uq_tasks_idempotency_key` returns the existing row.

### Claim

A candidate in `tasks` is `status = 'available'` **or** (`status = 'leased'` and `lease_expires_at < now()`). Order: `created_at ASC`. One row, `SKIP LOCKED`.

On the chosen row: a new `claim_id`, a new `lease_expires_at`, `status = 'leased'`, `delivery_count = delivery_count + 1`.

There is no separate reaper for lease expiry: an expired lease is claimable again by the same query.

### Heartbeat

Select `tasks` by `claim_id` where `status = 'leased'` and `lease_expires_at >= now()`. Move `lease_expires_at` forward.

### Complete

In one transaction:

1. The `tasks` row with this `claim_id`, `status = 'leased'`, and `lease_expires_at >= now()` becomes `status = 'completed'`. `claim_id` is left unchanged.
2. For each outbox item:
   - `INSERT tasks`: `status = 'available'`, that `payload`, `idempotency_key` NULL;
   - `INSERT outbox`: that `payload`, `source_task_id`, `source_claim_id`, `result_task_id` of the new task, `status = 'published'`, `published_at = now()`.

On a repeat of the same `claim_id`, when the source task is already `completed`, do not insert new rows. The response is the existing `tasks` found through `outbox.result_task_id` where `source_claim_id` matches.

No live lease means conflict / not found, as in the HTTP draft.

An empty outbox list is step 1 only; there are no rows in `outbox`.

In this draft, step 2 sets `published` immediately: the `complete` response returns the new **task** rows. Status `pending` is reserved for a later design where “enqueue the task” moves to a relay after commit. The `complete` response would then have to change — that design is not present now.

## Why a separate table

Transactional outbox pattern: the state change and the intent to deliver are different records in one transaction. `tasks` is the work. `outbox` is what must appear next from complete.

The queue keeps “claim the work” and “the outgoing journal” apart. A complete repeat reads `outbox` by `source_claim_id` and does not search for children among all `tasks`. If a relay into a broker is needed, the table is already there; the channel and the envelope are not specified.

This is not outbox v1: there is no `parse_result`, no HTTP callback, and no `processing` / `failed` / backoff.

## v1 → this schema

| v1 | Here |
| --- | --- |
| `jobs` + parser columns | `tasks` + `payload` |
| `outbox.payload` + `parse_result` | `outbox.payload` |
| `outbox.job_id` | `outbox.source_task_id` |
| `outbox.status` (`pending` / `processing` / `published` / `failed`) | `pending` \| `published` |
| `outbox` as a callback relay | `outbox` as the intent to enqueue the following `tasks` |
| `pending` / `processing` on jobs | `available` / `leased` |
| `claimed_by` TEXT | `tasks.claim_id` UUID |
| idempotency `(job_kind, file_id)` | `tasks.idempotency_key` |
| jobs-reaper | claim itself picks up an expired `leased` row |
| outbox-reaper, TTL, stats views | none |

## Out of scope

- Inbox table
- Broker envelope and a deferred relay as the working path. Transport is out of scope
- `failed`, cancel, TTL, fillfactor, metrics views
- How to join complete with the application's business database
- An Alembic revision (there is none yet)

Next: [03-formats.md](03-formats.md), [outbox as a pattern](../01-concepts/10-transactional-outbox.md).
