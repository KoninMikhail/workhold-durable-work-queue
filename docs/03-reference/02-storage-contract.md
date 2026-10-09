# Storage contract — PostgreSQL 18.6

**Status:** authoritative physical catalog  
**DBMS:** PostgreSQL 18.6  
**ORM:** SQLAlchemy 2 metadata on `queue_service.db.Base` (`src/queue_service/storage/models.py`)

This document is the exact catalog of relations, columns, constraints, and indexes. It does **not**
describe queue runtime behavior. Migrations must match this contract.

Normative sources: [storage topology](../04-architecture/04-storage-topology.md), ADR 006, ADR 007,
ADR 008, ADR 017, ADR 024, [state machine](../04-architecture/01-state-machine.md).

## Summary: tables and columns

| Table | Columns | Partitions |
| --- | --- | --- |
| `queues` | `id`, `queue_id`, `name`, `state_code`, `config_version`, `active_policy_version_id`, `created_at`, `updated_at` | none |
| `queue_policy_versions` | `id`, `queue_id`, `version`, `enabled`, `max_attempts`, `backoff_strategy_code`, `retry_delay_seconds`, `created_at` | none |
| `tasks_active` | `id`, `task_id`, `queue_id`, `producer_id`, `state_code`, `priority`, `available_at`, `retry_policy_version_id`, `generation`, `current_claim_id`, `claimed_at`, `lease_expires_at`, `cancel_requested_at`, `worker_id`, `source_task_id`, `spawn_ordinal`, `created_at`, `updated_at` | none |
| `task_payloads_active` | `task_id` (PK/FK), `payload`, `payload_bytes` | none |
| `delivery_events_active` | `id`, `event_id`, `source_task_id`, `ordinal`, `state_code`, `envelope`, `envelope_bytes`, `available_at`, `generation`, `current_claim_id`, `claimed_at`, `lease_expires_at`, `relay_principal_id`, `delivery_attempt`, `last_failure_code`, `created_at`, `updated_at` | none |
| `enqueue_dedup` | `id`, `producer_id`, `queue_id`, `key_hash`, `request_fingerprint`, `task_id`, `created_at`, `expires_at` | none |
| `claim_registry` | `id`, `claim_id`, `task_id`, `claim_token`, `generation`, `claimed_at`, `lease_expires_at`, `created_at` | none |
| `complete_replay` | `id`, `claim_id`, `operation_code`, `request_fingerprint`, `task_id`, `result_state_code`, `available_at`, `terminal_at`, `spawned_task_ids`, `event_ids`, `created_at`, `expires_at` | none |
| `admin_replay` | `id`, `admin_principal_id`, `operation_code`, `key_hash`, `request_fingerprint`, `http_status`, `response_body`, `created_at`, `expires_at` | none |
| `completion_effects` | `id`, `source_claim_id`, `effect_kind_code`, `ordinal`, `resource_id`, `created_at` | none |
| `queue_counters` | `queue_id` (PK/FK), `delayed_count`, `ready_count`, `leased_count`, `as_of` | none |
| `partition_maintenance_status` | `singleton_id` (PK=1), `last_started_at`, `last_succeeded_at`, `premade_through`, `retained_from`, `last_error_code`, `last_error_detail`, `updated_at` | none |
| `admin_audit_log` | `id`, `audit_at`, `queue_id`, `actor_id`, `operation_code`, `previous_config_version`, `new_config_version`, `request_id`, `details` | daily `RANGE (audit_at)` |
| `task_attempts` | `id`, `task_id`, `claim_id`, `generation`, `claimed_at`, `worker_id`, `lease_expires_at`, `ended_at`, `outcome_code`, `failure_code`, `failure_detail` | daily `RANGE (claimed_at)` |
| `tasks_terminal` | `id`, `task_id`, `queue_id`, `producer_id`, `state_code`, `priority`, `available_at`, `retry_policy_version`, `payload`, `payload_bytes`, `created_at`, `terminal_at`, `failure_code`, `failure_detail`, `source_task_id`, `spawn_ordinal` | daily `RANGE (terminal_at)` |
| `delivery_events_terminal` | `id`, `event_id`, `source_task_id`, `ordinal`, `state_code`, `envelope`, `envelope_bytes`, `created_at`, `terminal_at`, `failure_code`, `failure_detail`, `delivery_attempt` | daily `RANGE (terminal_at)` |

Column types, CHECK constraints, and indexes are in the sections below. The `alembic_version` table is not
part of the product catalog.

## Global rules

- All timestamps are queue-service-store `timestamptz`.
- Public UUID identifiers are generated at runtime (without a database UUID extension and without
  UUID column defaults).
- Internal keys use `bigint GENERATED ALWAYS AS IDENTITY`, unless stated otherwise.
- Opaque JSON is stored outside frequently updated lease rows; the hard DB ceiling is
  **1 MiB (`1048576` bytes)**. Runtime default acceptance ceiling is **262144**
  bytes (OpenAPI/config) and is **not** pinned by a DB CHECK.
- GIN indexes on payload/envelope are **not** created.
- A DEFAULT partition on daily RANGE parents is **not** created.
- History FKs **must not** block detach; CASCADE **must not** reach history parents.
- Correctness registries in this baseline are **not** HASH-partitioned.
- Exact physical names (no aliases): `admin_replay`,
  `partition_maintenance_status`, `completion_effects`.

## TTL correctness registry (MVP)

Phase 3.8 consumes these values unchanged.

| Registry | Min | Max | Runtime default |
| --- | --- | --- | --- |
| `enqueue_dedup` | 30 days / 2592000 s | 365 days / 31536000 s | 90 days / 7776000 s |
| `complete_replay` | 1 day / 86400 s | 30 days / 2592000 s | 7 days / 604800 s |
| `admin_replay` | 7 days / 604800 s | 90 days / 7776000 s | 30 days / 2592000 s |

DB CHECK expressions set min/max intervals in days relative to `created_at`.

## Canonical code maps

| Map | Codes |
| --- | --- |
| queue state | 1=active, 2=paused, 3=draining |
| active task state | 1=delayed, 2=ready, 3=leased |
| replay-only result | 3=retry_scheduled |
| terminal task state | 10=succeeded, 11=dead_lettered, 12=cancelled |
| attempt outcome | 1=active, 2=succeeded, 3=retry_scheduled, 4=dead_lettered, 5=expired, 6=cancelled |
| delivery state | 1=pending, 2=publishing, 10=published, 11=dead_lettered |
| backoff strategy | 1=fixed |
| terminal operation | 1=complete, 2=fail, 3=ack_cancel |
| admin/audit operation | 1=create_queue, 2=create_policy, 3=activate_policy, 4=set_queue_state, 5=run_maintenance, 6=replay_dead_letter, 7=bulk_replay, 8=bulk_cancel, 9=force_lease_expiry, 10=reconcile_counters, 11=raise_replay_limit, 12=drop_expired_partition, 13=repair_registry |
| completion effect | 1=spawn, 2=event |

## Unpartitioned relations

### `queues`

| Column | Type | Null | Default | Notes |
| --- | --- | --- | --- | --- |
| id | bigint identity | NO | identity | PK |
| queue_id | uuid | NO | — | UNIQUE; runtime UUID |
| name | text | NO | — | UNIQUE; length 1..128; lowercase regex `^[a-z0-9][a-z0-9._-]*$` |
| state_code | smallint | NO | 1 | CHECK IN (1,2,3) |
| config_version | bigint | NO | 1 | CHECK >= 1 |
| active_policy_version_id | bigint | YES | — | FK → `queue_policy_versions(id)` DEFERRABLE INITIALLY DEFERRED, no cascade |
| created_at | timestamptz | NO | statement_timestamp() | |
| updated_at | timestamptz | NO | statement_timestamp() | |

**Same-queue invariant:** `active_policy_version_id` must reference a
`queue_policy_versions` row whose `queue_id` equals this named queue's `id`. It is enforced
in the activating transaction (not a DB CHECK).

### `queue_policy_versions`

| Column | Type | Null | Default | Notes |
| --- | --- | --- | --- | --- |
| id | bigint identity | NO | identity | PK; target of queues.active_policy_version_id |
| queue_id | bigint | NO | — | FK → `queues(id)` RESTRICT |
| version | integer | NO | — | CHECK >= 1; UNIQUE `(queue_id, version)` |
| enabled | boolean | NO | — | |
| max_attempts | integer | NO | — | CHECK >= 1 |
| backoff_strategy_code | smallint | NO | 1 | CHECK = 1 |
| retry_delay_seconds | integer | NO | — | CHECK 0..86400 |
| created_at | timestamptz | NO | statement_timestamp() | |

### `tasks_active`

| Column | Type | Null | Default | Notes |
| --- | --- | --- | --- | --- |
| id | bigint identity | NO | identity | PK |
| task_id | uuid | NO | — | UNIQUE |
| queue_id | bigint | NO | — | FK → queues(id) RESTRICT |
| producer_id | text | NO | — | length 1..128 |
| state_code | smallint | NO | — | CHECK IN (1,2,3) |
| priority | smallint | NO | 0 | CHECK BETWEEN -32768 AND 32767 |
| available_at | timestamptz | NO | — | |
| retry_policy_version_id | bigint | NO | — | FK → queue_policy_versions(id) RESTRICT |
| generation | integer | NO | 0 | CHECK >= 0 |
| current_claim_id | uuid | YES | — | paired nullability with claim fields |
| claimed_at | timestamptz | YES | — | |
| lease_expires_at | timestamptz | YES | — | |
| cancel_requested_at | timestamptz | YES | — | |
| worker_id | text | YES | — | length 1..128 when non-null; paired with claim fields |
| source_task_id | uuid | YES | — | paired with spawn_ordinal |
| spawn_ordinal | integer | YES | — | >= 0 when non-null |
| created_at | timestamptz | NO | statement_timestamp() | |
| updated_at | timestamptz | NO | statement_timestamp() | |

CHECK: claim fields are either all-null or all-non-null; spawn lineage is either both-null
or both-non-null.

Indexes:

- `tasks_active_claim_idx` on `(queue_id, state_code, priority DESC, available_at, id)`
- `tasks_active_spawn_lineage_uidx` UNIQUE `(source_task_id, spawn_ordinal)` WHERE `source_task_id IS NOT NULL`

**Runtime behavior (WORK-15 / WORK-16):** the claimable predicate includes
`state_code IN (1,2)` (`delayed`, `ready`) with
`available_at <= transaction_timestamp()`. Due `delayed` rows move
directly to `leased` (state `3`); there is no separate promotion state/job.
Retry-scheduled work uses the same predicate. The physical catalog
(`1201_bounded_priority_claim_ordering`) widens `priority` to the full
signed `smallint` and reorders `tasks_active_claim_idx` with
`priority DESC` before `available_at`; runtime claim order is a separate phase.

**Migration `1201_bounded_priority_claim_ordering`:** a normal transactional
`DROP INDEX` / `CREATE INDEX` on the hot `tasks_active_claim_idx` blocks
writes to `tasks_active` for the duration of the rebuild. Downgrade fail-closed: if
`tasks_active` or the parent `tasks_terminal` contains `priority <> 0`, the downgrade
is rejected before any DDL, without changing data or the revision.

### `task_payloads_active`

| Column | Type | Null | Default | Notes |
| --- | --- | --- | --- | --- |
| task_id | bigint | NO | — | PK; FK → tasks_active(id) ON DELETE CASCADE; no identity |
| payload | jsonb | NO | — | opaque; no GIN |
| payload_bytes | integer | NO | — | CHECK 1..1048576 |

### `delivery_events_active`

| Column | Type | Null | Default | Notes |
| --- | --- | --- | --- | --- |
| id | bigint identity | NO | identity | PK |
| event_id | uuid | NO | — | UNIQUE |
| source_task_id | uuid | NO | — | UNIQUE with ordinal |
| ordinal | integer | NO | — | CHECK >= 0 |
| state_code | smallint | NO | — | CHECK IN (1,2) |
| envelope | jsonb | NO | — | no GIN |
| envelope_bytes | integer | NO | — | CHECK 1..1048576 |
| available_at | timestamptz | NO | — | |
| generation | integer | NO | 0 | CHECK >= 0 |
| current_claim_id | uuid | YES | — | state/claim fence CHECK |
| claimed_at | timestamptz | YES | — | state/claim fence CHECK |
| lease_expires_at | timestamptz | YES | — | state/claim fence CHECK |
| relay_principal_id | text | YES | — | length 1..128 when non-null; state/claim fence CHECK |
| delivery_attempt | integer | NO | 0 | CHECK >= 0 |
| last_failure_code | text | YES | — | length 1..128 when non-null |
| created_at | timestamptz | NO | statement_timestamp() | |
| updated_at | timestamptz | NO | statement_timestamp() | |

CHECK `delivery_events_active_state_claim_fence_check`: state `1` (pending) ⇒
`current_claim_id`, `claimed_at`, `lease_expires_at`, `relay_principal_id` all NULL;
state `2` (publishing) ⇒ `generation >= 1` and all four claim fields NOT NULL.

### `enqueue_dedup`

| Column | Type | Null | Default | Notes |
| --- | --- | --- | --- | --- |
| id | bigint identity | NO | identity | PK |
| producer_id | text | NO | — | length 1..128 |
| queue_id | bigint | NO | — | FK → queues(id) RESTRICT |
| key_hash | bytea | NO | — | octet_length = 32 |
| request_fingerprint | bytea | NO | — | octet_length = 32 |
| task_id | uuid | NO | — | |
| created_at | timestamptz | NO | statement_timestamp() | |
| expires_at | timestamptz | NO | — | CHECK 30..365 days after created_at |

UNIQUE `(producer_id, queue_id, key_hash)`. Index `enqueue_dedup_expires_at_idx` on `expires_at`.

### `claim_registry`

| Column | Type | Null | Default | Notes |
| --- | --- | --- | --- | --- |
| id | bigint identity | NO | identity | PK |
| claim_id | uuid | NO | — | UNIQUE |
| task_id | uuid | NO | — | |
| claim_token | uuid | NO | — | UNIQUE; secret capability |
| generation | integer | NO | — | CHECK >= 1 |
| claimed_at | timestamptz | NO | — | |
| lease_expires_at | timestamptz | NO | — | CHECK > claimed_at |
| created_at | timestamptz | NO | — | |

### `complete_replay`

Retained name for all terminal worker commands (complete/fail/ack_cancel).

| Column | Type | Null | Default | Notes |
| --- | --- | --- | --- | --- |
| id | bigint identity | NO | identity | PK |
| claim_id | uuid | NO | — | |
| operation_code | smallint | NO | — | CHECK IN (1,2,3) |
| request_fingerprint | bytea | NO | — | octet_length = 32 |
| task_id | uuid | NO | — | |
| result_state_code | smallint | NO | — | CHECK IN (3,10,11,12); 3=retry_scheduled |
| available_at | timestamptz | YES | — | required iff result_state_code = 3 |
| terminal_at | timestamptz | YES | — | required iff result_state_code IN (10,11,12) |
| spawned_task_ids | uuid[] | NO | `'{}'::uuid[]` | |
| event_ids | uuid[] | NO | `'{}'::uuid[]` | |
| created_at | timestamptz | NO | — | |
| expires_at | timestamptz | NO | — | CHECK 1..30 days after created_at |

UNIQUE `(claim_id, operation_code)`. Index `complete_replay_expires_at_idx` on `expires_at`.

Result-shape CHECK: state 3 ⇒ `available_at` NOT NULL and `terminal_at` NULL;
states 10..12 ⇒ `terminal_at` NOT NULL and `available_at` NULL.

### `admin_replay`

| Column | Type | Null | Default | Notes |
| --- | --- | --- | --- | --- |
| id | bigint identity | NO | identity | PK |
| admin_principal_id | text | NO | — | length 1..128 |
| operation_code | smallint | NO | — | CHECK IN (1..13) |
| key_hash | bytea | NO | — | octet_length = 32 |
| request_fingerprint | bytea | NO | — | octet_length = 32 |
| http_status | smallint | NO | — | CHECK 200..299 |
| response_body | jsonb | NO | — | |
| created_at | timestamptz | NO | — | |
| expires_at | timestamptz | NO | — | CHECK 7..90 days after created_at |

UNIQUE `(admin_principal_id, operation_code, key_hash)`. Index
`admin_replay_expires_at_idx` on `expires_at`.

### `completion_effects`

Global ordinal registry. Task rows copy `source_task_id` / `spawn_ordinal` for
lineage.

| Column | Type | Null | Default | Notes |
| --- | --- | --- | --- | --- |
| id | bigint identity | NO | identity | PK |
| source_claim_id | uuid | NO | — | |
| effect_kind_code | smallint | NO | — | CHECK IN (1,2); 1=spawn, 2=event |
| ordinal | integer | NO | — | CHECK >= 0 |
| resource_id | uuid | NO | — | UNIQUE |
| created_at | timestamptz | NO | statement_timestamp() | |

UNIQUE `(source_claim_id, effect_kind_code, ordinal)`.

### `queue_counters`

| Column | Type | Null | Default | Notes |
| --- | --- | --- | --- | --- |
| queue_id | bigint | NO | — | PK; FK → queues(id) ON DELETE CASCADE; no identity |
| delayed_count | bigint | NO | 0 | CHECK >= 0 |
| ready_count | bigint | NO | 0 | CHECK >= 0 |
| leased_count | bigint | NO | 0 | CHECK >= 0 |
| as_of | timestamptz | NO | statement_timestamp() | |

### `partition_maintenance_status`

| Column | Type | Null | Default | Notes |
| --- | --- | --- | --- | --- |
| singleton_id | smallint | NO | — | PK CHECK = 1; no identity |
| last_started_at | timestamptz | YES | — | |
| last_succeeded_at | timestamptz | YES | — | |
| premade_through | date | YES | — | |
| retained_from | date | YES | — | |
| last_error_code | text | YES | — | length 1..128 when non-null |
| last_error_detail | text | YES | — | length at most 4096 when non-null |
| updated_at | timestamptz | NO | statement_timestamp() | |

## Daily UTC RANGE parents

FKs to or from these parents **must not** block detach. CASCADE into history is **not**
allowed.

### `admin_audit_log` — `RANGE (audit_at)`

PK `(id, audit_at)`.

| Column | Type | Null | Default | Notes |
| --- | --- | --- | --- | --- |
| id | bigint identity | NO | identity | part of PK |
| audit_at | timestamptz | NO | — | partition key; part of PK |
| queue_id | bigint | YES | — | no FK |
| actor_id | text | NO | — | length 1..128 |
| operation_code | smallint | NO | — | CHECK 1..13 |
| previous_config_version | bigint | YES | — | |
| new_config_version | bigint | YES | — | |
| request_id | uuid | NO | — | |
| details | jsonb | NO | `'{}'::jsonb` | |

Index `admin_audit_log_queue_audit_idx` on `(queue_id, audit_at DESC, id)`.

### `task_attempts` — `RANGE (claimed_at)`

PK `(id, claimed_at)`. Append-only; UPDATE/DELETE permissions are **not** implied.

| Column | Type | Null | Default | Notes |
| --- | --- | --- | --- | --- |
| id | bigint identity | NO | identity | part of PK |
| task_id | uuid | NO | — | |
| claim_id | uuid | NO | — | |
| generation | integer | NO | — | CHECK >= 1 |
| claimed_at | timestamptz | NO | — | partition key; part of PK |
| worker_id | text | NO | — | length 1..128 |
| lease_expires_at | timestamptz | NO | — | |
| ended_at | timestamptz | YES | — | |
| outcome_code | smallint | NO | — | CHECK IN (1..6) |
| failure_code | text | YES | — | length 1..128 when non-null |
| failure_detail | text | YES | — | at most 4096 when non-null |

Index `task_attempts_task_claimed_idx` on `(task_id, claimed_at DESC, id)`.

### `tasks_terminal` — `RANGE (terminal_at)`

PK `(id, terminal_at)`. There are **no** business result fields.

| Column | Type | Null | Default | Notes |
| --- | --- | --- | --- | --- |
| id | bigint identity | NO | identity | part of PK |
| task_id | uuid | NO | — | |
| queue_id | bigint | NO | — | no history-blocking FK |
| producer_id | text | NO | — | length 1..128 |
| state_code | smallint | NO | — | CHECK IN (10,11,12) |
| priority | smallint | NO | — | CHECK BETWEEN -32768 AND 32767 |
| available_at | timestamptz | NO | — | |
| retry_policy_version | integer | NO | — | CHECK >= 1 |
| payload | jsonb | NO | — | CHECK payload_bytes 1..1048576 |
| payload_bytes | integer | NO | — | |
| created_at | timestamptz | NO | — | |
| terminal_at | timestamptz | NO | — | partition key; part of PK |
| failure_code | text | YES | — | length 1..128 when non-null |
| failure_detail | text | YES | — | at most 4096 when non-null |
| source_task_id | uuid | YES | — | paired with spawn_ordinal |
| spawn_ordinal | integer | YES | — | >= 0 when non-null |

Indexes:

- `tasks_terminal_task_terminal_idx` on `(task_id, terminal_at DESC)`
- `tasks_terminal_spawn_lineage_idx` on `(source_task_id, spawn_ordinal, terminal_at)` WHERE `source_task_id IS NOT NULL`

### `delivery_events_terminal` — `RANGE (terminal_at)`

PK `(id, terminal_at)`.

| Column | Type | Null | Default | Notes |
| --- | --- | --- | --- | --- |
| id | bigint identity | NO | identity | part of PK |
| event_id | uuid | NO | — | |
| source_task_id | uuid | NO | — | |
| ordinal | integer | NO | — | CHECK >= 0 |
| state_code | smallint | NO | — | CHECK IN (10,11) |
| envelope | jsonb | NO | — | |
| envelope_bytes | integer | NO | — | CHECK 1..1048576 |
| created_at | timestamptz | NO | — | |
| terminal_at | timestamptz | NO | — | partition key; part of PK |
| failure_code | text | YES | — | length 1..128 when non-null |
| failure_detail | text | YES | — | at most 4096 when non-null |
| delivery_attempt | integer | NO | 0 | CHECK >= 0 |

Index `delivery_events_terminal_event_idx` on `(event_id, terminal_at DESC)`.

## Explicitly out of scope

- Business result fields / parser-v1
- Delivery transport, brokers, webhooks, CloudEvents wiring
- Per-queue LIST partitions
- DEFAULT partitions on RANGE parents
- HASH partitioning registries (benchmark-driven later)
- Payload GIN indexes
- Queue runtime behavior (claim, enqueue, relay)
