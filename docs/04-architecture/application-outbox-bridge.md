# Application outbox bridge protocol

**Status:** Accepted architecture contract (Phase 6 Plan 01)  
**Requirement:** BRDG-02  
**Companion schema:** [application-outbox-intent.schema.json](schemas/application-outbox-intent.schema.json)  
**Decision record:** [ADR 004 — App-local outbox bridge](adr/004-app-local-outbox-bridge.md)

This document is the normative application-outbox bridge protocol. It defines the
app-owned enqueue-intent envelope, ownership and lifecycle, deterministic queue-service
idempotency mapping, failure and replay windows, lag/health semantics, security
and telemetry constraints, and compatibility rules. It does **not** prescribe an
application framework, ORM, or fixed table name.

## Product position (ADR 004)

This integration is **optional**.

- queue-service always owns its PostgreSQL store. A client business database is
  optional and is not the queue store.
- An application with a business database MAY atomically persist business state
  and enqueue intent in that **application-owned** database, then relay intent to
  queue-service through a bridge.
- A **DB-less** application continues to **enqueue directly** to queue-service and does
  **not** need the bridge or an application database.
- queue-service does **not** own, migrate, or query application tables or schemas.
- There is **no** two-phase commit and **no** distributed transaction between the
  application database and queue-service PostgreSQL.
- The supported result is **eventual, loss-resistant** enqueue: at-least-once
  relay of intent, converging to **one** queue-service task only because enqueue is
  idempotent under the deterministic key below.
- This protocol does **not** claim exactly-once worker execution or exactly-once
  external effects.

## Trust and ownership

| Concern | Owner |
| --- | --- |
| Business mutation + intent insert atomicity | Application transaction |
| App outbox table name, DDL, migrations, retention | Application |
| Bridge process replicas, leasing, retry scheduling | Application / supported bridge |
| Named queue existence, enqueue admission, task identity | queue-service |
| Payload meaning | Application |

Bridge principals are dedicated service principals scoped to idempotent enqueue
on allowed named queues (see [security contract](../05-operations/security.md)).
Producer identity of the bridge principal is part of queue-service idempotency scope.

## Intent envelope

Every unpublished row carries an immutable enqueue-intent document that MUST
conform to [application-outbox-intent.schema.json](schemas/application-outbox-intent.schema.json).

### Required fields

| Field | Meaning |
| --- | --- |
| `schema_version` | Envelope major version. Supported major for this revision is `1`. |
| `source_namespace` | Application-selected opaque namespace scoping `source_row_id`. |
| `source_row_id` | Opaque application-owned outbox row identity within the namespace. |
| `target_queue` | Named Work Queue for enqueue. |
| `enqueue_request` | queue-service enqueue body (`payload`, `priority`, optional `available_at`). |
| `created_at` | Intent creation timestamp (UTC RFC 3339). |

### Optional fields

| Field | Meaning |
| --- | --- |
| `traceparent` | Optional W3C `traceparent` propagated on queue-service enqueue. |
| `tracestate` | Optional W3C `tracestate` propagated on queue-service enqueue. |
| `extensions` | Additive extension object; unknown keys are ignored by older bridges. |

Identifiers (`source_namespace`, `source_row_id`) are opaque UTF-8 strings. They
MUST NOT be trimmed or case-folded. Whitespace is significant. Control characters
(U+0000–U+001F, U+007F) are rejected as mutation-prone / ambiguous wire forms.
Empty strings are rejected. Numeric or null identifiers are rejected.

After the application transaction that first publishes the intent commits,
`source_namespace`, `source_row_id`, `target_queue`, and `enqueue_request` are
**immutable**. A changed queue-service request fingerprint for the same identity is a
surfaced conflict (`idempotency_conflict` or equivalent), never a second task.

## Transaction boundary

```text
BEGIN application transaction
  mutate business state
  INSERT enqueue intent (pending)
COMMIT

bridge (after commit only):
  claim/lease pending intent under app-DB time
  POST queue-service enqueue with deterministic Idempotency-Key
  on queue-service success → mark delivered in app DB
  on retryable queue-service/network failure → schedule retry in app DB
  on permanent fingerprint conflict → surface terminal operator action
```

Rules:

1. Business mutation and intent insert MUST commit in **one** application-DB
   transaction.
2. The bridge MUST perform network enqueue **only after** that commit.
3. The bridge MUST mark the intent **delivered** only after queue-service reports success
   for the immutable request (including idempotent replay of the same fingerprint).
4. The bridge MUST NOT hold an open application transaction across queue-service network
   I/O.
5. queue-service never reads the application outbox.

## Deterministic idempotency mapping

For schema major `1`, the queue-service `Idempotency-Key` is:

```text
bridge:v1: || lowercase(base64url_nopad(SHA-256(canonical)))
```

where `canonical` is the UTF-8 **length-prefixed** tuple
`(source_namespace, source_row_id)`:

```text
canonical =
    uint32_be(len(UTF8(source_namespace))) || UTF8(source_namespace) ||
    uint32_be(len(UTF8(source_row_id)))    || UTF8(source_row_id)
```

- `uint32_be` is a 4-byte big-endian unsigned length of the following UTF-8 bytes.
- `base64url_nopad` is Base64URL without `=` padding; the encoded digest is then
  lowercased.
- The mapping MUST NOT include payload, `target_queue`, timestamps, replica IDs,
  credentials, or random values.

queue-service still scopes the key by the authenticated bridge principal and the target
named queue. Distinct length-delimited identities such as `("ab","c")` and
`("a","bc")` MUST produce different keys. The resulting key length is within the
queue-service 1..256 character `Idempotency-Key` limit.

When queue-service accepts the first enqueue, later replays with the same key and the
same normalized request fingerprint return the original task. A changed
fingerprint for that key is an `idempotency_conflict` (or equivalent permanent
conflict): the bridge MUST surface it as a terminal operator-action state and
MUST NOT rewrite the intent into a different enqueue body to “force” a second
task.

## Application-owned lifecycle (behavioral)

States are behavioral; applications choose physical storage.

| State | Meaning |
| --- | --- |
| `pending` | Durable intent awaiting bridge claim. |
| `leased` / `processing` | A bridge replica holds a time-bounded ownership lease. |
| `delivered` | queue-service enqueue succeeded (including idempotent replay). |
| `retryable_failure` | Transient/retryable outcome; retry after backoff. |
| `terminal_operator_action` | Permanent conflict or exhausted policy; human/ops action. |

### Transitions

1. Insert → `pending` inside the business transaction.
2. Bridge claims → `leased`/`processing` with opaque rotating lease token and
   expiry under **application-DB time**.
3. queue-service success → `delivered`.
4. Retryable queue-service/network/error-model outcome → `retryable_failure` then back to
   `pending`/`leased` per schedule.
5. Permanent fingerprint conflict or exhausted local policy →
   `terminal_operator_action`.

### Concurrent replicas

Multiple bridge replicas MAY run. Claiming MUST ensure at most one **current**
owner per intent (for example `FOR UPDATE SKIP LOCKED` or equivalent). Abandoned
leases become reclaimable after expiry using application-DB time. Stale owners
MUST NOT successfully mark delivered or failed after losing the lease.

### Retry classification

Bridges classify queue-service structured errors using queue-service’s explicit `retryable`
contract ([error model](error-model.md)). Retryable outcomes back off; non-retryable
fingerprint conflicts terminate to operator action. Uncertain transport outcomes
(timeout after possible commit) MUST replay the **same** idempotency key and
immutable body.

### Crash windows and replay

| Window | Behavior |
| --- | --- |
| Crash before app commit | No intent; business change absent — correct. |
| Crash after app commit, before enqueue | Intent remains pending; bridge replays. |
| Crash after queue-service success, before mark-delivered | Replay same key; queue-service returns original task; bridge marks delivered. |
| Crash while leased | Lease expires; another replica reclaims. |

Replay is at-least-once HTTP toward queue-service. Loss resistance comes from durable
intent plus idempotent enqueue — not from a distributed transaction.

### Retention and tombstones

Applications MUST bound retention of delivered and terminal rows (delete or
tombstone) so lag and depth queries stay healthy. Tombstones, if used, MUST
preserve enough identity to prevent accidental reuse of `(source_namespace,
source_row_id)` within the application’s uniqueness window. Exact retention
durations are application policy; queue-service enqueue dedup TTL remains queue-service’s
(ADR 017 / OpenAPI).

## Lag, health, and readiness

- **Lag** is `now - oldest pending created_at` using a consistent clock basis
  documented by the bridge (prefer application-DB `now()` for the pending set).
- **Pending depth** is a bounded count of non-delivered intents (cap/truncation
  MUST be explicit when scanned with a depth cap).
- **Health / readiness** of a bridge process reflects: application-store
  connectivity for claim/snapshot queries, ability to reach queue-service enqueue, and
  absence of sustained critical invariant errors. The bridge **cannot** promise
  zero lag.
- Readiness MAY fail when the app store is unreachable or when lease/claim
  machinery cannot run; lag alone is an SLO/alert signal, not automatically an
  unready process unless operator policy says otherwise.

## Observability and security constraints

Keep payload bodies, `source_namespace`, `source_row_id`, idempotency keys, and
free-text error detail **out of metric labels and default logs**. Prefer bounded
labels (queue, result, retryability) and redacted correlation. Align with
[observability](../05-operations/observability.md) and
[security](../05-operations/security.md) contracts. Traces may propagate W3C
context from optional intent fields without logging payloads.

## Compatibility and rolling upgrade

- **Major (`schema_version`)**: unsupported majors MUST be rejected; do not guess.
- **Additive fields**: new optional properties or `extensions` keys under the same
  major MUST be ignored by older bridges (tolerant readers).
- **Unknown enum / state labels**: treat unrecognized lifecycle labels as
  non-claimable and surface for operator upgrade — do not silently drop intent.
- **Rolling upgrade**: mixed old/new bridge replicas MUST safely claim/reclaim
  via app-DB leases; both MUST compute the same `bridge:v1:` key for the same
  identity. Changing the mapping algorithm requires a new key prefix / major.

## Non-goals

- This protocol does not claim exactly-once execution or exactly-once external
  effects.
- This protocol does not use distributed transactions or 2PC across app DB and
  queue-service.
- queue-service does not own application tables.
- An application database is not mandatory for queue-service users; DB-less direct
  enqueue remains supported.
- This protocol does not prescribe one application framework or ORM.

## References

- [ADR 004 — App-local outbox bridge](adr/004-app-local-outbox-bridge.md)
- [Transactional outbox concept](../01-concepts/transactional-outbox.md)
- [Guarantees](../01-concepts/guarantees.md)
- [Product boundary](../01-concepts/product-boundary.md)
- [Data flow](data-flow.md)
- [Client protocol](client-protocol.md)
- [Error model](error-model.md)
- OpenAPI `EnqueueTaskRequest` / `Idempotency-Key` (1..256)
