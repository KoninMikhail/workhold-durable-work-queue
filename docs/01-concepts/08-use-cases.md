# Product scenarios

[Documentation](../README.md) › [Concepts](README.md) › **Use cases**

These scenarios define what the product must support. They intentionally avoid HTTP paths and physical tables.

| Group | Scenarios |
| --- | --- |
| Producer / intake | [UC-1](#uc-1-application-without-a-database-enqueues-work), [UC-8](#uc-8-application-with-a-business-database) |
| Worker / execution | [UC-2](#uc-2-many-replicas-compete-for-tasks) … [UC-7](#uc-7-cooperative-cancellation) |
| Operators | [UC-9](#uc-9-operators-observe-and-recover) … [UC-13](#uc-13-a-producer-and-an-operator-inspect-work) |

## UC-1 Application without a database enqueues work

1. Producer chooses a named queue and a stable idempotency key.
2. It sends an opaque payload with `priority` (default `0`, optional `-32768`…`32767`) and an optional **one-shot** `available_at` (aware RFC 3339 / SDK `datetime`).
3. Omitted/null/past/current `available_at` → immediate (`ready`) by workhold store time; aware future within `QUEUE_SCHEDULE_HORIZON_SECONDS` (default/max **86400**, range **0..86400**) → `delayed` until the time is reached.
4. Naive timestamp, a future beyond the horizon, or `priority` outside the range / not integer → `validation_failed`, not silent ignore.
5. workhold commits the task before acknowledgement success.
6. Retry with the same key and the same request returns the original task.
7. Reuse key with a different request is rejected.

The application does not create its own business DB only for workhold: the queue store already belongs to the service. This is a **one-shot delay**, not cron, calendar recurrence, or per-task retry override.

## UC-2 Many replicas compete for tasks

1. Worker replicas subscribe to one or more named queues they can process.
2. workhold issues one active lease for each claimed task.
3. workhold records `claimed_at` as workhold store time and the claiming `worker_id`.
4. The claim response returns this metadata for logs and task diagnostics.
5. Long work heartbeats until the lease expires.
6. A crashed or partitioned worker loses the lease; another replica may claim the task.
7. A stale worker cannot heartbeat or finish workhold state with the old token.

The business handler stays idempotent: external work may already have happened before the lease was lost.

## UC-3 A successful worker spawns more work

1. The worker finishes the claimed task.
2. In one workhold transaction the source task becomes succeeded and zero or more follow-up tasks are created in named queues.
3. A repeat complete with the same claim returns the same tasks.
4. There is no partial state in which the source succeeded and the spawns were lost.

Spawn is a Work Queue operation, not a Delivery Outbox publication. What a follow-up is in plain words — [05-follow-up.md](05-follow-up.md).

## UC-4 A successful worker writes outbound events

1. The worker completes the task with zero or more outbound events.
2. The source transition, spawns, and Delivery Outbox records commit together.
3. The relay publishes events after commit and records successful delivery.
4. If publication succeeded and the acknowledgement state was lost, the event may be published again.
5. Downstream consumers deduplicate by the stable event ID.

## UC-5 Retryable processing failure

1. The worker reports a stable machine-readable `failure_code` and an optional diagnostic detail, or its lease expires.
2. workhold records the attempt outcome and moves the task to `delayed` with `available_at = workhold store now + retry_delay_seconds` according to the named queue retry policy.
3. The task is not claimable until `available_at <= transaction_timestamp()`; after it is due, claim moves `delayed` **directly** to `leased` without a promotion job.
4. Attempts are limited; exhausted work becomes a dead letter.

The retry policy belongs to the named queue configuration, not the task payload. It is versioned and configures `enabled`, `max_attempts`, and backoff. The initial policy supports fixed delay. The task snapshots the active policy version at enqueue.

If retry is disabled, the first worker failure or lease expiry dead-letters the task. workhold still records the failed attempt; the work does not disappear silently.

## UC-6 Non-retryable failure and dead letter

1. The worker reports a non-retryable failure, or the retry policy is exhausted.
2. workhold records a terminal dead-letter outcome and the failure code.
3. Operators can view counters and the saved diagnostic details.
4. Replay, if supported, creates a new auditable attempt/task and does not quietly rewrite history.

## UC-7 Cooperative cancellation

1. The producer requests cancellation of a delayed, ready, or leased task.
2. A delayed or ready task becomes terminal immediately and is never claimed.
3. A leased task shows the cancellation to the worker on heartbeat/checkpoints.
4. The current worker confirms cooperative cancellation with an idempotent `ack_cancel`; lease expiry also finalizes a pending cancellation.
5. Complete and cancellation compete by one documented winner rule.
6. A cancelled task does not create spawns or events after cancellation wins.

## UC-8 Application with a business database

1. The application transaction changes business state and inserts an app-local outbox row.
2. The bridge reads that row after commit and enqueues into the named queue.
3. The app-outbox row ID produces the workhold idempotency key.
4. The bridge marks the row delivered only after acknowledgement of the durable enqueue from workhold.
5. Crashes and retries produce at most one workhold task for that outbox row.

Delivery is eventual; a distributed transaction is not implied.

## UC-9 Operators observe and recover

Operators can determine:

- depth ready, delayed, leased, and dead-letter by named queue;
- the age of the oldest ready task;
- throughput enqueue, claim, retry, success, and dead-letter;
- distributions of wait and processing latency;
- the number of pending events and the oldest lag of delivery-relay;
- the active version, schema readiness, and unhealthy dependencies.

Statistics have an `as_of` time and documented freshness. They are not arbitrary queries over payload fields.

## UC-10 Safe deploy and upgrade

1. One-shot migration role updates the workhold-owned schema.
2. API replicas become ready only against a compatible schema.
3. Rolling upgrade API does not require sticky sessions.
4. Shutdown stops new claims, drains limited in-flight, and lets abandoned leases expire.
5. PostgreSQL recovery can produce safe redelivery or republish events, but not a silent loss of acknowledgement inside the documented recovery point.

## UC-11 An operator controls the queue runtime

1. Admin explicitly creates a named queue and its initial immutable policy.
2. Pause stops new claims, while producers may continue to enqueue.
3. Drain rejects new external enqueue, while claims and internal task chains finish.
4. Policy/state changes use expected config version and write audit atomically.
5. Delivery Outbox publication proceeds independently of Work Queue state.

## UC-12 workhold protects PostgreSQL

1. Oversized payload/fan-out is rejected before the write transaction.
2. Active-depth admission uses transactional counters, not hot-table scans.
3. Idempotent replay succeeds even if the queue is currently full or draining.
4. On overload, enqueue is throttled, while claims continue to drain the backlog.
5. A dependency failure returns explicit retryability and never local success.

## UC-13 A producer and an operator inspect work

1. Producer finds its task by task ID or scoped idempotency key.
2. The operator views state, the current claim summary, attempts, and the dead-letter reason.
3. Full claim tokens are visible only to the worker that holds them.
4. workhold returns spawn/event lineage, but does not store the application's business result.
5. Replay dead-letter creates a new auditable task, **keeps source `priority`**, and lineage source.

---

← [Product boundary](07-product-boundary.md) · [Guarantees](09-guarantees.md) →
