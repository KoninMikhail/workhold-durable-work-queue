# Multi-replica concurrency

The workhold API, workers, and delivery relays may have several replicas. PostgreSQL is
the serialization point; process-local locks are never correctness
mechanisms.

## Task claim

The claim transaction:

1. filters by named queue and the claimable predicate (**due gate**):
   `(state_code IN (delayed, ready) AND available_at <= transaction_timestamp())
   OR (state_code = leased AND lease_expires_at <= transaction_timestamp())`;
2. among **eligible** candidates (the due gate is already applied) orders by scheduling
   policy: **`priority` DESC**, then **`available_at` ASC**, then **`id` ASC**;
3. locks candidates without waiting for already locked rows;
4. updates one candidate with a new claim token, generation, workhold store `claimed_at`,
   a diagnostic `worker_id`, and lease expiry;
5. adjusts counters: `delayed→leased` or `ready→leased`, then
   `leased_count +1`;
6. appends an attempt record with the same claim metadata;
7. commits before returning the task.

A suitable PostgreSQL implementation is `FOR UPDATE SKIP LOCKED`, usually in one
short `UPDATE ... FROM (...) RETURNING`. The normative serialization contract and
the exact SQL are Phase 3 storage decisions.

**Direct due claim:** when workhold store time reaches `available_at`, a task in
`delayed` is claimable without a promotion job or an intermediate `ready` transition.
Retry-scheduled work (a retryable fail or lease expiry with a positive
`retry_delay_seconds`) stays in `delayed` until `available_at`; once due, it
uses the same claim predicate. Before the time is reached, claim returns an empty
array without mutating counters or attempt rows.

The index `tasks_active_claim_idx (queue_id, state_code, priority DESC, available_at, id)`
covers both active-state branches of the predicate and matches priority-first tuple order.

## Non-preemption and starvation

Due eligibility is **always before** static priority: a task with a future `available_at` or an
unexpired lease is outside the eligible set and is **not preempted** by a later
enqueue with a higher `priority`.

Expired leased rows (reclaim) take part in the **same** tuple ordering among due
candidates. Under a continuous stream of higher-priority due work, a low `priority`
may **starve**. That is the expected behavior of static priority without fairness/aging.

## Lease invariants

- A task has at most one current claim.
- Each claim and reclaim uses a new opaque token.
- A monotonically increasing generation identifies the attempt.
- workhold store time, not the worker, decides expiry and due eligibility.
- Heartbeat resets expiry from the current workhold store time; it does not add to
  the old deadline.
- Heartbeat, complete, and fail match task, token, generation, and the active lease.
- `worker_id` comes from the authenticated caller identity or an explicitly validated
  worker-instance identifier; it is not derived from an arbitrary payload.
- Worker identity is diagnostic metadata only and does not authorize a mutation.

The token fences workhold state. A stale worker can still call an
external system after losing the lease, so external idempotency is mandatory.

## Complete transaction

In one workhold transaction:

1. lock and validate the current unexpired claim;
2. validate the terminal request fingerprint;
3. move the source task to a terminal state;
4. close the attempt;
5. insert zero or more spawned tasks;
6. insert zero or more pending delivery events;
7. store the response required for idempotent replay.

There is no network publication in this transaction.

A retry with the same claim and body returns the stored result. The same claim with
a different body is rejected. Uncertain API outcomes therefore cannot create
duplicate spawns or events.

Spawn `available_at` follows the same bounded scheduling policy as producer
enqueue (aware RFC 3339, workhold store horizon, `delayed`/`ready` persistence).

## Retry and dead-letter transition

A retryable failure assigns `available_at` from server policy and leaves the task
in `delayed` until `available_at <= transaction_timestamp()`. Reclaim after an expired
lease records an expired attempt. When the attempt limit is exhausted, the same
transaction marks the task dead and records a diagnostic reason.

Poison work must not retry forever.

## Delivery relay

Delivery events use an independent claim token, generation, and lease. Relay
replicas claim pending or expired-publishing rows with the same non-blocking pattern.

Publication and acknowledgement cannot be one transaction across PostgreSQL
and an external channel:

1. claim and commit the event;
2. publish with a stable event ID;
3. mark acknowledged if the relay claim is still current.

If the relay crashes after step 2, publication may be repeated. A downstream inbox
or idempotency handles the duplicate.

## Mandatory crash tests

| Injection | Expected result |
| --- | --- |
| API dies before or after the enqueue commit | No task, or one task on idempotent retry |
| Worker dies after claim | The lease expires; another attempt may claim |
| Stale worker complete after reclaim | Rejected; no duplicate workhold transition |
| API dies after the complete commit | The same claim/body returns the original result |
| Two replicas complete one claim | One commit; the other sees replay/conflict |
| Relay dies after publish, before acknowledgement | The event may be published again |
| PostgreSQL unavailable | API readiness fails; no split-brain writes |
| Claim before `available_at` of a delayed task | Empty claim; counters and attempts unchanged |
| Two replicas claim one due-delayed task | Exactly one lease; counters converge |

## Performance limits

- Claim, heartbeat, and terminal transactions stay short and contain no
  application work.
- The pool budget is explicit: every API and relay pool must fit within PostgreSQL
  connection limits.
- Depth and statistics queries do not scan or lock the hot claim path on every
  request.
- Payload and fan-out limits bound transaction size and WAL amplification.
- Ordering guarantees are not extended without load tests and a separate scheduling decision (out of scope: fairness/aging).
