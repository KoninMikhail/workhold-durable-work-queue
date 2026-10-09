# Guarantees: what Workhold promises and what it does not promise

[Documentation](../README.md) › [Concepts](README.md) › **Guarantees**

This page answers the question: **on which stretch of the path something can be treated as done, and where it cannot**.

Guarantees are tied to a boundary — to one step between the producer, workhold, the worker, and the delivery channel. No statement below means that a task passed the whole path from enqueue to the external effect exactly-once.

> **In short.** workhold reliably stores work in its own store and does not lose it between steps inside its PostgreSQL. Execution at the worker and publication outward are **at-least-once** (at least once, sometimes again). The service does not promise Exactly-once external effects.

## Guarantee matrix

| Stretch | What is guaranteed | How it is done |
| --- | --- | --- |
| Producer → workhold | A "success" response means the task is already in the store | `enqueue` reports success only after commit in workhold PostgreSQL |
| Repeat `enqueue` | At most one task per producer, named queue, key, and request body | A unique key in that scope plus the request fingerprint |
| workhold → workers | The task is delivered at least once | If the lease expired, unfinished work can be taken again |
| Several workers at once | A task has at most one active lease | An atomic `claim` and a new claim token on every capture |
| Worker → workhold | A repeat of the same terminal command does not change the outcome | The stored complete result and the request fingerprint |
| Complete → `spawn[]` | In the workhold store a follow-up appears at most once per accepted claim | The original task and spawned tasks are written in one transaction |
| Complete → `events[]` | The intent to deliver an event is written at most once per accepted claim | The original task and the Delivery Outbox rows are in the same transaction |
| Relay → external channel | at-least-once publication | Publish first, then record the acknowledgement; an indeterminate outcome is a repeat |
| Application business DB → workhold | The enqueue arrives eventually, even if the response was lost | App-local outbox plus a deterministic idempotency key in workhold |

## What workhold does

### Enqueueing work

- Writes a new task to its store **before** the successful `enqueue` response.
- Requires a named queue that already exists. A typo in the name does not create a queue.
- Rejects a repeat of the same idempotency key with a **different** normalized body.
- A repeat of the same key with the same body returns the existing task — while current admission limits and drain mode allow it.
- Explicitly stores `priority` (default `0`, inclusive `-32768`…`32767`, strict integer) and `available_at`. Unsupported scheduling values are rejected, not silently replaced.

### Issuing work and the lease

- Only one current lease can mutate the task.
- Issues a new claim token on every `claim` and on every re-capture.
- On every `claim` records `claimed_at` from its store clock and a diagnostic `worker_id`.
- Checks the lease deadline against its store time, not the worker clock.
- Rejects `heartbeat`, `complete`, and `fail` from an expired or already replaced claim.

Worker identity (`worker_id`) does **not** grant the right to change the lease. Only the current claim token and generation move state in workhold.

### Completion, retry, and history

- `ack_cancel` idempotently finishes work only for the current claim and only if the cancellation request is already recorded.
- Writes a stable machine-readable code for every reported processing failure.
- A repeat of the same terminal command with the same claim and the same body returns the original result: no second `spawn[]` and no second `events[]`.
- Commits a successful `complete`, spawned tasks, Delivery Outbox records, and the attempt outcome atomically. Broker calls, callbacks, and any other network I/O are not part of this transaction.
- Limits retry by the named queue policy: timings and the attempt limit come from the queue configuration. Exhausted work goes to dead letter.
- Captures the retry-policy version at `enqueue`. A later configuration change does not rewrite an existing task.
- If retry is disabled, records the failed first attempt and moves it to dead letter immediately.
- Keeps enough attempt history and terminal outcomes to explain a retry within the configured retention.

### Limits and access

- Checks hard deployment limits **before** an unsafe payload or an overly wide fan-out reaches the store.
- Authenticates the service principal and checks the right to the operation in the scope of a specific named queue.

## What the application must do

| Role | Duty |
| --- | --- |
| **Producer** | On operations that may repeat (a retry after a timeout, a client retry), passes a stable idempotency key |
| **Worker** | Assumes the same task may be executed more than once. Extends the lease with a heartbeat if the work may not finish in time. After a "lease lost" response, no longer changes state in workhold. Makes external effects idempotent — itself, or through an inbox at the consumer, natural uniqueness, or compare-and-set |
| **Delivery consumer** | If a repeat publication would produce a wrong effect, deduplicates by the stable event identifier |
| **Application with a business DB** | If the business change and the intent to enqueue a task must appear together, writes an app-local outbox in its own transaction. There is no distributed transaction with workhold PostgreSQL |

## What the service does not promise

workhold does **not** guarantee:

- exactly-once execution of a task at the worker;
- exactly-once external side effects;
- atomicity between its PostgreSQL and another database;
- exactly-once delivery to a broker, an HTTP endpoint, or a subscriber;
- strict global FIFO when several workers process one queue at once;
- order across different named queues;
- availability if workhold PostgreSQL is unavailable;
- payload schema validity and application permissions inside the payload;
- durable storage or search of the application's business result;
- infinite retention and arbitrary replay of old events;
- pub/sub delivery to several independent subscribers.

## What happens on failures

| What broke | What you will see |
| --- | --- |
| The API crashed before the `enqueue` commit | There is no task. The producer repeats with the same key |
| `enqueue` committed and the response was lost | The repeat returns the existing task |
| The worker died after `claim` | The lease expires and another attempt starts |
| The worker already made an external effect and lost the lease | The effect may repeat. Idempotency is required on the application side |
| `complete` committed and the response was lost | The same claim and the same body return the stored result |
| The relay published the event and died before recording success | The same event may be published again |
| The bridge enqueued the task and died before marking the app outbox | The deterministic key returns the existing task in workhold |

## Task order

Claim considers only **due** tasks: `(delayed|ready AND available_at <= store time) OR (leased AND lease expired)`. Among eligible candidates the **scheduling** order is: `priority` DESC, then `available_at` ASC, then `id` ASC. At equal `priority`, the earlier `available_at` becomes claimable sooner, then the smaller `id`.

Future `available_at` values and **unexpired** leases are **not** displaced by a later enqueue with a higher `priority`. Expired leases compete in the same tuple; under a continuous stream of higher-priority due work, a low `priority` may **starve**.

Replay/dead-letter creates a new task and **keeps the source `priority`** (and scheduling fields) per the operations contract.

This is **scheduling** order, not a promise that `complete` order matches `enqueue` order. Strict sequential order is a separate accepted design; there is no such contract now. Out of scope: bands, aging, weighted queues, reclaim quota, lease preemption, priority filters of inspection.

## Operational statistics

Counters and slices are snapshots, not the source of truth for correctness:

- each response states the `as_of` moment;
- how fresh the figures are and how they are reconciled is described separately;
- counters may lag behind task rows;
- success of `enqueue`, `claim`, or `complete` **never** depends on whether statistics are available.

## Where next

- End-to-end task path — [03-how-it-works.md](03-how-it-works.md).
- Product boundary — [07-product-boundary.md](07-product-boundary.md).
- How to close a repeat on reception — [11-inbox.md](11-inbox.md).
- How not to lose an enqueue from the business DB — [10-transactional-outbox.md](10-transactional-outbox.md).
- Terms — [06-glossary.md](06-glossary.md).

---

← [Use cases](08-use-cases.md) · [Transactional outbox](10-transactional-outbox.md) →
