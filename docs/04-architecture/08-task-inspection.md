# Task inspection and the result boundary

**Status:** Accepted architecture

queue-service stores the operational outcome, not the application's business result. The worker
returns business output through application storage, `spawn[]` tasks, or
Delivery Outbox `events[]`. There are no `result`/`parse_result` fields.

The replay result held by queue-service is protocol metadata: the terminal state,
the fingerprint, and the IDs and ordinals of created spawns and events.

## Addressability

- a task by the public `task_id`;
- a producer submission by `(producer identity, queue, idempotency key)`;
- the claim token only for heartbeat and terminal commands;
- an event by the public `event_id`;
- attempts by a required `task_id`.

queue-service does not support filtering by payload fields, arbitrary business-key
lookup, or free-text search.

## Task representation

The inspection representation includes:

- ID, named queue, producer identity, and creation time;
- state, priority, available time, and retry policy version;
- the opaque payload where the caller's rights allow it;
- a current-claim summary: generation, `claimed_at`, worker ID, expiry, and
  cancellation request;
- terminal time and outcome, and the latest failure code and detail;
- counts and IDs of spawned tasks and delivery events.

The full claim token is visible only to the worker that holds it, and general
producer or admin inspection never returns it.

## Attempt history

Append-only attempts expose claim generation and time, worker ID, lease window,
outcome, failure code and detail, and end time. Pagination is cursor-based on
`(claimed_at, attempt_id)` and requires a time or task bound compatible with partition
pruning.

## Visibility

- producer: fetch tasks created in its own scope, resolve the idempotency key,
  cancel;
- worker: payload and the current claim for processing, without arbitrary listing;
- observer/admin: bounded task/attempt/dead-letter inspection, claim token
  redacted;
- metrics reader: aggregates only.

## Retention

Inspection is limited by the configured retention. After expiry, lookup may return
`task_not_found`; queue-service is not a permanent result archive. The enqueue-dedup
and complete-replay registries have explicit independent TTLs and may outlive fact rows
during retry windows.

Dead-letter replay creates a new task linked to the immutable source terminal
row. It never changes the original outcome in place.
