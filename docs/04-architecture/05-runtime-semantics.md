# Queue runtime semantics

**Status:** Accepted architecture

## Named queue lifecycle

Named queues are created explicitly through the private admin plane **or** one-shot
`workhold apply` from a mounted catalog (`QUEUE_CATALOG_PATH`). The catalog is
deployment configuration (ensure-exists create), **not** runtime desired state:
it does not control `active|paused|draining` and does not activate policy versions.
Producer enqueue never creates a queue implicitly: a typo must fail and must not
spawn an unobserved backlog.

| Operation | `active` | `paused` | `draining` |
| --- | --- | --- | --- |
| External enqueue / bridge | allowed | allowed | rejected |
| Internal `spawn[]` from accepted complete | allowed | allowed | allowed |
| Claim | allowed | empty result | allowed |
| Heartbeat / complete / fail | lease rules | lease rules | lease rules |
| Cancel | allowed | allowed | allowed |
| Delivery relay | independent | independent | independent |

`paused` stops processing while allowing the backlog to grow. `draining` closes
external intake while existing work and its descendants finish. Internal
spawns stay allowed during drain so a valid complete is not blocked after the
worker has already performed application work.

Drain is complete when delayed + ready + leased tasks equal zero. Delivery-event
backlog is reported separately and does not block work queue drain completion.
There is no automatic state transition after drain; the operator explicitly chooses
the next state.

Queue state does not revoke existing leases. Process shutdown is separate from
persisted queue state and must not mutate it.

## State transitions

Every transition uses the expected `config_version`, increments it, and
appends audit in the same transaction:

- `active ↔ paused`;
- `active|paused → draining`;
- `draining → active|paused`.

## Idempotency and state gates

Enqueue resolves an existing idempotency record before applying the current queue
state gate. A retry of a previously committed enqueue returns the original task even when
the queue is now draining. A genuinely new enqueue is rejected.

A claim while paused is a successful empty claim response with queue-state metadata,
not an operational error.

## Cancellation

- delayed/ready task: terminal cancellation immediately;
- leased task: persist the cancellation request; heartbeat surfaces it;
- the current worker acknowledges cancellation with a dedicated idempotent `ack_cancel`;
- complete succeeds only if it validates before the cancellation is recorded;
- lease expiry with cancel requested yields cancelled, not another claim;
- cancellation never creates spawns or delivery events.

## Runtime versus deployment settings

Runtime-mutable:

- queue state;
- the active immutable retry policy version;
- soft quotas introduced by the operations phase.

Deployment only:

- DDL and partition layout;
- PostgreSQL connectivity and pool ceilings;
- hard security, payload, and request limits;
- listener/TLS configuration;
- the mounted named-queue catalog path plus one-shot `workhold apply` (not live reconcile).
