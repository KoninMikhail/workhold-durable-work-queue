# Does Workhold have exactly-once?

[Documentation](../README.md) › [FAQ](README.md) › **Exactly-once**

**In short.** No. queue-service promises **at-least-once**: a task may
run again if the lease expired or the worker did not finish the work in time.
External effects (HTTP, a write to someone else's database, an email) may repeat too.
The service does not guarantee exactly-once on the whole path "queue → outside world".

## What is promised, and what is not

Inside its own PostgreSQL, queue-service does not lose work between steps:
a successful enqueue means "the task is already in the store"; a successful complete with
`spawn[]` / `events[]` writes the source task and the descendants in one transaction.

Execution at the worker and publication outward are different. Between "took the task" and
"closed the task", the worker talks to the outside world *outside* the queue's transactions.
If it died after the HTTP call but before complete, the lease expires and another worker
takes the same task. If the relay published the event and died before writing the ack,
the same event goes out again.

| Segment | Promise |
| --- | --- |
| Enqueue committed | The task exists; a retry of the same key and body returns it |
| Claim | At most one active lease; after expiry, a new attempt |
| Complete with the same claim and body | No second spawn or events |
| External HTTP / email / someone else's database | May repeat |
| Delivery Relay publication | At-least-once to the channel |

## What the application must do

Idempotency of effects is the application's duty, not the queue's:

- **Inbox** at the consumer: in one transaction, "already saw this id" plus
  the business change. A retry of the same id does not apply the effect.
- **A natural unique key** in the external API ("create a payment with
  `idempotency_key=order-42`").
- **Compare-and-set**: accept the effect only if the state is still
  "not applied".

`claim_token` protects state *inside* queue-service. It is not
a fence for the outside world: another service does not know about your lease.

## Common mistakes

| Mistake | Why that is wrong |
| --- | --- |
| "Since complete is atomic, the effect happens exactly once" | The effect happened before complete, outside the queue transaction |
| "We'll put a unique index in queue-service on the payload" | The queue does not interpret the business meaning of the payload |
| "The relay promises exactly one delivery" | Publish may have succeeded; the ack may not |

What to do if the effect already happened and the lease was lost:
[04-side-effect-then-lease-lost.md](../07-troubleshooting/04-side-effect-then-lease-lost.md).

---

Guarantee matrix: [guarantees.md](../01-concepts/09-guarantees.md).
Receiving pattern: [inbox.md](../01-concepts/11-inbox.md).
