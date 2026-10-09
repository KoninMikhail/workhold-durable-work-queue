# Delivery relay published twice

[Documentation](../README.md) › [Troubleshooting](README.md) › **Duplicate publish**

**What you see.** The same delivery event arrived at the webhook twice
(two POSTs with one CloudEvents `id`). The consumer applied the effect a
second time — a duplicate email, a second callback, a repeated write.

**This is expected at-least-once.** The publish may have succeeded while the
acknowledgement in the relay store did not (crash, timeout, restart). The
relay retries the same event.

## Why

The event path:

1. On complete the worker submits `events[]`. Pending delivery outbox rows
   appear in the workhold transaction. That is an intention, not a
   publication.
2. The `relay` role claims pending rows with its own lease.
3. The relay does an HTTP POST to the one webhook of the deployment.
4. It writes an ack that the delivery is recorded.

Between 3 and 4 the process can die. For the store the event is still
unacknowledged, so it is published again. The same `id`, the same envelope.

This is not "complete created two events". A retry of complete with the same
claim and body does not create a second delivery outbox *row*. The duplicate
here is a repeated *publication* of one row.

An indeterminate HTTP outcome (a timeout: it is unclear whether the sink
accepted it) is retried too. For the relay, "it might already have arrived"
means "send it again".

An exhausted relay retry goes to the delivery dead letter. That tail is
separate from the Work Queue task dead letter.

## What to do

Downstream **must** deduplicate on the stable `id` that workhold assigns
(not a client uuid from `data`).

The classic pattern is an inbox on the consumer:

1. In one transaction: `INSERT inbox (event_id)` plus the business change.
2. If `event_id` is already there, do not apply the effect; respond with
   success to the relay.
3. Ack the channel only after that transaction.

Without its own database, use a naturally idempotent operation on the X side
("create a resource whose key is the event id") or CAS in an external store.

Two different events from one complete have two different `id` values. They
must not be collapsed. One event that the sink fans out to X and Y carries
a single `id` — X and Y deduplicate it as one message. Why there are usually
two events: [15-events-to-two-services.md](../06-faq/15-events-to-two-services.md).

## What not to do

| Do not | Why |
| --- | --- |
| Deduplicate by time, payload, or "similar data" | Unstable; two different events can be merged |
| Blame the relay for the second POST | That is how at-least-once works |
| Turn off relay retries "so there are no duplicates" | Delivery is lost on a crash after a timeout |
| Expect exactly-once from workhold | The product does not promise that |

How the inbox works: [11-inbox.md](../01-concepts/11-inbox.md).
Why there is no exactly-once: [05-exactly-once.md](../06-faq/05-exactly-once.md).
