# Idempotency key conflict

[Documentation](../README.md) › [Troubleshooting](README.md) › **Idempotency conflict**

**What you see.** An enqueue with a key you already used is rejected:
the same scoped key, but a different normalized body fingerprint.

**This is not at-least-once redelivery and not "the queue is broken".** It is a conflict:
the key promises "this operation already happened", while the body says "this is a different operation".

## Why

The idempotency key applies in the scope producer + named queue + key.
queue-service remembers not only the key, but also the fingerprint of the
normalized request (the body after canonicalizing the fields that belong
to the contract).

| Retry | Result |
| --- | --- |
| Same key, same body | The original task — the normal replay |
| Same key, different body | Conflict — rejection |
| Different key, any body | A new operation |

This guards against "retried with the same key, but a different `order_id`".
Otherwise the second call would silently return the first task, and the
business would think it had enqueued different work.

Typical causes of a different fingerprint:

- on retry the payload, priority, or queue name changed;
- the client serializes JSON differently *and* normalization sees that as
  a different body (if the field is part of the fingerprint);
- a key was copied from another operation (a "universal" uuid on every enqueue);
- two threads independently enqueue different work under one business key.

## What to do

1. Decide whether this is the *same* meaning or a *new* one.
2. The same meaning (lost response, client retry) — repeat **the same
   logical body byte for byte**, the same key, the same named queue. If
   the conflict remains, the body really is different: compare what was
   sent the first time.
3. A new meaning — a new key. Do not reuse a key "because the order is
   the same" when the enqueue fields are different.
4. Derive the key from a stable business identifier of the operation
   (`order-42-enqueue-v1`), not from `uuid4()` on every HTTP call and not
   from "one key for the whole service".

## What not to do

| Do not | Why |
| --- | --- |
| "I will fix the payload and retry with the same key" | That is exactly the conflict |
| Treat the conflict as "retry in a second" | The bodies will not match on their own |
| Copy a key across named queues, expecting one task | The scope includes the queue name; these are different keys, not a conflict |
| Swallow the conflict and silently send a new key | You hide a client bug and get two pieces of work |

A conflict is a reason to fix the client, not queue-service.
