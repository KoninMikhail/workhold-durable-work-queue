# Complete response lost

[Documentation](../README.md) › [Troubleshooting](README.md) › **Complete response lost**

**What you see.** The worker sent complete, and there is no response (timeout,
502, restart). It is unclear whether the task closed, and whether a retry
would create `spawn[]` / `events[]` twice.

**This is the same class as a lost enqueue response.** Complete responds
after the commit. The commit may have landed: the original task is already
succeeded, and spawn and events are already written. The HTTP response may
not have arrived.

## Why

In one queue-service transaction:

1. the original task → succeeded;
2. `spawn[]` → new tasks;
3. `events[]` → pending rows of the delivery outbox;
4. the request fingerprint and the replayable result are stored.

Then the API writes the response. If the process dies between steps 1–4 and
the response, the client sees an error and the store sees success.

A retry of *the same* terminal command with *the same* claim and *the same*
body does not create a second spawn and second events: queue-service returns
the stored result.

## What to do

1. Retry complete with the same `claim_id`, the same `claim_token`, the same
   `generation`, and the same body (`spawn[]` / `events[]` as in the first
   call).
2. If the lease is still the same one, you get the stored result. No new
   follow-ups or events appear.
3. If the lease already expired during the timeout and you received
   `lease_lost`, see [03-lease-lost.md](03-lease-lost.md). Do not change the
   body and do not send a different claim "to create the spawn once more":
   once complete is committed the task is terminal, and the spawn is already
   there.

For `events[]`, retrying complete does not publish a second time *from
complete*. The relay publishes, and the relay itself is at-least-once. That
is a different symptom:
[10-relay-duplicate-publish.md](10-relay-duplicate-publish.md).

## What not to do

| Do not | Why |
| --- | --- |
| Retry complete with a different `spawn[]` | A different fingerprint: that is no longer a replay |
| Enqueue the follow-up manually "just in case" | Most likely a duplicate of the spawn already created |
| Treat a timeout as "complete did not go through" | Often it did go through |
| After success, send fail "for cleanup" | The task is already terminal |

## How to tell it is fine

The retry returned the same protocol result expected from the first complete.
In the target named queues — the same follow-ups (the same ids), not a second
set. The original task is terminal succeeded.
