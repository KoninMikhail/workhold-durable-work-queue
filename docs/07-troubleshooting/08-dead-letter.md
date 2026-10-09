# Task went to dead letter

[Documentation](../README.md) › [Troubleshooting](README.md) › **Dead letter**

**What you see.** The task is no longer claimed. The status is terminal:
dead-lettered. Workers do not take it. Enqueueing the same work "as if it
still needs another try" is a different conversation.

**This is the outcome of the retry policy**, not an accidental disappearance.
Either the attempts are exhausted, or retry on the named queue is off and the
first attempt failed.

## Why

Each named queue has its own versioned retry policy: whether retry is on,
how many attempts, what backoff. The version is captured at enqueue time.
A later config change does not rewrite a task that is already alive.

A task goes to dead letter when:

- the worker sent `fail` with a code, and the policy said "no more"
  (attempt limit, or retry is off);
- the lease expired as many times as the policy allows, and a further retry
  is no longer granted;
- retry is off — a failed first attempt is parked immediately.

On every failed attempt queue-service writes a machine-readable
`failure_code` and a diagnostic detail. Attempt history is kept within
retention, so it is possible to see *why* the task reached dead letter.

Dead-lettered is terminal: the task is no longer claimed. It will not sit
there and come back on its own.

## What to do

1. Open the task and its attempts: `failure_code`, detail, how many attempts,
   which worker, whether the lease expired.
2. From the code, tell apart a handler bug, a persistent external-API 5xx,
   a bad payload, and cancellation versus plain expiry.
3. Fix the cause (code, dependency, data), not by restarting workers.
4. Replay (when the operation is available) creates a **new** auditable task.
   It does not rewrite the old history. The old row stays dead-lettered —
   that is how it should be.
5. A new enqueue with the same key will not resurrect the dead-lettered task:
   the key is already bound to the original enqueue. A new attempt after the
   fix needs a new key or the normal replay, not "the same enqueue once more,
   hoping".

If retry is off and a dead letter after a single fail is a surprise, that is
the named queue config, not a core failure.

## What not to do

| Do not | Why |
| --- | --- |
| Wait for the task to become available again | It is terminal |
| Change the retry policy and expect it to affect this task | The version was captured at enqueue |
| Quietly enqueue a duplicate without reading the code | You hide the root cause and get a second task like the first |
| Confuse a task dead letter with a delivery dead letter | The relay has its own tail and its own exhausted events |

Retry policy and guarantees: [guarantees.md](../01-concepts/09-guarantees.md).
