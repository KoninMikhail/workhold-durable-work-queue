# Drain: a new enqueue is rejected

[Documentation](../README.md) › [Troubleshooting](README.md) › **Draining enqueue**

**What you see.** Enqueue into a familiar named queue fails. Worker claims
still work, and old tasks are still being finished. It looks as if "the
named queue is half broken".

**This is expected when `draining`.** New external enqueue is closed. Claims
and internal `spawn[]` continue, so that work already accepted can be
finished off.

## Why

Drain means "do not take new work, finish the tail". Typical before shutting
a named queue down or changing the handler contract.

| Operation | While `draining` |
| --- | --- |
| New enqueue / bridge with a new key | rejected |
| Retry of an already committed key with the same body | returns the original task |
| Claim | yes |
| `spawn[]` from an accepted complete | yes |
| Delivery relay | independent of Work Queue drain |

A retry of an old key is allowed *before* the current state gate check.
Otherwise a lost enqueue response (see [01](01-enqueue-response-lost.md))
during drain would turn into "the task exists, but the client cannot learn
the id". Genuinely new work does not get in.

`spawn[]` stays so a worker that already did the application work can close
and enqueue the next task. Otherwise drain would stick on "complete is not
allowed because the named queue is closed".

Work Queue drain is finished when delayed + ready + leased = 0. The
delivery-event tail is counted separately: the task queue can be empty while
the relay is still publishing. There is no automatic transition to `active`
/ `paused` — the operator chooses the next state.

## What to do

1. Check the named queue state. If it is `draining`, do not send new keys.
2. New work needs either a return to `active`, or another live named queue
   that an admin created ahead of time.
3. If this is a retry of an enqueue that already succeeded, use the same key
   and the same body. The original id should come back, not a drain error.
4. Do not fix a "rejected enqueue" by restarting the API.

## What not to do

| Do not | Why |
| --- | --- |
| Treat drain as pause | Pause is what accepts enqueue |
| Bypass drain with a second instance "for a while" | That is a different deploy boundary |
| Wait for the named queue to become active on its own | There is no automatic transition |
| Panic that spawn during drain is a hole | It is intentional; otherwise complete breaks the tail |

How pause differs from drain: [10-pause-vs-drain.md](../06-faq/10-pause-vs-drain.md).
