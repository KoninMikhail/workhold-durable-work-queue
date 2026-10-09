# Heartbeat or complete returned lease_lost

[Documentation](../README.md) › [Troubleshooting](README.md) › **lease_lost**

**What you see.** The worker sends heartbeat, complete, or fail and receives
`lease_lost`. It looks as if "the task vanished" or "workhold does not
recognize my worker".

**This is expected** when the lease has expired or the generation has already
changed. The current `claim_token` no longer fences state in workhold.

## Why

On claim, workhold issues a time-limited right: token + generation.
The deadline is the service PostgreSQL clock, not the worker clock. The right
is gone when:

- the heartbeat did not arrive in time — the work ran longer than the lease,
  the network dropped, the process hung;
- another worker already made a new claim after expiry (a new generation);
- this same process restarted and still holds the *old* token in memory;
- you call complete with the token of a previous attempt.

`worker_id` has nothing to do with it. It is a "who took it" label for
diagnostics. Substituting your own id and continuing is not possible.

After the lease is lost the task is claimable again (unless there is a pending
cancellation on it — then expiry finalizes the cancellation and does not hand
the work to someone else). Another attempt is normal at-least-once.

## What to do

1. Immediately stop mutating the queue with this claim: no further
   heartbeat / complete / fail with the old token "for luck".
2. Do not treat the work as "yours", even if the process is still alive and
   `worker_id` is the same.
3. If the external effect has not been done yet — just leave processing.
   Another worker will take the task.
4. If the effect has already been done — that is a different symptom:
   [04-side-effect-then-lease-lost.md](04-side-effect-then-lease-lost.md).
5. A new claim brings a new token. Handle it as a new attempt,
   not as a continuation of the old one.

To hit expiry less often: heartbeat at the interval from the claim response
(plus jitter), before the deadline runs out. If the work is known to be longer
than the default lease, extend it. Do not lengthen it "by eye" in the client,
bypassing the API.

## What not to do

| Do not | Why |
| --- | --- |
| Retry complete with the old token | workhold is right to reject it |
| Find "your" task by `worker_id` and close it | Identity does not authorize |
| Treat `lease_lost` as a deploy error | Often it is simply expiry |
| Quietly start the external effect again without idempotency | The next worker will do the same |

If `lease_lost` keeps showing up on short work with a live heartbeat,
treat clocks/NTP only as a hypothesis: the source of truth is still the
workhold store. Next, find why the heartbeat does not arrive (network,
API overload, the wrong token in the header).
