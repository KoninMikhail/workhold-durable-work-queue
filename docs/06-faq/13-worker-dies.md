# What happens if the worker dies?

[Documentation](../README.md) › [FAQ](README.md) › **Worker died**

**In short.** If the worker dies after claim and does not manage complete/fail, the lease
expires by the queue store's clock. The task becomes available again, and another
replica takes it. This is the expected **at-least-once**. The old `claim_token`
no longer moves state. If the external effect already happened, it may
repeat — the handler must be idempotent.

## Timeline

1. The worker claims and receives `claim_id`, `claim_token`, `generation`,
   and the lease deadline.
2. While it is working, it sends heartbeats and extends the lease. The deadline is read by
   queue-service PostgreSQL, not by the worker's clock.
3. The worker dies (OOM, kill, network, pod restart). Heartbeats stop.
4. The lease deadline passes. The task is claimable again. Another replica (or the same one
   after a restart) makes a new claim: a new token, a new generation,
   a new `claimed_at`.
5. The old process, if it comes back, gets `lease_lost` on heartbeat/complete/fail.
   It must not keep mutating the queue with this claim.

This is not a queue failure. Several replicas deliberately compete for the same
tasks; a crashed one must not hold the work forever.

## Two outcomes for the external effect

| When the worker died | What happens |
| --- | --- |
| Before the external HTTP call / write | There was no effect. The new worker does it once (until it dies itself) |
| After the effect, before complete | The effect already exists. The new worker does it again if the handler is not idempotent |

The queue does not know whether the worker called the payment API. It only knows
whether the lease is alive and whether complete was recorded.

If a cancel was already pending on the task and the lease expired, the task becomes
`cancelled` and does not go to another worker. Otherwise the cancel would have been
lost because the holder died.

## What to do in the worker code

- Heartbeat at the recommended interval if the work may not fit
  into the lease deadline.
- After `lease_lost`, stop. Do not retry complete with the old token
  in the hope of "pushing it through". Do not treat `worker_id` as a right to the lease.
- Make every external effect idempotent: an inbox, a unique key
  of the external API, compare-and-set.
- Do not rely on "I am still alive in Kubernetes, so the lease is mine": the network
  between the worker and the API may have dropped before the process did.

A detailed walkthrough of "the effect already happened":
[04-side-effect-then-lease-lost.md](../07-troubleshooting/04-side-effect-then-lease-lost.md).
`lease_lost` with no effect: [03-lease-lost.md](../07-troubleshooting/03-lease-lost.md).
