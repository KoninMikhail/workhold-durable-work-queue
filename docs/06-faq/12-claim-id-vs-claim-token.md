# Why are claim_id and claim_token separate?

[Documentation](../README.md) › [FAQ](README.md) › **claim_id vs token**

**In short.** These are two different identifiers of one lease. `claim_id` is
the public id for the path, logs, and tracing. `claim_token` is a secret
capability in the header: only it authorizes heartbeat, complete, and fail.
`worker_id` by itself does not authorize the lease.

## Why two fields

If one secret lives in the URL and in the logs, anyone who has seen an access log,
a trace, or a ticket with the URL can close or extend someone else's lease. That is why
the resource identifier and the right to mutate the resource are separate.

| | `claim_id` | `claim_token` |
| --- | --- | --- |
| Role | Public id of the lease | Secret capability |
| Where | The path (`/v1/claims/{claim_id}:complete`), logs, tracing, links | The `X-Queue-Claim-Token` header |
| What it gives | Correlation of "which lease this is about" | The right to move the state of this lease |
| Rotation | A new id on every claim / reclaim | A new token on every claim / reclaim |

In the claim response the worker receives both, plus `generation`, `claimed_at`, and
expiry. After that, heartbeat / complete / fail go to the path with `claim_id` and
carry the token in the header. The secret is not put in the URL, the query string, or ordinary
logs.

The token is checked together with the generation. An expired or already replaced claim
gets `lease_lost`: another worker (or the same one after reclaim) holds
the new generation.

## What is not a key

`worker_id` is a diagnostic label of the replica ("who took it"). It is written into
the attempt history. You cannot substitute your own `worker_id` and call complete on someone else's
task: without the current token the queue will not move the state.

An analogy: `claim_id` is like an order number in a tracker, and `claim_token` is like
a one-time confirmation code. The number can be shown; the code cannot.

## Common mistakes

| Mistake | Why that is wrong |
| --- | --- |
| "I'll log the whole claim response" | The response contains a secret. Log `claim_id`, not the token |
| "I'll put the token in the query; it's easier in curl" | It will land in the access log and in shell history |
| "I have the same worker_id, so the lease is mine" | After expiry the lease already belongs to someone else |
| "The token is a fence for external HTTP" | No. It protects only the queue store |

What to do on `lease_lost`: [03-lease-lost.md](../07-troubleshooting/03-lease-lost.md).
