# HTTP API (draft, not a contract)

> **Superseded proposal — do not implement.** This API is not a contract. It assumes one
> logical queue and one ambiguous `outbox[]`. The accepted product uses
> named queues and separates `spawn[]` and `events[]`; see
> [product boundary](../01-concepts/07-product-boundary.md) and
> [ADR 003](../04-architecture/adr/003-separate-spawns-and-events.md).
> Normative OpenAPI will be created in Phase 3.

Original contract draft. **It is not a specification.** Records:
[03-formats.md](03-formats.md). Tables: [04-storage.md](04-storage.md).

Scope: the queue guarantees persist/claim/complete+outbox. It does not control application side effects.

## Calls

| Method | Path | Meaning |
| --- | --- | --- |
| `POST` | `/v1/tasks` | Ingress. The `Idempotency-Key` header is required |
| `POST` | `/v1/claims` | Claim one free task (lease). Empty — `204` |
| `POST` | `/v1/claims/{claim_id}/heartbeat` | Extend the lease |
| `POST` | `/v1/claims/{claim_id}/complete` | Egress: close the task and write the outbox in one commit |
| `GET` | `/v1/stats` | Optional. A snapshot of the queue. If disabled — `404` |
| `GET` | `/metrics` | Optional. Prometheus text. If disabled — `404` |

A repeated `complete` on the same claim does not create new outbox rows.

`/v1/stats` and `/metrics` are outside the ingress/egress guarantee. This draft does not fix Prometheus metric names, only the path and the format.

## Draft assumptions

- The producer and the worker use HTTP; a broker, if one exists, sits behind this API
- One instance = one logical queue
- The outbox on `complete` is new tasks on this same queue
- There is no explicit release/nack: with no complete, the lease expires and the task is available again
- Statistics and Prometheus are optional paths and are not required for an instance

Do not bring parser-queue v1 contracts into this document.
