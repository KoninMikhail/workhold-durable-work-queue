# 003. Separate spawned tasks and delivery events

**Status:** Accepted  
**Date:** 2026-09-18  
**Scope:** Task completion, Work Queue and Delivery Outbox

## Context

The initial draft uses `outbox` for records that immediately become tasks in
the same database. Ordinary outbox records are an intent to publish to an
external channel. Combining them yields one resource with incompatible
lifecycle, retry, and observability requirements.

## Decision

A task terminal command can atomically create two distinct collections:

- `spawn[]`: new work queue tasks in named queues;
- `events[]`: pending delivery outbox records for asynchronous publication.

Both commit with the source transition. Network publication never happens
inside this transaction.

## Alternatives considered

| Option | Why not chosen |
| --- | --- |
| One generic outbox item for both | Mixes competing work with outbound delivery and freezes ambiguous semantics |
| Only spawned tasks | Does not preserve the proven callback/event delivery use case |
| Only outbound events | Forces internal task chains to go through a broker or relay |

## Consequences

**Positive:** Each resource has its own correct retry, state, metrics, and
retention; complete stays atomic and extensible.

**Negative / trade-offs:** Complete and storage contracts are larger; delivery
transport remains a separate decision.

**Follow-up:** Supersede the complete `outbox[]` draft; decide the event
envelope and relay transport before implementing `events[]`.

## References

- [State machines](../01-state-machine.md)
- [Guarantees](../../01-concepts/09-guarantees.md)
