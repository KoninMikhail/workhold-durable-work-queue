# 010. Three queue runtime states

**Status:** Accepted  
**Date:** 2026-09-18  
**Scope:** Named queue runtime behavior and control plane

## Context

Operators need to stop processing, close intake, or drain work without
breaking existing leases. Ambiguous pause and drain semantics lead to retries,
lost completions, and deployment incidents.

## Decision

Named queues are created explicitly by admin and have state `active`,
`paused`, or `draining`. Paused allows enqueue but returns empty claims.
Draining rejects new external enqueue but allows claims, lease operations, and
internal spawns until active depth reaches zero. Delivery relay and process
shutdown stay independent.

## Alternatives considered

| Option | Why not chosen |
| --- | --- |
| Pause rejects enqueue but allows claim | Describes draining rather than a processing pause |
| Drain stops claims | Cannot consume the backlog that should drain |
| Reject spawn into draining target | A valid complete can stall after business work has already run |
| Generic independent boolean flags | Allows invalid combinations and weak client semantics |

## Consequences

**Positive:** Orthogonal stop-processing and stop-intake controls with stable
race rules.

**Negative / trade-offs:** Internal task chains can extend drain time; a full
freeze would need a later explicit state or a deployment-level deny.

**Follow-up:** Metrics report delayed, ready, and leased depth and explicit
drain completion.

## References

- [Runtime semantics](../05-runtime-semantics.md) — including cancellation and `ack_cancel`
- [Runtime control plane ADR](009-runtime-control-plane.md)
