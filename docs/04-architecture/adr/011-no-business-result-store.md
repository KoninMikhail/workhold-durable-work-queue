# 011. queue-service does not store business results

**Status:** Accepted  
**Date:** 2026-09-18  
**Scope:** Task inspection, completion and retention

## Context

A generic result field would recreate parser-v1 `parse_result`, inflate
retention cost, and turn queue-service into a business-data query service.
Idempotent complete still needs a replayable protocol result.

## Decision

queue-service stores the operational terminal outcome, failure metadata,
attempts, and spawn/event lineage. Business output goes to application
storage, spawned tasks, or delivery outbox events. Complete replay stores only
terminal and protocol metadata and the IDs of created resources.

## Alternatives considered

| Option | Why not chosen |
| --- | --- |
| Opaque business-result JSON | Blurs ownership and makes queue-service retention a contract for application data |
| No retained completion response | Breaks safe retry after an uncertain complete response |
| Payload-field result conventions | Brings back application-specific contracts |

## Consequences

**Positive:** Bounded operational storage and a generic inspection API.

**Negative / trade-offs:** Applications that need synchronous result lookup
must own that result store or consume an event.

**Follow-up:** Phase 3 task inspection returns terminal metadata, without
`result` or `parse_result`.

## References

- [Task inspection](../08-task-inspection.md)
- [Separate spawns and events ADR](003-separate-spawns-and-events.md)
