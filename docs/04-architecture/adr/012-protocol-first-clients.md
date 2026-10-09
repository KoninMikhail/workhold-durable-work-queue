# 012. Protocol-first, batch-ready clients

**Status:** Accepted  
**Date:** 2026-09-18  
**Scope:** Producer/worker/admin clients and compatibility

## Context

Making the Python SDK the specification would tie every application to one
implementation. A response with a single claim object would require a breaking
response change when batch claim or long polling is introduced.

## Decision

A machine-readable protocol plus black-box conformance tests is authoritative.
The claim request and response are array-shaped from v1, while the MVP
enforces one task and no long wait. Producer, worker, and admin SDK surfaces
are thin and separately credentialed.

## Alternatives considered

| Option | Why not chosen |
| --- | --- |
| SDK is the only contract | Excludes other languages and hides server semantics |
| Single-task response shape | Breaks when batch claim is enabled |
| Implement batch/long polling immediately | Adds hot-path complexity before measurements |
| Monolithic SDK with admin methods | Widens the credential blast radius |

## Consequences

**Positive:** Additive batch evolution, language-neutral conformance, and
optional SDK ergonomics.

**Negative / trade-offs:** Protocol artifacts and the conformance harness
require first-class maintenance.

**Follow-up:** Phase 3 OpenAPI and tests enforce `max_tasks=1` and
`wait_seconds=0`.

## References

- [Client protocol](../09-client-protocol.md)
- [Fenced leases ADR](005-fenced-leases.md)
