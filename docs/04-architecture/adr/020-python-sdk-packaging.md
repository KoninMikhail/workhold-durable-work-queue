# 020. Python SDK is a separate distribution in this repository

**Status:** Superseded  
**Date:** 2026-09-18  
**Scope:** Repository packaging and client release compatibility

> **Superseded by** [029-role-split-python-clients.md](029-role-split-python-clients.md).
> Historical decision text below is retained unchanged.

## Context

Applications benefit from supported heartbeat, lost-lease, and cancel loops. A separate
repository repeats v1 cross-repository coordination; bundling client imports
into the server distribution couples runtime dependencies.

## Decision

This repository ships a separate Python distribution `queue-client` with import package
`queue_service_client`. It provides producer and worker primitives and an optional supervised
worker loop. OpenAPI and conformance remain authoritative. An admin client may be
added separately and is not part of the application defaults.

## Alternatives considered

| Option | Why not chosen |
| --- | --- |
| Separate SDK repository | Independent release drift and coordination overhead |
| No official SDK | Each team reimplements heartbeat, cancellation, and lease loss |
| SDK inside server package | Couples server and client dependencies and deployment |
| SDK is the specification | Excludes non-Python clients and hides protocol bugs |

## Consequences

**Positive:** Aligned compatibility with independent installation and thin-client
ergonomics.

**Negative / trade-offs:** Monorepo packaging and release tooling must test
several distributions.

**Follow-up:** Phase 3 defines the package layout, compatibility matrix, and
generated-model strategy.

## References

- [Client protocol](../09-client-protocol.md)
- [Protocol-first clients ADR](012-protocol-first-clients.md)
