# 029. Role-split producer/consumer/admin Python clients

> **Distribution names superseded by** [030-workhold-distribution-names.md](030-workhold-distribution-names.md). Role split remains Accepted.

**Status:** Accepted  
**Date:** 2026-09-22  
**Scope:** Python client distributions, package layout, operation ownership

## Context

ADR 020 shipped a single unreleased `queue-client` prototype. Role-separated
credentials and dual public/admin base URLs need independently installable
surfaces without a legacy facade or server-package coupling.

## Decision

- Public distributions: `queue-service-producer` → `queue_service_producer`,
  `queue-service-consumer` → `queue_service_consumer`,
  `queue-service-admin` → `queue_service_admin`.
- Shared implementation is dependency-only `queue-service-client-core`
  (`_queue_service_client_core`); it exposes no operation client classes.
- Public classes: `ProducerClient`; `ConsumerClient` + `ConsumerSupervisor`;
  `ObserverClient`, `AdminClient`, `BreakGlassClient` inside admin.
- Remove the unreleased `queue-client` / `queue_service_client` prototype before
  release; no facade, aliases, or deprecation window.
- App-local outbox bridge lives in `queue_service_producer.bridge` with an
  explicit producer extra for the PostgreSQL reference integration.
- OpenAPI/conformance stay authoritative; handwritten tolerant models are OK;
  no generated SDK becomes specification. Sync and async are first-class
  (async via optional transport extra). Dual public/admin base URLs; coordinated
  monorepo version across core + three public clients.

## Alternatives considered

| Option | Why not chosen |
| --- | --- |
| Keep monolithic `queue-client` | Couples producer/consumer/admin credentials and imports |
| Legacy facade over prototype | Unreleased; no compatibility promise to preserve |
| Generated SDK as specification | Hides protocol bugs; blocks non-Python clients |
| Core with operation methods | Would leak admin/break-glass into thin installs |

## Consequences

**Positive:** Clean install isolation; machine-checked operation ownership
(`packages/client-operation-ownership.json`).

**Negative / trade-offs:** More distributions and release coordination.

**Follow-up:** Extract packages (phase 15+); delete prototype before release.

## References

- [Client protocol](../09-client-protocol.md)
- [ADR 012](012-protocol-first-clients.md), [ADR 020](020-python-sdk-packaging.md) (superseded)
