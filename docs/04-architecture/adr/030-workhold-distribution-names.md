# 030. Workhold distribution names

**Status:** Accepted  
**Date:** 2026-10-09  
**Scope:** Runtime distribution, console script, and Python client package names

## Context

Generic identifiers (`queue`, `queue_service`, `queue-service-*`) collide with the
stdlib module `queue` and with the domain noun for a named queue. Role-split
clients from ADR 029 need a product name base that is safe to install and import.

## Decision

- Public and runtime distributions use `workhold` as the name base.
- Runtime distribution and import are both `workhold`. Console script is `workhold`.
- Role clients: `workhold-producer` (`workhold_producer`), `workhold-consumer` (`workhold_consumer`), `workhold-admin` (`workhold_admin`).
- Shared dependency-only core: `workhold-client-core` importing `_workhold_client_core`, plus test kit `workhold_client_testing`.
- The ADR 029 role split stays Accepted. Its distribution identifiers are superseded by this ADR.

## Alternatives considered

| Option | Why it was not chosen |
| --- | --- |
| Keep `queue` / `queue_service` / `queue-service-*` | Too generic; `queue` collides with the stdlib module and the domain noun |

## Consequences

**Positive:** One product name on the install and import path, with no stdlib clash.

**Negative / trade-offs:** Install snippets and docs use the new identifiers. ADR 029 keeps its historical names in the decision body.

## References

- [ADR 029](029-role-split-python-clients.md) (role split remains Accepted)
- [ADR 020](020-python-sdk-packaging.md) (superseded monolithic `queue-client`)
- [Client protocol](../09-client-protocol.md)
