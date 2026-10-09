# 004. App-local outbox bridge for the business database

**Status:** Accepted  
**Date:** 2026-09-18  
**Scope:** Applications with a business DB, producer integration

## Context

HTTP enqueue into queue-service PostgreSQL cannot be atomic with a change
committed in another application database. Requiring queue-service tables in
every app database would break schema ownership and force a database on
applications that do not need one.

## Decision

An application that needs atomicity of a business change plus an enqueue intent
writes an app-local outbox in its own transaction. The bridge relays each row
into an idempotent queue-service enqueue with a key derived from the app-outbox
row ID. Applications without a database enqueue directly.

## Alternatives considered

| Option | Why not chosen |
| --- | --- |
| Direct HTTP dual write | Can lose the enqueue after the business commit |
| Distributed transaction / 2PC | Couples independent stores and complicates failures |
| Put queue-service tables in the app DB | Breaks ownership and the no-app-DB requirement |

## Consequences

**Positive:** The durable business transaction records the enqueue intent;
relay retries do not create duplicate queue-service tasks.

**Negative / trade-offs:** Enqueue is eventual, and the application owns the
local outbox lifecycle. The bridge does not promise zero lag.

**Follow-up:** Define a minimal bridge protocol and conformance tests without
imposing one application framework.

## References

- [Use cases](../../01-concepts/08-use-cases.md)
- [Data flow](../03-data-flow.md)
