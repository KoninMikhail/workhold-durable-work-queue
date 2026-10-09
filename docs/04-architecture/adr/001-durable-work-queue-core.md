# 001. Durable Work Queue — product core

**Status:** Accepted  
**Date:** 2026-09-18  
**Scope:** Product identity, API and persistence architecture

## Context

Expected use is to enqueue, claim, process, and observe tasks across many
application replicas. An early draft named the whole product a transactional
outbox, although HTTP enqueue cannot share a transaction with the application's
business database, and follow-up tasks are not ordinary outbox events.

## Decision

queue-service is first a PostgreSQL-backed durable competing-consumer work queue,
deployed per application. Transactional outbox is a separate delivery capability
and an integration pattern, not the name of task processing itself.

## Alternatives considered

| Option | Why not chosen |
| --- | --- |
| Transactional-outbox service as the primary identity | Overstates cross-database atomicity and does not describe claim/lease work |
| Shared platform event bus | Conflicts with per-application deployment and competing-consumer tasks |
| Workflow engine | DAGs, joins, and compensation exceed the required task lifecycle |

## Consequences

**Positive:** Product guarantees match what queue-service PostgreSQL controls;
parser v1 fields stay outside the model.

**Negative / trade-offs:** Outbound delivery and app-database integration
require separate contracts and phases.

**Follow-up:** Derive the API and storage only after guarantees and state
machines.

## References

- [Product boundary](../../01-concepts/07-product-boundary.md)
- [Guarantees](../../01-concepts/09-guarantees.md)
