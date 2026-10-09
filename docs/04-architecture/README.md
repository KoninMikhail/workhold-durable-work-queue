# Architecture

[Documentation](../README.md) › **Architecture**

Accepted product architecture and cross-module invariants. The physical API and DDL live in OpenAPI and [storage-contract.md](../03-reference/02-storage-contract.md); the rationale lives in [adr/](adr/README.md).

Start with the [reading path](../00-onboarding/01-reading-path.md) if you have not yet followed the 15-minute path.

| # | Document | Purpose |
| --- | --- | --- |
| 1 | [01-state-machine.md](01-state-machine.md) | Task, claim, attempt, and delivery-event lifecycles |
| 2 | [02-concurrency.md](02-concurrency.md) | Multi-replica claims, fencing, and crash races |
| 3 | [03-data-flow.md](03-data-flow.md) | End-to-end producer, worker, spawn, event, and bridge flows |
| 4 | [04-storage-topology.md](04-storage-topology.md) | Hot/cold relations, automatic partitioning, retention, and type constraints |
| 5 | [05-runtime-semantics.md](05-runtime-semantics.md) | active/paused/draining behavior, cancellation, and state gates |
| 6 | [06-admission-control.md](06-admission-control.md) | Hard limits, depth protection, and overload |
| 7 | [07-error-model.md](07-error-model.md) | Stable protocol-independent errors and retryability |
| 8 | [08-task-inspection.md](08-task-inspection.md) | Inspection, attempts, lineage, and the result boundary |
| 9 | [09-client-protocol.md](09-client-protocol.md) | Batch-ready claims, SDK duties, and compatibility |
| 10 | [10-application-outbox-bridge.md](10-application-outbox-bridge.md) | Application outbox bridge protocol |
| 11 | [11-application-outbox-bridge-compatibility.md](11-application-outbox-bridge-compatibility.md) | Bridge compatibility and evolution |
| 12 | [12-physical-contract-benchmarks.md](12-physical-contract-benchmarks.md) | Physical-contract benchmark hypotheses |
| — | [adr/](adr/README.md) | Why repository-level architecture decisions were accepted |

> **Non-normative.** Current `docs/03-reference/03-formats.md`, `04-storage.md`, and `05-http-api.md` predate these decisions and remain drafts until they are replaced.

---

← [Contents](../README.md) · [ADR](adr/README.md)
