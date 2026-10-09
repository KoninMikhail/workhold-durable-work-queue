# 024. PostgreSQL 18.6 is the only runtime engine

**Status:** Accepted  
**Date:** 2026-09-19  
**Scope:** Runtime storage engine, compose/readiness pins, verification topology

## Context

The kernel, operations, Delivery Outbox, and app-local bridge were first proven on
PostgreSQL 16. Without an explicit engine of record, a deployment could silently stay on 16,
drift onto floating `18` or `latest`, or gain a dual runtime. Live production
queue-service instances did not exist: this is a repository cutover, not a product
major upgrade.

## Decision

The only runtime engine is **PostgreSQL 18.6.x**. The local and dev image is
`postgres:18.6-alpine`; conformance and qualification pin an **immutable digest**
of that same tag, not floating `18` or `latest`. Catalog, readiness, and verification
assert `server_version` major 18 and minor 6 and **fail-closed** on PostgreSQL 16.
Volumes are destroyed and created again; leftover PG 16 datadirs are not
supported. `pg_upgrade`, dual-runtime, and a 16→18 playbook are not part of the product.
Hot/cold uniqueness (ADR 006) and the QUAL-03 profile (ADR 019 / 023) are not
reopened by this ADR.

## Alternatives considered

| Option | Why not chosen |
| --- | --- |
| Stay on PostgreSQL 16 | The target-engine cutover (STOR-09) is already accepted |
| Floating tag `18` / `latest` | Minor drift breaks catalog and readiness asserts |
| Debian/trixie image | Changes the accepted alpine topology |
| Dual-runtime 16+18 or `pg_upgrade` | No production 16 instances; complicates ownership |
| PostgreSQL 19 | Out of scope (not GA on the decision date) |

## Consequences

**Positive:** One verifiable engine; readiness catches a foreign major or minor;
qualification is reproducible from the digest.

**Negative / trade-offs:** There is no in-place upgrade path from 16; operators must
`down -v` and recreate volumes. The official PG 18 `VOLUME` is `/var/lib/postgresql`,
not `/var/lib/postgresql/data`.

**Follow-up:** PostgreSQL 19 is a separate milestone after GA. Do not retune HASH, index, or payload
ceilings unless qualification is BLOCK.

## References

- [ADR 006](006-hot-cold-partitioning.md), [ADR 019](019-initial-production-gate.md), [ADR 023](023-qualified-kernel-storage-profile.md)
- [04-storage-topology.md](../04-storage-topology.md), [storage-contract.md](../../03-reference/02-storage-contract.md)
- [deployment.md](../../05-operations/02-deployment.md)
