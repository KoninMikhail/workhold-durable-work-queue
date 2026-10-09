# 023. Qualified kernel storage profile (Phase 3.9 QUAL-03)

**Status:** Accepted  
**Date:** 2026-09-19  
**Scope:** Release-candidate physical indexes, HASH modulus, and payload/request ceiling

## Context

Phase 3.1 locked the physical contract with provisional indexes and unpartitioned
correctness registries. Phase 3.9 measured storage candidates and produced a
checksum-valid PASS recommendation. QUAL-03 requires the release candidate to
apply those exact selections before final qualification.

Evidence package: `benchmarks/results/phase-3.9-candidates/`  
Candidate-index manifest SHA-256: `8798ae88dc7ac7ad6e121a3d2e56fcfdaca5c9b24c5345466777d57a884166d1`  
Evidence mode: `synthetic` (Plan 09 may use synthetic metrics; checksums are valid
and the PASS recommendation is applied as required).  
Verdict: `PASS`

## Decision

Apply the measured recommendation exactly:

| Axis | Selection |
| --- | --- |
| Indexes | `admin_audit_log_queue_audit_idx,admin_replay_expires_at_idx,complete_replay_expires_at_idx,delivery_events_terminal_event_idx,task_attempts_task_claimed_idx,tasks_active_claim_idx,tasks_terminal_spawn_lineage_idx,tasks_terminal_task_terminal_idx` (omit `enqueue_dedup_expires_at_idx`) |
| HASH `enqueue_dedup` | modulus **1** (unpartitioned) |
| HASH `complete_replay` | modulus **1** (unpartitioned) |
| Payload / request ceiling | **1048576** bytes (≤ 1 MiB hard maximum) |

Selected candidate bundles:

| Kind | Candidate ID | Bundle SHA-256 |
| --- | --- | --- |
| index | `15d5e1d25b678ae15d511c4c5e64a8f93d2077e4f31ff4eee1580bb47b9b730a` | `5e738b2bbf1e9840688b055752e2dfbe17cc85bb4cd2728507ccbcfdaa4381e0` |
| hash (`enqueue_dedup:1`) | `44f139e2e27faa2730f33cb099e25843db3c91c5b05b73b3cdedc31b92faacea` | `bbc16c2f75c8e826399482b1862c2d5aea5519c23a9e7ed465a999024b6f85f2` |
| hash (`complete_replay:1`) | `c35229f846f2d54a56a54b5ca8e6e459a1d9c46425c57d9c68a4b7e30338184d` | `fec96d45c2ca9b7e2c3b21c0f3b1be156cc4852db11fe608518c5816b448259b` |
| payload | `af1f864a346052282af4f59ec9634e952d995d59a0abd843d6af9fec2ff6dfbd` | `e957bd140b327cd8b2c635db2c6bcf3180a230ac7da358c79fe778cdd1d79fd4` |

Migration `039_apply_qualified_storage_layout` implements the index transition
with a safe downgrade that restores `enqueue_dedup_expires_at_idx`. Deployment
settings expose `QUALIFIED_PAYLOAD_CEILING_BYTES` /
`QUALIFIED_REQUEST_MAX_BYTES` (= 1048576). Time-partition topology, retention
windows, and runtime queue policies are unchanged.

This is **not** a universal SLA: values are tied to the cited evidence package
and environment profile of the candidate run.

## Alternatives considered

| Option | Why not chosen |
| --- | --- |
| Keep Phase 3.1 provisional indexes including `enqueue_dedup_expires_at_idx` | Eligible index candidates with lower index_bytes and competitive claim p99 omitted that expiry index under fixed tie-breaks |
| HASH modulus 4/8/16/32 for registries | Higher modulus candidates were eligible but lost on (hash_count, claim p99, WAL) tie-break; modulus 1 won |
| Payload ceiling 262144 | Eligible but smaller than 1048576 under the ≤10% p99 regression vs 1 KiB rule |
| Defer until live Compose re-measure | Plan 10 requires applying the checksum-valid PASS recommendation now; synthetic mode is disclosed |

## Consequences

**Positive:** Release candidate has no unresolved index / HASH / payload-ceiling
decision; docs and ADR cite immutable evidence hashes.

**Negative / trade-offs:** Synthetic evidence mode may diverge from production
hardware; operators must re-measure before raising ceilings. Omitting the
enqueue-dedup expiry index may increase expiry-scan cost under heavy purge load.

**Follow-up:** Phase 7 re-validated this profile unchanged on PostgreSQL 18.6
(`benchmarks/results/phase-7-postgresql-18.6/`; D-12: do not retune HASH/index/
payload). Live Compose re-measurement may supersede this ADR with a new
revision if results differ.

## References

- `benchmarks/results/phase-3.9-candidates/recommendation.json`
- `benchmarks/results/phase-3.9-candidates/report.md`
- `benchmarks/results/phase-3.9-candidates/index-manifest.json`
- `benchmarks/results/phase-7-postgresql-18.6/`
- ADR 006, ADR 019, ADR 022, [ADR 024](024-postgresql-18-6-runtime-engine.md)
- `docs/04-architecture/04-storage-topology.md`, `docs/04-architecture/06-admission-control.md`