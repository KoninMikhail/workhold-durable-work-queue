# Release Qualification Record — Phase 3.9 reference

**Verdict:** `CI_SYNTHETIC_PASS`  
**Evidence mode:** `synthetic`  
**Production qualified:** `false`

This record finalizes the Plan 11 raw reference evidence into an immutable
12-class qualification bundle. It is **not a universal SLA**. Numeric thresholds
come from ADR 019 for the documented reference profile only. Storage layout is
pinned by ADR 023. Synthetic CI evidence must never be read as a live production
PASS.

## Identity

| Field | Value |
| --- | --- |
| Run ID | `run-phase39-reference-001` |
| Bundle | `benchmarks/results/phase-3.9-reference/` |
| Bundle stage | `final` |
| Git SHA (recorded) | `1612ebf590c9379ad4cb1159f53e7a9687081041` |
| Environment hash | `da3e34dc7051decb8374f34d69e876c881ab93ed933b7ed1da3ab08cfcaa5e97` |
| Workload hash | `772d19e930ec621bc4a17463cd635f4465119cd8fd2ae25e42f9d8b4ce3095d3` |
| Validated raw-set digest | `f9173e888dc6c98d096ef25406ca1d60163bf7a02e8883fbb297a4476fdf75d1` |
| `SHA256SUMS` digest | `da05cfb54ab21099505ead6cd77b0b942c9c538b83092f8f25b7b4b0d84f71c4` |
| Schema revision | `039_apply_qualified_storage_layout` |
| Catalog signature | `admin_audit_log_queue_audit_idx,admin_replay_expires_at_idx,complete_replay_expires_at_idx,delivery_events_terminal_event_idx,task_attempts_task_claimed_idx,tasks_active_claim_idx,tasks_terminal_spawn_lineage_idx,tasks_terminal_task_terminal_idx` |
| Profile | `phase-3.9-linux-x86_64-v1` |
| Image digests | postgres `sha256:3c5c8892d184f738f4fe282d14ddaa613a38f00f4189d2d94725ebe6f2909ddb`; queue `sha256:ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff` |

## Conformance

Both client variants present with zero failures / errors / skips:

- `raw_http`
- `sdk`

## Thresholds vs actuals (ADR 019)

| Gate | Threshold | Actual | Pass |
| --- | --- | --- | --- |
| Successful claims / s | ≥ 500 | 500.0 | yes |
| Valid-operation success ratio | ≥ 99.9% (0.999) | 1.0 | yes |
| Enqueue p99 | ≤ 100 ms | 2.49 ms | yes |
| Claim p99 | ≤ 100 ms | 1.98 ms | yes |
| Heartbeat p99 | ≤ 100 ms | 1.045 ms | yes |
| Baseline complete p99 (fan-out 0/8) | ≤ 200 ms | 5.98 ms | yes |
| Fan-out 64 complete p99 | reported separately (no baseline merge) | 86.3 ms | reported |

## Qualified storage profile (ADR 023)

Release-candidate storage layout is the ADR 023 qualified kernel profile
(`039_apply_qualified_storage_layout`), including the catalog signature above.
ADR 022 remains the physical-contract baseline; the qualified profile lives in
**ADR 023** (022 was already allocated).

## Bundle integrity

Exactly **12** top-level artifact classes. `SHA256SUMS` covers the eleven
non-checksum classes plus every indexed EXPLAIN file and does not checksum
itself. Sequence used:

1. `validate-raw`
2. `derive --allow-synthetic`
3. `checksums`
4. `validate-final --allow-synthetic`
5. `evaluate --input … --check --allow-synthetic`

Tampering with any covered artifact fails `validate-final` / `evaluate` fail-closed.

## Verdict note

Synthetic CI evidence met numeric thresholds; NOT a live production qualification PASS.

Re-run on the live reference host without `--allow-synthetic` before treating
the kernel as production-qualified.
