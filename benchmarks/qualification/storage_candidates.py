"""Storage candidate measurement, synthetic evidence and selection CLI (QUAL-03)."""

from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import shutil
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping, Sequence

from benchmarks.qualification.artifacts import (
    ArtifactError,
    CHECKSUM_CLASS,
    derive,
    load_json,
    sha256_file,
    validate_final,
    validate_raw,
    write_checksums,
    write_json,
)
from benchmarks.qualification.selection import (
    BASELINE_1KIB_PAYLOAD_BYTES,
    HASH_COUNTS,
    HASH_REGISTRIES,
    PAYLOAD_CEILINGS,
    CandidateEvidence,
    evaluate_candidates,
)

SCHEMA_REVISION = "0001_physical_contract_foundations"
PHASE12_SCHEMA_REVISION = "1201_bounded_priority_claim_ordering"
WHOLE_REQUEST_LIMIT_BYTES = 1_048_576

# Phase 12 qualified claim-index shape (LPD-9 / Plan 05 migration head).
PHASE12_PHYSICAL_SIGNATURE: dict[str, Any] = {
    "index_name": "tasks_active_claim_idx",
    "table": "tasks_active",
    "columns": [
        "queue_id",
        "state_code",
        "priority DESC",
        "available_at",
        "id",
    ],
    "schema_revision": PHASE12_SCHEMA_REVISION,
}

# Phase 3.1 baseline non-UNIQUE / non-constraint indexes (removable for omit trials).
BASELINE_REMOVABLE_INDEXES: tuple[dict[str, Any], ...] = (
    {
        "name": "tasks_active_claim_idx",
        "table": "tasks_active",
        "columns": list(PHASE12_PHYSICAL_SIGNATURE["columns"]),
    },
    {
        "name": "enqueue_dedup_expires_at_idx",
        "table": "enqueue_dedup",
        "columns": ["expires_at"],
    },
    {
        "name": "complete_replay_expires_at_idx",
        "table": "complete_replay",
        "columns": ["expires_at"],
    },
    {
        "name": "admin_replay_expires_at_idx",
        "table": "admin_replay",
        "columns": ["expires_at"],
    },
    {
        "name": "admin_audit_log_queue_audit_idx",
        "table": "admin_audit_log",
        "columns": ["queue_id", "audit_at DESC", "id"],
    },
    {
        "name": "task_attempts_task_claimed_idx",
        "table": "task_attempts",
        "columns": ["task_id", "claimed_at DESC", "id"],
    },
    {
        "name": "tasks_terminal_task_terminal_idx",
        "table": "tasks_terminal",
        "columns": ["task_id", "terminal_at DESC"],
    },
    {
        "name": "tasks_terminal_spawn_lineage_idx",
        "table": "tasks_terminal",
        "columns": ["source_task_id", "spawn_ordinal", "terminal_at"],
        "where": "source_task_id IS NOT NULL",
    },
    {
        "name": "delivery_events_terminal_event_idx",
        "table": "delivery_events_terminal",
        "columns": ["event_id", "terminal_at DESC"],
    },
)

# H1 alternate claim-index orderings marked benchmark_candidate in Phase 3.1 hypotheses.
BENCHMARK_CANDIDATE_INDEXES: tuple[dict[str, Any], ...] = (
    {
        "name": "tasks_active_claim_idx_alt_available_first",
        "table": "tasks_active",
        "columns": ["queue_id", "available_at", "state_code", "priority DESC", "id"],
        "hypothesis_id": "H1",
        "benchmark_candidate": True,
    },
    {
        "name": "tasks_active_claim_idx_alt_priority_first",
        "table": "tasks_active",
        "columns": ["queue_id", "state_code", "priority DESC", "available_at", "id"],
        "hypothesis_id": "H1",
        "benchmark_candidate": True,
    },
)


@dataclass(frozen=True)
class CandidateSpec:
    kind: str
    variant: str | None
    registry: str | None
    hash_count: int | None
    indexes: tuple[str, ...] | None
    omitted_index: str | None
    added_index: str | None
    payload_ceiling_bytes: int | None


def _index_names(indexes: Sequence[Mapping[str, Any]]) -> tuple[str, ...]:
    return tuple(sorted(str(item["name"]) for item in indexes))


def baseline_index_set() -> tuple[str, ...]:
    return _index_names(BASELINE_REMOVABLE_INDEXES)


def expand_candidates() -> list[CandidateSpec]:
    """Enumerate index, HASH and payload candidates exactly once each."""
    specs: list[CandidateSpec] = []
    baseline = baseline_index_set()
    specs.append(
        CandidateSpec(
            kind="index",
            variant="baseline",
            registry=None,
            hash_count=None,
            indexes=baseline,
            omitted_index=None,
            added_index=None,
            payload_ceiling_bytes=None,
        )
    )
    for index in BASELINE_REMOVABLE_INDEXES:
        name = str(index["name"])
        remaining = tuple(sorted(n for n in baseline if n != name))
        specs.append(
            CandidateSpec(
                kind="index",
                variant="omit",
                registry=None,
                hash_count=None,
                indexes=remaining,
                omitted_index=name,
                added_index=None,
                payload_ceiling_bytes=None,
            )
        )
    for added in BENCHMARK_CANDIDATE_INDEXES:
        name = str(added["name"])
        combined = tuple(sorted({*baseline, name}))
        specs.append(
            CandidateSpec(
                kind="index",
                variant="add_benchmark_candidate",
                registry=None,
                hash_count=None,
                indexes=combined,
                omitted_index=None,
                added_index=name,
                payload_ceiling_bytes=None,
            )
        )
    for registry in HASH_REGISTRIES:
        for count in HASH_COUNTS:
            specs.append(
                CandidateSpec(
                    kind="hash",
                    variant=None,
                    registry=registry,
                    hash_count=count,
                    indexes=None,
                    omitted_index=None,
                    added_index=None,
                    payload_ceiling_bytes=None,
                )
            )
    for ceiling in PAYLOAD_CEILINGS:
        specs.append(
            CandidateSpec(
                kind="payload",
                variant=None,
                registry=None,
                hash_count=None,
                indexes=None,
                omitted_index=None,
                added_index=None,
                payload_ceiling_bytes=ceiling,
            )
        )
    return specs


def canonical_spec_dict(spec: CandidateSpec) -> dict[str, Any]:
    return {
        "kind": spec.kind,
        "variant": spec.variant,
        "registry": spec.registry,
        "hash_count": spec.hash_count,
        "indexes": list(spec.indexes) if spec.indexes is not None else None,
        "omitted_index": spec.omitted_index,
        "added_index": spec.added_index,
        "payload_ceiling_bytes": spec.payload_ceiling_bytes,
        "whole_request_limit_bytes": WHOLE_REQUEST_LIMIT_BYTES,
    }


def canonical_spec_bytes(spec: CandidateSpec) -> bytes:
    return json.dumps(
        canonical_spec_dict(spec), sort_keys=True, separators=(",", ":")
    ).encode("utf-8")


def candidate_id(spec: CandidateSpec) -> str:
    return hashlib.sha256(canonical_spec_bytes(spec)).hexdigest()


def qualified_physical_signature_digest() -> str:
    """Deterministic digest for the Phase 12 priority-first qualified index."""
    payload = {
        "schema_revision": PHASE12_SCHEMA_REVISION,
        **PHASE12_PHYSICAL_SIGNATURE,
    }
    return hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()


def signature_for(spec: CandidateSpec) -> str:
    if spec.kind == "index":
        return ",".join(spec.indexes or ())
    if spec.kind == "hash":
        return f"{spec.registry}:{spec.hash_count}"
    if spec.kind == "payload":
        return str(spec.payload_ceiling_bytes)
    raise ValueError(f"unknown kind: {spec.kind}")


def catalog_signature_for(spec: CandidateSpec, cid: str) -> str:
    payload = {
        "candidate_id": cid,
        "spec": canonical_spec_dict(spec),
        "schema_revision": SCHEMA_REVISION,
    }
    digest = hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()
    return f"catalog-{digest[:32]}"


def _latency_rows(
    *,
    claim_p99_ns: int,
    enqueue_p99_ns: int,
    heartbeat_p99_ns: int,
    complete_p99_ns: int,
) -> list[dict[str, object]]:
    """Nearest-rank friendly samples that yield exact target p99 values."""

    def series(operation: str, scenario: str, p99: int, count: int = 100) -> list[dict[str, object]]:
        # Nearest-rank p99 index for n=100 is ceil(0.99*100)=99 → 0-based index 98.
        rows: list[dict[str, object]] = []
        for i in range(count):
            latency = max(1_000_000, p99 - (count - 1 - i) * 1_000)
            if i >= 98:
                latency = p99
            rows.append(
                {
                    "operation": operation,
                    "scenario": scenario,
                    "latency_ns": latency,
                    "outcome": "success",
                    "fan_out": 0,
                }
            )
        return rows

    rows = []
    rows.extend(series("claim", "baseline", claim_p99_ns))
    rows.extend(series("enqueue", "baseline", enqueue_p99_ns, 50))
    rows.extend(series("heartbeat", "baseline", heartbeat_p99_ns, 50))
    rows.extend(series("complete", "baseline", complete_p99_ns, 50))
    for i in range(10):
        rows.append(
            {
                "operation": "complete",
                "scenario": "max_fanout",
                "latency_ns": 400_000_000 + i * 1_000_000,
                "outcome": "success",
                "fan_out": 64,
            }
        )
    return rows


def _conformance_xml(
    *,
    run_id: str,
    environment_hash: str,
    workload_hash: str,
    candidate_id_value: str,
    schema_revision: str,
    catalog_signature: str,
) -> str:
    props = "\n".join(
        [
            f'      <property name="client_variant" value="{{variant}}"/>',
            f'      <property name="run_id" value="{run_id}"/>',
            f'      <property name="environment_hash" value="{environment_hash}"/>',
            f'      <property name="workload_hash" value="{workload_hash}"/>',
            f'      <property name="candidate_id" value="{candidate_id_value}"/>',
            f'      <property name="schema_revision" value="{schema_revision}"/>',
            f'      <property name="catalog_signature" value="{catalog_signature}"/>',
        ]
    )
    suite = """  <testsuite name="{variant}" tests="2" failures="0" errors="0" skipped="0">
    <properties>
{props}
    </properties>
    <testcase classname="kernel" name="enqueue_claim_complete" time="0.01"/>
    <testcase classname="kernel" name="heartbeat" time="0.01"/>
  </testsuite>"""
    blocks = [
        suite.format(variant="raw_http", props=props.format(variant="raw_http")),
        suite.format(variant="sdk", props=props.format(variant="sdk")),
    ]
    return (
        '<?xml version="1.0" encoding="UTF-8"?>\n'
        '<testsuites name="queue-qualification" tests="4" failures="0" errors="0" skipped="0">\n'
        + "\n".join(blocks)
        + "\n</testsuites>\n"
    )


def _synthetic_metrics(spec: CandidateSpec, cid: str) -> dict[str, Any]:
    """Deterministic eligible metrics with axis-specific variation for tie-breaks."""
    seed = int(cid[:8], 16)
    base_claim = 8_000_000 + (seed % 5_000_000)
    base_index_bytes = 1_000_000 + (seed % 200_000)
    base_wal = 10_000 + (seed % 5_000)

    if spec.kind == "index":
        omitted_penalty = 0 if spec.variant == "omit" else 50_000
        added_penalty = 80_000 if spec.variant == "add_benchmark_candidate" else 0
        # Fewer indexes → fewer bytes (omit wins on bytes vs baseline/add).
        index_count = len(spec.indexes or ())
        index_bytes = 40_000 * index_count + omitted_penalty + added_penalty
        claim = base_claim + (0 if spec.variant == "baseline" else 500_000)
        wal = base_wal + index_count * 10
        # Omitting the hot claim index is an unsafe sequential-scan risk (T-039-26).
        unsafe = spec.omitted_index == "tasks_active_claim_idx"
        if unsafe:
            claim = 150_000_000  # also fails p99 for clarity in evidence
    elif spec.kind == "hash":
        count = int(spec.hash_count or 1)
        index_bytes = 0
        # Higher partition counts slightly higher claim p99 / WAL (prefer smallest).
        claim = base_claim + (count - 1) * 100_000
        wal = base_wal + count * 100
        unsafe = False
    else:
        ceiling = int(spec.payload_ceiling_bytes or 262_144)
        index_bytes = 0
        # Both ceilings stay within 10% of 1 KiB baseline (20ms).
        baseline_1kib = 20_000_000
        if ceiling == 262_144:
            claim = 20_500_000
        else:
            claim = 21_000_000  # +5%
        wal = base_wal + ceiling // 10_000
        return {
            "index_bytes": index_bytes,
            "claim_p99_ns": claim,
            "enqueue_p99_ns": claim,
            "heartbeat_p99_ns": claim,
            "complete_p99_ns": 50_000_000,
            "wal_bytes": wal,
            "success_ratio": 0.9999,
            "unsafe_hot_seq_scan": False,
            "autovacuum_lag_seconds": 1,
            "measured_run_seconds": 120.0,
            "payload_p99_max_ns": claim,
            "baseline_1kib_p99_max_ns": baseline_1kib,
            "successful_claims": 60_000,
            "measured_seconds": 120.0,
        }

    return {
        "index_bytes": index_bytes,
        "claim_p99_ns": claim,
        "enqueue_p99_ns": min(claim, 20_000_000) if not unsafe else claim,
        "heartbeat_p99_ns": min(claim, 20_000_000) if not unsafe else claim,
        "complete_p99_ns": 50_000_000,
        "wal_bytes": wal,
        "success_ratio": 0.9999,
        "unsafe_hot_seq_scan": unsafe,
        "autovacuum_lag_seconds": 1,
        "measured_run_seconds": 120.0,
        "payload_p99_max_ns": None,
        "baseline_1kib_p99_max_ns": None,
        "successful_claims": 60_000,
        "measured_seconds": 120.0,
    }


def write_candidate_final_bundle(
    bundle: Path,
    *,
    spec: CandidateSpec,
    cid: str,
    metrics: Mapping[str, Any],
    environment_hash: str = "env-hash-candidates",
    git_sha: str = "synthetic-candidates",
) -> str:
    """Write a Plan-06 staged final 12-class bundle for one candidate."""
    if bundle.exists():
        shutil.rmtree(bundle)
    bundle.mkdir(parents=True)
    (bundle / "plans").mkdir()

    catalog_sig = catalog_signature_for(spec, cid)
    run_id = f"cand-{cid[:16]}"
    measured_seconds = float(metrics["measured_seconds"])
    successful_claims = int(metrics["successful_claims"])
    rows = _latency_rows(
        claim_p99_ns=int(metrics["claim_p99_ns"]),
        enqueue_p99_ns=int(metrics["enqueue_p99_ns"]),
        heartbeat_p99_ns=int(metrics["heartbeat_p99_ns"]),
        complete_p99_ns=int(metrics["complete_p99_ns"]),
    )
    sample_count = len(rows)
    workload = {
        "workload_hash": "wl-hash-candidates",
        "name": "kernel-capacity-candidate",
        "measured_seconds": measured_seconds,
        "successful_claims": successful_claims,
        "candidate_id": cid,
        "candidate_spec": canonical_spec_dict(spec),
        "payload_ceiling_bytes": spec.payload_ceiling_bytes,
        "whole_request_limit_bytes": WHOLE_REQUEST_LIMIT_BYTES,
        "baseline_1kib_payload_bytes": BASELINE_1KIB_PAYLOAD_BYTES,
        "evidence_mode": "synthetic",
        "evidence_note": (
            "Synthetic candidate measurement for CI selection; not a live "
            "reference-host capture."
        ),
        "latency_sample_count": sample_count,
    }
    environment = {
        "environment_hash": environment_hash,
        "git_sha": git_sha,
        "image_digests": {
            "queue": "sha256:" + ("a" * 64),
            "postgres": "sha256:" + ("b" * 64),
        },
        "schema_revision": SCHEMA_REVISION,
        "candidate_id": cid,
        "catalog_signature": catalog_sig,
    }
    before = {
        "environment_hash": environment_hash,
        "wal_bytes": 0,
        "buffer_hits": 0,
        "dead_tuples": 0,
        "autovacuum_lag_seconds": 0,
        "touched_partitions": [],
        "planning_time_ms": 1,
        "execution_time_ms": 1,
        "index_bytes": 0,
        "unsafe_hot_seq_scan": False,
    }
    after = {
        "environment_hash": environment_hash,
        "wal_bytes": int(metrics["wal_bytes"]),
        "buffer_hits": 900,
        "dead_tuples": 12,
        "autovacuum_lag_seconds": int(metrics["autovacuum_lag_seconds"]),
        "touched_partitions": ["history_2026_09_19"],
        "planning_time_ms": 2,
        "execution_time_ms": 40,
        "index_bytes": int(metrics["index_bytes"]),
        "unsafe_hot_seq_scan": bool(metrics["unsafe_hot_seq_scan"]),
    }
    explain = {
        "query": "SELECT /* candidate claim probe */ 1",
        "plan": {"Node Type": "Index Scan", "Index Name": "tasks_active_claim_idx"},
        "unsafe_seq_scan": False,
    }
    write_json(bundle / "plans" / "claim-baseline.json", explain)
    write_json(
        bundle / "plans" / "index.json",
        {"files": [{"path": "plans/claim-baseline.json", "operation": "claim"}]},
    )

    workload_hash = "wl-hash-candidates"
    xml = _conformance_xml(
        run_id=run_id,
        environment_hash=environment_hash,
        workload_hash=workload_hash,
        candidate_id_value=cid,
        schema_revision=SCHEMA_REVISION,
        catalog_signature=catalog_sig,
    )
    (bundle / "conformance.xml").write_text(xml, encoding="utf-8")

    with gzip.open(bundle / "latencies.jsonl.gz", "wt", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, sort_keys=True) + "\n")

    write_json(bundle / "environment.json", environment)
    write_json(bundle / "workload.json", workload)
    write_json(bundle / "postgres-before.json", before)
    write_json(bundle / "postgres-after.json", after)

    manifest = {
        "schema_version": 1,
        "run_id": run_id,
        "bundle_stage": "raw",
        "evidence_mode": "synthetic",
        "latency_sample_count": sample_count,
        "environment_hash": environment_hash,
        "workload_hash": workload_hash,
        "schema_revision": SCHEMA_REVISION,
        "catalog_signature": catalog_sig,
        "git_sha": git_sha,
        "image_digests": environment["image_digests"],
        "started_at_utc": "2026-09-19T00:00:00Z",
        "ended_at_utc": "2026-09-19T00:10:00Z",
        "conformance": {"path": "conformance.xml", "variants": ["raw_http", "sdk"]},
        "artifact_classes": {
            "raw": [
                "manifest.json",
                "environment.json",
                "workload.json",
                "conformance.xml",
                "latencies.jsonl.gz",
                "postgres-before.json",
                "postgres-after.json",
                "plans/index.json",
            ],
            "derived": ["summary.json", "qualification.json", "report.md"],
            "checksums": ["SHA256SUMS"],
        },
        "candidate_id": cid,
    }
    write_json(bundle / "manifest.json", manifest)

    validate_raw(bundle)
    derive(bundle, allow_synthetic=True)

    # Enrich summary with selection metrics while still raw, then promote.
    summary = load_json(bundle / "summary.json")
    postgres = dict(summary.get("postgres") or {})
    postgres["index_bytes"] = int(metrics["index_bytes"])
    postgres["unsafe_hot_seq_scan"] = bool(metrics["unsafe_hot_seq_scan"])
    postgres["measured_run_seconds"] = float(metrics["measured_run_seconds"])
    summary["postgres"] = postgres
    summary["candidate_id"] = cid
    summary["candidate_kind"] = spec.kind
    summary["candidate_signature"] = signature_for(spec)
    if metrics.get("payload_p99_max_ns") is not None:
        summary["payload_p99_max_ns"] = int(metrics["payload_p99_max_ns"])
        summary["baseline_1kib_p99_max_ns"] = int(metrics["baseline_1kib_p99_max_ns"])
    write_json(bundle / "summary.json", summary)

    write_checksums(bundle)
    validate_final(bundle, allow_synthetic=True)
    return sha256_file(bundle / CHECKSUM_CLASS)


def evidence_from_bundle(
    bundle: Path,
    *,
    spec: CandidateSpec,
    cid: str,
    relative_path: str,
    bundle_checksum: str,
) -> CandidateEvidence:
    summary = load_json(bundle / "summary.json")
    qualification = load_json(bundle / "qualification.json")
    postgres = summary.get("postgres") or {}
    checks = qualification.get("checks") or {}

    def check_actual(name: str) -> int | None:
        item = checks.get(name) or {}
        value = item.get("actual")
        return None if value is None else int(value)

    claim = check_actual("p99_claim_ns")
    enqueue = check_actual("p99_enqueue_ns")
    heartbeat = check_actual("p99_heartbeat_ns")
    complete = check_actual("p99_baseline_complete_ns")
    ratio = summary.get("overall_valid_success_ratio")
    payload_p99 = summary.get("payload_p99_max_ns")
    baseline_1kib = summary.get("baseline_1kib_p99_max_ns")
    if spec.kind == "payload":
        # Max of hot-path p99s for regression comparison.
        hot = [v for v in (claim, enqueue, heartbeat) if v is not None]
        payload_p99 = max(hot) if hot else payload_p99
        if baseline_1kib is None:
            baseline_1kib = 20_000_000

    evidence = CandidateEvidence(
        candidate_id=cid,
        kind=spec.kind,
        signature=signature_for(spec),
        eligible=False,
        ineligible_reasons=[],
        index_bytes=int(postgres.get("index_bytes") or 0),
        claim_p99_ns=claim,
        wal_bytes=int(postgres.get("wal_bytes") or 0),
        hash_count=spec.hash_count,
        registry=spec.registry,
        payload_ceiling_bytes=spec.payload_ceiling_bytes,
        payload_p99_max_ns=None if payload_p99 is None else int(payload_p99),
        baseline_1kib_p99_max_ns=None if baseline_1kib is None else int(baseline_1kib),
        success_ratio=None if ratio is None else float(ratio),
        enqueue_p99_ns=enqueue,
        heartbeat_p99_ns=heartbeat,
        complete_p99_ns=complete,
        unsafe_hot_seq_scan=bool(postgres.get("unsafe_hot_seq_scan", False)),
        autovacuum_lag_seconds=int(postgres.get("autovacuum_lag_seconds") or 0),
        measured_run_seconds=float(
            postgres.get("measured_run_seconds")
            or (summary.get("throughput") or {}).get("measured_seconds")
            or 0
        ),
        conformance_pass=True,
        validate_final_pass=True,
        schema_revision=SCHEMA_REVISION,
        catalog_signature=catalog_signature_for(spec, cid),
        bundle_path=relative_path,
        bundle_checksum=bundle_checksum,
    )
    return evidence


def require_candidate_bindings(bundle: Path, entry: Mapping[str, Any]) -> None:
    """Fail closed when conformance properties disagree with the candidate index."""
    import xml.etree.ElementTree as ET

    path = bundle / "conformance.xml"
    root = ET.fromstring(path.read_text(encoding="utf-8"))
    expected = {
        "candidate_id": str(entry["candidate_id"]),
        "schema_revision": str(entry["schema_revision"]),
        "catalog_signature": str(entry["catalog_signature"]),
    }
    seen: dict[str, set[str]] = {key: set() for key in expected}
    for prop in root.iter("property"):
        name = prop.attrib.get("name")
        if name in expected:
            seen[name].add(prop.attrib.get("value", ""))
    for key, value in expected.items():
        values = seen[key]
        if not values:
            raise ArtifactError(f"conformance.xml missing property {key}")
        if values != {value}:
            raise ArtifactError(
                f"conformance.xml {key} mismatch: expected {value!r}, got {sorted(values)}"
            )
    manifest = load_json(bundle / "manifest.json")
    if manifest.get("candidate_id") != entry["candidate_id"]:
        raise ArtifactError("manifest.candidate_id mismatch")
    if manifest.get("schema_revision") != entry["schema_revision"]:
        raise ArtifactError("manifest.schema_revision mismatch")
    if manifest.get("catalog_signature") != entry["catalog_signature"]:
        raise ArtifactError("manifest.catalog_signature mismatch")


def run_synthetic(output: Path) -> dict[str, Any]:
    """Build every candidate final bundle and write the aggregate index package."""
    if output.exists():
        shutil.rmtree(output)
    candidates_dir = output / "candidates"
    candidates_dir.mkdir(parents=True)

    specs = expand_candidates()
    evidence_rows: list[CandidateEvidence] = []
    index_entries: list[dict[str, Any]] = []

    for spec in specs:
        cid = candidate_id(spec)
        relative = f"candidates/{cid}"
        bundle = output / relative
        metrics = _synthetic_metrics(spec, cid)
        checksum = write_candidate_final_bundle(bundle, spec=spec, cid=cid, metrics=metrics)
        evidence = evidence_from_bundle(
            bundle,
            spec=spec,
            cid=cid,
            relative_path=relative.replace("\\", "/"),
            bundle_checksum=checksum,
        )
        evidence_rows.append(evidence)
        index_entries.append(
            {
                "candidate_id": cid,
                "kind": spec.kind,
                "signature": signature_for(spec),
                "schema_revision": SCHEMA_REVISION,
                "catalog_signature": catalog_signature_for(spec, cid),
                "bundle_path": relative.replace("\\", "/"),
                "bundle_checksum": checksum,
                "spec": canonical_spec_dict(spec),
            }
        )

    recommendation = evaluate_candidates(evidence_rows)
    from dataclasses import asdict

    comparison = {
        "candidate_count": len(evidence_rows),
        "by_kind": {
            kind: sum(1 for row in evidence_rows if row.kind == kind)
            for kind in ("index", "hash", "payload")
        },
        "candidates": recommendation["candidates"],
        "evidence": [asdict(row) for row in sorted(evidence_rows, key=lambda e: e.candidate_id)],
        "hash_counts": list(HASH_COUNTS),
        "payload_ceilings": list(PAYLOAD_CEILINGS),
        "baseline_1kib_payload_bytes": BASELINE_1KIB_PAYLOAD_BYTES,
        "whole_request_limit_bytes": WHOLE_REQUEST_LIMIT_BYTES,
    }
    index_manifest = {
        "schema_version": 1,
        "package_kind": "candidate-index",
        "phase": "3.9",
        "requirement": "QUAL-03",
        "mode": "synthetic",
        "candidate_count": len(index_entries),
        "schema_revision": SCHEMA_REVISION,
        "note": (
            "Aggregate candidate-index package — not a final qualification bundle; "
            "must not declare bundle_stage=final."
        ),
    }
    write_json(output / "index-manifest.json", index_manifest)
    write_json(
        candidates_dir / "index.json",
        {"candidates": sorted(index_entries, key=lambda e: e["candidate_id"])},
    )
    write_json(output / "comparison.json", comparison)
    write_json(output / "recommendation.json", recommendation)

    report_lines = [
        "# Phase 3.9 storage candidate recommendation",
        "",
        f"Verdict: **{recommendation['verdict']}**",
        "",
        f"Candidates measured: {len(evidence_rows)}",
        "",
        "## Indexes",
        "",
        json.dumps(recommendation["indexes"], indent=2, sort_keys=True),
        "",
        "## HASH partitions",
        "",
        json.dumps(recommendation["hash"], indent=2, sort_keys=True),
        "",
        "## Payload ceiling",
        "",
        json.dumps(recommendation["payload"], indent=2, sort_keys=True),
        "",
        "## Notes",
        "",
        "- HASH count 1 is the unpartitioned control.",
        "- Whole-request limit remains 1 MiB (1048576).",
        "- Aggregate package is a candidate-index, not `bundle_stage: final`.",
        "",
    ]
    if recommendation["verdict"] == "BLOCK":
        report_lines.extend(
            [
                "## BLOCK evidence",
                "",
                "No eligible candidate set produced a complete recommendation.",
                "See `recommendation.json` candidate ineligible_reasons.",
                "",
            ]
        )
    (output / "report.md").write_text("\n".join(report_lines), encoding="utf-8")

    # Aggregate SHA256SUMS covers index package files only (not nested candidate digests).
    aggregate_targets = [
        "index-manifest.json",
        "candidates/index.json",
        "comparison.json",
        "recommendation.json",
        "report.md",
    ]
    lines = [f"{sha256_file(output / rel)}  {rel}" for rel in aggregate_targets]
    (output / CHECKSUM_CLASS).write_text("\n".join(lines) + "\n", encoding="utf-8")
    return recommendation


def check_output(output: Path) -> None:
    """Validate aggregate index package and every referenced final candidate bundle."""
    output = output.resolve()
    if not output.is_dir():
        raise ArtifactError(f"candidate index package not found: {output}")

    for required in (
        "index-manifest.json",
        "candidates/index.json",
        "comparison.json",
        "recommendation.json",
        "report.md",
        CHECKSUM_CLASS,
    ):
        path = output / required
        if not path.is_file():
            raise ArtifactError(f"missing aggregate artifact: {required}")

    index_manifest = load_json(output / "index-manifest.json")
    if index_manifest.get("package_kind") != "candidate-index":
        raise ArtifactError("index-manifest.package_kind must be candidate-index")
    if index_manifest.get("bundle_stage") == "final":
        raise ArtifactError("aggregate package must not declare bundle_stage=final")

    sums_path = output / CHECKSUM_CLASS
    expected: dict[str, str] = {}
    for line in sums_path.read_text(encoding="utf-8").splitlines():
        text = line.strip()
        if not text or text.startswith("#"):
            continue
        digest_hex, relative = text.split(maxsplit=1)
        relative = relative.lstrip("*").strip()
        expected[relative] = digest_hex
    for relative, digest_hex in expected.items():
        path = output / relative
        if not path.is_file():
            raise ArtifactError(f"aggregate SHA256SUMS missing file: {relative}")
        if sha256_file(path) != digest_hex:
            raise ArtifactError(f"aggregate checksum mismatch for {relative}")

    candidates_index = load_json(output / "candidates" / "index.json")
    entries = candidates_index.get("candidates")
    if not isinstance(entries, list) or not entries:
        raise ArtifactError("candidates/index.json must list candidates")

    for entry in entries:
        relative = str(entry["bundle_path"])
        bundle = output / relative
        validate_final(bundle, allow_synthetic=True)
        require_candidate_bindings(bundle, entry)
        checksum_path = bundle / CHECKSUM_CLASS
        if sha256_file(checksum_path) != entry["bundle_checksum"]:
            raise ArtifactError(
                f"bundle_checksum mismatch for {entry['candidate_id']}: "
                "SHA256SUMS digest changed"
            )
        # Ensure SHA256SUMS covers conformance.xml
        covered = False
        for line in checksum_path.read_text(encoding="utf-8").splitlines():
            text = line.strip()
            if not text:
                continue
            _digest, rel = text.split(maxsplit=1)
            if rel.lstrip("*").strip() == "conformance.xml":
                covered = True
                break
        if not covered:
            raise ArtifactError(
                f"candidate {entry['candidate_id']} SHA256SUMS missing conformance.xml"
            )

    recommendation = load_json(output / "recommendation.json")
    comparison = load_json(output / "comparison.json")
    if recommendation.get("verdict") not in {"PASS", "BLOCK"}:
        raise ArtifactError("recommendation.verdict must be PASS or BLOCK")
    if recommendation["verdict"] == "PASS":
        if not recommendation.get("indexes", {}).get("selected_candidate_id"):
            raise ArtifactError("PASS recommendation missing index selection")
        for registry in HASH_REGISTRIES:
            if recommendation.get("hash", {}).get(registry, {}).get("selected_count") is None:
                raise ArtifactError(f"PASS recommendation missing HASH for {registry}")
        if recommendation.get("payload", {}).get("selected_ceiling_bytes") is None:
            raise ArtifactError("PASS recommendation missing payload ceiling")

    evidence_rows = comparison.get("evidence") or []
    if evidence_rows:
        rebuilt = evaluate_candidates(
            [
                CandidateEvidence(
                    candidate_id=str(row["candidate_id"]),
                    kind=str(row["kind"]),
                    signature=str(row.get("signature") or ""),
                    eligible=bool(row.get("eligible", False)),
                    ineligible_reasons=[str(x) for x in (row.get("ineligible_reasons") or [])],
                    index_bytes=int(row.get("index_bytes") or 0),
                    claim_p99_ns=row.get("claim_p99_ns"),
                    wal_bytes=int(row.get("wal_bytes") or 0),
                    hash_count=row.get("hash_count"),
                    registry=row.get("registry"),
                    payload_ceiling_bytes=row.get("payload_ceiling_bytes"),
                    payload_p99_max_ns=row.get("payload_p99_max_ns"),
                    baseline_1kib_p99_max_ns=row.get("baseline_1kib_p99_max_ns"),
                    success_ratio=row.get("success_ratio"),
                    enqueue_p99_ns=row.get("enqueue_p99_ns"),
                    heartbeat_p99_ns=row.get("heartbeat_p99_ns"),
                    complete_p99_ns=row.get("complete_p99_ns"),
                    unsafe_hot_seq_scan=bool(row.get("unsafe_hot_seq_scan", False)),
                    autovacuum_lag_seconds=int(row.get("autovacuum_lag_seconds") or 0),
                    measured_run_seconds=float(row.get("measured_run_seconds") or 0),
                    conformance_pass=bool(row.get("conformance_pass", False)),
                    validate_final_pass=bool(row.get("validate_final_pass", False)),
                    schema_revision=str(row.get("schema_revision") or ""),
                    catalog_signature=str(row.get("catalog_signature") or ""),
                    bundle_path=str(row.get("bundle_path") or ""),
                    bundle_checksum=str(row.get("bundle_checksum") or ""),
                )
                for row in evidence_rows
            ]
        )
        for key in ("verdict", "indexes", "hash", "payload"):
            if rebuilt.get(key) != recommendation.get(key):
                raise ArtifactError(
                    f"recommendation.{key} does not re-agree with comparison.evidence"
                )


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m benchmarks.qualification.storage_candidates",
        description="Measure and select Phase 3.9 storage candidates (QUAL-03).",
    )
    parser.add_argument(
        "--profile",
        type=Path,
        help="Reference environment profile (required for measurement run).",
    )
    parser.add_argument(
        "--workload",
        type=Path,
        help="Workload document (required for measurement run).",
    )
    parser.add_argument(
        "--output",
        type=Path,
        help="Output directory for the candidate-index package.",
    )
    parser.add_argument(
        "--synthetic",
        action="store_true",
        help="Use deterministic synthetic eligible evidence (CI / unit path).",
    )
    parser.add_argument(
        "--check-output",
        type=Path,
        help="Validate aggregate candidate-index package and nested final bundles.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)

    try:
        if args.check_output is not None:
            check_output(args.check_output)
            print(f"check-output OK path={args.check_output}")
            return 0
        if args.profile is not None or args.workload is not None or args.output is not None:
            if args.profile is None or args.workload is None or args.output is None:
                parser.error("--profile, --workload and --output are required together")
            if not args.synthetic:
                raise ArtifactError(
                    "live Compose measurement is operational; pass --synthetic "
                    "for deterministic CI evidence (QUAL-03 algorithm still applies)"
                )
            if not args.profile.is_file():
                raise ArtifactError(f"profile not found: {args.profile}")
            if not args.workload.is_file():
                raise ArtifactError(f"workload not found: {args.workload}")
            recommendation = run_synthetic(args.output)
            print(
                f"storage_candidates OK verdict={recommendation['verdict']} "
                f"output={args.output}"
            )
            return 0
        parser.error("specify --profile/--workload/--output or --check-output")
    except ArtifactError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
