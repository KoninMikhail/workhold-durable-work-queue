"""Deterministic storage-candidate acceptance and selection (QUAL-03)."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any, Iterable, Mapping, Sequence

from benchmarks.qualification.artifacts import (
    MIN_SUCCESS_RATIO,
    P99_BASELINE_COMPLETE_NS,
    P99_HOT_NS,
)

HASH_COUNTS: tuple[int, ...] = (1, 4, 8, 16, 32)
PAYLOAD_CEILINGS: tuple[int, ...] = (262_144, 1_048_576)
PAYLOAD_P99_REGRESSION_LIMIT = 0.10
BASELINE_1KIB_PAYLOAD_BYTES = 1024
HASH_REGISTRIES: tuple[str, ...] = ("enqueue_dedup", "complete_replay")


@dataclass(frozen=True)
class CandidateEvidence:
    """Measured or synthetic evidence for one storage candidate."""

    candidate_id: str
    kind: str
    signature: str
    eligible: bool
    ineligible_reasons: list[str]
    index_bytes: int
    claim_p99_ns: int | None
    wal_bytes: int
    hash_count: int | None
    registry: str | None
    payload_ceiling_bytes: int | None
    payload_p99_max_ns: int | None
    baseline_1kib_p99_max_ns: int | None
    success_ratio: float | None
    enqueue_p99_ns: int | None
    heartbeat_p99_ns: int | None
    complete_p99_ns: int | None
    unsafe_hot_seq_scan: bool
    autovacuum_lag_seconds: int
    measured_run_seconds: float
    conformance_pass: bool
    validate_final_pass: bool
    schema_revision: str
    catalog_signature: str
    bundle_path: str
    bundle_checksum: str


def assess_eligibility(evidence: CandidateEvidence) -> tuple[bool, list[str]]:
    """Apply fixed release gates; never executor judgment."""
    reasons: list[str] = []
    if not evidence.validate_final_pass:
        reasons.append("validate_final")
    if not evidence.conformance_pass:
        reasons.append("conformance")
    ratio = evidence.success_ratio
    if ratio is None or ratio < MIN_SUCCESS_RATIO:
        reasons.append("success_ratio")
    for name, value in (
        ("enqueue_p99", evidence.enqueue_p99_ns),
        ("claim_p99", evidence.claim_p99_ns),
        ("heartbeat_p99", evidence.heartbeat_p99_ns),
    ):
        if value is None or value > P99_HOT_NS:
            reasons.append(name)
    if evidence.complete_p99_ns is None or evidence.complete_p99_ns > P99_BASELINE_COMPLETE_NS:
        reasons.append("complete_p99")
    if evidence.unsafe_hot_seq_scan:
        reasons.append("unsafe_hot_seq_scan")
    if evidence.autovacuum_lag_seconds >= evidence.measured_run_seconds:
        reasons.append("autovacuum_lag")
    return (len(reasons) == 0, reasons)


def _with_eligibility(items: Iterable[CandidateEvidence]) -> list[CandidateEvidence]:
    refreshed: list[CandidateEvidence] = []
    for item in items:
        ok, reasons = assess_eligibility(item)
        refreshed.append(
            CandidateEvidence(
                **{
                    **asdict(item),
                    "eligible": ok,
                    "ineligible_reasons": reasons,
                }
            )
        )
    return refreshed


def _eligible(items: Sequence[CandidateEvidence], kind: str) -> list[CandidateEvidence]:
    return [item for item in items if item.kind == kind and item.eligible]


def _select_index(candidates: Sequence[CandidateEvidence]) -> dict[str, Any]:
    pool = _eligible(candidates, "index")
    if not pool:
        return {
            "selected_candidate_id": None,
            "selected_signature": None,
            "eligible_count": 0,
            "tie_break": ["index_bytes", "claim_p99_ns", "wal_bytes", "candidate_id"],
        }
    winner = sorted(
        pool,
        key=lambda c: (
            c.index_bytes,
            c.claim_p99_ns if c.claim_p99_ns is not None else 2**63 - 1,
            c.wal_bytes,
            c.candidate_id,
        ),
    )[0]
    return {
        "selected_candidate_id": winner.candidate_id,
        "selected_signature": winner.signature,
        "eligible_count": len(pool),
        "tie_break": ["index_bytes", "claim_p99_ns", "wal_bytes", "candidate_id"],
        "index_bytes": winner.index_bytes,
        "claim_p99_ns": winner.claim_p99_ns,
        "wal_bytes": winner.wal_bytes,
    }


def _select_hash_registry(
    candidates: Sequence[CandidateEvidence], registry: str
) -> dict[str, Any]:
    pool = [
        item
        for item in candidates
        if item.kind == "hash" and item.eligible and item.registry == registry
    ]
    if not pool:
        return {
            "selected_candidate_id": None,
            "selected_count": None,
            "eligible_count": 0,
            "tie_break": ["hash_count", "claim_p99_ns", "wal_bytes", "candidate_id"],
        }
    winner = sorted(
        pool,
        key=lambda c: (
            c.hash_count if c.hash_count is not None else 2**31 - 1,
            c.claim_p99_ns if c.claim_p99_ns is not None else 2**63 - 1,
            c.wal_bytes,
            c.candidate_id,
        ),
    )[0]
    return {
        "selected_candidate_id": winner.candidate_id,
        "selected_count": winner.hash_count,
        "eligible_count": len(pool),
        "tie_break": ["hash_count", "claim_p99_ns", "wal_bytes", "candidate_id"],
        "claim_p99_ns": winner.claim_p99_ns,
        "wal_bytes": winner.wal_bytes,
    }


def _payload_regression_ok(item: CandidateEvidence) -> bool:
    baseline = item.baseline_1kib_p99_max_ns
    actual = item.payload_p99_max_ns
    if baseline is None or actual is None:
        return False
    if baseline <= 0:
        return actual <= 0
    return actual <= baseline * (1.0 + PAYLOAD_P99_REGRESSION_LIMIT)


def _select_payload(candidates: Sequence[CandidateEvidence]) -> dict[str, Any]:
    pool = [
        item
        for item in candidates
        if item.kind == "payload" and item.eligible and _payload_regression_ok(item)
    ]
    if not pool:
        return {
            "selected_candidate_id": None,
            "selected_ceiling_bytes": None,
            "eligible_count": 0,
            "regression_limit": PAYLOAD_P99_REGRESSION_LIMIT,
            "tie_break": ["payload_ceiling_bytes_desc", "candidate_id"],
        }
    winner = sorted(
        pool,
        key=lambda c: (
            -(c.payload_ceiling_bytes or 0),
            c.candidate_id,
        ),
    )[0]
    return {
        "selected_candidate_id": winner.candidate_id,
        "selected_ceiling_bytes": winner.payload_ceiling_bytes,
        "eligible_count": len(pool),
        "regression_limit": PAYLOAD_P99_REGRESSION_LIMIT,
        "payload_p99_max_ns": winner.payload_p99_max_ns,
        "baseline_1kib_p99_max_ns": winner.baseline_1kib_p99_max_ns,
        "tie_break": ["payload_ceiling_bytes_desc", "candidate_id"],
    }


def evaluate_candidates(items: Sequence[CandidateEvidence]) -> dict[str, Any]:
    """Return a machine-readable recommendation; BLOCK when any axis lacks eligibility."""
    refreshed = _with_eligibility(items)
    indexes = _select_index(refreshed)
    hash_ed = _select_hash_registry(refreshed, "enqueue_dedup")
    hash_cr = _select_hash_registry(refreshed, "complete_replay")
    payload = _select_payload(refreshed)

    axes_ok = (
        indexes["selected_candidate_id"] is not None
        and hash_ed["selected_count"] is not None
        and hash_cr["selected_count"] is not None
        and payload["selected_ceiling_bytes"] is not None
    )
    verdict = "PASS" if axes_ok else "BLOCK"
    return {
        "verdict": verdict,
        "indexes": indexes,
        "hash": {
            "enqueue_dedup": hash_ed,
            "complete_replay": hash_cr,
        },
        "payload": payload,
        "candidates": [
            {
                "candidate_id": item.candidate_id,
                "kind": item.kind,
                "signature": item.signature,
                "eligible": item.eligible,
                "ineligible_reasons": list(item.ineligible_reasons),
                "index_bytes": item.index_bytes,
                "claim_p99_ns": item.claim_p99_ns,
                "wal_bytes": item.wal_bytes,
                "hash_count": item.hash_count,
                "registry": item.registry,
                "payload_ceiling_bytes": item.payload_ceiling_bytes,
                "bundle_path": item.bundle_path,
                "bundle_checksum": item.bundle_checksum,
                "schema_revision": item.schema_revision,
                "catalog_signature": item.catalog_signature,
            }
            for item in sorted(refreshed, key=lambda c: c.candidate_id)
        ],
        "gates": {
            "min_success_ratio": MIN_SUCCESS_RATIO,
            "p99_hot_ns_max": P99_HOT_NS,
            "p99_baseline_complete_ns_max": P99_BASELINE_COMPLETE_NS,
            "payload_p99_regression_limit": PAYLOAD_P99_REGRESSION_LIMIT,
        },
        "sources": {
            "adr_006": "docs/04-architecture/adr/006-hot-cold-partitioning.md",
            "adr_019": "docs/04-architecture/adr/019-initial-production-gate.md",
            "storage_topology": "docs/04-architecture/04-storage-topology.md",
            "benchmarks": "docs/04-architecture/12-physical-contract-benchmarks.md",
        },
    }


def evidence_from_mapping(row: Mapping[str, Any]) -> CandidateEvidence:
    """Rebuild CandidateEvidence from comparison/recommendation JSON rows."""
    return CandidateEvidence(
        candidate_id=str(row["candidate_id"]),
        kind=str(row["kind"]),
        signature=str(row.get("signature") or row["candidate_id"]),
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
