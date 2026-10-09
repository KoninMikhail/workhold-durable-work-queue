"""TDD: storage candidate measurement and deterministic selection (QUAL-03 / 03.9-09)."""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
CLI = [sys.executable, "-m", "benchmarks.qualification.storage_candidates"]
RESULTS = ROOT / "benchmarks" / "results" / "phase-3.9-candidates"


def _run(args: list[str], *, cwd: Path = ROOT) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [*CLI, *args],
        cwd=cwd,
        capture_output=True,
        text=True,
        check=False,
    )


def test_import_selection_and_storage_candidates() -> None:
    from benchmarks.qualification import selection, storage_candidates

    assert hasattr(selection, "evaluate_candidates")
    assert hasattr(storage_candidates, "expand_candidates")
    assert hasattr(storage_candidates, "candidate_id")


def test_hash_and_payload_candidate_axes() -> None:
    from benchmarks.qualification.selection import HASH_COUNTS, PAYLOAD_CEILINGS
    from benchmarks.qualification.storage_candidates import expand_candidates

    assert list(HASH_COUNTS) == [1, 4, 8, 16, 32]
    assert list(PAYLOAD_CEILINGS) == [262144, 1048576]

    specs = expand_candidates()
    kinds = {s.kind for s in specs}
    assert kinds == {"index", "hash", "payload"}

    hash_specs = [s for s in specs if s.kind == "hash"]
    registries = {(s.registry, s.hash_count) for s in hash_specs}
    assert registries == {
        (reg, count)
        for reg in ("enqueue_dedup", "complete_replay")
        for count in (1, 4, 8, 16, 32)
    }

    payload_specs = [s for s in specs if s.kind == "payload"]
    assert {s.payload_ceiling_bytes for s in payload_specs} == {262144, 1048576}

    index_specs = [s for s in specs if s.kind == "index"]
    assert any(s.variant == "baseline" for s in index_specs)
    assert any(s.variant == "omit" for s in index_specs)
    assert any(s.variant == "add_benchmark_candidate" for s in index_specs)


def test_candidate_id_is_sha256_of_canonical_spec() -> None:
    from benchmarks.qualification.storage_candidates import (
        CandidateSpec,
        candidate_id,
        canonical_spec_bytes,
    )

    a = CandidateSpec(
        kind="hash",
        registry="enqueue_dedup",
        hash_count=4,
        variant=None,
        indexes=None,
        payload_ceiling_bytes=None,
        omitted_index=None,
        added_index=None,
    )
    b = CandidateSpec(
        kind="hash",
        registry="enqueue_dedup",
        hash_count=4,
        variant=None,
        indexes=None,
        payload_ceiling_bytes=None,
        omitted_index=None,
        added_index=None,
    )
    cid = candidate_id(a)
    assert cid == candidate_id(b)
    assert len(cid) == 64
    assert cid == __import__("hashlib").sha256(canonical_spec_bytes(a)).hexdigest()


def _axis_stubs() -> list:
    """Minimal eligible HASH + payload rows so PASS requires only index under test."""
    from benchmarks.qualification.selection import CandidateEvidence

    def base(**kwargs: object) -> CandidateEvidence:
        defaults = dict(
            eligible=True,
            ineligible_reasons=[],
            index_bytes=0,
            claim_p99_ns=5_000_000,
            wal_bytes=100,
            hash_count=None,
            registry=None,
            payload_ceiling_bytes=None,
            payload_p99_max_ns=None,
            baseline_1kib_p99_max_ns=None,
            success_ratio=0.9999,
            enqueue_p99_ns=5_000_000,
            heartbeat_p99_ns=5_000_000,
            complete_p99_ns=40_000_000,
            unsafe_hot_seq_scan=False,
            autovacuum_lag_seconds=1,
            measured_run_seconds=100,
            conformance_pass=True,
            validate_final_pass=True,
            schema_revision="0001",
            catalog_signature="sig",
            bundle_checksum="abc",
        )
        defaults.update(kwargs)
        return CandidateEvidence(**defaults)  # type: ignore[arg-type]

    return [
        base(
            candidate_id="stub-ed-1",
            kind="hash",
            signature="enqueue_dedup:1",
            hash_count=1,
            registry="enqueue_dedup",
            bundle_path="candidates/stub-ed-1",
        ),
        base(
            candidate_id="stub-cr-1",
            kind="hash",
            signature="complete_replay:1",
            hash_count=1,
            registry="complete_replay",
            bundle_path="candidates/stub-cr-1",
        ),
        base(
            candidate_id="stub-p-256",
            kind="payload",
            signature="262144",
            payload_ceiling_bytes=262144,
            payload_p99_max_ns=5_000_000,
            baseline_1kib_p99_max_ns=5_000_000,
            bundle_path="candidates/stub-p-256",
        ),
    ]


def test_selection_index_tiebreak_fewest_bytes_then_claim_p99_then_wal_then_lexical() -> None:
    from benchmarks.qualification.selection import CandidateEvidence, evaluate_candidates

    stubs = _axis_stubs()

    def ev(
        cid: str,
        *,
        index_bytes: int,
        claim_p99: int,
        wal: int,
    ) -> CandidateEvidence:
        return CandidateEvidence(
            candidate_id=cid,
            kind="index",
            signature=cid,
            eligible=True,
            ineligible_reasons=[],
            index_bytes=index_bytes,
            claim_p99_ns=claim_p99,
            wal_bytes=wal,
            hash_count=None,
            registry=None,
            payload_ceiling_bytes=None,
            payload_p99_max_ns=None,
            baseline_1kib_p99_max_ns=None,
            success_ratio=0.9999,
            enqueue_p99_ns=10_000_000,
            heartbeat_p99_ns=10_000_000,
            complete_p99_ns=50_000_000,
            unsafe_hot_seq_scan=False,
            autovacuum_lag_seconds=1,
            measured_run_seconds=100,
            conformance_pass=True,
            validate_final_pass=True,
            schema_revision="0001",
            catalog_signature="sig",
            bundle_path=f"candidates/{cid}",
            bundle_checksum="abc",
        )

    # Same bytes; differ by claim p99 then WAL then lexical id.
    results = evaluate_candidates(
        [
            *stubs,
            ev("idx-bbb", index_bytes=100, claim_p99=20_000_000, wal=500),
            ev("idx-aaa", index_bytes=100, claim_p99=20_000_000, wal=500),
            ev("idx-ccc", index_bytes=90, claim_p99=50_000_000, wal=900),
            ev("idx-ddd", index_bytes=100, claim_p99=10_000_000, wal=800),
        ]
    )
    assert results["verdict"] == "PASS"
    assert results["indexes"]["selected_candidate_id"] == "idx-ccc"
    # Among 100-byte candidates, lowest claim p99 wins
    rerank = evaluate_candidates(
        [
            *stubs,
            ev("idx-bbb", index_bytes=100, claim_p99=20_000_000, wal=500),
            ev("idx-aaa", index_bytes=100, claim_p99=20_000_000, wal=500),
            ev("idx-ddd", index_bytes=100, claim_p99=10_000_000, wal=800),
        ]
    )
    assert rerank["indexes"]["selected_candidate_id"] == "idx-ddd"
    # Same bytes + claim; lower WAL then lexical
    tie = evaluate_candidates(
        [
            *stubs,
            ev("idx-bbb", index_bytes=100, claim_p99=10_000_000, wal=500),
            ev("idx-aaa", index_bytes=100, claim_p99=10_000_000, wal=500),
            ev("idx-zzz", index_bytes=100, claim_p99=10_000_000, wal=400),
        ]
    )
    assert tie["indexes"]["selected_candidate_id"] == "idx-zzz"
    lexical = evaluate_candidates(
        [
            *stubs,
            ev("idx-bbb", index_bytes=100, claim_p99=10_000_000, wal=400),
            ev("idx-aaa", index_bytes=100, claim_p99=10_000_000, wal=400),
        ]
    )
    assert lexical["indexes"]["selected_candidate_id"] == "idx-aaa"


def test_selection_hash_smallest_eligible_count() -> None:
    from benchmarks.qualification.selection import CandidateEvidence, evaluate_candidates

    stubs = [
        item
        for item in _axis_stubs()
        if item.kind != "hash"
    ]

    def hash_ev(
        cid: str,
        registry: str,
        count: int,
        *,
        claim_p99: int = 10_000_000,
        success_ratio: float = 0.9999,
    ) -> CandidateEvidence:
        return CandidateEvidence(
            candidate_id=cid,
            kind="hash",
            signature=f"{registry}:{count}",
            eligible=True,
            ineligible_reasons=[],
            index_bytes=0,
            claim_p99_ns=claim_p99,
            wal_bytes=1000 + count,
            hash_count=count,
            registry=registry,
            payload_ceiling_bytes=None,
            payload_p99_max_ns=None,
            baseline_1kib_p99_max_ns=None,
            success_ratio=success_ratio,
            enqueue_p99_ns=10_000_000,
            heartbeat_p99_ns=10_000_000,
            complete_p99_ns=50_000_000,
            unsafe_hot_seq_scan=False,
            autovacuum_lag_seconds=1,
            measured_run_seconds=100,
            conformance_pass=True,
            validate_final_pass=True,
            schema_revision="0001",
            catalog_signature="sig",
            bundle_path=f"candidates/{cid}",
            bundle_checksum="abc",
        )

    # count=1 fails success gate → smallest eligible for enqueue_dedup is 4
    results = evaluate_candidates(
        [
            *stubs,
            hash_ev("h-ed-1", "enqueue_dedup", 1, success_ratio=0.99),
            hash_ev("h-ed-4", "enqueue_dedup", 4),
            hash_ev("h-ed-8", "enqueue_dedup", 8),
            hash_ev("h-cr-1", "complete_replay", 1),
            hash_ev("h-cr-32", "complete_replay", 32),
            CandidateEvidence(
                candidate_id="stub-idx",
                kind="index",
                signature="stub-idx",
                eligible=True,
                ineligible_reasons=[],
                index_bytes=10,
                claim_p99_ns=5_000_000,
                wal_bytes=100,
                hash_count=None,
                registry=None,
                payload_ceiling_bytes=None,
                payload_p99_max_ns=None,
                baseline_1kib_p99_max_ns=None,
                success_ratio=0.9999,
                enqueue_p99_ns=5_000_000,
                heartbeat_p99_ns=5_000_000,
                complete_p99_ns=40_000_000,
                unsafe_hot_seq_scan=False,
                autovacuum_lag_seconds=1,
                measured_run_seconds=100,
                conformance_pass=True,
                validate_final_pass=True,
                schema_revision="0001",
                catalog_signature="sig",
                bundle_path="candidates/stub-idx",
                bundle_checksum="abc",
            ),
        ]
    )
    assert results["hash"]["enqueue_dedup"]["selected_count"] == 4
    assert results["hash"]["complete_replay"]["selected_count"] == 1


def test_selection_payload_largest_with_10pct_regression_cap() -> None:
    from benchmarks.qualification.selection import CandidateEvidence, evaluate_candidates

    stubs = [item for item in _axis_stubs() if item.kind != "payload"]
    stubs.append(
        CandidateEvidence(
            candidate_id="stub-idx",
            kind="index",
            signature="stub-idx",
            eligible=True,
            ineligible_reasons=[],
            index_bytes=10,
            claim_p99_ns=5_000_000,
            wal_bytes=100,
            hash_count=None,
            registry=None,
            payload_ceiling_bytes=None,
            payload_p99_max_ns=None,
            baseline_1kib_p99_max_ns=None,
            success_ratio=0.9999,
            enqueue_p99_ns=5_000_000,
            heartbeat_p99_ns=5_000_000,
            complete_p99_ns=40_000_000,
            unsafe_hot_seq_scan=False,
            autovacuum_lag_seconds=1,
            measured_run_seconds=100,
            conformance_pass=True,
            validate_final_pass=True,
            schema_revision="0001",
            catalog_signature="sig",
            bundle_path="candidates/stub-idx",
            bundle_checksum="abc",
        )
    )

    def payload_ev(
        cid: str,
        ceiling: int,
        p99_max: int,
        *,
        baseline_1kib: int = 20_000_000,
    ) -> CandidateEvidence:
        return CandidateEvidence(
            candidate_id=cid,
            kind="payload",
            signature=str(ceiling),
            eligible=True,
            ineligible_reasons=[],
            index_bytes=0,
            claim_p99_ns=min(p99_max, 90_000_000),
            wal_bytes=1000,
            hash_count=None,
            registry=None,
            payload_ceiling_bytes=ceiling,
            payload_p99_max_ns=p99_max,
            baseline_1kib_p99_max_ns=baseline_1kib,
            success_ratio=0.9999,
            enqueue_p99_ns=min(p99_max, 90_000_000),
            heartbeat_p99_ns=min(p99_max, 90_000_000),
            complete_p99_ns=50_000_000,
            unsafe_hot_seq_scan=False,
            autovacuum_lag_seconds=1,
            measured_run_seconds=100,
            conformance_pass=True,
            validate_final_pass=True,
            schema_revision="0001",
            catalog_signature="sig",
            bundle_path=f"candidates/{cid}",
            bundle_checksum="abc",
        )

    # 1 MiB regresses >10% vs 1 KiB → only 256 KiB eligible by regression rule
    results = evaluate_candidates(
        [
            *stubs,
            payload_ev("p-256", 262144, 21_000_000),  # +5%
            payload_ev("p-1m", 1048576, 25_000_000),  # +25% → reject by regression
        ]
    )
    assert results["payload"]["selected_ceiling_bytes"] == 262144

    both_ok = evaluate_candidates(
        [
            *stubs,
            payload_ev("p-256", 262144, 21_000_000),
            payload_ev("p-1m", 1048576, 21_500_000),  # +7.5%
        ]
    )
    assert both_ok["payload"]["selected_ceiling_bytes"] == 1048576


def test_ineligible_gates_and_block_when_none() -> None:
    from benchmarks.qualification.selection import (
        CandidateEvidence,
        assess_eligibility,
        evaluate_candidates,
    )

    bad = CandidateEvidence(
        candidate_id="bad",
        kind="index",
        signature="bad",
        eligible=False,
        ineligible_reasons=[],
        index_bytes=10,
        claim_p99_ns=150_000_000,  # >100ms
        wal_bytes=1,
        hash_count=None,
        registry=None,
        payload_ceiling_bytes=None,
        payload_p99_max_ns=None,
        baseline_1kib_p99_max_ns=None,
        success_ratio=0.99,
        enqueue_p99_ns=10_000_000,
        heartbeat_p99_ns=10_000_000,
        complete_p99_ns=50_000_000,
        unsafe_hot_seq_scan=True,
        autovacuum_lag_seconds=200,
        measured_run_seconds=100,
        conformance_pass=False,
        validate_final_pass=False,
        schema_revision="0001",
        catalog_signature="sig",
        bundle_path="candidates/bad",
        bundle_checksum="abc",
    )
    eligible, reasons = assess_eligibility(bad)
    assert eligible is False
    assert "success_ratio" in reasons
    assert "claim_p99" in reasons
    assert "unsafe_hot_seq_scan" in reasons
    assert "autovacuum_lag" in reasons
    assert "conformance" in reasons
    assert "validate_final" in reasons

    blocked = evaluate_candidates([bad])
    assert blocked["verdict"] == "BLOCK"
    assert blocked["indexes"]["selected_candidate_id"] is None


def test_selection_byte_identical_on_rerun() -> None:
    from benchmarks.qualification.selection import CandidateEvidence, evaluate_candidates

    items = [
        CandidateEvidence(
            candidate_id="idx-a",
            kind="index",
            signature="idx-a",
            eligible=True,
            ineligible_reasons=[],
            index_bytes=50,
            claim_p99_ns=5_000_000,
            wal_bytes=100,
            hash_count=None,
            registry=None,
            payload_ceiling_bytes=None,
            payload_p99_max_ns=None,
            baseline_1kib_p99_max_ns=None,
            success_ratio=0.9999,
            enqueue_p99_ns=5_000_000,
            heartbeat_p99_ns=5_000_000,
            complete_p99_ns=40_000_000,
            unsafe_hot_seq_scan=False,
            autovacuum_lag_seconds=1,
            measured_run_seconds=100,
            conformance_pass=True,
            validate_final_pass=True,
            schema_revision="0001",
            catalog_signature="sig",
            bundle_path="candidates/idx-a",
            bundle_checksum="abc",
        ),
        CandidateEvidence(
            candidate_id="h-ed-1",
            kind="hash",
            signature="enqueue_dedup:1",
            eligible=True,
            ineligible_reasons=[],
            index_bytes=0,
            claim_p99_ns=5_000_000,
            wal_bytes=100,
            hash_count=1,
            registry="enqueue_dedup",
            payload_ceiling_bytes=None,
            payload_p99_max_ns=None,
            baseline_1kib_p99_max_ns=None,
            success_ratio=0.9999,
            enqueue_p99_ns=5_000_000,
            heartbeat_p99_ns=5_000_000,
            complete_p99_ns=40_000_000,
            unsafe_hot_seq_scan=False,
            autovacuum_lag_seconds=1,
            measured_run_seconds=100,
            conformance_pass=True,
            validate_final_pass=True,
            schema_revision="0001",
            catalog_signature="sig",
            bundle_path="candidates/h-ed-1",
            bundle_checksum="abc",
        ),
        CandidateEvidence(
            candidate_id="h-cr-1",
            kind="hash",
            signature="complete_replay:1",
            eligible=True,
            ineligible_reasons=[],
            index_bytes=0,
            claim_p99_ns=5_000_000,
            wal_bytes=100,
            hash_count=1,
            registry="complete_replay",
            payload_ceiling_bytes=None,
            payload_p99_max_ns=None,
            baseline_1kib_p99_max_ns=None,
            success_ratio=0.9999,
            enqueue_p99_ns=5_000_000,
            heartbeat_p99_ns=5_000_000,
            complete_p99_ns=40_000_000,
            unsafe_hot_seq_scan=False,
            autovacuum_lag_seconds=1,
            measured_run_seconds=100,
            conformance_pass=True,
            validate_final_pass=True,
            schema_revision="0001",
            catalog_signature="sig",
            bundle_path="candidates/h-cr-1",
            bundle_checksum="abc",
        ),
        CandidateEvidence(
            candidate_id="p-256",
            kind="payload",
            signature="262144",
            eligible=True,
            ineligible_reasons=[],
            index_bytes=0,
            claim_p99_ns=5_000_000,
            wal_bytes=100,
            hash_count=None,
            registry=None,
            payload_ceiling_bytes=262144,
            payload_p99_max_ns=5_000_000,
            baseline_1kib_p99_max_ns=5_000_000,
            success_ratio=0.9999,
            enqueue_p99_ns=5_000_000,
            heartbeat_p99_ns=5_000_000,
            complete_p99_ns=40_000_000,
            unsafe_hot_seq_scan=False,
            autovacuum_lag_seconds=1,
            measured_run_seconds=100,
            conformance_pass=True,
            validate_final_pass=True,
            schema_revision="0001",
            catalog_signature="sig",
            bundle_path="candidates/p-256",
            bundle_checksum="abc",
        ),
    ]
    a = json.dumps(evaluate_candidates(items), sort_keys=True, separators=(",", ":"))
    b = json.dumps(evaluate_candidates(items), sort_keys=True, separators=(",", ":"))
    assert a == b


def test_synthetic_run_and_check_output(tmp_path: Path) -> None:
    out = tmp_path / "phase-3.9-candidates"
    result = _run(
        [
            "--synthetic",
            "--profile",
            "benchmarks/qualification/reference-environment.yaml",
            "--workload",
            "benchmarks/qualification/workloads/kernel-capacity.yaml",
            "--output",
            str(out),
        ]
    )
    assert result.returncode == 0, result.stderr + result.stdout

    index_manifest = json.loads((out / "index-manifest.json").read_text(encoding="utf-8"))
    assert index_manifest.get("package_kind") == "candidate-index"
    assert index_manifest.get("bundle_stage") != "final"

    candidates_index = json.loads((out / "candidates" / "index.json").read_text(encoding="utf-8"))
    assert len(candidates_index["candidates"]) >= 10

    recommendation = json.loads((out / "recommendation.json").read_text(encoding="utf-8"))
    assert recommendation["verdict"] in {"PASS", "BLOCK"}
    if recommendation["verdict"] == "PASS":
        assert recommendation["indexes"]["selected_candidate_id"]
        assert recommendation["hash"]["enqueue_dedup"]["selected_count"] in {1, 4, 8, 16, 32}
        assert recommendation["hash"]["complete_replay"]["selected_count"] in {1, 4, 8, 16, 32}
        assert recommendation["payload"]["selected_ceiling_bytes"] in {262144, 1048576}

    # Each candidate is a final 12-class bundle with matching conformance bindings
    first = candidates_index["candidates"][0]
    bundle = out / first["bundle_path"]
    assert (bundle / "SHA256SUMS").is_file()
    assert (bundle / "conformance.xml").is_file()
    conf = (bundle / "conformance.xml").read_text(encoding="utf-8")
    assert first["candidate_id"] in conf
    assert first["schema_revision"] in conf
    assert first["catalog_signature"] in conf

    check = _run(["--check-output", str(out)])
    assert check.returncode == 0, check.stderr + check.stdout


def test_checked_in_results_pass_check_output() -> None:
    assert RESULTS.is_dir(), "checked-in phase-3.9-candidates results missing"
    check = _run(["--check-output", str(RESULTS)])
    assert check.returncode == 0, check.stderr + check.stdout


def test_tampered_conformance_fails_check_output(tmp_path: Path) -> None:
    out = tmp_path / "phase-3.9-candidates"
    result = _run(
        [
            "--synthetic",
            "--profile",
            "benchmarks/qualification/reference-environment.yaml",
            "--workload",
            "benchmarks/qualification/workloads/kernel-capacity.yaml",
            "--output",
            str(out),
        ]
    )
    assert result.returncode == 0, result.stderr
    candidates_index = json.loads((out / "candidates" / "index.json").read_text(encoding="utf-8"))
    bundle = out / candidates_index["candidates"][0]["bundle_path"]
    xml_path = bundle / "conformance.xml"
    xml_path.write_text(
        xml_path.read_text(encoding="utf-8").replace("failures=\"0\"", "failures=\"1\"", 1),
        encoding="utf-8",
    )
    check = _run(["--check-output", str(out)])
    assert check.returncode != 0
