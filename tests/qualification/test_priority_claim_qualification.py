"""PostgreSQL 18.6 priority claim qualification evidence (Phase 12 / WORK-16)."""

from __future__ import annotations

import hashlib
import json
import os
import subprocess
import sys
from pathlib import Path

import psycopg
import pytest

from benchmarks.qualification.artifacts import (
    MIN_CLAIMS_PER_SECOND,
    MIN_SUCCESS_RATIO,
    P99_HOT_NS,
    PHASE12_EVIDENCE_PACKAGE,
    PHASE12_QUALIFICATION_PROFILE,
    PHASE12_PRIORITY_WORKLOADS,
    validate_final,
    validate_phase12_priority_bindings,
)
from benchmarks.qualification.postgres_probe import validate_claim_explain_plan
from benchmarks.qualification.storage_candidates import (
    PHASE12_PHYSICAL_SIGNATURE,
    PHASE12_SCHEMA_REVISION,
    qualified_physical_signature_digest,
)

ROOT = Path(__file__).resolve().parents[2]
PHASE_39_FIXTURE = ROOT / "tests" / "fixtures" / "qualification" / "valid-run"
PHASE_39_MANIFEST = PHASE_39_FIXTURE / "manifest.json"
PHASE_39_CHECKSUMS = PHASE_39_FIXTURE / "SHA256SUMS"
PHASE_12_ROOT = ROOT / "benchmarks" / "results" / PHASE12_EVIDENCE_PACKAGE
CLI = [sys.executable, "-m", "benchmarks.qualification.cli"]
RUNNER = [sys.executable, "-m", "benchmarks.qualification.runner"]
CLAIMERS = 32
TARGET_CLAIMS_PER_SECOND = 500
SUCCESS_RATE_MIN = 0.999
PRIORITY_FIRST_INDEX = tuple(PHASE12_PHYSICAL_SIGNATURE["columns"])


def _require_database_url() -> str:
    url = os.environ.get("TEST_DATABASE_URL", "").strip()
    if not url:
        pytest.fail(
            "TEST_DATABASE_URL is required for priority claim qualification "
            "(PostgreSQL 18.6). Refusing to skip or xfail."
        )
    return url


def _to_psycopg_conninfo(url: str) -> str:
    if url.startswith("postgresql+psycopg://"):
        return "postgresql://" + url.removeprefix("postgresql+psycopg://")
    return url


def _phase_39_manifest_digest() -> str:
    return hashlib.sha256(PHASE_39_MANIFEST.read_bytes()).hexdigest()


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(65536), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _load(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


@pytest.fixture(scope="module")
def phase_39_baseline_digest() -> str:
    return _phase_39_manifest_digest()


@pytest.fixture(scope="module")
def phase_12_evidence_root() -> Path:
    _require_database_url()
    if not PHASE_12_ROOT.is_dir():
        result = subprocess.run(
            [
                *RUNNER,
                "--profile",
                "priority-claim",
                "--claimers",
                str(CLAIMERS),
            ],
            cwd=ROOT,
            capture_output=True,
            text=True,
            check=False,
        )
        assert result.returncode == 0, result.stderr + result.stdout
    for workload in PHASE12_PRIORITY_WORKLOADS:
        bundle = PHASE_12_ROOT / workload
        assert bundle.is_dir(), f"missing Phase 12 bundle for {workload}"
    return PHASE_12_ROOT


def test_phase_39_fixture_exists_and_is_not_phase_12_evidence() -> None:
    assert PHASE_39_MANIFEST.is_file()
    manifest = _load(PHASE_39_MANIFEST)
    assert manifest.get("qualification_profile") != PHASE12_QUALIFICATION_PROFILE
    assert manifest.get("evidence_package") != PHASE12_EVIDENCE_PACKAGE
    assert manifest.get("schema_revision") != PHASE12_SCHEMA_REVISION


def test_phase_39_checksum_file_unchanged_by_wave_0(phase_39_baseline_digest: str) -> None:
    assert PHASE_39_CHECKSUMS.is_file()
    assert _phase_39_manifest_digest() == phase_39_baseline_digest


@pytest.mark.parametrize("workload", list(PHASE12_PRIORITY_WORKLOADS))
def test_priority_workload_passes_release_gates_on_postgresql_18_6(
    workload: str,
    phase_12_evidence_root: Path,
) -> None:
    bundle = phase_12_evidence_root / workload
    validate_final(bundle)
    qualification = _load(bundle / "qualification.json")
    checks = qualification["checks"]
    assert qualification["verdict"] == "PASS"
    assert qualification["production_qualified"] is True
    assert checks["claims_per_second"]["pass"] is True
    assert float(checks["claims_per_second"]["actual"]) >= TARGET_CLAIMS_PER_SECOND
    assert checks["valid_success_ratio"]["pass"] is True
    assert float(checks["valid_success_ratio"]["actual"]) >= SUCCESS_RATE_MIN
    workload_doc = _load(bundle / "workload.json")
    assert workload_doc["claimers"] == CLAIMERS
    assert workload_doc["priority_workload"] == workload
    assert workload_doc.get("hot_path_scope") == "claim-only"
    assert checks["p99_claim_ns"]["pass"] is True
    assert int(checks["p99_claim_ns"]["actual"]) <= P99_HOT_NS
    for key in ("p99_enqueue_ns", "p99_heartbeat_ns"):
        assert checks[key]["pass"] is True
        assert checks[key]["actual"] is None
        assert "claim-only" in str(checks[key].get("note") or "")


def test_priority_first_physical_signature_binds_manifest_and_plan_json(
    phase_12_evidence_root: Path,
) -> None:
    expected_digest = qualified_physical_signature_digest()
    for workload in PHASE12_PRIORITY_WORKLOADS:
        bundle = phase_12_evidence_root / workload
        manifest = _load(bundle / "manifest.json")
        environment = _load(bundle / "environment.json")
        workload_doc = _load(bundle / "workload.json")
        assert manifest["schema_revision"] == PHASE12_SCHEMA_REVISION
        assert manifest["physical_signature"]["columns"] == list(PRIORITY_FIRST_INDEX)
        assert manifest["physical_signature_digest"] == expected_digest
        assert environment["schema_revision"] == PHASE12_SCHEMA_REVISION
        assert environment["physical_signature"] == PHASE12_PHYSICAL_SIGNATURE
        assert workload_doc["physical_signature_digest"] == expected_digest
        validate_phase12_priority_bindings(bundle, manifest)


def test_claim_explain_recursively_forbids_hot_table_seq_scan(
    phase_12_evidence_root: Path,
) -> None:
    for workload in PHASE12_PRIORITY_WORKLOADS:
        plan_doc = _load(phase_12_evidence_root / workload / "plans" / "claim-priority.json")
        plan = plan_doc.get("Plan") if isinstance(plan_doc.get("Plan"), dict) else plan_doc
        validate_claim_explain_plan(plan)


def test_qualification_bundle_cannot_relabel_phase_39_synthetic_as_phase_12(
    phase_12_evidence_root: Path,
    phase_39_baseline_digest: str,
) -> None:
    phase_39 = _load(PHASE_39_MANIFEST)
    assert phase_39.get("bundle_stage") in {"raw", "final"}
    assert _phase_39_manifest_digest() == phase_39_baseline_digest
    assert phase_39.get("schema_revision") != PHASE12_SCHEMA_REVISION
    for workload in PHASE12_PRIORITY_WORKLOADS:
        bundle = phase_12_evidence_root / workload
        manifest = _load(bundle / "manifest.json")
        assert manifest["evidence_mode"] == "live"
        assert manifest["evidence_package"] == PHASE12_EVIDENCE_PACKAGE
        assert manifest["run_id"].startswith("phase12-priority-")
        assert str(bundle).replace("\\", "/").endswith(
            f"{PHASE12_EVIDENCE_PACKAGE}/{workload}"
        )
    conn = psycopg.connect(_to_psycopg_conninfo(_require_database_url()))
    try:
        with conn.cursor() as cur:
            cur.execute("SHOW server_version_num")
            version_num = int(cur.fetchone()[0])
    finally:
        conn.close()
    major, minor = divmod(version_num, 10_000)
    assert (major, minor) == (18, 6)
