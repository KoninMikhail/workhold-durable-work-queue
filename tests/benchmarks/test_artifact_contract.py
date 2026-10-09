"""Lifecycle and fail-closed contract tests for qualification artifacts (QUAL-03/05)."""

from __future__ import annotations

import gzip
import hashlib
import json
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
CLI = [sys.executable, "-m", "benchmarks.qualification.cli"]
VALID_RUN = ROOT / "tests" / "fixtures" / "qualification" / "valid-run"

RAW_CLASSES = (
    "manifest.json",
    "environment.json",
    "workload.json",
    "conformance.xml",
    "latencies.jsonl.gz",
    "postgres-before.json",
    "postgres-after.json",
    "plans/index.json",
)
DERIVED_CLASSES = ("summary.json", "qualification.json", "report.md")
FINAL_TOP_LEVEL = (
    "manifest.json",
    "environment.json",
    "workload.json",
    "conformance.xml",
    "latencies.jsonl.gz",
    "postgres-before.json",
    "postgres-after.json",
    "plans",
    "summary.json",
    "qualification.json",
    "report.md",
    "SHA256SUMS",
)


def _run(args: list[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [*CLI, *args],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=False,
    )


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(65536), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _conformance_xml(*, failures: int = 0, skipped: int = 0) -> str:
    return f"""<?xml version="1.0" encoding="UTF-8"?>
<testsuites name="queue-qualification" tests="4" failures="{failures}" errors="0" skipped="{skipped}">
  <testsuite name="raw_http" tests="2" failures="{failures}" errors="0" skipped="{skipped}">
    <properties>
      <property name="client_variant" value="raw_http"/>
      <property name="run_id" value="run-fixture-001"/>
      <property name="environment_hash" value="env-hash-aaa"/>
      <property name="workload_hash" value="wl-hash-bbb"/>
    </properties>
    <testcase classname="kernel" name="enqueue_claim_complete" time="0.01"/>
    <testcase classname="kernel" name="heartbeat" time="0.01"/>
  </testsuite>
  <testsuite name="sdk" tests="2" failures="0" errors="0" skipped="0">
    <properties>
      <property name="client_variant" value="sdk"/>
      <property name="run_id" value="run-fixture-001"/>
      <property name="environment_hash" value="env-hash-aaa"/>
      <property name="workload_hash" value="wl-hash-bbb"/>
    </properties>
    <testcase classname="kernel" name="enqueue_claim_complete" time="0.01"/>
    <testcase classname="kernel" name="heartbeat" time="0.01"/>
  </testsuite>
</testsuites>
"""


def _latency_lines() -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for i in range(100):
        rows.append(
            {
                "operation": "claim",
                "scenario": "baseline",
                "latency_ns": 1_000_000 + i * 100_000,
                "outcome": "empty" if i % 10 == 0 else "success",
                "fan_out": 0,
            }
        )
    for op, base in (("enqueue", 2_000_000), ("heartbeat", 3_000_000)):
        for i in range(50):
            rows.append(
                {
                    "operation": op,
                    "scenario": "baseline",
                    "latency_ns": base + i * 50_000,
                    "outcome": "success",
                    "fan_out": 0,
                }
            )
    for i in range(50):
        rows.append(
            {
                "operation": "complete",
                "scenario": "baseline",
                "latency_ns": 5_000_000 + i * 100_000,
                "outcome": "success",
                "fan_out": 0 if i % 2 == 0 else 8,
            }
        )
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
    rows.append(
        {
            "operation": "claim",
            "scenario": "pause",
            "latency_ns": 1_000_000,
            "outcome": "planned_pause",
            "fan_out": 0,
        }
    )
    rows.append(
        {
            "operation": "enqueue",
            "scenario": "invalid",
            "latency_ns": 1_000_000,
            "outcome": "invalid_request",
            "fan_out": 0,
        }
    )
    return rows


def build_raw_bundle(
    dest: Path,
    *,
    env_hash: str = "env-hash-aaa",
    evidence_mode: str = "live",
) -> None:
    if dest.exists():
        shutil.rmtree(dest)
    dest.mkdir(parents=True)
    _write_json(
        dest / "plans" / "claim-baseline.json",
        {"Plan": {"Node Type": "Index Scan", "Total Cost": 1.0}},
    )
    _write_json(
        dest / "plans" / "index.json",
        {
            "files": [
                {
                    "path": "plans/claim-baseline.json",
                    "operation": "claim",
                    "scenario": "baseline",
                }
            ]
        },
    )
    environment = {
        "environment_hash": env_hash,
        "git_sha": "abc123def456",
        "image_digests": {
            "queue": "sha256:" + "a" * 64,
            "postgres": "sha256:" + "b" * 64,
        },
        "postgres_version": "16.6",
        "started_at_utc": "2026-09-19T00:00:00Z",
        "ended_at_utc": "2026-09-19T00:10:00Z",
    }
    latency_rows = _latency_lines()
    sample_count = len(latency_rows)
    workload: dict[str, object] = {
        "workload_hash": "wl-hash-bbb",
        "profile": "phase-3.9-linux-x86_64-v1",
        "target_claims_per_second": 500,
        "measured_seconds": 300,
        "successful_claims": 150_000,
        "terminal_lifecycles": 1_000_000,
        "evidence_mode": evidence_mode,
        "latency_sample_count": sample_count,
    }
    if evidence_mode != "live":
        workload["evidence_note"] = (
            "Synthetic fixture for unit tests; not a live reference-host capture."
        )
    _write_json(dest / "environment.json", environment)
    _write_json(dest / "workload.json", workload)
    (dest / "conformance.xml").write_text(_conformance_xml(), encoding="utf-8")
    with gzip.open(dest / "latencies.jsonl.gz", "wt", encoding="utf-8") as handle:
        for row in latency_rows:
            handle.write(json.dumps(row, sort_keys=True) + "\n")
    _write_json(
        dest / "postgres-before.json",
        {
            "environment_hash": env_hash,
            "wal_bytes": 1000,
            "buffer_hits": 100,
            "dead_tuples": 0,
            "autovacuum_lag_seconds": 0,
            "touched_partitions": [],
        },
    )
    _write_json(
        dest / "postgres-after.json",
        {
            "environment_hash": env_hash,
            "wal_bytes": 5000,
            "buffer_hits": 900,
            "dead_tuples": 12,
            "autovacuum_lag_seconds": 1,
            "touched_partitions": ["history_2026_09_19"],
            "planning_time_ms": 2,
            "execution_time_ms": 40,
        },
    )
    _write_json(
        dest / "manifest.json",
        {
            "schema_version": 1,
            "run_id": "run-fixture-001",
            "bundle_stage": "raw",
            "environment_hash": env_hash,
            "workload_hash": "wl-hash-bbb",
            "schema_revision": "0001_physical_contract_foundations",
            "catalog_signature": "catalog-sig-001",
            "git_sha": "abc123def456",
            "image_digests": environment["image_digests"],
            "started_at_utc": environment["started_at_utc"],
            "ended_at_utc": environment["ended_at_utc"],
            "evidence_mode": evidence_mode,
            "latency_sample_count": sample_count,
            "conformance": {
                "path": "conformance.xml",
                "variants": ["raw_http", "sdk"],
            },
            "artifact_classes": {
                "raw": list(RAW_CLASSES),
                "derived": list(DERIVED_CLASSES),
                "checksums": ["SHA256SUMS"],
            },
        },
    )


@pytest.fixture()
def raw_bundle(tmp_path: Path) -> Path:
    dest = tmp_path / "raw-run"
    build_raw_bundle(dest)
    return dest


def test_derive_refuses_without_validate_raw(raw_bundle: Path) -> None:
    result = _run(["derive", str(raw_bundle)])
    assert result.returncode != 0
    assert "validate-raw" in (result.stderr + result.stdout).lower()


def test_validate_raw_rejects_missing_evidence_mode(raw_bundle: Path) -> None:
    from benchmarks.qualification.artifacts import ArtifactError, validate_raw

    manifest = json.loads((raw_bundle / "manifest.json").read_text(encoding="utf-8"))
    del manifest["evidence_mode"]
    _write_json(raw_bundle / "manifest.json", manifest)
    with pytest.raises(ArtifactError, match="evidence_mode"):
        validate_raw(raw_bundle)


def test_derive_refuses_synthetic_without_allow_synthetic(tmp_path: Path) -> None:
    dest = tmp_path / "synthetic-raw"
    build_raw_bundle(dest, evidence_mode="synthetic")
    assert _run(["validate-raw", str(dest)]).returncode == 0
    refused = _run(["derive", str(dest)])
    assert refused.returncode != 0
    assert "allow-synthetic" in (refused.stderr + refused.stdout).lower()
    allowed = _run(["derive", "--allow-synthetic", str(dest)])
    assert allowed.returncode == 0, allowed.stderr + allowed.stdout


def test_assert_evidence_mode_helper_unit() -> None:
    from benchmarks.qualification.artifacts import (
        ArtifactError,
        assert_evidence_mode_for_qualification,
    )

    assert_evidence_mode_for_qualification("live")
    with pytest.raises(ArtifactError, match="allow-synthetic"):
        assert_evidence_mode_for_qualification("synthetic")
    assert_evidence_mode_for_qualification("synthetic", allow_synthetic=True)


def test_checksums_refuses_before_derived_files(raw_bundle: Path) -> None:
    assert _run(["validate-raw", str(raw_bundle)]).returncode == 0
    result = _run(["checksums", str(raw_bundle)])
    assert result.returncode != 0


def test_full_lifecycle_raw_to_final(raw_bundle: Path) -> None:
    assert _run(["validate-raw", str(raw_bundle)]).returncode == 0
    assert _run(["derive", str(raw_bundle)]).returncode == 0
    assert _run(["checksums", str(raw_bundle)]).returncode == 0
    assert _run(["validate-final", str(raw_bundle)]).returncode == 0
    manifest = json.loads((raw_bundle / "manifest.json").read_text(encoding="utf-8"))
    assert manifest["bundle_stage"] == "final"
    assert "validated_raw_set_digest" in manifest
    assert (raw_bundle / "SHA256SUMS").is_file()
    for name in DERIVED_CLASSES:
        assert (raw_bundle / name).is_file()


def test_raw_bundle_rejected_by_validate_final(raw_bundle: Path) -> None:
    assert _run(["validate-raw", str(raw_bundle)]).returncode == 0
    assert _run(["validate-final", str(raw_bundle)]).returncode != 0


def test_final_fixture_has_exactly_twelve_authoritative_classes() -> None:
    assert VALID_RUN.is_dir()
    top = sorted(p.name for p in VALID_RUN.iterdir())
    assert top == sorted(FINAL_TOP_LEVEL)
    assert _run(["validate-final", str(VALID_RUN)]).returncode == 0
    sums = (VALID_RUN / "SHA256SUMS").read_text(encoding="utf-8").strip().splitlines()
    listed = {line.split(maxsplit=1)[1] for line in sums if line.strip()}
    assert "SHA256SUMS" not in listed
    for relative in (
        "manifest.json",
        "environment.json",
        "workload.json",
        "conformance.xml",
        "latencies.jsonl.gz",
        "postgres-before.json",
        "postgres-after.json",
        "plans/index.json",
        "summary.json",
        "qualification.json",
        "report.md",
        "plans/claim-baseline.json",
    ):
        assert relative in listed
        expected = next(line.split()[0] for line in sums if line.endswith(f" {relative}"))
        assert _sha256_file(VALID_RUN / relative) == expected


def test_missing_artifact_fails_validate_raw(raw_bundle: Path) -> None:
    (raw_bundle / "workload.json").unlink()
    assert _run(["validate-raw", str(raw_bundle)]).returncode != 0


def test_mixed_environment_fails_validate_raw(raw_bundle: Path) -> None:
    after = json.loads((raw_bundle / "postgres-after.json").read_text(encoding="utf-8"))
    after["environment_hash"] = "env-hash-OTHER"
    _write_json(raw_bundle / "postgres-after.json", after)
    assert _run(["validate-raw", str(raw_bundle)]).returncode != 0


def test_conformance_failure_fails_validate_raw(raw_bundle: Path) -> None:
    (raw_bundle / "conformance.xml").write_text(
        _conformance_xml(failures=1), encoding="utf-8"
    )
    assert _run(["validate-raw", str(raw_bundle)]).returncode != 0


def test_conformance_missing_variants_fails_validate_raw(raw_bundle: Path) -> None:
    """Tests present but no client_variant properties must fail validate-raw."""
    xml = """<?xml version="1.0" encoding="UTF-8"?>
<testsuites name="queue-qualification" tests="2" failures="0" errors="0" skipped="0">
  <testsuite name="kernel" tests="2" failures="0" errors="0" skipped="0">
    <properties>
      <property name="run_id" value="run-fixture-001"/>
      <property name="environment_hash" value="env-hash-aaa"/>
      <property name="workload_hash" value="wl-hash-bbb"/>
    </properties>
    <testcase classname="kernel" name="enqueue_claim_complete" time="0.01"/>
    <testcase classname="kernel" name="heartbeat" time="0.01"/>
  </testsuite>
</testsuites>
"""
    (raw_bundle / "conformance.xml").write_text(xml, encoding="utf-8")
    assert _run(["validate-raw", str(raw_bundle)]).returncode != 0


def _rewrite_checksums(bundle: Path) -> None:
    """Regenerate SHA256SUMS for a final bundle after deliberate tampering."""
    from benchmarks.qualification.artifacts import checksum_targets, sha256_file

    lines = [
        f"{sha256_file(bundle / relative)}  {relative}"
        for relative in checksum_targets(bundle)
    ]
    (bundle / "SHA256SUMS").write_text("\n".join(lines) + "\n", encoding="utf-8")


def test_tampered_file_fails_validate_final(tmp_path: Path) -> None:
    dest = tmp_path / "final"
    shutil.copytree(VALID_RUN, dest)
    summary = json.loads((dest / "summary.json").read_text(encoding="utf-8"))
    summary["tampered"] = True
    _write_json(dest / "summary.json", summary)
    assert _run(["validate-final", str(dest)]).returncode != 0


def test_tampered_success_ratio_fails_validate_final_even_with_checksums(
    tmp_path: Path,
) -> None:
    dest = tmp_path / "final-ratio"
    shutil.copytree(VALID_RUN, dest)
    qualification = json.loads((dest / "qualification.json").read_text(encoding="utf-8"))
    qualification["checks"]["valid_success_ratio"]["actual"] = 0.5
    qualification["checks"]["valid_success_ratio"]["pass"] = False
    qualification["verdict"] = "FAIL"
    _write_json(dest / "qualification.json", qualification)
    summary = json.loads((dest / "summary.json").read_text(encoding="utf-8"))
    summary["overall_valid_success_ratio"] = 0.5
    _write_json(dest / "summary.json", summary)
    _rewrite_checksums(dest)
    assert _run(["validate-final", str(dest)]).returncode != 0


def test_tampered_verdict_pass_flags_fails_validate_final_even_with_checksums(
    tmp_path: Path,
) -> None:
    dest = tmp_path / "final-verdict"
    shutil.copytree(VALID_RUN, dest)
    qualification = json.loads((dest / "qualification.json").read_text(encoding="utf-8"))
    # Keep actuals looking plausible but flip pass flags + verdict to hide a FAIL.
    qualification["checks"]["claims_per_second"]["pass"] = False
    qualification["verdict"] = "PASS"
    _write_json(dest / "qualification.json", qualification)
    _rewrite_checksums(dest)
    assert _run(["validate-final", str(dest)]).returncode != 0


def test_max_fanout_reported_separately_in_summary(raw_bundle: Path) -> None:
    assert _run(["validate-raw", str(raw_bundle)]).returncode == 0
    assert _run(["derive", str(raw_bundle)]).returncode == 0
    summary = json.loads((raw_bundle / "summary.json").read_text(encoding="utf-8"))
    baseline = next(
        row
        for row in summary["operations"]
        if row["operation"] == "complete" and row["scenario"] == "baseline"
    )
    max_fan = next(
        row
        for row in summary["operations"]
        if row["operation"] == "complete" and row["scenario"] == "max_fanout"
    )
    assert baseline["p99_ns"] < max_fan["p99_ns"]
