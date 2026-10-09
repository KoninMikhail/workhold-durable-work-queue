"""Release-gate raw evidence contract (QUAL-03 / SDK-02 / Plan 03.9-11)."""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
RELEASE_GATE = (
    ROOT / "benchmarks" / "qualification" / "workloads" / "release-gate.yaml"
)
REFERENCE = ROOT / "benchmarks" / "results" / "phase-3.9-reference"
CLI = [sys.executable, "-m", "benchmarks.qualification.cli"]

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

FORBIDDEN_TOP_LEVEL = (
    "summary.json",
    "qualification.json",
    "report.md",
    "SHA256SUMS",
    ".immutable",
    "PASS",
)

EXPECTED_MIX = {
    "claim": 0.35,
    "enqueue": 0.20,
    "heartbeat": 0.20,
    "complete_fanout_0": 0.10,
    "complete_fanout_8": 0.10,
    "fail_cancel_inspect": 0.05,
}


def _run_validate_raw() -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [*CLI, "validate-raw", str(REFERENCE)],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=False,
    )


def test_release_gate_yaml_declares_phase_39_profile_and_envelope() -> None:
    from benchmarks.qualification.load import load_release_gate

    gate = load_release_gate(RELEASE_GATE)
    assert gate["profile"] == "phase-3.9-linux-x86_64-v1"
    assert gate["named_queues"] == 100
    assert gate["claimers"] == 32
    assert gate["payload_bytes_baseline"] == 1024
    assert gate["warmup_seconds"] == 120
    assert gate["target_claims_per_second"] == 500
    assert gate["terminal_lifecycles"] == 1_000_000
    assert gate["min_measured_seconds"] == 300
    mix = gate["operation_mix"]
    assert mix == EXPECTED_MIX
    assert abs(sum(mix.values()) - 1.0) < 1e-9
    sample = gate["fanout_64_sample"]
    assert sample["completes"] == 10_000
    assert sample["fan_out"] == 64


def test_reference_bundle_is_raw_only_with_eight_classes() -> None:
    assert REFERENCE.is_dir(), "phase-3.9-reference raw evidence directory missing"
    for relative in RAW_CLASSES:
        assert (REFERENCE / relative).is_file(), f"missing raw class {relative}"
    for name in FORBIDDEN_TOP_LEVEL:
        assert not (REFERENCE / name).exists(), f"forbidden artifact present: {name}"

    manifest = json.loads((REFERENCE / "manifest.json").read_text(encoding="utf-8"))
    assert manifest["bundle_stage"] == "raw"
    assert "validated_raw_set_digest" not in manifest
    assert tuple(manifest["artifact_classes"]["raw"]) == RAW_CLASSES
    assert sorted(manifest["conformance"]["variants"]) == ["raw_http", "sdk"]


def test_reference_bundle_declares_exact_terminal_envelope() -> None:
    workload = json.loads((REFERENCE / "workload.json").read_text(encoding="utf-8"))
    assert workload["terminal_lifecycles"] == 1_000_000
    assert float(workload["measured_seconds"]) >= 300.0
    assert workload["profile"] == "phase-3.9-linux-x86_64-v1"
    assert workload["named_queues"] == 100
    assert workload["claimers"] == 32
    assert workload["payload_bytes_baseline"] == 1024
    assert workload["operation_mix"] == EXPECTED_MIX
    assert workload["fanout_64_sample"]["completes"] == 10_000
    assert workload["fanout_64_sample"]["fan_out"] == 64


def test_reference_bindings_cross_link_identity() -> None:
    manifest = json.loads((REFERENCE / "manifest.json").read_text(encoding="utf-8"))
    environment = json.loads(
        (REFERENCE / "environment.json").read_text(encoding="utf-8")
    )
    workload = json.loads((REFERENCE / "workload.json").read_text(encoding="utf-8"))
    before = json.loads(
        (REFERENCE / "postgres-before.json").read_text(encoding="utf-8")
    )
    after = json.loads((REFERENCE / "postgres-after.json").read_text(encoding="utf-8"))
    xml = (REFERENCE / "conformance.xml").read_text(encoding="utf-8")

    assert manifest["git_sha"]
    assert manifest["image_digests"]["queue"]
    assert manifest["image_digests"]["postgres"]
    assert manifest["schema_revision"] == "039_apply_qualified_storage_layout"
    assert manifest["catalog_signature"]
    assert manifest["environment_hash"] == environment["environment_hash"]
    assert manifest["workload_hash"] == workload["workload_hash"]
    assert before["environment_hash"] == manifest["environment_hash"]
    assert after["environment_hash"] == manifest["environment_hash"]
    assert environment["git_sha"] == manifest["git_sha"]
    assert environment["image_digests"] == manifest["image_digests"]

    for key, value in (
        ("run_id", manifest["run_id"]),
        ("environment_hash", manifest["environment_hash"]),
        ("workload_hash", manifest["workload_hash"]),
        ("git_sha", manifest["git_sha"]),
        ("schema_revision", manifest["schema_revision"]),
        ("catalog_signature", manifest["catalog_signature"]),
    ):
        assert f'name="{key}"' in xml
        assert f'value="{value}"' in xml
    assert 'value="raw_http"' in xml
    assert 'value="sdk"' in xml
    assert 'failures="0"' in xml
    assert 'errors="0"' in xml
    assert 'skipped="0"' in xml


def test_reference_bundle_passes_validate_raw_only() -> None:
    result = _run_validate_raw()
    assert result.returncode == 0, result.stderr + result.stdout
    assert "validate-raw OK" in result.stdout
    assert not (REFERENCE / "summary.json").exists()
    assert not (REFERENCE / "qualification.json").exists()
    assert not (REFERENCE / "SHA256SUMS").exists()
    manifest = json.loads((REFERENCE / "manifest.json").read_text(encoding="utf-8"))
    assert manifest["bundle_stage"] == "raw"


def test_reference_evidence_mode_is_disclosed() -> None:
    """CI may use synthetic/compressed evidence; mode must be explicit."""
    import gzip

    manifest = json.loads((REFERENCE / "manifest.json").read_text(encoding="utf-8"))
    workload = json.loads((REFERENCE / "workload.json").read_text(encoding="utf-8"))
    assert manifest["evidence_mode"] in {"synthetic", "compressed", "live"}
    assert workload["evidence_mode"] == manifest["evidence_mode"]
    if workload["evidence_mode"] != "live":
        assert "evidence_note" in workload
        assert workload["evidence_mode"] == "synthetic"

    sample_count = sum(
        1
        for line in gzip.open(REFERENCE / "latencies.jsonl.gz", "rt", encoding="utf-8")
        if line.strip()
    )
    assert manifest["latency_sample_count"] == sample_count
    assert workload["latency_sample_count"] == sample_count
    assert workload["terminal_lifecycles"] == 1_000_000
    assert sample_count != workload["terminal_lifecycles"], (
        "synthetic compressed latency rows must not silently equal declared terminals"
    )


def test_validate_raw_rejects_missing_manifest_evidence_mode(
    tmp_path: Path,
) -> None:
    """Fail closed: evidence_mode is required on the raw manifest."""
    import shutil

    from benchmarks.qualification.artifacts import ArtifactError, validate_raw

    dest = tmp_path / "no-mode"
    shutil.copytree(REFERENCE, dest)
    stamp = dest / ".raw-validation.json"
    if stamp.exists():
        stamp.unlink()
    manifest = json.loads((dest / "manifest.json").read_text(encoding="utf-8"))
    manifest.pop("evidence_mode", None)
    (dest / "manifest.json").write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    with pytest.raises(ArtifactError, match="evidence_mode"):
        validate_raw(dest)


def test_assert_evidence_mode_for_qualification_fail_closed() -> None:
    from benchmarks.qualification.artifacts import (
        ArtifactError,
        assert_evidence_mode_for_qualification,
    )

    assert_evidence_mode_for_qualification("live")
    assert_evidence_mode_for_qualification("synthetic", allow_synthetic=True)
    with pytest.raises(ArtifactError, match="allow-synthetic|allow_synthetic"):
        assert_evidence_mode_for_qualification("synthetic", allow_synthetic=False)
    with pytest.raises(ArtifactError, match="allow-synthetic|allow_synthetic"):
        assert_evidence_mode_for_qualification("compressed")
