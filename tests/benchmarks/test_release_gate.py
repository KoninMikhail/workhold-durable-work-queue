"""Release qualification gate (QUAL-03 / QUAL-05 / SDK-02) — Plan 03.9-12."""

from __future__ import annotations

import json
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
REFERENCE = ROOT / "benchmarks" / "results" / "phase-3.9-reference"
CLI = [sys.executable, "-m", "benchmarks.qualification.cli"]
EVAL = [sys.executable, "-m", "benchmarks.qualification.evaluate"]

FINAL_TOP_LEVEL = {
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
}

THRESHOLDS = {
    "claims_per_second_min": 500.0,
    "valid_success_ratio_min": 0.999,
    "p99_hot_ms_max": 100.0,
    "p99_baseline_complete_ms_max": 200.0,
}


def _run(argv: list[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        argv,
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=False,
    )


def _load(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


@pytest.fixture(scope="module")
def final_reference() -> Path:
    """Require the checked-in reference bundle to already be finalized."""
    assert REFERENCE.is_dir(), "phase-3.9-reference missing"
    qual = REFERENCE / "qualification.json"
    assert qual.is_file(), "qualification.json missing — run Plan 12 derive sequence first"
    return REFERENCE


def test_evaluate_module_importable() -> None:
    import benchmarks.qualification.evaluate as evaluate

    assert hasattr(evaluate, "main")
    assert hasattr(evaluate, "evaluate_bundle")


def test_synthetic_reference_verdict_is_ci_synthetic_pass(final_reference: Path) -> None:
    manifest = _load(final_reference / "manifest.json")
    qualification = _load(final_reference / "qualification.json")
    assert manifest["evidence_mode"] == "synthetic"
    assert qualification["evidence_mode"] == "synthetic"
    assert qualification["verdict"] == "CI_SYNTHETIC_PASS"
    assert qualification["production_qualified"] is False
    assert "synthetic" in qualification["verdict_note"].lower()
    assert "not" in qualification["verdict_note"].lower()


def test_final_bundle_has_exactly_twelve_classes(final_reference: Path) -> None:
    top = {p.name for p in final_reference.iterdir() if p.name != ".raw-validation.json"}
    assert top == FINAL_TOP_LEVEL
    manifest = _load(final_reference / "manifest.json")
    assert manifest["bundle_stage"] == "final"
    assert manifest.get("validated_raw_set_digest")


def test_thresholds_meet_adr_019(final_reference: Path) -> None:
    qualification = _load(final_reference / "qualification.json")
    checks = qualification["checks"]
    assert checks["claims_per_second"]["pass"] is True
    assert checks["claims_per_second"]["actual"] >= THRESHOLDS["claims_per_second_min"]
    assert checks["valid_success_ratio"]["pass"] is True
    assert checks["valid_success_ratio"]["actual"] >= THRESHOLDS["valid_success_ratio_min"]
    for key in ("p99_enqueue_ns", "p99_claim_ns", "p99_heartbeat_ns"):
        assert checks[key]["pass"] is True
        assert checks[key]["actual"] <= int(THRESHOLDS["p99_hot_ms_max"] * 1_000_000)
    assert checks["p99_baseline_complete_ns"]["pass"] is True
    assert checks["p99_baseline_complete_ns"]["actual"] <= int(
        THRESHOLDS["p99_baseline_complete_ms_max"] * 1_000_000
    )
    fanout = checks["max_fanout_complete_p99_ns"]
    assert fanout["pass"] is True
    assert fanout["limit"] is None
    note = (fanout.get("note") or "").lower()
    assert "separat" in note or "never" in note or "merged" in note


def test_evaluate_check_requires_allow_synthetic_for_synthetic_bundle(
    final_reference: Path,
) -> None:
    refused = _run([*EVAL, "--input", str(final_reference), "--check"])
    assert refused.returncode != 0
    assert "allow-synthetic" in (refused.stderr + refused.stdout).lower()


def test_evaluate_check_allow_synthetic_accepts_ci_synthetic_pass(
    final_reference: Path,
) -> None:
    allowed = _run(
        [*EVAL, "--input", str(final_reference), "--check", "--allow-synthetic"]
    )
    assert allowed.returncode == 0, allowed.stderr + allowed.stdout
    assert "CI_SYNTHETIC_PASS" in (allowed.stdout + allowed.stderr)


def test_evaluate_rejects_tampered_qualification(tmp_path: Path, final_reference: Path) -> None:
    dest = tmp_path / "tampered"
    shutil.copytree(final_reference, dest)
    qualification = _load(dest / "qualification.json")
    qualification["verdict"] = "PASS"
    qualification["production_qualified"] = True
    (dest / "qualification.json").write_text(
        json.dumps(qualification, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    # Checksums no longer match OR validate-final / evaluate fail-closed.
    result = _run([*EVAL, "--input", str(dest), "--check", "--allow-synthetic"])
    assert result.returncode != 0


def test_live_evidence_still_emits_pass_not_ci_synthetic() -> None:
    from benchmarks.qualification.artifacts import compute_qualification
    from benchmarks.qualification.statistics import LatencySample, Outcome

    samples = [
        LatencySample("enqueue", "baseline", 1_000_000, Outcome.SUCCESS, 0),
        LatencySample("claim", "baseline", 1_000_000, Outcome.SUCCESS, 0),
        LatencySample("heartbeat", "baseline", 1_000_000, Outcome.SUCCESS, 0),
        LatencySample("complete", "baseline", 2_000_000, Outcome.SUCCESS, 0),
        LatencySample("complete", "max_fanout", 50_000_000, Outcome.SUCCESS, 64),
    ]
    # Pad enough samples so success ratio and p99 are defined stably.
    for _ in range(20):
        samples.append(
            LatencySample("claim", "baseline", 1_000_000, Outcome.SUCCESS, 0)
        )
    workload = {
        "measured_seconds": 10.0,
        "successful_claims": 5000,
    }
    *_, live_verdict = compute_qualification(
        samples, workload, evidence_mode="live"
    )
    *_, synth_verdict = compute_qualification(
        samples, workload, evidence_mode="synthetic"
    )
    assert live_verdict == "PASS"
    assert synth_verdict == "CI_SYNTHETIC_PASS"


def test_release_qualification_markdown_exists_and_discloses_synthetic() -> None:
    path = ROOT / "docs" / "05-operations" / "08-release-qualification.md"
    assert path.is_file(), "08-release-qualification.md missing"
    text = path.read_text(encoding="utf-8")
    assert "CI_SYNTHETIC_PASS" in text or "synthetic" in text.lower()
    assert "not a universal SLA" in text.lower() or "not a universal sla" in text.lower()
    assert "500" in text
    assert "99.9" in text or "0.999" in text
    assert "ADR 023" in text or "023" in text
