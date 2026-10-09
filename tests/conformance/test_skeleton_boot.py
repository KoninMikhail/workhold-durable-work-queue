"""End-to-end proof of the Phase 3.1 bootable conformance skeleton."""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

from tests.conformance.cases import (
    COMPLETE_OPERATION_ID,
    RESERVED_COMPLETE_FIELD,
    build_phase_31_skeleton_cases,
)
from tests.conformance.harness import ConformanceHarness, OpenApiCatalogError

ROOT = Path(__file__).resolve().parents[2]
RUNNER = ROOT / "tools" / "run_conformance.py"
OPENAPI_PATH = ROOT / "openapi" / "queue.openapi.json"


def _docker_available() -> bool:
    try:
        docker = subprocess.run(
            ["docker", "version", "--format", "{{.Server.Version}}"],
            capture_output=True,
            text=True,
            timeout=15,
            check=False,
        )
        compose = subprocess.run(
            ["docker", "compose", "version"],
            capture_output=True,
            text=True,
            timeout=15,
            check=False,
        )
    except (FileNotFoundError, subprocess.TimeoutExpired, OSError):
        return False
    return docker.returncode == 0 and compose.returncode == 0


def test_complete_reserved_events_rejected_by_closed_fixture() -> None:
    """Reserved complete `events` field must not pass closed request validation."""
    harness = ConformanceHarness(
        openapi_path=OPENAPI_PATH,
        base_url="http://127.0.0.1:9",
    )
    with pytest.raises(OpenApiCatalogError):
        harness.validate_request_fixture(
            COMPLETE_OPERATION_ID,
            {
                "generation": 1,
                "spawn": [],
                RESERVED_COMPLETE_FIELD: [],
            },
        )


def test_phase_31_case_catalog_covers_required_operations() -> None:
    cases = build_phase_31_skeleton_cases()
    operation_ids = [case.operation_id for case in cases]
    assert operation_ids == [
        "getCapabilities",
        "enqueueTask",
        "claimTasks",
        "heartbeatClaim",
        "completeClaim",
        "listQueues",
    ]
    complete = next(c for c in cases if c.operation_id == COMPLETE_OPERATION_ID)
    assert isinstance(complete.body, dict)
    assert RESERVED_COMPLETE_FIELD not in complete.body


def test_skeleton_boot_end_to_end(tmp_path: Path) -> None:
    """One command boots Postgres 16, migrates, runs black-box cases, and cleans up."""
    if not _docker_available():
        pytest.fail(
            "Docker and Docker Compose are required for "
            "tests/conformance/test_skeleton_boot.py (preflight failure)"
        )

    assert RUNNER.is_file(), f"missing runner: {RUNNER}"
    out_dir = tmp_path / "conformance-out"
    env = os.environ.copy()
    env["PYTHONPATH"] = str(ROOT)

    skeleton = subprocess.run(
        [
            sys.executable,
            str(RUNNER),
            "--skeleton-assert",
            "--output-dir",
            str(out_dir),
        ],
        cwd=str(ROOT),
        capture_output=True,
        text=True,
        env=env,
        check=False,
    )
    combined = f"{skeleton.stdout}\n{skeleton.stderr}"
    assert skeleton.returncode == 0, combined
    assert "DATABASE_URL" not in combined
    assert "postgresql+psycopg://queue:queue@" not in combined
    assert "Authorization" not in combined
    assert "X-Queue-Claim-Token" not in combined

    report_path = out_dir / "conformance-report.json"
    assert report_path.is_file()
    report = json.loads(report_path.read_text(encoding="utf-8"))
    report_text = json.dumps(report)

    assert report["boot"]["postgres_major"] == 16
    assert report["boot"]["migration_ok"] is True
    assert report["boot"]["alembic_revision"] == "039_apply_qualified_storage_layout"
    assert report["expected_from_openapi"]["protocol_version"] == "1.0"
    assert report["expected_from_openapi"]["schema_revision"] == "0001"
    assert report["expected_from_openapi"]["service_capabilities_observed"] is False
    assert report["case_counts"]["passed"] == 0
    assert report["case_counts"]["skipped"] == 0
    assert report["case_counts"]["unsupported_operation"] == report["case_counts"]["total"]
    assert report["case_counts"]["total"] == 6
    assert report["raw_conformance"]["overall"] == "FAILED"
    assert report["raw_conformance"]["exit_code"] != 0
    assert report["skeleton_assert"]["passed"] is True

    assert "DATABASE_URL" not in report_text
    assert "queue:queue@" not in report_text
    assert "claim-secret" not in report_text
    assert "Bearer secret-token" not in report_text

    normal = subprocess.run(
        [
            sys.executable,
            str(RUNNER),
            "--output-dir",
            str(tmp_path / "conformance-out-normal"),
        ],
        cwd=str(ROOT),
        capture_output=True,
        text=True,
        env=env,
        check=False,
    )
    assert normal.returncode != 0
    assert "DATABASE_URL" not in f"{normal.stdout}\n{normal.stderr}"
