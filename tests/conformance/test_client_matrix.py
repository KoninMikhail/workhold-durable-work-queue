"""Full client matrix guard for role-split ownership (SDK-02 / SDK-08 / SDK-16).

Supersedes the SDK-05 interim raw+producer-only catalog: every ownership
manifest cell must collect through the operation-coverage gate, and kernel
dual-client scenarios remain collected for raw_http + producer.
"""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

from tests.conformance.clients import (
    CLIENT_KINDS,
    COVERAGE_CLIENT_KINDS,
    COVERAGE_MODES,
    MANIFEST_CLIENT_TO_KIND,
    REQUIRED_KERNEL_SCENARIOS,
)

ROOT = Path(__file__).resolve().parents[2]
OWNERSHIP = ROOT / "packages" / "client-operation-ownership.json"


def test_kernel_client_kinds_remain_raw_and_producer() -> None:
    assert CLIENT_KINDS == ("raw_http", "producer")
    assert set(REQUIRED_KERNEL_SCENARIOS) == {
        "enqueue_idempotency",
        "claim_fencing",
        "stale_heartbeat_complete",
        "uncertain_complete_replay",
        "cancellation",
        "queue_active_paused_draining_gates",
        "structured_failures",
        "authorization",
    }


def test_coverage_kinds_cover_every_manifest_owner() -> None:
    assert COVERAGE_CLIENT_KINDS == (
        "raw_http",
        "producer",
        "consumer",
        "observer",
        "admin",
        "break_glass",
    )
    assert COVERAGE_MODES == ("raw", "sync", "async")
    manifest = json.loads(OWNERSHIP.read_text(encoding="utf-8"))
    owners = {c for entry in manifest["operations"] for c in entry["clients"]}
    assert owners == set(MANIFEST_CLIENT_TO_KIND)
    assert set(MANIFEST_CLIENT_TO_KIND.values()) == set(COVERAGE_CLIENT_KINDS) - {"raw_http"}


def test_kernel_collection_exposes_raw_http_and_producer_for_every_required_scenario() -> None:
    proc = subprocess.run(
        [
            sys.executable,
            "-m",
            "pytest",
            "tests/conformance/test_kernel_protocol.py",
            "--collect-only",
            "-q",
        ],
        check=False,
        capture_output=True,
        text=True,
        cwd=str(ROOT),
    )
    assert proc.returncode == 0, proc.stdout + proc.stderr
    output = proc.stdout
    for scenario in REQUIRED_KERNEL_SCENARIOS:
        assert f"test_{scenario}[raw_http]" in output, output
        assert f"test_{scenario}[producer]" in output, output
        assert f"test_{scenario}[sdk]" not in output, output


def test_operation_coverage_module_is_collectable() -> None:
    proc = subprocess.run(
        [
            sys.executable,
            "-m",
            "pytest",
            "tests/conformance/test_client_operation_coverage.py",
            "--collect-only",
            "-q",
        ],
        check=False,
        capture_output=True,
        text=True,
        cwd=str(ROOT),
    )
    assert proc.returncode == 0, proc.stdout + proc.stderr
    assert "test_manifest_cells_have_raw_sync_async_live_evidence" in proc.stdout
