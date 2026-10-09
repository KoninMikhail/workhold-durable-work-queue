"""Contract tests for the role-split client operation ownership manifest.

SDK-08: every authenticated OpenAPI operationId maps to approved client
surfaces; the checker fails on missing, stale, unapproved-duplicate, unknown
client, or role/security mismatches.
"""

from __future__ import annotations

import copy
import json
import sys
from pathlib import Path
from typing import Any

import pytest

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from tools.check_client_operation_ownership import (  # noqa: E402
    APPROVED_MULTI_CLIENT_OPERATIONS,
    KNOWN_CLIENT_CLASSES,
    check_ownership,
    load_manifest,
    load_openapi_authenticated_operations,
    main,
)
MANIFEST_PATH = ROOT / "packages" / "client-operation-ownership.json"
OPENAPI_PATH = ROOT / "openapi" / "queue.openapi.json"

OBSERVER_MUTATIONS = frozenset(
    {
        "enqueueTask",
        "cancelTask",
        "claimTasks",
        "completeClaim",
        "createQueue",
        "forceLeaseExpiry",
        "runMaintenance",
    }
)


@pytest.fixture(scope="module")
def openapi_ops() -> dict[str, dict[str, Any]]:
    return load_openapi_authenticated_operations(OPENAPI_PATH)


@pytest.fixture(scope="module")
def manifest() -> dict[str, Any]:
    return load_manifest(MANIFEST_PATH)


def test_repo_manifest_passes_checker(openapi_ops: dict[str, dict[str, Any]], manifest: dict[str, Any]) -> None:
    errors = check_ownership(openapi_ops, manifest)
    assert errors == [], errors


def test_manifest_covers_every_authenticated_operation(
    openapi_ops: dict[str, dict[str, Any]],
    manifest: dict[str, Any],
) -> None:
    listed = {entry["operationId"] for entry in manifest["operations"]}
    assert listed == set(openapi_ops)


def test_break_glass_ops_map_only_to_break_glass_client(manifest: dict[str, Any]) -> None:
    break_glass_ops = {
        "forceLeaseExpiry",
        "forceDeliveryReclaim",
        "forceDeliveryDeadLetter",
        "reconcileCounters",
        "raiseReplayLimit",
        "dropExpiredPartition",
        "repairRegistryEntry",
    }
    by_id = {entry["operationId"]: entry for entry in manifest["operations"]}
    for op_id in break_glass_ops:
        entry = by_id[op_id]
        assert entry["clients"] == ["BreakGlassClient"]
        assert entry["credential_roles"] == ["BREAK_GLASS"]


def test_observer_entries_contain_no_mutations(
    openapi_ops: dict[str, dict[str, Any]],
    manifest: dict[str, Any],
) -> None:
    for entry in manifest["operations"]:
        if "ObserverClient" not in entry["clients"]:
            continue
        op_id = entry["operationId"]
        assert op_id not in OBSERVER_MUTATIONS
        assert openapi_ops[op_id]["method"] == "GET"
        if entry["clients"] == ["ObserverClient"]:
            assert entry["credential_roles"] == ["OBSERVER"]


def test_approved_multi_client_allowlist_matches_context(manifest: dict[str, Any]) -> None:
    assert set(manifest["approved_multi_client_operations"]) == APPROVED_MULTI_CLIENT_OPERATIONS
    multi = [
        entry["operationId"]
        for entry in manifest["operations"]
        if len(entry["clients"]) > 1
    ]
    assert set(multi) <= APPROVED_MULTI_CLIENT_OPERATIONS


def test_known_client_classes_are_closed(manifest: dict[str, Any]) -> None:
    used: set[str] = set()
    for entry in manifest["operations"]:
        used.update(entry["clients"])
    assert used <= KNOWN_CLIENT_CLASSES
    assert used == KNOWN_CLIENT_CLASSES


def _clone_manifest(manifest: dict[str, Any]) -> dict[str, Any]:
    return copy.deepcopy(manifest)


def test_checker_fails_missing_operation(
    openapi_ops: dict[str, dict[str, Any]],
    manifest: dict[str, Any],
) -> None:
    broken = _clone_manifest(manifest)
    broken["operations"] = [
        entry for entry in broken["operations"] if entry["operationId"] != "enqueueTask"
    ]
    errors = check_ownership(openapi_ops, broken)
    assert any("missing" in err.lower() and "enqueueTask" in err for err in errors)


def test_checker_fails_stale_operation(
    openapi_ops: dict[str, dict[str, Any]],
    manifest: dict[str, Any],
) -> None:
    broken = _clone_manifest(manifest)
    broken["operations"].append(
        {
            "operationId": "notInOpenApiAnymore",
            "clients": ["ProducerClient"],
            "credential_roles": ["PRODUCER"],
        }
    )
    errors = check_ownership(openapi_ops, broken)
    assert any("stale" in err.lower() and "notInOpenApiAnymore" in err for err in errors)


def test_checker_fails_unapproved_duplicate(
    openapi_ops: dict[str, dict[str, Any]],
    manifest: dict[str, Any],
) -> None:
    broken = _clone_manifest(manifest)
    for entry in broken["operations"]:
        if entry["operationId"] == "enqueueTask":
            entry["clients"] = ["ProducerClient", "AdminClient"]
            entry["credential_roles"] = ["PRODUCER", "ADMIN"]
            break
    errors = check_ownership(openapi_ops, broken)
    assert any(
        "unapproved" in err.lower() and "enqueueTask" in err for err in errors
    ) or any("duplicate" in err.lower() and "enqueueTask" in err for err in errors)


def test_checker_fails_unknown_client_class(
    openapi_ops: dict[str, dict[str, Any]],
    manifest: dict[str, Any],
) -> None:
    broken = _clone_manifest(manifest)
    for entry in broken["operations"]:
        if entry["operationId"] == "enqueueTask":
            entry["clients"] = ["LegacyFacadeClient"]
            break
    errors = check_ownership(openapi_ops, broken)
    assert any("unknown client" in err.lower() and "LegacyFacadeClient" in err for err in errors)


def test_checker_fails_role_security_mismatch(
    openapi_ops: dict[str, dict[str, Any]],
    manifest: dict[str, Any],
) -> None:
    broken = _clone_manifest(manifest)
    for entry in broken["operations"]:
        if entry["operationId"] == "listTaskAttempts":
            entry["credential_roles"] = ["OBSERVER", "PRODUCER"]
            entry["clients"] = ["ObserverClient", "ProducerClient"]
            break
    errors = check_ownership(openapi_ops, broken)
    assert any("listTaskAttempts" in err for err in errors)
    assert any(
        "mismatch" in err.lower() or "role" in err.lower() or "security" in err.lower()
        for err in errors
    )


def test_cli_main_exits_zero_on_repo_files(capsys: pytest.CaptureFixture[str]) -> None:
    assert main([]) == 0
    captured = capsys.readouterr()
    assert "OK" in captured.out or captured.out == ""


def test_cli_main_exits_nonzero_on_broken_fixture(
    tmp_path: Path,
    openapi_ops: dict[str, dict[str, Any]],
    manifest: dict[str, Any],
) -> None:
    broken = _clone_manifest(manifest)
    broken["operations"] = broken["operations"][:1]
    manifest_path = tmp_path / "broken.json"
    manifest_path.write_text(json.dumps(broken), encoding="utf-8")
    openapi_path = tmp_path / "openapi.json"
    # Minimal OpenAPI reconstructed from loaded ops for the CLI path.
    paths: dict[str, Any] = {}
    for op_id, meta in openapi_ops.items():
        path = meta["path"]
        method = meta["method"].lower()
        paths.setdefault(path, {})[method] = {
            "operationId": op_id,
            "security": meta["security"],
        }
    openapi_path.write_text(
        json.dumps({"openapi": "3.1.0", "paths": paths}),
        encoding="utf-8",
    )
    code = main(["--manifest", str(manifest_path), "--openapi", str(openapi_path)])
    assert code != 0
