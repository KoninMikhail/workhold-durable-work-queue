"""Drift checks for the checked-in MCP tool manifest.

The catalog is the public sync surface of ObserverClient, ProducerClient, and
AdminClient. Preset membership is not cumulative. Worker claims, break-glass
operations, and pagination iterators stay out.
"""

from __future__ import annotations

import inspect
import json
from pathlib import Path
from typing import Any

import pytest
from workhold_admin.admin import AdminClient
from workhold_admin.observer import ObserverClient
from workhold_producer.client import ProducerClient

ROOT = Path(__file__).resolve().parents[3]
OWNERSHIP_PATH = ROOT / "packages" / "client-operation-ownership.json"

_CLIENTS = {
    "ObserverClient": ObserverClient,
    "ProducerClient": ProducerClient,
    "AdminClient": AdminClient,
}

_PRESETS_BY_ROLE = {
    "observer": ("observer", "routine"),
    "producer": ("producer", "routine"),
    "admin": ("operator", "routine"),
}

_OBSERVER = (
    "observer_get_capabilities",
    "observer_get_maintenance_status",
    "observer_get_queue",
    "observer_get_stats",
    "observer_get_task",
    "observer_list_dead_letters",
    "observer_list_inspection_attempts",
    "observer_list_inspection_tasks",
    "observer_list_task_attempts",
)

_PRODUCER = (
    "producer_cancel_task",
    "producer_enqueue",
    "producer_get_capabilities",
    "producer_inspect_task",
    "producer_resolve_submission",
)

_OPERATOR = (
    "admin_activate_queue_policy",
    "admin_create_queue",
    "admin_create_queue_policy",
    "admin_execute_bulk_cancel",
    "admin_execute_bulk_replay",
    "admin_get_capabilities",
    "admin_get_maintenance_status",
    "admin_get_queue",
    "admin_get_stats",
    "admin_list_admin_audit",
    "admin_list_dead_letters",
    "admin_list_inspection_attempts",
    "admin_list_inspection_tasks",
    "admin_list_queues",
    "admin_preview_bulk_cancel",
    "admin_preview_bulk_replay",
    "admin_replay_dead_letter",
    "admin_run_maintenance",
    "admin_set_queue_state",
)

_EXCLUDED_OPERATION_IDS = (
    "acknowledgeClaimCancellation",
    "claimTasks",
    "completeClaim",
    "failClaim",
    "heartbeatClaim",
    "dropExpiredPartition",
    "forceDeliveryDeadLetter",
    "forceDeliveryReclaim",
    "forceLeaseExpiry",
    "raiseReplayLimit",
    "reconcileCounters",
    "repairRegistryEntry",
)

_EXCLUDED_METHODS = (
    "ack_cancel",
    "claim",
    "complete",
    "fail",
    "heartbeat",
    "drop_expired_partition",
    "force_delivery_dead_letter",
    "force_delivery_reclaim",
    "force_lease_expiry",
    "raise_replay_limit",
    "reconcile_counters",
    "repair_registry_entry",
)

_PAGINATION_NAMES = (
    "iter_task_attempt_pages",
    "iter_task_attempts",
    "iter_inspection_task_pages",
    "iter_inspection_tasks",
    "iter_inspection_attempt_pages",
    "iter_inspection_attempts",
    "iter_dead_letter_pages",
    "iter_dead_letters",
    "iter_queue_pages",
    "iter_queues",
    "iter_admin_audit_pages",
    "iter_admin_audit",
)

_ALIAS_PREFIXES = ("consumer_", "worker_", "break_glass_", "breakglass_")


def _public_sync_methods(cls: type) -> set[str]:
    return {
        name
        for name, member in inspect.getmembers(cls, inspect.isfunction)
        if not name.startswith("_") and inspect.isfunction(member)
    }


def _sync_parts(sync: str) -> tuple[str, ...]:
    return tuple(part.strip() for part in sync.split("->"))


def _operation_id_for(ownership: dict[str, Any], client: str, method: str) -> str:
    needle = f"{client}.{method}"
    found: list[str] = []
    for entry in ownership["operations"]:
        for surface in entry["sync_async_surfaces"]:
            if needle in _sync_parts(surface["sync"]):
                found.append(entry["operationId"])
    assert len(found) == 1, (client, method, found)
    return found[0]


def _tokens(rows: tuple[Any, ...]) -> set[str]:
    tokens: set[str] = set()
    for row in rows:
        tokens.add(row.name)
        tokens.add(row.role)
        tokens.add(row.client)
        tokens.add(row.method)
        tokens.add(row.operation_id)
        tokens.update(row.presets)
    return tokens


@pytest.fixture(scope="module")
def ownership() -> dict[str, Any]:
    loaded: dict[str, Any] = json.loads(OWNERSHIP_PATH.read_text(encoding="utf-8"))
    return loaded


@pytest.fixture(scope="module")
def catalog() -> tuple[tuple[Any, ...], Any, Any]:
    from workhold_mcp.registry import (
        load_manifest,
        omitted_policy_names,
        preset_names,
    )

    return tuple(load_manifest()), omitted_policy_names, preset_names


def test_manifest_matches_public_sync_methods(
    catalog: tuple[tuple[Any, ...], Any, Any],
) -> None:
    rows, _omitted, _preset_names = catalog
    expected = {
        (client_name, method)
        for client_name, cls in _CLIENTS.items()
        for method in _public_sync_methods(cls)
    }
    actual = {(row.client, row.method) for row in rows}
    assert actual == expected
    assert len(rows) == 33
    assert sum(1 for row in rows if row.client == "ObserverClient") == 9
    assert sum(1 for row in rows if row.client == "ProducerClient") == 5
    assert sum(1 for row in rows if row.client == "AdminClient") == 19


def test_names_are_role_prefixed(catalog: tuple[tuple[Any, ...], Any, Any]) -> None:
    rows, _omitted, _preset_names = catalog
    names = [row.name for row in rows]
    assert len(names) == len(set(names))
    for row in rows:
        assert row.role in _PRESETS_BY_ROLE
        assert row.name == f"{row.role}_{row.method}"
    assert "observer_get_task" in names
    assert "producer_inspect_task" in names


def test_operation_ids_match_ownership(
    catalog: tuple[tuple[Any, ...], Any, Any],
    ownership: dict[str, Any],
) -> None:
    rows, _omitted, _preset_names = catalog
    for row in rows:
        assert row.operation_id == _operation_id_for(ownership, row.client, row.method)
    assert all(row.client != "ConsumerClient" for row in rows)
    operation_ids = [row.operation_id for row in rows]
    assert operation_ids.count("getCapabilities") == 3
    assert operation_ids.count("getTask") == 2
    assert _operation_id_for(ownership, "ConsumerClient", "get_capabilities") == (
        "getCapabilities"
    )


def test_presets_match_role_sets(catalog: tuple[tuple[Any, ...], Any, Any]) -> None:
    rows, _omitted, preset_names = catalog
    assert preset_names("observer") == _OBSERVER
    assert preset_names("producer") == _PRODUCER
    assert preset_names("operator") == _OPERATOR
    routine = preset_names("routine")
    union = tuple(sorted(set(_OBSERVER) | set(_PRODUCER) | set(_OPERATOR)))
    assert routine == union
    assert routine[0] == "admin_activate_queue_policy"
    assert routine[-1] == "producer_resolve_submission"
    assert set(preset_names("operator")).isdisjoint(preset_names("producer"))
    assert set(preset_names("operator")).isdisjoint(preset_names("observer"))
    assert set(preset_names("producer")).isdisjoint(preset_names("observer"))
    for row in rows:
        assert tuple(row.presets) == _PRESETS_BY_ROLE[row.role]


def test_default_membership_is_observer(
    catalog: tuple[tuple[Any, ...], Any, Any],
) -> None:
    _rows, omitted_policy_names, preset_names = catalog
    assert omitted_policy_names() == preset_names("observer") == _OBSERVER
    assert omitted_policy_names() != preset_names("routine")


def test_excluded_operations_are_absent(
    catalog: tuple[tuple[Any, ...], Any, Any],
) -> None:
    rows, _omitted, preset_names = catalog
    present = _tokens(rows)
    for preset_id in ("observer", "producer", "operator", "routine"):
        present.update(preset_names(preset_id))
    banned = set(_EXCLUDED_OPERATION_IDS)
    banned.update(_EXCLUDED_METHODS)
    banned.update(_PAGINATION_NAMES)
    for method in _EXCLUDED_METHODS:
        for prefix in _ALIAS_PREFIXES:
            banned.add(f"{prefix}{method}")
    banned.update(
        {
            "BreakGlassClient",
            "ConsumerClient",
            "consumer_get_capabilities",
        }
    )
    assert present.isdisjoint(banned)
    for row in rows:
        assert not row.name.startswith(_ALIAS_PREFIXES)
        assert not row.name.startswith("iter_")
        assert not row.name.startswith("aiter_")
