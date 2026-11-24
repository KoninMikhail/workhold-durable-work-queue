"""Static observer tool registry for the local MCP adapter."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from workhold_admin.models import (
    CURSOR_MAX_LENGTH,
    PAGE_LIMIT_DEFAULT,
    PAGE_LIMIT_MAX,
    PAGE_LIMIT_MIN,
)
from workhold_admin.observer import ObserverClient
from workhold_client_testing import ScriptedSyncTransport, ok
from workhold_mcp.registry import REGISTRY, REGISTRY_BY_NAME

_REGISTRY_PATH = (
    Path(__file__).resolve().parents[3]
    / "packages"
    / "workhold-mcp"
    / "src"
    / "workhold_mcp"
    / "registry.py"
)

_OBSERVER_TOOL_NAMES = (
    "observer_get_capabilities",
    "observer_get_task",
    "observer_list_task_attempts",
    "observer_get_queue",
    "observer_get_stats",
    "observer_get_maintenance_status",
    "observer_list_inspection_tasks",
    "observer_list_inspection_attempts",
    "observer_list_dead_letters",
)

_TOOL_PARAMETERS: dict[str, tuple[str, ...]] = {
    "observer_get_capabilities": (),
    "observer_get_task": ("task_id",),
    "observer_list_task_attempts": ("task_id", "cursor", "limit"),
    "observer_get_queue": ("queue_name",),
    "observer_get_stats": (),
    "observer_get_maintenance_status": (),
    "observer_list_inspection_tasks": ("queue_name", "cursor", "limit"),
    "observer_list_inspection_attempts": (
        "task_id",
        "time_from",
        "time_to",
        "cursor",
        "limit",
    ),
    "observer_list_dead_letters": (
        "queue_name",
        "time_from",
        "time_to",
        "cursor",
        "limit",
    ),
}

_REQUIRED: dict[str, tuple[str, ...]] = {
    "observer_get_capabilities": (),
    "observer_get_task": ("task_id",),
    "observer_list_task_attempts": ("task_id",),
    "observer_get_queue": ("queue_name",),
    "observer_get_stats": (),
    "observer_get_maintenance_status": (),
    "observer_list_inspection_tasks": ("queue_name",),
    "observer_list_inspection_attempts": ("task_id", "time_from", "time_to"),
    "observer_list_dead_letters": ("queue_name", "time_from", "time_to"),
}

_TASK_ID = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
_QUEUE_NAME = "orders"


def _task_body() -> dict[str, Any]:
    return {
        "task_id": _TASK_ID,
        "queue_name": _QUEUE_NAME,
        "producer_id": "producer-1",
        "state": "ready",
        "priority": 0,
        "available_at": "2026-09-19T00:00:00Z",
        "retry_policy_version": 1,
        "created_at": "2026-09-19T00:00:00Z",
        "spawned_task_ids": [],
        "delivery_event_ids": [],
    }


def _queue_body() -> dict[str, Any]:
    return {
        "queue_id": "cccccccc-cccc-4ccc-8ccc-cccccccccccc",
        "name": _QUEUE_NAME,
        "state": "active",
        "config_version": 1,
        "active_policy": {
            "version": 1,
            "enabled": True,
            "max_attempts": 3,
            "backoff_strategy": "fixed",
            "retry_delay_seconds": 30,
            "created_at": "2026-09-19T00:00:00Z",
        },
        "created_at": "2026-09-19T00:00:00Z",
        "updated_at": "2026-09-19T00:00:00Z",
    }


def _observer(
    public_steps: list[Any],
    admin_steps: list[Any],
) -> tuple[ObserverClient, ScriptedSyncTransport, ScriptedSyncTransport]:
    public = ScriptedSyncTransport(public_steps)
    admin = ScriptedSyncTransport(admin_steps)
    client = ObserverClient(
        public,
        bearer_token="observer-secret-token",
        admin_transport=admin,
    )
    return client, public, admin


def test_registry_names_are_the_nine_observer_reads() -> None:
    names = set(REGISTRY_BY_NAME)
    for name in _OBSERVER_TOOL_NAMES:
        assert name in names
    assert all("iter_" not in spec.name for spec in REGISTRY)


def test_unknown_name_is_absent() -> None:
    for name in ("observer_iter_task_attempts", "claim", "break_glass"):
        assert name not in REGISTRY_BY_NAME


def test_schemas_mirror_public_parameters() -> None:
    observer_specs = [
        spec for spec in REGISTRY if spec.name.startswith("observer_")
    ]
    assert {spec.name for spec in observer_specs} == set(_OBSERVER_TOOL_NAMES)
    assert set(_TOOL_PARAMETERS) == set(_OBSERVER_TOOL_NAMES)
    assert set(_REQUIRED) == set(_OBSERVER_TOOL_NAMES)
    for name in _OBSERVER_TOOL_NAMES:
        schema = REGISTRY_BY_NAME[name].input_schema
        assert schema["type"] == "object"
        assert tuple(schema["properties"]) == _TOOL_PARAMETERS[name]
        assert tuple(schema["required"]) == _REQUIRED[name]
        properties = schema["properties"]
        if "limit" in properties:
            limit = properties["limit"]
            assert limit["default"] == PAGE_LIMIT_DEFAULT == 50
            assert limit["minimum"] == PAGE_LIMIT_MIN == 1
            assert limit["maximum"] == PAGE_LIMIT_MAX == 100
        if "cursor" in properties:
            assert properties["cursor"]["maxLength"] == CURSOR_MAX_LENGTH == 512
        for field_name in ("task_id", "queue_name", "time_from", "time_to"):
            if field_name in properties:
                assert properties[field_name]["type"] == "string"


def test_hints_are_read_only() -> None:
    for spec in REGISTRY:
        if not spec.name.startswith("observer_"):
            continue
        assert spec.surface == "observer"
        assert spec.read_only_hint is True
        assert spec.open_world_hint is True
        assert not hasattr(spec, "destructive_hint")
        assert not hasattr(spec, "idempotent_hint")
        assert spec.description.strip()
        assert "Read-only" in spec.description


def test_public_and_admin_dispatch() -> None:
    public_observer, public_transport, admin_transport = _observer(
        [ok(_task_body())],
        [],
    )
    REGISTRY_BY_NAME["observer_get_task"].call(public_observer, _TASK_ID)
    assert len(public_transport.calls) == 1
    assert public_transport.calls[0].method == "GET"
    assert "/v1/tasks/" in public_transport.calls[0].path
    assert admin_transport.calls == []

    admin_observer, public_transport, admin_transport = _observer(
        [],
        [ok(_queue_body())],
    )
    REGISTRY_BY_NAME["observer_get_queue"].call(admin_observer, _QUEUE_NAME)
    assert len(admin_transport.calls) == 1
    assert admin_transport.calls[0].method == "GET"
    assert "/admin/v1/queues/" in admin_transport.calls[0].path
    assert public_transport.calls == []


def test_time_range_is_one_page() -> None:
    observer, public_transport, admin_transport = _observer(
        [],
        [ok({"items": [], "next_cursor": None})],
    )
    REGISTRY_BY_NAME["observer_list_dead_letters"].call(
        observer,
        _QUEUE_NAME,
        time_from="2026-11-23T00:00:00Z",
        time_to="2026-11-23T01:00:00Z",
    )
    assert len(admin_transport.calls) == 1
    assert admin_transport.calls[0].method == "GET"
    assert public_transport.calls == []

    observer, public_transport, admin_transport = _observer(
        [],
        [ok({"items": [], "next_cursor": None})],
    )
    with pytest.raises(ValueError):
        REGISTRY_BY_NAME["observer_list_dead_letters"].call(
            observer,
            _QUEUE_NAME,
            time_from="2026-11-23T00:00:00",
            time_to="2026-11-23T01:00:00Z",
        )
    assert admin_transport.calls == []
    assert public_transport.calls == []


def test_source_has_no_dynamic_dispatch() -> None:
    source = _REGISTRY_PATH.read_text(encoding="utf-8")
    assert "getattr" not in source
