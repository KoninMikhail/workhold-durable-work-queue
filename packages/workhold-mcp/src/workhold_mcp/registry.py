"""MCP tool catalog and the static observer callables.

``load_manifest`` reads the checked-in role-prefixed catalog. ``REGISTRY``
still exposes only the observer tools that have a static callable.
"""

from __future__ import annotations

import json
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

from workhold_admin.models import (
    CURSOR_MAX_LENGTH,
    PAGE_LIMIT_DEFAULT,
    PAGE_LIMIT_MAX,
    PAGE_LIMIT_MIN,
)
from workhold_admin.observer import ObserverClient


@dataclass(frozen=True, slots=True)
class ToolSpec:
    """One observer tool: name, schema, and an explicit client call."""

    name: str
    description: str
    input_schema: Mapping[str, Any]
    call: Callable[..., Any]
    surface: str
    read_only_hint: bool
    open_world_hint: bool


_MANIFEST_PATH = Path(__file__).with_name("data") / "tool-manifest.json"

_PRESET_IDS = ("observer", "producer", "operator", "routine")

_ROLE_CATALOG = {
    "observer": ("ObserverClient", ("observer", "routine")),
    "producer": ("ProducerClient", ("producer", "routine")),
    "admin": ("AdminClient", ("operator", "routine")),
}


@dataclass(frozen=True, slots=True)
class ManifestTool:
    """One catalog row: role-prefixed name, client method, and preset membership."""

    name: str
    role: str
    client: str
    method: str
    operation_id: str
    presets: tuple[str, ...]


def _manifest_string(raw: object, field: str) -> str:
    if not isinstance(raw, str) or not raw:
        raise ValueError(f"tool manifest {field} is missing")
    return raw


def load_manifest() -> tuple[ManifestTool, ...]:
    """Return the checked-in catalog. A duplicate tool name fails closed."""
    document = json.loads(_MANIFEST_PATH.read_text(encoding="utf-8"))
    if document.get("schema_version") != 1:
        raise ValueError("unsupported tool manifest schema")
    raw_tools = document["tools"]
    if not isinstance(raw_tools, list):
        raise ValueError("tool manifest tools must be a list")
    seen: set[str] = set()
    seen_methods: set[tuple[str, str]] = set()
    rows: list[ManifestTool] = []
    for raw in raw_tools:
        if not isinstance(raw, dict):
            raise ValueError("tool manifest row must be an object")
        name = _manifest_string(raw.get("name"), "name")
        if name in seen:
            raise ValueError(f"duplicate tool name: {name}")
        seen.add(name)
        role = _manifest_string(raw.get("role"), "role")
        client = _manifest_string(raw.get("client"), "client")
        method = _manifest_string(raw.get("method"), "method")
        operation_id = _manifest_string(raw.get("operation_id"), "operation_id")
        raw_presets = raw.get("presets")
        if not isinstance(raw_presets, list) or not all(
            isinstance(item, str) for item in raw_presets
        ):
            raise ValueError(f"tool presets are invalid: {name}")
        presets = tuple(raw_presets)
        expected = _ROLE_CATALOG.get(role)
        if expected is None:
            raise ValueError(f"unknown tool role: {role}")
        expected_client, expected_presets = expected
        if client != expected_client:
            raise ValueError(f"tool client does not match role: {name}")
        if name != f"{role}_{method}":
            raise ValueError(f"tool name does not match role and method: {name}")
        if presets != expected_presets:
            raise ValueError(f"tool presets do not match role: {name}")
        method_key = (client, method)
        if method_key in seen_methods:
            raise ValueError(f"duplicate client method: {client}.{method}")
        seen_methods.add(method_key)
        rows.append(
            ManifestTool(
                name=name,
                role=role,
                client=client,
                method=method,
                operation_id=operation_id,
                presets=presets,
            )
        )
    return tuple(rows)


def preset_names(preset_id: str) -> tuple[str, ...]:
    """Sorted tool names that belong to one preset."""
    if preset_id not in _PRESET_IDS:
        raise ValueError(f"unknown preset: {preset_id}")
    names = [row.name for row in load_manifest() if preset_id in row.presets]
    return tuple(sorted(names))


def omitted_policy_names() -> tuple[str, ...]:
    """Tool names used when no policy is selected."""
    return preset_names("observer")


def _string_schema() -> dict[str, str]:
    return {"type": "string"}


def _cursor_schema() -> dict[str, Any]:
    return {
        "type": "string",
        "maxLength": CURSOR_MAX_LENGTH,
        "default": None,
    }


def _limit_schema() -> dict[str, Any]:
    return {
        "type": "integer",
        "default": PAGE_LIMIT_DEFAULT,
        "minimum": PAGE_LIMIT_MIN,
        "maximum": PAGE_LIMIT_MAX,
    }


def _object_schema(
    properties: dict[str, Any],
    required: tuple[str, ...],
) -> dict[str, Any]:
    return {
        "type": "object",
        "properties": properties,
        "required": list(required),
        "additionalProperties": False,
    }


def _require_aware(value: str) -> datetime:
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError("datetime must be timezone-aware")
    return parsed


def _get_capabilities(observer: ObserverClient) -> Any:
    return observer.get_capabilities()


def _get_task(observer: ObserverClient, task_id: str) -> Any:
    return observer.get_task(task_id)


def _list_task_attempts(
    observer: ObserverClient,
    task_id: str,
    *,
    cursor: str | None = None,
    limit: int = PAGE_LIMIT_DEFAULT,
) -> Any:
    return observer.list_task_attempts(task_id, cursor=cursor, limit=limit)


def _get_queue(observer: ObserverClient, queue_name: str) -> Any:
    return observer.get_queue(queue_name)


def _get_stats(observer: ObserverClient) -> Any:
    return observer.get_stats()


def _get_maintenance_status(observer: ObserverClient) -> Any:
    return observer.get_maintenance_status()


def _list_inspection_tasks(
    observer: ObserverClient,
    queue_name: str,
    *,
    cursor: str | None = None,
    limit: int = PAGE_LIMIT_DEFAULT,
) -> Any:
    return observer.list_inspection_tasks(queue_name, cursor=cursor, limit=limit)


def _list_inspection_attempts(
    observer: ObserverClient,
    task_id: str,
    *,
    time_from: str,
    time_to: str,
    cursor: str | None = None,
    limit: int = PAGE_LIMIT_DEFAULT,
) -> Any:
    return observer.list_inspection_attempts(
        task_id,
        time_from=_require_aware(time_from),
        time_to=_require_aware(time_to),
        cursor=cursor,
        limit=limit,
    )


def _list_dead_letters(
    observer: ObserverClient,
    queue_name: str,
    *,
    time_from: str,
    time_to: str,
    cursor: str | None = None,
    limit: int = PAGE_LIMIT_DEFAULT,
) -> Any:
    return observer.list_dead_letters(
        queue_name,
        time_from=_require_aware(time_from),
        time_to=_require_aware(time_to),
        cursor=cursor,
        limit=limit,
    )


def _spec(
    name: str,
    description: str,
    input_schema: dict[str, Any],
    call: Callable[..., Any],
) -> ToolSpec:
    return ToolSpec(
        name=name,
        description=description,
        input_schema=input_schema,
        call=call,
        surface="observer",
        read_only_hint=True,
        open_world_hint=True,
    )


def _index_registry(rows: tuple[ToolSpec, ...]) -> dict[str, ToolSpec]:
    indexed: dict[str, ToolSpec] = {}
    for row in rows:
        if row.name in indexed:
            raise ValueError(f"duplicate tool name: {row.name}")
        indexed[row.name] = row
    return indexed


_OBSERVER_CALLS: dict[str, tuple[str, dict[str, Any], Callable[..., Any]]] = {
    "get_capabilities": (
        "Read-only view of deployment capabilities.",
        _object_schema({}, ()),
        _get_capabilities,
    ),
    "get_task": (
        "Read-only lookup of one task.",
        _object_schema({"task_id": _string_schema()}, ("task_id",)),
        _get_task,
    ),
    "list_task_attempts": (
        "Read-only one page of attempts for one task.",
        _object_schema(
            {
                "task_id": _string_schema(),
                "cursor": _cursor_schema(),
                "limit": _limit_schema(),
            },
            ("task_id",),
        ),
        _list_task_attempts,
    ),
    "get_queue": (
        "Read-only lookup of one queue.",
        _object_schema({"queue_name": _string_schema()}, ("queue_name",)),
        _get_queue,
    ),
    "get_stats": (
        "Read-only statistics snapshot.",
        _object_schema({}, ()),
        _get_stats,
    ),
    "get_maintenance_status": (
        "Read-only maintenance status.",
        _object_schema({}, ()),
        _get_maintenance_status,
    ),
    "list_inspection_tasks": (
        "Read-only one page of tasks in one queue.",
        _object_schema(
            {
                "queue_name": _string_schema(),
                "cursor": _cursor_schema(),
                "limit": _limit_schema(),
            },
            ("queue_name",),
        ),
        _list_inspection_tasks,
    ),
    "list_inspection_attempts": (
        "Read-only one page of attempts in a time range.",
        _object_schema(
            {
                "task_id": _string_schema(),
                "time_from": _string_schema(),
                "time_to": _string_schema(),
                "cursor": _cursor_schema(),
                "limit": _limit_schema(),
            },
            ("task_id", "time_from", "time_to"),
        ),
        _list_inspection_attempts,
    ),
    "list_dead_letters": (
        "Read-only one page of dead letters in a time range.",
        _object_schema(
            {
                "queue_name": _string_schema(),
                "time_from": _string_schema(),
                "time_to": _string_schema(),
                "cursor": _cursor_schema(),
                "limit": _limit_schema(),
            },
            ("queue_name", "time_from", "time_to"),
        ),
        _list_dead_letters,
    ),
}


def _observer_registry(rows: tuple[ManifestTool, ...]) -> tuple[ToolSpec, ...]:
    """Join manifest rows to static observer callables. Names come from the rows."""
    by_method = {
        row.method: row
        for row in rows
        if row.role == "observer" and row.client == "ObserverClient"
    }
    specs: list[ToolSpec] = []
    for method, (description, schema, call) in _OBSERVER_CALLS.items():
        row = by_method.get(method)
        if row is None:
            continue
        specs.append(_spec(row.name, description, schema, call))
    return tuple(specs)


REGISTRY: tuple[ToolSpec, ...] = _observer_registry(load_manifest())
REGISTRY_BY_NAME: dict[str, ToolSpec] = _index_registry(REGISTRY)
