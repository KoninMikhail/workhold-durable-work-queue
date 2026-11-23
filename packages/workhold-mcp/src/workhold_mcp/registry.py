"""Authoritative catalog of read-only observer MCP tools."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import datetime
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


REGISTRY: tuple[ToolSpec, ...] = (
    _spec(
        "observer_get_capabilities",
        "Read-only view of deployment capabilities.",
        _object_schema({}, ()),
        _get_capabilities,
    ),
    _spec(
        "observer_get_task",
        "Read-only lookup of one task.",
        _object_schema({"task_id": _string_schema()}, ("task_id",)),
        _get_task,
    ),
    _spec(
        "observer_list_task_attempts",
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
    _spec(
        "observer_get_queue",
        "Read-only lookup of one queue.",
        _object_schema({"queue_name": _string_schema()}, ("queue_name",)),
        _get_queue,
    ),
    _spec(
        "observer_get_stats",
        "Read-only statistics snapshot.",
        _object_schema({}, ()),
        _get_stats,
    ),
    _spec(
        "observer_get_maintenance_status",
        "Read-only maintenance status.",
        _object_schema({}, ()),
        _get_maintenance_status,
    ),
    _spec(
        "observer_list_inspection_tasks",
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
    _spec(
        "observer_list_inspection_attempts",
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
    _spec(
        "observer_list_dead_letters",
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
)

REGISTRY_BY_NAME: dict[str, ToolSpec] = _index_registry(REGISTRY)
