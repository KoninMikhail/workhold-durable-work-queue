"""MCP tool catalog and static observer and producer callables.

``load_manifest`` reads the checked-in role-prefixed catalog. ``REGISTRY``
joins manifest rows to explicit per-role handlers (no dynamic dispatch).
"""

from __future__ import annotations

import json
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

from workhold_admin.admin import AdminClient
from workhold_admin.models import (
    CURSOR_MAX_LENGTH,
    PAGE_LIMIT_DEFAULT,
    PAGE_LIMIT_MAX,
    PAGE_LIMIT_MIN,
    BulkPreviewResult,
    RetryPolicyDraft,
)
from _workhold_client_core.priority import PRIORITY_DEFAULT
from workhold_admin.observer import ObserverClient
from workhold_producer.client import ProducerClient


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


def _require_idempotency_key(key: str | None) -> str:
    if not isinstance(key, str) or not key.strip():
        raise ValueError("idempotency_key is required")
    return key


def _require_expected_config_version(value: int | None) -> int:
    if value is None:
        raise ValueError("expected_config_version is required")
    return value


def _require_reason(value: str | None) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError("reason is required")
    return value


def _loose_object_schema() -> dict[str, Any]:
    return {"type": "object", "additionalProperties": True}


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
    *,
    surface: str,
    read_only_hint: bool,
) -> ToolSpec:
    return ToolSpec(
        name=name,
        description=description,
        input_schema=input_schema,
        call=call,
        surface=surface,
        read_only_hint=read_only_hint,
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
        specs.append(
            _spec(
                row.name,
                description,
                schema,
                call,
                surface="observer",
                read_only_hint=True,
            )
        )
    return tuple(specs)


def _producer_get_capabilities(producer: ProducerClient) -> Any:
    return producer.get_capabilities()


def _producer_enqueue(
    producer: ProducerClient,
    queue_name: str,
    *,
    idempotency_key: str | None = None,
    payload: Any,
    priority: int = PRIORITY_DEFAULT,
    available_at: str | None = None,
) -> Any:
    wire_key = _require_idempotency_key(idempotency_key)
    if available_at is not None:
        return producer.enqueue(
            queue_name,
            idempotency_key=wire_key,
            payload=payload,
            priority=priority,
            available_at=_require_aware(available_at),
        )
    return producer.enqueue(
        queue_name,
        idempotency_key=wire_key,
        payload=payload,
        priority=priority,
    )


def _producer_resolve_submission(
    producer: ProducerClient,
    queue_name: str,
    *,
    idempotency_key: str | None = None,
) -> Any:
    wire_key = _require_idempotency_key(idempotency_key)
    return producer.resolve_submission(queue_name, idempotency_key=wire_key)


def _producer_inspect_task(producer: ProducerClient, task_id: str) -> Any:
    return producer.inspect_task(task_id)


def _producer_cancel_task(
    producer: ProducerClient,
    task_id: str,
    *,
    reason: str | None = None,
) -> Any:
    if reason is None:
        return producer.cancel_task(task_id)
    return producer.cancel_task(task_id, reason=reason)


_PRODUCER_CALLS: dict[str, tuple[str, dict[str, Any], Callable[..., Any], bool]] = {
    "get_capabilities": (
        "Producer deployment capabilities.",
        _object_schema({}, ()),
        _producer_get_capabilities,
        True,
    ),
    "enqueue": (
        "Enqueue one task with caller-supplied idempotency identity.",
        _object_schema(
            {
                "queue_name": _string_schema(),
                "idempotency_key": _string_schema(),
                "payload": _loose_object_schema(),
                "priority": {
                    "type": "integer",
                    "default": PRIORITY_DEFAULT,
                },
                "available_at": _string_schema(),
            },
            ("queue_name", "idempotency_key", "payload"),
        ),
        _producer_enqueue,
        False,
    ),
    "resolve_submission": (
        "Resolve a prior enqueue by idempotency key.",
        _object_schema(
            {
                "queue_name": _string_schema(),
                "idempotency_key": _string_schema(),
            },
            ("queue_name", "idempotency_key"),
        ),
        _producer_resolve_submission,
        True,
    ),
    "inspect_task": (
        "Producer-authorized task lookup (opened payload when keyed).",
        _object_schema({"task_id": _string_schema()}, ("task_id",)),
        _producer_inspect_task,
        True,
    ),
    "cancel_task": (
        "Cancel one task; reason is optional.",
        _object_schema(
            {
                "task_id": _string_schema(),
                "reason": _string_schema(),
            },
            ("task_id",),
        ),
        _producer_cancel_task,
        False,
    ),
}


def _producer_registry(rows: tuple[ManifestTool, ...]) -> tuple[ToolSpec, ...]:
    by_method = {
        row.method: row
        for row in rows
        if row.role == "producer" and row.client == "ProducerClient"
    }
    specs: list[ToolSpec] = []
    for method, (description, schema, call, read_only) in _PRODUCER_CALLS.items():
        row = by_method.get(method)
        if row is None:
            continue
        specs.append(
            _spec(
                row.name,
                description,
                schema,
                call,
                surface="producer",
                read_only_hint=read_only,
            )
        )
    return tuple(specs)


def _admin_list_queues(
    admin: AdminClient,
    *,
    cursor: str | None = None,
    limit: int = PAGE_LIMIT_DEFAULT,
) -> Any:
    return admin.list_queues(cursor=cursor, limit=limit)


def _admin_create_queue(
    admin: AdminClient,
    name: str,
    *,
    initial_policy: Any,
    idempotency_key: str | None = None,
) -> Any:
    wire_key = _require_idempotency_key(idempotency_key)
    policy = RetryPolicyDraft.parse(initial_policy)
    return admin.create_queue(
        name, initial_policy=policy, idempotency_key=wire_key
    )


def _admin_get_queue(admin: AdminClient, queue_name: str) -> Any:
    return admin.get_queue(queue_name)


def _admin_create_queue_policy(
    admin: AdminClient,
    queue_name: str,
    policy: Any,
    *,
    idempotency_key: str | None = None,
) -> Any:
    wire_key = _require_idempotency_key(idempotency_key)
    draft = RetryPolicyDraft.parse(policy)
    return admin.create_queue_policy(
        queue_name, draft, idempotency_key=wire_key
    )


def _admin_activate_queue_policy(
    admin: AdminClient,
    queue_name: str,
    policy_version: int,
    *,
    expected_config_version: int | None = None,
    idempotency_key: str | None = None,
) -> Any:
    wire_key = _require_idempotency_key(idempotency_key)
    wire_config = _require_expected_config_version(expected_config_version)
    return admin.activate_queue_policy(
        queue_name,
        policy_version,
        expected_config_version=wire_config,
        idempotency_key=wire_key,
    )


def _admin_set_queue_state(
    admin: AdminClient,
    queue_name: str,
    state: str,
    *,
    expected_config_version: int | None = None,
    idempotency_key: str | None = None,
) -> Any:
    wire_key = _require_idempotency_key(idempotency_key)
    wire_config = _require_expected_config_version(expected_config_version)
    return admin.set_queue_state(
        queue_name,
        state,
        expected_config_version=wire_config,
        idempotency_key=wire_key,
    )


def _admin_get_maintenance_status(admin: AdminClient) -> Any:
    return admin.get_maintenance_status()


def _admin_get_capabilities(admin: AdminClient) -> Any:
    return admin.get_capabilities()


def _admin_get_stats(admin: AdminClient) -> Any:
    return admin.get_stats()


def _admin_list_inspection_tasks(
    admin: AdminClient,
    queue_name: str,
    *,
    cursor: str | None = None,
    limit: int = PAGE_LIMIT_DEFAULT,
) -> Any:
    return admin.list_inspection_tasks(queue_name, cursor=cursor, limit=limit)


def _admin_list_inspection_attempts(
    admin: AdminClient,
    task_id: str,
    *,
    time_from: str,
    time_to: str,
    cursor: str | None = None,
    limit: int = PAGE_LIMIT_DEFAULT,
) -> Any:
    return admin.list_inspection_attempts(
        task_id,
        time_from=_require_aware(time_from),
        time_to=_require_aware(time_to),
        cursor=cursor,
        limit=limit,
    )


def _admin_list_dead_letters(
    admin: AdminClient,
    queue_name: str,
    *,
    time_from: str,
    time_to: str,
    cursor: str | None = None,
    limit: int = PAGE_LIMIT_DEFAULT,
) -> Any:
    return admin.list_dead_letters(
        queue_name,
        time_from=_require_aware(time_from),
        time_to=_require_aware(time_to),
        cursor=cursor,
        limit=limit,
    )


def _admin_list_admin_audit(
    admin: AdminClient,
    *,
    time_from: str,
    time_to: str,
    queue_name: str | None = None,
    cursor: str | None = None,
    limit: int = PAGE_LIMIT_DEFAULT,
) -> Any:
    return admin.list_admin_audit(
        time_from=_require_aware(time_from),
        time_to=_require_aware(time_to),
        queue_name=queue_name,
        cursor=cursor,
        limit=limit,
    )


def _admin_run_maintenance(
    admin: AdminClient,
    *,
    idempotency_key: str | None = None,
) -> Any:
    wire_key = _require_idempotency_key(idempotency_key)
    return admin.run_maintenance(idempotency_key=wire_key)


def _admin_replay_dead_letter(
    admin: AdminClient,
    queue_name: str,
    task_id: str,
    *,
    idempotency_key: str | None = None,
    reason: str | None = None,
) -> Any:
    wire_key = _require_idempotency_key(idempotency_key)
    wire_reason = _require_reason(reason)
    return admin.replay_dead_letter(
        queue_name,
        task_id,
        idempotency_key=wire_key,
        reason=wire_reason,
    )


def _admin_preview_bulk_replay(
    admin: AdminClient,
    queue_name: str,
    *,
    filters: Any | None = None,
) -> Any:
    return admin.preview_bulk_replay(queue_name, filters=filters)


def _admin_preview_bulk_cancel(
    admin: AdminClient,
    queue_name: str,
    *,
    filters: Any | None = None,
) -> Any:
    return admin.preview_bulk_cancel(queue_name, filters=filters)


def _admin_execute_bulk_replay(
    admin: AdminClient,
    queue_name: str,
    *,
    preview: Any = None,
    idempotency_key: str | None = None,
    reason: str | None = None,
    filters: Any = None,
    start_index: int = 0,
    batch_limit: int | None = None,
) -> Any:
    bound_preview = BulkPreviewResult.parse(preview)
    wire_key = _require_idempotency_key(idempotency_key)
    wire_reason = _require_reason(reason)
    return admin.execute_bulk_replay(
        queue_name,
        preview=bound_preview,
        idempotency_key=wire_key,
        reason=wire_reason,
        filters=filters,
        start_index=start_index,
        batch_limit=batch_limit,
    )


def _admin_execute_bulk_cancel(
    admin: AdminClient,
    queue_name: str,
    *,
    preview: Any = None,
    reason: str | None = None,
    filters: Any = None,
    start_index: int = 0,
    batch_limit: int | None = None,
) -> Any:
    bound_preview = BulkPreviewResult.parse(preview)
    wire_reason = _require_reason(reason)
    return admin.execute_bulk_cancel(
        queue_name,
        preview=bound_preview,
        reason=wire_reason,
        filters=filters,
        start_index=start_index,
        batch_limit=batch_limit,
    )


_ADMIN_CALLS: dict[str, tuple[str, dict[str, Any], Callable[..., Any], bool]] = {
    "list_queues": (
        "Admin one page of queue registrations.",
        _object_schema(
            {
                "cursor": _cursor_schema(),
                "limit": _limit_schema(),
            },
            (),
        ),
        _admin_list_queues,
        True,
    ),
    "create_queue": (
        "Create a queue with an initial retry policy draft.",
        _object_schema(
            {
                "name": _string_schema(),
                "initial_policy": _loose_object_schema(),
                "idempotency_key": _string_schema(),
            },
            ("name", "initial_policy", "idempotency_key"),
        ),
        _admin_create_queue,
        False,
    ),
    "get_queue": (
        "Admin lookup of one queue.",
        _object_schema({"queue_name": _string_schema()}, ("queue_name",)),
        _admin_get_queue,
        True,
    ),
    "create_queue_policy": (
        "Draft a new retry policy version for one queue.",
        _object_schema(
            {
                "queue_name": _string_schema(),
                "policy": _loose_object_schema(),
                "idempotency_key": _string_schema(),
            },
            ("queue_name", "policy", "idempotency_key"),
        ),
        _admin_create_queue_policy,
        False,
    ),
    "activate_queue_policy": (
        "Activate a policy version with caller-supplied config version.",
        _object_schema(
            {
                "queue_name": _string_schema(),
                "policy_version": {"type": "integer"},
                "expected_config_version": {"type": "integer"},
                "idempotency_key": _string_schema(),
            },
            (
                "queue_name",
                "policy_version",
                "expected_config_version",
                "idempotency_key",
            ),
        ),
        _admin_activate_queue_policy,
        False,
    ),
    "set_queue_state": (
        "Set queue state with caller-supplied config version.",
        _object_schema(
            {
                "queue_name": _string_schema(),
                "state": _string_schema(),
                "expected_config_version": {"type": "integer"},
                "idempotency_key": _string_schema(),
            },
            ("queue_name", "state", "expected_config_version", "idempotency_key"),
        ),
        _admin_set_queue_state,
        False,
    ),
    "get_maintenance_status": (
        "Maintenance status (admin-authorized).",
        _object_schema({}, ()),
        _admin_get_maintenance_status,
        True,
    ),
    "get_capabilities": (
        "Deployment capabilities (admin-authorized).",
        _object_schema({}, ()),
        _admin_get_capabilities,
        True,
    ),
    "get_stats": (
        "Statistics snapshot (admin plane).",
        _object_schema({}, ()),
        _admin_get_stats,
        True,
    ),
    "list_inspection_tasks": (
        "Admin one page of tasks in one queue.",
        _object_schema(
            {
                "queue_name": _string_schema(),
                "cursor": _cursor_schema(),
                "limit": _limit_schema(),
            },
            ("queue_name",),
        ),
        _admin_list_inspection_tasks,
        True,
    ),
    "list_inspection_attempts": (
        "Admin one page of attempts in a time range.",
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
        _admin_list_inspection_attempts,
        True,
    ),
    "list_dead_letters": (
        "Admin one page of dead letters in a time range.",
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
        _admin_list_dead_letters,
        True,
    ),
    "list_admin_audit": (
        "Admin one page of audit records in a time range.",
        _object_schema(
            {
                "time_from": _string_schema(),
                "time_to": _string_schema(),
                "queue_name": _string_schema(),
                "cursor": _cursor_schema(),
                "limit": _limit_schema(),
            },
            ("time_from", "time_to"),
        ),
        _admin_list_admin_audit,
        True,
    ),
    "run_maintenance": (
        "Run maintenance with caller-supplied idempotency identity.",
        _object_schema({"idempotency_key": _string_schema()}, ("idempotency_key",)),
        _admin_run_maintenance,
        False,
    ),
    "replay_dead_letter": (
        "Replay one dead letter with caller reason and idempotency key.",
        _object_schema(
            {
                "queue_name": _string_schema(),
                "task_id": _string_schema(),
                "idempotency_key": _string_schema(),
                "reason": _string_schema(),
            },
            ("queue_name", "task_id", "idempotency_key", "reason"),
        ),
        _admin_replay_dead_letter,
        False,
    ),
    "preview_bulk_replay": (
        "Dry-run bulk replay preview for one queue.",
        _object_schema(
            {
                "queue_name": _string_schema(),
                "filters": _loose_object_schema(),
            },
            ("queue_name",),
        ),
        _admin_preview_bulk_replay,
        True,
    ),
    "preview_bulk_cancel": (
        "Dry-run bulk cancel preview for one queue.",
        _object_schema(
            {
                "queue_name": _string_schema(),
                "filters": _loose_object_schema(),
            },
            ("queue_name",),
        ),
        _admin_preview_bulk_cancel,
        True,
    ),
    "execute_bulk_replay": (
        "Execute one bulk replay batch from the complete preview object.",
        _object_schema(
            {
                "queue_name": _string_schema(),
                "preview": _loose_object_schema(),
                "idempotency_key": _string_schema(),
                "reason": _string_schema(),
                "filters": _loose_object_schema(),
                "start_index": {"type": "integer", "default": 0},
                "batch_limit": {"type": "integer", "default": None},
            },
            ("queue_name", "preview", "idempotency_key", "reason", "filters"),
        ),
        _admin_execute_bulk_replay,
        False,
    ),
    "execute_bulk_cancel": (
        "Execute one bulk cancel batch from the complete preview object.",
        _object_schema(
            {
                "queue_name": _string_schema(),
                "preview": _loose_object_schema(),
                "reason": _string_schema(),
                "filters": _loose_object_schema(),
                "start_index": {"type": "integer", "default": 0},
                "batch_limit": {"type": "integer", "default": None},
            },
            ("queue_name", "preview", "reason", "filters"),
        ),
        _admin_execute_bulk_cancel,
        False,
    ),
}


def _admin_registry(rows: tuple[ManifestTool, ...]) -> tuple[ToolSpec, ...]:
    by_method = {
        row.method: row
        for row in rows
        if row.role == "admin" and row.client == "AdminClient"
    }
    specs: list[ToolSpec] = []
    for method, (description, schema, call, read_only) in _ADMIN_CALLS.items():
        row = by_method.get(method)
        if row is None:
            continue
        specs.append(
            _spec(
                row.name,
                description,
                schema,
                call,
                surface="admin",
                read_only_hint=read_only,
            )
        )
    return tuple(specs)


_MANIFEST_ROWS = load_manifest()
REGISTRY: tuple[ToolSpec, ...] = (
    _observer_registry(_MANIFEST_ROWS)
    + _producer_registry(_MANIFEST_ROWS)
    + _admin_registry(_MANIFEST_ROWS)
)
REGISTRY_BY_NAME: dict[str, ToolSpec] = _index_registry(REGISTRY)
