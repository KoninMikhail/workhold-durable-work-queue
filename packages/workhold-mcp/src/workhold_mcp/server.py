"""Register observer tools on an MCP stdio server."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Annotated, Any

from mcp.server import MCPServer
from mcp.types import CallToolResult, TextContent, ToolAnnotations
from pydantic import Field
from workhold_admin.models import (
    CURSOR_MAX_LENGTH,
    PAGE_LIMIT_DEFAULT,
    PAGE_LIMIT_MAX,
    PAGE_LIMIT_MIN,
)
from workhold_admin.observer import ObserverClient

from workhold_mcp.errors import project_error
from workhold_mcp.registry import REGISTRY_BY_NAME
from workhold_mcp.serialize import canonical_json, to_tool_payload

_Cursor = Annotated[str, Field(max_length=CURSOR_MAX_LENGTH)]
_Limit = Annotated[int, Field(ge=PAGE_LIMIT_MIN, le=PAGE_LIMIT_MAX)]
_ANNOTATIONS = ToolAnnotations(read_only_hint=True, open_world_hint=True)


def build_server(observer: ObserverClient) -> MCPServer:
    """Register the nine observer tools and refuse a semantic schema drift."""

    server = MCPServer("workhold-mcp", subscriptions=False)

    def observer_get_capabilities() -> CallToolResult:
        return call_tool(observer, "observer_get_capabilities", {})

    def observer_get_task(task_id: str) -> CallToolResult:
        return call_tool(observer, "observer_get_task", {"task_id": task_id})

    def observer_list_task_attempts(
        task_id: str,
        cursor: _Cursor = None,
        limit: _Limit = PAGE_LIMIT_DEFAULT,
    ) -> CallToolResult:
        return call_tool(
            observer,
            "observer_list_task_attempts",
            {"task_id": task_id, "cursor": cursor, "limit": limit},
        )

    def observer_get_queue(queue_name: str) -> CallToolResult:
        return call_tool(observer, "observer_get_queue", {"queue_name": queue_name})

    def observer_get_stats() -> CallToolResult:
        return call_tool(observer, "observer_get_stats", {})

    def observer_get_maintenance_status() -> CallToolResult:
        return call_tool(observer, "observer_get_maintenance_status", {})

    def observer_list_inspection_tasks(
        queue_name: str,
        cursor: _Cursor = None,
        limit: _Limit = PAGE_LIMIT_DEFAULT,
    ) -> CallToolResult:
        return call_tool(
            observer,
            "observer_list_inspection_tasks",
            {"queue_name": queue_name, "cursor": cursor, "limit": limit},
        )

    def observer_list_inspection_attempts(
        task_id: str,
        time_from: str,
        time_to: str,
        cursor: _Cursor = None,
        limit: _Limit = PAGE_LIMIT_DEFAULT,
    ) -> CallToolResult:
        return call_tool(
            observer,
            "observer_list_inspection_attempts",
            {
                "task_id": task_id,
                "time_from": time_from,
                "time_to": time_to,
                "cursor": cursor,
                "limit": limit,
            },
        )

    def observer_list_dead_letters(
        queue_name: str,
        time_from: str,
        time_to: str,
        cursor: _Cursor = None,
        limit: _Limit = PAGE_LIMIT_DEFAULT,
    ) -> CallToolResult:
        return call_tool(
            observer,
            "observer_list_dead_letters",
            {
                "queue_name": queue_name,
                "time_from": time_from,
                "time_to": time_to,
                "cursor": cursor,
                "limit": limit,
            },
        )

    for fn, name in (
        (observer_get_capabilities, "observer_get_capabilities"),
        (observer_get_task, "observer_get_task"),
        (observer_list_task_attempts, "observer_list_task_attempts"),
        (observer_get_queue, "observer_get_queue"),
        (observer_get_stats, "observer_get_stats"),
        (observer_get_maintenance_status, "observer_get_maintenance_status"),
        (observer_list_inspection_tasks, "observer_list_inspection_tasks"),
        (observer_list_inspection_attempts, "observer_list_inspection_attempts"),
        (observer_list_dead_letters, "observer_list_dead_letters"),
    ):
        spec = REGISTRY_BY_NAME[name]
        server.add_tool(
            fn,
            name=spec.name,
            description=spec.description,
            annotations=_ANNOTATIONS,
        )
    _check_published_schemas(server)
    return server


def call_tool(
    observer: ObserverClient,
    name: str,
    arguments: Mapping[str, Any] | None = None,
) -> CallToolResult:
    """Dispatch one registry call and project success or a bounded failure."""

    spec = REGISTRY_BY_NAME.get(name)
    if spec is None:
        return _error_result(ValueError("unknown tool"))
    try:
        value = spec.call(observer, **dict(arguments or {}))
    except Exception as exc:
        return _error_result(exc)
    return _success_result(to_tool_payload(value))


def assert_published_schema(tool_name: str, published: Mapping[str, Any]) -> None:
    """Raise when semantic input-schema fields differ from the registry."""

    spec = REGISTRY_BY_NAME.get(tool_name)
    if spec is None or not _schemas_match(published, spec.input_schema):
        raise RuntimeError(f"schema mismatch: {tool_name}")


def _check_published_schemas(server: MCPServer) -> None:
    tools = server._tool_manager.list_tools()
    seen: set[str] = set()
    for tool in tools:
        assert_published_schema(tool.name, tool.parameters)
        seen.add(tool.name)
    for name in REGISTRY_BY_NAME:
        if name not in seen:
            raise RuntimeError(f"schema mismatch: {name}")


def _schemas_match(published: Mapping[str, Any], expected: Mapping[str, Any]) -> bool:
    published_props = published.get("properties") or {}
    expected_props = expected.get("properties") or {}
    if not isinstance(published_props, Mapping) or set(published_props) != set(expected_props):
        return False
    for name, expected_prop in expected_props.items():
        published_prop = published_props[name]
        if not isinstance(published_prop, Mapping) or not isinstance(expected_prop, Mapping):
            return False
        if published_prop.get("type") != expected_prop.get("type"):
            return False
        if name == "limit":
            for key in ("minimum", "maximum", "default"):
                if published_prop.get(key) != expected_prop.get(key):
                    return False
        if name == "cursor" and published_prop.get("maxLength") != expected_prop.get("maxLength"):
            return False
    published_required = set(published.get("required") or [])
    expected_required = set(expected.get("required") or [])
    return published_required == expected_required


def _success_result(payload: Any) -> CallToolResult:
    text = canonical_json(payload)
    return CallToolResult(
        content=[TextContent(type="text", text=text)],
        structured_content=payload,
        is_error=False,
    )


def _error_result(exc: Exception) -> CallToolResult:
    payload = project_error(exc)
    text = canonical_json(payload)
    return CallToolResult(
        content=[TextContent(type="text", text=text)],
        structured_content=payload,
        is_error=True,
    )
