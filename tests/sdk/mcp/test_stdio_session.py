"""Real stdio session for the local MCP observer server."""

from __future__ import annotations

import copy
import inspect
import json
import sys
import tempfile
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from typing import Any

import pytest
from mcp import Client
from mcp.client.session import ClientSession
from mcp.client.stdio import StdioServerParameters, stdio_client

from workhold_mcp.config import load_settings
from workhold_mcp.registry import REGISTRY_BY_NAME
from workhold_mcp.server import assert_published_schema, build_server

_CANARY = "WHMCP-TOKEN-CANARY"
_TASK_ID = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
_REPO_ROOT = Path(__file__).resolve().parents[3]
_TOOL_NAMES = (
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
_READ_TIMEOUT_SECONDS = 10.0


class _RecordingHandler(BaseHTTPRequestHandler):
    def log_message(self, format: str, *args: Any) -> None:  # noqa: A003
        return

    def _record(self, body: bytes) -> None:
        self.server.recorded.append(  # type: ignore[attr-defined]
            {"method": self.command, "path": self.path, "body": body}
        )

    def _respond(self, status: int, payload: dict[str, Any] | None) -> None:
        raw = b"" if payload is None else json.dumps(payload).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(raw)))
        self.end_headers()
        if raw:
            self.wfile.write(raw)

    def do_GET(self) -> None:  # noqa: N802
        length = int(self.headers.get("Content-Length", "0") or "0")
        body = self.rfile.read(length) if length else b""
        self._record(body)
        path = self.path.split("?", 1)[0]
        responder = self.server.routes.get(("GET", path))  # type: ignore[attr-defined]
        if responder is None:
            self._respond(
                404,
                {
                    "code": "task_not_found",
                    "message": "missing route",
                    "retryable": False,
                    "request_id": "00000000-0000-4000-8000-000000000099",
                    "details": {},
                },
            )
            return
        status, payload = responder()
        self._respond(status, payload)


@pytest.fixture
def recording_server() -> Any:
    server = HTTPServer(("127.0.0.1", 0), _RecordingHandler)
    server.recorded = []  # type: ignore[attr-defined]
    server.routes = {}  # type: ignore[attr-defined]
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    host, port = server.server_address[:2]
    base_url = f"http://{host}:{port}"
    try:
        yield server, base_url
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)


def _capabilities_body() -> dict[str, Any]:
    return {
        "protocol_major": 1,
        "protocol_version": "1.0",
        "schema_revision": "0001",
        "scheduling": True,
        "priority": True,
        "delivery_events": False,
        "batch_claim": False,
        "long_polling": False,
        "max_claim_tasks": 1,
        "max_wait_seconds": 0,
        "payload_runtime_max_bytes": 262144,
        "payload_hard_max_bytes": 1048576,
        "enqueue_dedup_ttl_seconds": 7776000,
        "enqueue_dedup_ttl_min_seconds": 2592000,
        "enqueue_dedup_ttl_max_seconds": 31536000,
        "terminal_replay_ttl_seconds": 604800,
        "terminal_replay_ttl_min_seconds": 86400,
        "terminal_replay_ttl_max_seconds": 2592000,
        "admin_replay_ttl_seconds": 2592000,
        "admin_replay_ttl_min_seconds": 604800,
        "admin_replay_ttl_max_seconds": 7776000,
    }


def _task_body() -> dict[str, Any]:
    return {
        "task_id": _TASK_ID,
        "queue_name": "orders",
        "producer_id": "producer-1",
        "state": "ready",
        "priority": 0,
        "available_at": "2026-09-19T00:00:00Z",
        "retry_policy_version": 1,
        "created_at": "2026-09-19T00:00:00Z",
        "spawned_task_ids": [],
        "delivery_event_ids": [],
    }


def _parameters(base_url: str) -> StdioServerParameters:
    return StdioServerParameters(
        command=sys.executable,
        args=["-m", "workhold_mcp"],
        cwd=str(_REPO_ROOT),
        env={
            "WORKHOLD_MCP_PUBLIC_BASE_URL": base_url,
            "WORKHOLD_MCP_ADMIN_BASE_URL": base_url,
            "WORKHOLD_MCP_OBSERVER_TOKEN": _CANARY,
        },
    )


def _install_routes(server: Any) -> None:
    server.routes[("GET", "/v1/capabilities")] = lambda: (200, _capabilities_body())
    server.routes[("GET", f"/v1/tasks/{_TASK_ID}")] = lambda: (200, _task_body())


def _raises_schema_mismatch(tool_name: str, published: dict[str, Any]) -> None:
    with pytest.raises(RuntimeError, match="schema mismatch") as exc_info:
        assert_published_schema(tool_name, published)
    assert tool_name in str(exc_info.value)


def test_schema_check_is_semantic() -> None:
    tool_name = "observer_list_task_attempts"
    spec = REGISTRY_BY_NAME[tool_name]
    published = copy.deepcopy(dict(spec.input_schema))
    published["title"] = "ObserverListTaskAttempts"
    published["description"] = "SDK description"
    published["$schema"] = "https://json-schema.org/draft/2020-12/schema"
    published["additionalProperties"] = False
    published["unevaluatedProperties"] = False
    published["properties"]["cursor"]["title"] = "Cursor"
    published["properties"]["limit"]["description"] = "page size"
    published["properties"]["task_id"]["title"] = "Task Id"
    assert_published_schema(tool_name, published)

    bad_cursor = copy.deepcopy(dict(spec.input_schema))
    bad_cursor["properties"]["cursor"]["maxLength"] = 256
    _raises_schema_mismatch(tool_name, bad_cursor)

    for key, value in (("minimum", 2), ("maximum", 10), ("default", 10)):
        bad_limit = copy.deepcopy(dict(spec.input_schema))
        bad_limit["properties"]["limit"][key] = value
        _raises_schema_mismatch(tool_name, bad_limit)

    bad_required = copy.deepcopy(dict(spec.input_schema))
    bad_required["required"] = ["task_id", "cursor"]
    _raises_schema_mismatch(tool_name, bad_required)

    policy = frozenset(_TOOL_NAMES)
    settings = load_settings(
        {
            "WORKHOLD_MCP_PUBLIC_BASE_URL": "http://127.0.0.1:9",
            "WORKHOLD_MCP_ADMIN_BASE_URL": "http://127.0.0.1:10",
            "WORKHOLD_MCP_OBSERVER_TOKEN": _CANARY,
        },
        names=policy,
    )
    server = build_server(settings, policy)
    tools = server._tool_manager.list_tools()
    assert [tool.name for tool in tools] == sorted(_TOOL_NAMES)
    assert len(tools) == len(_TOOL_NAMES)
    for tool in tools:
        assert tool.is_async is False
        assert inspect.iscoroutinefunction(tool.fn) is False


async def test_stdio_initialize_lists_nine_and_calls(recording_server: Any) -> None:
    server, base_url = recording_server
    _install_routes(server)
    params = _parameters(base_url)
    with tempfile.TemporaryFile(mode="w+", encoding="utf-8") as errlog:
        async with stdio_client(params, errlog=errlog) as (read, write):
            async with ClientSession(read, write, read_timeout_seconds=_READ_TIMEOUT_SECONDS) as session:
                await session.initialize()
                listed = await session.list_tools()
                resources = await session.list_resources()
                prompts = await session.list_prompts()
                result = await session.call_tool("observer_get_capabilities", arguments={})
        errlog.seek(0)
        captured = errlog.read()

    names = [tool.name for tool in listed.tools]
    assert names == sorted(_TOOL_NAMES)
    assert len(names) == len(_TOOL_NAMES)
    assert list(resources.resources) == []
    assert list(prompts.prompts) == []
    assert result.is_error is False
    block = result.content[0]
    assert block.type == "text"
    assert json.loads(block.text) == result.structured_content
    assert _CANARY not in block.text
    assert _CANARY not in json.dumps(result.structured_content)
    assert _CANARY not in captured


async def test_modern_discover_lists_the_same_nine(recording_server: Any) -> None:
    server, base_url = recording_server
    _install_routes(server)
    params = _parameters(base_url)
    async with Client(params, mode="auto", read_timeout_seconds=_READ_TIMEOUT_SECONDS) as client:
        listed = await client.list_tools()
    names = [tool.name for tool in listed.tools]
    assert names == sorted(_TOOL_NAMES)
    assert len(names) == len(_TOOL_NAMES)
