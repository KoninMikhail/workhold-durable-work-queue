"""One policy advertises one tool contract on stdio and on Streamable HTTP.

stdio runs through a real ``python -m workhold_mcp`` session against a local recording
server. HTTP runs through the guarded app on ``TestClient`` with a full-scope injected
verifier and a scripted Workhold transport. Both sides read the same registry row and
project errors through the same function, so every snapshot below must be equal.
"""

from __future__ import annotations

import json
import sys
import tempfile
import threading
from collections.abc import Iterator, Mapping, Sequence
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from typing import Any

import pytest
from _workhold_client_core.errors import ProtocolError, raise_for_protocol_status
from _workhold_client_core.transport import HttpJsonTransport
from mcp.client.session import ClientSession
from mcp.client.stdio import StdioServerParameters, stdio_client
from mcp.server.auth.provider import AccessToken
from mcp.server.transport_security import TransportSecuritySettings
from starlette.testclient import TestClient
from workhold_client_testing import ScriptedSyncTransport, ok, protocol_error, task_body
from workhold_mcp.config import load_settings
from workhold_mcp.registry import preset_names
from workhold_mcp.server import build_server

RESOURCE = "https://mcp.example.invalid/mcp"
ISSUER = "https://idp.example.invalid"
BASE_URL = "http://127.0.0.1:8000"
CALLER_JWT = "WHMCP-CALLER-JWT"
PAYLOAD_CANARY = "WHMCP-PAYLOAD-CANARY"
MESSAGE_CANARY = "WHMCP-MESSAGE-CANARY"
ALL_SCOPES = ["workhold-mcp:read", "workhold-mcp:produce", "workhold-mcp:operate"]
HEADERS = {
    "Accept": "application/json, text/event-stream",
    "Content-Type": "application/json",
}
_REPO_ROOT = Path(__file__).resolve().parents[3]
_OBSERVER_TOKEN = "parity-observer-token"
_TASK_ID = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
_REQUEST_ID = "44444444-4444-4444-8444-444444444444"
_TWO_NAMES = ("observer_get_task", "observer_get_capabilities")
_THIRD_NAME = "observer_get_queue"
_SIX_FIELDS = {"kind", "code", "retryable", "status_code", "request_id", "retry_after_ms"}
_READ_TIMEOUT_SECONDS = 10.0
_MCP_PATH = "/mcp"


class _ScriptedHandler(BaseHTTPRequestHandler):
    """Serve one scripted Workhold response per route and record the bearer."""

    def log_message(self, format: str, *args: Any) -> None:  # noqa: A003
        return

    def do_GET(self) -> None:  # noqa: N802
        path = self.path.split("?", 1)[0]
        self.server.bearers.append(self.headers.get("Authorization", ""))  # type: ignore[attr-defined]
        status, payload = self.server.routes[path]  # type: ignore[attr-defined]
        raw = json.dumps(payload).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(raw)))
        self.end_headers()
        self.wfile.write(raw)


@pytest.fixture
def workhold_server() -> Iterator[tuple[HTTPServer, str]]:
    server = HTTPServer(("127.0.0.1", 0), _ScriptedHandler)
    server.routes = {}  # type: ignore[attr-defined]
    server.bearers = []  # type: ignore[attr-defined]
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    host, port = server.server_address[:2]
    try:
        yield server, f"http://{host}:{port}"
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)


class _Verifier:
    """Accept only the caller JWT and grant every MCP scope."""

    async def verify_token(self, token: str) -> AccessToken | None:
        if token != CALLER_JWT:
            return None
        return AccessToken(
            token=token, client_id="client-1", scopes=ALL_SCOPES, resource=RESOURCE
        )


def _env(base_url: str) -> dict[str, str]:
    return {
        "WORKHOLD_MCP_PUBLIC_BASE_URL": base_url,
        "WORKHOLD_MCP_ADMIN_BASE_URL": base_url,
        "WORKHOLD_MCP_OBSERVER_TOKEN": _OBSERVER_TOKEN,
    }


def _stdio_parameters(base_url: str, names: Sequence[str] | None) -> StdioServerParameters:
    args = ["-m", "workhold_mcp"]
    if names is not None:
        args += ["--allow-tool", ",".join(names)]
    return StdioServerParameters(
        command=sys.executable, args=args, cwd=str(_REPO_ROOT), env=_env(base_url)
    )


def _http_app(
    monkeypatch: pytest.MonkeyPatch,
    names: frozenset[str],
    steps: list[Any],
) -> tuple[Any, list[ScriptedSyncTransport]]:
    from workhold_mcp.http_app import build_auth_settings, build_http_app

    built: list[ScriptedSyncTransport] = []

    def _from_config(cls: Any, config: Any) -> ScriptedSyncTransport:
        transport = ScriptedSyncTransport(list(steps))
        built.append(transport)
        return transport

    monkeypatch.setattr(HttpJsonTransport, "from_config", classmethod(_from_config))
    server = build_server(
        load_settings(_env("http://127.0.0.1:9"), names=names),
        names,
        token_verifier=_Verifier(),
        auth=build_auth_settings(RESOURCE, ISSUER),
    )
    app = build_http_app(
        server,
        resource_url=RESOURCE,
        issuer_url=ISSUER,
        security=TransportSecuritySettings(
            enable_dns_rebinding_protection=True,
            allowed_hosts=["127.0.0.1:8000"],
            allowed_origins=[],
        ),
    )
    return app, built


def _http_rpc(app: Any, method: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
    body = {"jsonrpc": "2.0", "id": 1, "method": method, "params": params or {}}
    with TestClient(app, base_url=BASE_URL) as client:
        response = client.post(
            _MCP_PATH, headers={**HEADERS, "Authorization": f"Bearer {CALLER_JWT}"}, json=body
        )
    assert response.status_code == 200
    return response.json()["result"]


async def _stdio_list(base_url: str, names: Sequence[str] | None) -> list[dict[str, Any]]:
    params = _stdio_parameters(base_url, names)
    with tempfile.TemporaryFile(mode="w+", encoding="utf-8") as errlog:
        async with stdio_client(params, errlog=errlog) as (read, write):
            async with ClientSession(read, write, read_timeout_seconds=_READ_TIMEOUT_SECONDS) as session:
                await session.initialize()
                listed = await session.list_tools()
    return [tool.model_dump(by_alias=True, exclude_none=True) for tool in listed.tools]


async def _stdio_call(
    base_url: str, name: str, arguments: dict[str, Any]
) -> dict[str, Any]:
    params = _stdio_parameters(base_url, None)
    with tempfile.TemporaryFile(mode="w+", encoding="utf-8") as errlog:
        async with stdio_client(params, errlog=errlog) as (read, write):
            async with ClientSession(read, write, read_timeout_seconds=_READ_TIMEOUT_SECONDS) as session:
                await session.initialize()
                result = await session.call_tool(name, arguments=arguments)
    return result.model_dump(by_alias=True, exclude_none=True)


def _snapshot(tools: Sequence[Mapping[str, Any]]) -> list[tuple[str, str, str]]:
    """One tuple per tool: name, description, canonical input schema."""

    return sorted(
        (
            tool["name"],
            tool.get("description", ""),
            json.dumps(tool["inputSchema"], sort_keys=True),
        )
        for tool in tools
    )


def _result(raw: Mapping[str, Any]) -> tuple[bool, Any, str]:
    """The comparable cell of one tool call: error flag, structured result, text."""

    return (
        bool(raw.get("isError", False)),
        raw["structuredContent"],
        raw["content"][0]["text"],
    )


def _scripted_calls(transports: list[ScriptedSyncTransport]) -> list[Any]:
    return [call for transport in transports for call in transport.calls]


async def test_snapshots_match_for_observer_preset(
    monkeypatch: pytest.MonkeyPatch, workhold_server: tuple[HTTPServer, str]
) -> None:
    _, base_url = workhold_server
    observer = frozenset(preset_names("observer"))
    app, _ = _http_app(monkeypatch, observer, [])
    http_tools = _http_rpc(app, "tools/list")["tools"]
    stdio_tools = await _stdio_list(base_url, None)
    assert _snapshot(stdio_tools) == _snapshot(http_tools)
    assert len(_snapshot(http_tools)) == len(observer)
    assert {name for name, _, _ in _snapshot(http_tools)} == observer


async def test_snapshots_match_for_custom_allowlist(
    monkeypatch: pytest.MonkeyPatch, workhold_server: tuple[HTTPServer, str]
) -> None:
    _, base_url = workhold_server
    app, _ = _http_app(monkeypatch, frozenset(_TWO_NAMES), [])
    http_tools = _http_rpc(app, "tools/list")["tools"]
    stdio_tools = await _stdio_list(base_url, _TWO_NAMES)
    assert _snapshot(stdio_tools) == _snapshot(http_tools)
    names = [name for name, _, _ in _snapshot(http_tools)]
    assert names == sorted(_TWO_NAMES)
    assert _THIRD_NAME not in names


async def test_success_result_matches(
    monkeypatch: pytest.MonkeyPatch, workhold_server: tuple[HTTPServer, str]
) -> None:
    server, base_url = workhold_server
    body = task_body(payload={"marker": PAYLOAD_CANARY})
    server.routes[f"/v1/tasks/{_TASK_ID}"] = (200, body)  # type: ignore[attr-defined]
    arguments = {"task_id": _TASK_ID}
    stdio = await _stdio_call(base_url, "observer_get_task", arguments)

    app, built = _http_app(monkeypatch, frozenset(preset_names("observer")), [ok(body)])
    http = _http_rpc(app, "tools/call", {"name": "observer_get_task", "arguments": arguments})

    assert _result(http) == _result(stdio)
    is_error, structured, text = _result(http)
    assert is_error is False
    assert structured["payload"] == {"marker": PAYLOAD_CANARY}
    assert json.loads(text) == structured
    calls = _scripted_calls(built)
    assert len(calls) == 1
    bearer = calls[0].headers(include_secrets=True)["Authorization"]
    assert bearer == f"Bearer {_OBSERVER_TOKEN}"
    assert server.bearers == [bearer]  # type: ignore[attr-defined]
    for value in calls[0].headers(include_secrets=True).values():
        assert CALLER_JWT not in value


async def test_permission_denied_matches(
    monkeypatch: pytest.MonkeyPatch, workhold_server: tuple[HTTPServer, str]
) -> None:
    server, base_url = workhold_server
    denied = protocol_error(
        403,
        code="permission_denied",
        message=MESSAGE_CANARY,
        retryable=False,
        request_id=_REQUEST_ID,
        details={},
    )
    server.routes[f"/v1/tasks/{_TASK_ID}"] = (403, denied.body)  # type: ignore[attr-defined]
    arguments = {"task_id": _TASK_ID}
    stdio = await _stdio_call(base_url, "observer_get_task", arguments)

    # The scripted transport replays steps verbatim, so raise what HttpJsonTransport raises.
    with pytest.raises(ProtocolError) as raised:
        raise_for_protocol_status(denied.status_code, denied.body)
    app, built = _http_app(
        monkeypatch, frozenset(preset_names("observer")), [raised.value]
    )
    http = _http_rpc(app, "tools/call", {"name": "observer_get_task", "arguments": arguments})

    assert _result(http) == _result(stdio)
    is_error, structured, text = _result(http)
    assert is_error is True
    assert set(structured) == _SIX_FIELDS
    assert structured["kind"] == "protocol"
    assert structured["code"] == "permission_denied"
    assert structured["status_code"] == 403
    assert structured["request_id"] == _REQUEST_ID
    assert MESSAGE_CANARY not in text
    assert CALLER_JWT not in text
    calls = _scripted_calls(built)
    assert len(calls) == 1
    assert calls[0].headers(include_secrets=True)["Authorization"] == f"Bearer {_OBSERVER_TOKEN}"
