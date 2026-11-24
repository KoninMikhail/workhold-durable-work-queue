"""Startup policy, MCP scope, and Workhold authorization are three separate gates.

A caller scope can only narrow the startup policy, never widen it. A missing or
insufficient scope never reaches a role client. The caller JWT is never a Workhold
credential. Every row runs on a scripted transport: no socket, no Workhold server.
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any

import pytest
from mcp.server.auth.provider import AccessToken
from mcp.server.mcpserver.exceptions import ToolError
from mcp.server.transport_security import TransportSecuritySettings
from starlette.testclient import TestClient

from _workhold_client_core.errors import ProtocolError, raise_for_protocol_status
from _workhold_client_core.transport import HttpJsonTransport
from workhold_client_testing import (
    ScriptedSyncTransport,
    ok,
    page_body,
    protocol_error,
    task_body,
)
from workhold_mcp.config import load_settings
from workhold_mcp.policy import resolve_tool_policy
from workhold_mcp.registry import load_manifest, preset_names
from workhold_mcp.server import build_server

RESOURCE = "https://mcp.example.invalid/mcp"
ISSUER = "https://idp.example.invalid"
BASE_URL = "http://127.0.0.1:8000"
CALLER_JWT = "WHMCP-CALLER-JWT"
READ = "workhold-mcp:read"
PRODUCE = "workhold-mcp:produce"
OPERATE = "workhold-mcp:operate"
ALL_SCOPES = [READ, PRODUCE, OPERATE]
UNRELATED = "profile:unrelated"
HEADERS = {
    "Accept": "application/json, text/event-stream",
    "Content-Type": "application/json",
}
MCP_SRC = Path(__file__).resolve().parents[3] / "packages" / "workhold-mcp" / "src"

_OBSERVER = "matrix-observer-token"
_PRODUCER = "matrix-producer-token"
_ADMIN = "matrix-admin-token"
_TOKENS = {"observer": _OBSERVER, "producer": _PRODUCER, "admin": _ADMIN}
_TOKEN_ENV = {
    "observer": "WORKHOLD_MCP_OBSERVER_TOKEN",
    "producer": "WORKHOLD_MCP_PRODUCER_TOKEN",
    "admin": "WORKHOLD_MCP_ADMIN_TOKEN",
}
_BASE_ENV = {
    "WORKHOLD_MCP_PUBLIC_BASE_URL": "http://127.0.0.1:9",
    "WORKHOLD_MCP_ADMIN_BASE_URL": "http://127.0.0.1:10",
}
_TASK_ID = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
_REQUEST_ID = "33333333-3333-4333-8333-333333333333"
_SIX_FIELDS = {
    "kind",
    "code",
    "retryable",
    "status_code",
    "request_id",
    "retry_after_ms",
}
_SCOPE_BY_ROLE = {"observer": READ, "producer": PRODUCE, "admin": OPERATE}
_ROLES = ("observer", "producer", "admin")


def _queue_body() -> dict[str, Any]:
    return {
        "queue_id": "cccccccc-cccc-4ccc-8ccc-cccccccccccc",
        "name": "orders",
        "state": "active",
        "config_version": 1,
        "active_policy": {
            "version": 1,
            "enabled": True,
            "max_attempts": 3,
            "backoff_strategy": "fixed",
            "retry_delay_seconds": 5,
            "created_at": "2026-09-19T00:00:00Z",
        },
        "created_at": "2026-09-19T00:00:00Z",
        "updated_at": "2026-09-19T00:00:00Z",
    }


def _success_step(role: str) -> Any:
    if role == "observer":
        return ok(task_body())
    if role == "producer":
        return ok({"task": task_body(), "replayed": False})
    return ok(page_body([_queue_body()]))


_TOOL = {
    "observer": ("observer_get_task", {"task_id": _TASK_ID}),
    "producer": (
        "producer_enqueue",
        {"queue_name": "orders", "idempotency_key": "idem-scope-1", "payload": {"n": 1}},
    ),
    "admin": ("admin_list_queues", {}),
}
_ALL_TOOL_NAMES = frozenset(name for name, _ in _TOOL.values())


def _denied() -> ProtocolError:
    response = protocol_error(
        403,
        code="permission_denied",
        message="denied",
        retryable=False,
        request_id=_REQUEST_ID,
        details={},
    )
    try:
        raise_for_protocol_status(response.status_code, response.body)
    except ProtocolError as exc:
        return exc
    raise AssertionError("403 body did not raise")


class _ScopedVerifier:
    """Accept only the caller JWT and return the scopes the matrix cell chose."""

    def __init__(self, scopes: list[str]) -> None:
        self.scopes = scopes

    async def verify_token(self, token: str) -> AccessToken | None:
        if token != CALLER_JWT:
            return None
        return AccessToken(
            token=token,
            client_id="client-1",
            scopes=self.scopes,
            resource=RESOURCE,
        )


def _security() -> TransportSecuritySettings:
    return TransportSecuritySettings(
        enable_dns_rebinding_protection=True,
        allowed_hosts=["127.0.0.1:8000"],
        allowed_origins=[],
    )


def _env(overrides: dict[str, str] | None = None) -> dict[str, str]:
    env = dict(_BASE_ENV)
    for role, name in _TOKEN_ENV.items():
        env[name] = _TOKENS[role]
    env.update(overrides or {})
    return env


def _patch_transports(
    monkeypatch: pytest.MonkeyPatch, steps: list[Any]
) -> list[ScriptedSyncTransport]:
    built: list[ScriptedSyncTransport] = []

    def _from_config(cls: Any, config: Any) -> ScriptedSyncTransport:
        transport = ScriptedSyncTransport(list(steps))
        built.append(transport)
        return transport

    monkeypatch.setattr(HttpJsonTransport, "from_config", classmethod(_from_config))
    return built


class _HttpApp:
    """Build a fresh guarded app per request: an SDK session manager runs once."""

    def __init__(
        self, names: frozenset[str], scopes: list[str], env: dict[str, str] | None
    ) -> None:
        self._names = names
        self._scopes = scopes
        self._env = env

    def build(self) -> Any:
        from workhold_mcp.http_app import build_auth_settings, build_http_app

        server = build_server(
            load_settings(_env(self._env), names=self._names),
            self._names,
            token_verifier=_ScopedVerifier(self._scopes),
            auth=build_auth_settings(RESOURCE, ISSUER),
        )
        return build_http_app(
            server,
            resource_url=RESOURCE,
            issuer_url=ISSUER,
            security=_security(),
        )


def _http_server(
    monkeypatch: pytest.MonkeyPatch,
    names: frozenset[str],
    scopes: list[str],
    steps: list[Any],
    env: dict[str, str] | None = None,
) -> tuple[_HttpApp, list[ScriptedSyncTransport]]:
    built = _patch_transports(monkeypatch, steps)
    return _HttpApp(names, scopes, env), built


def _stdio_server(
    monkeypatch: pytest.MonkeyPatch,
    names: frozenset[str],
    steps: list[Any],
    env: dict[str, str] | None = None,
) -> tuple[Any, list[ScriptedSyncTransport]]:
    built = _patch_transports(monkeypatch, steps)
    return build_server(load_settings(_env(env), names=names), names), built


def _rpc(method: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
    return {"jsonrpc": "2.0", "id": 1, "method": method, "params": params or {}}


def _post(app: _HttpApp, method: str, params: dict[str, Any] | None = None) -> Any:
    with TestClient(app.build(), base_url=BASE_URL) as client:
        return client.post(
            "/mcp",
            headers={**HEADERS, "Authorization": f"Bearer {CALLER_JWT}"},
            json=_rpc(method, params),
        )


def _list(app: Any) -> list[str]:
    response = _post(app, "tools/list")
    assert response.status_code == 200
    return [tool["name"] for tool in response.json()["result"]["tools"]]


def _call(app: Any, name: str, arguments: dict[str, Any]) -> dict[str, Any]:
    response = _post(app, "tools/call", {"name": name, "arguments": arguments})
    assert response.status_code == 200
    return response.json()["result"]


def _calls(transports: list[ScriptedSyncTransport]) -> list[Any]:
    return [call for transport in transports for call in transport.calls]


def _bearer(call: Any) -> str:
    return call.headers(include_secrets=True)["Authorization"]


def _assert_no_caller_jwt(calls: list[Any]) -> None:
    for call in calls:
        for value in call.headers(include_secrets=True).values():
            assert CALLER_JWT not in value


def _handler(server: Any, name: str) -> Any:
    for tool in server._tool_manager.list_tools():
        if tool.name == name:
            return tool.fn
    raise AssertionError(name)


def _assert_insufficient_scope(result: dict[str, Any]) -> None:
    assert result["isError"] is True
    payload = result["structuredContent"]
    assert set(payload) == _SIX_FIELDS
    assert payload["kind"] == "insufficient_scope"
    for key in _SIX_FIELDS - {"kind"}:
        assert payload[key] is None


def _others(role: str) -> list[str]:
    return [other for other in _ROLES if other != role]


def test_required_scope_is_a_function_of_manifest_role_only() -> None:
    from workhold_mcp.scopes import required_scope

    assert required_scope("observer") == READ
    assert required_scope("producer") == PRODUCE
    assert required_scope("admin") == OPERATE
    for preset in ("operator", "routine"):
        with pytest.raises(ValueError):
            required_scope(preset)
    for row in load_manifest():
        assert required_scope(row.role) == _SCOPE_BY_ROLE[row.role], row.name


@pytest.mark.parametrize("role", _ROLES)
def test_unselected_tool_is_absent_and_never_called(
    monkeypatch: pytest.MonkeyPatch, role: str
) -> None:
    names = frozenset(_TOOL[other][0] for other in _others(role))
    tool, arguments = _TOOL[role]
    app, built = _http_server(monkeypatch, names, ALL_SCOPES, [_success_step(role)])
    assert tool not in _list(app)
    result = _call(app, tool, arguments)
    assert result["isError"] is True
    assert _calls(built) == []


@pytest.mark.parametrize("role", _ROLES)
def test_selected_tool_without_its_scope_is_insufficient_scope(
    monkeypatch: pytest.MonkeyPatch, role: str
) -> None:
    scopes = [_SCOPE_BY_ROLE[other] for other in _others(role)]
    tool, arguments = _TOOL[role]
    app, built = _http_server(monkeypatch, _ALL_TOOL_NAMES, scopes, [_success_step(role)])
    assert tool not in _list(app)
    _assert_insufficient_scope(_call(app, tool, arguments))
    assert _calls(built) == []


@pytest.mark.parametrize("role", _ROLES)
def test_selected_tool_with_no_scope_at_all_is_insufficient_scope(
    monkeypatch: pytest.MonkeyPatch, role: str
) -> None:
    tool, arguments = _TOOL[role]
    app, built = _http_server(monkeypatch, _ALL_TOOL_NAMES, [], [_success_step(role)])
    assert _list(app) == []
    _assert_insufficient_scope(_call(app, tool, arguments))
    assert _calls(built) == []


@pytest.mark.parametrize("role", _ROLES)
def test_selected_tool_with_its_scope_sends_one_role_bearer_call(
    monkeypatch: pytest.MonkeyPatch, role: str
) -> None:
    tool, arguments = _TOOL[role]
    app, built = _http_server(
        monkeypatch, _ALL_TOOL_NAMES, [_SCOPE_BY_ROLE[role]], [_success_step(role)]
    )
    assert tool in _list(app)
    result = _call(app, tool, arguments)
    assert result["isError"] is False
    calls = _calls(built)
    assert len(calls) == 1
    assert _bearer(calls[0]) == f"Bearer {_TOKENS[role]}"
    _assert_no_caller_jwt(calls)


@pytest.mark.parametrize("role", _ROLES)
def test_request_uses_the_token_configured_for_the_tool_role(
    monkeypatch: pytest.MonkeyPatch, role: str
) -> None:
    wrong = _TOKENS[_others(role)[0]]
    tool, arguments = _TOOL[role]
    app, built = _http_server(
        monkeypatch,
        _ALL_TOOL_NAMES,
        ALL_SCOPES,
        [_success_step(role)],
        env={_TOKEN_ENV[role]: wrong},
    )
    result = _call(app, tool, arguments)
    assert result["isError"] is False
    calls = _calls(built)
    assert len(calls) == 1
    assert _bearer(calls[0]) == f"Bearer {wrong}"
    _assert_no_caller_jwt(calls)


@pytest.mark.parametrize("role", _ROLES)
def test_matching_scope_does_not_hide_permission_denied(
    monkeypatch: pytest.MonkeyPatch, role: str
) -> None:
    tool, arguments = _TOOL[role]
    app, built = _http_server(
        monkeypatch, _ALL_TOOL_NAMES, [_SCOPE_BY_ROLE[role]], [_denied()]
    )
    result = _call(app, tool, arguments)
    assert result["isError"] is True
    payload = result["structuredContent"]
    assert set(payload) == _SIX_FIELDS
    assert payload["kind"] == "protocol"
    assert payload["code"] == "permission_denied"
    calls = _calls(built)
    assert len(calls) == 1
    assert _bearer(calls[0]) == f"Bearer {_TOKENS[role]}"
    _assert_no_caller_jwt(calls)


@pytest.mark.parametrize("role", _ROLES)
def test_all_three_scopes_match_the_stdio_result(
    monkeypatch: pytest.MonkeyPatch, role: str
) -> None:
    tool, arguments = _TOOL[role]
    stdio, stdio_built = _stdio_server(
        monkeypatch, _ALL_TOOL_NAMES, [_success_step(role)]
    )
    expected = _handler(stdio, tool)(**arguments)
    app, built = _http_server(
        monkeypatch, _ALL_TOOL_NAMES, ALL_SCOPES, [_success_step(role)]
    )
    result = _call(app, tool, arguments)
    assert result["isError"] is expected.is_error is False
    assert result["structuredContent"] == expected.structured_content
    assert len(_calls(built)) == len(_calls(stdio_built)) == 1


@pytest.mark.parametrize("role", _ROLES)
def test_unrelated_scope_does_not_change_the_workhold_bearer(
    monkeypatch: pytest.MonkeyPatch, role: str
) -> None:
    tool, arguments = _TOOL[role]
    app, built = _http_server(
        monkeypatch,
        _ALL_TOOL_NAMES,
        [UNRELATED, _SCOPE_BY_ROLE[role]],
        [_success_step(role)],
    )
    result = _call(app, tool, arguments)
    assert result["isError"] is False
    calls = _calls(built)
    assert len(calls) == 1
    assert _bearer(calls[0]) == f"Bearer {_TOKENS[role]}"
    _assert_no_caller_jwt(calls)


def test_unrelated_scope_alone_grants_nothing(monkeypatch: pytest.MonkeyPatch) -> None:
    app, built = _http_server(monkeypatch, _ALL_TOOL_NAMES, [UNRELATED], [])
    assert _list(app) == []
    for tool, arguments in _TOOL.values():
        _assert_insufficient_scope(_call(app, tool, arguments))
    assert _calls(built) == []


def test_list_is_the_intersection_of_policy_and_scopes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    observer_policy = frozenset(preset_names("observer"))
    app, _ = _http_server(monkeypatch, observer_policy, ALL_SCOPES, [])
    assert _list(app) == sorted(observer_policy)

    app, _ = _http_server(monkeypatch, _ALL_TOOL_NAMES, [READ, PRODUCE], [])
    assert _list(app) == ["observer_get_task", "producer_enqueue"]


def test_observer_only_policy_with_operate_scope_hides_admin_tools(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    names = frozenset(preset_names("observer"))
    app, built = _http_server(monkeypatch, names, [OPERATE], [_success_step("admin")])
    assert _list(app) == []
    result = _call(app, "admin_list_queues", {})
    assert result["isError"] is True
    _assert_insufficient_scope(_call(app, "observer_get_task", {"task_id": _TASK_ID}))
    calls = _calls(built)
    assert calls == []
    for call in calls:
        assert _ADMIN not in _bearer(call)


def test_custom_allowlist_does_not_retag_admin_create_queue(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    known = frozenset(row.name for row in load_manifest())
    names = resolve_tool_policy(
        ("--allow-tool", "admin_create_queue,observer_get_task"), _env(), known
    )
    assert "admin_create_queue" in names
    app, built = _http_server(monkeypatch, names, [READ], [_success_step("admin")])
    assert _list(app) == ["observer_get_task"]
    _assert_insufficient_scope(
        _call(
            app,
            "admin_create_queue",
            {
                "name": "orders",
                "initial_policy": {},
                "idempotency_key": "idem-scope-2",
            },
        )
    )
    assert _calls(built) == []


async def test_missing_http_access_token_lists_nothing_and_denies_calls(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from workhold_mcp.http_app import build_auth_settings

    built = _patch_transports(monkeypatch, [_success_step("observer")])
    server = build_server(
        load_settings(_env(), names=_ALL_TOOL_NAMES),
        _ALL_TOOL_NAMES,
        token_verifier=_ScopedVerifier(ALL_SCOPES),
        auth=build_auth_settings(RESOURCE, ISSUER),
    )
    assert await server.list_tools() == []
    result = await server.call_tool("observer_get_task", {"task_id": _TASK_ID})
    assert result.is_error is True
    assert result.structured_content["kind"] == "insufficient_scope"
    assert _calls(built) == []


@pytest.mark.parametrize("role", _ROLES)
async def test_stdio_selected_tool_needs_no_mcp_bearer_or_http_settings(
    monkeypatch: pytest.MonkeyPatch, role: str
) -> None:
    import mcp.server.auth.middleware.auth_context as auth_context

    def _forbidden() -> None:
        raise AssertionError("stdio read the OAuth context")

    monkeypatch.setattr(auth_context, "get_access_token", _forbidden)
    monkeypatch.setattr("workhold_mcp.server.get_access_token", _forbidden, raising=False)
    tool, arguments = _TOOL[role]
    server, built = _stdio_server(monkeypatch, _ALL_TOOL_NAMES, [_success_step(role)])
    assert sorted(item.name for item in await server.list_tools()) == sorted(_ALL_TOOL_NAMES)
    result = _handler(server, tool)(**arguments)
    assert result.is_error is False
    calls = _calls(built)
    assert len(calls) == 1
    assert _bearer(calls[0]) == f"Bearer {_TOKENS[role]}"
    _assert_no_caller_jwt(calls)


@pytest.mark.parametrize("role", _ROLES)
async def test_stdio_unselected_tool_is_absent_and_never_called(
    monkeypatch: pytest.MonkeyPatch, role: str
) -> None:
    import mcp.server.auth.middleware.auth_context as auth_context

    def _forbidden() -> None:
        raise AssertionError("stdio read the OAuth context")

    monkeypatch.setattr(auth_context, "get_access_token", _forbidden)
    monkeypatch.setattr("workhold_mcp.server.get_access_token", _forbidden, raising=False)
    names = frozenset(_TOOL[other][0] for other in _others(role))
    tool, arguments = _TOOL[role]
    server, built = _stdio_server(monkeypatch, names, [_success_step(role)])
    assert tool not in [item.name for item in await server.list_tools()]
    with pytest.raises(ToolError):
        await server.call_tool(tool, arguments)
    assert _calls(built) == []


def test_handlers_never_forward_the_caller_token() -> None:
    for path in MCP_SRC.rglob("*.py"):
        text = path.read_text(encoding="utf-8")
        assert re.search(r"\.token\b", text) is None, path
        assert "WORKHOLD_CONTENT_KEY" not in text, path
    scopes_text = (MCP_SRC / "workhold_mcp" / "scopes.py").read_text(encoding="utf-8")
    assert "workhold-mcp:operate" in scopes_text
