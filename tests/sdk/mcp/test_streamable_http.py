"""Stateless JSON ``/mcp`` served from the one shared ``build_server`` registration."""

from __future__ import annotations

import ast
import time
from pathlib import Path
from typing import Any

import pytest
import uvicorn
from mcp.server.auth.provider import AccessToken
from mcp.server.transport_security import TransportSecuritySettings
from starlette.testclient import TestClient

from _workhold_client_core.transport import HttpJsonTransport
from tests.sdk.mcp.jwks_fixture import base_claims, make_rsa_key, mint
from workhold_client_testing import ScriptedSyncTransport, ok, task_body
from workhold_mcp.config import load_settings, resolve_http_config
from workhold_mcp.registry import preset_names
from workhold_mcp.server import build_server

RESOURCE = "https://mcp.example.invalid/mcp"
ISSUER = "https://idp.example.invalid"
JWKS = "https://idp.example.invalid/jwks"
BASE_URL = "http://127.0.0.1:8000"
GOOD_TOKEN = "static-good-token"
TASK_ID = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
HEADERS = {
    "Accept": "application/json, text/event-stream",
    "Content-Type": "application/json",
}
MCP_SRC = Path(__file__).resolve().parents[3] / "packages" / "workhold-mcp" / "src"
OBSERVER_NAMES = frozenset(preset_names("observer"))
OMITTED = "observer_get_stats"
PARTIAL_NAMES = OBSERVER_NAMES - {OMITTED}
MCP_ENV = {
    "WORKHOLD_MCP_PUBLIC_BASE_URL": "http://127.0.0.1:9",
    "WORKHOLD_MCP_ADMIN_BASE_URL": "http://127.0.0.1:10",
    "WORKHOLD_MCP_OBSERVER_TOKEN": "observer-token-canary",
}


class _StaticVerifier:
    def __init__(self, scopes: list[str]) -> None:
        self.scopes = scopes
        self.tokens: list[str] = []

    async def verify_token(self, token: str) -> AccessToken | None:
        self.tokens.append(token)
        if token != GOOD_TOKEN:
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


def _injected_app(verifier: Any, names: frozenset[str] = OBSERVER_NAMES) -> Any:
    from workhold_mcp.http_app import build_auth_settings, build_http_app

    server = build_server(
        load_settings(MCP_ENV, names=names),
        names,
        token_verifier=verifier,
        auth=build_auth_settings(RESOURCE, ISSUER),
    )
    return build_http_app(
        server,
        resource_url=RESOURCE,
        issuer_url=ISSUER,
        security=_security(),
    )


def _rpc(method: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
    return {"jsonrpc": "2.0", "id": 1, "method": method, "params": params or {}}


def _bearer(token: str = GOOD_TOKEN) -> dict[str, str]:
    return {**HEADERS, "Authorization": f"Bearer {token}"}


def _http_env(**extra: str) -> dict[str, str]:
    env = {
        "WORKHOLD_MCP_TRANSPORT": "streamable-http",
        "WORKHOLD_MCP_RESOURCE_URL": RESOURCE,
        "WORKHOLD_MCP_ISSUER_URL": ISSUER,
        "WORKHOLD_MCP_JWKS_URL": JWKS,
        "WORKHOLD_MCP_ALLOWED_HOSTS": "127.0.0.1:8000",
        "WORKHOLD_MCP_NO_BROWSER_ORIGINS": "true",
    }
    env.update(extra)
    return env


class _FakeJwkClient:
    urls: list[str] = []
    key: Any

    def __init__(self, url: str) -> None:
        type(self).urls.append(url)

    def get_signing_key_from_jwt(self, token: str) -> Any:
        return type(self).key.pyjwk()


def _serve_and_capture(
    monkeypatch: pytest.MonkeyPatch,
    env: dict[str, str],
    names: frozenset[str],
) -> uvicorn.Config:
    from workhold_mcp import http_app

    key = make_rsa_key()
    _FakeJwkClient.urls = []
    _FakeJwkClient.key = key
    monkeypatch.setattr(http_app, "PyJWKClient", _FakeJwkClient)
    captured: list[uvicorn.Config] = []

    async def _fake_serve(self: uvicorn.Server, sockets: Any = None) -> None:
        captured.append(self.config)

    monkeypatch.setattr(uvicorn.Server, "serve", _fake_serve)
    http = resolve_http_config([], env)
    http_app.serve_streamable_http(http, load_settings(MCP_ENV, names=names), names)
    assert len(captured) == 1
    return captured[0]


def test_sse_and_auth_server_routes_are_absent() -> None:
    app = _injected_app(_StaticVerifier(["workhold-mcp:read"]))
    with TestClient(app, base_url=BASE_URL) as client:
        for path in ("/sse", "/messages", "/authorize", "/token"):
            assert client.get(path).status_code == 404, path


def test_json_tool_call(monkeypatch: pytest.MonkeyPatch) -> None:
    scripted = ScriptedSyncTransport([ok(task_body())])
    monkeypatch.setattr(
        HttpJsonTransport,
        "from_config",
        classmethod(lambda cls, config: scripted),
    )
    app = _injected_app(_StaticVerifier(["workhold-mcp:read"]))
    with TestClient(app, base_url=BASE_URL) as client:
        response = client.post(
            "/mcp",
            headers=_bearer(),
            json=_rpc(
                "tools/call",
                {"name": "observer_get_task", "arguments": {"task_id": TASK_ID}},
            ),
        )
    assert response.status_code == 200
    assert response.headers["content-type"].startswith("application/json")
    body = response.json()
    assert body["id"] == 1
    assert body["result"]["isError"] is False
    assert body["result"]["structuredContent"]["task_id"] == TASK_ID
    assert len(scripted.calls) == 1
    assert "Bearer observer-token-canary" == scripted.calls[0].headers(
        include_secrets=True
    )["Authorization"]


def test_proxy_headers_follow_the_allowlist(monkeypatch: pytest.MonkeyPatch) -> None:
    config = _serve_and_capture(monkeypatch, _http_env(), OBSERVER_NAMES)
    assert config.proxy_headers is False
    assert list(config.forwarded_allow_ips) == []
    assert config.host == "127.0.0.1"
    assert config.port == 8000

    config = _serve_and_capture(
        monkeypatch,
        _http_env(
            WORKHOLD_MCP_FORWARDED_ALLOW_IPS="10.0.0.1,10.0.0.2",
            WORKHOLD_MCP_BIND_HOST="127.0.0.2",
            WORKHOLD_MCP_BIND_PORT="8123",
        ),
        OBSERVER_NAMES,
    )
    assert config.proxy_headers is True
    assert list(config.forwarded_allow_ips) == ["10.0.0.1", "10.0.0.2"]
    assert "*" not in config.forwarded_allow_ips
    assert config.host == "127.0.0.2"
    assert config.port == 8123

    for path in MCP_SRC.rglob("*.py"):
        text = path.read_text(encoding="utf-8")
        assert "run_streamable_http_async" not in text, path
        assert "sse_app" not in text, path
        assert "auth_server_provider" not in text, path


def test_http_serves_build_server_registration(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from workhold_mcp import http_app

    assert OMITTED in OBSERVER_NAMES
    built: list[dict[str, Any]] = []
    real_build_server = http_app.build_server

    def _spy_build_server(settings: Any, names: Any, **kwargs: Any) -> Any:
        server = real_build_server(settings, names, **kwargs)
        built.append({"kwargs": kwargs, "server": server})
        return server

    served: list[Any] = []
    real_build_http_app = http_app.build_http_app

    def _spy_build_http_app(mcp: Any, **kwargs: Any) -> Any:
        served.append(mcp)
        return real_build_http_app(mcp, **kwargs)

    monkeypatch.setattr(http_app, "build_server", _spy_build_server)
    monkeypatch.setattr(http_app, "build_http_app", _spy_build_http_app)
    config = _serve_and_capture(monkeypatch, _http_env(), PARTIAL_NAMES)

    assert len(built) == 1
    assert set(built[0]["kwargs"]) == {"token_verifier", "auth"}
    assert built[0]["kwargs"]["auth"].required_scopes == []
    assert served == [built[0]["server"]]

    now = int(time.time())
    token = mint(
        _FakeJwkClient.key,
        base_claims(issuer=ISSUER, resource=RESOURCE, now=now, aud=[RESOURCE]),
    )
    with TestClient(config.app, base_url=BASE_URL) as client:
        response = client.post(
            "/mcp",
            headers=_bearer(token),
            json=_rpc("tools/list"),
        )
    assert response.status_code == 200
    listed = [tool["name"] for tool in response.json()["result"]["tools"]]
    assert listed == sorted(PARTIAL_NAMES)
    assert OMITTED not in listed


def test_one_mcp_server_constructor_inside_build_server() -> None:
    sites: list[tuple[str, str]] = []
    for path in MCP_SRC.rglob("*.py"):
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for function in ast.walk(tree):
            if not isinstance(function, ast.FunctionDef):
                continue
            for node in ast.walk(function):
                if (
                    isinstance(node, ast.Call)
                    and isinstance(node.func, ast.Name)
                    and node.func.id == "MCPServer"
                ):
                    sites.append((path.name, function.name))
    assert sorted(set(sites)) == [("server.py", "build_server")]
    http_source = (MCP_SRC / "workhold_mcp" / "http_app.py").read_text(encoding="utf-8")
    assert "MCPServer(" not in http_source
