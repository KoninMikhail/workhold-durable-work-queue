"""The outer Host and Origin guard rejects before the inner app and the token verifier."""

from __future__ import annotations

import asyncio
from typing import Any

from mcp.server.auth.provider import AccessToken
from mcp.server.transport_security import TransportSecuritySettings
from starlette.testclient import TestClient

from workhold_mcp.config import load_settings
from workhold_mcp.registry import preset_names
from workhold_mcp.server import build_server

RESOURCE = "https://mcp.example.invalid/mcp"
ISSUER = "https://idp.example.invalid"
BASE_URL = "http://127.0.0.1:8000"
GOOD_TOKEN = "static-good-token"
ALLOWED_ORIGIN = "https://app.example.invalid"
HEADERS = {
    "Accept": "application/json, text/event-stream",
    "Content-Type": "application/json",
}
NAMES = frozenset(preset_names("observer"))
MCP_ENV = {
    "WORKHOLD_MCP_PUBLIC_BASE_URL": "http://127.0.0.1:9",
    "WORKHOLD_MCP_ADMIN_BASE_URL": "http://127.0.0.1:10",
    "WORKHOLD_MCP_OBSERVER_TOKEN": "observer-token-canary",
}


class _InnerSpy:
    """One-shot inner ASGI app that records every scope it receives."""

    def __init__(self) -> None:
        self.scopes: list[str] = []

    async def __call__(self, scope: Any, receive: Any, send: Any) -> None:
        self.scopes.append(scope["type"])
        if scope["type"] != "http":
            return
        await send({"type": "http.response.start", "status": 204, "headers": []})
        await send({"type": "http.response.body", "body": b"", "more_body": False})


class _SpyVerifier:
    def __init__(self) -> None:
        self.tokens: list[str] = []

    async def verify_token(self, token: str) -> AccessToken | None:
        self.tokens.append(token)
        if token != GOOD_TOKEN:
            return None
        return AccessToken(
            token=token,
            client_id="client-1",
            scopes=["workhold-mcp:read"],
            resource=RESOURCE,
        )


def _security(
    hosts: list[str],
    origins: list[str] | None = None,
) -> TransportSecuritySettings:
    return TransportSecuritySettings(
        enable_dns_rebinding_protection=True,
        allowed_hosts=hosts,
        allowed_origins=origins or [],
    )


def _guard(inner: _InnerSpy, security: TransportSecuritySettings) -> Any:
    from workhold_mcp.transport_guard import HostOriginGuard

    return HostOriginGuard(inner, security)


def _http_scope(headers: list[tuple[str, str]]) -> dict[str, Any]:
    return {
        "type": "http",
        "asgi": {"version": "3.0"},
        "http_version": "1.1",
        "method": "POST",
        "path": "/mcp",
        "headers": [(k.lower().encode("latin-1"), v.encode("latin-1")) for k, v in headers],
        "client": ("127.0.0.1", 9),
        "server": ("127.0.0.1", 8000),
        "scheme": "http",
    }


def _call(app: Any, scope: dict[str, Any]) -> tuple[int | None, bytes]:
    sent: list[dict[str, Any]] = []

    async def receive() -> dict[str, Any]:
        return {"type": "http.request", "body": b"", "more_body": False}

    async def send(message: dict[str, Any]) -> None:
        sent.append(message)

    asyncio.run(app(scope, receive, send))
    status = next((m["status"] for m in sent if m["type"] == "http.response.start"), None)
    body = b"".join(m.get("body", b"") for m in sent if m["type"] == "http.response.body")
    return status, body


def test_disallowed_host_does_not_call_inner() -> None:
    inner = _InnerSpy()
    guard = _guard(inner, _security(["127.0.0.1:8000"]))

    status, body = _call(guard, _http_scope([("Host", "evil.example")]))

    assert status == 421
    assert body == b"Invalid Host header"
    assert inner.scopes == []


def test_missing_host_does_not_call_inner() -> None:
    inner = _InnerSpy()
    guard = _guard(inner, _security(["127.0.0.1:8000"]))

    status, body = _call(guard, _http_scope([]))

    assert status == 421
    assert body == b"Invalid Host header"
    assert inner.scopes == []


def test_disallowed_origin_does_not_call_inner() -> None:
    inner = _InnerSpy()
    guard = _guard(inner, _security(["127.0.0.1:8000"], [ALLOWED_ORIGIN]))

    status, body = _call(
        guard,
        _http_scope([("Host", "127.0.0.1:8000"), ("Origin", "https://evil.example")]),
    )
    assert status == 403
    assert body == b"Invalid Origin header"
    assert inner.scopes == []

    status, _ = _call(guard, _http_scope([("Host", "127.0.0.1:8000")]))
    assert status == 204
    assert inner.scopes == ["http"]

    status, _ = _call(
        guard,
        _http_scope([("Host", "127.0.0.1:8000"), ("Origin", ALLOWED_ORIGIN)]),
    )
    assert status == 204
    assert inner.scopes == ["http", "http"]


def test_port_and_wildcard_match() -> None:
    inner = _InnerSpy()
    exact = _guard(inner, _security(["127.0.0.1:8000"]))

    status, body = _call(exact, _http_scope([("Host", "127.0.0.1")]))
    assert status == 421
    assert body == b"Invalid Host header"
    assert inner.scopes == []

    wildcard = _guard(inner, _security(["127.0.0.1:*"]))
    status, _ = _call(wildcard, _http_scope([("Host", "127.0.0.1:8000")]))
    assert status == 204
    assert inner.scopes == ["http"]

    status, _ = _call(wildcard, _http_scope([("Host", "127.0.0.1")]))
    assert status == 421
    assert inner.scopes == ["http"]


def test_no_browser_mode() -> None:
    inner = _InnerSpy()
    guard = _guard(inner, _security(["127.0.0.1:8000"], []))

    status, _ = _call(guard, _http_scope([("Host", "127.0.0.1:8000")]))
    assert status == 204
    assert inner.scopes == ["http"]

    status, body = _call(
        guard,
        _http_scope(
            [("Host", "127.0.0.1:8000"), ("Origin", "https://app.example.invalid")]
        ),
    )
    assert status == 403
    assert body == b"Invalid Origin header"
    assert inner.scopes == ["http"]


def test_lifespan_is_forwarded() -> None:
    inner = _InnerSpy()
    guard = _guard(inner, _security(["127.0.0.1:8000"]))

    async def receive() -> dict[str, Any]:
        return {"type": "lifespan.startup"}

    async def send(message: dict[str, Any]) -> None:
        raise AssertionError(f"unexpected send {message}")

    asyncio.run(guard({"type": "lifespan"}, receive, send))
    assert inner.scopes == ["lifespan"]

    status, _ = _call(guard, _http_scope([("Host", "evil.example")]))
    assert status == 421
    assert inner.scopes == ["lifespan"]


def _composed_app(verifier: _SpyVerifier, security: TransportSecuritySettings) -> Any:
    from workhold_mcp.http_app import build_auth_settings, build_http_app

    server = build_server(
        load_settings(MCP_ENV, names=NAMES),
        NAMES,
        token_verifier=verifier,
        auth=build_auth_settings(RESOURCE, ISSUER),
    )
    return build_http_app(
        server,
        resource_url=RESOURCE,
        issuer_url=ISSUER,
        security=security,
    )


def _tools_list(client: TestClient, extra: dict[str, str]) -> Any:
    return client.post(
        "/mcp",
        headers={**HEADERS, "Authorization": f"Bearer {GOOD_TOKEN}", **extra},
        json={"jsonrpc": "2.0", "id": 1, "method": "tools/list", "params": {}},
    )


def test_spy_verifier_on_composed_app() -> None:
    verifier = _SpyVerifier()
    app = _composed_app(verifier, _security(["127.0.0.1:8000"], [ALLOWED_ORIGIN]))

    with TestClient(app, base_url=BASE_URL) as client:
        bad_host = _tools_list(client, {"Host": "evil.example"})
        assert bad_host.status_code == 421
        assert bad_host.text == "Invalid Host header"
        assert verifier.tokens == []

        bad_origin = _tools_list(client, {"Origin": "https://evil.example"})
        assert bad_origin.status_code == 403
        assert bad_origin.text == "Invalid Origin header"
        assert verifier.tokens == []

        allowed = _tools_list(client, {"Origin": ALLOWED_ORIGIN})
        assert allowed.status_code == 200
        assert allowed.json()["result"]["tools"]
        assert verifier.tokens == [GOOD_TOKEN]
