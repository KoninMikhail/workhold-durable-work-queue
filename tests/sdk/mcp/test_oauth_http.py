"""Bearer authentication on ``/mcp``: scopes are never AND-gated, and skew is configured."""

from __future__ import annotations

import time
from typing import Any

import pytest
import uvicorn
from mcp.server.auth.provider import AccessToken
from mcp.server.transport_security import TransportSecuritySettings
from starlette.testclient import TestClient

from _workhold_client_core.transport import HttpJsonTransport
from tests.sdk.mcp.jwks_fixture import KeyMaterial, base_claims, make_rsa_key, mint
from workhold_client_testing import ScriptedSyncTransport, ok, task_body
from workhold_mcp.config import load_settings, resolve_http_config
from workhold_mcp.registry import preset_names
from workhold_mcp.server import build_server

RESOURCE = "https://mcp.example.invalid/mcp"
ISSUER = "https://idp.example.invalid"
JWKS = "https://idp.example.invalid/jwks"
BASE_URL = "http://127.0.0.1:8000"
GOOD_TOKEN = "static-good-token"
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


class _FakeJwkClient:
    urls: list[str] = []
    key: KeyMaterial

    def __init__(self, url: str) -> None:
        type(self).urls.append(url)

    def get_signing_key_from_jwt(self, token: str) -> Any:
        return type(self).key.pyjwk()


def _security() -> TransportSecuritySettings:
    return TransportSecuritySettings(
        enable_dns_rebinding_protection=True,
        allowed_hosts=["127.0.0.1:8000"],
        allowed_origins=[],
    )


def _app(verifier: Any) -> Any:
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
        security=_security(),
    )


def _rpc(method: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
    return {"jsonrpc": "2.0", "id": 1, "method": method, "params": params or {}}


def _scripted(monkeypatch: pytest.MonkeyPatch) -> ScriptedSyncTransport:
    scripted = ScriptedSyncTransport([ok(task_body())])
    monkeypatch.setattr(
        HttpJsonTransport,
        "from_config",
        classmethod(lambda cls, config: scripted),
    )
    return scripted


def _expired_token(key: KeyMaterial, seconds_ago: int) -> str:
    now = int(time.time())
    return mint(
        key,
        base_claims(
            issuer=ISSUER,
            resource=RESOURCE,
            now=now - 600,
            aud=[RESOURCE],
            exp=now - seconds_ago,
        ),
    )


def test_read_token_is_not_and_gated() -> None:
    verifier = _StaticVerifier(["workhold-mcp:read"])
    with TestClient(_app(verifier), base_url=BASE_URL) as client:
        response = client.post(
            "/mcp",
            headers={**HEADERS, "Authorization": f"Bearer {GOOD_TOKEN}"},
            json=_rpc("tools/list"),
        )
    assert response.status_code != 403
    assert response.status_code == 200
    assert response.json()["result"]["tools"]
    assert verifier.tokens == [GOOD_TOKEN]


@pytest.mark.parametrize(
    "authorization",
    [None, "Basic dXNlcjpwYXNz", "Bearer not-the-good-token", "Bearer"],
)
def test_missing_and_malformed_tokens_are_401(
    monkeypatch: pytest.MonkeyPatch,
    authorization: str | None,
) -> None:
    scripted = _scripted(monkeypatch)
    headers = dict(HEADERS)
    if authorization is not None:
        headers["Authorization"] = authorization
    with TestClient(_app(_StaticVerifier(["workhold-mcp:read"])), base_url=BASE_URL) as client:
        response = client.post(
            "/mcp",
            headers=headers,
            json=_rpc(
                "tools/call",
                {
                    "name": "observer_get_task",
                    "arguments": {"task_id": "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"},
                },
            ),
        )
    assert response.status_code == 401
    assert response.json()["error_description"] == "Authentication required"
    assert "resource_metadata=" in response.headers["www-authenticate"]
    assert len(scripted.calls) == 0


def test_skew_survives_middleware() -> None:
    from workhold_mcp.jwt_verifier import JwtJwksTokenVerifier

    key = make_rsa_key()
    verifier = JwtJwksTokenVerifier(
        issuer=ISSUER,
        resource=RESOURCE,
        key_for_token=lambda token: key.pyjwk(),
        clock_skew_seconds=60,
    )
    with TestClient(_app(verifier), base_url=BASE_URL) as client:
        within = client.post(
            "/mcp",
            headers={**HEADERS, "Authorization": f"Bearer {_expired_token(key, 30)}"},
            json=_rpc("tools/list"),
        )
        beyond = client.post(
            "/mcp",
            headers={**HEADERS, "Authorization": f"Bearer {_expired_token(key, 120)}"},
            json=_rpc("tools/list"),
        )
    assert within.status_code == 200
    assert beyond.status_code == 401


@pytest.mark.parametrize(("skew", "expected_status"), [("0", 401), ("60", 200)])
def test_configured_clock_skew_reaches_serve_streamable_http(
    monkeypatch: pytest.MonkeyPatch,
    skew: str,
    expected_status: int,
) -> None:
    from workhold_mcp import http_app

    key = make_rsa_key()
    _FakeJwkClient.urls = []
    _FakeJwkClient.key = key
    monkeypatch.setattr(http_app, "PyJWKClient", _FakeJwkClient)
    captured: list[uvicorn.Config] = []

    async def _fake_serve(self: uvicorn.Server, sockets: Any = None) -> None:
        captured.append(self.config)

    monkeypatch.setattr(uvicorn.Server, "serve", _fake_serve)
    settings = resolve_http_config(
        [],
        {
            "WORKHOLD_MCP_TRANSPORT": "streamable-http",
            "WORKHOLD_MCP_RESOURCE_URL": RESOURCE,
            "WORKHOLD_MCP_ISSUER_URL": ISSUER,
            "WORKHOLD_MCP_JWKS_URL": JWKS,
            "WORKHOLD_MCP_ALLOWED_HOSTS": "127.0.0.1:8000",
            "WORKHOLD_MCP_NO_BROWSER_ORIGINS": "true",
            "WORKHOLD_MCP_CLOCK_SKEW_SECONDS": skew,
        },
    )
    assert settings.clock_skew_seconds == int(skew)

    http_app.serve_streamable_http(settings, load_settings(MCP_ENV, names=NAMES), NAMES)

    assert _FakeJwkClient.urls == [settings.jwks_url]
    assert len(captured) == 1
    with TestClient(captured[0].app, base_url=BASE_URL) as client:
        response = client.post(
            "/mcp",
            headers={**HEADERS, "Authorization": f"Bearer {_expired_token(key, 30)}"},
            json=_rpc("tools/list"),
        )
    assert response.status_code == expected_status
