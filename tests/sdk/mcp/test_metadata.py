"""Protected-resource metadata lists the resource, issuer, and all three scopes."""

from __future__ import annotations

from mcp.server.transport_security import TransportSecuritySettings
from starlette.testclient import TestClient

from workhold_mcp.config import load_settings
from workhold_mcp.registry import preset_names
from workhold_mcp.server import build_server

RESOURCE = "https://mcp.example.invalid/mcp"
ISSUER = "https://idp.example.invalid/realms/workhold"
WELL_KNOWN = "/.well-known/oauth-protected-resource/mcp"
SCOPES = ["workhold-mcp:read", "workhold-mcp:produce", "workhold-mcp:operate"]
NAMES = frozenset(preset_names("observer"))


class _RejectEverything:
    def __init__(self) -> None:
        self.calls = 0

    async def verify_token(self, token: str) -> None:
        self.calls += 1
        return None


def _app(verifier: _RejectEverything) -> object:
    from workhold_mcp.http_app import build_auth_settings, build_http_app

    server = build_server(
        load_settings(
            {
                "WORKHOLD_MCP_PUBLIC_BASE_URL": "http://127.0.0.1:9",
                "WORKHOLD_MCP_ADMIN_BASE_URL": "http://127.0.0.1:10",
                "WORKHOLD_MCP_OBSERVER_TOKEN": "observer-token",
            },
            names=NAMES,
        ),
        NAMES,
        token_verifier=verifier,
        auth=build_auth_settings(RESOURCE, ISSUER),
    )
    return build_http_app(
        server,
        resource_url=RESOURCE,
        issuer_url=ISSUER,
        security=TransportSecuritySettings(
            enable_dns_rebinding_protection=True,
            allowed_hosts=["127.0.0.1:8000"],
            allowed_origins=[],
        ),
    )


def test_metadata_lists_all_scopes() -> None:
    verifier = _RejectEverything()
    with TestClient(_app(verifier), base_url="http://127.0.0.1:8000") as client:
        response = client.get(WELL_KNOWN)
    assert response.status_code == 200
    body = response.json()
    assert body["resource"] == RESOURCE
    assert body["authorization_servers"] == [ISSUER]
    assert body["scopes_supported"] == SCOPES
    assert "header" in body["bearer_methods_supported"]
    assert verifier.calls == 0


def test_metadata_route_is_replaced_once() -> None:
    app = _app(_RejectEverything())
    paths = [getattr(route, "path", None) for route in app.router.routes]  # type: ignore[attr-defined]
    assert paths.count(WELL_KNOWN) == 1
    assert paths.count("/mcp") == 1
