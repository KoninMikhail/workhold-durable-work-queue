"""Signature pin for every version-sensitive SDK and PyJWT call the HTTP app makes.

This file passes by itself, before ``http_app`` exists. It records, rather than
requires, whether an ``httpx2`` ASGI transport imports.
"""

from __future__ import annotations

import importlib.metadata
import inspect

import jwt
import uvicorn
from mcp.server import MCPServer
from mcp.server.auth.middleware.auth_context import get_access_token
from mcp.server.auth.provider import AccessToken
from mcp.server.auth.routes import (
    build_resource_metadata_url,
    create_protected_resource_routes,
)
from mcp.server.auth.settings import AuthSettings
from mcp.server.transport_security import TransportSecuritySettings

try:
    from httpx2 import ASGITransport  # noqa: F401

    HTTPX2_ASGI_AVAILABLE = True
except ImportError:
    HTTPX2_ASGI_AVAILABLE = False

RECORDED_PARAMETERS: dict[str, tuple[str, ...]] = {}


def _parameters(target: object) -> tuple[str, ...]:
    return tuple(inspect.signature(target).parameters)  # type: ignore[arg-type]


def test_installed_mcp_is_the_pinned_release() -> None:
    assert importlib.metadata.version("mcp").startswith("2.3.")


def test_mcp_server_constructor_names() -> None:
    names = _parameters(MCPServer.__init__)
    for name in ("subscriptions", "token_verifier", "auth"):
        assert name in names


def test_auth_settings_fields() -> None:
    for name in ("required_scopes", "validate_token_resource"):
        assert name in AuthSettings.model_fields


def test_streamable_http_app_keywords() -> None:
    names = _parameters(MCPServer.streamable_http_app)
    for name in (
        "streamable_http_path",
        "json_response",
        "stateless_http",
        "event_store",
        "transport_security",
    ):
        assert name in names


def test_protected_resource_route_builders() -> None:
    assert callable(create_protected_resource_routes)
    assert callable(build_resource_metadata_url)
    RECORDED_PARAMETERS["create_protected_resource_routes"] = _parameters(
        create_protected_resource_routes
    )
    RECORDED_PARAMETERS["build_resource_metadata_url"] = _parameters(
        build_resource_metadata_url
    )
    recorded = RECORDED_PARAMETERS["create_protected_resource_routes"]
    for name in ("resource_url", "authorization_servers", "scopes_supported"):
        assert name in recorded


def test_access_token_context_and_fields() -> None:
    assert callable(get_access_token)
    for name in (
        "token",
        "client_id",
        "scopes",
        "expires_at",
        "resource",
        "subject",
        "claims",
    ):
        assert name in AccessToken.model_fields


def test_transport_security_fields() -> None:
    for name in (
        "enable_dns_rebinding_protection",
        "allowed_hosts",
        "allowed_origins",
    ):
        assert name in TransportSecuritySettings.model_fields


def test_uvicorn_config_proxy_keywords() -> None:
    names = _parameters(uvicorn.Config.__init__)
    assert "proxy_headers" in names
    assert "forwarded_allow_ips" in names


def test_streamable_http_client_accepts_an_http_client() -> None:
    from mcp.client.streamable_http import streamable_http_client

    assert "http_client" in _parameters(streamable_http_client)


def test_pyjwt_calls_the_verifier_uses() -> None:
    decode = _parameters(jwt.decode)
    assert "algorithms" in decode
    assert "audience" in decode
    assert "issuer" in decode
    assert "leeway" in decode
    assert callable(jwt.PyJWKClient.get_signing_key_from_jwt)


def test_httpx2_asgi_transport_availability_is_recorded() -> None:
    assert isinstance(HTTPX2_ASGI_AVAILABLE, bool)
