"""Serve the shared MCP registry over stateless JSON Streamable HTTP at ``/mcp``.

``build_http_app`` returns the official SDK Starlette app after one metadata
route replacement. ``serve_streamable_http`` wires the JWKS token verifier,
the shared ``build_server`` registration, and an explicit uvicorn config whose
proxy trust follows the configured forward list.
"""

from __future__ import annotations

from urllib.parse import urlparse

import uvicorn
from jwt import PyJWKClient
from mcp.server import MCPServer
from mcp.server.auth.routes import (
    build_resource_metadata_url,
    create_protected_resource_routes,
)
from mcp.server.auth.settings import AuthSettings
from mcp.server.transport_security import TransportSecuritySettings
from pydantic import AnyHttpUrl
from starlette.applications import Starlette
from starlette.routing import Route

from workhold_mcp.config import ConfigError, HttpSettings, McpSettings
from workhold_mcp.jwt_verifier import JwtJwksTokenVerifier
from workhold_mcp.server import build_server

__all__ = [
    "MCP_PATH",
    "SUPPORTED_SCOPES",
    "build_auth_settings",
    "build_http_app",
    "serve_streamable_http",
]

MCP_PATH = "/mcp"
SUPPORTED_SCOPES = ("workhold-mcp:read", "workhold-mcp:produce", "workhold-mcp:operate")
_RESOURCE_URL = "WORKHOLD_MCP_RESOURCE_URL"
_ISSUER_URL = "WORKHOLD_MCP_ISSUER_URL"
_JWKS_URL = "WORKHOLD_MCP_JWKS_URL"


def build_auth_settings(resource_url: str, issuer_url: str) -> AuthSettings:
    """SDK auth settings with an empty scope list.

    The SDK copies ``required_scopes`` into an AND gate, so the three Workhold
    scope groups are advertised by the metadata route instead of listed here.
    """

    return AuthSettings(
        issuer_url=AnyHttpUrl(issuer_url),
        resource_server_url=AnyHttpUrl(resource_url),
        required_scopes=[],
        validate_token_resource=True,
    )


def build_http_app(
    mcp: MCPServer,
    *,
    resource_url: str,
    issuer_url: str,
    security: TransportSecuritySettings,
) -> Starlette:
    """Return the SDK app for ``mcp`` with the three-scope metadata route.

    ``mcp`` already carries its token verifier and auth settings, because
    ``build_server`` attached them. The SDK lifespan is kept as registered.
    """

    app = mcp.streamable_http_app(
        streamable_http_path=MCP_PATH,
        json_response=True,
        stateless_http=True,
        event_store=None,
        transport_security=security,
    )
    _replace_metadata_route(app, resource_url=resource_url, issuer_url=issuer_url)
    return app


def _replace_metadata_route(app: Starlette, *, resource_url: str, issuer_url: str) -> None:
    resource = AnyHttpUrl(resource_url)
    path = urlparse(str(build_resource_metadata_url(resource))).path
    replacements = {
        route.path: route
        for route in create_protected_resource_routes(
            resource_url=resource,
            authorization_servers=[AnyHttpUrl(issuer_url)],
            scopes_supported=list(SUPPORTED_SCOPES),
        )
        if isinstance(route, Route)
    }
    routes = app.router.routes
    for index, route in enumerate(routes):
        if isinstance(route, Route) and route.path == path and path in replacements:
            routes[index] = replacements[path]
            return
    raise RuntimeError("protected-resource metadata route missing")


def serve_streamable_http(
    settings: HttpSettings,
    mcp_settings: McpSettings,
    names: frozenset[str],
) -> None:
    """Build the verifier, the shared server and the app, then listen.

    ``settings`` is the validated HTTP configuration. ``mcp_settings`` and
    ``names`` are the same role-client settings and selected tool names stdio
    uses. Proxy headers are trusted only when a forward list is configured.
    """

    resource_url = _required(settings.resource_url, _RESOURCE_URL)
    issuer_url = _required(settings.issuer_url, _ISSUER_URL)
    jwks_url = _required(settings.jwks_url, _JWKS_URL)
    verifier = JwtJwksTokenVerifier(
        issuer=issuer_url,
        resource=resource_url,
        key_for_token=PyJWKClient(jwks_url).get_signing_key_from_jwt,
        clock_skew_seconds=settings.clock_skew_seconds,
    )
    server = build_server(
        mcp_settings,
        names,
        token_verifier=verifier,
        auth=build_auth_settings(resource_url, issuer_url),
    )
    security = TransportSecuritySettings(
        enable_dns_rebinding_protection=True,
        allowed_hosts=list(settings.allowed_hosts),
        allowed_origins=list(settings.allowed_origins),
    )
    app = build_http_app(
        server,
        resource_url=resource_url,
        issuer_url=issuer_url,
        security=security,
    )
    config = uvicorn.Config(
        app,
        host=settings.bind_host,
        port=settings.bind_port,
        proxy_headers=bool(settings.forwarded_allow_ips),
        forwarded_allow_ips=list(settings.forwarded_allow_ips),
        log_level="info",
    )
    uvicorn.Server(config).run()


def _required(value: str | None, setting_name: str) -> str:
    if not value:
        raise ConfigError(setting_name)
    return value
