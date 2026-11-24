"""Outer ASGI guard that rejects bad ``Host`` and ``Origin`` values first.

The SDK checks both headers inside the session manager, which runs behind the
bearer middleware and so after token verification. ``HostOriginGuard`` is the
outermost callable that uvicorn and ``TestClient`` serve. A rejection returns a
plain-text response and never awaits the inner app, so a DNS-rebinding request
cannot reach ``verify_token`` or a tool. The same ``TransportSecuritySettings``
also go to the SDK app as defense in depth.

The module imports only the SDK settings model, never the Workhold API stack.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Mapping, MutableMapping, Sequence
from typing import Any

from mcp.server.transport_security import TransportSecuritySettings

__all__ = ["ASGIApp", "HostOriginGuard"]

ASGIApp = Callable[
    [MutableMapping[str, Any], Callable[[], Awaitable[dict[str, Any]]], Callable[[dict[str, Any]], Awaitable[None]]],
    Awaitable[None],
]

_INVALID_HOST = b"Invalid Host header"
_INVALID_ORIGIN = b"Invalid Origin header"
_WILDCARD_PORT = ":*"


def _header_map(scope: Mapping[str, Any]) -> dict[str, str]:
    out: dict[str, str] = {}
    for raw_key, raw_val in scope.get("headers", []):
        key = raw_key.decode("latin-1").lower()
        out[key] = raw_val.decode("latin-1")
    return out


def _matches(value: str, allowed: Sequence[str]) -> bool:
    """Exact entry match, or ``base:*`` matching ``base`` followed by ``:``."""

    if value in allowed:
        return True
    for entry in allowed:
        if entry.endswith(_WILDCARD_PORT) and value.startswith(entry[: -len(_WILDCARD_PORT)] + ":"):
            return True
    return False


async def _reject(send: Callable[[dict[str, Any]], Awaitable[None]], status: int, body: bytes) -> None:
    await send(
        {
            "type": "http.response.start",
            "status": status,
            "headers": [
                (b"content-type", b"text/plain; charset=utf-8"),
                (b"content-length", str(len(body)).encode("ascii")),
            ],
        }
    )
    await send({"type": "http.response.body", "body": body, "more_body": False})


class HostOriginGuard:
    """Reject disallowed ``Host`` (421) and ``Origin`` (403) before ``inner``.

    A missing ``Host`` fails. A missing ``Origin`` passes. An empty Origin
    allowlist is no-browser mode: every request that carries an ``Origin``
    fails. Non-HTTP scopes, including ``lifespan``, go straight to ``inner``.
    DNS-rebinding protection cannot be disabled here.
    """

    def __init__(self, inner: ASGIApp, security: TransportSecuritySettings) -> None:
        if not security.enable_dns_rebinding_protection:
            raise ValueError("DNS-rebinding protection must stay enabled")
        self._inner = inner
        self._hosts = tuple(security.allowed_hosts)
        self._origins = tuple(security.allowed_origins)

    def __getattr__(self, name: str) -> Any:
        if name.startswith("_"):
            raise AttributeError(name)
        return getattr(self._inner, name)

    async def __call__(
        self,
        scope: MutableMapping[str, Any],
        receive: Callable[[], Awaitable[dict[str, Any]]],
        send: Callable[[dict[str, Any]], Awaitable[None]],
    ) -> None:
        if scope["type"] != "http":
            await self._inner(scope, receive, send)
            return
        headers = _header_map(scope)
        host = headers.get("host")
        if not host or not _matches(host, self._hosts):
            await _reject(send, 421, _INVALID_HOST)
            return
        origin = headers.get("origin")
        if origin and not _matches(origin, self._origins):
            await _reject(send, 403, _INVALID_ORIGIN)
            return
        await self._inner(scope, receive, send)
