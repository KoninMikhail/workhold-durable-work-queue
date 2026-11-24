"""Resource-server JWT verification for the remote MCP transport.

``JwtJwksTokenVerifier`` implements the SDK ``TokenVerifier`` protocol for
asymmetric bearer tokens. It accepts RS256 and ES256 only, enforces the
configured issuer and resource audience, bounds expiry by a clock skew, and
turns the ``scope`` or ``scp`` claim into SDK scopes. Every failure returns
``None`` and never echoes the token, the claims, or the key source.

The signing key comes from an injected callable. The CLI passes a JWKS client
for the one configured JWKS URL. No token header or claim (``jku``, ``x5u``)
selects a URL to fetch; only the header ``alg`` is read before the key loads.
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import Callable
from typing import Any

import jwt
from mcp.server.auth.provider import AccessToken

__all__ = ["ALLOWED_ALGORITHMS", "JwtJwksTokenVerifier"]

ALLOWED_ALGORITHMS = ["RS256", "ES256"]
_CLIENT_ID_CLAIMS = ("client_id", "azp", "sub")
_REQUIRED_CLAIMS = ["exp", "iss", "aud"]


class _MalformedScopeClaim(ValueError):
    """A scope claim is neither a space-delimited string nor a list."""


def _scope_list(value: Any) -> list[str]:
    if isinstance(value, str):
        items: list[str] = value.split()
    elif isinstance(value, list):
        items = [item for item in value if isinstance(item, str)]
    else:
        raise _MalformedScopeClaim
    seen: dict[str, None] = {}
    for item in items:
        seen.setdefault(item, None)
    return list(seen)


def _scopes(claims: dict[str, Any]) -> list[str] | None:
    """Scopes from ``scope`` or ``scp``, or ``None`` when the claims disagree."""
    scope = _scope_list(claims["scope"]) if "scope" in claims else None
    scp = _scope_list(claims["scp"]) if "scp" in claims else None
    if scope is None and scp is None:
        return []
    if scope is None:
        return scp
    if scp is None:
        return scope
    if set(scope) != set(scp):
        return None
    return scope


def _client_id(claims: dict[str, Any]) -> str | None:
    for name in _CLIENT_ID_CLAIMS:
        value = claims.get(name)
        if isinstance(value, str) and value:
            return value
    return None


class JwtJwksTokenVerifier:
    """Verify RS256 and ES256 bearer JWTs for one issuer and one resource.

    ``key_for_token`` receives the raw token and returns the verification key
    (for example ``PyJWKClient(url).get_signing_key_from_jwt``). It may block,
    so it runs in a worker thread.
    """

    def __init__(
        self,
        *,
        issuer: str,
        resource: str,
        key_for_token: Callable[[str], jwt.PyJWK],
        clock_skew_seconds: int = 60,
    ) -> None:
        self._issuer = issuer
        self._resource = resource.removesuffix("/")
        self._key_for_token = key_for_token
        self._clock_skew_seconds = clock_skew_seconds

    async def verify_token(self, token: str) -> AccessToken | None:
        try:
            return await self._verify(token)
        except Exception:
            # Fail closed on every bad token, key-loader error, or malformed claim.
            return None

    async def _verify(self, token: str) -> AccessToken | None:
        header = jwt.get_unverified_header(token)
        if header.get("alg") not in ALLOWED_ALGORITHMS:
            return None
        key = await asyncio.to_thread(self._key_for_token, token)
        claims = jwt.decode(
            token,
            key,
            algorithms=ALLOWED_ALGORITHMS,
            audience=self._resource,
            issuer=self._issuer,
            leeway=self._clock_skew_seconds,
            options={"require": _REQUIRED_CLAIMS},
        )
        client_id = _client_id(claims)
        scopes = _scopes(claims)
        if client_id is None or scopes is None:
            return None
        subject = claims.get("sub")
        return AccessToken(
            token=token,
            client_id=client_id,
            scopes=scopes,
            expires_at=self._expires_at(claims["exp"]),
            resource=self._resource,
            subject=subject if isinstance(subject, str) else None,
        )

    @staticmethod
    def _expires_at(exp: float) -> int:
        """Keep the SDK bearer middleware, which has no leeway, from undoing skew."""
        now = time.time()
        if exp > now:
            return int(exp)
        return int(now) + 1
