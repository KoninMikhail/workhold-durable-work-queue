"""Test-only local key material and JWT minting for the MCP verifier tests.

This module is not an identity provider and not part of the product. It
generates throwaway RSA and EC keys in memory so the verifier can be tested
without a network JWKS endpoint. No key is written to disk or committed.
"""

from __future__ import annotations

import json
import time
from dataclasses import dataclass, field
from typing import Any

import jwt
from cryptography.hazmat.primitives.asymmetric import ec, rsa
from jwt.algorithms import ECAlgorithm, RSAAlgorithm

ISSUER = "https://idp.example.test"
RESOURCE = "https://mcp.example.test/mcp"
HMAC_SECRET = "test-only-hmac-secret-of-at-least-32-bytes"


@dataclass(frozen=True)
class KeyMaterial:
    """One throwaway signing key and its public JWK."""

    alg: str
    kid: str
    private_key: Any
    public_jwk: dict[str, Any] = field(default_factory=dict)

    @property
    def public_key(self) -> Any:
        return self.private_key.public_key()

    def pyjwk(self) -> jwt.PyJWK:
        return jwt.PyJWK.from_dict(self.public_jwk)


def _jwk_for(algorithm: Any, public_key: Any, *, alg: str, kid: str) -> dict[str, Any]:
    jwk = json.loads(algorithm.to_jwk(public_key))
    jwk.update({"alg": alg, "kid": kid, "use": "sig"})
    return jwk


def make_rsa_key(kid: str = "rsa-test") -> KeyMaterial:
    private = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    jwk = _jwk_for(RSAAlgorithm(RSAAlgorithm.SHA256), private.public_key(), alg="RS256", kid=kid)
    return KeyMaterial(alg="RS256", kid=kid, private_key=private, public_jwk=jwk)


def make_ec_key(kid: str = "ec-test") -> KeyMaterial:
    private = ec.generate_private_key(ec.SECP256R1())
    jwk = _jwk_for(ECAlgorithm(ECAlgorithm.SHA256), private.public_key(), alg="ES256", kid=kid)
    return KeyMaterial(alg="ES256", kid=kid, private_key=private, public_jwk=jwk)


def jwks_document(*keys: KeyMaterial) -> dict[str, Any]:
    """A JWKS document holding only public keys."""
    return {"keys": [key.public_jwk for key in keys]}


def base_claims(
    *,
    issuer: str = ISSUER,
    resource: str = RESOURCE,
    now: float | None = None,
    **overrides: Any,
) -> dict[str, Any]:
    """Valid claims for the default issuer and resource audience.

    Pass ``name=None`` in overrides to drop a claim.
    """
    issued = int(time.time() if now is None else now)
    claims: dict[str, Any] = {
        "iss": issuer,
        "aud": resource,
        "iat": issued,
        "exp": issued + 300,
        "sub": "user-1",
        "azp": "client-1",
        "scope": "workhold-mcp:read",
    }
    for name, value in overrides.items():
        if value is None:
            claims.pop(name, None)
        else:
            claims[name] = value
    return claims


def mint(
    key: KeyMaterial,
    claims: dict[str, Any],
    *,
    headers: dict[str, Any] | None = None,
) -> str:
    """Sign claims with the key's own algorithm and kid."""
    merged = {"kid": key.kid}
    if headers:
        merged.update(headers)
    return jwt.encode(claims, key.private_key, algorithm=key.alg, headers=merged)


def mint_unsigned(claims: dict[str, Any]) -> str:
    """An ``alg=none`` token with an empty signature."""
    return jwt.encode(claims, key=None, algorithm="none")


def mint_hs256(claims: dict[str, Any], secret: str = HMAC_SECRET) -> str:
    """An HMAC token signed with a shared secret."""
    return jwt.encode(claims, secret, algorithm="HS256")
