"""Deployment-issued bearer credential authentication.

Maps opaque bearer material to a stable :class:`Principal` with one
:class:`ServiceRole`. Failures are a single unauthenticated result (error-model
``unauthenticated``), never ``permission_denied``.

Cleartext secrets are hashed at construction and discarded; comparison uses
constant-time digest equality. Ambiguous shared secrets across distinct
principals fail closed.
"""

from __future__ import annotations

import hashlib
import hmac
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Callable, Final, Protocol

from workhold.settings import CredentialGeneration, Secret
from workhold.security.principals import Principal, ServiceRole

# Bound bearer token length before any hashing/comparison (DoS mitigation).
_MAX_BEARER_TOKEN_BYTES: Final[int] = 512
_BEARER_PREFIX: Final[str] = "Bearer "
# Single TOKENCHAR run after "Bearer " — rejects tabs, multiple tokens, empty.
_BEARER_RE: Final[re.Pattern[str]] = re.compile(
    r"^Bearer ([A-Za-z0-9\-._~+/]+=*)$"
)

_UNAUTHENTICATED_CODE: Final[str] = "unauthenticated"


@dataclass(frozen=True, slots=True)
class Unauthenticated:
    """Stable authentication failure without secret disclosure."""

    code: str = _UNAUTHENTICATED_CODE

    def __repr__(self) -> str:
        return "Unauthenticated(code='unauthenticated')"

    def __str__(self) -> str:
        return "unauthenticated"


AuthenticationResult = Principal | Unauthenticated


@dataclass(frozen=True, slots=True)
class CredentialBinding:
    """One credential generation bound to exactly one principal and role.

    ``secret`` is consumed only when building an authenticator; the authenticator
    retains digests, not cleartext. For ``ServiceRole.BREAK_GLASS``, timezone-aware
    ``expires_at`` and a non-empty ``allowed_operations`` audience are mandatory
    (REC-03 / D-01). Other roles may omit those fields.
    """

    principal_id: str
    role: ServiceRole
    generation_id: str
    secret: Secret
    enabled: bool = True
    expires_at: datetime | None = None
    allowed_operations: frozenset[str] | None = None

    def __post_init__(self) -> None:
        if not self.principal_id:
            raise ValueError("principal_id must be non-empty")
        if not self.generation_id:
            raise ValueError("generation_id must be non-empty")
        if not isinstance(self.role, ServiceRole):
            raise TypeError("role must be a ServiceRole")
        if self.role is ServiceRole.BREAK_GLASS:
            if self.expires_at is None:
                raise ValueError(
                    "BREAK_GLASS credentials require timezone-aware expires_at"
                )
            if self.expires_at.tzinfo is None:
                raise ValueError("expires_at must be timezone-aware when set")
            if not self.allowed_operations:
                raise ValueError(
                    "BREAK_GLASS credentials require non-empty allowed_operations"
                )
            object.__setattr__(
                self, "allowed_operations", frozenset(self.allowed_operations)
            )
            return
        if self.expires_at is not None and self.expires_at.tzinfo is None:
            raise ValueError("expires_at must be timezone-aware when set")
        if self.allowed_operations is not None:
            object.__setattr__(
                self, "allowed_operations", frozenset(self.allowed_operations)
            )

    def __repr__(self) -> str:
        return (
            "CredentialBinding("
            f"principal_id={self.principal_id!r}, "
            f"role={self.role.value!r}, "
            f"generation_id={self.generation_id!r}, "
            "secret=Secret('***'), "
            f"enabled={self.enabled!r})"
        )

    def __str__(self) -> str:
        return (
            f"CredentialBinding({self.role.value}:{self.principal_id}/"
            f"{self.generation_id}, enabled={self.enabled})"
        )


class IdentityAuthenticator(Protocol):
    """Adapter-oriented identity surface (bearer today; mTLS later)."""

    def authenticate(self, authorization_header: str | None) -> AuthenticationResult:
        """Authenticate using the OpenAPI HTTP bearer Authorization header only."""


def _digest_secret(cleartext: str) -> bytes:
    return hashlib.sha256(cleartext.encode("utf-8")).digest()


@dataclass(frozen=True, slots=True)
class _LookupEntry:
    principal: Principal
    ambiguous: bool
    expires_at: datetime | None = None


class BearerCredentialAuthenticator:
    """Constant-time bearer authenticator over hashed credential generations."""

    def __init__(
        self,
        by_digest: Mapping[bytes, _LookupEntry],
        *,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        # Digests only — never retain Secret or cleartext after construction.
        self._by_digest = dict(by_digest)
        self._clock = clock or (lambda: datetime.now(timezone.utc))

    @classmethod
    def from_bindings(
        cls,
        bindings: Sequence[CredentialBinding],
        *,
        clock: Callable[[], datetime] | None = None,
    ) -> BearerCredentialAuthenticator:
        by_digest: dict[bytes, _LookupEntry] = {}
        for binding in bindings:
            if not binding.enabled:
                continue
            digest = _digest_secret(binding.secret.get_secret_value())
            principal = Principal(
                principal_id=binding.principal_id,
                role=binding.role,
                credential_expires_at=binding.expires_at,
                operation_audience=binding.allowed_operations,
            )
            existing = by_digest.get(digest)
            if existing is None:
                by_digest[digest] = _LookupEntry(
                    principal=principal,
                    ambiguous=False,
                    expires_at=binding.expires_at,
                )
                continue
            if existing.ambiguous:
                continue
            if (
                existing.principal.principal_id != principal.principal_id
                or existing.principal.role is not principal.role
            ):
                by_digest[digest] = _LookupEntry(
                    principal=existing.principal,
                    ambiguous=True,
                    expires_at=existing.expires_at,
                )
            # Same principal+role with duplicate secret material: keep first.
        return cls(by_digest, clock=clock)

    @classmethod
    def from_credential_generations(
        cls,
        generations: Sequence[CredentialGeneration],
        roles_by_principal: Mapping[str, ServiceRole],
        *,
        disabled_generation_ids: frozenset[str] | None = None,
    ) -> BearerCredentialAuthenticator:
        disabled = disabled_generation_ids or frozenset()
        bindings: list[CredentialBinding] = []
        for generation in generations:
            role = roles_by_principal.get(generation.principal_id)
            if role is None:
                raise ValueError(
                    "missing service role for principal_id="
                    f"{generation.principal_id!r}"
                )
            bindings.append(
                CredentialBinding(
                    principal_id=generation.principal_id,
                    role=role,
                    generation_id=generation.generation_id,
                    secret=generation.secret,
                    enabled=generation.generation_id not in disabled,
                )
            )
        return cls.from_bindings(bindings)

    def authenticate(self, authorization_header: str | None) -> AuthenticationResult:
        token = self._parse_bearer_token(authorization_header)
        if token is None:
            return Unauthenticated()
        digest = _digest_secret(token)
        entry = self._lookup(digest)
        if entry is None or entry.ambiguous:
            return Unauthenticated()
        if entry.expires_at is not None:
            now = self._clock()
            if now.tzinfo is None:
                now = now.replace(tzinfo=timezone.utc)
            expires = entry.expires_at
            if expires.tzinfo is None:
                expires = expires.replace(tzinfo=timezone.utc)
            if now >= expires:
                return Unauthenticated()
        return entry.principal

    def _lookup(self, digest: bytes) -> _LookupEntry | None:
        # Constant-time scan so unknown vs known digests do not short-circuit.
        found: _LookupEntry | None = None
        for stored, entry in self._by_digest.items():
            if hmac.compare_digest(stored, digest):
                found = entry
        return found

    @staticmethod
    def _parse_bearer_token(authorization_header: str | None) -> str | None:
        if authorization_header is None:
            return None
        if not isinstance(authorization_header, str):
            return None
        if len(authorization_header.encode("utf-8")) > (
            len(_BEARER_PREFIX) + _MAX_BEARER_TOKEN_BYTES
        ):
            return None
        match = _BEARER_RE.fullmatch(authorization_header)
        if match is None:
            return None
        token = match.group(1)
        if len(token.encode("utf-8")) > _MAX_BEARER_TOKEN_BYTES:
            return None
        return token

    def __repr__(self) -> str:
        return (
            f"BearerCredentialAuthenticator(generations={len(self._by_digest)})"
        )

    def __str__(self) -> str:
        return self.__repr__()
