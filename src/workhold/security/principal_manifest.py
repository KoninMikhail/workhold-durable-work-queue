"""Strict bounded parser for deployment-issued HTTP service principals.

The parser consumes one secret JSON document entirely in memory and returns
immutable authentication bindings plus exact named-queue scopes. Diagnostics
contain field paths only and never include source JSON or bearer material.
"""

from __future__ import annotations

import json
import re
from collections.abc import Mapping
from dataclasses import dataclass
from types import MappingProxyType
from typing import Any, Final

from workhold.security.credentials import CredentialBinding
from workhold.security.principals import ServiceRole
from workhold.settings import Secret, SettingsValidationError

PRINCIPAL_MANIFEST_MAX_BYTES: Final[int] = 1_048_576
_MAX_PRINCIPALS: Final[int] = 256
_MAX_CREDENTIALS_PER_PRINCIPAL: Final[int] = 8
_MAX_QUEUE_SCOPES_PER_PRINCIPAL: Final[int] = 256
_MAX_IDENTIFIER_LENGTH: Final[int] = 128
_MAX_BEARER_BYTES: Final[int] = 512

_ROOT_KEYS: Final[frozenset[str]] = frozenset({"schema_version", "principals"})
_PRINCIPAL_KEYS: Final[frozenset[str]] = frozenset(
    {"principal_id", "role", "queue_scopes", "credentials"}
)
_CREDENTIAL_KEYS: Final[frozenset[str]] = frozenset(
    {"generation_id", "secret"}
)
_ALLOWED_ROLES: Final[dict[str, ServiceRole]] = {
    "PRODUCER": ServiceRole.PRODUCER,
    "WORKER": ServiceRole.WORKER,
    "OBSERVER": ServiceRole.OBSERVER,
    "ADMIN": ServiceRole.ADMIN,
}
_IDENTIFIER_RE: Final[re.Pattern[str]] = re.compile(
    r"^[A-Za-z0-9][A-Za-z0-9._:-]*$"
)
_QUEUE_NAME_RE: Final[re.Pattern[str]] = re.compile(r"^[a-z0-9][a-z0-9._-]*$")
_BEARER_RE: Final[re.Pattern[str]] = re.compile(r"^[A-Za-z0-9\-._~+/]+=*$")


@dataclass(frozen=True, slots=True)
class PrincipalManifest:
    """Immutable authentication and authorization inputs."""

    bindings: tuple[CredentialBinding, ...]
    queue_scopes: Mapping[str, frozenset[str]]

    def __post_init__(self) -> None:
        object.__setattr__(self, "bindings", tuple(self.bindings))
        object.__setattr__(
            self,
            "queue_scopes",
            MappingProxyType(dict(self.queue_scopes)),
        )


class _InvalidJson(ValueError):
    pass


def _object_without_duplicates(
    pairs: list[tuple[str, Any]],
) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise _InvalidJson("duplicate JSON object property")
        result[key] = value
    return result


def _fail(message: str) -> SettingsValidationError:
    return SettingsValidationError(f"QUEUE_API_PRINCIPALS_MANIFEST: {message}")


def _reject_unknown_keys(
    payload: Mapping[str, Any],
    allowed: frozenset[str],
    *,
    path: str,
) -> None:
    unknown = sorted(set(payload) - allowed)
    if unknown:
        raise _fail(f"{path}: unknown property")


def _require_keys(
    payload: Mapping[str, Any],
    required: frozenset[str],
    *,
    path: str,
) -> None:
    missing = sorted(required - set(payload))
    if missing:
        raise _fail(f"{path}: missing required property '{missing[0]}'")


def _identifier(value: object, *, path: str) -> str:
    if (
        not isinstance(value, str)
        or not (1 <= len(value) <= _MAX_IDENTIFIER_LENGTH)
        or _IDENTIFIER_RE.fullmatch(value) is None
    ):
        raise _fail(
            f"{path}: must match ^[A-Za-z0-9][A-Za-z0-9._:-]*$ "
            f"(1..{_MAX_IDENTIFIER_LENGTH})"
        )
    return value


def _queue_name(value: object, *, path: str) -> str:
    if (
        not isinstance(value, str)
        or not (1 <= len(value) <= 128)
        or _QUEUE_NAME_RE.fullmatch(value) is None
    ):
        raise _fail(
            f"{path}: queue name must match ^[a-z0-9][a-z0-9._-]*$ (1..128)"
        )
    return value


def _secret(value: object, *, path: str) -> Secret:
    if (
        not isinstance(value, str)
        or not value
        or len(value.encode("utf-8")) > _MAX_BEARER_BYTES
        or _BEARER_RE.fullmatch(value) is None
    ):
        raise _fail(f"{path}: must be a non-empty opaque bearer value")
    return Secret(value)


def parse_principal_manifest(raw: bytes) -> PrincipalManifest:
    """Parse a bounded v1 manifest without disclosing source or secrets."""
    if len(raw) > PRINCIPAL_MANIFEST_MAX_BYTES:
        raise _fail("exceeds maximum size")
    if not raw.strip():
        raise _fail("is empty")

    try:
        parsed = json.loads(
            raw.decode("utf-8-sig"),
            object_pairs_hook=_object_without_duplicates,
            parse_constant=lambda _value: (_ for _ in ()).throw(
                _InvalidJson("non-standard JSON constant")
            ),
        )
    except (UnicodeDecodeError, json.JSONDecodeError, _InvalidJson, RecursionError):
        # JSONDecodeError retains the complete source document in ``doc``.
        # Suppress exception chaining so traceback/log formatters cannot expose it.
        raise _fail("must be valid UTF-8 JSON") from None

    if not isinstance(parsed, dict):
        raise _fail("$: root must be an object")
    _reject_unknown_keys(parsed, _ROOT_KEYS, path="$")
    _require_keys(parsed, _ROOT_KEYS, path="$")

    schema_version = parsed["schema_version"]
    if type(schema_version) is not int or schema_version != 1:
        raise _fail("$.schema_version must be 1")

    principals = parsed["principals"]
    if not isinstance(principals, list):
        raise _fail("$.principals must be an array")
    if not principals:
        raise _fail("$.principals must be non-empty")
    if len(principals) > _MAX_PRINCIPALS:
        raise _fail(f"$.principals exceeds maximum {_MAX_PRINCIPALS}")

    bindings: list[CredentialBinding] = []
    queue_scopes: dict[str, frozenset[str]] = {}
    principal_ids: set[str] = set()
    generation_ids: set[str] = set()
    secret_owners: dict[str, tuple[str, ServiceRole]] = {}

    for principal_index, principal_item in enumerate(principals):
        principal_path = f"$.principals[{principal_index}]"
        if not isinstance(principal_item, dict):
            raise _fail(f"{principal_path} must be an object")
        _reject_unknown_keys(principal_item, _PRINCIPAL_KEYS, path=principal_path)
        _require_keys(principal_item, _PRINCIPAL_KEYS, path=principal_path)

        principal_id = _identifier(
            principal_item["principal_id"],
            path=f"{principal_path}.principal_id",
        )
        if principal_id in principal_ids:
            raise _fail(f"{principal_path}.principal_id: duplicate principal")
        principal_ids.add(principal_id)

        role_raw = principal_item["role"]
        if not isinstance(role_raw, str) or role_raw not in _ALLOWED_ROLES:
            raise _fail(
                f"{principal_path}.role: must be one of "
                "PRODUCER, WORKER, OBSERVER, ADMIN"
            )
        role = _ALLOWED_ROLES[role_raw]

        scopes_raw = principal_item["queue_scopes"]
        if not isinstance(scopes_raw, list):
            raise _fail(f"{principal_path}.queue_scopes must be an array")
        if len(scopes_raw) > _MAX_QUEUE_SCOPES_PER_PRINCIPAL:
            raise _fail(
                f"{principal_path}.queue_scopes exceeds maximum "
                f"{_MAX_QUEUE_SCOPES_PER_PRINCIPAL}"
            )
        scopes: set[str] = set()
        for scope_index, scope_raw in enumerate(scopes_raw):
            scope = _queue_name(
                scope_raw,
                path=f"{principal_path}.queue_scopes[{scope_index}]",
            )
            if scope in scopes:
                raise _fail(
                    f"{principal_path}.queue_scopes[{scope_index}]: "
                    "duplicate queue name"
                )
            scopes.add(scope)
        if role is ServiceRole.ADMIN and scopes:
            raise _fail(f"{principal_path}.queue_scopes must be empty for ADMIN")
        if role is not ServiceRole.ADMIN and not scopes:
            raise _fail(
                f"{principal_path}.queue_scopes must be non-empty for {role_raw}"
            )
        queue_scopes[principal_id] = frozenset(scopes)

        credentials = principal_item["credentials"]
        credentials_path = f"{principal_path}.credentials"
        if not isinstance(credentials, list):
            raise _fail(f"{credentials_path} must be an array")
        if not credentials:
            raise _fail(f"{credentials_path} must be non-empty")
        if len(credentials) > _MAX_CREDENTIALS_PER_PRINCIPAL:
            raise _fail(
                f"{credentials_path} exceeds maximum "
                f"{_MAX_CREDENTIALS_PER_PRINCIPAL}"
            )

        for credential_index, credential_item in enumerate(credentials):
            credential_path = f"{credentials_path}[{credential_index}]"
            if not isinstance(credential_item, dict):
                raise _fail(f"{credential_path} must be an object")
            _reject_unknown_keys(
                credential_item, _CREDENTIAL_KEYS, path=credential_path
            )
            _require_keys(
                credential_item, _CREDENTIAL_KEYS, path=credential_path
            )
            generation_id = _identifier(
                credential_item["generation_id"],
                path=f"{credential_path}.generation_id",
            )
            if generation_id in generation_ids:
                raise _fail(
                    f"{credential_path}.generation_id: duplicate generation"
                )
            generation_ids.add(generation_id)

            secret = _secret(
                credential_item["secret"],
                path=f"{credential_path}.secret",
            )
            cleartext = secret.get_secret_value()
            owner = secret_owners.get(cleartext)
            if owner is not None and owner != (principal_id, role):
                raise _fail(
                    f"{credential_path}.secret: reused across distinct identities"
                )
            secret_owners[cleartext] = (principal_id, role)
            bindings.append(
                CredentialBinding(
                    principal_id=principal_id,
                    role=role,
                    generation_id=generation_id,
                    secret=secret,
                )
            )

    return PrincipalManifest(
        bindings=tuple(bindings),
        queue_scopes=queue_scopes,
    )
