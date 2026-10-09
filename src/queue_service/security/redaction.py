"""Allowlist-based diagnostic sanitization for secrets and opaque payloads.

Claim tokens, credentials, authorization material, cookies, and payload bodies
must never enter logs, metric labels, generic inspection, or error details.
Output is constructed from an explicit allowlist; secret-shaped keys are always
replaced with a fixed marker even when nested under allowed containers.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any, Final

REDACTED: Final[str] = "[REDACTED]"
TRUNCATED: Final[str] = "[TRUNCATED]"

MAX_DEPTH: Final[int] = 8
MAX_ITEMS: Final[int] = 64
MAX_STRING_BYTES: Final[int] = 2048

# Keys permitted in sanitized diagnostic trees (case-insensitive match).
DIAGNOSTIC_ALLOWLIST: Final[frozenset[str]] = frozenset(
    {
        "request_id",
        "code",
        "retryable",
        "retry_after_ms",
        "message",
        "operation",
        "queue_name",
        "queue_id",
        "task_id",
        "claim_id",
        "event_id",
        "worker_id",
        "principal_id",
        "generation",
        "generation_id",
        "status",
        "safe_status",
        "details",
        "items",
        "nested",
        "outcome",
        "reason",
        "role",
        "replica_id",
        "schema_revision",
        "protocol_version",
    }
)

# Normalized key names / fragments that always redact (deny overrides allowlist).
_SECRET_KEY_EXACT: Final[frozenset[str]] = frozenset(
    {
        "authorization",
        "cookie",
        "set-cookie",
        "payload",
        "payload_body",
        "task_payload",
        "event_payload",
        "claim_token",
        "claim-token",
        "x-queue-claim-token",
        "credential",
        "credentials",
        "secret",
        "password",
        "passwd",
        "token",
        "access_token",
        "refresh_token",
        "api_key",
        "apikey",
        "database_url",
        "dsn",
        "sentry_dsn",
        "bearer",
    }
)

_SECRET_KEY_FRAGMENTS: Final[tuple[str, ...]] = (
    "claim_token",
    "claim-token",
    "authorization",
    "password",
    "secret",
    "credential",
    "payload",
    "cookie",
)


class DiagnosticSanitizerError(ValueError):
    """Raised when diagnostic construction fails; never echoes secrets."""

    def __init__(self, _reason: str = "diagnostic sanitization failed") -> None:
        super().__init__("diagnostic sanitization failed")

    def __repr__(self) -> str:
        return "DiagnosticSanitizerError('diagnostic sanitization failed')"

    def __str__(self) -> str:
        return "diagnostic sanitization failed"


def _normalize_key(key: object) -> str:
    return str(key).strip().lower().replace(" ", "_")


def _is_secret_key(key: object) -> bool:
    normalized = _normalize_key(key)
    if normalized in _SECRET_KEY_EXACT:
        return True
    return any(fragment in normalized for fragment in _SECRET_KEY_FRAGMENTS)


def _is_allowed_key(key: object, allowlist: frozenset[str]) -> bool:
    return _normalize_key(key) in {_normalize_key(item) for item in allowlist}


def _bound_string(value: str) -> str:
    encoded = value.encode("utf-8", errors="replace")
    if len(encoded) > MAX_STRING_BYTES:
        return TRUNCATED
    return value


def sanitize_for_diagnostics(
    value: object,
    *,
    allowlist: frozenset[str] | None = None,
    max_depth: int = MAX_DEPTH,
    max_items: int = MAX_ITEMS,
    _depth: int = 0,
) -> Any:
    """Return a safe, bounded diagnostic projection of ``value``.

    Mapping keys outside the allowlist are omitted. Secret/payload-shaped keys
    are always replaced with :data:`REDACTED`. Sequences and strings are
    truncated when size limits are exceeded.
    """
    keys = allowlist if allowlist is not None else DIAGNOSTIC_ALLOWLIST

    if _depth > max_depth:
        return TRUNCATED

    if value is None or isinstance(value, (bool, int, float)):
        return value

    if isinstance(value, str):
        return _bound_string(value)

    if isinstance(value, bytes):
        if len(value) > MAX_STRING_BYTES:
            return TRUNCATED
        return REDACTED

    if isinstance(value, Mapping):
        out: dict[str, Any] = {}
        for raw_key, raw_val in value.items():
            key_str = str(raw_key)
            if _is_secret_key(raw_key):
                out[key_str] = REDACTED
                continue
            if not _is_allowed_key(raw_key, keys):
                continue
            out[key_str] = sanitize_for_diagnostics(
                raw_val,
                allowlist=keys,
                max_depth=max_depth,
                max_items=max_items,
                _depth=_depth + 1,
            )
        return out

    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        items: list[Any] = []
        for index, item in enumerate(value):
            if index >= max_items:
                items.append(TRUNCATED)
                break
            items.append(
                sanitize_for_diagnostics(
                    item,
                    allowlist=keys,
                    max_depth=max_depth,
                    max_items=max_items,
                    _depth=_depth + 1,
                )
            )
        return items

    # Unknown / novel types never dump repr into diagnostics.
    return TRUNCATED
