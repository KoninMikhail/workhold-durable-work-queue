"""Diagnostic redaction for headers, free text, and nested diagnostic trees.

Client-local helper: never depends on ``queue_service`` server redaction.
"""

from __future__ import annotations

import re
from collections.abc import Mapping, Sequence
from typing import Any, Final

REDACTED: Final[str] = "[REDACTED]"
_HEADER_REDACTED: Final[str] = "<redacted>"

_REDACTED_HEADER_NAMES = frozenset(
    {"authorization", "x-queue-claim-token", "idempotency-key"}
)

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
        "idempotency_key",
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
    "dsn",
)

_DIAGNOSTIC_ALLOWLIST: Final[frozenset[str]] = frozenset(
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
        "generation",
        "status",
        "details",
        "items",
        "nested",
        "outcome",
        "reason",
        "role",
        "schema_revision",
        "protocol_version",
        "protocol_major",
    }
)

# URL-shaped secrets: postgres/mysql/redis DSNs and user:pass@host forms.
_DSN_LIKE = re.compile(
    r"(?i)\b(?:postgres(?:ql)?(?:\+[\w]+)?|mysql|redis|mongodb|amqp|http|https)"
    r"://[^\s\"']+"
)
_USERINFO_AT = re.compile(r"(?i)\b[\w.+-]+://[^\s\"']*:[^\s\"'/]+@[^\s\"']+")
_BEARER = re.compile(r"(?i)\bbearer\s+[A-Za-z0-9._\-+=/]+")


def redact_headers(headers: Mapping[str, str]) -> dict[str, str]:
    """Return a copy with secret header values replaced (for diagnostics)."""

    out: dict[str, str] = {}
    for key, value in headers.items():
        if key.lower() in _REDACTED_HEADER_NAMES:
            out[key] = _HEADER_REDACTED
        else:
            out[key] = value
    return out


def redact_text(text: str) -> str:
    """Mask DSN-like URLs and bearer tokens in free-form diagnostic text."""

    cleaned = _DSN_LIKE.sub(_HEADER_REDACTED, text)
    cleaned = _USERINFO_AT.sub(_HEADER_REDACTED, cleaned)
    cleaned = _BEARER.sub(f"Bearer {_HEADER_REDACTED}", cleaned)
    lowered = cleaned.lower()
    if "authorization" in lowered and "bearer" not in cleaned.lower():
        # Avoid echoing Authorization-looking substrings from transport reasons.
        return "transport_error"
    return cleaned


def sanitize_for_diagnostics(value: object, *, _depth: int = 0) -> Any:
    """Allowlist nested diagnostic trees; always redact secret-shaped keys."""

    if _depth > 8:
        return REDACTED
    if isinstance(value, Mapping):
        out: dict[str, Any] = {}
        for key, item in value.items():
            key_s = str(key)
            if _is_secret_key(key_s):
                out[key_s] = REDACTED
            elif _is_allowed_key(key_s):
                out[key_s] = sanitize_for_diagnostics(item, _depth=_depth + 1)
        return out
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        items = list(value)[:64]
        return [sanitize_for_diagnostics(item, _depth=_depth + 1) for item in items]
    if isinstance(value, str):
        return redact_text(value) if _looks_secret_text(value) else value
    return value


def _normalize_key(key: object) -> str:
    return str(key).strip().lower().replace(" ", "_")


def _is_secret_key(key: object) -> bool:
    normalized = _normalize_key(key)
    if normalized in _SECRET_KEY_EXACT:
        return True
    return any(fragment in normalized for fragment in _SECRET_KEY_FRAGMENTS)


def _is_allowed_key(key: object) -> bool:
    return _normalize_key(key) in _DIAGNOSTIC_ALLOWLIST


def _looks_secret_text(text: str) -> bool:
    if _DSN_LIKE.search(text) or _USERINFO_AT.search(text) or _BEARER.search(text):
        return True
    lowered = text.lower()
    return "authorization" in lowered or "postgresql://" in lowered
