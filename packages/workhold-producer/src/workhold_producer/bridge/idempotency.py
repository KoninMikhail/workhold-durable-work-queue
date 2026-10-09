"""Deterministic app-outbox → Queue Idempotency-Key mapping (bridge:v1).

Normative algorithm: docs/04-architecture/application-outbox-bridge.md
"""

from __future__ import annotations

import base64
import hashlib
import re

_KEY_PREFIX = "bridge:v1:"
_MAX_IDENTITY_CHARS = 256
_CONTROL_OR_EMPTY = re.compile(r"[\u0000-\u001F\u007F]")


def bridge_idempotency_key(source_namespace: str, source_row_id: str) -> str:
    """Map one app-outbox identity to a stable Queue ``Idempotency-Key``.

    Same ``(source_namespace, source_row_id)`` always yields the same
    ``bridge:v1:`` key. Length-prefixed UTF-8 encoding prevents concatenation
    collisions such as ``("ab", "c")`` vs ``("a", "bc")``. Opaque identifiers
    are not trimmed or case-folded; validation matches the intent schema.
    """
    ns_bytes = _validated_identity_utf8(source_namespace, field="source_namespace")
    row_bytes = _validated_identity_utf8(source_row_id, field="source_row_id")
    canonical = (
        len(ns_bytes).to_bytes(4, "big")
        + ns_bytes
        + len(row_bytes).to_bytes(4, "big")
        + row_bytes
    )
    digest = hashlib.sha256(canonical).digest()
    token = base64.urlsafe_b64encode(digest).decode("ascii").rstrip("=").lower()
    key = f"{_KEY_PREFIX}{token}"
    if not (1 <= len(key) <= 256):
        # Defensive: SHA-256 base64url is fixed-size; keep OpenAPI bound explicit.
        raise ValueError("idempotency key exceeds OpenAPI 1..256 limit")
    return key


def _validated_identity_utf8(value: object, *, field: str) -> bytes:
    if not isinstance(value, str):
        raise TypeError(f"{field} must be str, got {type(value).__name__}")
    if not value:
        raise ValueError(f"{field} must be non-empty")
    if len(value) > _MAX_IDENTITY_CHARS:
        raise ValueError(f"{field} exceeds maxLength {_MAX_IDENTITY_CHARS}")
    if _CONTROL_OR_EMPTY.search(value) is not None:
        raise ValueError(f"{field} must not contain control characters")
    try:
        return value.encode("utf-8")
    except UnicodeEncodeError as exc:
        raise ValueError(f"{field} is not valid UTF-8") from exc
