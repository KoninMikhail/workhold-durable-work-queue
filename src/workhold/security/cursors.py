"""Versioned integrity-protected opaque continuation cursors (API-05).

Uses the Phase 3 ``Secret`` + ``hmac.compare_digest`` primitive so clients cannot
forge offsets or smuggle filter changes through continuation tokens. Tampered or
malformed cursors raise a stable non-retryable ``validation_failed``.
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
from typing import Any, Final, Mapping

from workhold.domain.queue_control import DomainValidationError
from workhold.settings import Secret

_CURSOR_VERSION: Final[str] = "v1"
_MAX_CURSOR_CHARS: Final[int] = 512
_MAX_PAYLOAD_BYTES: Final[int] = 384


class InspectionCursorCodec:
    """Encode/decode keyed cursors for operational list pagination."""

    __slots__ = ("_key",)

    def __init__(self, secret: Secret) -> None:
        material = secret.get_secret_value().encode("utf-8")
        if not material:
            raise ValueError("cursor signing secret must be non-empty")
        self._key = hashlib.sha256(material).digest()

    def encode(self, kind: str, payload: Mapping[str, Any]) -> str:
        if not kind or not isinstance(kind, str):
            raise ValueError("cursor kind must be a non-empty string")
        body = {
            "v": _CURSOR_VERSION,
            "k": kind,
            "p": dict(payload),
        }
        raw = json.dumps(body, separators=(",", ":"), sort_keys=True).encode("utf-8")
        if len(raw) > _MAX_PAYLOAD_BYTES:
            raise DomainValidationError(
                "validation_failed",
                "cursor payload exceeds bound",
            )
        digest = hmac.new(self._key, raw, hashlib.sha256).digest()
        token = (
            base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")
            + "."
            + base64.urlsafe_b64encode(digest).decode("ascii").rstrip("=")
        )
        if len(token) > _MAX_CURSOR_CHARS:
            raise DomainValidationError(
                "validation_failed",
                "cursor exceeds 512 characters",
            )
        return token

    def decode(self, kind: str, cursor: str | None) -> dict[str, Any] | None:
        if cursor is None or cursor == "":
            return None
        if not isinstance(cursor, str) or len(cursor) > _MAX_CURSOR_CHARS:
            raise DomainValidationError(
                "validation_failed",
                "cursor is invalid",
            )
        try:
            payload_b64, mac_b64 = cursor.split(".", 1)
        except ValueError as exc:
            raise DomainValidationError(
                "validation_failed",
                "cursor is invalid",
            ) from exc
        try:
            raw = _b64url_decode(payload_b64)
            mac = _b64url_decode(mac_b64)
        except (ValueError, UnicodeDecodeError) as exc:
            raise DomainValidationError(
                "validation_failed",
                "cursor is invalid",
            ) from exc
        expected = hmac.new(self._key, raw, hashlib.sha256).digest()
        if not hmac.compare_digest(expected, mac):
            raise DomainValidationError(
                "validation_failed",
                "cursor is invalid",
            )
        try:
            body = json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise DomainValidationError(
                "validation_failed",
                "cursor is invalid",
            ) from exc
        if not isinstance(body, dict):
            raise DomainValidationError("validation_failed", "cursor is invalid")
        if body.get("v") != _CURSOR_VERSION:
            raise DomainValidationError("validation_failed", "cursor is invalid")
        if body.get("k") != kind:
            raise DomainValidationError("validation_failed", "cursor is invalid")
        payload = body.get("p")
        if not isinstance(payload, dict):
            raise DomainValidationError("validation_failed", "cursor is invalid")
        return payload


def _b64url_decode(value: str) -> bytes:
    padding = "=" * (-len(value) % 4)
    return base64.urlsafe_b64decode(value + padding)
