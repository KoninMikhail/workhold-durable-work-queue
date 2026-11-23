"""Deterministic client-side task-payload envelope (AES-256-GCM, HMAC-derived nonce)."""

from __future__ import annotations

import base64
import binascii
import hashlib
import hmac
import json
from dataclasses import replace
from typing import Any

from cryptography.hazmat.primitives.ciphers.aead import AESGCM

from _workhold_client_core.errors import QueueClientError
from _workhold_client_core.models import Task

__all__ = [
    "ContentDecryptError",
    "is_content_envelope",
    "open_task_payload",
    "parse_content_key",
    "seal_task_payload",
    "task_with_opened_payload",
]

_KEY_LENGTH = 32
_NONCE_LENGTH = 12
_AAD = b"workhold-content-v1"
_ENVELOPE_FIELDS = frozenset({"$workhold_enc", "n", "c"})
_DECRYPT_FAILED = "content_decrypt_failed"
_KEY_ERROR = "content_key must be 32 bytes or standard base64 that decodes to 32 bytes"


class ContentDecryptError(QueueClientError):
    """Envelope could not be opened; text never carries key, plaintext or envelope."""

    def __init__(self, *, raw: Any, reason: str = _DECRYPT_FAILED) -> None:
        self.reason = reason
        self._raw = raw
        super().__init__(reason)

    @property
    def raw(self) -> Any:
        """Explicit recovery access to the unopened wire value (never logged here)."""

        return self._raw

    def __str__(self) -> str:
        return self.reason

    def __repr__(self) -> str:
        return f"ContentDecryptError(reason={_DECRYPT_FAILED!r})"


def parse_content_key(value: bytes | str | None) -> bytes | None:
    """Validate an explicit content key; the environment is never consulted."""

    if value is None:
        return None
    if type(value) is bytes:
        if len(value) == _KEY_LENGTH:
            return value
        raise ValueError(_KEY_ERROR)
    if type(value) is str:
        try:
            decoded = base64.b64decode(value, validate=True)
        except (binascii.Error, ValueError):
            raise ValueError(_KEY_ERROR) from None
        if len(decoded) == _KEY_LENGTH:
            return decoded
    raise ValueError(_KEY_ERROR)


def _canonical_json_bytes(value: Any) -> bytes:
    try:
        return json.dumps(
            value,
            ensure_ascii=False,
            separators=(",", ":"),
            allow_nan=False,
            sort_keys=True,
        ).encode("utf-8")
    except (TypeError, ValueError):
        raise ValueError("payload must be a JSON value") from None


def seal_task_payload(value: Any, key: bytes) -> dict[str, Any]:
    """Seal a JSON value; the same value and key always give the same envelope."""

    plaintext = _canonical_json_bytes(value)
    nonce = hmac.new(key, plaintext, hashlib.sha256).digest()[:_NONCE_LENGTH]
    ciphertext = AESGCM(key).encrypt(nonce, plaintext, _AAD)
    return {
        "$workhold_enc": 1,
        "n": base64.b64encode(nonce).decode("ascii"),
        "c": base64.b64encode(ciphertext).decode("ascii"),
    }


def is_content_envelope(value: Any) -> bool:
    """True only for the exact three-field version-1 envelope."""

    if type(value) is not dict or set(value) != _ENVELOPE_FIELDS:
        return False
    version = value["$workhold_enc"]
    return (
        type(version) is int
        and version == 1
        and type(value["n"]) is str
        and type(value["c"]) is str
    )


def open_task_payload(value: Any, key: bytes | None) -> Any:
    """Open an envelope with ``key``; anything else is returned unchanged."""

    if key is None or not is_content_envelope(value):
        return value
    try:
        nonce = base64.b64decode(value["n"], validate=True)
        ciphertext = base64.b64decode(value["c"], validate=True)
        if len(nonce) != _NONCE_LENGTH:
            raise ValueError
        return json.loads(AESGCM(key).decrypt(nonce, ciphertext, _AAD))
    except Exception:
        raise ContentDecryptError(raw=value, reason=_DECRYPT_FAILED) from None


def task_with_opened_payload(task: Task, key: bytes | None) -> Task:
    """Return ``task`` with its envelope payload opened, or ``task`` if unchanged."""

    opened = open_task_payload(task.payload, key)
    if opened is task.payload:
        return task
    return replace(task, payload=opened)
