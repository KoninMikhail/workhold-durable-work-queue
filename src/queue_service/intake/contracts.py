"""Deterministic producer enqueue command normalization and fingerprinting.

Implements the Phase 3.4 intake boundary over the Phase 3.1 OpenAPI enqueue
fields and Phase 3.2 non-retryable protocol error codes. Authenticated producer
identity and queue identity are idempotency *scope* supplied by callers; they are
not taken from the opaque payload. Fingerprints are SHA-256 digests (32 bytes)
over a canonical JSON encoding of Queue-visible request fields only.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Final, Mapping

from queue_service.priority import (
    PriorityValidationError,
    validate_priority,
)
from queue_service.security.payload_policy import HARD_PAYLOAD_CEILING_BYTES

FINGERPRINT_SIZE_BYTES: Final[int] = 32
_IDEMPOTENCY_KEY_MAX_LEN: Final[int] = 256

_CODE_VALIDATION_FAILED: Final[str] = "validation_failed"
_CODE_IDEMPOTENCY_KEY_REQUIRED: Final[str] = "idempotency_key_required"
_CODE_PAYLOAD_TOO_LARGE: Final[str] = "payload_too_large"


class IntakeValidationError(Exception):
    """Stable non-retryable intake validation failure (Phase 3.1/3.2 codes).

    Messages and repr never include opaque payload content or credentials.
    """

    def __init__(
        self,
        code: str,
        message: str,
        *,
        retryable: bool = False,
        retry_after_ms: int | None = None,
        details: Mapping[str, Any] | None = None,
    ) -> None:
        self.code = code
        self.message = message
        self.retryable = retryable
        self.retry_after_ms = retry_after_ms
        self.details: dict[str, Any] = dict(details or {})
        super().__init__(message)

    def __repr__(self) -> str:
        return (
            "IntakeValidationError("
            f"code={self.code!r}, retryable={self.retryable}, "
            f"retry_after_ms={self.retry_after_ms!r}, "
            f"details={self.details!r})"
        )


@dataclass(frozen=True, slots=True)
class EnqueueCommand:
    """Immutable validated enqueue command ready for later persistence plans."""

    producer_id: str
    queue_name: str
    idempotency_key: str
    payload: Any
    priority: int
    available_at: datetime | None
    fingerprint: bytes = field(repr=False)


def normalize_enqueue_command(
    *,
    producer_id: str,
    queue_name: str,
    idempotency_key: str | None,
    payload: Any,
    priority: int | None = None,
    available_at: datetime | None = None,
) -> EnqueueCommand:
    """Validate and normalize an enqueue request; compute a 32-byte fingerprint.

    Does not open storage or perform HTTP I/O. Shape and timezone awareness for
    ``available_at`` are validated here; horizon eligibility uses Queue-store time
    in the service layer after idempotent replay resolution.
    """
    _require_scope(producer_id=producer_id, queue_name=queue_name)
    key = _normalize_idempotency_key(idempotency_key)
    normalized_priority = _normalize_priority(priority)
    normalized_available_at = _normalize_available_at(available_at)
    _validate_payload_shape_and_size(payload)

    fingerprint = _compute_fingerprint(
        payload=payload,
        priority=normalized_priority,
        available_at=normalized_available_at,
    )
    return EnqueueCommand(
        producer_id=producer_id,
        queue_name=queue_name,
        idempotency_key=key,
        payload=payload,
        priority=normalized_priority,
        available_at=normalized_available_at,
        fingerprint=fingerprint,
    )


def _require_scope(*, producer_id: str, queue_name: str) -> None:
    if not isinstance(producer_id, str) or not producer_id.strip():
        raise IntakeValidationError(
            _CODE_VALIDATION_FAILED,
            "producer_id is required from authenticated context",
        )
    if not isinstance(queue_name, str) or not queue_name.strip():
        raise IntakeValidationError(
            _CODE_VALIDATION_FAILED,
            "queue_name is required from runtime context",
        )


def _normalize_idempotency_key(idempotency_key: str | None) -> str:
    if not isinstance(idempotency_key, str):
        raise IntakeValidationError(
            _CODE_IDEMPOTENCY_KEY_REQUIRED,
            "Idempotency-Key is required",
        )
    if not (1 <= len(idempotency_key) <= _IDEMPOTENCY_KEY_MAX_LEN):
        raise IntakeValidationError(
            _CODE_IDEMPOTENCY_KEY_REQUIRED,
            "Idempotency-Key must be 1..256 characters",
        )
    if idempotency_key.strip() == "":
        raise IntakeValidationError(
            _CODE_IDEMPOTENCY_KEY_REQUIRED,
            "Idempotency-Key must be 1..256 characters",
        )
    return idempotency_key


def _normalize_priority(priority: int | None) -> int:
    if priority is None:
        raise IntakeValidationError(
            _CODE_VALIDATION_FAILED,
            "priority is required",
            details={"field": "priority"},
        )
    try:
        return validate_priority(priority, field="priority")
    except PriorityValidationError as exc:
        raise IntakeValidationError(
            _CODE_VALIDATION_FAILED,
            exc.message,
            details=exc.details,
        ) from exc


def _normalize_available_at(available_at: datetime | None) -> datetime | None:
    if available_at is None:
        return None
    if not isinstance(available_at, datetime):
        raise IntakeValidationError(
            _CODE_VALIDATION_FAILED,
            "available_at must be a datetime or null",
        )
    if available_at.tzinfo is None or available_at.utcoffset() is None:
        raise IntakeValidationError(
            _CODE_VALIDATION_FAILED,
            "available_at must be timezone-aware",
            details={"field": "available_at"},
        )
    return available_at


def _validate_payload_shape_and_size(payload: Any) -> None:
    try:
        canonical_payload = _canonicalize_json(payload)
    except (TypeError, ValueError):
        raise IntakeValidationError(
            _CODE_VALIDATION_FAILED,
            "payload must be a JSON value",
        ) from None
    encoded = _dump_canonical(canonical_payload)
    if len(encoded) > HARD_PAYLOAD_CEILING_BYTES:
        raise IntakeValidationError(
            _CODE_PAYLOAD_TOO_LARGE,
            "payload exceeds hard byte ceiling",
        )


def _compute_fingerprint(
    *,
    payload: Any,
    priority: int,
    available_at: datetime | None,
) -> bytes:
    document = {
        "available_at": _available_at_canonical(available_at),
        "payload": _canonicalize_json(payload),
        "priority": priority,
    }
    digest = hashlib.sha256(_dump_canonical(document)).digest()
    if len(digest) != FINGERPRINT_SIZE_BYTES:
        raise RuntimeError("SHA-256 digest must be 32 bytes")
    return digest


def _available_at_canonical(available_at: datetime | None) -> str | None:
    if available_at is None:
        return None
    return available_at.astimezone(tz=available_at.tzinfo).isoformat()


def _canonicalize_json(value: Any) -> Any:
    if value is None or isinstance(value, bool):
        return value
    if isinstance(value, int) and not isinstance(value, bool):
        return value
    if isinstance(value, float):
        if value != value or value in (float("inf"), float("-inf")):
            raise ValueError("non-finite float is not JSON")
        return value
    if isinstance(value, str):
        return value
    if isinstance(value, list):
        return [_canonicalize_json(item) for item in value]
    if isinstance(value, dict):
        # Reject non-string keys; sort for determinism.
        out: dict[str, Any] = {}
        for raw_key, raw_val in value.items():
            if not isinstance(raw_key, str):
                raise TypeError("JSON object keys must be strings")
            out[raw_key] = _canonicalize_json(raw_val)
        return {key: out[key] for key in sorted(out)}
    raise TypeError("unsupported JSON value type")


def _dump_canonical(value: Any) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        separators=(",", ":"),
        allow_nan=False,
        sort_keys=True,
    ).encode("utf-8")
