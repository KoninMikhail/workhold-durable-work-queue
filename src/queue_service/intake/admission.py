"""Pre-transaction producer enqueue byte and cardinality admission.

Consumes Phase 3.1 OpenAPI enqueue bounds and Phase 3.2 payload ceilings.
Validation is pure: no SQLAlchemy session, storage mutation, or hashing side
effects. Oversized keys/payloads/bodies/cardinality fail closed before any
write transaction.
"""

from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, Final

from queue_service.intake.contracts import IntakeValidationError
from queue_service.security.payload_policy import (
    DEFAULT_PAYLOAD_BYTES,
    HARD_PAYLOAD_CEILING_BYTES,
)
from queue_service.settings import QUALIFIED_REQUEST_MAX_BYTES

# Phase 3.1 OpenAPI / admission-control.md baselines, locked by Phase 3.9 QUAL-03.
DEFAULT_PAYLOAD_MAX_BYTES: Final[int] = DEFAULT_PAYLOAD_BYTES  # 256 KiB default
DEFAULT_REQUEST_MAX_BYTES: Final[int] = QUALIFIED_REQUEST_MAX_BYTES  # 1 MiB qualified
DEFAULT_IDEMPOTENCY_KEY_MAX_CHARS: Final[int] = 256

if DEFAULT_REQUEST_MAX_BYTES > HARD_PAYLOAD_CEILING_BYTES:
    raise RuntimeError("qualified request max exceeds hard payload ceiling")

_ALLOWED_BODY_KEYS: Final[frozenset[str]] = frozenset(
    {"payload", "priority", "available_at"}
)
_FAN_OUT_KEYS: Final[frozenset[str]] = frozenset(
    {"spawn", "events", "delivery_events"}
)

_CODE_VALIDATION_FAILED: Final[str] = "validation_failed"
_CODE_PAYLOAD_TOO_LARGE: Final[str] = "payload_too_large"
_CODE_IDEMPOTENCY_KEY_REQUIRED: Final[str] = "idempotency_key_required"


@dataclass(frozen=True, slots=True)
class EnqueueAdmissionLimits:
    """Deployment hard ceilings that runtime policy may only tighten."""

    max_payload_bytes: int = DEFAULT_PAYLOAD_MAX_BYTES
    max_request_bytes: int = DEFAULT_REQUEST_MAX_BYTES
    max_idempotency_key_chars: int = DEFAULT_IDEMPOTENCY_KEY_MAX_CHARS

    def __post_init__(self) -> None:
        if not (1 <= self.max_payload_bytes <= HARD_PAYLOAD_CEILING_BYTES):
            raise ValueError(
                "max_payload_bytes must be between 1 and "
                f"{HARD_PAYLOAD_CEILING_BYTES} inclusive"
            )
        if not (1 <= self.max_request_bytes <= HARD_PAYLOAD_CEILING_BYTES):
            raise ValueError(
                "max_request_bytes must be between 1 and "
                f"{HARD_PAYLOAD_CEILING_BYTES} inclusive"
            )
        if self.max_payload_bytes > self.max_request_bytes:
            raise ValueError("max_payload_bytes cannot exceed max_request_bytes")
        if not (1 <= self.max_idempotency_key_chars <= DEFAULT_IDEMPOTENCY_KEY_MAX_CHARS):
            raise ValueError(
                "max_idempotency_key_chars must be between 1 and "
                f"{DEFAULT_IDEMPOTENCY_KEY_MAX_CHARS} inclusive"
            )


def validate_producer_enqueue_admission(
    *,
    idempotency_key: str | None,
    body: Mapping[str, Any],
    body_bytes: bytes,
    limits: EnqueueAdmissionLimits | None = None,
) -> None:
    """Reject unsafe producer enqueue inputs before any storage mutation.

    Raises:
        IntakeValidationError: stable Phase 3.1/3.2 codes with ``retryable=false``.
    """
    effective = limits or EnqueueAdmissionLimits()
    _validate_idempotency_key(idempotency_key, effective.max_idempotency_key_chars)
    _validate_body_shape(body)
    _validate_request_bytes(body_bytes, effective.max_request_bytes)
    _validate_payload_bytes(body.get("payload"), effective.max_payload_bytes)


def _validate_idempotency_key(idempotency_key: str | None, max_chars: int) -> None:
    if not isinstance(idempotency_key, str):
        raise IntakeValidationError(
            _CODE_IDEMPOTENCY_KEY_REQUIRED,
            "Idempotency-Key is required",
        )
    if not (1 <= len(idempotency_key) <= max_chars) or idempotency_key.strip() == "":
        raise IntakeValidationError(
            _CODE_IDEMPOTENCY_KEY_REQUIRED,
            f"Idempotency-Key must be 1..{max_chars} characters",
        )


def _validate_body_shape(body: Mapping[str, Any]) -> None:
    if not isinstance(body, Mapping):
        raise IntakeValidationError(
            _CODE_VALIDATION_FAILED,
            "request body must be an object",
        )
    keys = set(body.keys())
    smuggled = keys & _FAN_OUT_KEYS
    if smuggled:
        raise IntakeValidationError(
            _CODE_PAYLOAD_TOO_LARGE,
            "external enqueue cannot include spawn or delivery-event fan-out",
            details={"rejected_fields": sorted(smuggled)},
        )
    unknown = keys - _ALLOWED_BODY_KEYS
    if unknown:
        raise IntakeValidationError(
            _CODE_VALIDATION_FAILED,
            "request body contains unsupported fields",
            details={"rejected_fields": sorted(unknown)},
        )
    if "payload" not in body:
        raise IntakeValidationError(
            _CODE_VALIDATION_FAILED,
            "payload is required",
        )


def _validate_request_bytes(body_bytes: bytes, max_request_bytes: int) -> None:
    if not isinstance(body_bytes, (bytes, bytearray)):
        raise IntakeValidationError(
            _CODE_VALIDATION_FAILED,
            "request body bytes are required",
        )
    size = len(body_bytes)
    if size > max_request_bytes:
        raise IntakeValidationError(
            _CODE_PAYLOAD_TOO_LARGE,
            "request body exceeds hard byte ceiling",
            details={
                "limit_bytes": max_request_bytes,
                "observed_bytes": size,
            },
        )


def _validate_payload_bytes(payload: Any, max_payload_bytes: int) -> None:
    try:
        encoded = _encode_json_value(payload)
    except (TypeError, ValueError):
        raise IntakeValidationError(
            _CODE_VALIDATION_FAILED,
            "payload must be a JSON value",
        ) from None
    size = len(encoded)
    if size > max_payload_bytes:
        raise IntakeValidationError(
            _CODE_PAYLOAD_TOO_LARGE,
            "payload exceeds hard byte ceiling",
            details={
                "limit_bytes": max_payload_bytes,
                "observed_bytes": size,
            },
        )


def _encode_json_value(value: Any) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        separators=(",", ":"),
        allow_nan=False,
        sort_keys=True,
    ).encode("utf-8")
