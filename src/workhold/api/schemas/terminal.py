"""Terminal worker command schemas and fingerprint helpers (fail / ack_cancel / complete)."""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Final, Mapping

from workhold.delivery.models import EventCommand
from workhold.intake.contracts import IntakeValidationError
from workhold.priority import PriorityValidationError, validate_priority

FINGERPRINT_SIZE_BYTES: Final[int] = 32
FAILURE_CODE_RE: Final[re.Pattern[str]] = re.compile(r"^[a-z][a-z0-9._-]{0,127}$")
FAILURE_CODE_MAX: Final[int] = 128
FAILURE_DETAIL_MAX_CHARS: Final[int] = 4096

_FAIL_BODY_KEYS: Final[frozenset[str]] = frozenset(
    {"generation", "retryable", "failure_code", "failure_detail"}
)
_ACK_CANCEL_BODY_KEYS: Final[frozenset[str]] = frozenset({"generation"})
_COMPLETE_BODY_KEYS: Final[frozenset[str]] = frozenset({"generation", "spawn"})
_COMPLETE_RESERVED_KEYS: Final[frozenset[str]] = frozenset({"events"})
_SPAWN_ITEM_KEYS: Final[frozenset[str]] = frozenset(
    {"queue_name", "idempotency_key", "payload", "priority", "available_at"}
)
_SPAWN_REQUIRED_KEYS: Final[frozenset[str]] = frozenset(
    {"queue_name", "idempotency_key", "payload", "priority"}
)
_MAX_SPAWN_ITEMS: Final[int] = 64
_MAX_SPAWN_FANOUT_BYTES: Final[int] = 512 * 1024
_DEFAULT_SPAWN_PAYLOAD_BYTES: Final[int] = 256 * 1024
_IDEMPOTENCY_KEY_MAX: Final[int] = 256
_QUEUE_NAME_MAX: Final[int] = 128
_QUEUE_NAME_RE: Final[re.Pattern[str]] = re.compile(r"^[a-z0-9][a-z0-9._-]*$")


@dataclass(frozen=True, slots=True)
class FailCommand:
    """Validated fail body ready for persistence (path claim_id + header token separate)."""

    generation: int
    retryable: bool
    failure_code: str
    failure_detail: str | None
    failure_detail_present: bool
    fingerprint: bytes


def parse_fail_command(parsed: Mapping[str, Any]) -> FailCommand:
    """Deserialize and canonicalize FailRequest; reject noncanonical codes/details."""
    unknown = sorted(set(parsed) - _FAIL_BODY_KEYS)
    if unknown:
        raise IntakeValidationError(
            "validation_failed",
            "request body contains unsupported fields",
            details={"rejected_fields": unknown},
        )
    for key in ("generation", "retryable", "failure_code"):
        if key not in parsed:
            raise IntakeValidationError("validation_failed", f"{key} is required")

    generation = parsed["generation"]
    if type(generation) is not int or isinstance(generation, bool) or generation < 1:
        raise IntakeValidationError(
            "validation_failed",
            "generation must be an integer >= 1",
        )

    retryable = parsed["retryable"]
    if type(retryable) is not bool:
        raise IntakeValidationError(
            "validation_failed",
            "retryable must be a boolean",
        )

    failure_code = parsed["failure_code"]
    if not isinstance(failure_code, str):
        raise IntakeValidationError(
            "validation_failed",
            "failure_code must be a string",
        )
    if len(failure_code) < 1 or len(failure_code) > FAILURE_CODE_MAX:
        raise IntakeValidationError(
            "validation_failed",
            "failure_code must be 1..128 lowercase ASCII characters",
            details={"limit": FAILURE_CODE_MAX, "observed": len(failure_code)},
        )
    if FAILURE_CODE_RE.fullmatch(failure_code) is None:
        raise IntakeValidationError(
            "validation_failed",
            "failure_code must match ^[a-z][a-z0-9._-]{0,127}$",
        )

    detail_present = "failure_detail" in parsed
    failure_detail: str | None
    if not detail_present:
        failure_detail = None
    else:
        raw_detail = parsed["failure_detail"]
        if raw_detail is None:
            failure_detail = None
        elif not isinstance(raw_detail, str):
            raise IntakeValidationError(
                "validation_failed",
                "failure_detail must be a string or null",
            )
        else:
            if len(raw_detail) > FAILURE_DETAIL_MAX_CHARS:
                raise IntakeValidationError(
                    "validation_failed",
                    "failure_detail exceeds 4096 Unicode code points",
                    details={
                        "limit": FAILURE_DETAIL_MAX_CHARS,
                        "observed": len(raw_detail),
                    },
                )
            failure_detail = raw_detail

    fingerprint = compute_fail_fingerprint(
        retryable=retryable,
        failure_code=failure_code,
        failure_detail=failure_detail,
        failure_detail_present=detail_present,
    )
    return FailCommand(
        generation=generation,
        retryable=retryable,
        failure_code=failure_code,
        failure_detail=failure_detail,
        failure_detail_present=detail_present,
        fingerprint=fingerprint,
    )


def compute_fail_fingerprint(
    *,
    retryable: bool,
    failure_code: str,
    failure_detail: str | None,
    failure_detail_present: bool,
) -> bytes:
    """SHA-256 over canonical JSON of retryability, code, and detail presence/value."""
    document: dict[str, Any] = {
        "failure_code": failure_code,
        "retryable": retryable,
    }
    if failure_detail_present:
        document["failure_detail"] = failure_detail
    digest = hashlib.sha256(_dump_canonical(_canonicalize_json(document))).digest()
    if len(digest) != FINGERPRINT_SIZE_BYTES:
        raise RuntimeError("SHA-256 digest must be 32 bytes")
    return digest


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


@dataclass(frozen=True, slots=True)
class AckCancelCommand:
    """Validated ack_cancel body (path claim_id + header token separate)."""

    generation: int
    fingerprint: bytes


def parse_ack_cancel_command(parsed: Mapping[str, Any]) -> AckCancelCommand:
    """Deserialize and canonicalize AckCancelRequest; reject unknown fields."""
    unknown = sorted(set(parsed) - _ACK_CANCEL_BODY_KEYS)
    if unknown:
        raise IntakeValidationError(
            "validation_failed",
            "request body contains unsupported fields",
            details={"rejected_fields": unknown},
        )
    if "generation" not in parsed:
        raise IntakeValidationError("validation_failed", "generation is required")

    generation = parsed["generation"]
    if type(generation) is not int or isinstance(generation, bool) or generation < 1:
        raise IntakeValidationError(
            "validation_failed",
            "generation must be an integer >= 1",
        )

    fingerprint = compute_ack_cancel_fingerprint(generation=generation)
    return AckCancelCommand(generation=generation, fingerprint=fingerprint)


def compute_ack_cancel_fingerprint(*, generation: int) -> bytes:
    """SHA-256 over the closed AckCancelRequest body (generation only)."""
    digest = hashlib.sha256(
        _dump_canonical(_canonicalize_json({"generation": generation}))
    ).digest()
    if len(digest) != FINGERPRINT_SIZE_BYTES:
        raise RuntimeError("SHA-256 digest must be 32 bytes")
    return digest


@dataclass(frozen=True, slots=True)
class SpawnCommand:
    """One validated CompleteRequest.spawn[] Work Queue descendant."""

    queue_name: str
    idempotency_key: str
    payload: Any
    priority: int
    available_at: datetime | None
    canonical: Mapping[str, Any]


@dataclass(frozen=True, slots=True)
class CompleteCommand:
    """Validated CompleteRequest with ordered spawn[] (path claim_id + header token separate).

    ``events`` is admitted for in-process Complete persistence (Phase 5). The HTTP
    parser still rejects the reserved ``events`` key until OpenAPI activates it.
    """

    generation: int
    spawn: tuple[SpawnCommand, ...]
    fingerprint: bytes
    events: tuple[EventCommand, ...] = ()


def parse_complete_command(parsed: Mapping[str, Any]) -> CompleteCommand:
    """Deserialize CompleteRequest; reject reserved ``events``; admit 0..64 spawns."""
    reserved = sorted(set(parsed) & _COMPLETE_RESERVED_KEYS)
    if reserved:
        raise IntakeValidationError(
            "validation_failed",
            "request body contains reserved unsupported fields",
            details={"rejected_fields": reserved},
        )
    unknown = sorted(set(parsed) - _COMPLETE_BODY_KEYS)
    if unknown:
        raise IntakeValidationError(
            "validation_failed",
            "request body contains unsupported fields",
            details={"rejected_fields": unknown},
        )
    for key in ("generation", "spawn"):
        if key not in parsed:
            raise IntakeValidationError("validation_failed", f"{key} is required")

    generation = parsed["generation"]
    if type(generation) is not int or isinstance(generation, bool) or generation < 1:
        raise IntakeValidationError(
            "validation_failed",
            "generation must be an integer >= 1",
        )

    spawn_raw = parsed["spawn"]
    if not isinstance(spawn_raw, list):
        raise IntakeValidationError(
            "validation_failed",
            "spawn must be an array",
        )
    if len(spawn_raw) > _MAX_SPAWN_ITEMS:
        raise IntakeValidationError(
            "validation_failed",
            "spawn exceeds hard item ceiling",
            details={"limit": _MAX_SPAWN_ITEMS, "observed": len(spawn_raw)},
        )

    spawn_items: list[SpawnCommand] = []
    fanout_bytes = 0
    for index, item in enumerate(spawn_raw):
        spawn_items.append(_parse_spawn_item(item, index=index))
        fanout_bytes += _payload_utf8_bytes(spawn_items[-1].payload)
        if fanout_bytes > _MAX_SPAWN_FANOUT_BYTES:
            raise IntakeValidationError(
                "payload_too_large",
                "spawn fan-out exceeds hard byte ceiling",
                details={
                    "limit_bytes": _MAX_SPAWN_FANOUT_BYTES,
                    "observed_bytes": fanout_bytes,
                },
            )

    fingerprint = compute_complete_fingerprint(
        spawn=[dict(item.canonical) for item in spawn_items],
        events=[],
    )
    return CompleteCommand(
        generation=generation,
        spawn=tuple(spawn_items),
        events=(),
        fingerprint=fingerprint,
    )


def compute_complete_fingerprint(
    *,
    spawn: tuple[Any, ...] | list[Any],
    events: tuple[Any, ...] | list[Any] = (),
) -> bytes:
    """SHA-256 over canonical JSON of spawn (+ events when non-empty).

    Empty ``events`` keeps the Phase 3.7 spawn-only fingerprint bytes so
    existing zero-event Completes remain compatible. Non-empty ``events`` are
    included so same-claim/changed-body Complete conflicts cover Delivery
    Outbox intent (COMP-02 / COMP-03).
    """
    payload: dict[str, Any] = {"spawn": list(spawn)}
    if events:
        payload["events"] = list(events)
    digest = hashlib.sha256(_dump_canonical(_canonicalize_json(payload))).digest()
    if len(digest) != FINGERPRINT_SIZE_BYTES:
        raise RuntimeError("SHA-256 digest must be 32 bytes")
    return digest


def _parse_spawn_item(item: Any, *, index: int) -> SpawnCommand:
    if not isinstance(item, Mapping):
        raise IntakeValidationError(
            "validation_failed",
            "spawn item must be an object",
            details={"index": index},
        )
    unknown = sorted(set(item) - _SPAWN_ITEM_KEYS)
    if unknown:
        raise IntakeValidationError(
            "validation_failed",
            "spawn item contains unsupported fields",
            details={"index": index, "rejected_fields": unknown},
        )
    missing = sorted(_SPAWN_REQUIRED_KEYS - set(item))
    if missing:
        raise IntakeValidationError(
            "validation_failed",
            "spawn item is missing required fields",
            details={"index": index, "missing_fields": missing},
        )

    queue_name = item["queue_name"]
    if (
        not isinstance(queue_name, str)
        or not (1 <= len(queue_name) <= _QUEUE_NAME_MAX)
        or _QUEUE_NAME_RE.fullmatch(queue_name) is None
    ):
        raise IntakeValidationError(
            "validation_failed",
            "spawn queue_name must match the named-queue pattern",
            details={"index": index},
        )

    idempotency_key = item["idempotency_key"]
    if (
        not isinstance(idempotency_key, str)
        or not (1 <= len(idempotency_key) <= _IDEMPOTENCY_KEY_MAX)
        or idempotency_key.strip() == ""
    ):
        raise IntakeValidationError(
            "validation_failed",
            "spawn idempotency_key must be 1..256 characters",
            details={"index": index},
        )

    try:
        priority = validate_priority(item["priority"], field="priority", index=index)
    except PriorityValidationError as exc:
        raise IntakeValidationError(
            "validation_failed",
            exc.message,
            details=exc.details,
        ) from exc

    parsed_available_at: datetime | None
    if "available_at" in item:
        parsed_available_at = _parse_spawn_available_at(
            item["available_at"],
            index=index,
        )
    else:
        parsed_available_at = None

    payload = item["payload"]
    payload_bytes = _payload_utf8_bytes(payload)
    if payload_bytes > _DEFAULT_SPAWN_PAYLOAD_BYTES:
        raise IntakeValidationError(
            "payload_too_large",
            "spawn payload exceeds hard byte ceiling",
            details={
                "index": index,
                "limit_bytes": _DEFAULT_SPAWN_PAYLOAD_BYTES,
                "observed_bytes": payload_bytes,
            },
        )

    canonical: dict[str, Any] = {
        "idempotency_key": idempotency_key,
        "payload": payload,
        "priority": priority,
        "queue_name": queue_name,
    }
    if parsed_available_at is not None:
        canonical["available_at"] = _spawn_available_at_canonical(parsed_available_at)

    return SpawnCommand(
        queue_name=queue_name,
        idempotency_key=idempotency_key,
        payload=payload,
        priority=priority,
        available_at=parsed_available_at,
        canonical=canonical,
    )


def _parse_spawn_available_at(raw: Any, *, index: int) -> datetime | None:
    if raw is None:
        return None
    if isinstance(raw, datetime):
        if raw.tzinfo is None or raw.utcoffset() is None:
            raise IntakeValidationError(
                "validation_failed",
                "available_at must be timezone-aware",
                details={"index": index, "field": "available_at"},
            )
        return raw
    if not isinstance(raw, str):
        raise IntakeValidationError(
            "validation_failed",
            "available_at must be a datetime or null",
            details={"index": index, "field": "available_at"},
        )
    text = raw.strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError as exc:
        raise IntakeValidationError(
            "validation_failed",
            "available_at must be a valid date-time",
            details={"index": index, "field": "available_at"},
        ) from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise IntakeValidationError(
            "validation_failed",
            "available_at must be timezone-aware",
            details={"index": index, "field": "available_at"},
        )
    return parsed


def _spawn_available_at_canonical(available_at: datetime) -> str:
    return available_at.astimezone(tz=available_at.tzinfo).isoformat()


def _payload_utf8_bytes(payload: Any) -> int:
    try:
        return len(_dump_canonical(_canonicalize_json(payload)))
    except (TypeError, ValueError) as exc:
        raise IntakeValidationError(
            "validation_failed",
            "spawn payload must be a JSON value",
        ) from exc
