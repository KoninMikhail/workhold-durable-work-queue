"""Producer cancelTask request schemas (Phase 3.1 CancelTaskRequest)."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Final

from workhold.intake.contracts import IntakeValidationError

_CANCEL_BODY_KEYS: Final[frozenset[str]] = frozenset({"reason"})
_REASON_MAX_LEN: Final[int] = 1024


@dataclass(frozen=True, slots=True)
class CancelCommand:
    """Validated cancel body; path task_id and bearer producer are separate."""

    reason: str | None
    reason_present: bool


def parse_cancel_command(parsed: Mapping[str, Any]) -> CancelCommand:
    """Deserialize the closed CancelTaskRequest object (body optional upstream)."""

    if not isinstance(parsed, Mapping):
        raise IntakeValidationError(
            "validation_failed",
            "request body must be an object",
        )
    unknown = sorted(set(parsed) - _CANCEL_BODY_KEYS)
    if unknown:
        raise IntakeValidationError(
            "validation_failed",
            "request body contains unsupported fields",
            details={"rejected_fields": unknown},
        )

    reason_present = "reason" in parsed
    reason: str | None
    if not reason_present:
        reason = None
    else:
        raw = parsed["reason"]
        if raw is None:
            reason = None
        elif not isinstance(raw, str):
            raise IntakeValidationError(
                "validation_failed",
                "reason must be a string or null",
            )
        elif len(raw) > _REASON_MAX_LEN:
            raise IntakeValidationError(
                "validation_failed",
                "reason exceeds 1024 Unicode code points",
                details={"limit": _REASON_MAX_LEN, "observed": len(raw)},
            )
        else:
            reason = raw

    return CancelCommand(reason=reason, reason_present=reason_present)


def format_task_datetime(value: datetime) -> str:
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
