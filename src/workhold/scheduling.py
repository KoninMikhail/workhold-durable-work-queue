"""Shared Queue-store-time scheduling policy for producer enqueue and Complete spawn.

Resolves omitted/null ``available_at`` against a caller-supplied store timestamp,
classifies immediate versus delayed work, and rejects naive or over-horizon values.
This module never reads wall clock or SQL; callers supply ``store_now`` from the
Queue store transaction.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Final, Mapping

_CODE_VALIDATION_FAILED: Final[str] = "validation_failed"


class SchedulingValidationError(Exception):
    """Stable non-retryable scheduling validation failure.

    Messages and repr never include opaque payloads, credentials, or raw request
    bodies.
    """

    def __init__(
        self,
        code: str,
        message: str,
        *,
        retryable: bool = False,
        details: Mapping[str, Any] | None = None,
    ) -> None:
        self.code = code
        self.message = message
        self.retryable = retryable
        self.details: dict[str, Any] = dict(details or {})
        super().__init__(message)

    def __repr__(self) -> str:
        return (
            "SchedulingValidationError("
            f"code={self.code!r}, retryable={self.retryable}, "
            f"details={self.details!r})"
        )


@dataclass(frozen=True, slots=True)
class SchedulingDecision:
    """Resolved scheduling outcome for one enqueue or spawn item."""

    available_at: datetime
    is_delayed: bool


@dataclass(frozen=True, slots=True)
class SchedulingPolicy:
    """Deployment-bounded scheduling policy evaluated at Queue-store time."""

    horizon_seconds: int

    def resolve(
        self,
        available_at: datetime | None,
        *,
        store_now: datetime,
    ) -> SchedulingDecision:
        _require_timezone_aware(store_now, field="store_now")

        if available_at is None:
            return SchedulingDecision(available_at=store_now, is_delayed=False)

        _require_timezone_aware(available_at, field="available_at")

        resolved = available_at
        is_delayed = resolved > store_now
        horizon_limit = store_now + timedelta(seconds=self.horizon_seconds)
        if resolved > horizon_limit:
            raise SchedulingValidationError(
                _CODE_VALIDATION_FAILED,
                "available_at exceeds deployment scheduling horizon",
                retryable=False,
                details={
                    "field": "available_at",
                    "limit": self.horizon_seconds,
                },
            )

        return SchedulingDecision(available_at=resolved, is_delayed=is_delayed)


def _require_timezone_aware(value: datetime, *, field: str) -> None:
    if value.tzinfo is None or value.utcoffset() is None:
        raise SchedulingValidationError(
            _CODE_VALIDATION_FAILED,
            f"{field} must be timezone-aware",
            retryable=False,
            details={"field": field},
        )
