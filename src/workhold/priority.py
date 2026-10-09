"""Authoritative WORK-16 signed-smallint priority bounds and strict validation."""

from __future__ import annotations

from typing import Any, Final, Mapping

PRIORITY_MIN: Final[int] = -32_768
PRIORITY_DEFAULT: Final[int] = 0
PRIORITY_MAX: Final[int] = 32_767


class PriorityValidationError(Exception):
    """Strict priority validation failure with safe, structured details."""

    def __init__(
        self,
        message: str,
        *,
        details: Mapping[str, Any] | None = None,
    ) -> None:
        self.message = message
        self.details: dict[str, Any] = dict(details or {})
        super().__init__(message)


def validate_priority(
    value: object,
    *,
    field: str = "priority",
    index: int | None = None,
) -> int:
    """Validate untrusted priority using exact Python integer semantics.

    Never coerces with ``int()``; rejects ``bool``, float, string, null, and
    values outside the inclusive signed ``smallint`` range.
    """
    details: dict[str, Any] = {"field": field}
    if index is not None:
        details["index"] = index

    if value is None:
        raise PriorityValidationError(
            f"{field} is required",
            details=details,
        )

    if type(value) is not int or isinstance(value, bool):
        raise PriorityValidationError(
            f"{field} must be an integer",
            details=details,
        )

    if value < PRIORITY_MIN or value > PRIORITY_MAX:
        raise PriorityValidationError(
            f"{field} must be between {PRIORITY_MIN} and {PRIORITY_MAX} inclusive",
            details={**details, "min": PRIORITY_MIN, "max": PRIORITY_MAX},
        )

    return value
