"""Distribution-local WORK-16 signed-smallint priority bounds and strict validation."""

from __future__ import annotations

from typing import Final

PRIORITY_MIN: Final[int] = -32_768
PRIORITY_DEFAULT: Final[int] = 0
PRIORITY_MAX: Final[int] = 32_767


def validate_priority(value: object, *, field: str = "priority") -> int:
    """Validate priority using exact Python integer semantics.

    Never coerces with ``int()``; rejects ``bool``, float, string, null, and
    values outside the inclusive signed ``smallint`` range.
    """

    if value is None:
        raise ValueError(f"{field} is required")

    if type(value) is not int or isinstance(value, bool):
        raise ValueError(f"{field} must be an integer")

    if value < PRIORITY_MIN or value > PRIORITY_MAX:
        raise ValueError(
            f"{field} must be between {PRIORITY_MIN} and {PRIORITY_MAX} inclusive"
        )

    return value
