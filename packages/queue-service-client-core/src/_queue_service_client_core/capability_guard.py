"""Fail-closed capability gates for reserved protocol features."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

from _queue_service_client_core.capabilities import Capabilities

__all__ = [
    "require_batch_claim",
    "require_delivery_events",
    "require_long_polling",
]


def require_long_polling(capabilities: Capabilities, wait_seconds: int) -> None:
    """Require authenticated live long-polling for positive waits.

    ``wait_seconds == 0`` bypasses the feature guard (immediate claim).
    Malformed waits and incomplete advertisements fail closed before HTTP.
    """

    if isinstance(wait_seconds, bool) or type(wait_seconds) is not int:
        raise ValueError("wait_seconds must be an integer")
    if wait_seconds < 0:
        raise ValueError("wait_seconds must be >= 0")
    if wait_seconds == 0:
        return
    if not capabilities.long_polling:
        raise ValueError("long_polling capability is not enabled")
    if capabilities.max_wait_seconds <= 0:
        raise ValueError(
            "max_wait_seconds must be positive when long_polling is enabled"
        )
    if wait_seconds > capabilities.max_wait_seconds:
        raise ValueError(
            f"wait_seconds {wait_seconds} exceeds max_wait_seconds "
            f"{capabilities.max_wait_seconds}"
        )


def require_batch_claim(capabilities: Capabilities, max_tasks: int) -> None:
    """Fail closed for ``max_tasks>1`` unless live ``batch_claim`` advertises it.

    ``max_tasks == 1`` is always the MVP path and does not require batch_claim.
    """

    if isinstance(max_tasks, bool) or type(max_tasks) is not int:
        raise ValueError("max_tasks must be an integer")
    if max_tasks < 1:
        raise ValueError("max_tasks must be >= 1")
    if max_tasks == 1:
        if not capabilities.batch_claim and capabilities.max_claim_tasks != 1:
            raise ValueError("max_claim_tasks must be 1 while batch_claim is disabled")
        if capabilities.batch_claim and capabilities.max_claim_tasks < 1:
            raise ValueError(
                "max_claim_tasks must be positive when batch_claim is enabled"
            )
        return
    if not capabilities.batch_claim:
        raise ValueError("batch_claim capability is not enabled")
    if capabilities.max_claim_tasks < 1:
        raise ValueError("max_claim_tasks must be positive when batch_claim is enabled")
    if max_tasks > capabilities.max_claim_tasks:
        raise ValueError(
            f"max_tasks {max_tasks} exceeds max_claim_tasks "
            f"{capabilities.max_claim_tasks}"
        )


def require_delivery_events(
    capabilities: Capabilities,
    events: Sequence[Mapping[str, Any]] | None,
) -> None:
    """Fail closed for non-empty ``complete.events[]`` unless advertised.

    Empty/omitted ``events`` bypasses the guard (MVP complete without events).
    When enabled, each event must be a JSON object (reserved OpenAPI shape).
    """

    if events is None:
        return
    if not isinstance(events, Sequence) or isinstance(events, (str, bytes, bytearray)):
        raise ValueError("events must be a sequence of objects")
    if len(events) == 0:
        return
    if not capabilities.delivery_events:
        raise ValueError("delivery_events capability is not enabled")
    for index, event in enumerate(events):
        if not isinstance(event, Mapping):
            raise ValueError(f"events[{index}] must be an object")
