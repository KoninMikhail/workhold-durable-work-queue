"""Delivery Outbox domain constants and command types (Phase 5).

Physical PostgreSQL column names follow the Phase 3.1 compact contract
(``ordinal``, ``generation``, ``current_claim_id``, ``claimed_at``,
``lease_expires_at``, ``state_code``). Logical relay names from the Phase 5
plan map as:

- completion_ordinal → ``ordinal``
- relay_claim_token → ``current_claim_id``
- relay_generation → ``generation``
- relay_claimed_at → ``claimed_at``
- relay_lease_expires_at → ``lease_expires_at``
- terminal_outcome → ``state_code`` (10 published / 11 dead_lettered)
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Final, Mapping

# Active lifecycle (delivery_events_active.state_code).
STATE_PENDING: Final[int] = 1
STATE_PUBLISHING: Final[int] = 2

# Terminal lifecycle (delivery_events_terminal.state_code).
STATE_PUBLISHED: Final[int] = 10
STATE_DEAD_LETTERED: Final[int] = 11

TERMINAL_OUTCOME_PUBLISHED: Final[str] = "published"
TERMINAL_OUTCOME_DEAD_LETTERED: Final[str] = "dead_lettered"

_OUTCOME_TO_STATE: Final[Mapping[str, int]] = {
    TERMINAL_OUTCOME_PUBLISHED: STATE_PUBLISHED,
    TERMINAL_OUTCOME_DEAD_LETTERED: STATE_DEAD_LETTERED,
}

# completion_effects.effect_kind_code — 1=spawn, 2=delivery event.
EFFECT_KIND_EVENT: Final[int] = 2

RELAY_PRINCIPAL_ID_MAX: Final[int] = 128
FAILURE_CODE_MAX: Final[int] = 128


@dataclass(frozen=True, slots=True)
class EventCommand:
    """One validated CompleteRequest.events[] Delivery Outbox intent (in-process)."""

    source: str
    type: str
    subject: str | None
    datacontenttype: str | None
    data: Any
    extensions: Mapping[str, Any]
    canonical: Mapping[str, Any]


def terminal_outcome_to_state_code(outcome: str) -> int:
    """Map terminal outcome label to Phase 3.1 state_code."""
    try:
        return _OUTCOME_TO_STATE[outcome]
    except KeyError as exc:
        raise ValueError(f"unknown terminal_outcome={outcome!r}") from exc


def state_code_to_terminal_outcome(state_code: int) -> str:
    """Map Phase 3.1 terminal state_code to outcome label."""
    if int(state_code) == STATE_PUBLISHED:
        return TERMINAL_OUTCOME_PUBLISHED
    if int(state_code) == STATE_DEAD_LETTERED:
        return TERMINAL_OUTCOME_DEAD_LETTERED
    raise ValueError(f"unknown terminal state_code={state_code!r}")
