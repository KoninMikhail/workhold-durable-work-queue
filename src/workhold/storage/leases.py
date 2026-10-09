"""Phase 3.5 current-lease fence re-export for terminal commands.

The authoritative implementation lives in
``workhold.infrastructure.postgres.lease_repository``. Terminal handlers
must call through this module rather than duplicating fence SQL.
"""

from __future__ import annotations

from uuid import UUID

from sqlalchemy.orm import Session

from workhold.infrastructure.postgres.lease_repository import (
    FenceDecision,
    LeaseFenceResult,
    LeaseRepository,
)

__all__ = [
    "FenceDecision",
    "LeaseFenceResult",
    "validate_current_lease",
]


def validate_current_lease(
    session: Session,
    *,
    claim_id: UUID,
    claim_token: UUID,
    generation: int,
    for_update: bool = True,
) -> LeaseFenceResult:
    """Return CURRENT or STALE for the full fence; never mutates rows."""
    return LeaseRepository().validate_current_lease(
        session,
        claim_id=claim_id,
        claim_token=claim_token,
        generation=generation,
        for_update=for_update,
    )
