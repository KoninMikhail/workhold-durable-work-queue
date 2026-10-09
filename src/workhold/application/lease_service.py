"""Lease use-case boundary over the PostgreSQL current-lease fence."""

from __future__ import annotations

from collections.abc import Callable
from uuid import UUID

from sqlalchemy.orm import Session

from workhold.infrastructure.postgres.lease_repository import (
    HeartbeatPersistenceResult,
    LeaseFenceResult,
    LeaseRepository,
)


class LeaseService:
    """Commit one heartbeat transaction after the shared current-lease fence."""

    def __init__(
        self,
        *,
        session_factory: Callable[[], Session],
        repository: LeaseRepository | None = None,
    ) -> None:
        self._session_factory = session_factory
        self._repository = repository if repository is not None else LeaseRepository()

    def probe_current_lease(
        self,
        *,
        claim_id: UUID,
        claim_token: UUID,
        generation: int,
    ) -> LeaseFenceResult:
        """Return the shared CURRENT/STALE fence decision without mutation.

        Later fail/ack-cancel/complete handlers must reuse this authority rather
        than duplicating a weaker check.
        """
        session = self._session_factory()
        try:
            result = self._repository.validate_current_lease(
                session,
                claim_id=claim_id,
                claim_token=claim_token,
                generation=generation,
                for_update=False,
            )
            session.rollback()
            return result
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    def heartbeat(
        self,
        *,
        claim_id: UUID,
        claim_token: UUID,
        generation: int,
        lease_seconds: int,
        authorize_queue: Callable[[str], bool],
    ) -> HeartbeatPersistenceResult:
        """Fence, authorize queue scope, then reset expiry from Queue-store now.

        ``authorize_queue`` receives the named queue after a CURRENT fence and
        before the write. Stale fences raise ``lease_lost``; scope failures raise
        ``permission_denied`` — both without Queue mutation.
        """
        session = self._session_factory()
        try:
            result = self._repository.heartbeat(
                session,
                claim_id=claim_id,
                claim_token=claim_token,
                generation=generation,
                lease_seconds=lease_seconds,
                authorize_queue=authorize_queue,
            )
            session.commit()
            return result
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()
