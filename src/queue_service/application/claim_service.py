"""Claim use-case boundary over the PostgreSQL fenced-lease primitive."""

from __future__ import annotations

from collections.abc import Callable

from sqlalchemy.orm import Session

from queue_service.infrastructure.postgres.claim_repository import (
    ClaimPersistenceResult,
    ClaimRepository,
)


class ClaimService:
    """Commit one claim transaction before returning claim credentials."""

    def __init__(
        self,
        *,
        session_factory: Callable[[], Session],
        repository: ClaimRepository | None = None,
    ) -> None:
        self._session_factory = session_factory
        self._repository = repository if repository is not None else ClaimRepository()

    def claim(
        self,
        *,
        queue_name: str,
        worker_id: str,
        lease_seconds: int,
    ) -> ClaimPersistenceResult:
        """Claim at most one task from ``queue_name`` for ``worker_id``.

        The opaque claim token and public claim identity are returned only after
        the Queue-store transaction commits.
        """
        session = self._session_factory()
        try:
            result = self._repository.claim_one(
                session,
                queue_name=queue_name,
                worker_id=worker_id,
                lease_seconds=lease_seconds,
            )
            session.commit()
            return result
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()
