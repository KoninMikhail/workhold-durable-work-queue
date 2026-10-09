"""Lease-expiry finalization use-case over PostgreSQL transitions."""

from __future__ import annotations

from collections.abc import Callable
from uuid import UUID

from sqlalchemy.orm import Session

from queue_service.infrastructure.postgres.task_transitions import (
    ExpiryPersistenceResult,
    TaskTransitionRepository,
)


class LeaseExpiryService:
    """Commit one lease-expiry transaction under the reclaim row-lock serialization."""

    def __init__(
        self,
        *,
        session_factory: Callable[[], Session],
        repository: TaskTransitionRepository | None = None,
    ) -> None:
        self._session_factory = session_factory
        self._repository = (
            repository if repository is not None else TaskTransitionRepository()
        )

    def finalize_expired(self, *, task_id: UUID) -> ExpiryPersistenceResult:
        """Close an expired current lease and apply cancel / retry / dead-letter.

        Uses Queue-store time and the task's enqueue-time policy snapshot. Concurrent
        finalizers serialize on the ``tasks_active`` / ``claim_registry`` row locks;
        losers observe ``already_finalized``. Does not grant a new claim.
        """
        session = self._session_factory()
        try:
            result = self._repository.expire_lease(session, task_id=task_id)
            session.commit()
            return result
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()
