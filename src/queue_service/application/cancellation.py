"""Producer cancellation use-case over PostgreSQL transitions."""

from __future__ import annotations

from collections.abc import Callable
from uuid import UUID

from sqlalchemy.orm import Session

from queue_service.infrastructure.postgres.task_transitions import (
    CancelPersistenceResult,
    TaskTransitionRepository,
)


class CancellationService:
    """Commit one producer cancel transaction (immediate or cooperative request)."""

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

    def cancel(
        self,
        *,
        task_id: UUID,
        producer_id: str,
        authorize_queue: Callable[[str], bool],
    ) -> CancelPersistenceResult:
        """Lock the task, authorize producer/queue scope, then cancel state-aware."""

        session = self._session_factory()
        try:
            result = self._repository.cancel_task(
                session,
                task_id=task_id,
                producer_id=producer_id,
                authorize_queue=authorize_queue,
            )
            session.commit()
            return result
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()
