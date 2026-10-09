"""Worker terminal fail / ack_cancel use-cases over PostgreSQL transitions."""

from __future__ import annotations

from collections.abc import Callable
from uuid import UUID

from sqlalchemy.orm import Session

from queue_service.api.schemas.terminal import AckCancelCommand, FailCommand
from queue_service.infrastructure.postgres.task_transitions import (
    AckCancelPersistenceResult,
    FailPersistenceResult,
    TaskTransitionRepository,
)


class WorkerTerminalService:
    """Commit one fail or ack_cancel transaction after shared lease fence and replay."""

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

    def fail(
        self,
        *,
        claim_id: UUID,
        claim_token: UUID,
        command: FailCommand,
        authorize_queue: Callable[[str], bool],
    ) -> FailPersistenceResult:
        """Fence, authorize queue scope, then fail/retry/dead-letter atomically."""
        session = self._session_factory()
        try:
            result = self._repository.fail_claim(
                session,
                claim_id=claim_id,
                claim_token=claim_token,
                command=command,
                authorize_queue=authorize_queue,
            )
            session.commit()
            return result
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    def ack_cancel(
        self,
        *,
        claim_id: UUID,
        claim_token: UUID,
        command: AckCancelCommand,
        authorize_queue: Callable[[str], bool],
    ) -> AckCancelPersistenceResult:
        """Fence, authorize queue scope, then cancel attempt/task atomically."""
        session = self._session_factory()
        try:
            result = self._repository.ack_cancel_claim(
                session,
                claim_id=claim_id,
                claim_token=claim_token,
                command=command,
                authorize_queue=authorize_queue,
            )
            session.commit()
            return result
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()
