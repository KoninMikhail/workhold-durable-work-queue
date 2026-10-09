"""Complete use-case orchestration over the PostgreSQL completion transaction."""

from __future__ import annotations

from collections.abc import Callable
from uuid import UUID

from sqlalchemy.orm import Session

from workhold.api.schemas.terminal import CompleteCommand
from workhold.infrastructure.postgres.completion import (
    CompleteFaultHooks,
    CompletePersistenceResult,
    CompleteRepository,
)
from workhold.intake.depth import DepthCeilings
from workhold.scheduling import SchedulingPolicy
from workhold.settings import SCHEDULE_HORIZON_SECONDS_DEFAULT

# Re-export fault hooks so integration tests can inject without reaching storage.
CompletionFaultHooks = CompleteFaultHooks


class CompletionService:
    """Commit one Complete transaction (optional ordered spawn[]) after lease fence."""

    def __init__(
        self,
        *,
        session_factory: Callable[[], Session],
        repository: CompleteRepository | None = None,
        fault_hooks: CompleteFaultHooks | None = None,
        depth_ceilings: DepthCeilings | None = None,
        scheduling_policy: SchedulingPolicy | None = None,
    ) -> None:
        self._session_factory = session_factory
        self._scheduling_policy = scheduling_policy or SchedulingPolicy(
            horizon_seconds=SCHEDULE_HORIZON_SECONDS_DEFAULT,
        )
        self._repository = (
            repository
            if repository is not None
            else CompleteRepository(
                depth_ceilings=depth_ceilings,
                scheduling_policy=self._scheduling_policy,
            )
        )
        self._fault_hooks = fault_hooks or CompleteFaultHooks()

    def complete(
        self,
        *,
        claim_id: UUID,
        claim_token: UUID,
        command: CompleteCommand,
        authorize_queue: Callable[[str], bool],
    ) -> CompletePersistenceResult:
        """Fence, authorize queue scope, then succeed the source (+ spawns) atomically."""
        session = self._session_factory()
        try:
            result = self._repository.complete_claim(
                session,
                claim_id=claim_id,
                claim_token=claim_token,
                command=command,
                authorize_queue=authorize_queue,
                fault_hooks=self._fault_hooks,
            )
            session.commit()
            return result
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()
