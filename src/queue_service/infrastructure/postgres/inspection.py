"""Bounded PostgreSQL projections for authorized task inspection (API-04).

Loads spawn lineage from the authoritative ``completion_effects`` registry and
cross-checks ``complete_replay.spawned_task_ids``. Copied task fields
``source_task_id`` / ``spawn_ordinal`` remain inspectable lineage only — not the
global uniqueness key.
"""

from __future__ import annotations

from typing import Final
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.orm import Session

from queue_service.domain.queue_control import DomainValidationError
from queue_service.storage.models import CompleteReplay, CompletionEffect

_OP_COMPLETE: Final[int] = 1
_EFFECT_KIND_SPAWN: Final[int] = 1
_MAX_SPAWN_IDS: Final[int] = 64


class InspectionRepository:
    """Index-backed spawn lineage reads for single-task inspection."""

    def load_spawned_task_ids(
        self,
        session: Session,
        *,
        source_task_id: UUID,
    ) -> list[UUID]:
        """Return spawn IDs ordered by registry ``ordinal`` for a completed source.

        Queries by public ``task_id`` and completing claim id only (no payload
        search, no unbounded history scan). Empty when the source has no retained
        Complete replay row.
        """
        replay = session.execute(
            select(CompleteReplay).where(
                CompleteReplay.task_id == source_task_id,
                CompleteReplay.operation_code == _OP_COMPLETE,
            )
        ).scalar_one_or_none()
        if replay is None:
            return []

        effects = list(
            session.execute(
                select(CompletionEffect)
                .where(
                    CompletionEffect.source_claim_id == replay.claim_id,
                    CompletionEffect.effect_kind_code == _EFFECT_KIND_SPAWN,
                )
                .order_by(CompletionEffect.ordinal)
                .limit(_MAX_SPAWN_IDS)
            )
            .scalars()
            .all()
        )
        registry_ids = [effect.resource_id for effect in effects]
        replay_ids = list(replay.spawned_task_ids or [])
        if registry_ids != replay_ids:
            raise DomainValidationError(
                "internal_error",
                "spawn lineage registry and replay disagree",
            )
        for expected_ordinal, effect in enumerate(effects):
            if int(effect.ordinal) != expected_ordinal:
                raise DomainValidationError(
                    "internal_error",
                    "spawn lineage ordinals are not contiguous",
                )
        return registry_ids
