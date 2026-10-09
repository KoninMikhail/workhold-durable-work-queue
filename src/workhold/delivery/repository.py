"""Transaction-bound Delivery Outbox persistence (Phase 5 / COMP-02 / DLVR).

Inserts pending events into the caller's SQLAlchemy session — never opens a
network path, never commits/rolls back the caller transaction.

Relay claim/outcome mutations also bind to the caller's short transaction and
use ``FOR UPDATE SKIP LOCKED`` plus token/generation fencing.
"""

from __future__ import annotations

import json
import uuid
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any
from uuid import UUID

from sqlalchemy import and_, delete, func, or_, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from workhold.delivery.cloudevents import (
    CloudEventInput,
    CloudEventValidationError,
    StoredCloudEvent,
    serialize_structured_event,
    validate_event_input,
)
from workhold.delivery.models import (
    EFFECT_KIND_EVENT,
    FAILURE_CODE_MAX,
    RELAY_PRINCIPAL_ID_MAX,
    STATE_PENDING,
    STATE_PUBLISHING,
    EventCommand,
    TERMINAL_OUTCOME_DEAD_LETTERED,
    TERMINAL_OUTCOME_PUBLISHED,
    terminal_outcome_to_state_code,
)
from workhold.domain.queue_control import DomainValidationError
from workhold.storage.models import (
    CompletionEffect,
    DeliveryEventActive,
    DeliveryEventTerminal,
)

_LEASE_SECONDS_MIN = 1
_LEASE_SECONDS_MAX = 3600


@dataclass(frozen=True, slots=True)
class DeliveryEventFaultHooks:
    """Optional test-only seams for proving all-or-nothing rollback."""

    after_event_effect: Callable[[], None] | None = None


@dataclass(frozen=True, slots=True)
class ClaimedDeliveryEvent:
    """One fenced publishing lease handed to the relay after claim commit."""

    event_id: UUID
    claim_token: UUID
    generation: int
    envelope: Mapping[str, Any]
    relay_principal_id: str
    delivery_attempt: int
    claimed_at: datetime
    lease_expires_at: datetime
    available_at: datetime
    source_task_id: UUID
    ordinal: int
    reclaimed: bool = False


class DeliveryEventRepository:
    """Persist Delivery Outbox rows on an existing Queue-store session."""

    def claim_next(
        self,
        session: Session,
        *,
        relay_principal_id: str,
        lease_seconds: int,
    ) -> ClaimedDeliveryEvent | None:
        """Claim one pending or expired-publishing event without blocking peers.

        Uses ``FOR UPDATE SKIP LOCKED``. Rotates an opaque claim token, increments
        generation, stamps Queue-store claim/lease times, and increments
        ``delivery_attempt``. Does not publish.
        """
        self._validate_principal(relay_principal_id)
        self._validate_lease_seconds(lease_seconds)
        now = session.scalar(select(func.transaction_timestamp()))
        if now is None:
            raise DomainValidationError(
                "internal_error",
                "transaction_timestamp() returned null",
            )
        row = session.execute(
            select(DeliveryEventActive)
            .where(
                or_(
                    and_(
                        DeliveryEventActive.state_code == STATE_PENDING,
                        DeliveryEventActive.available_at <= now,
                    ),
                    and_(
                        DeliveryEventActive.state_code == STATE_PUBLISHING,
                        DeliveryEventActive.lease_expires_at <= now,
                    ),
                )
            )
            .order_by(
                DeliveryEventActive.available_at,
                DeliveryEventActive.id,
            )
            .with_for_update(skip_locked=True)
            .limit(1)
        ).scalar_one_or_none()
        if row is None:
            return None

        reclaimed = int(row.state_code) == STATE_PUBLISHING
        claim_token = uuid.uuid4()
        new_generation = int(row.generation) + 1
        lease_expires_at = now + timedelta(seconds=lease_seconds)
        new_attempt = int(row.delivery_attempt) + 1

        row.state_code = STATE_PUBLISHING
        row.generation = new_generation
        row.current_claim_id = claim_token
        row.claimed_at = now
        row.lease_expires_at = lease_expires_at
        row.relay_principal_id = relay_principal_id
        row.delivery_attempt = new_attempt
        row.updated_at = now
        session.flush()

        return ClaimedDeliveryEvent(
            event_id=row.event_id,
            claim_token=claim_token,
            generation=new_generation,
            envelope=dict(row.envelope),
            relay_principal_id=relay_principal_id,
            delivery_attempt=new_attempt,
            claimed_at=now,
            lease_expires_at=lease_expires_at,
            available_at=row.available_at,
            source_task_id=row.source_task_id,
            ordinal=int(row.ordinal),
            reclaimed=reclaimed,
        )

    def acknowledge(
        self,
        session: Session,
        *,
        event_id: UUID,
        claim_token: UUID,
        generation: int,
    ) -> None:
        """Move a current unexpired publishing claim to terminal published."""
        active = self._require_current_fence(
            session,
            event_id=event_id,
            claim_token=claim_token,
            generation=generation,
        )
        terminal_at = session.scalar(select(func.transaction_timestamp()))
        if terminal_at is None:
            raise DomainValidationError(
                "internal_error",
                "transaction_timestamp() returned null",
            )
        self.transition_active_to_terminal(
            session,
            event_id=active.event_id,
            terminal_outcome=TERMINAL_OUTCOME_PUBLISHED,
            terminal_at=terminal_at,
            final_delivery_attempt=int(active.delivery_attempt),
            final_failure_code=None,
        )

    def schedule_retry(
        self,
        session: Session,
        *,
        event_id: UUID,
        claim_token: UUID,
        generation: int,
        failure_code: str,
        available_at_delay_seconds: float,
    ) -> None:
        """Clear the current fence and return the event to pending with backoff.

        Preserves ``generation`` and ``delivery_attempt`` so reclaim continues
        monotonic fencing and restart-safe attempt accounting.
        """
        active = self._require_current_fence(
            session,
            event_id=event_id,
            claim_token=claim_token,
            generation=generation,
        )
        if (
            isinstance(available_at_delay_seconds, bool)
            or not isinstance(available_at_delay_seconds, (int, float))
            or float(available_at_delay_seconds) < 0
        ):
            raise DomainValidationError(
                "validation_failed",
                "available_at_delay_seconds must be a non-negative duration",
            )
        code = self._validate_failure_code(failure_code)
        now = session.scalar(select(func.transaction_timestamp()))
        if now is None:
            raise DomainValidationError(
                "internal_error",
                "transaction_timestamp() returned null",
            )
        active.state_code = STATE_PENDING
        active.current_claim_id = None
        active.claimed_at = None
        active.lease_expires_at = None
        active.relay_principal_id = None
        active.last_failure_code = code
        active.available_at = now + timedelta(seconds=float(available_at_delay_seconds))
        active.updated_at = now
        # generation and delivery_attempt retained intentionally
        session.flush()

    def dead_letter(
        self,
        session: Session,
        *,
        event_id: UUID,
        claim_token: UUID,
        generation: int,
        failure_code: str,
    ) -> None:
        """Move a current unexpired publishing claim to terminal dead-lettered."""
        active = self._require_current_fence(
            session,
            event_id=event_id,
            claim_token=claim_token,
            generation=generation,
        )
        code = self._validate_failure_code(failure_code)
        terminal_at = session.scalar(select(func.transaction_timestamp()))
        if terminal_at is None:
            raise DomainValidationError(
                "internal_error",
                "transaction_timestamp() returned null",
            )
        self.transition_active_to_terminal(
            session,
            event_id=active.event_id,
            terminal_outcome=TERMINAL_OUTCOME_DEAD_LETTERED,
            terminal_at=terminal_at,
            final_delivery_attempt=int(active.delivery_attempt),
            final_failure_code=code,
        )

    def force_reclaim_to_pending(
        self,
        session: Session,
        *,
        event_id: UUID,
    ) -> tuple[int, int]:
        """Tokenless break-glass: publishing → pending; preserve generation/attempt.

        Clears the relay fence without minting a claim token. Fail-closed when the
        row is missing or not in publishing state (including already-terminal).
        """
        active = session.execute(
            select(DeliveryEventActive)
            .where(DeliveryEventActive.event_id == event_id)
            .with_for_update()
        ).scalar_one_or_none()
        if active is None:
            raise DomainValidationError(
                "task_not_found",
                "delivery event not found in active store",
            )
        if int(active.state_code) != STATE_PUBLISHING:
            raise DomainValidationError(
                "validation_failed",
                "delivery event is not publishing",
            )
        generation = int(active.generation)
        delivery_attempt = int(active.delivery_attempt)
        now = session.scalar(select(func.transaction_timestamp()))
        if now is None:
            raise DomainValidationError(
                "internal_error",
                "transaction_timestamp() returned null",
            )
        active.state_code = STATE_PENDING
        active.current_claim_id = None
        active.claimed_at = None
        active.lease_expires_at = None
        active.relay_principal_id = None
        active.available_at = now
        active.updated_at = now
        # generation and delivery_attempt retained intentionally (D-04 / D-06)
        session.flush()
        return generation, delivery_attempt

    def force_dead_letter(
        self,
        session: Session,
        *,
        event_id: UUID,
        failure_code: str,
    ) -> tuple[int, int]:
        """Tokenless break-glass: active → terminal dead-lettered; no claim token.

        Accepts any active (pending or publishing) row. Preserves generation and
        delivery_attempt on the terminal lineage. Fail-closed when missing/terminal.
        """
        active = session.execute(
            select(DeliveryEventActive)
            .where(DeliveryEventActive.event_id == event_id)
            .with_for_update()
        ).scalar_one_or_none()
        if active is None:
            raise DomainValidationError(
                "task_not_found",
                "delivery event not found in active store",
            )
        if int(active.state_code) not in (STATE_PENDING, STATE_PUBLISHING):
            raise DomainValidationError(
                "validation_failed",
                "delivery event is not active",
            )
        generation = int(active.generation)
        delivery_attempt = int(active.delivery_attempt)
        code = self._validate_failure_code(failure_code)
        terminal_at = session.scalar(select(func.transaction_timestamp()))
        if terminal_at is None:
            raise DomainValidationError(
                "internal_error",
                "transaction_timestamp() returned null",
            )
        self.transition_active_to_terminal(
            session,
            event_id=active.event_id,
            terminal_outcome=TERMINAL_OUTCOME_DEAD_LETTERED,
            terminal_at=terminal_at,
            final_delivery_attempt=delivery_attempt,
            final_failure_code=code,
        )
        return generation, delivery_attempt

    def _require_current_fence(
        self,
        session: Session,
        *,
        event_id: UUID,
        claim_token: UUID,
        generation: int,
    ) -> DeliveryEventActive:
        now = session.scalar(select(func.transaction_timestamp()))
        if now is None:
            raise DomainValidationError(
                "internal_error",
                "transaction_timestamp() returned null",
            )
        active = session.execute(
            select(DeliveryEventActive)
            .where(DeliveryEventActive.event_id == event_id)
            .with_for_update()
        ).scalar_one_or_none()
        if active is None:
            raise DomainValidationError(
                "not_found",
                "delivery event not found in active store",
            )
        if (
            int(active.state_code) != STATE_PUBLISHING
            or active.current_claim_id != claim_token
            or int(active.generation) != int(generation)
            or active.lease_expires_at is None
            or active.lease_expires_at <= now
        ):
            raise DomainValidationError(
                "stale_claim",
                "delivery event claim is not current or has expired",
            )
        return active

    @staticmethod
    def _validate_principal(relay_principal_id: str) -> None:
        if not isinstance(relay_principal_id, str) or not (
            1 <= len(relay_principal_id) <= RELAY_PRINCIPAL_ID_MAX
        ):
            raise DomainValidationError(
                "validation_failed",
                "relay_principal_id must be 1..128 characters",
            )

    @staticmethod
    def _validate_lease_seconds(lease_seconds: int) -> None:
        if (
            type(lease_seconds) is not int
            or isinstance(lease_seconds, bool)
            or not (_LEASE_SECONDS_MIN <= lease_seconds <= _LEASE_SECONDS_MAX)
        ):
            raise DomainValidationError(
                "validation_failed",
                "lease_seconds is outside the deployment hard ceiling",
            )

    @staticmethod
    def _validate_failure_code(failure_code: str) -> str:
        if not isinstance(failure_code, str) or not (
            1 <= len(failure_code) <= FAILURE_CODE_MAX
        ):
            raise DomainValidationError(
                "validation_failed",
                f"failure_code must be 1..{FAILURE_CODE_MAX} characters",
            )
        return failure_code

    def insert_pending_for_complete(
        self,
        session: Session,
        *,
        source_claim_id: UUID,
        source_task_id: UUID,
        events: Sequence[EventCommand],
        now: datetime,
        fault_hooks: DeliveryEventFaultHooks | None = None,
    ) -> list[UUID]:
        """Insert ordered pending events + completion_effects in request order.

        Assigns stable Queue UUIDs and CloudEvents envelopes. Pending rows carry
        no relay claim authority (generation=0, claim fields null).
        """
        hooks = fault_hooks or DeliveryEventFaultHooks()
        event_ids: list[UUID] = []
        for ordinal, item in enumerate(events):
            event_ids.append(
                self._insert_one_pending(
                    session,
                    item=item,
                    ordinal=ordinal,
                    source_claim_id=source_claim_id,
                    source_task_id=source_task_id,
                    now=now,
                    hooks=hooks,
                )
            )
        return event_ids

    def _insert_one_pending(
        self,
        session: Session,
        *,
        item: EventCommand,
        ordinal: int,
        source_claim_id: UUID,
        source_task_id: UUID,
        now: datetime,
        hooks: DeliveryEventFaultHooks,
    ) -> UUID:
        try:
            stored = validate_event_input(
                CloudEventInput(
                    source=item.source,
                    type=item.type,
                    subject=item.subject,
                    datacontenttype=item.datacontenttype,
                    data=item.data,
                    extensions=item.extensions,
                ),
                queue_time=now,
            )
        except CloudEventValidationError as exc:
            raise DomainValidationError(exc.code, exc.message) from exc
        event_uuid = UUID(stored.id)
        envelope, envelope_bytes = _envelope_json(stored)
        try:
            with session.begin_nested():
                session.add(
                    DeliveryEventActive(
                        event_id=event_uuid,
                        source_task_id=source_task_id,
                        ordinal=ordinal,
                        state_code=STATE_PENDING,
                        envelope=envelope,
                        envelope_bytes=envelope_bytes,
                        available_at=now,
                        generation=0,
                        current_claim_id=None,
                        claimed_at=None,
                        lease_expires_at=None,
                        relay_principal_id=None,
                        delivery_attempt=0,
                        last_failure_code=None,
                        created_at=now,
                        updated_at=now,
                    )
                )
                session.add(
                    CompletionEffect(
                        source_claim_id=source_claim_id,
                        effect_kind_code=EFFECT_KIND_EVENT,
                        ordinal=ordinal,
                        resource_id=event_uuid,
                        created_at=now,
                    )
                )
                session.flush()
        except IntegrityError as exc:
            raise DomainValidationError(
                "internal_error",
                "delivery event ordinal or identity uniqueness conflict",
            ) from exc
        if hooks.after_event_effect is not None:
            hooks.after_event_effect()
        return event_uuid

    def transition_active_to_terminal(
        self,
        session: Session,
        *,
        event_id: UUID,
        terminal_outcome: str,
        terminal_at: datetime,
        final_delivery_attempt: int,
        final_failure_code: str | None,
    ) -> None:
        """Move one active row into daily-partitioned terminal history.

        Removes active claim authority. Preserves stable event_id / source /
        ordinal / envelope lineage. Used by relay plans and partition tests;
        Complete never calls this.
        """
        if final_delivery_attempt < 0:
            raise DomainValidationError(
                "validation_failed",
                "delivery_attempt must be non-negative",
            )
        state_code = terminal_outcome_to_state_code(terminal_outcome)
        active = session.execute(
            select(DeliveryEventActive)
            .where(DeliveryEventActive.event_id == event_id)
            .with_for_update()
        ).scalar_one_or_none()
        if active is None:
            raise DomainValidationError(
                "not_found",
                "delivery event not found in active store",
            )
        session.add(
            DeliveryEventTerminal(
                event_id=active.event_id,
                source_task_id=active.source_task_id,
                ordinal=int(active.ordinal),
                state_code=state_code,
                envelope=dict(active.envelope),
                envelope_bytes=int(active.envelope_bytes),
                created_at=active.created_at,
                terminal_at=terminal_at,
                failure_code=final_failure_code,
                failure_detail=None,
                delivery_attempt=int(final_delivery_attempt),
            )
        )
        session.execute(
            delete(DeliveryEventActive).where(DeliveryEventActive.event_id == event_id)
        )
        session.flush()


def _envelope_json(stored: StoredCloudEvent) -> tuple[dict[str, Any], int]:
    serialization = serialize_structured_event(stored)
    envelope = json.loads(serialization.body.decode("utf-8"))
    return envelope, len(serialization.body)
