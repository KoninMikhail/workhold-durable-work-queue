"""Bounded task and attempt inspection projections (Phase 03.6-06 / 03.7-04).

Exposes operational state for retry, dead-letter, cancellation, and completion
spawn lineage without a business-result field or full claim token. Retention
bounds map expired history to ``task_not_found``.
"""

from __future__ import annotations

import base64
import binascii
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Final
from uuid import UUID

from sqlalchemy import desc, func, or_, select
from sqlalchemy.orm import Session

from queue_service.api.schemas.tasks import format_task_datetime
from queue_service.domain.queue_control import DomainValidationError
from queue_service.infrastructure.postgres.inspection import InspectionRepository
from queue_service.security.payload_policy import (
    PayloadRetentionPolicy,
)
from queue_service.security.principals import Principal, ServiceRole
from queue_service.settings import DEFAULT_PAYLOAD_RETENTION_DAYS
from queue_service.storage.models import (
    Queue,
    QueuePolicyVersion,
    TaskActive,
    TaskAttempt,
    TaskTerminal,
)

_TASK_DELAYED: Final[int] = 1
_TASK_READY: Final[int] = 2
_TASK_LEASED: Final[int] = 3

_TERMINAL_SUCCEEDED: Final[int] = 10
_TERMINAL_DEAD_LETTERED: Final[int] = 11
_TERMINAL_CANCELLED: Final[int] = 12

_OUTCOME_BY_CODE: Final[dict[int, str]] = {
    1: "active",
    2: "succeeded",
    3: "retry_scheduled",
    4: "dead_lettered",
    5: "expired",
    6: "cancelled",
}

_TERMINAL_STATE_BY_CODE: Final[dict[int, str]] = {
    _TERMINAL_SUCCEEDED: "succeeded",
    _TERMINAL_DEAD_LETTERED: "dead_lettered",
    _TERMINAL_CANCELLED: "cancelled",
}

_DEFAULT_ATTEMPT_LIMIT: Final[int] = 50
_MAX_ATTEMPT_LIMIT: Final[int] = 100


@dataclass(frozen=True, slots=True)
class AttemptPage:
    """Cursor page of append-only attempt projections."""

    items: list[dict[str, Any]]
    next_cursor: str | None


class TaskInspectionService:
    """Read-only inspection of active/terminal tasks and attempt history."""

    def __init__(
        self,
        *,
        session_factory: Callable[[], Session],
        retention_policy: PayloadRetentionPolicy | None = None,
        inspection_repository: InspectionRepository | None = None,
    ) -> None:
        self._session_factory = session_factory
        self._retention = retention_policy or PayloadRetentionPolicy(
            retention_days=DEFAULT_PAYLOAD_RETENTION_DAYS
        )
        self._inspection = inspection_repository or InspectionRepository()

    def get_task(
        self,
        *,
        task_id: UUID,
        principal: Principal,
        authorize_queue: Callable[[str], bool],
    ) -> dict[str, Any]:
        """Project one task for producer/observer inspection."""

        session = self._session_factory()
        try:
            return self._get_task_locked(
                session,
                task_id=task_id,
                principal=principal,
                authorize_queue=authorize_queue,
            )
        finally:
            session.close()

    def list_attempts(
        self,
        *,
        task_id: UUID,
        principal: Principal,
        authorize_queue: Callable[[str], bool],
        cursor: str | None = None,
        limit: int | None = None,
    ) -> AttemptPage:
        """List append-only attempts newest-first with optional cursor."""

        session = self._session_factory()
        try:
            # Ensure the task is visible under the same auth/retention rules.
            self._get_task_locked(
                session,
                task_id=task_id,
                principal=principal,
                authorize_queue=authorize_queue,
            )
            page_limit = _DEFAULT_ATTEMPT_LIMIT if limit is None else int(limit)
            if page_limit < 1 or page_limit > _MAX_ATTEMPT_LIMIT:
                raise DomainValidationError(
                    "validation_failed",
                    f"limit must be between 1 and {_MAX_ATTEMPT_LIMIT}",
                )
            claimed_before, id_before = _decode_cursor(cursor)
            query = (
                select(TaskAttempt)
                .where(TaskAttempt.task_id == task_id)
                .order_by(desc(TaskAttempt.claimed_at), desc(TaskAttempt.id))
            )
            if claimed_before is not None and id_before is not None:
                query = query.where(
                    or_(
                        TaskAttempt.claimed_at < claimed_before,
                        (TaskAttempt.claimed_at == claimed_before)
                        & (TaskAttempt.id < id_before),
                    )
                )
            rows = list(session.execute(query.limit(page_limit + 1)).scalars().all())
            has_more = len(rows) > page_limit
            page_rows = rows[:page_limit]
            items = [_project_attempt(row) for row in page_rows]
            next_cursor: str | None = None
            if has_more and page_rows:
                last = page_rows[-1]
                next_cursor = _encode_cursor(last.claimed_at, int(last.id))
            return AttemptPage(items=items, next_cursor=next_cursor)
        finally:
            session.close()

    def _get_task_locked(
        self,
        session: Session,
        *,
        task_id: UUID,
        principal: Principal,
        authorize_queue: Callable[[str], bool],
    ) -> dict[str, Any]:
        now = session.scalar(select(func.transaction_timestamp()))
        if now is None:
            raise DomainValidationError(
                "internal_error",
                "transaction_timestamp() returned null",
            )

        active = session.execute(
            select(TaskActive, Queue.name, QueuePolicyVersion.version)
            .join(Queue, Queue.id == TaskActive.queue_id)
            .join(
                QueuePolicyVersion,
                QueuePolicyVersion.id == TaskActive.retry_policy_version_id,
            )
            .where(TaskActive.task_id == task_id)
        ).one_or_none()
        if active is not None:
            task, queue_name, policy_version = active
            self._authorize_visibility(
                principal=principal,
                producer_id=str(task.producer_id),
                queue_name=str(queue_name),
                authorize_queue=authorize_queue,
            )
            ended_count = int(
                session.scalar(
                    select(func.count())
                    .select_from(TaskAttempt)
                    .where(
                        TaskAttempt.task_id == task_id,
                        TaskAttempt.outcome_code != 1,
                    )
                )
                or 0
            )
            last_failure = session.execute(
                select(TaskAttempt)
                .where(
                    TaskAttempt.task_id == task_id,
                    TaskAttempt.failure_code.is_not(None),
                )
                .order_by(desc(TaskAttempt.ended_at), desc(TaskAttempt.id))
                .limit(1)
            ).scalar_one_or_none()
            return _project_active_task(
                task=task,
                queue_name=str(queue_name),
                policy_version=int(policy_version),
                ended_attempt_count=ended_count,
                last_failure=last_failure,
            )

        terminal = session.execute(
            select(TaskTerminal, Queue.name)
            .join(Queue, Queue.id == TaskTerminal.queue_id)
            .where(TaskTerminal.task_id == task_id)
        ).one_or_none()
        if terminal is None:
            raise DomainValidationError("task_not_found", "task not found")

        row, queue_name = terminal
        self._authorize_visibility(
            principal=principal,
            producer_id=str(row.producer_id),
            queue_name=str(queue_name),
            authorize_queue=authorize_queue,
        )
        expires_at = self._retention.expires_at(_ensure_aware(row.terminal_at))
        if self._retention.is_expired(_ensure_aware(now), expires_at):
            raise DomainValidationError("task_not_found", "task not found")
        spawned_ids = self._inspection.load_spawned_task_ids(
            session,
            source_task_id=task_id,
        )
        return _project_terminal_task(
            terminal=row,
            queue_name=str(queue_name),
            spawned_task_ids=spawned_ids,
        )

    @staticmethod
    def _authorize_visibility(
        *,
        principal: Principal,
        producer_id: str,
        queue_name: str,
        authorize_queue: Callable[[str], bool],
    ) -> None:
        if not authorize_queue(queue_name):
            raise DomainValidationError("permission_denied", "permission denied")
        if principal.role is ServiceRole.PRODUCER:
            if principal.principal_id != producer_id:
                raise DomainValidationError("task_not_found", "task not found")
            return
        if principal.role is ServiceRole.OBSERVER:
            return
        raise DomainValidationError("permission_denied", "permission denied")


def _project_active_task(
    *,
    task: TaskActive,
    queue_name: str,
    policy_version: int,
    ended_attempt_count: int,
    last_failure: TaskAttempt | None,
) -> dict[str, Any]:
    state_code = int(task.state_code)
    if state_code == _TASK_LEASED:
        state = "leased"
    elif state_code in (_TASK_DELAYED, _TASK_READY) and ended_attempt_count > 0:
        state = "retry_scheduled"
    elif state_code == _TASK_DELAYED:
        state = "delayed"
    elif state_code == _TASK_READY:
        state = "ready"
    else:
        raise DomainValidationError(
            "internal_error",
            f"unknown active state_code={state_code}",
        )

    body: dict[str, Any] = {
        "task_id": str(task.task_id),
        "queue_name": queue_name,
        "producer_id": str(task.producer_id),
        "state": state,
        "priority": int(task.priority),
        "available_at": format_task_datetime(task.available_at),
        "retry_policy_version": int(policy_version),
        "created_at": format_task_datetime(task.created_at),
        "spawned_task_ids": [],
        "delivery_event_ids": [],
    }
    if task.source_task_id is not None:
        body["source_task_id"] = str(task.source_task_id)
        body["spawn_ordinal"] = int(task.spawn_ordinal) if task.spawn_ordinal is not None else None

    if state_code == _TASK_LEASED:
        if (
            task.current_claim_id is None
            or task.claimed_at is None
            or task.lease_expires_at is None
            or task.worker_id is None
        ):
            raise DomainValidationError(
                "internal_error",
                "leased task is missing claim projection fields",
            )
        body["current_claim"] = {
            "claim_id": str(task.current_claim_id),
            "generation": int(task.generation),
            "claimed_at": format_task_datetime(task.claimed_at),
            "lease_expires_at": format_task_datetime(task.lease_expires_at),
            "worker_id": str(task.worker_id),
            "cancel_requested": task.cancel_requested_at is not None,
        }
    else:
        body["current_claim"] = None

    if last_failure is not None and last_failure.failure_code is not None:
        body["failure_code"] = str(last_failure.failure_code)
        body["failure_detail"] = (
            None
            if last_failure.failure_detail is None
            else str(last_failure.failure_detail)
        )
    else:
        body["failure_code"] = None
        body["failure_detail"] = None
    body["terminal_at"] = None
    return body


def _project_terminal_task(
    *,
    terminal: TaskTerminal,
    queue_name: str,
    spawned_task_ids: list[UUID] | None = None,
) -> dict[str, Any]:
    state = _TERMINAL_STATE_BY_CODE.get(int(terminal.state_code))
    if state is None:
        raise DomainValidationError(
            "internal_error",
            f"unknown terminal state_code={terminal.state_code}",
        )
    ordered_spawns = spawned_task_ids if spawned_task_ids is not None else []
    body: dict[str, Any] = {
        "task_id": str(terminal.task_id),
        "queue_name": queue_name,
        "producer_id": str(terminal.producer_id),
        "state": state,
        "priority": int(terminal.priority),
        "available_at": format_task_datetime(terminal.available_at),
        "retry_policy_version": int(terminal.retry_policy_version),
        "created_at": format_task_datetime(terminal.created_at),
        "terminal_at": format_task_datetime(terminal.terminal_at),
        "current_claim": None,
        "failure_code": (
            None if terminal.failure_code is None else str(terminal.failure_code)
        ),
        "failure_detail": (
            None if terminal.failure_detail is None else str(terminal.failure_detail)
        ),
        "spawned_task_ids": [str(task_id) for task_id in ordered_spawns],
        "delivery_event_ids": [],
    }
    if terminal.source_task_id is not None:
        body["source_task_id"] = str(terminal.source_task_id)
        body["spawn_ordinal"] = (
            int(terminal.spawn_ordinal) if terminal.spawn_ordinal is not None else None
        )
    return body


def _project_attempt(row: TaskAttempt) -> dict[str, Any]:
    outcome = _OUTCOME_BY_CODE.get(int(row.outcome_code))
    if outcome is None:
        raise DomainValidationError(
            "internal_error",
            f"unknown attempt outcome_code={row.outcome_code}",
        )
    body: dict[str, Any] = {
        "attempt_id": int(row.id),
        "task_id": str(row.task_id),
        "claim_id": str(row.claim_id),
        "generation": int(row.generation),
        "claimed_at": format_task_datetime(row.claimed_at),
        "worker_id": str(row.worker_id),
        "lease_expires_at": format_task_datetime(row.lease_expires_at),
        "outcome": outcome,
        "failure_code": None if row.failure_code is None else str(row.failure_code),
        "failure_detail": (
            None if row.failure_detail is None else str(row.failure_detail)
        ),
    }
    if row.ended_at is not None:
        body["ended_at"] = format_task_datetime(row.ended_at)
    else:
        body["ended_at"] = None
    return body


def _ensure_aware(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value


def _encode_cursor(claimed_at: datetime, attempt_id: int) -> str:
    stamp = format_task_datetime(claimed_at)
    raw = f"{stamp}|{attempt_id}".encode("utf-8")
    return base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")


def _decode_cursor(cursor: str | None) -> tuple[datetime | None, int | None]:
    if cursor is None or cursor == "":
        return None, None
    if len(cursor) > 512:
        raise DomainValidationError("validation_failed", "cursor exceeds 512 characters")
    padding = "=" * (-len(cursor) % 4)
    try:
        raw = base64.urlsafe_b64decode(cursor + padding)
        text = raw.decode("utf-8")
        stamp, attempt_id_s = text.split("|", 1)
        attempt_id = int(attempt_id_s)
    except (ValueError, binascii.Error, UnicodeDecodeError) as exc:
        raise DomainValidationError("validation_failed", "cursor is invalid") from exc
    if stamp.endswith("Z"):
        stamp = stamp[:-1] + "+00:00"
    try:
        claimed_at = datetime.fromisoformat(stamp)
    except ValueError as exc:
        raise DomainValidationError("validation_failed", "cursor is invalid") from exc
    if claimed_at.tzinfo is None:
        claimed_at = claimed_at.replace(tzinfo=timezone.utc)
    if attempt_id < 1:
        raise DomainValidationError("validation_failed", "cursor is invalid")
    return claimed_at, attempt_id
