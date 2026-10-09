"""Bounded operational inspection lists (API-05).

Observer/admin keyset pages for active tasks, attempts, dead letters, and admin
audit. Every list enforces max page size, opaque integrity-protected cursors,
indexed/time-bounded filters, and claim-token / unauthorized-payload redaction.
Payload-field and free-text search are rejected.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Final
from uuid import UUID

from sqlalchemy import desc, or_, select, text
from sqlalchemy.orm import Session

from workhold.api.schemas.tasks import format_task_datetime
from workhold.domain.queue_control import DomainValidationError
from workhold.security.cursors import InspectionCursorCodec
from workhold.storage.models import (
    AdminAuditLog,
    Queue,
    QueuePolicyVersion,
    TaskActive,
    TaskAttempt,
    TaskTerminal,
)

_DEFAULT_LIMIT: Final[int] = 50
_MAX_LIMIT: Final[int] = 100

_TASK_DELAYED: Final[int] = 1
_TASK_READY: Final[int] = 2
_TASK_LEASED: Final[int] = 3
_TERMINAL_DEAD_LETTERED: Final[int] = 11

_KIND_TASKS: Final[str] = "tasks"
_KIND_ATTEMPTS: Final[str] = "attempts"
_KIND_DEAD_LETTERS: Final[str] = "dead_letters"
_KIND_AUDIT: Final[str] = "audit"

_AUDIT_OPERATION_BY_CODE: Final[dict[int, str]] = {
    1: "create_queue",
    2: "create_policy",
    3: "activate_policy",
    4: "set_queue_state",
    5: "run_maintenance",
    6: "replay_dead_letter",
    7: "bulk_replay",
    8: "bulk_cancel",
}

_OUTCOME_BY_CODE: Final[dict[int, str]] = {
    1: "active",
    2: "succeeded",
    3: "retry_scheduled",
    4: "dead_lettered",
    5: "expired",
    6: "cancelled",
}

_FORBIDDEN_FILTER_KEYS: Final[frozenset[str]] = frozenset(
    {
        "q",
        "query",
        "search",
        "text",
        "payload",
        "payload_field",
        "business_key",
        "worker_id",
        "sql",
        "offset",
    }
)


@dataclass(frozen=True, slots=True)
class InspectionPage:
    """Opaque-cursor page of redacted inspection projections."""

    items: list[dict[str, Any]]
    next_cursor: str | None


class OperationalInspectionService:
    """Keyset list queries against Phase 3 active/history relations."""

    def __init__(self, *, cursor_codec: InspectionCursorCodec) -> None:
        self._cursors = cursor_codec

    def list_tasks(
        self,
        session: Session,
        *,
        queue_name: str,
        limit: int | None = None,
        cursor: str | None = None,
        extra_filters: Mapping[str, str] | None = None,
    ) -> InspectionPage:
        """List active tasks for one named queue (depth-bounded working set)."""

        _reject_forbidden_filters(extra_filters)
        page_limit = _normalize_limit(limit)
        queue = _require_queue(session, queue_name)
        position = self._cursors.decode(_KIND_TASKS, cursor)
        query = (
            select(TaskActive, QueuePolicyVersion.version)
            .join(
                QueuePolicyVersion,
                QueuePolicyVersion.id == TaskActive.retry_policy_version_id,
            )
            .where(TaskActive.queue_id == queue.id)
            .order_by(desc(TaskActive.created_at), desc(TaskActive.task_id))
        )
        if position is not None:
            created_at = _parse_cursor_datetime(position.get("created_at"))
            task_id = _parse_cursor_uuid(position.get("task_id"))
            query = query.where(
                or_(
                    TaskActive.created_at < created_at,
                    (TaskActive.created_at == created_at)
                    & (TaskActive.task_id < task_id),
                )
            )
        rows = list(session.execute(query.limit(page_limit + 1)).all())
        has_more = len(rows) > page_limit
        page_rows = rows[:page_limit]
        items = [
            _project_active_task(
                task=task,
                queue_name=str(queue.name),
                policy_version=int(policy_version),
            )
            for task, policy_version in page_rows
        ]
        next_cursor: str | None = None
        if has_more and page_rows:
            last_task = page_rows[-1][0]
            next_cursor = self._cursors.encode(
                _KIND_TASKS,
                {
                    "created_at": format_task_datetime(last_task.created_at),
                    "task_id": str(last_task.task_id),
                },
            )
        return InspectionPage(items=items, next_cursor=next_cursor)

    def list_attempts(
        self,
        session: Session,
        *,
        task_id: UUID,
        time_from: datetime,
        time_to: datetime,
        limit: int | None = None,
        cursor: str | None = None,
        extra_filters: Mapping[str, str] | None = None,
    ) -> InspectionPage:
        """List attempts for one task inside a partition-prunable claimed_at window."""

        _reject_forbidden_filters(extra_filters)
        page_limit = _normalize_limit(limit)
        bound_from, bound_to = _normalize_time_bounds(time_from, time_to)
        position = self._cursors.decode(_KIND_ATTEMPTS, cursor)
        query = (
            select(TaskAttempt)
            .where(
                TaskAttempt.task_id == task_id,
                TaskAttempt.claimed_at >= bound_from,
                TaskAttempt.claimed_at <= bound_to,
            )
            .order_by(desc(TaskAttempt.claimed_at), desc(TaskAttempt.id))
        )
        if position is not None:
            claimed_at = _parse_cursor_datetime(position.get("claimed_at"))
            attempt_id = _parse_cursor_int(position.get("attempt_id"))
            query = query.where(
                or_(
                    TaskAttempt.claimed_at < claimed_at,
                    (TaskAttempt.claimed_at == claimed_at)
                    & (TaskAttempt.id < attempt_id),
                )
            )
        rows = list(session.execute(query.limit(page_limit + 1)).scalars().all())
        has_more = len(rows) > page_limit
        page_rows = rows[:page_limit]
        items = [_project_attempt(row) for row in page_rows]
        next_cursor: str | None = None
        if has_more and page_rows:
            last = page_rows[-1]
            next_cursor = self._cursors.encode(
                _KIND_ATTEMPTS,
                {
                    "claimed_at": format_task_datetime(last.claimed_at),
                    "attempt_id": int(last.id),
                },
            )
        return InspectionPage(items=items, next_cursor=next_cursor)

    def list_dead_letters(
        self,
        session: Session,
        *,
        queue_name: str,
        time_from: datetime,
        time_to: datetime,
        limit: int | None = None,
        cursor: str | None = None,
        extra_filters: Mapping[str, str] | None = None,
    ) -> InspectionPage:
        """List dead-letter terminals with partition-prunable terminal_at bounds."""

        _reject_forbidden_filters(extra_filters)
        page_limit = _normalize_limit(limit)
        bound_from, bound_to = _normalize_time_bounds(time_from, time_to)
        queue = _require_queue(session, queue_name)
        position = self._cursors.decode(_KIND_DEAD_LETTERS, cursor)
        query = (
            select(TaskTerminal)
            .where(
                TaskTerminal.queue_id == queue.id,
                TaskTerminal.state_code == _TERMINAL_DEAD_LETTERED,
                TaskTerminal.terminal_at >= bound_from,
                TaskTerminal.terminal_at <= bound_to,
            )
            .order_by(desc(TaskTerminal.terminal_at), desc(TaskTerminal.id))
        )
        if position is not None:
            terminal_at = _parse_cursor_datetime(position.get("terminal_at"))
            row_id = _parse_cursor_int(position.get("id"))
            query = query.where(
                or_(
                    TaskTerminal.terminal_at < terminal_at,
                    (TaskTerminal.terminal_at == terminal_at)
                    & (TaskTerminal.id < row_id),
                )
            )
        rows = list(session.execute(query.limit(page_limit + 1)).scalars().all())
        has_more = len(rows) > page_limit
        page_rows = rows[:page_limit]
        items = [
            _project_dead_letter(row, queue_name=str(queue.name)) for row in page_rows
        ]
        next_cursor: str | None = None
        if has_more and page_rows:
            last = page_rows[-1]
            next_cursor = self._cursors.encode(
                _KIND_DEAD_LETTERS,
                {
                    "terminal_at": format_task_datetime(last.terminal_at),
                    "id": int(last.id),
                },
            )
        return InspectionPage(items=items, next_cursor=next_cursor)

    def list_admin_audit(
        self,
        session: Session,
        *,
        time_from: datetime,
        time_to: datetime,
        queue_name: str | None = None,
        limit: int | None = None,
        cursor: str | None = None,
        extra_filters: Mapping[str, str] | None = None,
    ) -> InspectionPage:
        """List admin audit rows with partition-prunable audit_at bounds."""

        _reject_forbidden_filters(extra_filters)
        page_limit = _normalize_limit(limit)
        bound_from, bound_to = _normalize_time_bounds(time_from, time_to)
        queue_pk: int | None = None
        if queue_name is not None:
            queue_pk = _require_queue(session, queue_name).id
        position = self._cursors.decode(_KIND_AUDIT, cursor)
        query = (
            select(AdminAuditLog, Queue.queue_id)
            .outerjoin(Queue, Queue.id == AdminAuditLog.queue_id)
            .where(
                AdminAuditLog.audit_at >= bound_from,
                AdminAuditLog.audit_at <= bound_to,
            )
            .order_by(desc(AdminAuditLog.audit_at), desc(AdminAuditLog.id))
        )
        if queue_pk is not None:
            query = query.where(AdminAuditLog.queue_id == queue_pk)
        if position is not None:
            audit_at = _parse_cursor_datetime(position.get("audit_at"))
            audit_id = _parse_cursor_int(position.get("audit_id"))
            query = query.where(
                or_(
                    AdminAuditLog.audit_at < audit_at,
                    (AdminAuditLog.audit_at == audit_at)
                    & (AdminAuditLog.id < audit_id),
                )
            )
        rows = list(session.execute(query.limit(page_limit + 1)).all())
        has_more = len(rows) > page_limit
        page_rows = rows[:page_limit]
        items = [
            _project_audit(row, public_queue_id=public_queue_id)
            for row, public_queue_id in page_rows
        ]
        next_cursor: str | None = None
        if has_more and page_rows:
            last = page_rows[-1][0]
            next_cursor = self._cursors.encode(
                _KIND_AUDIT,
                {
                    "audit_at": format_task_datetime(last.audit_at),
                    "audit_id": int(last.id),
                },
            )
        return InspectionPage(items=items, next_cursor=next_cursor)

    def explain_attempts_plan(
        self,
        session: Session,
        *,
        task_id: UUID,
        time_from: datetime,
        time_to: datetime,
    ) -> str:
        """Return EXPLAIN text for the attempt keyset query (tests)."""

        bound_from, bound_to = _normalize_time_bounds(time_from, time_to)
        rows = session.execute(
            text(
                """
                EXPLAIN
                SELECT id
                FROM task_attempts
                WHERE task_id = :task_id
                  AND claimed_at >= :bound_from
                  AND claimed_at <= :bound_to
                ORDER BY claimed_at DESC, id DESC
                LIMIT :limit
                """
            ),
            {
                "task_id": str(task_id),
                "bound_from": bound_from,
                "bound_to": bound_to,
                "limit": _DEFAULT_LIMIT,
            },
        ).fetchall()
        return "\n".join(str(row[0]) for row in rows)

    def explain_dead_letters_plan(
        self,
        session: Session,
        *,
        queue_id: int,
        time_from: datetime,
        time_to: datetime,
    ) -> str:
        """Return EXPLAIN text for the dead-letter keyset query (tests)."""

        bound_from, bound_to = _normalize_time_bounds(time_from, time_to)
        rows = session.execute(
            text(
                """
                EXPLAIN
                SELECT id
                FROM tasks_terminal
                WHERE queue_id = :queue_id
                  AND state_code = :state_code
                  AND terminal_at >= :bound_from
                  AND terminal_at <= :bound_to
                ORDER BY terminal_at DESC, id DESC
                LIMIT :limit
                """
            ),
            {
                "queue_id": queue_id,
                "state_code": _TERMINAL_DEAD_LETTERED,
                "bound_from": bound_from,
                "bound_to": bound_to,
                "limit": _DEFAULT_LIMIT,
            },
        ).fetchall()
        return "\n".join(str(row[0]) for row in rows)

    def explain_audit_plan(
        self,
        session: Session,
        *,
        time_from: datetime,
        time_to: datetime,
        queue_id: int | None = None,
    ) -> str:
        """Return EXPLAIN text for the audit keyset query (tests)."""

        bound_from, bound_to = _normalize_time_bounds(time_from, time_to)
        if queue_id is None:
            sql = """
                EXPLAIN
                SELECT id
                FROM admin_audit_log
                WHERE audit_at >= :bound_from
                  AND audit_at <= :bound_to
                ORDER BY audit_at DESC, id DESC
                LIMIT :limit
                """
            params: dict[str, Any] = {
                "bound_from": bound_from,
                "bound_to": bound_to,
                "limit": _DEFAULT_LIMIT,
            }
        else:
            sql = """
                EXPLAIN
                SELECT id
                FROM admin_audit_log
                WHERE queue_id = :queue_id
                  AND audit_at >= :bound_from
                  AND audit_at <= :bound_to
                ORDER BY audit_at DESC, id DESC
                LIMIT :limit
                """
            params = {
                "queue_id": queue_id,
                "bound_from": bound_from,
                "bound_to": bound_to,
                "limit": _DEFAULT_LIMIT,
            }
        rows = session.execute(text(sql), params).fetchall()
        return "\n".join(str(row[0]) for row in rows)


def _reject_forbidden_filters(extra_filters: Mapping[str, str] | None) -> None:
    if not extra_filters:
        return
    for key in extra_filters:
        normalized = key.strip().lower()
        if normalized in _FORBIDDEN_FILTER_KEYS:
            raise DomainValidationError(
                "validation_failed",
                "payload-field and free-text search are not supported",
            )


def _normalize_limit(limit: int | None) -> int:
    page_limit = _DEFAULT_LIMIT if limit is None else int(limit)
    if page_limit < 1 or page_limit > _MAX_LIMIT:
        raise DomainValidationError(
            "validation_failed",
            f"limit must be between 1 and {_MAX_LIMIT}",
        )
    return page_limit


def _normalize_time_bounds(
    time_from: datetime,
    time_to: datetime,
) -> tuple[datetime, datetime]:
    bound_from = _ensure_aware(time_from)
    bound_to = _ensure_aware(time_to)
    if bound_from > bound_to:
        raise DomainValidationError(
            "validation_failed",
            "from must be no later than to",
        )
    return bound_from, bound_to


def _require_queue(session: Session, queue_name: str) -> Queue:
    if not queue_name or not isinstance(queue_name, str):
        raise DomainValidationError("validation_failed", "queue_name is required")
    queue = session.execute(
        select(Queue).where(Queue.name == queue_name)
    ).scalar_one_or_none()
    if queue is None:
        raise DomainValidationError("queue_not_found", "queue not found")
    return queue


def _ensure_aware(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _parse_cursor_datetime(raw: object) -> datetime:
    if not isinstance(raw, str) or not raw:
        raise DomainValidationError("validation_failed", "cursor is invalid")
    stamp = raw[:-1] + "+00:00" if raw.endswith("Z") else raw
    try:
        value = datetime.fromisoformat(stamp)
    except ValueError as exc:
        raise DomainValidationError("validation_failed", "cursor is invalid") from exc
    return _ensure_aware(value)


def _parse_cursor_uuid(raw: object) -> UUID:
    if not isinstance(raw, str):
        raise DomainValidationError("validation_failed", "cursor is invalid")
    try:
        return UUID(raw)
    except (ValueError, AttributeError, TypeError) as exc:
        raise DomainValidationError("validation_failed", "cursor is invalid") from exc


def _parse_cursor_int(raw: object) -> int:
    try:
        value = int(raw)  # type: ignore[arg-type]
    except (TypeError, ValueError) as exc:
        raise DomainValidationError("validation_failed", "cursor is invalid") from exc
    if value < 1:
        raise DomainValidationError("validation_failed", "cursor is invalid")
    return value


def _project_active_task(
    *,
    task: TaskActive,
    queue_name: str,
    policy_version: int,
) -> dict[str, Any]:
    state_code = int(task.state_code)
    if state_code == _TASK_LEASED:
        state = "leased"
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
        "payload": None,
        "terminal_at": None,
        "failure_code": None,
        "failure_detail": None,
    }
    if state_code == _TASK_LEASED:
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
    # Never project claim_token or opaque payload on operational lists.
    assert "claim_token" not in body
    return body


def _project_attempt(row: TaskAttempt) -> dict[str, Any]:
    outcome = _OUTCOME_BY_CODE.get(int(row.outcome_code))
    if outcome is None:
        raise DomainValidationError(
            "internal_error",
            f"unknown attempt outcome_code={row.outcome_code}",
        )
    return {
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
        "ended_at": (
            None if row.ended_at is None else format_task_datetime(row.ended_at)
        ),
    }


def _project_dead_letter(row: TaskTerminal, *, queue_name: str) -> dict[str, Any]:
    return {
        "task_id": str(row.task_id),
        "queue_name": queue_name,
        "producer_id": str(row.producer_id),
        "state": "dead_lettered",
        "priority": int(row.priority),
        "available_at": format_task_datetime(row.available_at),
        "retry_policy_version": int(row.retry_policy_version),
        "created_at": format_task_datetime(row.created_at),
        "spawned_task_ids": [],
        "delivery_event_ids": [],
        "terminal_at": format_task_datetime(row.terminal_at),
        "failure_code": None if row.failure_code is None else str(row.failure_code),
        "failure_detail": (
            None if row.failure_detail is None else str(row.failure_detail)
        ),
        "payload": None,
        "current_claim": None,
        "source_task_id": (
            None if row.source_task_id is None else str(row.source_task_id)
        ),
    }


def _project_audit(
    row: AdminAuditLog,
    *,
    public_queue_id: UUID | None,
) -> dict[str, Any]:
    operation = _AUDIT_OPERATION_BY_CODE.get(int(row.operation_code))
    if operation is None:
        operation = f"operation_{int(row.operation_code)}"
    body: dict[str, Any] = {
        "audit_id": int(row.id),
        "audit_at": format_task_datetime(row.audit_at),
        "actor_id": str(row.actor_id),
        "operation": operation,
        "request_id": str(row.request_id),
        "previous_config_version": (
            None
            if row.previous_config_version is None
            else int(row.previous_config_version)
        ),
        "new_config_version": (
            None if row.new_config_version is None else int(row.new_config_version)
        ),
    }
    if public_queue_id is not None:
        body["queue_id"] = str(public_queue_id)
    else:
        body["queue_id"] = None
    return body
