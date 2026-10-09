"""Observer/admin-authorized bounded inspection list handlers (API-05).

Remapped from plan path ``api/admin/inspection.py`` to Phase 3.x flat admin
module convention (``api/admin.py`` already occupies the admin package name).
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Mapping, MutableMapping
from datetime import datetime, timezone
from typing import Any, Final
from urllib.parse import parse_qs
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.orm import Session, sessionmaker

from workhold.api.security import (
    RequestContext,
    error_envelope,
    send_json,
    send_protocol_error,
)
from workhold.domain.queue_control import DomainValidationError
from workhold.operations.inspection import OperationalInspectionService
from workhold.security.authorization import Authorizer, Operation
from workhold.security.cursors import InspectionCursorCodec
from workhold.security.principals import ServiceRole
from workhold.security.redaction import sanitize_for_diagnostics
from workhold.settings import Secret
from workhold.storage.models import Queue, TaskActive, TaskTerminal

Handler = Callable[
    [
        MutableMapping[str, Any],
        Callable[[], Awaitable[dict[str, Any]]],
        Callable[[dict[str, Any]], Awaitable[None]],
        RequestContext,
        bytes,
    ],
    Awaitable[None],
]

_INSPECTION_OPS: Final[frozenset[Operation]] = frozenset(
    {
        Operation.LIST_INSPECTION_TASKS,
        Operation.LIST_INSPECTION_ATTEMPTS,
        Operation.LIST_DEAD_LETTERS,
        Operation.LIST_ADMIN_AUDIT,
    }
)


def build_admin_inspection_handler(
    *,
    session_factory: sessionmaker[Session],
    authorizer: Authorizer,
    cursor_secret: Secret,
) -> Handler:
    """Build handlers for operational list endpoints on the private admin plane."""

    service = OperationalInspectionService(
        cursor_codec=InspectionCursorCodec(cursor_secret)
    )

    async def handler(
        scope: MutableMapping[str, Any],
        _receive: Callable[[], Awaitable[dict[str, Any]]],
        send: Callable[[dict[str, Any]], Awaitable[None]],
        context: RequestContext,
        body: bytes,
    ) -> None:
        _ = body
        if context.operation not in _INSPECTION_OPS:
            await send_json(
                send,
                status=500,
                payload=error_envelope(
                    code="internal_error",
                    message="inspection handler invoked for unexpected operation",
                    retryable=True,
                    request_id=context.request_id,
                ),
            )
            return

        query = _query_map(scope)
        _ = sanitize_for_diagnostics(
            {
                "request_id": context.request_id,
                "operation": context.operation.value,
                "principal_id": context.principal.principal_id,
                "has_cursor": "cursor" in query,
                "queue_name": query.get("queue_name"),
            }
        )

        session = session_factory()
        try:
            page = _dispatch_list(
                service,
                session,
                authorizer=authorizer,
                context=context,
                query=query,
            )
            payload = {"items": page.items, "next_cursor": page.next_cursor}
            # Defense in depth: never leak claim tokens / payloads via lists.
            rendered = sanitize_for_diagnostics(payload)
            if not isinstance(rendered, dict):
                rendered = {"items": [], "next_cursor": None}
            # sanitize_for_diagnostics allowlists diagnostics keys; rebuild success body.
            safe_items: list[dict[str, Any]] = []
            for item in page.items:
                cleaned = dict(item)
                cleaned.pop("claim_token", None)
                if cleaned.get("payload") is not None:
                    cleaned["payload"] = None
                safe_items.append(cleaned)
            await send_json(
                send,
                status=200,
                payload={"items": safe_items, "next_cursor": page.next_cursor},
                extra_headers={"X-Request-ID": context.request_id},
            )
        except DomainValidationError as exc:
            await send_protocol_error(
                send,
                request_id=context.request_id,
                code=exc.code,
                message=exc.message,
                retryable=False,
            )
        except Exception:
            await send_json(
                send,
                status=500,
                payload=error_envelope(
                    code="internal_error",
                    message="failed to list inspection records",
                    retryable=True,
                    request_id=context.request_id,
                ),
            )
        finally:
            session.close()

    return handler


def _dispatch_list(
    service: OperationalInspectionService,
    session: Session,
    *,
    authorizer: Authorizer,
    context: RequestContext,
    query: Mapping[str, str],
):
    limit = _parse_optional_limit(query.get("limit"))
    cursor = query.get("cursor")
    extras = {
        key: value
        for key, value in query.items()
        if key
        not in {
            "cursor",
            "limit",
            "queue_name",
            "from",
            "to",
            "task_id",
        }
    }

    if context.operation is Operation.LIST_INSPECTION_TASKS:
        queue_name = _require_queue_name(query)
        _assert_queue_visible(authorizer, context, queue_name)
        return service.list_tasks(
            session,
            queue_name=queue_name,
            limit=limit,
            cursor=cursor,
            extra_filters=extras,
        )

    if context.operation is Operation.LIST_INSPECTION_ATTEMPTS:
        task_id = _require_task_id(query)
        time_from, time_to = _require_time_bounds(query)
        _assert_task_queue_visible(
            session,
            authorizer=authorizer,
            context=context,
            task_id=task_id,
        )
        return service.list_attempts(
            session,
            task_id=task_id,
            time_from=time_from,
            time_to=time_to,
            limit=limit,
            cursor=cursor,
            extra_filters=extras,
        )

    if context.operation is Operation.LIST_DEAD_LETTERS:
        queue_name = _require_queue_name(query)
        _assert_queue_visible(authorizer, context, queue_name)
        time_from, time_to = _require_time_bounds(query)
        return service.list_dead_letters(
            session,
            queue_name=queue_name,
            time_from=time_from,
            time_to=time_to,
            limit=limit,
            cursor=cursor,
            extra_filters=extras,
        )

    # LIST_ADMIN_AUDIT
    queue_name = query.get("queue_name")
    if queue_name is not None:
        _assert_queue_visible(authorizer, context, queue_name)
    time_from, time_to = _require_time_bounds(query)
    return service.list_admin_audit(
        session,
        time_from=time_from,
        time_to=time_to,
        queue_name=queue_name,
        limit=limit,
        cursor=cursor,
        extra_filters=extras,
    )


def _assert_queue_visible(
    authorizer: Authorizer,
    context: RequestContext,
    queue_name: str,
) -> None:
    if context.principal.role is ServiceRole.ADMIN:
        return
    if not authorizer.allows_queue(context.principal, queue_name):
        raise DomainValidationError("permission_denied", "permission denied")


def _assert_task_queue_visible(
    session: Session,
    *,
    authorizer: Authorizer,
    context: RequestContext,
    task_id: UUID,
) -> None:
    if context.principal.role is ServiceRole.ADMIN:
        return

    active = session.execute(
        select(Queue.name)
        .join(TaskActive, TaskActive.queue_id == Queue.id)
        .where(TaskActive.task_id == task_id)
    ).scalar_one_or_none()
    if active is not None:
        _assert_queue_visible(authorizer, context, str(active))
        return
    terminal = session.execute(
        select(Queue.name)
        .join(TaskTerminal, TaskTerminal.queue_id == Queue.id)
        .where(TaskTerminal.task_id == task_id)
    ).scalar_one_or_none()
    if terminal is None:
        # Non-enumerating: same as missing task for out-of-scope observers.
        raise DomainValidationError("permission_denied", "permission denied")
    _assert_queue_visible(authorizer, context, str(terminal))


def _require_queue_name(query: Mapping[str, str]) -> str:
    value = query.get("queue_name")
    if value is None or value == "":
        raise DomainValidationError("validation_failed", "queue_name is required")
    return value


def _require_task_id(query: Mapping[str, str]) -> UUID:
    raw = query.get("task_id")
    if raw is None or raw == "":
        raise DomainValidationError("validation_failed", "task_id is required")
    try:
        return UUID(raw)
    except (ValueError, AttributeError, TypeError) as exc:
        raise DomainValidationError(
            "validation_failed",
            "task_id must be a UUID",
        ) from exc


def _require_time_bounds(query: Mapping[str, str]) -> tuple[datetime, datetime]:
    raw_from = query.get("from")
    raw_to = query.get("to")
    if raw_from is None or raw_to is None or raw_from == "" or raw_to == "":
        raise DomainValidationError(
            "validation_failed",
            "from and to time bounds are required",
        )
    return _parse_datetime(raw_from), _parse_datetime(raw_to)


def _parse_datetime(raw: str) -> datetime:
    stamp = raw[:-1] + "+00:00" if raw.endswith("Z") else raw
    try:
        value = datetime.fromisoformat(stamp)
    except ValueError as exc:
        raise DomainValidationError(
            "validation_failed",
            "time bound must be an RFC3339 date-time",
        ) from exc
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _parse_optional_limit(raw: str | None) -> int | None:
    if raw is None:
        return None
    try:
        return int(raw)
    except (TypeError, ValueError) as exc:
        raise DomainValidationError(
            "validation_failed",
            "limit must be an integer",
        ) from exc


def _query_map(scope: Mapping[str, Any]) -> dict[str, str]:
    raw = scope.get("query_string", b"")
    if isinstance(raw, memoryview):
        raw = raw.tobytes()
    if not isinstance(raw, (bytes, bytearray)):
        return {}
    parsed = parse_qs(raw.decode("latin-1"), keep_blank_values=False)
    out: dict[str, str] = {}
    for key, values in parsed.items():
        if values:
            out[key] = values[0]
    return out
