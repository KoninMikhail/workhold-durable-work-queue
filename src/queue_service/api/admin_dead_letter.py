"""Private admin dead-letter replay handlers (CTRL-06 / REC-01 / OPS-08).

Remapped from plan path ``api/admin/dead_letter.py`` to Phase 3.x flat admin
module convention (``api/admin.py`` already occupies the admin package name).
"""

from __future__ import annotations

import json
import logging
from collections.abc import Awaitable, Callable, Mapping, MutableMapping
from datetime import datetime, timezone
from typing import Any
from uuid import UUID

from sqlalchemy.orm import Session, sessionmaker

from queue_service.api.security import RequestContext, error_envelope, send_json
from queue_service.domain.queue_control import DomainValidationError
from queue_service.operations.dead_letter import (
    emit_dead_letter_admin_correlation,
    replay_dead_letter,
)
from queue_service.security.authorization import Operation
from queue_service.settings import ADMIN_REPLAY_TTL_SECONDS_DEFAULT

logger = logging.getLogger(__name__)

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

_ERROR_HTTP: Mapping[str, int] = {
    "validation_failed": 400,
    "idempotency_key_required": 400,
    "payload_too_large": 413,
    "idempotency_conflict": 409,
    "queue_not_found": 404,
    "task_not_found": 404,
    "queue_draining": 409,
    "resource_exhausted": 429,
    "permission_denied": 403,
    "unauthenticated": 401,
    "dependency_unavailable": 503,
    "internal_error": 500,
}

_RETRYABLE = frozenset(
    {
        "dependency_unavailable",
        "internal_error",
        "idempotency_conflict",
        "queue_draining",
        "resource_exhausted",
    }
)


def _header_map(scope: Mapping[str, Any]) -> dict[str, str]:
    out: dict[str, str] = {}
    for raw_key, raw_val in scope.get("headers", []):
        key = raw_key.decode("latin-1").lower()
        out[key] = raw_val.decode("latin-1")
    return out


def _format_dt(value: datetime | None) -> str | None:
    if value is None:
        return None
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


async def _send_domain_error(
    send: Callable[[dict[str, Any]], Awaitable[None]],
    *,
    request_id: str,
    code: str,
    message: str,
) -> None:
    status = _ERROR_HTTP.get(code, 500)
    await send_json(
        send,
        status=status,
        payload=error_envelope(
            code=code,
            message=message,
            retryable=code in _RETRYABLE,
            request_id=request_id,
        ),
        extra_headers={"X-Request-ID": request_id},
    )


def build_admin_dead_letter_handler(
    *,
    session_factory: sessionmaker[Session],
    admin_replay_ttl_seconds: int = ADMIN_REPLAY_TTL_SECONDS_DEFAULT,
) -> Handler:
    """Build ``replayDeadLetter`` private handler."""

    async def handler(
        scope: MutableMapping[str, Any],
        _receive: Callable[[], Awaitable[dict[str, Any]]],
        send: Callable[[dict[str, Any]], Awaitable[None]],
        context: RequestContext,
        body: bytes,
    ) -> None:
        if context.operation is not Operation.REPLAY_DEAD_LETTER:
            await send_json(
                send,
                status=500,
                payload=error_envelope(
                    code="internal_error",
                    message="dead-letter handler invoked for unexpected operation",
                    retryable=True,
                    request_id=context.request_id,
                ),
            )
            return
        await _handle_replay(
            scope,
            send,
            context,
            body,
            session_factory=session_factory,
            admin_replay_ttl_seconds=admin_replay_ttl_seconds,
        )

    return handler


async def _handle_replay(
    scope: MutableMapping[str, Any],
    send: Callable[[dict[str, Any]], Awaitable[None]],
    context: RequestContext,
    body: bytes,
    *,
    session_factory: sessionmaker[Session],
    admin_replay_ttl_seconds: int,
) -> None:
    queue_name = context.path_params.get("queue_name") or context.authorization.queue_name
    raw_task_id = context.path_params.get("task_id")

    headers = _header_map(scope)
    idempotency_key = headers.get("idempotency-key")
    if idempotency_key is None or not idempotency_key.strip():
        emit_dead_letter_admin_correlation(
            logger,
            operation=context.operation.value,
            request_id=context.request_id,
            trace_id=context.request_id,
            actor_id=context.principal.principal_id,
            queue=queue_name if isinstance(queue_name, str) else None,
            task_id=None,
            source_task_id=raw_task_id if isinstance(raw_task_id, str) else None,
            policy_version=None,
            result="denied",
            code="idempotency_key_required",
        )
        await _send_domain_error(
            send,
            request_id=context.request_id,
            code="idempotency_key_required",
            message="Idempotency-Key header is required",
        )
        return

    if not isinstance(queue_name, str) or not queue_name:
        emit_dead_letter_admin_correlation(
            logger,
            operation=context.operation.value,
            request_id=context.request_id,
            trace_id=context.request_id,
            actor_id=context.principal.principal_id,
            queue=None,
            task_id=None,
            source_task_id=None,
            policy_version=None,
            result="denied",
            code="validation_failed",
        )
        await _send_domain_error(
            send,
            request_id=context.request_id,
            code="validation_failed",
            message="queue_name path parameter is required",
        )
        return

    try:
        source_task_id = UUID(str(raw_task_id))
    except (TypeError, ValueError):
        emit_dead_letter_admin_correlation(
            logger,
            operation=context.operation.value,
            request_id=context.request_id,
            trace_id=context.request_id,
            actor_id=context.principal.principal_id,
            queue=queue_name,
            task_id=None,
            source_task_id=None,
            policy_version=None,
            result="denied",
            code="validation_failed",
        )
        await _send_domain_error(
            send,
            request_id=context.request_id,
            code="validation_failed",
            message="task_id path parameter must be a UUID",
        )
        return

    try:
        parsed = json.loads(body.decode("utf-8") if body else b"{}")
    except (UnicodeDecodeError, json.JSONDecodeError):
        emit_dead_letter_admin_correlation(
            logger,
            operation=context.operation.value,
            request_id=context.request_id,
            trace_id=context.request_id,
            actor_id=context.principal.principal_id,
            queue=queue_name,
            task_id=None,
            source_task_id=str(source_task_id),
            policy_version=None,
            result="denied",
            code="validation_failed",
        )
        await _send_domain_error(
            send,
            request_id=context.request_id,
            code="validation_failed",
            message="request body must be JSON",
        )
        return

    if not isinstance(parsed, dict):
        emit_dead_letter_admin_correlation(
            logger,
            operation=context.operation.value,
            request_id=context.request_id,
            trace_id=context.request_id,
            actor_id=context.principal.principal_id,
            queue=queue_name,
            task_id=None,
            source_task_id=str(source_task_id),
            policy_version=None,
            result="denied",
            code="validation_failed",
        )
        await _send_domain_error(
            send,
            request_id=context.request_id,
            code="validation_failed",
            message="request body must be a JSON object",
        )
        return

    unknown = set(parsed) - {"reason"}
    if unknown:
        emit_dead_letter_admin_correlation(
            logger,
            operation=context.operation.value,
            request_id=context.request_id,
            trace_id=context.request_id,
            actor_id=context.principal.principal_id,
            queue=queue_name,
            task_id=None,
            source_task_id=str(source_task_id),
            policy_version=None,
            result="denied",
            code="validation_failed",
            extras={"reason": parsed.get("reason"), "payload": parsed},
        )
        await _send_domain_error(
            send,
            request_id=context.request_id,
            code="validation_failed",
            message=f"unknown body fields: {sorted(unknown)}",
        )
        return

    session = session_factory()
    try:
        result = replay_dead_letter(
            session,
            queue_name=queue_name,
            source_task_id=source_task_id,
            reason=parsed.get("reason"),
            principal_id=context.principal.principal_id,
            actor_id=context.principal.principal_id,
            request_id=context.request_id,
            idempotency_key=idempotency_key.strip(),
            admin_replay_ttl_seconds=admin_replay_ttl_seconds,
        )
        session.commit()

        emit_dead_letter_admin_correlation(
            logger,
            operation=context.operation.value,
            request_id=context.request_id,
            trace_id=context.request_id,
            actor_id=context.principal.principal_id,
            queue=result.queue_name,
            task_id=str(result.task_id),
            source_task_id=str(result.source_task_id),
            policy_version=result.policy_version,
            result="replayed" if result.replayed else "succeeded",
            code=None,
            extras={
                "reason": parsed.get("reason"),
                "idempotency_key": idempotency_key,
                "payload": {"secret": True},
                "claim_token": "should-not-leak",
            },
        )

        payload = {
            "task_id": str(result.task_id),
            "source_task_id": str(result.source_task_id),
            "queue": result.queue_name,
            "policy_version": result.policy_version,
            "replayed": result.replayed,
            "admin_replay_expires_at": _format_dt(result.admin_replay_expires_at),
            "warning": result.warning,
        }
        await send_json(
            send,
            status=200,
            payload=payload,
            extra_headers={"X-Request-ID": context.request_id},
        )
    except DomainValidationError as exc:
        session.rollback()
        result_name = (
            "conflict"
            if exc.code in {"idempotency_conflict", "queue_draining"}
            else "denied"
        )
        emit_dead_letter_admin_correlation(
            logger,
            operation=context.operation.value,
            request_id=context.request_id,
            trace_id=context.request_id,
            actor_id=context.principal.principal_id,
            queue=queue_name,
            task_id=None,
            source_task_id=str(source_task_id),
            policy_version=None,
            result=result_name,
            code=exc.code,
            extras={
                "reason": parsed.get("reason"),
                "idempotency_key": idempotency_key,
                "failure_detail": exc.message,
            },
        )
        await _send_domain_error(
            send,
            request_id=context.request_id,
            code=exc.code,
            message=exc.message,
        )
    except Exception:
        session.rollback()
        emit_dead_letter_admin_correlation(
            logger,
            operation=context.operation.value,
            request_id=context.request_id,
            trace_id=context.request_id,
            actor_id=context.principal.principal_id,
            queue=queue_name,
            task_id=None,
            source_task_id=str(source_task_id),
            policy_version=None,
            result="internal_error",
            code="internal_error",
        )
        await _send_domain_error(
            send,
            request_id=context.request_id,
            code="internal_error",
            message="internal error",
        )
    finally:
        session.close()
