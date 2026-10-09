"""Private admin bulk replay/cancel handlers (REC-02 / OPS-08).

Remapped from plan path ``api/admin/bulk.py`` to Phase 3.x flat admin module
convention (``api/admin.py`` already occupies the admin package name).
"""

from __future__ import annotations

import json
import logging
from collections.abc import Awaitable, Callable, Mapping, MutableMapping
from datetime import datetime, timezone
from typing import Any

from sqlalchemy.orm import Session, sessionmaker

from workhold.api.security import RequestContext, error_envelope, send_json
from workhold.domain.queue_control import DomainValidationError
from workhold.operations.bulk import (
    BULK_OP_CANCEL,
    BULK_OP_REPLAY,
    BulkConfirmationCodec,
    BulkReplayRateGate,
    emit_bulk_admin_correlation,
    execute_bulk_cancel,
    execute_bulk_replay,
    preview_bulk_operation,
)
from workhold.security.authorization import Operation
from workhold.settings import ADMIN_REPLAY_TTL_SECONDS_DEFAULT, Secret

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
    "confirmation_expired": 400,
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

_PREVIEW_OPS = frozenset(
    {Operation.PREVIEW_BULK_REPLAY, Operation.PREVIEW_BULK_CANCEL}
)
_EXECUTE_OPS = frozenset(
    {Operation.EXECUTE_BULK_REPLAY, Operation.EXECUTE_BULK_CANCEL}
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


def _parse_json_object(body: bytes) -> dict[str, Any]:
    try:
        parsed = json.loads(body.decode("utf-8") if body else b"{}")
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise DomainValidationError(
            "validation_failed",
            "request body must be JSON",
        ) from exc
    if not isinstance(parsed, dict):
        raise DomainValidationError(
            "validation_failed",
            "request body must be a JSON object",
        )
    return parsed


def build_admin_bulk_handler(
    *,
    session_factory: sessionmaker[Session],
    confirmation_secret: Secret,
    admin_replay_ttl_seconds: int = ADMIN_REPLAY_TTL_SECONDS_DEFAULT,
    rate_gate: BulkReplayRateGate | None = None,
) -> Handler:
    """Build preview/execute bulk replay and cancel private handlers."""

    codec = BulkConfirmationCodec(confirmation_secret)
    gate = rate_gate if rate_gate is not None else BulkReplayRateGate()

    async def handler(
        scope: MutableMapping[str, Any],
        _receive: Callable[[], Awaitable[dict[str, Any]]],
        send: Callable[[dict[str, Any]], Awaitable[None]],
        context: RequestContext,
        body: bytes,
    ) -> None:
        if context.operation in _PREVIEW_OPS:
            await _handle_preview(
                send,
                context,
                body,
                session_factory=session_factory,
                codec=codec,
            )
            return
        if context.operation in _EXECUTE_OPS:
            await _handle_execute(
                scope,
                send,
                context,
                body,
                session_factory=session_factory,
                codec=codec,
                rate_gate=gate,
                admin_replay_ttl_seconds=admin_replay_ttl_seconds,
            )
            return
        await send_json(
            send,
            status=500,
            payload=error_envelope(
                code="internal_error",
                message="bulk handler invoked for unexpected operation",
                retryable=True,
                request_id=context.request_id,
            ),
        )

    return handler


async def _handle_preview(
    send: Callable[[dict[str, Any]], Awaitable[None]],
    context: RequestContext,
    body: bytes,
    *,
    session_factory: sessionmaker[Session],
    codec: BulkConfirmationCodec,
) -> None:
    queue_name = context.path_params.get("queue_name") or context.authorization.queue_name
    operation = (
        BULK_OP_REPLAY
        if context.operation is Operation.PREVIEW_BULK_REPLAY
        else BULK_OP_CANCEL
    )
    op_name = context.operation.value

    def _emit(
        *,
        result: str,
        code: str | None,
        candidate_count: int | None = None,
        extras: Mapping[str, Any] | None = None,
    ) -> None:
        emit_bulk_admin_correlation(
            logger,
            operation=op_name,
            request_id=context.request_id,
            trace_id=context.request_id,
            actor_id=context.principal.principal_id,
            queue=queue_name if isinstance(queue_name, str) else None,
            result=result,
            code=code,
            candidate_count=candidate_count,
            extras=extras,
        )

    if not isinstance(queue_name, str) or not queue_name:
        _emit(result="denied", code="validation_failed")
        await _send_domain_error(
            send,
            request_id=context.request_id,
            code="validation_failed",
            message="queue_name path parameter is required",
        )
        return

    session = session_factory()
    try:
        parsed = _parse_json_object(body)
        unknown = set(parsed) - {"filters"}
        if unknown:
            raise DomainValidationError(
                "validation_failed",
                f"unknown body fields: {sorted(unknown)}",
            )
        filters = parsed.get("filters")
        preview = preview_bulk_operation(
            session,
            operation=operation,
            queue_name=queue_name,
            filters=filters if isinstance(filters, dict) or filters is None else filters,
            principal_id=context.principal.principal_id,
            codec=codec,
        )
        session.commit()
        _emit(
            result="previewed",
            code=None,
            candidate_count=preview.candidate_count,
            extras={
                "confirmation_token": preview.confirmation_token,
                "filters": filters,
                "sample_task_ids": list(preview.sample_task_ids),
                "payload": {"secret": True},
                "reason": "should-not-leak",
            },
        )
        await send_json(
            send,
            status=200,
            payload={
                "operation": preview.operation,
                "queue": preview.queue_name,
                "candidate_count": preview.candidate_count,
                "truncated": preview.truncated,
                "sample_task_ids": list(preview.sample_task_ids),
                "confirmation_token": preview.confirmation_token,
                "confirmation_expires_at": _format_dt(preview.confirmation_expires_at),
                "max_batch": preview.max_batch,
            },
            extra_headers={"X-Request-ID": context.request_id},
        )
    except DomainValidationError as exc:
        session.rollback()
        _emit(
            result="denied",
            code=exc.code,
            extras={"filters": locals().get("parsed"), "failure_detail": exc.message},
        )
        await _send_domain_error(
            send,
            request_id=context.request_id,
            code=exc.code,
            message=exc.message,
        )
    except Exception:
        session.rollback()
        _emit(result="internal_error", code="internal_error")
        await _send_domain_error(
            send,
            request_id=context.request_id,
            code="internal_error",
            message="internal error",
        )
    finally:
        session.close()


async def _handle_execute(
    scope: MutableMapping[str, Any],
    send: Callable[[dict[str, Any]], Awaitable[None]],
    context: RequestContext,
    body: bytes,
    *,
    session_factory: sessionmaker[Session],
    codec: BulkConfirmationCodec,
    rate_gate: BulkReplayRateGate,
    admin_replay_ttl_seconds: int,
) -> None:
    queue_name = context.path_params.get("queue_name") or context.authorization.queue_name
    is_replay = context.operation is Operation.EXECUTE_BULK_REPLAY
    op_name = context.operation.value
    headers = _header_map(scope)
    idempotency_key = headers.get("idempotency-key")

    def _emit(
        *,
        result: str,
        code: str | None,
        candidate_count: int | None = None,
        batch_size: int | None = None,
        succeeded_count: int | None = None,
        skipped_count: int | None = None,
        failed_count: int | None = None,
        extras: Mapping[str, Any] | None = None,
    ) -> None:
        emit_bulk_admin_correlation(
            logger,
            operation=op_name,
            request_id=context.request_id,
            trace_id=context.request_id,
            actor_id=context.principal.principal_id,
            queue=queue_name if isinstance(queue_name, str) else None,
            result=result,
            code=code,
            candidate_count=candidate_count,
            batch_size=batch_size,
            succeeded_count=succeeded_count,
            skipped_count=skipped_count,
            failed_count=failed_count,
            extras=extras,
        )

    if not isinstance(queue_name, str) or not queue_name:
        _emit(result="denied", code="validation_failed")
        await _send_domain_error(
            send,
            request_id=context.request_id,
            code="validation_failed",
            message="queue_name path parameter is required",
        )
        return

    if is_replay and (idempotency_key is None or not idempotency_key.strip()):
        _emit(result="denied", code="idempotency_key_required")
        await _send_domain_error(
            send,
            request_id=context.request_id,
            code="idempotency_key_required",
            message="Idempotency-Key header is required",
        )
        return

    session = session_factory()
    try:
        parsed = _parse_json_object(body)
        allowed = {
            "confirmation_token",
            "filters",
            "reason",
            "start_index",
            "batch_limit",
        }
        unknown = set(parsed) - allowed
        if unknown:
            raise DomainValidationError(
                "validation_failed",
                f"unknown body fields: {sorted(unknown)}",
            )
        token = parsed.get("confirmation_token")
        if not isinstance(token, str) or not token:
            raise DomainValidationError(
                "validation_failed",
                "confirmation_token is required",
            )
        reason = parsed.get("reason")
        start_index = parsed.get("start_index", 0)
        batch_limit = parsed.get("batch_limit")
        if not isinstance(start_index, int):
            raise DomainValidationError(
                "validation_failed",
                "start_index must be an integer",
            )
        if batch_limit is not None and not isinstance(batch_limit, int):
            raise DomainValidationError(
                "validation_failed",
                "batch_limit must be an integer",
            )
        filters = parsed.get("filters")
        if is_replay:
            result = execute_bulk_replay(
                session,
                queue_name=queue_name,
                filters=filters if isinstance(filters, dict) else filters,
                confirmation_token=token,
                reason=reason,
                principal_id=context.principal.principal_id,
                actor_id=context.principal.principal_id,
                request_id=context.request_id,
                idempotency_key=str(idempotency_key).strip(),
                codec=codec,
                rate_gate=rate_gate,
                start_index=start_index,
                batch_limit=batch_limit,
                admin_replay_ttl_seconds=admin_replay_ttl_seconds,
            )
        else:
            result = execute_bulk_cancel(
                session,
                queue_name=queue_name,
                filters=filters if isinstance(filters, dict) else filters,
                confirmation_token=token,
                reason=reason,
                principal_id=context.principal.principal_id,
                actor_id=context.principal.principal_id,
                request_id=context.request_id,
                codec=codec,
                start_index=start_index,
                batch_limit=batch_limit,
            )
        session.commit()
        _emit(
            result="partial" if result.partial else "succeeded",
            code=None,
            candidate_count=result.candidate_count,
            batch_size=result.processed,
            succeeded_count=result.succeeded,
            skipped_count=result.skipped,
            failed_count=result.failed,
            extras={
                "confirmation_token": token,
                "filters": filters,
                "idempotency_key": idempotency_key,
                "reason": reason,
                "payload": {"secret": True},
                "claim_token": "should-not-leak",
                "outcomes": [o.task_id for o in result.outcomes],
            },
        )
        await send_json(
            send,
            status=200,
            payload={
                "operation": result.operation,
                "queue": result.queue_name,
                "candidate_count": result.candidate_count,
                "start_index": result.start_index,
                "processed": result.processed,
                "succeeded": result.succeeded,
                "skipped": result.skipped,
                "failed": result.failed,
                "partial": result.partial,
                "next_start_index": result.next_start_index,
                "outcomes": [
                    {
                        "task_id": item.task_id,
                        "outcome": item.outcome,
                        "code": item.code,
                    }
                    for item in result.outcomes
                ],
                "warning": result.warning,
            },
            extra_headers={"X-Request-ID": context.request_id},
        )
    except DomainValidationError as exc:
        session.rollback()
        result_name = (
            "conflict"
            if exc.code in {"idempotency_conflict", "queue_draining"}
            else "denied"
        )
        _emit(
            result=result_name,
            code=exc.code,
            extras={
                "confirmation_token": "redacted",
                "filters": "redacted",
                "reason": "redacted",
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
        _emit(result="internal_error", code="internal_error")
        await _send_domain_error(
            send,
            request_id=context.request_id,
            code="internal_error",
            message="internal error",
        )
    finally:
        session.close()
