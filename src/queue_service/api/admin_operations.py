"""Private admin drain/maintenance routine handlers (CTRL-06 / OPS-08).

Remapped from plan path ``api/admin/operations.py`` to Phase 3.x flat admin
module convention (``api/admin.py`` already occupies the admin package name).
"""

from __future__ import annotations

import logging
from collections.abc import Awaitable, Callable, Mapping, MutableMapping
from datetime import datetime, timezone
from typing import Any

from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker

from queue_service.api.security import RequestContext, error_envelope, send_json
from queue_service.domain.queue_control import DomainValidationError
from queue_service.health import DEFAULT_PARTITION_PREMAKE_DAYS
from queue_service.operations.routine import (
    emit_routine_admin_correlation,
    read_maintenance_status,
    trigger_partition_maintenance,
)
from queue_service.security.authorization import Operation
from queue_service.security.payload_policy import PayloadRetentionPolicy
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
    "idempotency_conflict": 409,
    "permission_denied": 403,
    "unauthenticated": 401,
    "dependency_unavailable": 503,
    "internal_error": 500,
}

_RETRYABLE = frozenset({"dependency_unavailable", "internal_error", "idempotency_conflict"})


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


def build_admin_operations_handler(
    *,
    session_factory: sessionmaker[Session],
    engine: Engine,
    payload_retention_policy: PayloadRetentionPolicy,
    registry_purge_batch_size: int = 1000,
    admin_replay_ttl_seconds: int = ADMIN_REPLAY_TTL_SECONDS_DEFAULT,
    horizon_days: int = DEFAULT_PARTITION_PREMAKE_DAYS,
) -> Handler:
    """Build getMaintenanceStatus / runMaintenance private handlers."""

    async def handler(
        scope: MutableMapping[str, Any],
        _receive: Callable[[], Awaitable[dict[str, Any]]],
        send: Callable[[dict[str, Any]], Awaitable[None]],
        context: RequestContext,
        body: bytes,
    ) -> None:
        _ = body
        if context.operation is Operation.GET_MAINTENANCE_STATUS:
            await _handle_get_status(
                send,
                context,
                session_factory=session_factory,
            )
            return
        if context.operation is Operation.RUN_MAINTENANCE:
            await _handle_run(
                scope,
                send,
                context,
                session_factory=session_factory,
                engine=engine,
                payload_retention_policy=payload_retention_policy,
                registry_purge_batch_size=registry_purge_batch_size,
                admin_replay_ttl_seconds=admin_replay_ttl_seconds,
                horizon_days=horizon_days,
            )
            return
        await send_json(
            send,
            status=500,
            payload=error_envelope(
                code="internal_error",
                message="operations handler invoked for unexpected operation",
                retryable=True,
                request_id=context.request_id,
            ),
        )

    return handler


async def _handle_get_status(
    send: Callable[[dict[str, Any]], Awaitable[None]],
    context: RequestContext,
    *,
    session_factory: sessionmaker[Session],
) -> None:
    session = session_factory()
    try:
        status = read_maintenance_status(session)
        emit_routine_admin_correlation(
            logger,
            operation=context.operation.value,
            request_id=context.request_id,
            trace_id=context.request_id,
            actor_id=context.principal.principal_id,
            queue=None,
            config_version=None,
            maintenance_run_id=None,
            result="success",
            code=None,
        )
        await send_json(
            send,
            status=200,
            payload=status,
            extra_headers={"X-Request-ID": context.request_id},
        )
    except Exception:
        emit_routine_admin_correlation(
            logger,
            operation=context.operation.value,
            request_id=context.request_id,
            trace_id=context.request_id,
            actor_id=context.principal.principal_id,
            queue=None,
            config_version=None,
            maintenance_run_id=None,
            result="internal_error",
            code="internal_error",
        )
        await _send_domain_error(
            send,
            request_id=context.request_id,
            code="internal_error",
            message="failed to read maintenance status",
        )
    finally:
        session.close()


async def _handle_run(
    scope: MutableMapping[str, Any],
    send: Callable[[dict[str, Any]], Awaitable[None]],
    context: RequestContext,
    *,
    session_factory: sessionmaker[Session],
    engine: Engine,
    payload_retention_policy: PayloadRetentionPolicy,
    registry_purge_batch_size: int,
    admin_replay_ttl_seconds: int,
    horizon_days: int,
) -> None:
    headers = _header_map(scope)
    idempotency_key = headers.get("idempotency-key")
    if idempotency_key is None or not idempotency_key.strip():
        emit_routine_admin_correlation(
            logger,
            operation=context.operation.value,
            request_id=context.request_id,
            trace_id=context.request_id,
            actor_id=context.principal.principal_id,
            queue=None,
            config_version=None,
            maintenance_run_id=None,
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

    session = session_factory()
    try:
        result = trigger_partition_maintenance(
            session,
            engine=engine,
            principal_id=context.principal.principal_id,
            actor_id=context.principal.principal_id,
            request_id=context.request_id,
            idempotency_key=idempotency_key.strip(),
            payload_retention_policy=payload_retention_policy,
            registry_purge_batch_size=registry_purge_batch_size,
            admin_replay_ttl_seconds=admin_replay_ttl_seconds,
            horizon_days=horizon_days,
        )
        if result.outcome != "skipped_lock":
            session.commit()
        else:
            session.rollback()

        emit_routine_admin_correlation(
            logger,
            operation=context.operation.value,
            request_id=context.request_id,
            trace_id=context.request_id,
            actor_id=context.principal.principal_id,
            queue=None,
            config_version=None,
            maintenance_run_id=result.maintenance_run_id,
            result=result.outcome,
            code=result.error_code,
        )

        status_payload = dict(result.status)
        if result.outcome == "skipped_lock":
            status_payload["outcome"] = "skipped_lock"

        payload = {
            "status": status_payload,
            "replayed": result.replayed,
            "admin_replay_expires_at": _format_dt(result.admin_replay_expires_at)
            or status_payload.get("updated_at"),
        }
        await send_json(
            send,
            status=200,
            payload=payload,
            extra_headers={"X-Request-ID": context.request_id},
        )
    except DomainValidationError as exc:
        session.rollback()
        emit_routine_admin_correlation(
            logger,
            operation=context.operation.value,
            request_id=context.request_id,
            trace_id=context.request_id,
            actor_id=context.principal.principal_id,
            queue=None,
            config_version=None,
            maintenance_run_id=None,
            result="conflict",
            code=exc.code,
        )
        await _send_domain_error(
            send,
            request_id=context.request_id,
            code=exc.code,
            message=exc.message,
        )
    except Exception:
        session.rollback()
        emit_routine_admin_correlation(
            logger,
            operation=context.operation.value,
            request_id=context.request_id,
            trace_id=context.request_id,
            actor_id=context.principal.principal_id,
            queue=None,
            config_version=None,
            maintenance_run_id=None,
            result="internal_error",
            code="internal_error",
        )
        await _send_domain_error(
            send,
            request_id=context.request_id,
            code="internal_error",
            message="maintenance trigger failed",
        )
    finally:
        session.close()
