"""Private break-glass emergency repair handlers (REC-03 / OPS-08).

Remapped from plan path ``api/admin/break_glass.py`` to Phase 3.x flat admin
module convention (``api/admin.py`` already occupies the admin package name).
"""

from __future__ import annotations

import json
import logging
from collections.abc import Awaitable, Callable, Mapping, MutableMapping
from typing import Any
from uuid import UUID

from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker

from workhold.api.security import RequestContext, error_envelope, send_json
from workhold.domain.queue_control import DomainValidationError
from workhold.observability.metrics import KernelMetrics
from workhold.operations.break_glass import (
    emit_break_glass_correlation,
    force_delivery_dead_letter,
    force_delivery_reclaim,
    force_lease_expiry,
    parse_break_glass_ack,
    raise_replay_limit,
    reconcile_counters,
    record_break_glass_success,
    repair_registry_entry,
    drop_expired_partition,
    _hash_incident,
)
from workhold.operations.bulk import BulkReplayRateGate
from workhold.security.authorization import Operation
from workhold.security.payload_policy import PayloadRetentionPolicy

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
    "permission_denied": 403,
    "unauthenticated": 401,
    "queue_not_found": 404,
    "task_not_found": 404,
    "dependency_unavailable": 503,
    "internal_error": 500,
}

_RETRYABLE = frozenset({"dependency_unavailable", "internal_error"})

_BREAK_GLASS_OPS = frozenset(
    {
        Operation.FORCE_LEASE_EXPIRY,
        Operation.RECONCILE_COUNTERS,
        Operation.RAISE_REPLAY_LIMIT,
        Operation.DROP_EXPIRED_PARTITION,
        Operation.REPAIR_REGISTRY_ENTRY,
        Operation.FORCE_DELIVERY_RECLAIM,
        Operation.FORCE_DELIVERY_DEAD_LETTER,
    }
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


def build_admin_break_glass_handler(
    *,
    session_factory: sessionmaker[Session],
    engine: Engine,
    rate_gate: BulkReplayRateGate,
    payload_retention_policy: PayloadRetentionPolicy,
    metrics: KernelMetrics | None = None,
) -> Handler:
    """Build allowlisted break-glass private handlers."""

    kernel_metrics = metrics if metrics is not None else KernelMetrics(process_role="admin")

    async def handler(
        scope: MutableMapping[str, Any],
        _receive: Callable[[], Awaitable[dict[str, Any]]],
        send: Callable[[dict[str, Any]], Awaitable[None]],
        context: RequestContext,
        body: bytes,
    ) -> None:
        _ = scope
        if context.operation not in _BREAK_GLASS_OPS:
            await send_json(
                send,
                status=500,
                payload=error_envelope(
                    code="internal_error",
                    message="break-glass handler invoked for unexpected operation",
                    retryable=True,
                    request_id=context.request_id,
                ),
            )
            return

        queue_name = (
            context.path_params.get("queue_name")
            or context.authorization.queue_name
        )
        incident_hash: str | None = None
        target_id: str | None = None
        try:
            payload = _parse_json_object(body)
            ack = parse_break_glass_ack(payload)
            incident_hash = _hash_incident(ack.incident_reference)
            session = session_factory()
            try:
                if context.operation is Operation.FORCE_LEASE_EXPIRY:
                    if not isinstance(queue_name, str) or not queue_name:
                        raise DomainValidationError(
                            "validation_failed", "queue_name required"
                        )
                    raw_task = context.path_params.get("task_id") or payload.get(
                        "task_id"
                    )
                    if not isinstance(raw_task, str):
                        raise DomainValidationError(
                            "validation_failed", "task_id required"
                        )
                    task_id = UUID(raw_task)
                    target_id = str(task_id)
                    result = force_lease_expiry(
                        session,
                        queue_name=queue_name,
                        task_id=task_id,
                        actor_id=context.principal.principal_id,
                        request_id=context.request_id,
                        ack=ack,
                    )
                    session.commit()
                    record_break_glass_success(
                        kernel_metrics,
                        operation=context.operation.value,
                        result=result.outcome,
                        queue=queue_name,
                    )
                    emit_break_glass_correlation(
                        logger,
                        operation=context.operation.value,
                        request_id=context.request_id,
                        trace_id=context.request_id,
                        actor_id=context.principal.principal_id,
                        queue=queue_name,
                        incident_ref_hash=incident_hash,
                        target_id=target_id,
                        result=result.outcome,
                        code=None,
                        generation=result.generation,
                        extras={
                            "reason": ack.reason,
                            "incident_reference": ack.incident_reference,
                            "claim_token": "must-not-leak",
                            "payload": payload,
                        },
                    )
                    await send_json(
                        send,
                        status=200,
                        payload={
                            "operation": result.operation,
                            "queue": result.queue,
                            "target_id": result.target_id,
                            "outcome": result.outcome,
                            "generation": result.generation,
                        },
                        extra_headers={"X-Request-ID": context.request_id},
                    )
                    return

                if context.operation is Operation.RECONCILE_COUNTERS:
                    if queue_name is None:
                        raise DomainValidationError(
                            "validation_failed", "queue_name required"
                        )
                    target_id = queue_name
                    result = reconcile_counters(
                        session,
                        queue_name=queue_name,
                        actor_id=context.principal.principal_id,
                        request_id=context.request_id,
                        ack=ack,
                    )
                    session.commit()
                    record_break_glass_success(
                        kernel_metrics,
                        operation=context.operation.value,
                        result=result.outcome,
                        queue=queue_name if isinstance(queue_name, str) else None,
                    )
                    emit_break_glass_correlation(
                        logger,
                        operation=context.operation.value,
                        request_id=context.request_id,
                        trace_id=context.request_id,
                        actor_id=context.principal.principal_id,
                        queue=queue_name,
                        incident_ref_hash=incident_hash,
                        target_id=target_id,
                        result=result.outcome,
                        code=None,
                        extras={"reason": ack.reason, "sql": "UPDATE queue_counters"},
                    )
                    await send_json(
                        send,
                        status=200,
                        payload={
                            "operation": result.operation,
                            "queue": result.queue,
                            "target_id": result.target_id,
                            "outcome": result.outcome,
                            "delayed_count": result.delayed_count,
                            "ready_count": result.ready_count,
                            "leased_count": result.leased_count,
                        },
                        extra_headers={"X-Request-ID": context.request_id},
                    )
                    return

                if context.operation is Operation.RAISE_REPLAY_LIMIT:
                    if queue_name is None:
                        raise DomainValidationError(
                            "validation_failed", "queue_name required"
                        )
                    target_id = queue_name
                    factor = payload.get("factor", 2.0)
                    ttl_seconds = payload.get("ttl_seconds", 60)
                    if not isinstance(factor, (int, float)) or isinstance(factor, bool):
                        raise DomainValidationError(
                            "validation_failed", "factor must be a number"
                        )
                    if not isinstance(ttl_seconds, int) or isinstance(ttl_seconds, bool):
                        raise DomainValidationError(
                            "validation_failed", "ttl_seconds must be an integer"
                        )
                    result = raise_replay_limit(
                        session,
                        queue_name=queue_name,
                        actor_id=context.principal.principal_id,
                        request_id=context.request_id,
                        ack=ack,
                        rate_gate=rate_gate,
                        factor=float(factor),
                        ttl_seconds=ttl_seconds,
                    )
                    session.commit()
                    record_break_glass_success(
                        kernel_metrics,
                        operation=context.operation.value,
                        result=result.outcome,
                        queue=queue_name if isinstance(queue_name, str) else None,
                    )
                    emit_break_glass_correlation(
                        logger,
                        operation=context.operation.value,
                        request_id=context.request_id,
                        trace_id=context.request_id,
                        actor_id=context.principal.principal_id,
                        queue=queue_name,
                        incident_ref_hash=incident_hash,
                        target_id=target_id,
                        result=result.outcome,
                        code=None,
                        extras={"reason": ack.reason, "credential": "x"},
                    )
                    await send_json(
                        send,
                        status=200,
                        payload={
                            "operation": result.operation,
                            "queue": result.queue,
                            "target_id": result.target_id,
                            "outcome": result.outcome,
                            "effective_rps": result.effective_rps,
                        },
                        extra_headers={"X-Request-ID": context.request_id},
                    )
                    return

                if context.operation is Operation.DROP_EXPIRED_PARTITION:
                    partition_name = context.path_params.get(
                        "partition_name"
                    ) or payload.get("partition_name")
                    if not isinstance(partition_name, str):
                        raise DomainValidationError(
                            "validation_failed", "partition_name required"
                        )
                    target_id = partition_name
                    result = drop_expired_partition(
                        engine,
                        session,
                        partition_name=partition_name,
                        actor_id=context.principal.principal_id,
                        request_id=context.request_id,
                        ack=ack,
                        payload_retention_policy=payload_retention_policy,
                    )
                    # drop_expired_partition commits the audit txn itself.
                    record_break_glass_success(
                        kernel_metrics,
                        operation=context.operation.value,
                        result=result.outcome,
                        queue=None,
                    )
                    emit_break_glass_correlation(
                        logger,
                        operation=context.operation.value,
                        request_id=context.request_id,
                        trace_id=context.request_id,
                        actor_id=context.principal.principal_id,
                        queue=None,
                        incident_ref_hash=incident_hash,
                        target_id=None,  # never log raw partition name
                        result=result.outcome,
                        code=None,
                        extras={
                            "partition_name": partition_name,
                            "sql": "DROP TABLE",
                            "reason": ack.reason,
                        },
                    )
                    await send_json(
                        send,
                        status=200,
                        payload={
                            "operation": result.operation,
                            "target_id": hashlib_sha16(partition_name),
                            "outcome": result.outcome,
                        },
                        extra_headers={"X-Request-ID": context.request_id},
                    )
                    return

                if context.operation is Operation.REPAIR_REGISTRY_ENTRY:
                    if queue_name is None:
                        raise DomainValidationError(
                            "validation_failed", "queue_name required"
                        )
                    entry_id = payload.get("entry_id")
                    registry = payload.get("registry", "enqueue_dedup")
                    extend_seconds = payload.get("extend_seconds", 86400)
                    ack_window = payload.get("acknowledge_duplicate_window")
                    if not isinstance(entry_id, int) or isinstance(entry_id, bool):
                        raise DomainValidationError(
                            "validation_failed", "entry_id must be an integer"
                        )
                    target_id = str(entry_id)
                    result = repair_registry_entry(
                        session,
                        queue_name=queue_name,
                        actor_id=context.principal.principal_id,
                        request_id=context.request_id,
                        ack=ack,
                        registry=str(registry),
                        entry_id=entry_id,
                        extend_seconds=int(extend_seconds),
                        acknowledge_duplicate_window=ack_window is True,
                    )
                    session.commit()
                    record_break_glass_success(
                        kernel_metrics,
                        operation=context.operation.value,
                        result=result.outcome,
                        queue=queue_name if isinstance(queue_name, str) else None,
                    )
                    emit_break_glass_correlation(
                        logger,
                        operation=context.operation.value,
                        request_id=context.request_id,
                        trace_id=context.request_id,
                        actor_id=context.principal.principal_id,
                        queue=queue_name,
                        incident_ref_hash=incident_hash,
                        target_id=target_id,
                        result=result.outcome,
                        code=None,
                        extras={
                            "reason": ack.reason,
                            "repair_value": "secret-hash",
                            "registry_value": "raw",
                        },
                    )
                    await send_json(
                        send,
                        status=200,
                        payload={
                            "operation": result.operation,
                            "queue": result.queue,
                            "target_id": result.target_id,
                            "outcome": result.outcome,
                        },
                        extra_headers={"X-Request-ID": context.request_id},
                    )
                    return

                if context.operation is Operation.FORCE_DELIVERY_RECLAIM:
                    if not isinstance(queue_name, str) or not queue_name:
                        raise DomainValidationError(
                            "validation_failed", "queue_name required"
                        )
                    raw_event = context.path_params.get("event_id") or payload.get(
                        "event_id"
                    )
                    if not isinstance(raw_event, str):
                        raise DomainValidationError(
                            "validation_failed", "event_id required"
                        )
                    event_id = UUID(raw_event)
                    target_id = str(event_id)
                    result = force_delivery_reclaim(
                        session,
                        queue_name=queue_name,
                        event_id=event_id,
                        actor_id=context.principal.principal_id,
                        request_id=context.request_id,
                        ack=ack,
                    )
                    session.commit()
                    record_break_glass_success(
                        kernel_metrics,
                        operation=context.operation.value,
                        result=result.outcome,
                        queue=queue_name,
                    )
                    emit_break_glass_correlation(
                        logger,
                        operation=context.operation.value,
                        request_id=context.request_id,
                        trace_id=context.request_id,
                        actor_id=context.principal.principal_id,
                        queue=queue_name,
                        incident_ref_hash=incident_hash,
                        target_id=target_id,
                        result=result.outcome,
                        code=None,
                        generation=result.generation,
                        extras={
                            "reason": ack.reason,
                            "incident_reference": ack.incident_reference,
                            "claim_token": "must-not-leak",
                            "payload": payload,
                        },
                    )
                    await send_json(
                        send,
                        status=200,
                        payload={
                            "operation": result.operation,
                            "queue": result.queue,
                            "target_id": result.target_id,
                            "outcome": result.outcome,
                            "generation": result.generation,
                        },
                        extra_headers={"X-Request-ID": context.request_id},
                    )
                    return

                if context.operation is Operation.FORCE_DELIVERY_DEAD_LETTER:
                    if not isinstance(queue_name, str) or not queue_name:
                        raise DomainValidationError(
                            "validation_failed", "queue_name required"
                        )
                    raw_event = context.path_params.get("event_id") or payload.get(
                        "event_id"
                    )
                    if not isinstance(raw_event, str):
                        raise DomainValidationError(
                            "validation_failed", "event_id required"
                        )
                    event_id = UUID(raw_event)
                    target_id = str(event_id)
                    failure_code = payload.get(
                        "failure_code", "break_glass_force_dead_letter"
                    )
                    if not isinstance(failure_code, str):
                        raise DomainValidationError(
                            "validation_failed", "failure_code must be a string"
                        )
                    result = force_delivery_dead_letter(
                        session,
                        queue_name=queue_name,
                        event_id=event_id,
                        actor_id=context.principal.principal_id,
                        request_id=context.request_id,
                        ack=ack,
                        failure_code=failure_code,
                    )
                    session.commit()
                    record_break_glass_success(
                        kernel_metrics,
                        operation=context.operation.value,
                        result=result.outcome,
                        queue=queue_name,
                    )
                    emit_break_glass_correlation(
                        logger,
                        operation=context.operation.value,
                        request_id=context.request_id,
                        trace_id=context.request_id,
                        actor_id=context.principal.principal_id,
                        queue=queue_name,
                        incident_ref_hash=incident_hash,
                        target_id=target_id,
                        result=result.outcome,
                        code=None,
                        generation=result.generation,
                        extras={
                            "reason": ack.reason,
                            "incident_reference": ack.incident_reference,
                            "claim_token": "must-not-leak",
                            "payload": payload,
                        },
                    )
                    await send_json(
                        send,
                        status=200,
                        payload={
                            "operation": result.operation,
                            "queue": result.queue,
                            "target_id": result.target_id,
                            "outcome": result.outcome,
                            "generation": result.generation,
                        },
                        extra_headers={"X-Request-ID": context.request_id},
                    )
                    return
            except Exception:
                session.rollback()
                raise
            finally:
                session.close()
        except DomainValidationError as exc:
            emit_break_glass_correlation(
                logger,
                operation=context.operation.value,
                request_id=context.request_id,
                trace_id=context.request_id,
                actor_id=context.principal.principal_id,
                queue=queue_name,
                incident_ref_hash=incident_hash,
                target_id=target_id,
                result="denied" if exc.code == "permission_denied" else "error",
                code=exc.code,
                extras={"reason": "x", "payload": body[:32], "claim_token": "y"},
            )
            await _send_domain_error(
                send,
                request_id=context.request_id,
                code=exc.code,
                message=exc.message,
            )
            return
        except Exception:
            emit_break_glass_correlation(
                logger,
                operation=context.operation.value,
                request_id=context.request_id,
                trace_id=context.request_id,
                actor_id=context.principal.principal_id,
                queue=queue_name,
                incident_ref_hash=incident_hash,
                target_id=target_id,
                result="internal_error",
                code="internal_error",
            )
            await _send_domain_error(
                send,
                request_id=context.request_id,
                code="internal_error",
                message="break-glass operation failed",
            )

    return handler


def hashlib_sha16(value: str) -> str:
    import hashlib

    return hashlib.sha256(value.encode("utf-8")).hexdigest()[:16]
