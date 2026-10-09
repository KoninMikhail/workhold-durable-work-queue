"""Public producer HTTP enqueue adapter (`POST /v1/queues/{queue_name}/tasks`)."""

from __future__ import annotations

import json
import logging
from collections.abc import Awaitable, Callable, Mapping, MutableMapping
from datetime import datetime, timezone
from typing import Any
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.orm import Session, sessionmaker

from queue_service.api.security import (
    RequestContext,
    send_json,
    send_protocol_error,
)
from queue_service.intake.admission import EnqueueAdmissionLimits
from queue_service.intake.contracts import IntakeValidationError
from queue_service.intake.repository import EnqueuePersistenceResult
from queue_service.intake.service import EnqueueService
from queue_service.security.authorization import Operation
from queue_service.security.redaction import sanitize_for_diagnostics
from queue_service.storage.models import Queue, QueuePolicyVersion, TaskActive

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

_TASK_STATE_BY_CODE: Mapping[int, str] = {
    1: "delayed",
    2: "ready",
    3: "leased",
}

_ENQUEUE_BODY_KEYS = frozenset({"payload", "priority", "available_at"})


def _header_map(scope: Mapping[str, Any]) -> dict[str, str]:
    out: dict[str, str] = {}
    for raw_key, raw_val in scope.get("headers", []):
        key = raw_key.decode("latin-1").lower()
        out[key] = raw_val.decode("latin-1")
    return out


def _format_dt(value: datetime) -> str:
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _parse_available_at(raw: Any) -> datetime | None:
    if raw is None:
        return None
    if isinstance(raw, datetime):
        return raw
    if not isinstance(raw, str):
        raise IntakeValidationError(
            "validation_failed",
            "available_at must be a datetime or null",
        )
    text = raw.strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError as exc:
        raise IntakeValidationError(
            "validation_failed",
            "available_at must be a valid date-time",
        ) from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise IntakeValidationError(
            "validation_failed",
            "available_at must be timezone-aware",
            details={"field": "available_at"},
        )
    return parsed


def _parse_enqueue_json(body: bytes) -> dict[str, Any]:
    if not body:
        raise IntakeValidationError("validation_failed", "request body is required")
    try:
        parsed = json.loads(body.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise IntakeValidationError(
            "validation_failed",
            "request body must be valid JSON",
        ) from exc
    if not isinstance(parsed, dict):
        raise IntakeValidationError(
            "validation_failed",
            "request body must be an object",
        )
    return parsed


def _task_projection(
    *,
    task: TaskActive,
    queue_name: str,
    policy_version: int,
) -> dict[str, Any]:
    state = _TASK_STATE_BY_CODE.get(int(task.state_code))
    if state is None:
        raise IntakeValidationError(
            "internal_error",
            f"unknown task state_code={task.state_code}",
        )
    return {
        "task_id": str(task.task_id),
        "queue_name": queue_name,
        "producer_id": task.producer_id,
        "state": state,
        "priority": int(task.priority),
        "available_at": _format_dt(task.available_at),
        "retry_policy_version": int(policy_version),
        "created_at": _format_dt(task.created_at),
        "spawned_task_ids": [],
        "delivery_event_ids": [],
    }


def _load_task_response(
    session: Session,
    *,
    task_id: UUID,
) -> dict[str, Any]:
    row = session.execute(
        select(TaskActive, Queue.name, QueuePolicyVersion.version)
        .join(Queue, Queue.id == TaskActive.queue_id)
        .join(
            QueuePolicyVersion,
            QueuePolicyVersion.id == TaskActive.retry_policy_version_id,
        )
        .where(TaskActive.task_id == task_id)
    ).one_or_none()
    if row is None:
        raise IntakeValidationError(
            "internal_error",
            "committed enqueue task is not readable",
        )
    task, queue_name, policy_version = row
    return _task_projection(
        task=task,
        queue_name=str(queue_name),
        policy_version=int(policy_version),
    )


def build_enqueue_handler(
    *,
    enqueue_service: EnqueueService,
    session_factory: sessionmaker[Session],
    admission_limits: EnqueueAdmissionLimits | None = None,
) -> Handler:
    """Build the authenticated producer enqueue handler over :class:`EnqueueService`."""

    limits = admission_limits or EnqueueAdmissionLimits()
    max_request_bytes = int(limits.max_request_bytes)

    async def handler(
        scope: MutableMapping[str, Any],
        _receive: Callable[[], Awaitable[dict[str, Any]]],
        send: Callable[[dict[str, Any]], Awaitable[None]],
        context: RequestContext,
        body: bytes,
    ) -> None:
        if context.operation is not Operation.ENQUEUE_TASK:
            await send_protocol_error(
                send,
                request_id=context.request_id,
                code="internal_error",
                message="unsupported application enqueue operation",
            )
            return

        probe = scope.get("queue_lookup_probe")
        if probe is not None:
            probe.mark()

        headers = _header_map(scope)
        idempotency_key = headers.get("idempotency-key")
        queue_name = context.path_params.get("queue_name") or context.authorization.queue_name
        producer_id = context.authorization.producer_id or context.principal.principal_id

        _ = sanitize_for_diagnostics(
            {
                "request_id": context.request_id,
                "operation": context.operation.value,
                "principal_id": context.principal.principal_id,
                "queue_name": queue_name,
                "body_bytes": len(body),
            }
        )

        # Reject oversize bodies before JSON expansion (admission-control / OPS-04).
        if len(body) > max_request_bytes:
            await send_protocol_error(
                send,
                request_id=context.request_id,
                code="payload_too_large",
                message="request body exceeds hard byte ceiling",
                retryable=False,
                details={
                    "limit_bytes": max_request_bytes,
                    "observed_bytes": len(body),
                },
            )
            return

        try:
            if not queue_name:
                raise IntakeValidationError(
                    "validation_failed",
                    "queue_name is required from runtime context",
                )
            if not producer_id:
                raise IntakeValidationError(
                    "validation_failed",
                    "producer_id is required from authenticated context",
                )
            parsed = _parse_enqueue_json(body)
            unknown = sorted(set(parsed) - _ENQUEUE_BODY_KEYS)
            if unknown:
                raise IntakeValidationError(
                    "validation_failed",
                    "request body contains unsupported fields",
                    details={"rejected_fields": unknown},
                )
            if "payload" not in parsed:
                raise IntakeValidationError("validation_failed", "payload is required")
            if "priority" not in parsed:
                raise IntakeValidationError("validation_failed", "priority is required")
            available_at = _parse_available_at(parsed.get("available_at"))
            service_body = {
                "payload": parsed["payload"],
                "priority": parsed["priority"],
                "available_at": available_at,
            }
            result = enqueue_service.enqueue(
                producer_id=producer_id,
                queue_name=queue_name,
                idempotency_key=idempotency_key,
                body=service_body,
                body_bytes=body,
            )
            task_body = _read_committed_task(session_factory, result)
        except IntakeValidationError as exc:
            logger.info(
                "enqueue_rejected %s",
                sanitize_for_diagnostics(
                    {
                        "request_id": context.request_id,
                        "code": exc.code,
                        "queue_name": queue_name,
                    }
                ),
            )
            await send_protocol_error(
                send,
                request_id=context.request_id,
                code=exc.code,
                message=exc.message,
                retryable=exc.retryable,
                retry_after_ms=exc.retry_after_ms,
                details=exc.details,
            )
            return
        except Exception:
            logger.exception(
                "enqueue_failed %s",
                sanitize_for_diagnostics(
                    {
                        "request_id": context.request_id,
                        "queue_name": queue_name,
                    }
                ),
            )
            await send_protocol_error(
                send,
                request_id=context.request_id,
                code="internal_error",
                message="enqueue failed",
                retryable=True,
            )
            return

        payload = {"task": task_body, "replayed": result.replayed}
        response_headers = {"X-Request-ID": context.request_id}
        if result.replayed:
            status = 200
        else:
            status = 201
            response_headers["Location"] = f"/v1/tasks/{result.task_id}"
        await send_json(
            send,
            status=status,
            payload=payload,
            extra_headers=response_headers,
        )

    return handler


def _read_committed_task(
    session_factory: sessionmaker[Session],
    result: EnqueuePersistenceResult,
) -> dict[str, Any]:
    session = session_factory()
    try:
        return _load_task_response(session, task_id=result.task_id)
    finally:
        session.close()
