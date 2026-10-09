"""Public producer HTTP resolveSubmission adapter.

``POST /v1/queues/{queue_name}/submissions:resolve`` recovers a retained
enqueue by authenticated producer identity + named queue + idempotency key
via the ``enqueue_dedup`` correctness registry. Never searches payload fields.
"""

from __future__ import annotations

import json
import logging
from collections.abc import Awaitable, Callable, MutableMapping
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session, sessionmaker

from queue_service.api.security import (
    RequestContext,
    send_json,
    send_protocol_error,
)
from queue_service.api.v1.enqueue import _format_dt, _load_task_response
from queue_service.intake.contracts import IntakeValidationError
from queue_service.intake.repository import EnqueueRepository
from queue_service.security.authorization import Operation
from queue_service.security.payload_policy import PayloadHandlingPolicy
from queue_service.security.redaction import sanitize_for_diagnostics
from queue_service.storage.models import EnqueueDedup, Queue

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

_RESOLVE_BODY_KEYS = frozenset({"idempotency_key"})
_IDEMPOTENCY_KEY_MAX_LEN = 256
_CODE_TASK_NOT_FOUND = "task_not_found"
_CODE_QUEUE_NOT_FOUND = "queue_not_found"
_CODE_VALIDATION_FAILED = "validation_failed"


def _parse_resolve_json(body: bytes) -> dict[str, Any]:
    if not body:
        raise IntakeValidationError(_CODE_VALIDATION_FAILED, "request body is required")
    try:
        parsed = json.loads(body.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise IntakeValidationError(
            _CODE_VALIDATION_FAILED,
            "request body must be valid JSON",
        ) from exc
    if not isinstance(parsed, dict):
        raise IntakeValidationError(
            _CODE_VALIDATION_FAILED,
            "request body must be an object",
        )
    return parsed


def _normalize_idempotency_key(raw: Any) -> str:
    if not isinstance(raw, str):
        raise IntakeValidationError(
            _CODE_VALIDATION_FAILED,
            "idempotency_key is required",
        )
    if not (1 <= len(raw) <= _IDEMPOTENCY_KEY_MAX_LEN) or raw.strip() == "":
        raise IntakeValidationError(
            _CODE_VALIDATION_FAILED,
            "idempotency_key must be 1..256 characters",
        )
    return raw


def _probe_scoped_dedup(
    session: Session,
    *,
    producer_id: str,
    queue_id: int,
    key_hash: bytes,
) -> EnqueueDedup | None:
    """Indexed correctness-registry lookup (producer + queue + key_hash)."""

    return session.execute(
        select(EnqueueDedup).where(
            EnqueueDedup.producer_id == producer_id,
            EnqueueDedup.queue_id == queue_id,
            EnqueueDedup.key_hash == key_hash,
        )
    ).scalar_one_or_none()


def _resolve_submission(
    session: Session,
    *,
    producer_id: str,
    queue_name: str,
    idempotency_key: str,
    now: datetime,
) -> dict[str, Any]:
    queue = session.execute(
        select(Queue).where(Queue.name == queue_name)
    ).scalar_one_or_none()
    if queue is None:
        raise IntakeValidationError(_CODE_QUEUE_NOT_FOUND, "queue not found")

    key_hash = EnqueueRepository.key_hash_for(idempotency_key)
    dedup = _probe_scoped_dedup(
        session,
        producer_id=producer_id,
        queue_id=int(queue.id),
        key_hash=key_hash,
    )
    if dedup is None:
        raise IntakeValidationError(_CODE_TASK_NOT_FOUND, "submission not found")

    expires_at = dedup.expires_at
    if expires_at.tzinfo is None:
        expires_at = expires_at.replace(tzinfo=timezone.utc)
    if now.tzinfo is None:
        raise IntakeValidationError(
            _CODE_VALIDATION_FAILED,
            "queue-store now must be timezone-aware",
        )
    if now >= expires_at.astimezone(timezone.utc):
        raise IntakeValidationError(_CODE_TASK_NOT_FOUND, "submission not found")

    task_body = _load_task_response(session, task_id=dedup.task_id)
    # Producer-visible projection: no opaque payload, no claim credentials.
    task_body.pop("payload", None)
    task_body.pop("current_claim", None)
    return {
        "task": task_body,
        "dedup_expires_at": _format_dt(expires_at),
    }


def build_resolve_submission_handler(
    *,
    session_factory: sessionmaker[Session],
    payload_policy: PayloadHandlingPolicy | None = None,
) -> Handler:
    """Build authenticated producer resolveSubmission over ``enqueue_dedup``."""

    policy = payload_policy or PayloadHandlingPolicy()

    async def handler(
        scope: MutableMapping[str, Any],
        _receive: Callable[[], Awaitable[dict[str, Any]]],
        send: Callable[[dict[str, Any]], Awaitable[None]],
        context: RequestContext,
        body: bytes,
    ) -> None:
        if context.operation is not Operation.RESOLVE_SUBMISSION:
            await send_protocol_error(
                send,
                request_id=context.request_id,
                code="internal_error",
                message="unsupported application resolveSubmission operation",
            )
            return

        probe = scope.get("queue_lookup_probe")
        if probe is not None:
            probe.mark()

        queue_name = context.path_params.get("queue_name") or context.authorization.queue_name
        producer_id = context.authorization.producer_id or context.principal.principal_id

        # Metadata-only default: never project opaque body into diagnostics.
        _ = policy.inspect(
            {},
            payload_bytes=len(body),
            include_payload=False,
        )
        _ = sanitize_for_diagnostics(
            {
                "request_id": context.request_id,
                "operation": context.operation.value,
                "principal_id": context.principal.principal_id,
                "queue_name": queue_name,
                "body_bytes": len(body),
            }
        )

        session = session_factory()
        try:
            if not queue_name:
                raise IntakeValidationError(
                    _CODE_VALIDATION_FAILED,
                    "queue_name is required from runtime context",
                )
            if not producer_id:
                raise IntakeValidationError(
                    _CODE_VALIDATION_FAILED,
                    "producer_id is required from authenticated context",
                )
            parsed = _parse_resolve_json(body)
            unknown = sorted(set(parsed) - _RESOLVE_BODY_KEYS)
            if unknown:
                raise IntakeValidationError(
                    _CODE_VALIDATION_FAILED,
                    "request body contains unsupported fields",
                    details={"rejected_fields": unknown},
                )
            if "idempotency_key" not in parsed:
                raise IntakeValidationError(
                    _CODE_VALIDATION_FAILED,
                    "idempotency_key is required",
                )
            idempotency_key = _normalize_idempotency_key(parsed["idempotency_key"])
            now = datetime.now(tz=timezone.utc)
            payload = _resolve_submission(
                session,
                producer_id=producer_id,
                queue_name=queue_name,
                idempotency_key=idempotency_key,
                now=now,
            )
        except IntakeValidationError as exc:
            logger.info(
                "resolve_submission_rejected %s",
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
                "resolve_submission_failed %s",
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
                message="resolveSubmission failed",
                retryable=True,
            )
            return
        finally:
            session.close()

        await send_json(
            send,
            status=200,
            payload=payload,
            extra_headers={"X-Request-ID": context.request_id},
        )

    return handler
