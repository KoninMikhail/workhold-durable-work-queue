"""Public worker HTTP claim and heartbeat adapters.

- ``POST /v1/claims`` (operationId ``claimTasks``)
- ``POST /v1/claims/{claim_id}:heartbeat`` (operationId ``heartbeatClaim``)
"""

from __future__ import annotations

import json
import logging
import re
import uuid
from collections.abc import Awaitable, Callable, Mapping, MutableMapping
from datetime import datetime, timezone
from typing import Any, Final
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.orm import Session, sessionmaker

from queue_service.api.security import (
    RequestContext,
    send_json,
    send_protocol_error,
)
from queue_service.application.claim_long_poll import (
    ClaimAttemptBatch,
    ClaimLongPollService,
    ClaimWaitAborted,
)
from queue_service.application.claim_service import ClaimService
from queue_service.application.lease_service import LeaseService
from queue_service.domain.queue_control import DomainValidationError
from queue_service.infrastructure.postgres.claim_repository import ClaimPersistenceResult
from queue_service.infrastructure.postgres.lease_repository import HeartbeatPersistenceResult
from queue_service.intake.admission import DEFAULT_REQUEST_MAX_BYTES
from queue_service.intake.contracts import IntakeValidationError
from queue_service.security.authorization import (
    AuthorizationDenied,
    Authorizer,
    Operation,
)
from queue_service.security.redaction import sanitize_for_diagnostics
from queue_service.settings import CLAIM_MAX_WAIT_SECONDS_DEFAULT
from queue_service.storage.models import Queue

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

_CLAIM_BODY_KEYS: Final[frozenset[str]] = frozenset(
    {"queues", "max_tasks", "lease_seconds", "wait_seconds", "worker_id"}
)
_HEARTBEAT_BODY_KEYS: Final[frozenset[str]] = frozenset({"generation", "lease_seconds"})
_QUEUE_NAME_RE: Final[re.Pattern[str]] = re.compile(r"^[a-z0-9][a-z0-9._-]*$")
_QUEUE_NAME_MAX: Final[int] = 128
_WORKER_ID_MAX: Final[int] = 128
_LEASE_MIN: Final[int] = 1
_LEASE_MAX: Final[int] = 3600
_CLAIM_TOKEN_HEADER: Final[str] = "x-queue-claim-token"
_QUEUE_STATE_BY_CODE: Final[Mapping[int, str]] = {
    1: "active",
    2: "paused",
    3: "draining",
}


def _format_dt(value: datetime) -> str:
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _recommended_heartbeat_seconds(lease_seconds: int) -> int:
    return max(1, lease_seconds // 3)


def _diagnostic_worker_id(*, principal_id: str, replica_id: str) -> str:
    composed = f"{principal_id}/{replica_id}"
    if not (1 <= len(composed) <= _WORKER_ID_MAX):
        raise IntakeValidationError(
            "validation_failed",
            "composed worker_id exceeds 128 characters",
            details={"limit": _WORKER_ID_MAX, "observed": len(composed)},
        )
    return composed


def _parse_claim_json(body: bytes) -> dict[str, Any]:
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


def _validate_claim_request(
    parsed: Mapping[str, Any],
    *,
    max_wait_seconds: int,
) -> tuple[list[str], int, int, str]:
    unknown = sorted(set(parsed) - _CLAIM_BODY_KEYS)
    if unknown:
        raise IntakeValidationError(
            "validation_failed",
            "request body contains unsupported fields",
            details={"rejected_fields": unknown},
        )
    for key in ("queues", "max_tasks", "lease_seconds", "wait_seconds", "worker_id"):
        if key not in parsed:
            raise IntakeValidationError("validation_failed", f"{key} is required")

    queues_raw = parsed["queues"]
    if not isinstance(queues_raw, list) or not queues_raw:
        raise IntakeValidationError(
            "validation_failed",
            "queues must be a non-empty array",
        )
    if len(queues_raw) > 32:
        raise IntakeValidationError(
            "validation_failed",
            "queues exceeds maximum length",
            details={"limit": 32, "observed": len(queues_raw)},
        )
    queues: list[str] = []
    seen: set[str] = set()
    for item in queues_raw:
        if not isinstance(item, str):
            raise IntakeValidationError(
                "validation_failed",
                "queues items must be strings",
            )
        if not (1 <= len(item) <= _QUEUE_NAME_MAX) or _QUEUE_NAME_RE.fullmatch(item) is None:
            raise IntakeValidationError(
                "validation_failed",
                "queue name is invalid",
                details={"limit": _QUEUE_NAME_MAX},
            )
        if item in seen:
            raise IntakeValidationError(
                "validation_failed",
                "queues must be unique",
            )
        seen.add(item)
        queues.append(item)

    max_tasks = parsed["max_tasks"]
    if type(max_tasks) is not int or isinstance(max_tasks, bool):
        raise IntakeValidationError(
            "validation_failed",
            "max_tasks must be an integer",
        )
    if max_tasks != 1:
        raise IntakeValidationError(
            "validation_failed",
            "max_tasks must be 1",
            details={"supported": 1, "observed": max_tasks},
        )

    wait_seconds = parsed["wait_seconds"]
    if type(wait_seconds) is not int or isinstance(wait_seconds, bool):
        raise IntakeValidationError(
            "validation_failed",
            "wait_seconds must be an integer",
        )
    if not (0 <= wait_seconds <= max_wait_seconds):
        raise IntakeValidationError(
            "validation_failed",
            "wait_seconds is outside the deployment hard ceiling",
            details={"min": 0, "max": max_wait_seconds, "observed": wait_seconds},
        )

    lease_seconds = parsed["lease_seconds"]
    if type(lease_seconds) is not int or isinstance(lease_seconds, bool):
        raise IntakeValidationError(
            "validation_failed",
            "lease_seconds must be an integer",
        )
    if not (_LEASE_MIN <= lease_seconds <= _LEASE_MAX):
        raise IntakeValidationError(
            "validation_failed",
            "lease_seconds is outside the deployment hard ceiling",
            details={"min": _LEASE_MIN, "max": _LEASE_MAX, "observed": lease_seconds},
        )

    replica_id = parsed["worker_id"]
    if not isinstance(replica_id, str) or not (1 <= len(replica_id) <= _WORKER_ID_MAX):
        raise IntakeValidationError(
            "validation_failed",
            "worker_id must be a non-empty string up to 128 characters",
            details={"limit": _WORKER_ID_MAX},
        )

    return queues, lease_seconds, wait_seconds, replica_id


def _map_claimed_task(result: ClaimPersistenceResult) -> dict[str, Any]:
    assert result.task_id is not None
    assert result.claim_id is not None
    assert result.claim_token is not None
    assert result.generation is not None
    assert result.claimed_at is not None
    assert result.lease_expires_at is not None
    assert result.worker_id is not None
    assert result.producer_id is not None
    assert result.priority is not None
    assert result.available_at is not None
    assert result.retry_policy_version is not None
    assert result.created_at is not None
    assert result.queue_name is not None

    return {
        "task": {
            "task_id": str(result.task_id),
            "queue_name": result.queue_name,
            "producer_id": result.producer_id,
            "state": "leased",
            "priority": int(result.priority),
            "available_at": _format_dt(result.available_at),
            "retry_policy_version": int(result.retry_policy_version),
            "created_at": _format_dt(result.created_at),
            "payload": result.payload,
            "spawned_task_ids": [],
            "delivery_event_ids": [],
        },
        "claim": {
            "claim_id": str(result.claim_id),
            "claim_token": str(result.claim_token),
            "generation": int(result.generation),
            "claimed_at": _format_dt(result.claimed_at),
            "lease_expires_at": _format_dt(result.lease_expires_at),
            "worker_id": result.worker_id,
            "cancel_requested": bool(result.cancel_requested),
        },
    }


def _read_queue_state(session_factory: sessionmaker[Session], queue_name: str) -> str:
    session = session_factory()
    try:
        queue = session.execute(
            select(Queue).where(Queue.name == queue_name)
        ).scalar_one_or_none()
        if queue is None:
            raise IntakeValidationError("validation_failed", "queue not found")
        state = _QUEUE_STATE_BY_CODE.get(int(queue.state_code))
        if state is None:
            raise IntakeValidationError(
                "internal_error",
                "queue has an unknown state_code",
            )
        return state
    finally:
        session.close()


def build_claim_handler(
    *,
    claim_service: ClaimService,
    session_factory: sessionmaker[Session],
    max_request_bytes: int = DEFAULT_REQUEST_MAX_BYTES,
    max_wait_seconds: int = CLAIM_MAX_WAIT_SECONDS_DEFAULT,
    long_poll_service: ClaimLongPollService | None = None,
) -> Handler:
    """Build the authenticated worker claimTasks handler over :class:`ClaimService`.

    When ``long_poll_service`` is omitted, positive ``wait_seconds`` still validate
    against ``max_wait_seconds`` but execute as a single immediate attempt (tests
    that inject no coordinator). Production composition always injects the service.
    """

    ceiling = int(max_request_bytes)
    wait_ceiling = int(max_wait_seconds)

    async def handler(
        scope: MutableMapping[str, Any],
        _receive: Callable[[], Awaitable[dict[str, Any]]],
        send: Callable[[dict[str, Any]], Awaitable[None]],
        context: RequestContext,
        body: bytes,
    ) -> None:
        if context.operation is not Operation.CLAIM_TASKS:
            await send_protocol_error(
                send,
                request_id=context.request_id,
                code="internal_error",
                message="unsupported application claim operation",
            )
            return

        probe = scope.get("queue_lookup_probe")
        if probe is not None:
            probe.mark()

        _ = sanitize_for_diagnostics(
            {
                "request_id": context.request_id,
                "operation": context.operation.value,
                "principal_id": context.principal.principal_id,
                "body_bytes": len(body),
            }
        )

        if len(body) > ceiling:
            await send_protocol_error(
                send,
                request_id=context.request_id,
                code="payload_too_large",
                message="request body exceeds hard byte ceiling",
                retryable=False,
                details={
                    "limit_bytes": ceiling,
                    "observed_bytes": len(body),
                },
            )
            return

        try:
            parsed = _parse_claim_json(body)
            queues, lease_seconds, wait_seconds, replica_id = _validate_claim_request(
                parsed,
                max_wait_seconds=wait_ceiling,
            )
            worker_id = _diagnostic_worker_id(
                principal_id=context.principal.principal_id,
                replica_id=replica_id,
            )

            def _one_attempt() -> ClaimAttemptBatch:
                tasks: list[dict[str, Any]] = []
                queue_states: dict[str, str] = {}
                server_time: datetime | None = None
                for queue_name in queues:
                    if tasks:
                        queue_states[queue_name] = _read_queue_state(
                            session_factory, queue_name
                        )
                        continue
                    result = claim_service.claim(
                        queue_name=queue_name,
                        worker_id=worker_id,
                        lease_seconds=lease_seconds,
                    )
                    if result.queue_state is not None:
                        queue_states[queue_name] = result.queue_state
                    if result.server_time is not None:
                        server_time = result.server_time
                    if not result.empty:
                        tasks.append(_map_claimed_task(result))
                return ClaimAttemptBatch(
                    tasks=tasks,
                    queue_states=queue_states,
                    server_time=server_time,
                )

            cancel_probe = scope.get("queue_request_cancelled")
            stop_probe = scope.get("queue_lifecycle_stopping")

            def _is_cancelled() -> bool:
                if callable(cancel_probe):
                    return bool(cancel_probe())
                return False

            def _is_shutdown() -> bool:
                if callable(stop_probe):
                    return bool(stop_probe())
                return False

            if long_poll_service is not None:
                batch = long_poll_service.run(
                    queues=queues,
                    wait_seconds=wait_seconds,
                    attempt=_one_attempt,
                    is_cancelled=_is_cancelled,
                    is_shutdown=_is_shutdown,
                )
            else:
                if _is_shutdown():
                    raise ClaimWaitAborted("shutdown")
                if _is_cancelled():
                    raise ClaimWaitAborted("cancelled")
                batch = _one_attempt()

            server_time = batch.server_time
            if server_time is None:
                server_time = datetime.now(tz=timezone.utc)

            response = {
                "tasks": batch.tasks,
                "server_time": _format_dt(server_time),
                "recommended_heartbeat_seconds": _recommended_heartbeat_seconds(
                    lease_seconds
                ),
                "queue_states": batch.queue_states,
            }
        except ClaimWaitAborted as exc:
            if exc.reason == "shutdown":
                await send_protocol_error(
                    send,
                    request_id=context.request_id,
                    code="not_accepting",
                    message="process is shutting down",
                    retryable=True,
                )
                return
            # Client disconnect: do not perform another checkout or force a body.
            return
        except IntakeValidationError as exc:
            logger.info(
                "claim_rejected %s",
                sanitize_for_diagnostics(
                    {
                        "request_id": context.request_id,
                        "code": exc.code,
                        "principal_id": context.principal.principal_id,
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
        except DomainValidationError as exc:
            # claimTasks OpenAPI has no 404; unknown queue is a validation failure.
            code = (
                "validation_failed"
                if exc.code == "queue_not_found"
                else exc.code
            )
            logger.info(
                "claim_rejected %s",
                sanitize_for_diagnostics(
                    {
                        "request_id": context.request_id,
                        "code": code,
                        "principal_id": context.principal.principal_id,
                    }
                ),
            )
            await send_protocol_error(
                send,
                request_id=context.request_id,
                code=code,
                message=exc.message,
                retryable=False,
            )
            return
        except Exception:
            logger.exception(
                "claim_failed %s",
                sanitize_for_diagnostics(
                    {
                        "request_id": context.request_id,
                        "principal_id": context.principal.principal_id,
                    }
                ),
            )
            await send_protocol_error(
                send,
                request_id=context.request_id,
                code="internal_error",
                message="claim failed",
                retryable=True,
            )
            return

        await send_json(
            send,
            status=200,
            payload=response,
            extra_headers={"X-Request-ID": context.request_id},
        )

    return handler


def _parse_heartbeat_json(body: bytes) -> dict[str, Any]:
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


def _validate_heartbeat_request(parsed: Mapping[str, Any]) -> tuple[int, int]:
    unknown = sorted(set(parsed) - _HEARTBEAT_BODY_KEYS)
    if unknown:
        raise IntakeValidationError(
            "validation_failed",
            "request body contains unsupported fields",
            details={"rejected_fields": unknown},
        )
    for key in ("generation", "lease_seconds"):
        if key not in parsed:
            raise IntakeValidationError("validation_failed", f"{key} is required")

    generation = parsed["generation"]
    if type(generation) is not int or isinstance(generation, bool) or generation < 1:
        raise IntakeValidationError(
            "validation_failed",
            "generation must be an integer >= 1",
        )

    lease_seconds = parsed["lease_seconds"]
    if type(lease_seconds) is not int or isinstance(lease_seconds, bool):
        raise IntakeValidationError(
            "validation_failed",
            "lease_seconds must be an integer",
        )
    if not (_LEASE_MIN <= lease_seconds <= _LEASE_MAX):
        raise IntakeValidationError(
            "validation_failed",
            "lease_seconds is outside the deployment hard ceiling",
            details={"min": _LEASE_MIN, "max": _LEASE_MAX, "observed": lease_seconds},
        )
    return generation, lease_seconds


def _parse_claim_id(raw: str | None) -> UUID:
    if not raw:
        raise IntakeValidationError("validation_failed", "claim_id is required")
    try:
        return uuid.UUID(raw)
    except (ValueError, AttributeError, TypeError) as exc:
        raise IntakeValidationError(
            "validation_failed",
            "claim_id must be a UUID",
        ) from exc


def _parse_claim_token_header(headers: Mapping[str, str]) -> UUID:
    raw = headers.get(_CLAIM_TOKEN_HEADER)
    if raw is None or raw == "":
        raise IntakeValidationError(
            "validation_failed",
            "X-Queue-Claim-Token header is required",
        )
    try:
        return uuid.UUID(raw)
    except (ValueError, AttributeError, TypeError) as exc:
        raise IntakeValidationError(
            "validation_failed",
            "X-Queue-Claim-Token must be a UUID",
        ) from exc


def _header_map_from_scope(scope: Mapping[str, Any]) -> dict[str, str]:
    out: dict[str, str] = {}
    for raw_key, raw_val in scope.get("headers", []):
        key = raw_key.decode("latin-1").lower()
        out[key] = raw_val.decode("latin-1")
    return out


def _map_heartbeat_response(
    result: HeartbeatPersistenceResult,
) -> dict[str, Any]:
    return {
        "claim": {
            "claim_id": str(result.claim_id),
            "generation": int(result.generation),
            "claimed_at": _format_dt(result.claimed_at),
            "lease_expires_at": _format_dt(result.lease_expires_at),
            "worker_id": result.worker_id,
            "cancel_requested": bool(result.cancel_requested),
        },
        "server_time": _format_dt(result.server_time),
        "recommended_heartbeat_seconds": _recommended_heartbeat_seconds(
            result.lease_seconds
        ),
    }


def build_heartbeat_handler(
    *,
    lease_service: LeaseService,
    authorizer: Authorizer,
    max_request_bytes: int = DEFAULT_REQUEST_MAX_BYTES,
) -> Handler:
    """Build the authenticated worker heartbeatClaim handler over :class:`LeaseService`."""

    ceiling = int(max_request_bytes)

    async def handler(
        scope: MutableMapping[str, Any],
        _receive: Callable[[], Awaitable[dict[str, Any]]],
        send: Callable[[dict[str, Any]], Awaitable[None]],
        context: RequestContext,
        body: bytes,
    ) -> None:
        if context.operation is not Operation.HEARTBEAT_CLAIM:
            await send_protocol_error(
                send,
                request_id=context.request_id,
                code="internal_error",
                message="unsupported application heartbeat operation",
            )
            return

        probe = scope.get("queue_lookup_probe")
        if probe is not None:
            probe.mark()

        headers = _header_map_from_scope(scope)
        # Sanitize diagnostic view of the protected token header without logging it.
        _ = sanitize_for_diagnostics(
            {
                "request_id": context.request_id,
                "operation": context.operation.value,
                "principal_id": context.principal.principal_id,
                "claim_id": context.path_params.get("claim_id"),
                "body_bytes": len(body),
                "x-queue-claim-token": headers.get(_CLAIM_TOKEN_HEADER),
            }
        )

        if len(body) > ceiling:
            await send_protocol_error(
                send,
                request_id=context.request_id,
                code="payload_too_large",
                message="request body exceeds hard byte ceiling",
                retryable=False,
                details={
                    "limit_bytes": ceiling,
                    "observed_bytes": len(body),
                },
            )
            return

        try:
            claim_id = _parse_claim_id(context.path_params.get("claim_id"))
            claim_token = _parse_claim_token_header(headers)
            parsed = _parse_heartbeat_json(body)
            generation, lease_seconds = _validate_heartbeat_request(parsed)

            def _authorize_queue(queue_name: str) -> bool:
                decision = authorizer.authorize(
                    context.principal,
                    Operation.HEARTBEAT_CLAIM,
                    queue_name=queue_name,
                )
                return not isinstance(decision, AuthorizationDenied)

            result = lease_service.heartbeat(
                claim_id=claim_id,
                claim_token=claim_token,
                generation=generation,
                lease_seconds=lease_seconds,
                authorize_queue=_authorize_queue,
            )
            response = _map_heartbeat_response(result)
        except IntakeValidationError as exc:
            logger.info(
                "heartbeat_rejected %s",
                sanitize_for_diagnostics(
                    {
                        "request_id": context.request_id,
                        "code": exc.code,
                        "principal_id": context.principal.principal_id,
                        "claim_id": context.path_params.get("claim_id"),
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
        except DomainValidationError as exc:
            logger.info(
                "heartbeat_rejected %s",
                sanitize_for_diagnostics(
                    {
                        "request_id": context.request_id,
                        "code": exc.code,
                        "principal_id": context.principal.principal_id,
                        "claim_id": context.path_params.get("claim_id"),
                    }
                ),
            )
            await send_protocol_error(
                send,
                request_id=context.request_id,
                code=exc.code,
                message=exc.message,
                retryable=False,
            )
            return
        except Exception:
            logger.exception(
                "heartbeat_failed %s",
                sanitize_for_diagnostics(
                    {
                        "request_id": context.request_id,
                        "principal_id": context.principal.principal_id,
                        "claim_id": context.path_params.get("claim_id"),
                    }
                ),
            )
            await send_protocol_error(
                send,
                request_id=context.request_id,
                code="internal_error",
                message="heartbeat failed",
                retryable=True,
            )
            return

        await send_json(
            send,
            status=200,
            payload=response,
            extra_headers={"X-Request-ID": context.request_id},
        )

    return handler
