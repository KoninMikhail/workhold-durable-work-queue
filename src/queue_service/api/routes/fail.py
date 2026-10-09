"""Authenticated worker failClaim HTTP adapter (`POST /v1/claims/{claim_id}:fail`)."""

from __future__ import annotations

import json
import logging
import uuid
from collections.abc import Awaitable, Callable, Mapping, MutableMapping
from datetime import datetime, timezone
from typing import Any, Final
from uuid import UUID

from queue_service.api.schemas.terminal import parse_fail_command
from queue_service.api.security import (
    RequestContext,
    send_json,
    send_protocol_error,
)
from queue_service.application.worker_terminal import WorkerTerminalService
from queue_service.domain.queue_control import DomainValidationError
from queue_service.infrastructure.postgres.task_transitions import FailPersistenceResult
from queue_service.intake.admission import DEFAULT_REQUEST_MAX_BYTES
from queue_service.intake.contracts import IntakeValidationError
from queue_service.security.authorization import (
    AuthorizationDenied,
    Authorizer,
    Operation,
)
from queue_service.security.redaction import sanitize_for_diagnostics

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

_CLAIM_TOKEN_HEADER: Final[str] = "x-queue-claim-token"


def _format_dt(value: datetime) -> str:
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _parse_json_object(body: bytes) -> dict[str, Any]:
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


def _map_fail_response(result: FailPersistenceResult) -> dict[str, Any]:
    if result.state == "retry_scheduled":
        assert result.available_at is not None
        return {
            "task_id": str(result.task_id),
            "state": "retry_scheduled",
            "available_at": _format_dt(result.available_at),
            "replayed": bool(result.replayed),
        }
    assert result.state == "dead_lettered"
    assert result.terminal_at is not None
    return {
        "task_id": str(result.task_id),
        "state": "dead_lettered",
        "terminal_at": _format_dt(result.terminal_at),
        "replayed": bool(result.replayed),
    }


def build_fail_handler(
    *,
    worker_terminal_service: WorkerTerminalService,
    authorizer: Authorizer,
    max_request_bytes: int = DEFAULT_REQUEST_MAX_BYTES,
) -> Handler:
    """Build the authenticated worker failClaim handler."""

    ceiling = int(max_request_bytes)

    async def handler(
        scope: MutableMapping[str, Any],
        _receive: Callable[[], Awaitable[dict[str, Any]]],
        send: Callable[[dict[str, Any]], Awaitable[None]],
        context: RequestContext,
        body: bytes,
    ) -> None:
        if context.operation is not Operation.FAIL_CLAIM:
            await send_protocol_error(
                send,
                request_id=context.request_id,
                code="internal_error",
                message="unsupported application fail operation",
            )
            return

        probe = scope.get("queue_lookup_probe")
        if probe is not None:
            probe.mark()

        headers = _header_map_from_scope(scope)
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
            parsed = _parse_json_object(body)
            command = parse_fail_command(parsed)

            def _authorize_queue(queue_name: str) -> bool:
                decision = authorizer.authorize(
                    context.principal,
                    Operation.FAIL_CLAIM,
                    queue_name=queue_name,
                )
                return not isinstance(decision, AuthorizationDenied)

            result = worker_terminal_service.fail(
                claim_id=claim_id,
                claim_token=claim_token,
                command=command,
                authorize_queue=_authorize_queue,
            )
            response = _map_fail_response(result)
        except IntakeValidationError as exc:
            logger.info(
                "fail_rejected %s",
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
                "fail_rejected %s",
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
                "fail_failed %s",
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
                message="fail failed",
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
