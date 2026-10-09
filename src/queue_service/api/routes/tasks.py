"""Authenticated task cancel and inspection HTTP adapters."""

from __future__ import annotations

import json
import logging
import uuid
from collections.abc import Awaitable, Callable, Mapping, MutableMapping
from typing import Any, Final
from urllib.parse import parse_qs
from uuid import UUID

from queue_service.api.schemas.tasks import parse_cancel_command
from queue_service.api.security import (
    RequestContext,
    send_json,
    send_protocol_error,
)
from queue_service.application.cancellation import CancellationService
from queue_service.application.task_inspection import TaskInspectionService
from queue_service.domain.queue_control import DomainValidationError
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


def _parse_json_object_optional(body: bytes) -> dict[str, Any]:
    if not body:
        return {}
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


def _parse_task_id(raw: str | None) -> UUID:
    if not raw:
        raise IntakeValidationError("validation_failed", "task_id is required")
    try:
        return uuid.UUID(raw)
    except (ValueError, AttributeError, TypeError) as exc:
        raise IntakeValidationError(
            "validation_failed",
            "task_id must be a UUID",
        ) from exc


def _header_map_from_scope(scope: Mapping[str, Any]) -> dict[str, str]:
    out: dict[str, str] = {}
    for raw_key, raw_val in scope.get("headers", []):
        key = raw_key.decode("latin-1").lower()
        out[key] = raw_val.decode("latin-1")
    return out


def build_cancel_handler(
    *,
    cancellation_service: CancellationService,
    authorizer: Authorizer,
    max_request_bytes: int = DEFAULT_REQUEST_MAX_BYTES,
) -> Handler:
    """Build the authenticated producer cancelTask handler.

    Never treats ``X-Queue-Claim-Token`` as identity or authorization input.
    """

    ceiling = int(max_request_bytes)

    async def handler(
        scope: MutableMapping[str, Any],
        _receive: Callable[[], Awaitable[dict[str, Any]]],
        send: Callable[[dict[str, Any]], Awaitable[None]],
        context: RequestContext,
        body: bytes,
    ) -> None:
        if context.operation is not Operation.CANCEL_TASK:
            await send_protocol_error(
                send,
                request_id=context.request_id,
                code="internal_error",
                message="unsupported application cancel operation",
            )
            return

        probe = scope.get("queue_lookup_probe")
        if probe is not None:
            probe.mark()

        headers = _header_map_from_scope(scope)
        # Claim token may be present but is never accepted as cancel authority.
        _ = sanitize_for_diagnostics(
            {
                "request_id": context.request_id,
                "operation": context.operation.value,
                "principal_id": context.principal.principal_id,
                "task_id": context.path_params.get("task_id"),
                "body_bytes": len(body),
                "x-queue-claim-token-present": _CLAIM_TOKEN_HEADER in headers,
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
            task_id = _parse_task_id(context.path_params.get("task_id"))
            parsed = _parse_json_object_optional(body)
            _ = parse_cancel_command(parsed)
            producer_id = context.principal.principal_id

            def _authorize_queue(queue_name: str) -> bool:
                decision = authorizer.authorize(
                    context.principal,
                    Operation.CANCEL_TASK,
                    queue_name=queue_name,
                )
                return not isinstance(decision, AuthorizationDenied)

            result = cancellation_service.cancel(
                task_id=task_id,
                producer_id=producer_id,
                authorize_queue=_authorize_queue,
            )
            response = {"task": result.task}
        except IntakeValidationError as exc:
            logger.info(
                "cancel_rejected %s",
                sanitize_for_diagnostics(
                    {
                        "request_id": context.request_id,
                        "code": exc.code,
                        "principal_id": context.principal.principal_id,
                        "task_id": context.path_params.get("task_id"),
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
                "cancel_rejected %s",
                sanitize_for_diagnostics(
                    {
                        "request_id": context.request_id,
                        "code": exc.code,
                        "principal_id": context.principal.principal_id,
                        "task_id": context.path_params.get("task_id"),
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
                "cancel_failed %s",
                sanitize_for_diagnostics(
                    {
                        "request_id": context.request_id,
                        "principal_id": context.principal.principal_id,
                        "task_id": context.path_params.get("task_id"),
                    }
                ),
            )
            await send_protocol_error(
                send,
                request_id=context.request_id,
                code="internal_error",
                message="cancel failed",
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


def _parse_optional_limit(raw: str | None) -> int | None:
    if raw is None:
        return None
    try:
        value = int(raw)
    except (TypeError, ValueError) as exc:
        raise IntakeValidationError(
            "validation_failed",
            "limit must be an integer",
        ) from exc
    return value


def build_get_task_handler(
    *,
    inspection_service: TaskInspectionService,
    authorizer: Authorizer,
) -> Handler:
    """Build getTask inspection handler (producer/observer; claim token redacted).

    Terminal completed sources expose ordered ``spawned_task_ids`` from the
    completion_effects / complete_replay lineage projection (API-04). Descendants
    expose copied ``source_task_id`` / ``spawn_ordinal`` only — never raw registry
    rows, claim tokens, fingerprints, or business-result fields.
    """

    async def handler(
        scope: MutableMapping[str, Any],
        _receive: Callable[[], Awaitable[dict[str, Any]]],
        send: Callable[[dict[str, Any]], Awaitable[None]],
        context: RequestContext,
        body: bytes,
    ) -> None:
        if context.operation is not Operation.GET_TASK:
            await send_protocol_error(
                send,
                request_id=context.request_id,
                code="internal_error",
                message="unsupported application getTask operation",
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
                "task_id": context.path_params.get("task_id"),
                "body_bytes": len(body),
                "x-queue-claim-token-present": _CLAIM_TOKEN_HEADER in headers,
            }
        )

        try:
            task_id = _parse_task_id(context.path_params.get("task_id"))

            def _authorize_queue(queue_name: str) -> bool:
                decision = authorizer.authorize(
                    context.principal,
                    Operation.GET_TASK,
                    queue_name=queue_name,
                )
                return not isinstance(decision, AuthorizationDenied)

            response = inspection_service.get_task(
                task_id=task_id,
                principal=context.principal,
                authorize_queue=_authorize_queue,
            )
        except IntakeValidationError as exc:
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
                "get_task_failed %s",
                sanitize_for_diagnostics(
                    {
                        "request_id": context.request_id,
                        "principal_id": context.principal.principal_id,
                        "task_id": context.path_params.get("task_id"),
                    }
                ),
            )
            await send_protocol_error(
                send,
                request_id=context.request_id,
                code="internal_error",
                message="get task failed",
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


def build_list_attempts_handler(
    *,
    inspection_service: TaskInspectionService,
    authorizer: Authorizer,
) -> Handler:
    """Build listTaskAttempts handler (append-only history; claim token redacted)."""

    async def handler(
        scope: MutableMapping[str, Any],
        _receive: Callable[[], Awaitable[dict[str, Any]]],
        send: Callable[[dict[str, Any]], Awaitable[None]],
        context: RequestContext,
        body: bytes,
    ) -> None:
        if context.operation is not Operation.LIST_TASK_ATTEMPTS:
            await send_protocol_error(
                send,
                request_id=context.request_id,
                code="internal_error",
                message="unsupported application listTaskAttempts operation",
            )
            return

        probe = scope.get("queue_lookup_probe")
        if probe is not None:
            probe.mark()

        headers = _header_map_from_scope(scope)
        query = _query_map(scope)
        _ = sanitize_for_diagnostics(
            {
                "request_id": context.request_id,
                "operation": context.operation.value,
                "principal_id": context.principal.principal_id,
                "task_id": context.path_params.get("task_id"),
                "body_bytes": len(body),
                "x-queue-claim-token-present": _CLAIM_TOKEN_HEADER in headers,
                "has_cursor": "cursor" in query,
            }
        )

        try:
            task_id = _parse_task_id(context.path_params.get("task_id"))
            limit = _parse_optional_limit(query.get("limit"))
            cursor = query.get("cursor")

            def _authorize_queue(queue_name: str) -> bool:
                decision = authorizer.authorize(
                    context.principal,
                    Operation.LIST_TASK_ATTEMPTS,
                    queue_name=queue_name,
                )
                return not isinstance(decision, AuthorizationDenied)

            page = inspection_service.list_attempts(
                task_id=task_id,
                principal=context.principal,
                authorize_queue=_authorize_queue,
                cursor=cursor,
                limit=limit,
            )
            response: dict[str, Any] = {"items": page.items}
            if page.next_cursor is not None:
                response["next_cursor"] = page.next_cursor
            else:
                response["next_cursor"] = None
        except IntakeValidationError as exc:
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
                "list_attempts_failed %s",
                sanitize_for_diagnostics(
                    {
                        "request_id": context.request_id,
                        "principal_id": context.principal.principal_id,
                        "task_id": context.path_params.get("task_id"),
                    }
                ),
            )
            await send_protocol_error(
                send,
                request_id=context.request_id,
                code="internal_error",
                message="list attempts failed",
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
