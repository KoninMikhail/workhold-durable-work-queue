"""Private admin named-queue create, read, and policy mutation handlers."""

from __future__ import annotations

import json
import logging
from collections.abc import Awaitable, Callable, Mapping, MutableMapping
from datetime import datetime, timedelta, timezone
from typing import Any
from urllib.parse import parse_qs

from sqlalchemy.orm import Session, sessionmaker

from queue_service.api.security import RequestContext, error_envelope, send_json
from queue_service.domain.queue_control import (
    RETRY_DELAY_SECONDS_ABSOLUTE_MAX,
    ActivatePolicyMutation,
    AdminRequestMetadata,
    CreatePolicyMutation,
    CreateQueueMutation,
    DomainValidationError,
    QueueState,
    SetQueueStateMutation,
    parse_config_version,
    parse_policy_version,
    parse_queue_state,
    validate_retry_policy_draft,
)
from queue_service.infrastructure.postgres.queue_control_repository import (
    QueueConfiguration,
    QueueControlRepository,
)
from queue_service.operations.routine import (
    emit_routine_admin_correlation,
    enrich_queue_response,
    read_drain_progress,
)
from queue_service.security.authorization import Operation

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

_CREATE_TOP_LEVEL_KEYS = frozenset({"name", "initial_policy"})
_POLICY_KEYS = frozenset(
    {"enabled", "max_attempts", "backoff_strategy", "retry_delay_seconds"}
)
_ACTIVATE_KEYS = frozenset({"expected_config_version"})
_SET_STATE_KEYS = frozenset({"expected_config_version", "state"})
_ADMIN_SET_STATE_ALLOWED = frozenset(
    {QueueState.ACTIVE, QueueState.PAUSED, QueueState.DRAINING}
)
# ADR017 / Capabilities default admin replay TTL (30 days).
_ADMIN_REPLAY_TTL_SECONDS = 2_592_000

_ERROR_HTTP: Mapping[str, int] = {
    "validation_failed": 400,
    "idempotency_key_required": 400,
    "payload_too_large": 413,
    "idempotency_conflict": 409,
    "queue_not_found": 404,
    "config_version_conflict": 412,
    "permission_denied": 403,
    "unauthenticated": 401,
    "resource_exhausted": 429,
    "dependency_unavailable": 503,
    "internal_error": 500,
}

_RETRYABLE_CODES = frozenset(
    {
        "queue_draining",
        "config_version_conflict",
        "resource_exhausted",
        "dependency_unavailable",
        "internal_error",
    }
)


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


def queue_configuration_to_response(
    config: QueueConfiguration,
    *,
    drain_progress: Any | None = None,
) -> dict[str, Any]:
    """Project repository configuration into the OpenAPI ``Queue`` schema."""
    policy = config.active_policy
    base = {
        "queue_id": str(config.queue_id),
        "name": config.name,
        "state": config.state.value,
        "config_version": config.config_version.value,
        "active_policy": {
            "version": policy.version.value,
            "enabled": policy.policy.enabled,
            "max_attempts": policy.policy.max_attempts,
            "backoff_strategy": policy.policy.backoff_strategy.value,
            "retry_delay_seconds": policy.policy.retry_delay_seconds,
            "created_at": _format_dt(policy.created_at),
        },
        "created_at": _format_dt(config.created_at),
        "updated_at": _format_dt(config.updated_at),
    }
    if drain_progress is not None:
        return enrich_queue_response(base, drain_progress)
    return base


def _reject_unknown_keys(
    payload: Mapping[str, Any],
    allowed: frozenset[str],
    *,
    path: str,
) -> None:
    unknown = sorted(set(payload) - allowed)
    if unknown:
        raise DomainValidationError(
            "validation_failed",
            f"{path}: unknown property '{unknown[0]}'",
        )


def _parse_create_body(
    body: bytes,
    *,
    deployment_retry_delay_ceiling_seconds: int,
) -> tuple[str, Any]:
    if not body:
        raise DomainValidationError("validation_failed", "request body is required")
    try:
        parsed = json.loads(body.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise DomainValidationError(
            "validation_failed",
            "request body must be valid JSON",
        ) from exc
    if not isinstance(parsed, dict):
        raise DomainValidationError("validation_failed", "request body must be an object")

    _reject_unknown_keys(parsed, _CREATE_TOP_LEVEL_KEYS, path="$")
    name = parsed.get("name")
    initial = parsed.get("initial_policy")
    if not isinstance(initial, dict):
        raise DomainValidationError(
            "validation_failed",
            "$.initial_policy must be an object",
        )
    _reject_unknown_keys(initial, _POLICY_KEYS, path="$.initial_policy")
    for required in _POLICY_KEYS:
        if required not in initial:
            raise DomainValidationError(
                "validation_failed",
                f"$.initial_policy missing required property '{required}'",
            )

    policy = validate_retry_policy_draft(
        enabled=initial["enabled"],
        max_attempts=initial["max_attempts"],
        backoff_strategy=initial["backoff_strategy"],
        retry_delay_seconds=initial["retry_delay_seconds"],
        deployment_retry_delay_ceiling_seconds=deployment_retry_delay_ceiling_seconds,
    )
    if not isinstance(name, str):
        raise DomainValidationError("validation_failed", "$.name must be a string")
    return name, policy


def _parse_policy_body(
    body: bytes,
    *,
    deployment_retry_delay_ceiling_seconds: int,
) -> Any:
    if not body:
        raise DomainValidationError("validation_failed", "request body is required")
    try:
        parsed = json.loads(body.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise DomainValidationError(
            "validation_failed",
            "request body must be valid JSON",
        ) from exc
    if not isinstance(parsed, dict):
        raise DomainValidationError("validation_failed", "request body must be an object")
    _reject_unknown_keys(parsed, _POLICY_KEYS, path="$")
    for required in _POLICY_KEYS:
        if required not in parsed:
            raise DomainValidationError(
                "validation_failed",
                f"missing required property '{required}'",
            )
    return validate_retry_policy_draft(
        enabled=parsed["enabled"],
        max_attempts=parsed["max_attempts"],
        backoff_strategy=parsed["backoff_strategy"],
        retry_delay_seconds=parsed["retry_delay_seconds"],
        deployment_retry_delay_ceiling_seconds=deployment_retry_delay_ceiling_seconds,
    )


def _parse_activate_body(body: bytes) -> Any:
    if not body:
        raise DomainValidationError("validation_failed", "request body is required")
    try:
        parsed = json.loads(body.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise DomainValidationError(
            "validation_failed",
            "request body must be valid JSON",
        ) from exc
    if not isinstance(parsed, dict):
        raise DomainValidationError("validation_failed", "request body must be an object")
    _reject_unknown_keys(parsed, _ACTIVATE_KEYS, path="$")
    if "expected_config_version" not in parsed:
        raise DomainValidationError(
            "validation_failed",
            "missing required property 'expected_config_version'",
        )
    return parse_config_version(parsed["expected_config_version"])



def _parse_set_state_body(body: bytes) -> tuple[object, QueueState]:
    if not body:
        raise DomainValidationError("validation_failed", "request body is required")
    try:
        parsed = json.loads(body.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise DomainValidationError(
            "validation_failed",
            "request body must be valid JSON",
        ) from exc
    if not isinstance(parsed, dict):
        raise DomainValidationError("validation_failed", "request body must be an object")
    _reject_unknown_keys(parsed, _SET_STATE_KEYS, path="$")
    if "expected_config_version" not in parsed:
        raise DomainValidationError(
            "validation_failed",
            "missing required property 'expected_config_version'",
        )
    if "state" not in parsed:
        raise DomainValidationError(
            "validation_failed",
            "missing required property 'state'",
        )
    expected = parse_config_version(parsed["expected_config_version"])
    state = parse_queue_state(parsed["state"])
    if state not in _ADMIN_SET_STATE_ALLOWED:
        raise DomainValidationError(
            "validation_failed",
            "admin setQueueState accepts only active, paused, or draining",
        )
    return expected, state


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
            retryable=code in _RETRYABLE_CODES,
            request_id=request_id,
        ),
        extra_headers={"X-Request-ID": request_id},
    )


def build_admin_queues_handler(
    *,
    session_factory: sessionmaker[Session],
    repository: QueueControlRepository,
    deployment_retry_delay_ceiling_seconds: int = RETRY_DELAY_SECONDS_ABSOLUTE_MAX,
    admin_replay_ttl_seconds: int = _ADMIN_REPLAY_TTL_SECONDS,
) -> Handler:
    """Build create/read/policy handlers sharing one repository dependency."""

    ceiling = deployment_retry_delay_ceiling_seconds

    async def handler(
        scope: MutableMapping[str, Any],
        _receive: Callable[[], Awaitable[dict[str, Any]]],
        send: Callable[[dict[str, Any]], Awaitable[None]],
        context: RequestContext,
        body: bytes,
    ) -> None:
        probe = scope.get("queue_lookup_probe")
        if probe is not None:
            probe.mark()

        if context.operation is Operation.CREATE_QUEUE:
            await _handle_create(
                scope,
                send,
                context,
                body,
                session_factory=session_factory,
                repository=repository,
                ceiling=ceiling,
                admin_replay_ttl_seconds=admin_replay_ttl_seconds,
            )
            return

        if context.operation is Operation.LIST_QUEUES:
            await _handle_list(
                scope,
                send,
                context,
                session_factory=session_factory,
                repository=repository,
            )
            return

        if context.operation is Operation.GET_QUEUE:
            await _handle_get(
                send,
                context,
                session_factory=session_factory,
                repository=repository,
            )
            return

        if context.operation is Operation.CREATE_QUEUE_POLICY:
            await _handle_create_policy(
                scope,
                send,
                context,
                body,
                session_factory=session_factory,
                repository=repository,
                ceiling=ceiling,
                admin_replay_ttl_seconds=admin_replay_ttl_seconds,
            )
            return

        if context.operation is Operation.ACTIVATE_QUEUE_POLICY:
            await _handle_activate_policy(
                scope,
                send,
                context,
                body,
                session_factory=session_factory,
                repository=repository,
                admin_replay_ttl_seconds=admin_replay_ttl_seconds,
            )
            return

        if context.operation is Operation.SET_QUEUE_STATE:
            await _handle_set_queue_state(
                scope,
                send,
                context,
                body,
                session_factory=session_factory,
                repository=repository,
                admin_replay_ttl_seconds=admin_replay_ttl_seconds,
            )
            return

        await _send_domain_error(
            send,
            request_id=context.request_id,
            code="internal_error",
            message="unsupported admin queue operation",
        )

    return handler


async def _mutation_result(
    send: Callable[[dict[str, Any]], Awaitable[None]],
    *,
    request_id: str,
    config: QueueConfiguration,
    admin_replay_ttl_seconds: int,
    status: int = 200,
    extra_headers: Mapping[str, str] | None = None,
    drain_progress: Any | None = None,
) -> None:
    expires_at = config.updated_at + timedelta(seconds=admin_replay_ttl_seconds)
    payload = {
        "queue": queue_configuration_to_response(config, drain_progress=drain_progress),
        "replayed": False,
        "admin_replay_expires_at": _format_dt(expires_at),
    }
    headers = {"X-Request-ID": request_id}
    if extra_headers:
        headers.update(extra_headers)
    await send_json(
        send,
        status=status,
        payload=payload,
        extra_headers=headers,
    )


async def _handle_create(
    scope: MutableMapping[str, Any],
    send: Callable[[dict[str, Any]], Awaitable[None]],
    context: RequestContext,
    body: bytes,
    *,
    session_factory: sessionmaker[Session],
    repository: QueueControlRepository,
    ceiling: int,
    admin_replay_ttl_seconds: int,
) -> None:
    headers = _header_map(scope)
    idempotency_key = headers.get("idempotency-key")
    if idempotency_key is None or not idempotency_key.strip():
        await _send_domain_error(
            send,
            request_id=context.request_id,
            code="idempotency_key_required",
            message="Idempotency-Key header is required",
        )
        return

    try:
        name, policy = _parse_create_body(
            body,
            deployment_retry_delay_ceiling_seconds=ceiling,
        )
        mutation = CreateQueueMutation(
            name=name,
            initial_policy=policy,
            metadata=AdminRequestMetadata(
                actor_id=context.principal.principal_id,
                request_id=context.request_id,
                idempotency_key=idempotency_key,
            ),
        )
    except DomainValidationError as exc:
        await _send_domain_error(
            send,
            request_id=context.request_id,
            code=exc.code,
            message=exc.message,
        )
        return

    session = session_factory()
    try:
        config = repository.create_named_queue(session, mutation)
        session.commit()
    except DomainValidationError as exc:
        session.rollback()
        await _send_domain_error(
            send,
            request_id=context.request_id,
            code=exc.code,
            message=exc.message,
        )
        return
    except Exception:
        session.rollback()
        await _send_domain_error(
            send,
            request_id=context.request_id,
            code="internal_error",
            message="queue create failed",
        )
        return
    finally:
        session.close()

    queue_body = queue_configuration_to_response(config)
    expires_at = config.created_at + timedelta(seconds=admin_replay_ttl_seconds)
    payload = {
        "queue": queue_body,
        "replayed": False,
        "admin_replay_expires_at": _format_dt(expires_at),
    }
    location = f"/admin/v1/queues/{config.name}"
    await send_json(
        send,
        status=201,
        payload=payload,
        extra_headers={
            "X-Request-ID": context.request_id,
            "Location": location,
        },
    )


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


def _parse_list_limit(raw: str | None) -> int:
    if raw is None or raw == "":
        return 50
    try:
        value = int(raw)
    except (TypeError, ValueError) as exc:
        raise DomainValidationError(
            "validation_failed",
            "limit must be an integer",
        ) from exc
    if value < 1 or value > 100:
        raise DomainValidationError(
            "validation_failed",
            "limit must be between 1 and 100",
        )
    return value


async def _handle_list(
    scope: MutableMapping[str, Any],
    send: Callable[[dict[str, Any]], Awaitable[None]],
    context: RequestContext,
    *,
    session_factory: sessionmaker[Session],
    repository: QueueControlRepository,
) -> None:
    query = _query_map(scope)
    try:
        limit = _parse_list_limit(query.get("limit"))
        after_name = query.get("cursor")
    except DomainValidationError as exc:
        await _send_domain_error(
            send,
            request_id=context.request_id,
            code=exc.code,
            message=exc.message,
        )
        return

    session = session_factory()
    try:
        configs, next_cursor = repository.list_queue_configurations(
            session,
            limit=limit,
            after_name=after_name,
        )
        session.commit()
    except DomainValidationError as exc:
        session.rollback()
        await _send_domain_error(
            send,
            request_id=context.request_id,
            code=exc.code,
            message=exc.message,
        )
        return
    except Exception:
        session.rollback()
        await _send_domain_error(
            send,
            request_id=context.request_id,
            code="internal_error",
            message="queue list failed",
        )
        return
    finally:
        session.close()

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
        payload={
            "items": [queue_configuration_to_response(cfg) for cfg in configs],
            "next_cursor": next_cursor,
        },
        extra_headers={"X-Request-ID": context.request_id},
    )


async def _handle_get(
    send: Callable[[dict[str, Any]], Awaitable[None]],
    context: RequestContext,
    *,
    session_factory: sessionmaker[Session],
    repository: QueueControlRepository,
) -> None:
    queue_name = context.path_params.get("queue_name") or context.authorization.queue_name
    if not queue_name:
        await _send_domain_error(
            send,
            request_id=context.request_id,
            code="validation_failed",
            message="queue_name is required",
        )
        return

    session = session_factory()
    try:
        config = repository.get_queue_configuration(session, name=queue_name)
        progress = None
        if config is not None:
            progress = read_drain_progress(session, queue_name=queue_name)
        session.commit()
    except DomainValidationError as exc:
        session.rollback()
        await _send_domain_error(
            send,
            request_id=context.request_id,
            code=exc.code,
            message=exc.message,
        )
        return
    except Exception:
        session.rollback()
        await _send_domain_error(
            send,
            request_id=context.request_id,
            code="internal_error",
            message="queue read failed",
        )
        return
    finally:
        session.close()

    if config is None:
        await _send_domain_error(
            send,
            request_id=context.request_id,
            code="queue_not_found",
            message="queue not found",
        )
        return

    emit_routine_admin_correlation(
        logger,
        operation=context.operation.value,
        request_id=context.request_id,
        trace_id=context.request_id,
        actor_id=context.principal.principal_id,
        queue=queue_name,
        config_version=config.config_version.value,
        maintenance_run_id=None,
        result="success",
        code=None,
    )
    await send_json(
        send,
        status=200,
        payload=queue_configuration_to_response(config, drain_progress=progress),
        extra_headers={"X-Request-ID": context.request_id},
    )


async def _require_idempotency_key(
    scope: MutableMapping[str, Any],
    send: Callable[[dict[str, Any]], Awaitable[None]],
    *,
    request_id: str,
) -> str | None:
    headers = _header_map(scope)
    idempotency_key = headers.get("idempotency-key")
    if idempotency_key is None or not idempotency_key.strip():
        await _send_domain_error(
            send,
            request_id=request_id,
            code="idempotency_key_required",
            message="Idempotency-Key header is required",
        )
        return None
    return idempotency_key


async def _handle_create_policy(
    scope: MutableMapping[str, Any],
    send: Callable[[dict[str, Any]], Awaitable[None]],
    context: RequestContext,
    body: bytes,
    *,
    session_factory: sessionmaker[Session],
    repository: QueueControlRepository,
    ceiling: int,
    admin_replay_ttl_seconds: int,
) -> None:
    idempotency_key = await _require_idempotency_key(
        scope, send, request_id=context.request_id
    )
    if idempotency_key is None:
        return

    queue_name = context.path_params.get("queue_name") or context.authorization.queue_name
    if not queue_name:
        await _send_domain_error(
            send,
            request_id=context.request_id,
            code="validation_failed",
            message="queue_name is required",
        )
        return

    try:
        policy = _parse_policy_body(
            body,
            deployment_retry_delay_ceiling_seconds=ceiling,
        )
        mutation = CreatePolicyMutation(
            policy=policy,
            metadata=AdminRequestMetadata(
                actor_id=context.principal.principal_id,
                request_id=context.request_id,
                idempotency_key=idempotency_key,
            ),
        )
    except DomainValidationError as exc:
        await _send_domain_error(
            send,
            request_id=context.request_id,
            code=exc.code,
            message=exc.message,
        )
        return

    session = session_factory()
    try:
        config = repository.create_policy_version(
            session,
            queue_name=queue_name,
            mutation=mutation,
        )
        session.commit()
    except DomainValidationError as exc:
        session.rollback()
        await _send_domain_error(
            send,
            request_id=context.request_id,
            code=exc.code,
            message=exc.message,
        )
        return
    except Exception:
        session.rollback()
        await _send_domain_error(
            send,
            request_id=context.request_id,
            code="internal_error",
            message="policy create failed",
        )
        return
    finally:
        session.close()

    await _mutation_result(
        send,
        request_id=context.request_id,
        config=config,
        admin_replay_ttl_seconds=admin_replay_ttl_seconds,
    )


async def _handle_activate_policy(
    scope: MutableMapping[str, Any],
    send: Callable[[dict[str, Any]], Awaitable[None]],
    context: RequestContext,
    body: bytes,
    *,
    session_factory: sessionmaker[Session],
    repository: QueueControlRepository,
    admin_replay_ttl_seconds: int,
) -> None:
    idempotency_key = await _require_idempotency_key(
        scope, send, request_id=context.request_id
    )
    if idempotency_key is None:
        return

    queue_name = context.path_params.get("queue_name") or context.authorization.queue_name
    if not queue_name:
        await _send_domain_error(
            send,
            request_id=context.request_id,
            code="validation_failed",
            message="queue_name is required",
        )
        return

    raw_version = context.path_params.get("policy_version")
    try:
        expected = _parse_activate_body(body)
        policy_version = parse_policy_version(
            int(raw_version) if raw_version is not None else raw_version
        )
        mutation = ActivatePolicyMutation(
            expected_config_version=expected,
            policy_version=policy_version,
            metadata=AdminRequestMetadata(
                actor_id=context.principal.principal_id,
                request_id=context.request_id,
                idempotency_key=idempotency_key,
            ),
        )
    except DomainValidationError as exc:
        await _send_domain_error(
            send,
            request_id=context.request_id,
            code=exc.code,
            message=exc.message,
        )
        return
    except (TypeError, ValueError):
        await _send_domain_error(
            send,
            request_id=context.request_id,
            code="validation_failed",
            message="policy_version must be an integer >= 1",
        )
        return

    session = session_factory()
    try:
        config = repository.activate_policy_version(
            session,
            queue_name=queue_name,
            mutation=mutation,
        )
        session.commit()
    except DomainValidationError as exc:
        session.rollback()
        await _send_domain_error(
            send,
            request_id=context.request_id,
            code=exc.code,
            message=exc.message,
        )
        return
    except Exception:
        session.rollback()
        await _send_domain_error(
            send,
            request_id=context.request_id,
            code="internal_error",
            message="policy activation failed",
        )
        return
    finally:
        session.close()

    await _mutation_result(
        send,
        request_id=context.request_id,
        config=config,
        admin_replay_ttl_seconds=admin_replay_ttl_seconds,
    )

async def _handle_set_queue_state(
    scope: MutableMapping[str, Any],
    send: Callable[[dict[str, Any]], Awaitable[None]],
    context: RequestContext,
    body: bytes,
    *,
    session_factory: sessionmaker[Session],
    repository: QueueControlRepository,
    admin_replay_ttl_seconds: int,
) -> None:
    idempotency_key = await _require_idempotency_key(
        scope, send, request_id=context.request_id
    )
    if idempotency_key is None:
        return

    queue_name = context.path_params.get("queue_name") or context.authorization.queue_name
    if not queue_name:
        await _send_domain_error(
            send,
            request_id=context.request_id,
            code="validation_failed",
            message="queue_name is required",
        )
        return

    try:
        expected, state = _parse_set_state_body(body)
        mutation = SetQueueStateMutation(
            expected_config_version=expected,
            state=state,
            metadata=AdminRequestMetadata(
                actor_id=context.principal.principal_id,
                request_id=context.request_id,
                idempotency_key=idempotency_key,
            ),
        )
    except DomainValidationError as exc:
        await _send_domain_error(
            send,
            request_id=context.request_id,
            code=exc.code,
            message=exc.message,
        )
        return

    session = session_factory()
    try:
        config = repository.set_queue_state(
            session,
            queue_name=queue_name,
            mutation=mutation,
        )
        progress = read_drain_progress(session, queue_name=queue_name)
        session.commit()
    except DomainValidationError as exc:
        session.rollback()
        emit_routine_admin_correlation(
            logger,
            operation=context.operation.value,
            request_id=context.request_id,
            trace_id=context.request_id,
            actor_id=context.principal.principal_id,
            queue=queue_name,
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
        return
    except Exception:
        session.rollback()
        emit_routine_admin_correlation(
            logger,
            operation=context.operation.value,
            request_id=context.request_id,
            trace_id=context.request_id,
            actor_id=context.principal.principal_id,
            queue=queue_name,
            config_version=None,
            maintenance_run_id=None,
            result="internal_error",
            code="internal_error",
        )
        await _send_domain_error(
            send,
            request_id=context.request_id,
            code="internal_error",
            message="queue state change failed",
        )
        return
    finally:
        session.close()

    emit_routine_admin_correlation(
        logger,
        operation=context.operation.value,
        request_id=context.request_id,
        trace_id=context.request_id,
        actor_id=context.principal.principal_id,
        queue=queue_name,
        config_version=config.config_version.value,
        maintenance_run_id=None,
        result="success",
        code=None,
    )
    await _mutation_result(
        send,
        request_id=context.request_id,
        config=config,
        admin_replay_ttl_seconds=admin_replay_ttl_seconds,
        drain_progress=progress,
    )

