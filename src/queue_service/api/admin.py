"""Private admin-plane ASGI composition (`/admin/v1` only)."""

from __future__ import annotations

from collections.abc import Awaitable, Callable, MutableMapping
from typing import Any

from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker

from queue_service.api.admin_bulk import build_admin_bulk_handler
from queue_service.api.admin_break_glass import build_admin_break_glass_handler
from queue_service.api.admin_dead_letter import build_admin_dead_letter_handler
from queue_service.api.admin_inspection import build_admin_inspection_handler
from queue_service.api.admin_operations import build_admin_operations_handler
from queue_service.api.admin_queues import build_admin_queues_handler
from queue_service.api.admin_stats import build_admin_stats_handler
from queue_service.operations.bulk import BulkReplayRateGate
from queue_service.api.security import (
    Handler,
    ListenerBind,
    RequestContext,
    create_plane_app,
    error_envelope,
    send_json,
)
from queue_service.domain.queue_control import RETRY_DELAY_SECONDS_ABSOLUTE_MAX
from queue_service.infrastructure.postgres.queue_control_repository import (
    QueueControlRepository,
)
from queue_service.observability.metrics import KernelMetrics
from queue_service.security.authorization import Authorizer, Operation
from queue_service.security.credentials import IdentityAuthenticator
from queue_service.security.payload_policy import (
    PayloadHandlingPolicy,
    PayloadRetentionPolicy,
)
from queue_service.security.redaction import sanitize_for_diagnostics
from queue_service.settings import (
    ADMIN_REPLAY_TTL_SECONDS_DEFAULT,
    REGISTRY_PURGE_BATCH_SIZE_DEFAULT,
    Secret,
)

SKELETON_CODE = "skeleton_operation_unsupported"
SKELETON_MESSAGE = "operation is not implemented"

_DEFAULT_CURSOR_SECRET = Secret("queue-inspection-cursor-v1")
_DEFAULT_PAYLOAD_RETENTION_DAYS = 30

_IMPLEMENTED_QUEUE_OPS = frozenset(
    {
        Operation.LIST_QUEUES,
        Operation.CREATE_QUEUE,
        Operation.GET_QUEUE,
        Operation.CREATE_QUEUE_POLICY,
        Operation.ACTIVATE_QUEUE_POLICY,
        Operation.SET_QUEUE_STATE,
    }
)

_INSPECTION_OPS = frozenset(
    {
        Operation.LIST_INSPECTION_TASKS,
        Operation.LIST_INSPECTION_ATTEMPTS,
        Operation.LIST_DEAD_LETTERS,
        Operation.LIST_ADMIN_AUDIT,
    }
)

_OPERATIONS_OPS = frozenset(
    {
        Operation.GET_MAINTENANCE_STATUS,
        Operation.RUN_MAINTENANCE,
    }
)

_DEAD_LETTER_OPS = frozenset({Operation.REPLAY_DEAD_LETTER})

_BULK_OPS = frozenset(
    {
        Operation.PREVIEW_BULK_REPLAY,
        Operation.EXECUTE_BULK_REPLAY,
        Operation.PREVIEW_BULK_CANCEL,
        Operation.EXECUTE_BULK_CANCEL,
    }
)

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


def _skeleton_handler(
    *,
    payload_policy: PayloadHandlingPolicy,
) -> Handler:
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

        _ = payload_policy.inspect(
            {},
            payload_bytes=len(body),
            include_payload=False,
        )
        _ = sanitize_for_diagnostics(
            {
                "request_id": context.request_id,
                "operation": context.operation.value,
                "principal_id": context.principal.principal_id,
                "queue_name": context.authorization.queue_name,
            }
        )
        await send_json(
            send,
            status=501,
            payload=error_envelope(
                code=SKELETON_CODE,
                message=SKELETON_MESSAGE,
                retryable=False,
                request_id=context.request_id,
            ),
        )

    return handler


def _dispatch_handler(
    *,
    queue_handler: Handler | None,
    stats_handler: Handler | None,
    inspection_handler: Handler | None,
    operations_handler: Handler | None,
    dead_letter_handler: Handler | None,
    bulk_handler: Handler | None,
    break_glass_handler: Handler | None,
    skeleton: Handler,
) -> Handler:
    async def handler(
        scope: MutableMapping[str, Any],
        receive: Callable[[], Awaitable[dict[str, Any]]],
        send: Callable[[dict[str, Any]], Awaitable[None]],
        context: RequestContext,
        body: bytes,
    ) -> None:
        if stats_handler is not None and context.operation is Operation.GET_STATS:
            await stats_handler(scope, receive, send, context, body)
            return
        if (
            inspection_handler is not None
            and context.operation in _INSPECTION_OPS
        ):
            await inspection_handler(scope, receive, send, context, body)
            return
        if (
            operations_handler is not None
            and context.operation in _OPERATIONS_OPS
        ):
            await operations_handler(scope, receive, send, context, body)
            return
        if (
            dead_letter_handler is not None
            and context.operation in _DEAD_LETTER_OPS
        ):
            await dead_letter_handler(scope, receive, send, context, body)
            return
        if bulk_handler is not None and context.operation in _BULK_OPS:
            await bulk_handler(scope, receive, send, context, body)
            return
        if (
            break_glass_handler is not None
            and context.operation in _BREAK_GLASS_OPS
        ):
            await break_glass_handler(scope, receive, send, context, body)
            return
        if queue_handler is not None and context.operation in _IMPLEMENTED_QUEUE_OPS:
            await queue_handler(scope, receive, send, context, body)
            return
        await skeleton(scope, receive, send, context, body)

    return handler


def _resolve_engine(
    session_factory: sessionmaker[Session],
    engine: Engine | None,
) -> Engine | None:
    if engine is not None:
        return engine
    bind = getattr(session_factory, "kw", {}).get("bind")
    if isinstance(bind, Engine):
        return bind
    try:
        probe = session_factory()
        try:
            resolved = probe.get_bind()
        finally:
            probe.close()
        return resolved if isinstance(resolved, Engine) else None
    except Exception:
        return None


def create_admin_app(
    *,
    authenticator: IdentityAuthenticator,
    authorizer: Authorizer,
    bind: ListenerBind,
    lookup_probe: Any | None = None,
    payload_policy: PayloadHandlingPolicy | None = None,
    session_factory: sessionmaker[Session] | None = None,
    repository: QueueControlRepository | None = None,
    deployment_retry_delay_ceiling_seconds: int = RETRY_DELAY_SECONDS_ABSOLUTE_MAX,
    metrics: KernelMetrics | None = None,
    cursor_secret: Secret | None = None,
    engine: Engine | None = None,
    payload_retention_policy: PayloadRetentionPolicy | None = None,
    registry_purge_batch_size: int = REGISTRY_PURGE_BATCH_SIZE_DEFAULT,
    admin_replay_ttl_seconds: int = ADMIN_REPLAY_TTL_SECONDS_DEFAULT,
    bulk_rate_gate: BulkReplayRateGate | None = None,
) -> Any:
    """Build the private `/admin/v1` ASGI app bound for a distinct listener.

    When ``session_factory`` and ``repository`` are provided, named-queue create,
    read, policy, and state mutations are served by handlers. Stats, operational
    inspection lists, and routine maintenance tools are served when
    ``session_factory`` (and a resolvable engine for maintenance) are provided.
    """

    policy = payload_policy or PayloadHandlingPolicy()
    skeleton = _skeleton_handler(payload_policy=policy)
    queue_handler: Handler | None = None
    stats_handler: Handler | None = None
    inspection_handler: Handler | None = None
    operations_handler: Handler | None = None
    dead_letter_handler: Handler | None = None
    bulk_handler: Handler | None = None
    break_glass_handler: Handler | None = None
    resolved_cursor_secret = cursor_secret or _DEFAULT_CURSOR_SECRET
    shared_rate_gate = bulk_rate_gate if bulk_rate_gate is not None else BulkReplayRateGate()
    if session_factory is not None and repository is not None:
        queue_handler = build_admin_queues_handler(
            session_factory=session_factory,
            repository=repository,
            deployment_retry_delay_ceiling_seconds=deployment_retry_delay_ceiling_seconds,
            admin_replay_ttl_seconds=admin_replay_ttl_seconds,
        )
    if session_factory is not None:
        stats_handler = build_admin_stats_handler(
            session_factory=session_factory,
            metrics=metrics,
        )
        inspection_handler = build_admin_inspection_handler(
            session_factory=session_factory,
            authorizer=authorizer,
            cursor_secret=resolved_cursor_secret,
        )
        dead_letter_handler = build_admin_dead_letter_handler(
            session_factory=session_factory,
            admin_replay_ttl_seconds=admin_replay_ttl_seconds,
        )
        bulk_handler = build_admin_bulk_handler(
            session_factory=session_factory,
            confirmation_secret=resolved_cursor_secret,
            admin_replay_ttl_seconds=admin_replay_ttl_seconds,
            rate_gate=shared_rate_gate,
        )
        bind_engine = _resolve_engine(session_factory, engine)
        if bind_engine is not None:
            retention = payload_retention_policy or PayloadRetentionPolicy(
                retention_days=_DEFAULT_PAYLOAD_RETENTION_DAYS
            )
            operations_handler = build_admin_operations_handler(
                session_factory=session_factory,
                engine=bind_engine,
                payload_retention_policy=retention,
                registry_purge_batch_size=registry_purge_batch_size,
                admin_replay_ttl_seconds=admin_replay_ttl_seconds,
            )
            break_glass_handler = build_admin_break_glass_handler(
                session_factory=session_factory,
                engine=bind_engine,
                rate_gate=shared_rate_gate,
                payload_retention_policy=retention,
            )
    return create_plane_app(
        plane="admin",
        authenticator=authenticator,
        authorizer=authorizer,
        bind=bind,
        handler=_dispatch_handler(
            queue_handler=queue_handler,
            stats_handler=stats_handler,
            inspection_handler=inspection_handler,
            operations_handler=operations_handler,
            dead_letter_handler=dead_letter_handler,
            bulk_handler=bulk_handler,
            break_glass_handler=break_glass_handler,
            skeleton=skeleton,
        ),
        lookup_probe=lookup_probe,
    )
