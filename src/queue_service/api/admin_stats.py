"""Observer/admin-authorized bounded statistics handler (OPS-03).

Remapped from plan path ``api/admin/stats.py`` to Phase 3.x flat admin module
convention (``api/admin.py`` already occupies the admin package name).
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable, MutableMapping
from typing import Any

from sqlalchemy.orm import Session, sessionmaker

from queue_service.api.security import RequestContext, error_envelope, send_json
from queue_service.observability.metrics import KernelMetrics
from queue_service.operations.stats import build_stats_snapshot
from queue_service.security.authorization import Operation
from queue_service.security.redaction import sanitize_for_diagnostics

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


def build_admin_stats_handler(
    *,
    session_factory: sessionmaker[Session],
    metrics: KernelMetrics | None = None,
) -> Handler:
    """Build GET ``/admin/v1/stats`` handler using keyed counter reads."""

    async def handler(
        scope: MutableMapping[str, Any],
        _receive: Callable[[], Awaitable[dict[str, Any]]],
        send: Callable[[dict[str, Any]], Awaitable[None]],
        context: RequestContext,
        body: bytes,
    ) -> None:
        _ = scope
        _ = body
        if context.operation is not Operation.GET_STATS:
            await send_json(
                send,
                status=500,
                payload=error_envelope(
                    code="internal_error",
                    message="stats handler invoked for unexpected operation",
                    retryable=True,
                    request_id=context.request_id,
                ),
            )
            return

        session = session_factory()
        try:
            snapshot = build_stats_snapshot(session, metrics=metrics)
            _ = sanitize_for_diagnostics(
                {
                    "request_id": context.request_id,
                    "operation": context.operation.value,
                    "principal_id": context.principal.principal_id,
                    "freshness": snapshot.get("freshness"),
                    "queue_count": len(snapshot.get("queues", [])),
                }
            )
            await send_json(
                send,
                status=200,
                payload=snapshot,
                extra_headers={"X-Request-ID": context.request_id},
            )
        except Exception:
            await send_json(
                send,
                status=500,
                payload=error_envelope(
                    code="internal_error",
                    message="failed to build statistics snapshot",
                    retryable=True,
                    request_id=context.request_id,
                ),
            )
        finally:
            session.close()

    return handler
