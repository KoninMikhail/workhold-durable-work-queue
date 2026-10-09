"""Live GET /v1/capabilities handler — server-owned OpenAPI catalog."""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Mapping, MutableMapping
from typing import Any

from queue_service.api.security import Handler, RequestContext, send_json
from queue_service.settings import CLAIM_MAX_WAIT_SECONDS_DEFAULT

# Fields that do not vary with the deployment claim wait ceiling.
_STATIC_CAPABILITIES: Mapping[str, Any] = {
    "protocol_major": 1,
    "protocol_version": "1.0",
    "schema_revision": "0001",
    "scheduling": True,
    "priority": True,
    "delivery_events": False,
    "batch_claim": False,
    "max_claim_tasks": 1,
    "payload_runtime_max_bytes": 262144,
    "payload_hard_max_bytes": 1048576,
    "enqueue_dedup_ttl_seconds": 7776000,
    "enqueue_dedup_ttl_min_seconds": 2592000,
    "enqueue_dedup_ttl_max_seconds": 31536000,
    "terminal_replay_ttl_seconds": 604800,
    "terminal_replay_ttl_min_seconds": 86400,
    "terminal_replay_ttl_max_seconds": 2592000,
    "admin_replay_ttl_seconds": 2592000,
    "admin_replay_ttl_min_seconds": 604800,
    "admin_replay_ttl_max_seconds": 7776000,
}


def live_capabilities_for(*, max_wait_seconds: int) -> dict[str, Any]:
    """Advertise long polling from the deployment claim wait ceiling.

    ``max_wait_seconds == 0`` fail-closes: ``long_polling=false`` and
    ``max_wait_seconds=0``. A positive ceiling advertises ``long_polling=true``
    with that exact maximum. Batch claim stays disabled (``max_claim_tasks=1``).
    """

    return {
        **_STATIC_CAPABILITIES,
        "long_polling": max_wait_seconds > 0,
        "max_wait_seconds": max_wait_seconds,
    }


# Default catalog for the production default ceiling (true / 20).
LIVE_CAPABILITIES: Mapping[str, Any] = live_capabilities_for(
    max_wait_seconds=CLAIM_MAX_WAIT_SECONDS_DEFAULT
)


def build_capabilities_handler(
    *,
    max_wait_seconds: int = CLAIM_MAX_WAIT_SECONDS_DEFAULT,
) -> Handler:
    """Return authenticated storage-independent capability discovery."""

    payload = live_capabilities_for(max_wait_seconds=max_wait_seconds)

    async def handler(
        scope: MutableMapping[str, Any],
        _receive: Callable[[], Awaitable[dict[str, Any]]],
        send: Callable[[dict[str, Any]], Awaitable[None]],
        context: RequestContext,
        _body: bytes,
    ) -> None:
        probe = scope.get("queue_lookup_probe")
        if probe is not None:
            probe.mark()

        await send_json(
            send,
            status=200,
            payload=dict(payload),
            extra_headers={"X-Request-ID": context.request_id},
        )

    return handler
