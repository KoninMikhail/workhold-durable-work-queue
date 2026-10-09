"""Golden parity: ProducerClient vs AsyncProducerClient."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import pytest

from _queue_service_client_core.async_transport import HttpxAsyncTransport
from _queue_service_client_core.transport import HttpJsonTransport
from queue_service_producer import ProducerClient
from queue_service_producer.async_client import AsyncProducerClient

from .conftest import normalize_recorded

PRODUCER_TOKEN = "secret-token-value"


def _task_body(**overrides: Any) -> dict[str, Any]:
    body: dict[str, Any] = {
        "task_id": "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
        "queue_name": "orders",
        "producer_id": "producer-1",
        "state": "ready",
        "priority": 0,
        "available_at": "2026-09-19T00:00:00Z",
        "retry_policy_version": 1,
        "created_at": "2026-09-19T00:00:00Z",
        "spawned_task_ids": [],
        "delivery_event_ids": [],
    }
    body.update(overrides)
    return body


def _capabilities_body(**overrides: Any) -> dict[str, Any]:
    body: dict[str, Any] = {
        "protocol_major": 1,
        "protocol_version": "1.0",
        "schema_revision": "0001",
        "scheduling": True,
        "priority": True,
        "delivery_events": False,
        "batch_claim": False,
        "long_polling": False,
        "max_claim_tasks": 1,
        "max_wait_seconds": 0,
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
    body.update(overrides)
    return body


def _wire_producer_routes(server: Any, task_id: str) -> None:
    server.routes[("GET", "/v1/capabilities")] = (
        lambda body, headers: (200, _capabilities_body())
    )
    server.routes[("POST", "/v1/queues/orders/tasks")] = (
        lambda body, headers: (201, {"task": _task_body(), "replayed": False})
    )
    server.routes[("POST", "/v1/queues/orders/submissions:resolve")] = (
        lambda body, headers: (
            200,
            {
                "task": _task_body(),
                "dedup_expires_at": "2026-12-18T00:00:00Z",
                "replayed": False,
            },
        )
    )
    server.routes[("GET", f"/v1/tasks/{task_id}")] = (
        lambda body, headers: (200, _task_body())
    )
    server.routes[("POST", f"/v1/tasks/{task_id}:cancel")] = (
        lambda body, headers: (200, {"task": _task_body(state="cancelled")})
    )


async def test_producer_operations_emit_identical_requests(
    recording_server: Any,
) -> None:
    server, base_url = recording_server
    task_id = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
    _wire_producer_routes(server, task_id)

    sync_client = ProducerClient(
        HttpJsonTransport(base_url, timeout_s=2.0), bearer_token=PRODUCER_TOKEN
    )
    sync_client.get_capabilities()
    sync_client.enqueue(
        "orders",
        idempotency_key="idem-1",
        payload={"order_id": 42},
        priority=0,
        available_at=datetime(2026, 9, 20, 12, 0, tzinfo=UTC),
    )
    sync_client.resolve_submission("orders", idempotency_key="idem-1")
    sync_client.inspect_task(task_id)
    sync_client.cancel_task(task_id, reason="user_aborted")
    sync_records = normalize_recorded(server.recorded)
    server.recorded.clear()

    async_transport = HttpxAsyncTransport(base_url, timeout_s=2.0)
    async_client = AsyncProducerClient(async_transport, bearer_token=PRODUCER_TOKEN)
    await async_client.get_capabilities()
    await async_client.enqueue(
        "orders",
        idempotency_key="idem-1",
        payload={"order_id": 42},
        priority=0,
        available_at=datetime(2026, 9, 20, 12, 0, tzinfo=UTC),
    )
    await async_client.resolve_submission("orders", idempotency_key="idem-1")
    await async_client.inspect_task(task_id)
    await async_client.cancel_task(task_id, reason="user_aborted")
    await async_transport.aclose()
    async_records = normalize_recorded(server.recorded)

    assert sync_records == async_records


async def test_owned_transport_context_manager_closes(recording_server: Any) -> None:
    _, base_url = recording_server
    async with AsyncProducerClient.from_url(
        base_url, bearer_token=PRODUCER_TOKEN, timeout_s=2.0
    ) as client:
        assert isinstance(client, AsyncProducerClient)


async def test_injected_transport_not_closed_by_context_manager(
    recording_server: Any,
) -> None:
    server, base_url = recording_server
    server.routes[("GET", "/v1/capabilities")] = (
        lambda body, headers: (200, _capabilities_body())
    )
    transport = HttpxAsyncTransport(base_url, timeout_s=2.0)
    async with AsyncProducerClient(
        transport, bearer_token=PRODUCER_TOKEN, owns_transport=False
    ):
        pass
    response = await transport.request("GET", "/v1/capabilities", json_body=None)
    assert response.status_code == 200
    await transport.aclose()
