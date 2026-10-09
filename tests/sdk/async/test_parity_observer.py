"""Golden parity: ObserverClient vs AsyncObserverClient."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any
from urllib.parse import parse_qs

import pytest

from _workhold_client_core.async_transport import HttpxAsyncTransport
from _workhold_client_core.transport import HttpJsonTransport
from workhold_admin import ObserverClient
from workhold_admin.async_client import AsyncObserverClient

from .conftest import normalize_recorded

OBSERVER_TOKEN = "observer-secret-token"
TASK_ID = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"


def _capabilities_body() -> dict[str, Any]:
    return {
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


def _task_body(**overrides: Any) -> dict[str, Any]:
    body: dict[str, Any] = {
        "task_id": TASK_ID,
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


def _wire_observer_routes(server: Any) -> None:
    t_from = datetime(2026, 9, 19, 0, 0, tzinfo=UTC)
    t_to = t_from + timedelta(hours=1)

    server.routes[("GET", "/v1/capabilities")] = (
        lambda body, headers: (200, _capabilities_body())
    )
    server.routes[("GET", f"/v1/tasks/{TASK_ID}")] = (
        lambda body, headers: (200, _task_body())
    )
    server.routes[("GET", f"/v1/tasks/{TASK_ID}/attempts")] = (
        lambda body, headers: (200, {"items": [], "next_cursor": None})
    )
    server.routes[("GET", "/admin/v1/queues/orders")] = (
        lambda body, headers: (
            200,
            {
                "queue_id": "cccccccc-cccc-4ccc-8ccc-cccccccccccc",
                "name": "orders",
                "state": "active",
                "config_version": 1,
                "active_policy": {
                    "version": 1,
                    "enabled": True,
                    "max_attempts": 3,
                    "backoff_strategy": "fixed",
                    "retry_delay_seconds": 30,
                    "created_at": "2026-09-19T00:00:00Z",
                },
                "created_at": "2026-09-19T00:00:00Z",
                "updated_at": "2026-09-19T00:00:00Z",
            },
        )
    )
    server.routes[("GET", "/admin/v1/stats")] = (
        lambda body, headers: (
            200,
            {
                "as_of": "2026-09-19T00:00:00Z",
                "generated_at": "2026-09-19T00:00:01Z",
                "age_seconds": 1.0,
                "freshness": "fresh",
                "queues": [],
                "retry": {
                    "availability": "available",
                    "source": "process_telemetry",
                    "total": 0,
                },
                "dead_letter": {
                    "availability": "available",
                    "source": "process_telemetry",
                    "total": 0,
                },
                "maintenance": {"availability": "available"},
            },
        )
    )
    server.routes[("GET", "/admin/v1/maintenance")] = (
        lambda body, headers: (
            200,
            {
                "updated_at": "2026-09-19T00:00:00Z",
                "outcome": "succeeded",
                "maintenance_run_id": "dddddddd-dddd-4ddd-8ddd-dddddddddddd",
            },
        )
    )
    server.routes[("GET", "/admin/v1/tasks")] = (
        lambda body, headers: (200, {"items": [], "next_cursor": None})
    )
    server.routes[("GET", "/admin/v1/attempts")] = (
        lambda body, headers: (200, {"items": [], "next_cursor": None})
    )
    server.routes[("GET", "/admin/v1/dead-letters")] = (
        lambda body, headers: (200, {"items": [], "next_cursor": None})
    )
    _ = (t_from, t_to)


async def test_observer_operations_emit_identical_requests(
    recording_server: Any,
) -> None:
    server, base_url = recording_server
    _wire_observer_routes(server)
    t_from = datetime(2026, 9, 19, 0, 0, tzinfo=UTC)
    t_to = t_from + timedelta(hours=1)

    sync_client = ObserverClient(
        HttpJsonTransport(base_url, timeout_s=2.0),
        bearer_token=OBSERVER_TOKEN,
        admin_transport=HttpJsonTransport(base_url, timeout_s=2.0),
    )
    sync_client.get_capabilities()
    sync_client.get_task(TASK_ID)
    sync_client.list_task_attempts(TASK_ID)
    sync_client.get_queue("orders")
    sync_client.get_stats()
    sync_client.get_maintenance_status()
    sync_client.list_inspection_tasks("orders")
    sync_client.list_inspection_attempts(TASK_ID, time_from=t_from, time_to=t_to)
    sync_client.list_dead_letters("orders", time_from=t_from, time_to=t_to)
    sync_records = normalize_recorded(server.recorded)
    server.recorded.clear()

    async_transport = HttpxAsyncTransport(base_url, timeout_s=2.0)
    async_client = AsyncObserverClient(
        async_transport,
        bearer_token=OBSERVER_TOKEN,
        admin_transport=async_transport,
    )
    await async_client.get_capabilities()
    await async_client.get_task(TASK_ID)
    await async_client.list_task_attempts(TASK_ID)
    await async_client.get_queue("orders")
    await async_client.get_stats()
    await async_client.get_maintenance_status()
    await async_client.list_inspection_tasks("orders")
    await async_client.list_inspection_attempts(TASK_ID, time_from=t_from, time_to=t_to)
    await async_client.list_dead_letters("orders", time_from=t_from, time_to=t_to)
    await async_transport.aclose()
    async_records = normalize_recorded(server.recorded)

    assert sync_records == async_records
    for rec in sync_records:
        if rec["query"]:
            assert parse_qs(rec["query"])
