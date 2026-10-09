"""Golden parity: ConsumerClient/Claim vs AsyncConsumerClient/AsyncClaim."""

from __future__ import annotations

import json
from typing import Any

import pytest

from _queue_service_client_core.async_transport import HttpxAsyncTransport
from _queue_service_client_core.errors import (
    LeaseLostError,
    ProtocolError,
    TerminalConflictError,
)
from _queue_service_client_core.transport import HttpJsonTransport
from queue_service_consumer import Claim, ConsumerClient
from queue_service_consumer.async_client import AsyncClaim, AsyncConsumerClient

from .conftest import normalize_recorded

WORKER_BEARER = "worker-bearer-secret"
CLAIM_ID = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"
CLAIM_TOKEN = "claim-token-secret-value-do-not-leak"
TASK_ID = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"


def _task_body(**overrides: Any) -> dict[str, Any]:
    body: dict[str, Any] = {
        "task_id": TASK_ID,
        "queue_name": "orders",
        "producer_id": "producer-1",
        "state": "leased",
        "priority": 0,
        "available_at": "2026-09-19T00:00:00Z",
        "retry_policy_version": 1,
        "created_at": "2026-09-19T00:00:00Z",
        "spawned_task_ids": [],
        "delivery_event_ids": [],
        "payload": {"order_id": 7},
    }
    body.update(overrides)
    return body


def _claim_grant(**overrides: Any) -> dict[str, Any]:
    body: dict[str, Any] = {
        "claim_id": CLAIM_ID,
        "generation": 3,
        "claimed_at": "2026-09-19T00:01:00Z",
        "lease_expires_at": "2026-09-19T00:02:00Z",
        "worker_id": "worker-1",
        "cancel_requested": False,
        "claim_token": CLAIM_TOKEN,
    }
    body.update(overrides)
    return body


def _claim_summary(**overrides: Any) -> dict[str, Any]:
    grant = _claim_grant(**overrides)
    grant.pop("claim_token", None)
    return grant


def _claim_response(*, empty: bool = False) -> dict[str, Any]:
    tasks: list[dict[str, Any]] = []
    if not empty:
        tasks.append({"task": _task_body(), "claim": _claim_grant()})
    return {
        "tasks": tasks,
        "server_time": "2026-09-19T00:01:00Z",
        "recommended_heartbeat_seconds": 10,
        "queue_states": {"orders": "active"},
    }


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


def _wire_lease_routes(server: Any) -> None:
    server.routes[("GET", "/v1/capabilities")] = (
        lambda body, headers: (200, _capabilities_body())
    )
    server.routes[("POST", "/v1/claims")] = (
        lambda body, headers: (200, _claim_response())
    )
    server.routes[("POST", f"/v1/claims/{CLAIM_ID}:heartbeat")] = (
        lambda body, headers: (
            200,
            {
                "claim": _claim_summary(lease_expires_at="2026-09-19T00:03:00Z"),
                "server_time": "2026-09-19T00:02:00Z",
                "recommended_heartbeat_seconds": 8,
            },
        )
    )
    server.routes[("POST", f"/v1/claims/{CLAIM_ID}:complete")] = (
        lambda body, headers: (
            200,
            {
                "task_id": TASK_ID,
                "state": "succeeded",
                "spawned_task_ids": [],
                "replayed": False,
            },
        )
    )
    server.routes[("POST", f"/v1/claims/{CLAIM_ID}:fail")] = (
        lambda body, headers: (
            200,
            {
                "task_id": TASK_ID,
                "state": "retry_scheduled",
                "available_at": "2026-09-19T00:05:00Z",
                "replayed": False,
            },
        )
    )
    server.routes[("POST", f"/v1/claims/{CLAIM_ID}:ack-cancel")] = (
        lambda body, headers: (
            200,
            {
                "task_id": TASK_ID,
                "state": "cancelled",
                "terminal_at": "2026-09-19T00:04:00Z",
                "replayed": False,
            },
        )
    )


async def test_consumer_lifecycle_parity(recording_server: Any) -> None:
    server, base_url = recording_server
    _wire_lease_routes(server)

    sync_client = ConsumerClient(
        HttpJsonTransport(base_url, timeout_s=2.0), bearer_token=WORKER_BEARER
    )
    sync_client.claim(
        queues=["orders"], worker_id="worker-1", lease_seconds=60
    )[0].heartbeat(lease_seconds=60)
    sync_client.claim(
        queues=["orders"], worker_id="worker-1", lease_seconds=60
    )[0].complete(spawn=[])
    sync_client.claim(
        queues=["orders"], worker_id="worker-1", lease_seconds=60
    )[0].fail(retryable=True, failure_code="TransientError")
    sync_client.claim(
        queues=["orders"], worker_id="worker-1", lease_seconds=60
    )[0].ack_cancel()
    sync_records = normalize_recorded(server.recorded)
    server.recorded.clear()

    async_transport = HttpxAsyncTransport(base_url, timeout_s=2.0)
    async_client = AsyncConsumerClient(async_transport, bearer_token=WORKER_BEARER)
    await (
        await async_client.claim(
            queues=["orders"], worker_id="worker-1", lease_seconds=60
        )
    )[0].heartbeat(lease_seconds=60)
    await (
        await async_client.claim(
            queues=["orders"], worker_id="worker-1", lease_seconds=60
        )
    )[0].complete(spawn=[])
    await (
        await async_client.claim(
            queues=["orders"], worker_id="worker-1", lease_seconds=60
        )
    )[0].fail(retryable=True, failure_code="TransientError")
    await (
        await async_client.claim(
            queues=["orders"], worker_id="worker-1", lease_seconds=60
        )
    )[0].ack_cancel()
    await async_transport.aclose()
    async_records = normalize_recorded(server.recorded)

    assert sync_records == async_records
    lease_ops = [r for r in sync_records if r["path"] != "/v1/claims"]
    for rec in lease_ops:
        assert CLAIM_ID in rec["path"]
        assert CLAIM_TOKEN not in rec["path"]
        body_text = rec["body"].decode("utf-8") if rec["body"] else ""
        assert CLAIM_TOKEN not in body_text
        assert rec["headers"].get("x-queue-claim-token") == CLAIM_TOKEN


async def test_lease_lost_and_terminal_conflict_parity(recording_server: Any) -> None:
    server, base_url = recording_server
    _wire_lease_routes(server)
    server.routes[("POST", f"/v1/claims/{CLAIM_ID}:complete")] = (
        lambda body, headers: (
            409,
            {
                "code": "lease_lost",
                "message": "lease expired",
                "retryable": False,
                "request_id": "22222222-2222-4222-8222-222222222222",
                "details": {},
            },
        )
    )

    sync_client = ConsumerClient(
        HttpJsonTransport(base_url, timeout_s=2.0), bearer_token=WORKER_BEARER
    )
    sync_claim = sync_client.claim(
        queues=["orders"], worker_id="worker-1", lease_seconds=60
    )[0]
    with pytest.raises(ProtocolError) as sync_exc:
        sync_claim.complete()
    assert sync_exc.value.code.value == "lease_lost"
    assert sync_claim.lease_lost is True
    with pytest.raises(LeaseLostError):
        sync_claim.complete()

    server.recorded.clear()
    server.routes[("POST", f"/v1/claims/{CLAIM_ID}:complete")] = (
        lambda body, headers: (
            409,
            {
                "code": "lease_lost",
                "message": "lease expired",
                "retryable": False,
                "request_id": "22222222-2222-4222-8222-222222222222",
                "details": {},
            },
        )
    )

    async_transport = HttpxAsyncTransport(base_url, timeout_s=2.0)
    async_client = AsyncConsumerClient(async_transport, bearer_token=WORKER_BEARER)
    async_claim = (
        await async_client.claim(
            queues=["orders"], worker_id="worker-1", lease_seconds=60
        )
    )[0]
    with pytest.raises(ProtocolError) as async_exc:
        await async_claim.complete()
    assert async_exc.value.code.value == "lease_lost"
    assert async_claim.lease_lost is True
    with pytest.raises(LeaseLostError):
        await async_claim.complete()
    server.recorded.clear()
    server.routes[("POST", f"/v1/claims/{CLAIM_ID}:complete")] = (
        lambda body, headers: (
            200,
            {
                "task_id": TASK_ID,
                "state": "succeeded",
                "spawned_task_ids": [],
                "replayed": False,
            },
        )
    )
    async_claim2 = (
        await async_client.claim(
            queues=["orders"], worker_id="worker-1", lease_seconds=60
        )
    )[0]
    await async_claim2.complete(spawn=[])
    with pytest.raises(TerminalConflictError):
        await async_claim2.complete(spawn=[{"queue_name": "orders", "payload": {}}])
    await async_transport.aclose()

    assert isinstance(sync_claim, Claim)
    assert isinstance(async_claim, AsyncClaim)
