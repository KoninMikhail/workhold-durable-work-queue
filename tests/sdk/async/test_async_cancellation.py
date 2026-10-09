"""Async cancellation must not close caller-owned transport or send mutations."""

from __future__ import annotations

import asyncio
from collections.abc import Mapping
from typing import Any

import pytest

from _workhold_client_core.models import Task
from _workhold_client_core.transport import TransportResponse
from workhold_consumer.async_client import AsyncClaim, AsyncConsumerClient

CLAIM_ID = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"
TASK_ID = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"


class SlowAsyncTransport:
    """Blocks until released; records whether aclose was invoked."""

    def __init__(self) -> None:
        self.closed = False
        self.request_count = 0
        self._release = asyncio.Event()

    async def aclose(self) -> None:
        self.closed = True

    def release(self) -> None:
        self._release.set()

    async def request(
        self,
        method: str,
        path: str,
        *,
        headers: Mapping[str, str] | None = None,
        json_body: object | None = None,
        expect_body: bool = True,
        read_timeout_s: float | None = None,
        total_timeout_s: float | None = None,
        cancellation: object | None = None,
    ) -> TransportResponse:
        _ = (read_timeout_s, total_timeout_s, cancellation)
        self.request_count += 1
        await self._release.wait()
        if path == "/v1/claims" and method == "POST":
            return TransportResponse(
                status_code=200,
                headers={},
                body={
                    "tasks": [
                        {
                            "task": {
                                "task_id": TASK_ID,
                                "queue_name": "orders",
                                "producer_id": "p",
                                "state": "leased",
                                "priority": 0,
                                "available_at": "2026-09-19T00:00:00Z",
                                "retry_policy_version": 1,
                                "created_at": "2026-09-19T00:00:00Z",
                                "spawned_task_ids": [],
                                "delivery_event_ids": [],
                            },
                            "claim": {
                                "claim_id": CLAIM_ID,
                                "generation": 1,
                                "claimed_at": "2026-09-19T00:01:00Z",
                                "lease_expires_at": "2026-09-19T00:02:00Z",
                                "worker_id": "w",
                                "cancel_requested": False,
                                "claim_token": "tok",
                            },
                        }
                    ],
                    "server_time": "2026-09-19T00:01:00Z",
                    "recommended_heartbeat_seconds": 10,
                    "queue_states": {"orders": "active"},
                },
                raw_body=b"{}",
            )
        raise AssertionError(f"unexpected request after cancellation: {method} {path}")


async def test_cancelled_claim_does_not_close_injected_transport() -> None:
    transport = SlowAsyncTransport()
    client = AsyncConsumerClient(
        transport, bearer_token="worker-token", owns_transport=False
    )

    task = asyncio.create_task(
        client.claim(queues=["orders"], worker_id="w", lease_seconds=60)
    )
    await asyncio.sleep(0.05)
    task.cancel()
    transport.release()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert transport.closed is False
    assert transport.request_count == 1


async def test_cancelled_terminal_mutation_not_sent() -> None:
    transport = SlowAsyncTransport()
    client = AsyncConsumerClient(
        transport, bearer_token="worker-token", owns_transport=False
    )
    task_model = Task.parse(
        {
            "task_id": TASK_ID,
            "queue_name": "orders",
            "producer_id": "p",
            "state": "leased",
            "priority": 0,
            "available_at": "2026-09-19T00:00:00Z",
            "retry_policy_version": 1,
            "created_at": "2026-09-19T00:00:00Z",
            "spawned_task_ids": [],
            "delivery_event_ids": [],
        }
    )
    claim = AsyncClaim(
        client,
        task=task_model,
        claim_id=CLAIM_ID,
        generation=1,
        claimed_at="t",
        lease_expires_at="t",
        worker_id="w",
        cancel_requested=False,
        claim_token="tok",
        server_time="t",
        recommended_heartbeat_seconds=10,
    )

    transport._release.clear()
    complete_task = asyncio.create_task(claim.complete(spawn=[]))
    await asyncio.sleep(0.05)
    complete_task.cancel()
    transport.release()
    with pytest.raises(asyncio.CancelledError):
        await complete_task

    assert transport.closed is False
    assert claim.is_terminal is False
