"""AsyncConsumerClient long-poll parity tests (SDK-17 / API-09).

Mirrors the synchronous ConsumerClient wait_seconds / capability / empty /
timeout / cancellation scenario matrix. Native asyncio.CancelledError must
propagate unchanged; caller-owned transports must not be closed on cancel.
"""

from __future__ import annotations

import asyncio
import json
import threading
from typing import Any

import pytest

from _workhold_client_core.async_transport import HttpxAsyncTransport
from _workhold_client_core.capabilities import Capabilities
from _workhold_client_core.config import ClientConfig
from _workhold_client_core.errors import (
    MalformedResponseError,
    TimeoutError as ClientTimeoutError,
)
from workhold_consumer.async_client import AsyncConsumerClient
from tests.fixtures.claim_long_poll import (
    CLAIM_TOKEN_SENTINEL,
    assert_no_forbidden_diagnostics,
    delayed_empty_claim_responder,
    long_poll_recording_server,  # noqa: F401 — pytest fixture
)


CLAIM_ID = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"
CLAIM_TOKEN = "claim-token-secret-value-do-not-leak"
TASK_ID = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
WORKER_BEARER = "worker-bearer-secret"


@pytest.mark.parametrize("token", [None, "", "   ", "\t", "\n"])
def test_whitespace_bearer_token_rejected(token: str | None) -> None:
    transport = HttpxAsyncTransport("http://127.0.0.1:9", timeout_s=0.1)
    with pytest.raises(ValueError, match="bearer_token"):
        AsyncConsumerClient(transport, bearer_token=token)  # type: ignore[arg-type]


def test_bearer_token_preserved_exactly() -> None:
    transport = HttpxAsyncTransport("http://127.0.0.1:9", timeout_s=0.1)
    exact = " tok en "
    client = AsyncConsumerClient(transport, bearer_token=exact)
    assert client._auth_headers()["Authorization"] == f"Bearer {exact}"


def _claim_response(*, empty: bool = False) -> dict[str, Any]:
    tasks: list[dict[str, Any]] = []
    if not empty:
        tasks.append(
            {
                "task": {
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
                },
                "claim": {
                    "claim_id": CLAIM_ID,
                    "generation": 3,
                    "claimed_at": "2026-09-19T00:01:00Z",
                    "lease_expires_at": "2026-09-19T00:02:00Z",
                    "worker_id": "worker-1",
                    "cancel_requested": False,
                    "claim_token": CLAIM_TOKEN,
                },
            }
        )
    return {
        "tasks": tasks,
        "server_time": "2026-09-19T00:01:00Z",
        "recommended_heartbeat_seconds": 10,
        "queue_states": {"orders": "active"},
    }


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


@pytest.mark.parametrize(
    ("max_tasks", "match"),
    [
        (True, "integer"),
        (1.0, "integer"),
        ("1", "integer"),
        (0, "max_tasks=1"),
        (-1, "max_tasks=1"),
        (2, "max_tasks=1"),
    ],
)
async def test_claim_rejects_invalid_max_tasks_before_network(
    recording_server: Any,
    max_tasks: object,
    match: str,
) -> None:
    pytest.importorskip("httpx")
    server, base_url = recording_server
    caps = _capabilities_body(batch_claim=True, max_claim_tasks=8)
    server.routes[("GET", "/v1/capabilities")] = lambda body, headers: (200, caps)
    server.routes[("POST", "/v1/claims")] = (
        lambda body, headers: (200, _claim_response())
    )
    transport = HttpxAsyncTransport(base_url, timeout_s=2.0)
    client = AsyncConsumerClient(transport, bearer_token=WORKER_BEARER)
    try:
        with pytest.raises(ValueError, match=match):
            await client.claim(
                queues=["orders"],
                worker_id="worker-1",
                lease_seconds=60,
                max_tasks=max_tasks,  # type: ignore[arg-type]
                capabilities=Capabilities.parse(caps),
            )
        assert server.recorded == []
    finally:
        await transport.aclose()


async def test_claim_sends_wait_seconds_when_capability_allows(
    recording_server: Any,
) -> None:
    pytest.importorskip("httpx")
    server, base_url = recording_server
    caps = _capabilities_body(long_polling=True, max_wait_seconds=20)
    server.routes[("GET", "/v1/capabilities")] = lambda body, headers: (200, caps)
    server.routes[("POST", "/v1/claims")] = (
        lambda body, headers: (200, _claim_response(empty=True))
    )
    transport = HttpxAsyncTransport(
        base_url,
        config=ClientConfig.for_public(
            base_url, read_timeout_s=30.0, total_timeout_s=30.0
        ),
    )
    client = AsyncConsumerClient(transport, bearer_token=WORKER_BEARER)
    try:
        claims = await client.claim(
            queues=["orders"],
            worker_id="worker-1",
            lease_seconds=60,
            wait_seconds=15,
            capabilities=Capabilities.parse(caps),
        )
        assert claims == []
        body = json.loads(server.recorded[-1]["body"].decode("utf-8"))
        assert body["wait_seconds"] == 15
        assert body["max_tasks"] == 1
    finally:
        await transport.aclose()


async def test_positive_wait_fails_closed_without_capability(
    recording_server: Any,
) -> None:
    pytest.importorskip("httpx")
    server, base_url = recording_server
    caps = _capabilities_body(long_polling=False, max_wait_seconds=0)
    transport = HttpxAsyncTransport(base_url, timeout_s=2.0)
    client = AsyncConsumerClient(transport, bearer_token=WORKER_BEARER)
    try:
        with pytest.raises(ValueError, match="long_polling"):
            await client.claim(
                queues=["orders"],
                worker_id="worker-1",
                lease_seconds=60,
                wait_seconds=15,
                capabilities=Capabilities.parse(caps),
            )
        assert server.recorded == []
    finally:
        await transport.aclose()


async def test_undersized_transport_budget_fails_before_http(
    recording_server: Any,
) -> None:
    pytest.importorskip("httpx")
    server, base_url = recording_server
    caps = _capabilities_body(long_polling=True, max_wait_seconds=20)
    transport = HttpxAsyncTransport(
        base_url,
        config=ClientConfig.for_public(
            base_url, read_timeout_s=10.0, total_timeout_s=10.0
        ),
    )
    client = AsyncConsumerClient(transport, bearer_token=WORKER_BEARER)
    try:
        with pytest.raises(ValueError, match="read_timeout_s"):
            await client.claim(
                queues=["orders"],
                worker_id="worker-1",
                lease_seconds=60,
                wait_seconds=15,
                capabilities=Capabilities.parse(caps),
            )
        assert server.recorded == []
    finally:
        await transport.aclose()


async def test_empty_long_poll_expiry_is_success_not_timeout(
    long_poll_recording_server: Any,
) -> None:
    pytest.importorskip("httpx")
    release = threading.Event()
    long_poll_recording_server.set_route(
        "POST",
        "/v1/claims",
        delayed_empty_claim_responder(delay_seconds=2.0, release=release),
    )
    caps = _capabilities_body(long_polling=True, max_wait_seconds=20)
    transport = HttpxAsyncTransport(
        long_poll_recording_server.base_url,
        config=ClientConfig.for_public(
            long_poll_recording_server.base_url,
            read_timeout_s=30.0,
            total_timeout_s=30.0,
        ),
    )
    client = AsyncConsumerClient(transport, bearer_token=WORKER_BEARER)
    try:
        release.set()
        claims = await client.claim(
            queues=["orders"],
            worker_id="worker-1",
            lease_seconds=60,
            wait_seconds=5,
            capabilities=Capabilities.parse(caps),
        )
        assert claims == []
    finally:
        await transport.aclose()


async def test_transport_timeout_distinct_from_empty(
    long_poll_recording_server: Any,
) -> None:
    pytest.importorskip("httpx")
    hold = threading.Event()
    long_poll_recording_server.set_route(
        "POST",
        "/v1/claims",
        delayed_empty_claim_responder(delay_seconds=30.0, release=hold),
    )
    transport = HttpxAsyncTransport(
        long_poll_recording_server.base_url, timeout_s=0.2
    )
    client = AsyncConsumerClient(transport, bearer_token=WORKER_BEARER)
    try:
        with pytest.raises(ClientTimeoutError) as exc_info:
            await client.claim(
                queues=["orders"], worker_id="worker-1", lease_seconds=60
            )
        assert "TimeoutError" in repr(exc_info.value)
        assert WORKER_BEARER not in repr(exc_info.value)
    finally:
        hold.set()
        await transport.aclose()


async def test_surplus_tasks_raise_malformed_response(
    long_poll_recording_server: Any,
) -> None:
    payload = _claim_response()
    payload["tasks"].append(dict(payload["tasks"][0]))
    long_poll_recording_server.set_route(
        "POST", "/v1/claims", lambda body, headers: (200, payload)
    )
    transport = HttpxAsyncTransport(long_poll_recording_server.base_url, timeout_s=2.0)
    client = AsyncConsumerClient(transport, bearer_token=WORKER_BEARER)
    try:
        with pytest.raises(MalformedResponseError, match="returned 2 tasks"):
            await client.claim(
                queues=["orders"], worker_id="worker-1", lease_seconds=60
            )
    finally:
        await transport.aclose()


async def test_invalid_queue_states_raise_malformed_response(
    long_poll_recording_server: Any,
) -> None:
    payload = _claim_response()
    payload["queue_states"] = []
    long_poll_recording_server.set_route(
        "POST", "/v1/claims", lambda body, headers: (200, payload)
    )
    transport = HttpxAsyncTransport(long_poll_recording_server.base_url, timeout_s=2.0)
    client = AsyncConsumerClient(transport, bearer_token=WORKER_BEARER)
    try:
        with pytest.raises(MalformedResponseError, match="queue_states must be an object"):
            await client.claim(
                queues=["orders"], worker_id="worker-1", lease_seconds=60
            )
    finally:
        await transport.aclose()


async def test_async_cancellation_propagates_and_leaves_caller_transport(
    long_poll_recording_server: Any,
) -> None:
    """asyncio.CancelledError must not wrap and must not aclose caller transport."""

    pytest.importorskip("httpx")
    hold = threading.Event()
    entered = threading.Event()

    def responder(body: bytes, headers: dict[str, str]):
        entered.set()
        return delayed_empty_claim_responder(delay_seconds=30.0, release=hold)(
            body, headers
        )

    long_poll_recording_server.set_route("POST", "/v1/claims", responder)
    caps = _capabilities_body(long_polling=True, max_wait_seconds=20)
    config = ClientConfig.for_public(
        long_poll_recording_server.base_url,
        read_timeout_s=30.0,
        total_timeout_s=30.0,
    )
    transport = HttpxAsyncTransport(
        long_poll_recording_server.base_url, config=config
    )
    closed = False
    original_aclose = transport.aclose

    async def _tracking_aclose() -> None:
        nonlocal closed
        closed = True
        await original_aclose()

    transport.aclose = _tracking_aclose  # type: ignore[method-assign]
    client = AsyncConsumerClient(
        transport, bearer_token=WORKER_BEARER, owns_transport=False
    )

    task = asyncio.create_task(
        client.claim(
            queues=["orders"],
            worker_id="worker-1",
            lease_seconds=60,
            wait_seconds=15,
            capabilities=Capabilities.parse(caps),
        )
    )
    deadline = asyncio.get_running_loop().time() + 2.0
    while not entered.is_set() and asyncio.get_running_loop().time() < deadline:
        await asyncio.sleep(0.01)
    assert entered.is_set()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert closed is False
    assert_no_forbidden_diagnostics("CancelledError")
    assert CLAIM_TOKEN_SENTINEL not in "CancelledError"
    hold.set()
    await original_aclose()


@pytest.mark.asyncio
async def test_async_event_cancellation_aborts_mid_long_poll(
    long_poll_recording_server: Any,
) -> None:
    """asyncio.Event cancellation during claim long-poll aborts promptly."""

    pytest.importorskip("httpx")
    hold = threading.Event()
    entered = threading.Event()

    def responder(body: bytes, headers: dict[str, str]):
        entered.set()
        return delayed_empty_claim_responder(delay_seconds=30.0, release=hold)(
            body, headers
        )

    long_poll_recording_server.set_route("POST", "/v1/claims", responder)
    caps = _capabilities_body(long_polling=True, max_wait_seconds=20)
    config = ClientConfig.for_public(
        long_poll_recording_server.base_url,
        read_timeout_s=30.0,
        total_timeout_s=30.0,
    )
    transport = HttpxAsyncTransport(
        long_poll_recording_server.base_url, config=config
    )
    client = AsyncConsumerClient(transport, bearer_token=WORKER_BEARER)
    cancel = asyncio.Event()

    task = asyncio.create_task(
        client.claim(
            queues=["orders"],
            worker_id="worker-1",
            lease_seconds=60,
            wait_seconds=15,
            capabilities=Capabilities.parse(caps),
            cancellation=cancel,
        )
    )
    deadline = asyncio.get_running_loop().time() + 2.0
    while not entered.is_set() and asyncio.get_running_loop().time() < deadline:
        await asyncio.sleep(0.01)
    assert entered.is_set()
    cancel.set()
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(task, timeout=1.0)
    hold.set()
    await transport.aclose()


def _fake_async_claim_then_complete_transport(
    *,
    complete_bodies: list[dict[str, Any]],
) -> Any:
    """Minimal async transport: one claim grant, then record complete bodies."""

    class _Transport:
        async def request(self, method: str, path: str, **kwargs: Any) -> Any:
            if path == "/v1/claims":

                class _ClaimResp:
                    status_code = 200
                    body = _claim_response()

                return _ClaimResp()
            if path.endswith(":complete"):
                complete_bodies.append(kwargs["json_body"])

                class _CompleteResp:
                    status_code = 200
                    body = {
                        "task_id": TASK_ID,
                        "state": "succeeded",
                        "spawned_task_ids": [],
                        "replayed": False,
                    }

                return _CompleteResp()
            raise AssertionError(f"unexpected path {path}")

        async def aclose(self) -> None:
            return None

    return _Transport()


async def test_complete_explicit_max_payload_bytes_rejects_oversize_without_encoder() -> None:
    from _workhold_client_core.codecs import measure_json_bytes

    complete_bodies: list[dict[str, Any]] = []
    client = AsyncConsumerClient(
        _fake_async_claim_then_complete_transport(complete_bodies=complete_bodies),
        bearer_token=WORKER_BEARER,
    )
    claim = (
        await client.claim(queues=["orders"], worker_id="worker-1", lease_seconds=60)
    )[0]
    payload = {"blob": "x" * 64}
    tiny = measure_json_bytes(payload) - 1
    with pytest.raises(ValueError, match="payload exceeds"):
        await claim.complete(
            spawn=[
                {
                    "queue_name": "orders",
                    "idempotency_key": "spawn-oversized",
                    "payload": payload,
                    "priority": 0,
                }
            ],
            max_payload_bytes=tiny,
        )
    assert complete_bodies == []


async def test_complete_explicit_max_payload_bytes_keeps_wire_when_within_limit() -> None:
    complete_bodies: list[dict[str, Any]] = []
    client = AsyncConsumerClient(
        _fake_async_claim_then_complete_transport(complete_bodies=complete_bodies),
        bearer_token=WORKER_BEARER,
    )
    claim = (
        await client.claim(queues=["orders"], worker_id="worker-1", lease_seconds=60)
    )[0]
    payload = {"n": 1, "tag": "ok"}
    await claim.complete(
        spawn=[
            {
                "queue_name": "orders",
                "idempotency_key": "spawn-ok",
                "payload": payload,
                "priority": 0,
            }
        ],
        max_payload_bytes=1024,
    )
    assert complete_bodies[0]["spawn"][0]["payload"] is payload
    assert complete_bodies[0]["spawn"][0]["payload"] == {"n": 1, "tag": "ok"}
