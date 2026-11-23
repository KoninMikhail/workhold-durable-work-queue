"""Async producer and consumer content-key parity with sync clients."""

from __future__ import annotations

import base64
import json
from typing import Any

import pytest

from _workhold_client_core.capabilities import Capabilities
from _workhold_client_core.content_key import ContentDecryptError, seal_task_payload
from _workhold_client_core.errors import MalformedResponseError
from _workhold_client_core.transport import TransportResponse
from workhold_consumer.async_client import AsyncClaim, AsyncConsumerClient
from workhold_producer.async_client import AsyncProducerClient

KEY_BYTES = bytes(range(32))
OTHER_KEY_BYTES = bytes(range(32, 64))
KEY_B64 = base64.b64encode(KEY_BYTES).decode("ascii")
OTHER_KEY_B64 = base64.b64encode(OTHER_KEY_BYTES).decode("ascii")
SENTINEL = "plaintext-sentinel"
PLAINTEXT = {"order_id": 7, "sku": SENTINEL}
TASK_ID = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
QUEUE = "orders"


def _task_wire(**overrides: Any) -> dict[str, Any]:
    body: dict[str, Any] = {
        "task_id": TASK_ID,
        "queue_name": QUEUE,
        "producer_id": "p1",
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


def _assert_key_hidden(*parts: Any) -> None:
    blob = json.dumps(parts, ensure_ascii=False)
    assert KEY_B64 not in blob
    assert SENTINEL not in blob


class _AsyncProducerTransport:
    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []
        self._echo_payload: Any = None

    async def request(
        self,
        method: str,
        path: str,
        *,
        headers: dict[str, str] | None = None,
        json_body: object | None = None,
        **_: Any,
    ) -> TransportResponse:
        self.calls.append(
            {
                "method": method,
                "path": path,
                "headers": dict(headers or {}),
                "json_body": json_body,
            }
        )
        if path == f"/v1/queues/{QUEUE}/tasks":
            wire = (json_body or {}).get("payload")
            self._echo_payload = wire
            return TransportResponse(
                201,
                {"task": _task_wire(payload=wire, state="ready"), "replayed": False},
            )
        if path == f"/v1/queues/{QUEUE}/submissions:resolve":
            wire = self._echo_payload if self._echo_payload is not None else PLAINTEXT
            return TransportResponse(
                200,
                {
                    "task": _task_wire(payload=wire),
                    "dedup_expires_at": "2026-12-18T00:00:00Z",
                    "replayed": False,
                },
            )
        if path == f"/v1/tasks/{TASK_ID}":
            wire = self._echo_payload if self._echo_payload is not None else PLAINTEXT
            return TransportResponse(200, _task_wire(payload=wire))
        if path == f"/v1/tasks/{TASK_ID}:cancel":
            wire = self._echo_payload if self._echo_payload is not None else PLAINTEXT
            return TransportResponse(
                200,
                {"task": _task_wire(payload=wire, state="cancelled")},
            )
        return TransportResponse(404, None)


def _claim_body(payload: Any) -> dict[str, Any]:
    return {
        "tasks": [
            {
                "task": _task_wire(payload=payload, state="leased"),
                "claim": {
                    "claim_id": "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb",
                    "generation": 1,
                    "claimed_at": "2026-09-19T00:01:00Z",
                    "lease_expires_at": "2026-09-19T00:02:00Z",
                    "worker_id": "w1",
                    "cancel_requested": False,
                    "claim_token": "claim-secret",
                },
            }
        ],
        "server_time": "2026-09-19T00:01:00Z",
        "recommended_heartbeat_seconds": 10,
        "queue_states": {QUEUE: "active"},
    }


class _AsyncConsumerTransport:
    def __init__(self, claim_body: dict[str, Any] | None = None) -> None:
        self._claim_body = claim_body or _claim_body(PLAINTEXT)
        self.calls: list[dict[str, Any]] = []
        self.complete_bodies: list[dict[str, Any]] = []

    async def request(
        self,
        method: str,
        path: str,
        *,
        headers: dict[str, str] | None = None,
        json_body: object | None = None,
        **_: Any,
    ) -> TransportResponse:
        self.calls.append(
            {
                "method": method,
                "path": path,
                "headers": dict(headers or {}),
                "json_body": json_body,
            }
        )
        if path == "/v1/claims":
            return TransportResponse(200, self._claim_body)
        if path.endswith(":complete"):
            self.complete_bodies.append(dict(json_body or {}))
            return TransportResponse(
                200,
                {
                    "task_id": TASK_ID,
                    "state": "succeeded",
                    "spawned_task_ids": ["cccccccc-cccc-4ccc-8ccc-cccccccccccc"],
                    "replayed": False,
                },
            )
        return TransportResponse(404, None)


def _caps(**overrides: object) -> Capabilities:
    body: dict[str, object] = {
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
    return Capabilities.parse(body)


async def _claim_one(client: AsyncConsumerClient, **kwargs: Any) -> AsyncClaim:
    claims = await client.claim(
        queues=[QUEUE], worker_id="w1", lease_seconds=30, **kwargs
    )
    assert len(claims) == 1
    return claims[0]


async def test_async_enqueue_without_key_sends_plaintext(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("WORKHOLD_CONTENT_KEY", raising=False)
    transport = _AsyncProducerTransport()
    client = AsyncProducerClient(transport, bearer_token="tok")  # type: ignore[arg-type]

    result = await client.enqueue(
        QUEUE,
        idempotency_key="idem-1",
        payload=PLAINTEXT,
    )

    body = transport.calls[0]["json_body"]
    assert body["payload"] == PLAINTEXT
    assert "$workhold_enc" not in json.dumps(body)
    assert result.task.payload == PLAINTEXT


async def test_async_enqueue_with_key_sends_envelope_and_hides_key() -> None:
    transport = _AsyncProducerTransport()
    client = AsyncProducerClient(
        transport,
        bearer_token="tok",
        content_key=KEY_BYTES,
    )  # type: ignore[arg-type]

    result = await client.enqueue(
        QUEUE,
        idempotency_key="idem-1",
        payload=PLAINTEXT,
    )

    call = transport.calls[0]
    wire = call["json_body"]["payload"]
    assert wire["$workhold_enc"] == 1
    _assert_key_hidden(call["path"], call["headers"], call["json_body"])
    assert result.task.payload == PLAINTEXT


async def test_async_resolve_and_inspect_restore_json() -> None:
    transport = _AsyncProducerTransport()
    client = AsyncProducerClient(
        transport,
        bearer_token="tok",
        content_key=KEY_BYTES,
    )  # type: ignore[arg-type]

    await client.enqueue(QUEUE, idempotency_key="idem-1", payload=PLAINTEXT)
    resolved = await client.resolve_submission(QUEUE, idempotency_key="idem-1")
    inspected = await client.inspect_task(TASK_ID)

    assert resolved.task.payload == PLAINTEXT
    assert inspected.payload == PLAINTEXT


async def test_async_keyed_read_returns_plaintext_unchanged() -> None:
    transport = _AsyncProducerTransport()
    transport._echo_payload = {"order_id": 7}
    client = AsyncProducerClient(
        transport,
        bearer_token="tok",
        content_key=KEY_BYTES,
    )  # type: ignore[arg-type]

    resolved = await client.resolve_submission(QUEUE, idempotency_key="idem-1")
    inspected = await client.inspect_task(TASK_ID)

    assert resolved.task.payload == {"order_id": 7}
    assert inspected.payload == {"order_id": 7}


async def test_async_unkeyed_read_returns_envelope() -> None:
    envelope = seal_task_payload(PLAINTEXT, KEY_BYTES)
    transport = _AsyncProducerTransport()
    transport._echo_payload = envelope
    client = AsyncProducerClient(transport, bearer_token="tok")  # type: ignore[arg-type]

    resolved = await client.resolve_submission(QUEUE, idempotency_key="idem-1")
    inspected = await client.inspect_task(TASK_ID)

    assert resolved.task.payload == envelope
    assert inspected.payload == envelope
    assert "$workhold_enc" in resolved.task.payload


async def test_async_oversize_envelope_rejected_before_http() -> None:
    transport = _AsyncProducerTransport()
    client = AsyncProducerClient(
        transport,
        bearer_token="tok",
        content_key=KEY_BYTES,
    )  # type: ignore[arg-type]

    with pytest.raises(ValueError, match="payload exceeds"):
        await client.enqueue(
            QUEUE,
            idempotency_key="idem-1",
            payload=PLAINTEXT,
            max_payload_bytes=32,
        )

    assert transport.calls == []


async def test_async_enqueue_and_cancel_return_opened_payload() -> None:
    transport = _AsyncProducerTransport()
    client = AsyncProducerClient(
        transport,
        bearer_token="tok",
        content_key=KEY_BYTES,
    )  # type: ignore[arg-type]

    enq = await client.enqueue(
        QUEUE, idempotency_key="idem-1", payload=PLAINTEXT
    )
    cancel = await client.cancel_task(TASK_ID, reason="user")

    assert enq.task.payload == PLAINTEXT
    assert cancel.task.payload == PLAINTEXT
    assert transport.calls[0]["json_body"]["payload"]["$workhold_enc"] == 1


async def test_async_from_url_keeps_content_key(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    producer_transport = _AsyncProducerTransport()
    consumer_transport = _AsyncConsumerTransport(
        _claim_body(seal_task_payload(PLAINTEXT, KEY_BYTES))
    )

    class _FakeHttpx:
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            self._mode = kwargs.pop("_mode", "producer")

        async def request(self, method: str, path: str, **kwargs: Any) -> TransportResponse:
            if self._mode == "producer":
                return await producer_transport.request(method, path, **kwargs)
            return await consumer_transport.request(method, path, **kwargs)

        async def aclose(self) -> None:
            return None

    def _producer_ctor(base_url: str, **kwargs: Any) -> Any:
        return _FakeHttpx(_mode="producer")

    def _consumer_ctor(base_url: str, **kwargs: Any) -> Any:
        return _FakeHttpx(_mode="consumer")

    monkeypatch.setattr(
        "_workhold_client_core.async_transport.HttpxAsyncTransport",
        _producer_ctor,
    )
    producer = AsyncProducerClient.from_url(
        "http://127.0.0.1:9",
        bearer_token="tok",
        content_key=KEY_B64,
    )
    await producer.enqueue(QUEUE, idempotency_key="idem-1", payload=PLAINTEXT)
    assert producer_transport.calls[0]["json_body"]["payload"]["$workhold_enc"] == 1

    monkeypatch.setattr(
        "_workhold_client_core.async_transport.HttpxAsyncTransport",
        _consumer_ctor,
    )
    consumer = AsyncConsumerClient.from_url(
        "http://127.0.0.1:9",
        bearer_token="tok",
        content_key=KEY_B64,
    )
    claim = await _claim_one(consumer)
    assert claim.task.payload == PLAINTEXT


@pytest.mark.parametrize("key", [KEY_BYTES, KEY_B64], ids=["bytes", "base64"])
async def test_async_claim_same_key_restores_json(key: bytes | str) -> None:
    envelope = seal_task_payload(PLAINTEXT, KEY_BYTES)
    transport = _AsyncConsumerTransport(_claim_body(envelope))
    client = AsyncConsumerClient(
        transport,
        bearer_token="tok",
        content_key=key,
    )  # type: ignore[arg-type]

    claim = await _claim_one(client)

    assert claim.task.payload == PLAINTEXT


async def test_async_keyed_client_returns_plaintext_unchanged() -> None:
    transport = _AsyncConsumerTransport(_claim_body({"order_id": 7}))
    client = AsyncConsumerClient(
        transport,
        bearer_token="tok",
        content_key=KEY_BYTES,
    )  # type: ignore[arg-type]

    claim = await _claim_one(client)

    assert claim.task.payload == {"order_id": 7}


async def test_async_unkeyed_client_returns_envelope() -> None:
    envelope = seal_task_payload(PLAINTEXT, KEY_BYTES)
    transport = _AsyncConsumerTransport(_claim_body(envelope))
    client = AsyncConsumerClient(transport, bearer_token="tok")  # type: ignore[arg-type]

    claim = await _claim_one(client)

    assert claim.task.payload == envelope
    assert "$workhold_enc" in claim.task.payload


async def test_async_wrong_key_on_claim_is_content_decrypt_error() -> None:
    envelope = seal_task_payload(PLAINTEXT, KEY_BYTES)
    transport = _AsyncConsumerTransport(_claim_body(envelope))
    client = AsyncConsumerClient(
        transport,
        bearer_token="tok",
        content_key=OTHER_KEY_BYTES,
    )  # type: ignore[arg-type]

    with pytest.raises(ContentDecryptError) as excinfo:
        await _claim_one(client)

    assert not isinstance(excinfo.value, MalformedResponseError)
    assert str(excinfo.value) == "content_decrypt_failed"
    for text in (str(excinfo.value), repr(excinfo.value), repr(client)):
        assert OTHER_KEY_B64 not in text
        assert KEY_B64 not in text
        assert SENTINEL not in text


async def test_async_keyed_complete_leaves_spawn_and_events_plaintext() -> None:
    envelope = seal_task_payload(PLAINTEXT, KEY_BYTES)
    transport = _AsyncConsumerTransport(_claim_body(envelope))
    client = AsyncConsumerClient(
        transport,
        bearer_token="tok",
        content_key=KEY_BYTES,
    )  # type: ignore[arg-type]
    claim = await _claim_one(client)

    await claim.complete(
        spawn=[
            {
                "queue_name": QUEUE,
                "idempotency_key": "child-1",
                "payload": {"child": 1},
                "priority": 0,
            }
        ],
        events=[
            {
                "specversion": "1.0",
                "type": "com.example.done",
                "source": "/app",
                "id": "evt-1",
                "data": {"child": 1},
            }
        ],
        capabilities=_caps(delivery_events=True),
    )

    body = transport.complete_bodies[0]
    assert body["spawn"][0]["payload"] == {"child": 1}
    assert body["events"][0]["data"] == {"child": 1}
    assert "$workhold_enc" not in json.dumps(body)


def test_async_empty_content_key_raises_without_leaking() -> None:
    with pytest.raises(ValueError):
        AsyncProducerClient(_AsyncProducerTransport(), bearer_token="tok", content_key="")  # type: ignore[arg-type]
    with pytest.raises(ValueError):
        AsyncConsumerClient(_AsyncConsumerTransport(), bearer_token="tok", content_key="")  # type: ignore[arg-type]


def test_async_repr_does_not_contain_key() -> None:
    producer = AsyncProducerClient(
        _AsyncProducerTransport(),
        bearer_token="tok",
        content_key=KEY_B64,
    )  # type: ignore[arg-type]
    consumer = AsyncConsumerClient(
        _AsyncConsumerTransport(),
        bearer_token="tok",
        content_key=KEY_B64,
    )  # type: ignore[arg-type]

    assert repr(producer) == "AsyncProducerClient(transport=..., bearer_token=<redacted>)"
    assert repr(consumer) == "AsyncConsumerClient(transport=..., bearer_token=<redacted>)"
    assert KEY_B64 not in repr(producer)
    assert KEY_B64 not in repr(consumer)