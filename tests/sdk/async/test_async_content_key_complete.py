"""Async complete seals spawn payload with the consumer content key."""

from __future__ import annotations

import base64
import json
from typing import Any

import pytest

from _workhold_client_core.codecs import measure_json_bytes
from _workhold_client_core.content_key import ContentDecryptError, is_content_envelope
from workhold_consumer.async_client import AsyncConsumerClient

KEY = b"\x11" * 32
WRONG_KEY = b"\x22" * 32
PLAINTEXT = {"sku": "spawn-plain-enc09"}
BEARER = "worker-bearer"
TASK_ID = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"


def _claim_response(payload: Any) -> dict[str, Any]:
    return {
        "tasks": [
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
                    "payload": payload,
                },
                "claim": {
                    "claim_id": "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb",
                    "generation": 1,
                    "claimed_at": "2026-09-19T00:01:00Z",
                    "lease_expires_at": "2026-09-19T00:02:00Z",
                    "worker_id": "worker-1",
                    "cancel_requested": False,
                    "claim_token": "claim-token",
                },
            }
        ],
        "server_time": "2026-09-19T00:01:00Z",
        "recommended_heartbeat_seconds": 10,
        "queue_states": {"orders": "active"},
    }


class _Response:
    def __init__(self, body: Any) -> None:
        self.status_code = 200
        self.body = body


class _Transport:
    """One claim grant, then record complete bodies. Later claims echo a payload."""

    def __init__(
        self,
        complete_bodies: list[dict[str, Any]],
        *,
        claim_payload: Any | None = None,
    ) -> None:
        self._complete_bodies = complete_bodies
        self._claim_payload: Any = {"seed": True} if claim_payload is None else claim_payload

    async def request(self, method: str, path: str, **kwargs: Any) -> _Response:
        if path == "/v1/claims":
            return _Response(_claim_response(self._claim_payload))
        if path.endswith(":complete"):
            self._complete_bodies.append(kwargs["json_body"])
            return _Response(
                {
                    "task_id": TASK_ID,
                    "state": "succeeded",
                    "spawned_task_ids": [],
                    "replayed": False,
                }
            )
        raise AssertionError(f"unexpected path {path}")

    async def aclose(self) -> None:
        return None


def _spawn_item(payload: Any) -> dict[str, Any]:
    return {
        "queue_name": "orders",
        "idempotency_key": "spawn-enc09",
        "payload": payload,
        "priority": 4,
        "available_at": "2026-11-23T00:00:00Z",
    }


async def _complete(
    item: dict[str, Any],
    complete_bodies: list[dict[str, Any]],
    *,
    content_key: bytes | None = None,
    max_payload_bytes: int | None = None,
) -> None:
    kwargs: dict[str, Any] = {}
    if content_key is not None:
        kwargs["content_key"] = content_key
    client = AsyncConsumerClient(
        _Transport(complete_bodies),
        bearer_token=BEARER,
        **kwargs,
    )
    claim = (
        await client.claim(queues=["orders"], worker_id="worker-1", lease_seconds=60)
    )[0]
    await claim.complete(spawn=[item], max_payload_bytes=max_payload_bytes)


async def _claim_with(payload: Any, *, content_key: bytes | None = None) -> Any:
    kwargs: dict[str, Any] = {}
    if content_key is not None:
        kwargs["content_key"] = content_key
    client = AsyncConsumerClient(
        _Transport([], claim_payload=payload),
        bearer_token=BEARER,
        **kwargs,
    )
    return (
        await client.claim(queues=["orders"], worker_id="worker-1", lease_seconds=60)
    )[0]


def _assert_identity(wire: dict[str, Any], item: dict[str, Any]) -> None:
    assert wire["queue_name"] == item["queue_name"]
    assert wire["idempotency_key"] == item["idempotency_key"]
    assert wire["priority"] == item["priority"]
    assert wire["available_at"] == item["available_at"]


async def test_spawn_payload_plaintext_without_key() -> None:
    payload = {"sku": "spawn-plain-enc09"}
    item = _spawn_item(payload)
    complete_bodies: list[dict[str, Any]] = []

    await _complete(item, complete_bodies)

    wire = complete_bodies[0]["spawn"][0]
    assert wire["payload"] is payload
    _assert_identity(wire, item)
    assert item["payload"] is payload


async def test_spawn_payload_round_trip() -> None:
    first_payload = {"sku": "spawn-plain-enc09"}
    second_payload = {"sku": "spawn-plain-enc09"}
    first_item = _spawn_item(first_payload)
    second_item = _spawn_item(second_payload)
    first_bodies: list[dict[str, Any]] = []
    second_bodies: list[dict[str, Any]] = []

    await _complete(first_item, first_bodies, content_key=KEY)
    await _complete(second_item, second_bodies, content_key=KEY)

    recorded = first_bodies[0]["spawn"][0]["payload"]
    assert is_content_envelope(recorded)
    body_text = json.dumps(first_bodies[0])
    assert base64.b64encode(KEY).decode("ascii") not in body_text
    assert "spawn-plain-enc09" not in body_text
    assert recorded == second_bodies[0]["spawn"][0]["payload"]
    _assert_identity(first_bodies[0]["spawn"][0], first_item)
    assert first_item["payload"] is first_payload

    restored = await _claim_with(recorded, content_key=KEY)
    assert restored.task.payload == PLAINTEXT


async def test_spawn_payload_wrong_key() -> None:
    bodies: list[dict[str, Any]] = []
    await _complete(_spawn_item({"sku": "spawn-plain-enc09"}), bodies, content_key=KEY)
    envelope = bodies[0]["spawn"][0]["payload"]

    with pytest.raises(ContentDecryptError) as excinfo:
        await _claim_with(envelope, content_key=WRONG_KEY)

    err = excinfo.value
    text = f"{err!s}{err!r}"
    assert str(err) == "content_decrypt_failed"
    assert "spawn-plain-enc09" not in text
    assert base64.b64encode(KEY).decode("ascii") not in text
    assert base64.b64encode(WRONG_KEY).decode("ascii") not in text
    assert str(KEY) not in text
    assert str(WRONG_KEY) not in text


async def test_spawn_payload_passthrough() -> None:
    plain = {"sku": "kept-plaintext"}
    assert not is_content_envelope(plain)
    keyed = await _claim_with(plain, content_key=KEY)
    assert keyed.task.payload == plain

    bodies: list[dict[str, Any]] = []
    await _complete(_spawn_item({"sku": "spawn-plain-enc09"}), bodies, content_key=KEY)
    envelope = bodies[0]["spawn"][0]["payload"]
    unkeyed = await _claim_with(envelope)
    assert is_content_envelope(unkeyed.task.payload)
    assert unkeyed.task.payload["$workhold_enc"] == 1
    assert unkeyed.task.payload == envelope


async def test_spawn_envelope_oversize_rejected_before_http() -> None:
    payload = {"sku": "spawn-plain-enc09"}
    item = _spawn_item(payload)
    complete_bodies: list[dict[str, Any]] = []
    limit = measure_json_bytes(payload)

    with pytest.raises(ValueError, match="payload exceeds"):
        await _complete(item, complete_bodies, content_key=KEY, max_payload_bytes=limit)

    assert complete_bodies == []
