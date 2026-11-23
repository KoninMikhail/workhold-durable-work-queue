"""Sync consumer opens the claimed task payload; spawn and events stay plaintext."""

from __future__ import annotations

import base64
import json
from typing import Any

import pytest

from _workhold_client_core.capabilities import Capabilities
from _workhold_client_core.codecs import TypedClaimView
from _workhold_client_core.content_key import ContentDecryptError, seal_task_payload
from _workhold_client_core.errors import MalformedResponseError
import workhold_consumer
from workhold_consumer import ConsumerClient

KEY_BYTES = bytes(range(32))
OTHER_KEY_BYTES = bytes(range(32, 64))
KEY_B64 = base64.b64encode(KEY_BYTES).decode("ascii")
OTHER_KEY_B64 = base64.b64encode(OTHER_KEY_BYTES).decode("ascii")
SENTINEL = "plaintext-sentinel"
PLAINTEXT = {"order_id": 7, "sku": SENTINEL}


def _claim_body(payload: Any) -> dict[str, Any]:
    return {
        "tasks": [
            {
                "task": {
                    "task_id": "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
                    "queue_name": "orders",
                    "producer_id": "p1",
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
                    "worker_id": "w1",
                    "cancel_requested": False,
                    "claim_token": "claim-secret",
                },
            }
        ],
        "server_time": "2026-09-19T00:01:00Z",
        "recommended_heartbeat_seconds": 10,
        "queue_states": {"orders": "active"},
    }


class _Response:
    status_code = 200

    def __init__(self, body: Any) -> None:
        self.body = body


class _Transport:
    def __init__(self, claim_body: dict[str, Any]) -> None:
        self._claim_body = claim_body
        self.complete_bodies: list[dict[str, Any]] = []

    def request(self, method: str, path: str, **kwargs: Any) -> _Response:
        if path == "/v1/claims":
            return _Response(self._claim_body)
        self.complete_bodies.append(kwargs["json_body"])
        return _Response(
            {
                "task_id": "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
                "state": "succeeded",
                "spawned_task_ids": ["cccccccc-cccc-4ccc-8ccc-cccccccccccc"],
                "replayed": False,
            }
        )


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


def _claim_one(client: ConsumerClient, **kwargs: Any) -> Any:
    return client.claim(queues=["orders"], worker_id="w1", lease_seconds=30, **kwargs)[0]


@pytest.mark.parametrize("key", [KEY_BYTES, KEY_B64], ids=["bytes", "base64"])
def test_claim_same_key_restores_json(key: bytes | str) -> None:
    envelope = seal_task_payload(PLAINTEXT, KEY_BYTES)
    transport = _Transport(_claim_body(envelope))
    client = ConsumerClient(transport, bearer_token="tok", content_key=key)  # type: ignore[arg-type]

    claim = _claim_one(client)

    assert claim.task.payload == PLAINTEXT


def test_keyed_client_returns_plaintext_unchanged() -> None:
    transport = _Transport(_claim_body({"order_id": 7}))
    client = ConsumerClient(transport, bearer_token="tok", content_key=KEY_BYTES)  # type: ignore[arg-type]

    claim = _claim_one(client)

    assert claim.task.payload == {"order_id": 7}


def test_unkeyed_client_returns_envelope() -> None:
    envelope = seal_task_payload(PLAINTEXT, KEY_BYTES)
    transport = _Transport(_claim_body(envelope))
    client = ConsumerClient(transport, bearer_token="tok")  # type: ignore[arg-type]

    claim = _claim_one(client)

    assert claim.task.payload == envelope
    assert "$workhold_enc" in claim.task.payload


def test_wrong_key_on_claim_is_content_decrypt_error() -> None:
    envelope = seal_task_payload(PLAINTEXT, KEY_BYTES)
    transport = _Transport(_claim_body(envelope))
    client = ConsumerClient(transport, bearer_token="tok", content_key=OTHER_KEY_BYTES)  # type: ignore[arg-type]

    with pytest.raises(ContentDecryptError) as excinfo:
        _claim_one(client)

    assert not isinstance(excinfo.value, MalformedResponseError)
    assert str(excinfo.value) == "content_decrypt_failed"
    for text in (str(excinfo.value), repr(excinfo.value), repr(client)):
        assert OTHER_KEY_B64 not in text
        assert KEY_B64 not in text
        assert SENTINEL not in text


def test_decoder_sees_opened_plaintext() -> None:
    seen: list[object] = []

    class _Decoder:
        codec_name = "spy"
        value_type = "dict"

        def decode(self, raw: object) -> object:
            seen.append(raw)
            return raw

    envelope = seal_task_payload({"order_id": 7}, KEY_BYTES)
    transport = _Transport(_claim_body(envelope))
    client = ConsumerClient(transport, bearer_token="tok", content_key=KEY_BYTES)  # type: ignore[arg-type]

    claim = _claim_one(client, payload_decoder=_Decoder())
    TypedClaimView.from_claim(claim)

    assert seen == [{"order_id": 7}]
    assert "$workhold_enc" not in json.dumps(seen)


def test_keyed_complete_leaves_spawn_and_events_plaintext() -> None:
    envelope = seal_task_payload(PLAINTEXT, KEY_BYTES)
    transport = _Transport(_claim_body(envelope))
    client = ConsumerClient(transport, bearer_token="tok", content_key=KEY_BYTES)  # type: ignore[arg-type]
    claim = _claim_one(client)

    claim.complete(
        spawn=[
            {
                "queue_name": "orders",
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


def test_empty_content_key_raises_without_leaking() -> None:
    with pytest.raises(ValueError) as excinfo:
        ConsumerClient(_Transport({}), bearer_token="tok", content_key="")  # type: ignore[arg-type]

    assert KEY_B64 not in str(excinfo.value)


def test_repr_does_not_contain_key() -> None:
    client = ConsumerClient(_Transport({}), bearer_token="tok", content_key=KEY_B64)  # type: ignore[arg-type]

    assert repr(client) == "ConsumerClient(transport=..., bearer_token=<redacted>)"
    assert KEY_B64 not in repr(client)


def test_environment_key_does_not_open_claim(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("WORKHOLD_CONTENT_KEY", KEY_B64)
    envelope = seal_task_payload(PLAINTEXT, KEY_BYTES)
    transport = _Transport(_claim_body(envelope))
    client = ConsumerClient(transport, bearer_token="tok")  # type: ignore[arg-type]

    claim = _claim_one(client)

    assert claim.task.payload == envelope


def test_content_decrypt_error_is_reexported() -> None:
    assert workhold_consumer.ContentDecryptError is ContentDecryptError
    assert "ContentDecryptError" in workhold_consumer.__all__

