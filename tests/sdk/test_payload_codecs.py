"""SDK-14: opt-in typed payload codecs with opaque wire preservation."""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

import pytest

from _queue_service_client_core.codecs import (
    PayloadDecodeError,
    TypedClaimView,
    TypedTaskView,
    decode_payload,
    encode_payload,
    measure_json_bytes,
)
from _queue_service_client_core.models import Task, TaskState
from queue_service_consumer import ConsumerClient
from queue_service_producer import ProducerClient


@dataclass(frozen=True, slots=True)
class Order:
    order_id: int


class OrderCodec:
    """Example application codec: dataclass <-> JSON object."""

    codec_name = "order-v1"
    value_type = "Order"

    def encode(self, value: Order) -> dict[str, int]:
        return {"order_id": value.order_id}

    def decode(self, raw: object) -> Order:
        if not isinstance(raw, dict) or "order_id" not in raw:
            raise ValueError("expected order object")
        order_id = raw["order_id"]
        if not isinstance(order_id, int) or isinstance(order_id, bool):
            raise ValueError("order_id must be int")
        return Order(order_id=order_id)


class ExplodingDecoder:
    codec_name = "boom"
    value_type = "Secret"

    def decode(self, raw: object) -> object:
        raise RuntimeError(f"secret-leak:{raw!r}")


def test_raw_json_default_round_trips_unchanged() -> None:
    payload = {"order_id": 7, "nested": [1, True, None]}
    assert encode_payload(payload) is payload
    assert decode_payload(payload) is payload


def test_encoder_produces_wire_json_before_size_check() -> None:
    codec = OrderCodec()
    wire = encode_payload(Order(order_id=42), codec, max_bytes=1024)
    assert wire == {"order_id": 42}
    assert decode_payload(wire, codec) == Order(order_id=42)


def test_encoded_value_still_enforces_payload_size() -> None:
    codec = OrderCodec()
    tiny = measure_json_bytes({"order_id": 1}) - 1
    with pytest.raises(ValueError, match="payload exceeds"):
        encode_payload(Order(order_id=1), codec, max_bytes=tiny)


def test_decode_failure_hides_payload_but_exposes_raw() -> None:
    raw = {"secret": "do-not-log-me", "token": "abc"}
    with pytest.raises(PayloadDecodeError) as exc_info:
        decode_payload(raw, ExplodingDecoder())
    err = exc_info.value
    text = f"{err!s}{err!r}"
    assert "do-not-log-me" not in text
    assert "secret" not in text
    assert "abc" not in text
    assert err.codec_name == "boom"
    assert err.value_type == "Secret"
    assert err.raw is raw


def test_typed_task_view_preserves_exact_wire_payload() -> None:
    raw_payload: dict[str, Any] = {"order_id": 9}
    task = Task(
        task_id="aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
        queue_name="orders",
        producer_id="p1",
        state=TaskState.parse("leased"),
        priority=0,
        available_at="2026-09-19T00:00:00Z",
        retry_policy_version=1,
        created_at="2026-09-19T00:00:00Z",
        spawned_task_ids=(),
        delivery_event_ids=(),
        payload=raw_payload,
    )
    view = TypedTaskView.from_task(task, OrderCodec())
    assert view.payload == Order(order_id=9)
    assert view.raw_payload is raw_payload
    assert view.task is task


def test_producer_enqueue_uses_encoder_before_http(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    recorded: list[object] = []

    class _Transport:
        def request(self, method: str, path: str, **kwargs: Any) -> Any:
            recorded.append(kwargs["json_body"]["payload"])
            response_body = {
                "task": {
                    "task_id": "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
                    "queue_name": "orders",
                    "producer_id": "p1",
                    "state": "ready",
                    "priority": 0,
                    "available_at": "2026-09-19T00:00:00Z",
                    "retry_policy_version": 1,
                    "created_at": "2026-09-19T00:00:00Z",
                    "spawned_task_ids": [],
                    "delivery_event_ids": [],
                    "payload": kwargs["json_body"]["payload"],
                },
                "replayed": False,
            }

            class _Resp:
                status_code = 200
                body = response_body

            return _Resp()

    client = ProducerClient(_Transport(), bearer_token="tok")  # type: ignore[arg-type]
    client.enqueue(
        "orders",
        idempotency_key="idem-1",
        payload=Order(order_id=3),
        payload_encoder=OrderCodec(),
        max_payload_bytes=1024,
    )
    assert recorded == [{"order_id": 3}]


def test_consumer_claim_decoder_and_spawn_encoder(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    claim_body = {
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
                    "payload": {"order_id": 11},
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
    complete_bodies: list[dict[str, Any]] = []

    class _Transport:
        def request(self, method: str, path: str, **kwargs: Any) -> Any:
            if path == "/v1/claims":
                class _ClaimResp:
                    status_code = 200
                    body = claim_body

                return _ClaimResp()
            complete_bodies.append(kwargs["json_body"])

            class _CompleteResp:
                status_code = 200
                body = {
                    "task_id": "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
                    "state": "succeeded",
                    "spawned_task_ids": ["cccccccc-cccc-4ccc-8ccc-cccccccccccc"],
                    "replayed": False,
                }

            return _CompleteResp()

    client = ConsumerClient(_Transport(), bearer_token="tok")  # type: ignore[arg-type]
    claims = client.claim(
        queues=["orders"],
        worker_id="w1",
        lease_seconds=30,
        payload_decoder=OrderCodec(),
    )
    assert len(claims) == 1
    typed = TypedClaimView.from_claim(claims[0])
    assert typed.payload == Order(order_id=11)
    assert typed.raw_payload == {"order_id": 11}

    claims[0].complete(
        spawn=[
            {
                "queue_name": "orders",
                "idempotency_key": "child-1",
                "payload": Order(order_id=99),
                "priority": 0,
            }
        ],
        spawn_encoder=OrderCodec(),
        max_payload_bytes=1024,
    )
    assert complete_bodies[0]["spawn"][0]["payload"] == {"order_id": 99}


def test_measure_json_bytes_matches_compact_utf8() -> None:
    value = {"z": 1, "a": "я"}
    expected = len(
        json.dumps(
            value,
            ensure_ascii=False,
            separators=(",", ":"),
            allow_nan=False,
            sort_keys=True,
        ).encode("utf-8")
    )
    assert measure_json_bytes(value) == expected
