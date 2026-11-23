"""Sync producer seals the task payload on enqueue and opens it on reads."""

from __future__ import annotations

import base64
import json
from typing import Any

import pytest

from _workhold_client_core.codecs import measure_json_bytes
from _workhold_client_core.content_key import (
    ContentDecryptError,
    open_task_payload,
    seal_task_payload,
)
from workhold_producer import ProducerClient

KEY_BYTES = bytes(range(32))
KEY_B64 = "AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8="
SENTINEL = "plaintext-sentinel"
PLAINTEXT = {"order_id": 7, "sku": SENTINEL}
TASK_ID = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
_ECHO = object()


def _task_body(payload: Any) -> dict[str, Any]:
    return {
        "task_id": TASK_ID,
        "queue_name": "orders",
        "producer_id": "p1",
        "state": "ready",
        "priority": 0,
        "available_at": "2026-09-19T00:00:00Z",
        "retry_policy_version": 1,
        "created_at": "2026-09-19T00:00:00Z",
        "spawned_task_ids": [],
        "delivery_event_ids": [],
        "payload": payload,
    }


class _Response:
    status_code = 200

    def __init__(self, body: dict[str, Any]) -> None:
        self.body = body


class _Transport:
    """Records path, headers, and JSON body. Echoes the request payload by default."""

    def __init__(self, response_payload: Any = _ECHO) -> None:
        self.response_payload = response_payload
        self.calls: list[dict[str, Any]] = []

    def request(self, method: str, path: str, **kwargs: Any) -> _Response:
        json_body = kwargs.get("json_body")
        self.calls.append(
            {
                "method": method,
                "path": path,
                "headers": kwargs.get("headers"),
                "json_body": json_body,
            }
        )
        if self.response_payload is _ECHO:
            payload = json_body.get("payload") if isinstance(json_body, dict) else None
        else:
            payload = self.response_payload
        task = _task_body(payload)
        if path.endswith("/submissions:resolve"):
            body = {"task": task, "dedup_expires_at": "2026-09-20T00:00:00Z"}
        elif path.endswith(":cancel"):
            body = {"task": task}
        elif path.startswith("/v1/tasks/"):
            body = task
        else:
            body = {"task": task, "replayed": False}
        return _Response(body)


class _Order:
    def __init__(self, order_id: int) -> None:
        self.order_id = order_id


class _OrderEncoder:
    codec_name = "order-v1"

    def encode(self, value: _Order) -> dict[str, int]:
        return {"order_id": value.order_id}


def _client(transport: _Transport, **kwargs: Any) -> ProducerClient:
    return ProducerClient(transport, bearer_token="tok", **kwargs)  # type: ignore[arg-type]


def _wire_text(call: dict[str, Any]) -> str:
    return json.dumps(
        {"path": call["path"], "headers": call["headers"], "body": call["json_body"]},
        sort_keys=True,
    )


def test_enqueue_without_key_sends_plaintext(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("WORKHOLD_CONTENT_KEY", KEY_B64)
    transport = _Transport()
    client = _client(transport)

    result = client.enqueue("orders", idempotency_key="idem-1", payload=PLAINTEXT)

    body = transport.calls[0]["json_body"]
    assert body["payload"] == PLAINTEXT
    assert "$workhold_enc" not in json.dumps(body)
    assert result.task.payload == PLAINTEXT


def test_enqueue_with_key_sends_envelope_and_hides_key() -> None:
    assert base64.b64encode(KEY_BYTES).decode("ascii") == KEY_B64
    transport = _Transport()
    client = _client(transport, content_key=KEY_BYTES)

    result = client.enqueue("orders", idempotency_key="idem-1", payload=dict(PLAINTEXT))

    call = transport.calls[0]
    envelope = call["json_body"]["payload"]
    assert envelope["$workhold_enc"] == 1
    assert open_task_payload(envelope, KEY_BYTES) == PLAINTEXT
    assert result.task.payload == PLAINTEXT
    wire = _wire_text(call)
    assert KEY_B64 not in wire
    assert SENTINEL not in wire


def test_resolve_and_inspect_restore_json() -> None:
    envelope = seal_task_payload(PLAINTEXT, KEY_BYTES)
    transport = _Transport(envelope)
    client = _client(transport, content_key=KEY_BYTES)

    resolved = client.resolve_submission("orders", idempotency_key="idem-1")
    inspected = client.inspect_task(TASK_ID)

    assert resolved.task.payload == PLAINTEXT
    assert inspected.payload == PLAINTEXT


def test_keyed_read_returns_plaintext_unchanged() -> None:
    stored = {"order_id": 7, "sku": "already-json"}
    transport = _Transport(stored)
    client = _client(transport, content_key=KEY_BYTES)

    resolved = client.resolve_submission("orders", idempotency_key="idem-1")
    inspected = client.inspect_task(TASK_ID)

    assert resolved.task.payload == stored
    assert inspected.payload == stored


def test_unkeyed_read_returns_envelope() -> None:
    envelope = seal_task_payload(PLAINTEXT, KEY_BYTES)
    transport = _Transport(envelope)
    client = _client(transport)

    resolved = client.resolve_submission("orders", idempotency_key="idem-1")
    inspected = client.inspect_task(TASK_ID)

    assert resolved.task.payload == envelope
    assert inspected.payload == envelope


def test_oversize_envelope_rejected_before_http() -> None:
    plaintext_size = measure_json_bytes(PLAINTEXT)
    envelope_size = measure_json_bytes(seal_task_payload(PLAINTEXT, KEY_BYTES))
    assert plaintext_size < envelope_size
    transport = _Transport()
    client = _client(transport, content_key=KEY_BYTES)

    with pytest.raises(ValueError, match="payload exceeds"):
        client.enqueue(
            "orders",
            idempotency_key="idem-1",
            payload=dict(PLAINTEXT),
            max_payload_bytes=plaintext_size,
        )

    assert transport.calls == []


def test_enqueue_seals_encoder_output() -> None:
    application = _Order(order_id=3)
    encoded = {"order_id": 3}
    transport = _Transport()
    client = _client(transport, content_key=KEY_BYTES)

    result = client.enqueue(
        "orders",
        idempotency_key="idem-1",
        payload=application,
        payload_encoder=_OrderEncoder(),
        max_payload_bytes=1024,
    )

    envelope = transport.calls[0]["json_body"]["payload"]
    assert open_task_payload(envelope, KEY_BYTES) == encoded
    assert result.task.payload == encoded
    assert not isinstance(result.task.payload, _Order)


def test_enqueue_and_cancel_return_opened_payload() -> None:
    transport = _Transport()
    client = _client(transport, content_key=KEY_BYTES)

    enqueued = client.enqueue("orders", idempotency_key="idem-1", payload=dict(PLAINTEXT))
    envelope = transport.calls[0]["json_body"]["payload"]
    assert envelope["$workhold_enc"] == 1
    transport.response_payload = envelope
    cancelled = client.cancel_task(TASK_ID)

    assert enqueued.task.payload == PLAINTEXT
    assert cancelled.task.payload == PLAINTEXT


def test_empty_content_key_rejected_and_repr_omits_key() -> None:
    supplied = "supplied-key-sentinel"
    with pytest.raises(ValueError) as empty_key:
        _client(_Transport(), content_key="")
    assert supplied not in f"{empty_key.value!s}{empty_key.value!r}"

    with pytest.raises(ValueError) as bad_key:
        _client(_Transport(), content_key=supplied)
    assert supplied not in f"{bad_key.value!s}{bad_key.value!r}"

    client = _client(_Transport(), content_key=KEY_BYTES)
    text = repr(client)
    assert text == "ProducerClient(transport=..., bearer_token=<redacted>)"
    assert KEY_B64 not in text


def test_content_decrypt_error_reexported() -> None:
    import workhold_producer

    assert workhold_producer.ContentDecryptError is ContentDecryptError
    assert "ContentDecryptError" in workhold_producer.__all__
