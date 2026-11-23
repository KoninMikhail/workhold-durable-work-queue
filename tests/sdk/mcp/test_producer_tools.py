"""Producer MCP tools delegate to ProducerClient (recording transport)."""

from __future__ import annotations

import base64
import json
from typing import Any

import pytest

from _workhold_client_core.content_key import ContentDecryptError, seal_task_payload
from _workhold_client_core.priority import PRIORITY_DEFAULT
from workhold_client_testing import (
    ScriptedSyncTransport,
    capabilities_body,
    enqueue_response_body,
    ok,
    task_body,
)
from workhold_mcp.errors import project_error
from workhold_mcp.registry import REGISTRY_BY_NAME
from workhold_producer.client import ProducerClient

_PRODUCER_TOOL_NAMES = (
    "producer_cancel_task",
    "producer_enqueue",
    "producer_get_capabilities",
    "producer_inspect_task",
    "producer_resolve_submission",
)

_QUEUE_NAME = "orders"
_TASK_ID = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
_PRODUCER_TOKEN = "producer-secret-token"
_PLAINTEXT = {"order_id": 7, "sku": "plaintext-sentinel"}
_KEY_BYTES = bytes(range(32))
_KEY_B64 = base64.b64encode(_KEY_BYTES).decode("ascii")
_DECRYPT_CANARY = "content-decrypt-canary-reason"


def _producer(steps: list[Any], **kwargs: Any) -> tuple[ProducerClient, ScriptedSyncTransport]:
    transport = ScriptedSyncTransport(steps)
    client = ProducerClient(
        transport,
        bearer_token=_PRODUCER_TOKEN,
        **kwargs,
    )
    return client, transport


def test_producer_names_join_the_registry() -> None:
    for name in _PRODUCER_TOOL_NAMES:
        assert name in REGISTRY_BY_NAME
    for excluded in (
        "consumer_enqueue",
        "worker_claim",
        "break_glass_force_lease_expiry",
        "producer__enqueue_with_available_at_raw",
    ):
        assert excluded not in REGISTRY_BY_NAME


def test_enqueue_forwards_caller_idempotency_key() -> None:
    idem = "caller-idempotency-key-1"
    producer, transport = _producer([ok(enqueue_response_body())])
    REGISTRY_BY_NAME["producer_enqueue"].call(
        producer,
        _QUEUE_NAME,
        idempotency_key=idem,
        payload=_PLAINTEXT,
    )
    assert len(transport.calls) == 1
    call = transport.calls[0]
    assert call.method == "POST"
    assert _QUEUE_NAME in call.path
    headers = call.headers(include_secrets=True)
    assert headers["Authorization"] == f"Bearer {_PRODUCER_TOKEN}"
    assert headers["Idempotency-Key"] == idem


def test_resolve_forwards_queue_and_idempotency_key() -> None:
    idem = "resolve-idem-key"
    body = {
        "task": task_body(payload=_PLAINTEXT),
        "dedup_expires_at": "2026-12-18T00:00:00Z",
    }
    producer, transport = _producer([ok(body)])
    REGISTRY_BY_NAME["producer_resolve_submission"].call(
        producer,
        _QUEUE_NAME,
        idempotency_key=idem,
    )
    assert len(transport.calls) == 1
    call = transport.calls[0]
    assert _QUEUE_NAME in call.path
    assert call.json_body(include_secrets=True) == {"idempotency_key": idem}

    producer, transport = _producer([ok(body)])
    with pytest.raises(ValueError):
        REGISTRY_BY_NAME["producer_resolve_submission"].call(
            producer,
            _QUEUE_NAME,
            idempotency_key=None,
        )
    assert transport.calls == []


def test_inspect_returns_opened_payload() -> None:
    producer, transport = _producer([ok(task_body(payload=_PLAINTEXT))])
    schema = REGISTRY_BY_NAME["producer_inspect_task"].input_schema
    assert "content_key" not in schema.get("properties", {})
    result = REGISTRY_BY_NAME["producer_inspect_task"].call(producer, _TASK_ID)
    assert len(transport.calls) == 1
    assert _TASK_ID in transport.calls[0].path
    assert result.payload == _PLAINTEXT


def test_omitted_idempotency_key_does_not_call_http() -> None:
    producer, transport = _producer([ok(enqueue_response_body())])
    with pytest.raises(ValueError):
        REGISTRY_BY_NAME["producer_enqueue"].call(
            producer,
            _QUEUE_NAME,
            idempotency_key=None,
            payload=_PLAINTEXT,
        )
    assert transport.calls == []
    projected = project_error(ValueError("idempotency_key is required"))
    assert projected["kind"] == "invalid_argument"
    blob = json.dumps(projected)
    assert "idempotency_key is required" not in blob


def test_cancel_omits_absent_reason() -> None:
    cancel_body = {"task": task_body(state="cancelled")}
    producer, transport = _producer([ok(cancel_body)])
    REGISTRY_BY_NAME["producer_cancel_task"].call(producer, _TASK_ID)
    assert transport.calls[0].json_body() == {}

    producer, transport = _producer([ok(cancel_body)])
    REGISTRY_BY_NAME["producer_cancel_task"].call(
        producer,
        _TASK_ID,
        reason="user_aborted",
    )
    assert transport.calls[0].json_body() == {"reason": "user_aborted"}

    producer, transport = _producer([ok(cancel_body)])
    REGISTRY_BY_NAME["producer_cancel_task"].call(producer, _TASK_ID, reason=None)
    assert transport.calls[0].json_body() == {}


def test_hidden_enqueue_parameters_are_absent() -> None:
    schema = REGISTRY_BY_NAME["producer_enqueue"].input_schema
    props = set(schema.get("properties", {}))
    for hidden in (
        "payload_encoder",
        "capabilities",
        "content_key",
        "content_sealed",
        "max_payload_bytes",
    ):
        assert hidden not in props


def test_content_key_envelope_round_trip() -> None:
    sealed = seal_task_payload(_PLAINTEXT, _KEY_BYTES)
    producer, transport = _producer(
        [ok({"task": task_body(payload=sealed), "replayed": False})],
        content_key=_KEY_B64,
    )
    result = REGISTRY_BY_NAME["producer_enqueue"].call(
        producer,
        _QUEUE_NAME,
        idempotency_key="idem-envelope",
        payload=_PLAINTEXT,
    )
    wire = transport.calls[0].json_body(include_secrets=True)
    assert isinstance(wire, dict)
    payload = wire["payload"]
    assert isinstance(payload, dict)
    assert payload.get("$workhold_enc") == 1
    assert "n" in payload and "c" in payload
    assert result.task.payload == _PLAINTEXT


def test_content_decrypt_error_omits_text() -> None:
    exc = ContentDecryptError(raw={"$workhold_enc": 1}, reason=_DECRYPT_CANARY)
    projected = project_error(exc)
    assert projected["kind"] == "content_decrypt"
    assert projected["code"] is None
    assert projected["retryable"] is False
    blob = json.dumps(projected)
    assert _DECRYPT_CANARY not in blob