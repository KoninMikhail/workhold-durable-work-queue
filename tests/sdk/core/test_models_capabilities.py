"""Tolerant shared wire models and capability parsing."""

from __future__ import annotations

import pytest

from _queue_service_client_core.capabilities import Capabilities
from _queue_service_client_core.models import (
    ErrorCode,
    ProtocolErrorBody,
    Task,
    TaskState,
)


def _task_body(**overrides: object) -> dict[str, object]:
    body: dict[str, object] = {
        "task_id": "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
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


def _capabilities_body(**overrides: object) -> dict[str, object]:
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
    return body


def test_unknown_task_state_and_additive_fields_tolerated() -> None:
    task = Task.parse(
        _task_body(state="awaiting_approval", future_flag=True, nested={"ok": 1})
    )
    assert isinstance(task.state, TaskState)
    assert task.state.value == "awaiting_approval"
    assert task.state.is_unknown is True
    assert task.extra["future_flag"] is True
    assert task.extra["nested"] == {"ok": 1}


def test_protocol_error_rejects_bool_retry_after_ms() -> None:
    with pytest.raises(ValueError, match="retry_after_ms"):
        ProtocolErrorBody.parse(
            {
                "code": "internal_error",
                "message": "slow down",
                "retryable": True,
                "request_id": "22222222-2222-4222-8222-222222222222",
                "details": {},
                "retry_after_ms": True,
            }
        )


def test_unknown_error_code_preserved() -> None:
    body = ProtocolErrorBody.parse(
        {
            "code": "future_admission_denied",
            "message": "new",
            "retryable": True,
            "request_id": "22222222-2222-4222-8222-222222222222",
            "details": {"hint": 1},
            "retry_after_ms": 250,
        }
    )
    assert isinstance(body.code, ErrorCode)
    assert body.code.value == "future_admission_denied"
    assert body.code.is_unknown is True
    assert body.retryable is True
    assert body.retry_after_ms == 250


def test_task_missing_required_field_rejected() -> None:
    raw = _task_body()
    del raw["task_id"]
    with pytest.raises(ValueError, match="missing required"):
        Task.parse(raw)


def test_capabilities_parse_openapi_required_and_tolerates_extra() -> None:
    caps = Capabilities.parse(
        _capabilities_body(batch_claim=False, future_gate=True, durable_idempotent_enqueue=True)
    )
    assert caps.protocol_major == 1
    assert caps.protocol_version == "1.0"
    assert caps.schema_revision == "0001"
    assert caps.batch_claim is False
    assert caps.long_polling is False
    assert caps.max_claim_tasks == 1
    assert caps.extra["future_gate"] is True
    assert caps.extra["durable_idempotent_enqueue"] is True


def test_capabilities_reject_malformed() -> None:
    with pytest.raises(ValueError, match="capabilities"):
        Capabilities.parse("nope")
    raw = _capabilities_body()
    del raw["protocol_major"]
    with pytest.raises(ValueError, match="missing"):
        Capabilities.parse(raw)
