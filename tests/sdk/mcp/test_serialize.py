"""MCP wire JSON for observer results (MCP-04, MCP-06)."""

from __future__ import annotations

import json

from _workhold_client_core.models import ErrorCode, Task, TaskState
from workhold_mcp.serialize import canonical_json, to_tool_payload

_PAYLOAD_CANARY = "WHMCP-PAYLOAD-CANARY"


def test_task_payload_and_extra_survive() -> None:
    payload = {"marker": _PAYLOAD_CANARY}
    task = Task.parse(
        {
            "task_id": "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
            "queue_name": "orders",
            "producer_id": "producer-1",
            "state": "ready",
            "priority": 0,
            "available_at": "2026-09-19T00:00:00Z",
            "retry_policy_version": 1,
            "created_at": "2026-09-19T00:00:00Z",
            "spawned_task_ids": ["bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"],
            "delivery_event_ids": [],
            "payload": payload,
            "current_claim": {
                "claim_id": "cccccccc-cccc-4ccc-8ccc-cccccccccccc",
                "generation": 2,
                "claimed_at": "2026-09-19T00:01:00Z",
                "lease_expires_at": "2026-09-19T00:06:00Z",
                "worker_id": "worker-1",
                "cancel_requested": False,
                "claim_note": "nested",
            },
            "future_flag": "kept",
        }
    )
    projected = to_tool_payload(task)
    assert projected["state"] == "ready"
    assert projected["future_flag"] == "kept"
    assert "extra" not in projected
    assert projected["spawned_task_ids"] == ["bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"]
    assert isinstance(projected["spawned_task_ids"], list)
    assert projected["payload"] == task.payload
    assert projected["payload"] == {"marker": _PAYLOAD_CANARY}
    claim = projected["current_claim"]
    assert isinstance(claim, dict)
    assert claim["claim_id"] == "cccccccc-cccc-4ccc-8ccc-cccccccccccc"
    assert claim["claim_note"] == "nested"
    assert "extra" not in claim


def test_canonical_json_round_trip() -> None:
    payload = {"b": 1, "a": {"d": True, "c": "café"}}
    text = canonical_json(payload)
    assert json.loads(text) == payload
    assert " " not in text
    assert text.index('"a"') < text.index('"b"')
    assert text.index('"c"') < text.index('"d"')
    assert text == '{"a":{"c":"café","d":true},"b":1}'


def test_single_value_enum_is_a_string() -> None:
    assert to_tool_payload(TaskState.parse("ready")) == "ready"
    assert to_tool_payload(ErrorCode.parse("dependency_unavailable")) == (
        "dependency_unavailable"
    )
