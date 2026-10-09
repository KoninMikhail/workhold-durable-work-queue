"""OPS-08 break-glass admin correlation and redaction (REC-03)."""

from __future__ import annotations

import logging
from unittest.mock import MagicMock

import pytest

from queue_service.observability import context as obs_context
from queue_service.operations import break_glass as bg_ops

_DENIED_KEYS = (
    "payload",
    "claim_token",
    "dsn",
    "sql",
    "reason",
    "incident_reference",
    "repair_value",
    "registry_value",
    "partition_name",
    "credential",
    "credentials",
)


def test_break_glass_correlation_allowlists_and_redacts_secrets() -> None:
    projected = bg_ops.project_break_glass_correlation(
        operation="forceLeaseExpiry",
        request_id="req-1",
        trace_id="trace-1",
        actor_id="break-glass-1",
        queue="orders",
        incident_ref_hash="abcd1234efgh5678",
        target_id="11111111-1111-1111-1111-111111111111",
        result="retry_scheduled",
        code=None,
        generation=3,
        extras={
            "payload": {"body": True},
            "claim_token": "secret-token",
            "reason": "human free text must not leak",
            "incident_reference": "INC-RAW-999",
            "repair_value": "raw-hash-bytes",
            "registry_value": "fingerprint",
            "partition_name": "tasks_terminal_20260101",
            "sql": "DROP TABLE tasks_terminal_20260101",
            "dsn": "postgresql://u:p@h/db",
            "credential": "tok-secret",
            "failure_detail": "SQLSTATE 23505",
        },
    )
    assert projected["request_id"] == "req-1"
    assert projected["trace_id"] == "trace-1"
    assert projected["actor_id"] == "break-glass-1"
    assert projected["operation"] == "forceLeaseExpiry"
    assert projected["queue"] == "orders"
    assert projected["incident_ref_hash"] == "abcd1234efgh5678"
    assert projected["target_id"] == "11111111-1111-1111-1111-111111111111"
    assert projected["generation"] == 3
    assert projected["result"] == "retry_scheduled"
    for denied in _DENIED_KEYS:
        assert denied not in projected
    blob = str(projected)
    assert "secret-token" not in blob
    assert "INC-RAW-999" not in blob
    assert "human free text" not in blob
    assert "DROP TABLE" not in blob
    assert "tok-secret" not in blob
    assert "raw-hash-bytes" not in blob


def test_break_glass_projector_uses_shared_plan01_allowlist() -> None:
    for key in ("incident_ref_hash", "target_id", "actor_id", "generation"):
        assert key in obs_context.CORRELATION_ALLOWLIST
    raw = {
        "request_id": "r",
        "trace_id": "t",
        "actor_id": "a",
        "operation": "repairRegistryEntry",
        "queue": "q",
        "incident_ref_hash": "hash16",
        "target_id": "42",
        "result": "repaired",
        "code": None,
        "payload": "nope",
        "reason": "nope",
        "incident_reference": "INC-1",
        "sql": "SELECT 1",
        "claim_token": "nope",
    }
    via_plan01 = obs_context.project_correlation(raw)
    via_bg = bg_ops.project_break_glass_correlation(
        operation="repairRegistryEntry",
        request_id="r",
        trace_id="t",
        actor_id="a",
        queue="q",
        incident_ref_hash="hash16",
        target_id="42",
        result="repaired",
        code=None,
        extras={
            "payload": "nope",
            "reason": "nope",
            "incident_reference": "INC-1",
            "sql": "SELECT 1",
            "claim_token": "nope",
        },
    )
    assert via_bg == via_plan01
    assert set(via_bg) <= obs_context.CORRELATION_ALLOWLIST


def test_emit_break_glass_correlation_logs_and_spans(
    caplog: pytest.LogCaptureFixture,
) -> None:
    logger = logging.getLogger("queue_service.operations.break_glass.emit_test")
    projected = bg_ops.project_break_glass_correlation(
        operation="raiseReplayLimit",
        request_id="req-emit",
        trace_id="tr-emit",
        actor_id="bg-emit",
        queue="orders",
        incident_ref_hash="deadbeefcafebabe",
        target_id="orders",
        result="raised",
        code=None,
        extras={
            "reason": "raise for incident",
            "claim_token": "secret-token",
            "dsn": "postgresql://u:p@h/db",
        },
    )
    span = MagicMock()
    with caplog.at_level(logging.INFO, logger=logger.name):
        bg_ops.emit_break_glass_correlation(
            logger,
            operation="raiseReplayLimit",
            request_id="req-emit",
            trace_id="tr-emit",
            actor_id="bg-emit",
            queue="orders",
            incident_ref_hash="deadbeefcafebabe",
            target_id="orders",
            result="raised",
            code=None,
            extras={
                "reason": "raise for incident",
                "claim_token": "secret-token",
                "dsn": "postgresql://u:p@h/db",
            },
            span=span,
        )

    joined = " ".join(record.getMessage() for record in caplog.records)
    assert "break_glass_admin" in joined
    assert "deadbeefcafebabe" in joined
    assert "bg-emit" in joined
    assert "secret-token" not in joined
    assert "raise for incident" not in joined
    assert "password" not in joined
    set_calls = {call.args[0]: call.args[1] for call in span.set_attribute.call_args_list}
    assert set_calls["incident_ref_hash"] == "deadbeefcafebabe"
    assert set_calls["actor_id"] == "bg-emit"
    assert "claim_token" not in set_calls
    assert projected["incident_ref_hash"] == "deadbeefcafebabe"
