"""OPS-08 dead-letter admin correlation and redaction (CTRL-06 / REC-01)."""

from __future__ import annotations

import logging
from unittest.mock import MagicMock

import pytest

from queue_service.observability import context as obs_context
from queue_service.operations import dead_letter as dlq_ops

_DENIED_KEYS = ("payload", "claim_token", "dsn", "sql", "idempotency_key", "reason")


def test_dead_letter_correlation_allowlists_lineage_and_redacts_secrets() -> None:
    projected = dlq_ops.project_dead_letter_admin_correlation(
        operation="replayDeadLetter",
        request_id="req-1",
        trace_id="trace-1",
        actor_id="admin-1",
        queue="orders",
        task_id="11111111-1111-1111-1111-111111111111",
        source_task_id="22222222-2222-2222-2222-222222222222",
        policy_version=3,
        result="succeeded",
        code=None,
        extras={
            "payload": {"body": True},
            "claim_token": "secret-token",
            "idempotency_key": "idem-1",
            "reason": "fixed poison handler",
            "failure_detail": "SQLSTATE 23505",
            "dsn": "postgresql://u:p@h/db",
            "sql": "UPDATE tasks_terminal SET state_code=10",
        },
    )
    assert projected["request_id"] == "req-1"
    assert projected["actor_id"] == "admin-1"
    assert projected["operation"] == "replayDeadLetter"
    assert projected["queue"] == "orders"
    assert projected["task_id"] == "11111111-1111-1111-1111-111111111111"
    assert projected["source_task_id"] == "22222222-2222-2222-2222-222222222222"
    assert projected["policy_version"] == 3
    assert projected["result"] == "succeeded"
    for denied in _DENIED_KEYS:
        assert denied not in projected
    blob = str(projected)
    assert "secret-token" not in blob
    assert "fixed poison" not in blob
    assert "UPDATE" not in blob


def test_dead_letter_projector_uses_shared_plan01_allowlist() -> None:
    assert "source_task_id" in obs_context.CORRELATION_ALLOWLIST
    assert "actor_id" in obs_context.CORRELATION_ALLOWLIST
    raw = {
        "request_id": "r",
        "trace_id": "t",
        "actor_id": "a",
        "operation": "replayDeadLetter",
        "queue": "q",
        "task_id": "tid",
        "source_task_id": "sid",
        "policy_version": 1,
        "result": "denied",
        "code": "permission_denied",
        "payload": "nope",
        "claim_token": "nope",
        "reason": "nope",
        "sql": "SELECT 1",
    }
    via_plan01 = obs_context.project_correlation(raw)
    via_dlq = dlq_ops.project_dead_letter_admin_correlation(
        operation="replayDeadLetter",
        request_id="r",
        trace_id="t",
        actor_id="a",
        queue="q",
        task_id="tid",
        source_task_id="sid",
        policy_version=1,
        result="denied",
        code="permission_denied",
        extras={"payload": "nope", "claim_token": "nope", "reason": "nope", "sql": "SELECT 1"},
    )
    assert via_dlq == via_plan01
    assert set(via_dlq) <= obs_context.CORRELATION_ALLOWLIST


def test_emit_dead_letter_correlation_logs_and_spans(
    caplog: pytest.LogCaptureFixture,
) -> None:
    logger = logging.getLogger("queue_service.tests.dead_letter_admin_correlation")
    logger.handlers.clear()
    logger.propagate = True
    logger.setLevel(logging.INFO)
    span = MagicMock()
    with caplog.at_level(logging.INFO, logger=logger.name):
        dlq_ops.emit_dead_letter_admin_correlation(
            logger,
            operation="replayDeadLetter",
            request_id="req-e",
            trace_id="tr-e",
            actor_id="admin-e",
            queue="orders",
            task_id="11111111-1111-1111-1111-111111111111",
            source_task_id="22222222-2222-2222-2222-222222222222",
            policy_version=2,
            result="succeeded",
            code=None,
            extras={
                "payload": {"x": 1},
                "claim_token": "secret-token",
                "idempotency_key": "idem-secret",
                "reason": "should-not-appear",
            },
            span=span,
        )

    joined = " ".join(record.getMessage() for record in caplog.records)
    assert "dead_letter_admin" in joined
    assert "actor_id" in joined
    assert "admin-e" in joined
    assert "source_task_id" in joined
    assert "22222222-2222-2222-2222-222222222222" in joined
    assert "task_id" in joined
    assert "11111111-1111-1111-1111-111111111111" in joined
    assert "policy_version" in joined
    for denied in _DENIED_KEYS:
        assert denied not in joined
    assert "secret-token" not in joined
    assert "should-not-appear" not in joined

    set_calls = {call.args[0]: call.args[1] for call in span.set_attribute.call_args_list}
    assert set_calls["actor_id"] == "admin-e"
    assert set_calls["source_task_id"] == "22222222-2222-2222-2222-222222222222"
    assert set_calls["task_id"] == "11111111-1111-1111-1111-111111111111"
    assert set_calls["policy_version"] == 2
    for denied in _DENIED_KEYS:
        assert denied not in set_calls
