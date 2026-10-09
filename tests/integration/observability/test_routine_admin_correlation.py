"""OPS-08 routine admin correlation and redaction for drain/maintenance."""

from __future__ import annotations

import logging
from unittest.mock import MagicMock

import pytest

from queue_service.observability import context as obs_context
from queue_service.operations import routine as routine_ops

_DENIED_KEYS = ("payload", "claim_token", "dsn", "sql")


def test_routine_admin_correlation_allowlists_actor_and_run_id() -> None:
    projected = routine_ops.project_routine_admin_correlation(
        operation="runMaintenance",
        request_id="req-1",
        trace_id="trace-1",
        actor_id="admin-1",
        queue="orders",
        config_version=3,
        maintenance_run_id="11111111-1111-1111-1111-111111111111",
        result="succeeded",
        code=None,
        extras={
            "payload": {"body": True},
            "claim_token": "secret-token",
            "idempotency_key": "idem-1",
            "failure_detail": "SQLSTATE 23505",
            "error_detail": "password=x",
            "dsn": "postgresql://u:p@h/db",
            "database_url": "postgresql://u:p@h/db",
            "sql": "DETACH PARTITION x",
            "partition_name": "tasks_terminal_20260101",
        },
    )
    assert projected["request_id"] == "req-1"
    assert projected["trace_id"] == "trace-1"
    assert projected["actor_id"] == "admin-1"
    assert projected["operation"] == "runMaintenance"
    assert projected["queue"] == "orders"
    assert projected["config_version"] == 3
    assert projected["maintenance_run_id"] == "11111111-1111-1111-1111-111111111111"
    assert projected["result"] == "succeeded"
    for denied in (
        "payload",
        "claim_token",
        "idempotency_key",
        "failure_detail",
        "error_detail",
        "dsn",
        "database_url",
        "sql",
        "partition_name",
    ):
        assert denied not in projected
    blob = str(projected)
    assert "secret-token" not in blob
    assert "password" not in blob
    assert "DETACH" not in blob


def test_routine_projector_uses_shared_plan01_allowlist() -> None:
    assert "actor_id" in obs_context.CORRELATION_ALLOWLIST
    assert "maintenance_run_id" in obs_context.CORRELATION_ALLOWLIST
    raw = {
        "request_id": "r",
        "trace_id": "t",
        "actor_id": "a",
        "operation": "setQueueState",
        "queue": "q",
        "config_version": 1,
        "maintenance_run_id": None,
        "result": "denied",
        "code": "permission_denied",
        "payload": "nope",
        "claim_token": "nope",
        "sql": "SELECT 1",
    }
    via_plan01 = obs_context.project_correlation(raw)
    via_routine = routine_ops.project_routine_admin_correlation(
        operation="setQueueState",
        request_id="r",
        trace_id="t",
        actor_id="a",
        queue="q",
        config_version=1,
        maintenance_run_id=None,
        result="denied",
        code="permission_denied",
        extras={"payload": "nope", "claim_token": "nope", "sql": "SELECT 1"},
    )
    assert via_routine == via_plan01
    assert set(via_routine) <= obs_context.CORRELATION_ALLOWLIST


def test_denial_and_lock_loser_outcomes_are_projected() -> None:
    for result, code in (
        ("denied", "permission_denied"),
        ("conflict", "config_version_conflict"),
        ("skipped_lock", "lock_held"),
    ):
        projected = routine_ops.project_routine_admin_correlation(
            operation="runMaintenance",
            request_id="req",
            trace_id="tr",
            actor_id="admin",
            queue=None,
            config_version=None,
            maintenance_run_id=None,
            result=result,
            code=code,
            extras={"claim_token": "x", "dsn": "y"},
        )
        assert projected["result"] == result
        assert projected["code"] == code
        assert "claim_token" not in projected
        assert "dsn" not in projected


def test_emit_correlation_writes_allowlisted_fields_to_logs(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """OPS-08: projected correlation must appear in structured log records."""
    logger = logging.getLogger("queue_service.observability.context.emit_test")
    projected = routine_ops.project_routine_admin_correlation(
        operation="runMaintenance",
        request_id="req-emit",
        trace_id="tr-emit",
        actor_id="admin-emit",
        queue=None,
        config_version=None,
        maintenance_run_id="11111111-1111-1111-1111-111111111111",
        result="succeeded",
        code=None,
        extras={
            "payload": {"leak": True},
            "claim_token": "secret-token",
            "dsn": "postgresql://u:p@h/db",
            "sql": "DETACH PARTITION x",
        },
    )
    with caplog.at_level(logging.INFO, logger=logger.name):
        obs_context.emit_correlation(
            logger,
            "routine_admin",
            projected,
        )

    assert any("routine_admin" in record.getMessage() for record in caplog.records)
    joined = " ".join(record.getMessage() for record in caplog.records)
    assert "actor_id" in joined
    assert "admin-emit" in joined
    assert "maintenance_run_id" in joined
    assert "11111111-1111-1111-1111-111111111111" in joined
    assert "operation" in joined
    assert "runMaintenance" in joined
    assert "result" in joined
    assert "succeeded" in joined
    for denied in _DENIED_KEYS:
        assert denied not in joined
    assert "secret-token" not in joined
    assert "DETACH" not in joined


def test_emit_correlation_sets_span_attributes_when_span_present() -> None:
    """OPS-08: same projected dict is attached to trace span attributes."""
    logger = logging.getLogger("queue_service.observability.context.span_test")
    logger.addHandler(logging.NullHandler())
    span = MagicMock()
    projected = routine_ops.project_routine_admin_correlation(
        operation="runMaintenance",
        request_id="req-emit",
        trace_id="tr-emit",
        actor_id="admin-emit",
        queue="orders",
        config_version=None,
        maintenance_run_id="11111111-1111-1111-1111-111111111111",
        result="succeeded",
        code=None,
        extras={"payload": "x", "claim_token": "y", "dsn": "z", "sql": "SELECT 1"},
    )
    obs_context.emit_correlation(
        logger,
        "routine_admin",
        projected,
        span=span,
    )
    set_calls = {call.args[0]: call.args[1] for call in span.set_attribute.call_args_list}
    assert set_calls["actor_id"] == "admin-emit"
    assert set_calls["operation"] == "runMaintenance"
    assert set_calls["result"] == "succeeded"
    assert set_calls["maintenance_run_id"] == (
        "11111111-1111-1111-1111-111111111111"
    )
    assert set_calls["queue"] == "orders"
    for denied in _DENIED_KEYS:
        assert denied not in set_calls


def test_handler_emit_helpers_project_and_log_outcomes(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Drain/maintenance outcome helper must emit allowlisted keys for all results."""
    logger = logging.getLogger(
        "queue_service.tests.routine_admin_correlation.emit_outcomes"
    )
    logger.handlers.clear()
    logger.propagate = True
    logger.setLevel(logging.INFO)
    span = MagicMock()
    outcomes: list[tuple[str, str | None]] = [
        ("success", None),
        ("denied", "permission_denied"),
        ("conflict", "config_version_conflict"),
        ("skipped_lock", "lock_held"),
        ("internal_error", "internal_error"),
    ]
    with caplog.at_level(logging.INFO, logger=logger.name):
        for result, code in outcomes:
            routine_ops.emit_routine_admin_correlation(
                logger,
                operation="runMaintenance",
                request_id="req-h",
                trace_id="tr-h",
                actor_id="admin-h",
                queue=None,
                config_version=None,
                maintenance_run_id=(
                    "22222222-2222-2222-2222-222222222222"
                    if result == "success"
                    else None
                ),
                result=result,
                code=code,
                span=span,
            )

    messages = [
        record.getMessage()
        for record in caplog.records
        if record.name == logger.name
    ]
    assert len(messages) == len(outcomes)
    joined = " ".join(messages)
    assert "actor_id" in joined
    assert "admin-h" in joined
    assert "operation" in joined
    assert "runMaintenance" in joined
    for result, _code in outcomes:
        assert result in joined
    assert "maintenance_run_id" in joined
    for denied in _DENIED_KEYS:
        assert denied not in joined
    assert span.set_attribute.call_count >= 1
