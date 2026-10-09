"""OPS-08 bulk admin correlation and redaction (REC-02)."""

from __future__ import annotations

import logging
from unittest.mock import MagicMock

import pytest

from queue_service.observability import context as obs_context
from queue_service.operations import bulk as bulk_ops

_DENIED_KEYS = (
    "payload",
    "claim_token",
    "dsn",
    "sql",
    "idempotency_key",
    "reason",
    "confirmation_token",
    "filters",
    "sample_task_ids",
    "candidate_ids",
    "outcomes",
)


def test_bulk_correlation_allowlists_aggregates_and_redacts_secrets() -> None:
    projected = bulk_ops.project_bulk_admin_correlation(
        operation="executeBulkReplay",
        request_id="req-1",
        trace_id="trace-1",
        actor_id="admin-1",
        queue="orders",
        result="succeeded",
        code=None,
        candidate_count=12,
        batch_size=25,
        succeeded_count=10,
        skipped_count=1,
        failed_count=1,
        extras={
            "payload": {"body": True},
            "claim_token": "secret-token",
            "idempotency_key": "idem-1",
            "reason": "fixed poison handler",
            "confirmation_token": "hmac.secret",
            "filters": {"from": "2026-01-01T00:00:00Z", "search": "nope"},
            "sample_task_ids": ["11111111-1111-1111-1111-111111111111"],
            "candidate_ids": ["22222222-2222-2222-2222-222222222222"],
            "outcomes": [{"task_id": "t", "outcome": "succeeded"}],
            "failure_detail": "SQLSTATE 23505",
            "dsn": "postgresql://u:p@h/db",
            "sql": "UPDATE tasks_terminal SET state_code=10",
        },
    )
    assert projected["request_id"] == "req-1"
    assert projected["actor_id"] == "admin-1"
    assert projected["operation"] == "executeBulkReplay"
    assert projected["queue"] == "orders"
    assert projected["candidate_count"] == 12
    assert projected["batch_size"] == 25
    assert projected["succeeded_count"] == 10
    assert projected["skipped_count"] == 1
    assert projected["failed_count"] == 1
    assert projected["result"] == "succeeded"
    for denied in _DENIED_KEYS:
        assert denied not in projected
    blob = str(projected)
    assert "secret-token" not in blob
    assert "hmac.secret" not in blob
    assert "fixed poison" not in blob
    assert "11111111" not in blob
    assert "UPDATE" not in blob


def test_bulk_projector_uses_shared_plan01_allowlist() -> None:
    for key in (
        "candidate_count",
        "batch_size",
        "succeeded_count",
        "skipped_count",
        "failed_count",
        "actor_id",
    ):
        assert key in obs_context.CORRELATION_ALLOWLIST
    raw = {
        "request_id": "r",
        "trace_id": "t",
        "actor_id": "a",
        "operation": "previewBulkCancel",
        "queue": "q",
        "result": "previewed",
        "code": None,
        "candidate_count": 3,
        "batch_size": None,
        "succeeded_count": None,
        "skipped_count": None,
        "failed_count": None,
        "payload": "nope",
        "confirmation_token": "nope",
        "filters": {"search": "x"},
        "reason": "nope",
        "sql": "SELECT 1",
    }
    via_plan01 = obs_context.project_correlation(raw)
    via_bulk = bulk_ops.project_bulk_admin_correlation(
        operation="previewBulkCancel",
        request_id="r",
        trace_id="t",
        actor_id="a",
        queue="q",
        result="previewed",
        code=None,
        candidate_count=3,
        extras={
            "payload": "nope",
            "confirmation_token": "nope",
            "filters": {"search": "x"},
            "reason": "nope",
            "sql": "SELECT 1",
        },
    )
    assert via_bulk == via_plan01
    assert set(via_bulk) <= obs_context.CORRELATION_ALLOWLIST
    assert "task_id" not in via_bulk  # no per-task high-cardinality labels


def test_emit_bulk_correlation_logs_and_spans(
    caplog: pytest.LogCaptureFixture,
) -> None:
    logger = logging.getLogger("queue_service.tests.bulk_admin_correlation")
    logger.handlers.clear()
    logger.propagate = True
    logger.setLevel(logging.INFO)
    span = MagicMock()
    with caplog.at_level(logging.INFO, logger=logger.name):
        bulk_ops.emit_bulk_admin_correlation(
            logger,
            operation="executeBulkCancel",
            request_id="req-e",
            trace_id="tr-e",
            actor_id="admin-e",
            queue="orders",
            result="partial",
            code=None,
            candidate_count=40,
            batch_size=25,
            succeeded_count=20,
            skipped_count=3,
            failed_count=2,
            extras={
                "payload": {"x": 1},
                "claim_token": "secret-token",
                "confirmation_token": "confirm-secret",
                "idempotency_key": "idem-secret",
                "reason": "should-not-appear",
                "filters": {"from": "x"},
                "outcomes": ["tid-1", "tid-2"],
            },
            span=span,
        )

    joined = " ".join(record.getMessage() for record in caplog.records)
    assert "bulk_admin" in joined
    assert "actor_id" in joined
    assert "admin-e" in joined
    assert "candidate_count" in joined
    assert "batch_size" in joined
    assert "succeeded_count" in joined
    for denied in _DENIED_KEYS:
        assert denied not in joined
    assert "secret-token" not in joined
    assert "confirm-secret" not in joined
    assert "should-not-appear" not in joined
    assert "tid-1" not in joined

    set_calls = {call.args[0]: call.args[1] for call in span.set_attribute.call_args_list}
    assert set_calls["actor_id"] == "admin-e"
    assert set_calls["candidate_count"] == 40
    assert set_calls["batch_size"] == 25
    assert set_calls["succeeded_count"] == 20
    for denied in _DENIED_KEYS:
        assert denied not in set_calls
