"""OPS-08 maintenance log/trace correlation and redaction coverage.

Maintenance start/success/failure/lock-loser/detach-drop/registry-purge
diagnostics must share Plan 01 ``project_correlation``; payloads, claim tokens,
DSNs, SQL, partition names, and free-text failure detail are absent.
"""

from __future__ import annotations

from datetime import datetime, timezone

from queue_service.observability import context as obs_context
from queue_service.observability import retention as retention_obs


def test_maintenance_events_project_through_shared_allowlist() -> None:
    store_now = datetime(2026, 9, 19, 12, 0, 0, tzinfo=timezone.utc)
    for event in (
        retention_obs.MaintenanceDiagEvent.START,
        retention_obs.MaintenanceDiagEvent.SUCCESS,
        retention_obs.MaintenanceDiagEvent.FAILURE,
        retention_obs.MaintenanceDiagEvent.LOCK_LOSER,
        retention_obs.MaintenanceDiagEvent.DETACH_DROP,
        retention_obs.MaintenanceDiagEvent.REGISTRY_PURGE,
    ):
        projected = retention_obs.project_maintenance_correlation(
            event=event,
            request_id="req-maint-1",
            trace_id="trace-maint-1",
            process_role="maintain",
            store_now=store_now,
            result="failed" if event is retention_obs.MaintenanceDiagEvent.FAILURE else "success",
            code="partition_horizon_unsafe"
            if event is retention_obs.MaintenanceDiagEvent.FAILURE
            else None,
            extras={
                "payload": {"secret": "body"},
                "claim_token": "tok-leak",
                "idempotency_key": "idem-leak",
                "failure_detail": "SQLSTATE 23505 detail=...",
                "error_detail": "password=s3cret host=db",
                "dsn": "postgresql://user:pass@host/db",
                "database_url": "postgresql://user:pass@host/db",
                "sql": "DETACH PARTITION tasks_terminal_20260101",
                "partition_name": "tasks_terminal_20260101",
                "child_name": "tasks_terminal_20260101",
                "task_id": "should-also-be-ok-if-allowlisted",
            },
        )
        assert projected["request_id"] == "req-maint-1"
        assert projected["trace_id"] == "trace-maint-1"
        assert projected["operation"] == event.value
        assert projected["process_role"] == "maintain"
        assert projected["store_now"] == store_now.isoformat()
        assert "payload" not in projected
        assert "claim_token" not in projected
        assert "idempotency_key" not in projected
        assert "failure_detail" not in projected
        assert "error_detail" not in projected
        assert "dsn" not in projected
        assert "database_url" not in projected
        assert "sql" not in projected
        assert "partition_name" not in projected
        assert "child_name" not in projected
        # task_id remains allowlisted for correlation but maintenance projector
        # must not forward it from extras that look like free-form dumps.
        assert "task_id" not in projected or projected.get("task_id") is None


def test_maintenance_projector_uses_plan01_project_correlation() -> None:
    """Same allowlist surface as Plan 01 — no drift between logs and traces."""
    raw = {
        "request_id": "r1",
        "trace_id": "t1",
        "operation": "maintain.start",
        "process_role": "maintain",
        "store_now": "2026-09-19T12:00:00+00:00",
        "result": "success",
        "code": None,
        "payload": "nope",
        "claim_token": "nope",
        "sql": "SELECT 1",
        "partition_name": "x",
    }
    via_plan01 = obs_context.project_correlation(raw)
    via_maint = retention_obs.project_maintenance_correlation(
        event=retention_obs.MaintenanceDiagEvent.START,
        request_id="r1",
        trace_id="t1",
        process_role="maintain",
        store_now=datetime(2026, 9, 19, 12, 0, 0, tzinfo=timezone.utc),
        result="success",
        code=None,
        extras={
            "payload": "nope",
            "claim_token": "nope",
            "sql": "SELECT 1",
            "partition_name": "x",
        },
    )
    assert via_maint == via_plan01
    assert set(via_maint) <= obs_context.CORRELATION_ALLOWLIST


def test_allowlist_includes_maintenance_role_and_store_timestamp() -> None:
    assert "process_role" in obs_context.CORRELATION_ALLOWLIST
    assert "store_now" in obs_context.CORRELATION_ALLOWLIST
    for denied in (
        "partition_name",
        "child_name",
        "sql",
        "error_detail",
        "dsn",
        "payload",
        "claim_token",
    ):
        assert denied not in obs_context.CORRELATION_ALLOWLIST


def test_detach_drop_and_purge_events_carry_bounded_result_only() -> None:
    projected = retention_obs.project_maintenance_correlation(
        event=retention_obs.MaintenanceDiagEvent.DETACH_DROP,
        request_id="req-2",
        trace_id="tr-2",
        process_role="maintain",
        store_now=datetime(2026, 9, 19, 12, 0, 0, tzinfo=timezone.utc),
        result="success",
        code=None,
        extras={
            "partitions_detached": 3,  # non-scalar / not allowlisted → dropped
            "partitions_dropped": 3,
            "purge_deleted_total": 9,
            "failure_detail": "orphan child tasks_terminal_20260101",
        },
    )
    assert projected["result"] == "success"
    assert projected["operation"] == "maintain.detach_drop"
    assert "partitions_detached" not in projected
    assert "failure_detail" not in projected
    blob = str(projected)
    assert "tasks_terminal" not in blob
