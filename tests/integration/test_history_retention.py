"""Real-PostgreSQL history partition detach/drop retention (STOR-04, SEC-05).

Proves fully-expired upper-bound detach/drop, 30/90-day boundaries, interruption
recovery, no bulk DELETE, and PayloadRetentionPolicy-driven terminal expiry.
"""

from __future__ import annotations

import ast
import inspect
import uuid
from collections.abc import Iterator, Mapping
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import psycopg
import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Connection, Engine

from workhold.health import DAILY_RANGE_PARENTS
from workhold.infrastructure.postgres import partition_catalog
from workhold.security.payload_policy import (
    PAYLOAD_RETENTION_DAYS_MAX,
    PAYLOAD_RETENTION_DAYS_MIN,
    PayloadRetentionPolicy,
)

# Under test — RED until history_retention exists under infrastructure/postgres.
from workhold.infrastructure.postgres import history_retention

UTC = timezone.utc
HISTORY_PARENTS_WITHOUT_PAYLOAD = (
    "admin_audit_log",
    "task_attempts",
    "delivery_events_terminal",
)

_RETENTION_SRC = Path(inspect.getsourcefile(history_retention) or "")
_CATALOG_SRC = Path(inspect.getsourcefile(partition_catalog) or "")


@pytest.fixture
def retention_schema(test_database_url: str) -> Iterator[tuple[str, str, Engine]]:
    """Fresh migrated schema; yield (schema, url, sqlalchemy engine). Always DROP."""
    from tests.integration.conftest import run_alembic, to_psycopg_conninfo

    schema = f"qit_{uuid.uuid4().hex}"
    admin = psycopg.connect(to_psycopg_conninfo(test_database_url))
    admin.autocommit = True
    try:
        admin.execute(f'CREATE SCHEMA "{schema}"')
    finally:
        admin.close()

    run_alembic("upgrade", "head", schema=schema, database_url=test_database_url)
    engine = create_engine(test_database_url, pool_pre_ping=True)
    try:
        yield schema, test_database_url, engine
    finally:
        engine.dispose()
        drop = psycopg.connect(to_psycopg_conninfo(test_database_url))
        drop.autocommit = True
        try:
            drop.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        finally:
            drop.close()


def _set_search_path(conn: Connection, schema: str) -> None:
    conn.execute(text(f'SET search_path TO "{schema}"'))


def _utc_today(conn: Connection) -> date:
    return conn.execute(
        text("SELECT (CURRENT_TIMESTAMP AT TIME ZONE 'UTC')::date")
    ).scalar_one()


def _store_now(conn: Connection) -> datetime:
    value = conn.execute(text("SELECT CURRENT_TIMESTAMP")).scalar_one()
    assert isinstance(value, datetime)
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value


def _child_attached(conn: Connection, parent: str, child: str) -> bool:
    row = conn.execute(
        text(
            """
            SELECT EXISTS (
              SELECT 1
              FROM pg_inherits i
              JOIN pg_class child ON child.oid = i.inhrelid
              JOIN pg_class parent ON parent.oid = i.inhparent
              JOIN pg_namespace n ON n.oid = parent.relnamespace
              WHERE n.nspname = current_schema()
                AND parent.relname = :parent
                AND child.relname = :child
                AND NOT i.inhdetachpending
            )
            """
        ),
        {"parent": parent, "child": child},
    ).scalar_one()
    return bool(row)


def _relation_exists(conn: Connection, relname: str) -> bool:
    return bool(
        conn.execute(
            text(
                """
                SELECT EXISTS (
                  SELECT 1 FROM pg_class c
                  JOIN pg_namespace n ON n.oid = c.relnamespace
                  WHERE n.nspname = current_schema() AND c.relname = :rel
                )
                """
            ),
            {"rel": relname},
        ).scalar_one()
    )


def _create_past_child(conn: Connection, parent: str, day: date) -> str:
    """Create an allowlisted past-day PARTITION OF child (tests only)."""
    spec = partition_catalog.day_spec_for(parent, day)
    ddl = conn.execute(
        text(
            """
            SELECT format(
                'CREATE TABLE %I PARTITION OF %I FOR VALUES FROM (%L) TO (%L)',
                CAST(:child_name AS text),
                CAST(:parent_name AS text),
                CAST(:bound_from AS timestamptz),
                CAST(:bound_to AS timestamptz)
            )
            """
        ),
        {
            "child_name": spec.child_name,
            "parent_name": parent,
            "bound_from": spec.bound_from,
            "bound_to": spec.bound_to,
        },
    ).scalar_one()
    conn.execute(text(str(ddl)))
    return spec.child_name


def _default_history_days(payload_days: int) -> dict[str, int]:
    return {
        "admin_audit_log": 90,
        "task_attempts": 90,
        "delivery_events_terminal": 30,
        "tasks_terminal": payload_days,
    }


def _run_retention(
    conn: Connection,
    *,
    payload_days: int,
    history_days: Mapping[str, int] | None = None,
):
    policy = PayloadRetentionPolicy(retention_days=payload_days)
    days = dict(history_days) if history_days is not None else _default_history_days(payload_days)
    # Caller must present a connection with no open transaction for CONCURRENTLY.
    if conn.in_transaction():
        conn.commit()
    return history_retention.retain_expired_history(
        conn,
        retention_days_by_parent=days,
        payload_retention_policy=policy,
    )


def _forbidden_ast_calls(module_path: Path) -> list[str]:
    tree = ast.parse(module_path.read_text(encoding="utf-8"))
    forbidden: list[str] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Call):
            name = _call_name(node.func)
            if name in {
                "pg_advisory_lock",
                "pg_try_advisory_lock",
                "pg_advisory_unlock",
                "create_engine",
                "create_role_engine",
            }:
                forbidden.append(name)
        if isinstance(node, ast.Constant) and isinstance(node.value, str):
            lowered = node.value.lower()
            if "pg_advisory_lock" in lowered or "pg_try_advisory_lock" in lowered:
                forbidden.append(node.value)
            if "delete from" in lowered and any(
                parent in lowered for parent in DAILY_RANGE_PARENTS
            ):
                forbidden.append(node.value)
    return forbidden


def _call_name(func: ast.AST) -> str:
    if isinstance(func, ast.Name):
        return func.id
    if isinstance(func, ast.Attribute):
        base = _call_name(func.value)
        return f"{base}.{func.attr}" if base else func.attr
    return ""


def test_modules_forbid_locks_engines_and_bulk_history_delete() -> None:
    assert _RETENTION_SRC.is_file()
    assert _CATALOG_SRC.is_file()
    hits = _forbidden_ast_calls(_RETENTION_SRC)
    assert hits == [], f"history_retention must not lock/engine/bulk-delete: {hits}"
    src = _RETENTION_SRC.read_text(encoding="utf-8")
    assert "create_engine" not in src
    assert "create_role_engine" not in src
    assert "pg_advisory" not in src.lower()
    assert "DELETE FROM" not in src.upper()
    assert "PayloadRetentionPolicy" in src
    assert "DETACH PARTITION" in src.upper()


def test_retention_rejects_days_outside_30_90(
    retention_schema: tuple[str, str, Engine],
) -> None:
    schema, _url, engine = retention_schema
    with engine.connect() as conn:
        _set_search_path(conn, schema)
        conn.commit()
        policy = PayloadRetentionPolicy(retention_days=30)
        with pytest.raises(ValueError):
            history_retention.retain_expired_history(
                conn,
                retention_days_by_parent={
                    **_default_history_days(30),
                    "task_attempts": 29,
                },
                payload_retention_policy=policy,
            )
        with pytest.raises(ValueError):
            history_retention.retain_expired_history(
                conn,
                retention_days_by_parent={
                    **_default_history_days(30),
                    "delivery_events_terminal": 91,
                },
                payload_retention_policy=policy,
            )


def test_only_fully_expired_upper_bounds_are_detached_and_dropped(
    retention_schema: tuple[str, str, Engine],
) -> None:
    schema, _url, engine = retention_schema
    retention_days = 30
    with engine.connect() as conn:
        _set_search_path(conn, schema)
        today = _utc_today(conn)
        expired_day = today - timedelta(days=retention_days + 2)
        boundary_day = today - timedelta(days=retention_days)  # upper bound == cutoff → keep
        partial_day = today - timedelta(days=1)

        parents = ("admin_audit_log", "task_attempts", "tasks_terminal")
        expired_children: dict[str, str] = {}
        boundary_children: dict[str, str] = {}
        for parent in parents:
            expired_children[parent] = _create_past_child(conn, parent, expired_day)
            boundary_children[parent] = _create_past_child(conn, parent, boundary_day)
            _create_past_child(conn, parent, partial_day)
        conn.commit()

        history_days = {
            "admin_audit_log": retention_days,
            "task_attempts": retention_days,
            "delivery_events_terminal": retention_days,
            "tasks_terminal": retention_days,
        }
        result = _run_retention(
            conn, payload_days=retention_days, history_days=history_days
        )

        for parent in parents:
            assert not _relation_exists(conn, expired_children[parent]), parent
            assert _child_attached(conn, parent, boundary_children[parent]), parent
            partial = partition_catalog.child_name_for(parent, partial_day)
            assert _child_attached(conn, parent, partial), parent

        statuses = {
            o.status for o in result.outcomes if o.child_name in expired_children.values()
        }
        assert "dropped" in statuses
        skipped = {
            o.child_name
            for o in result.outcomes
            if o.status == "skipped_not_expired"
        }
        assert boundary_children["tasks_terminal"] in skipped


def test_payload_policy_30_and_90_boundaries_retain_partial_current(
    retention_schema: tuple[str, str, Engine],
) -> None:
    schema, _url, engine = retention_schema
    assert PAYLOAD_RETENTION_DAYS_MIN == 30
    assert PAYLOAD_RETENTION_DAYS_MAX == 90

    for days in (30, 90):
        with engine.connect() as conn:
            _set_search_path(conn, schema)
            today = _utc_today(conn)
            expired_day = today - timedelta(days=days + 1)
            # Upper bound == store_today - days + 1 day → expires exactly at bound_to+days
            # Fully expired only when now >= expires_at(bound_to); boundary partition kept.
            keep_day = today - timedelta(days=days)
            expired = _create_past_child(conn, "tasks_terminal", expired_day)
            keep = _create_past_child(conn, "tasks_terminal", keep_day)
            conn.commit()

            result = _run_retention(conn, payload_days=days)
            assert not _relation_exists(conn, expired)
            assert _child_attached(conn, "tasks_terminal", keep)
            assert any(
                o.child_name == expired and o.status == "dropped" for o in result.outcomes
            )


def _seed_queue_with_live_payload(conn: Connection) -> tuple[int, int]:
    """Return (queue_pk, tasks_active.id) with an opaque active payload row."""
    qname = f"ret-q-{uuid.uuid4().hex[:8]}"
    queue_pk = conn.execute(
        text(
            """
            INSERT INTO queues (queue_id, name)
            VALUES (gen_random_uuid(), :name)
            RETURNING id
            """
        ),
        {"name": qname},
    ).scalar_one()
    policy_id = conn.execute(
        text(
            """
            INSERT INTO queue_policy_versions (
              queue_id, version, enabled, max_attempts,
              backoff_strategy_code, retry_delay_seconds
            ) VALUES (:qid, 1, true, 3, 1, 0)
            RETURNING id
            """
        ),
        {"qid": queue_pk},
    ).scalar_one()
    conn.execute(
        text("UPDATE queues SET active_policy_version_id = :pid WHERE id = :qid"),
        {"pid": policy_id, "qid": queue_pk},
    )
    task_pk = conn.execute(
        text(
            """
            INSERT INTO tasks_active (
              task_id, queue_id, producer_id, state_code, available_at,
              retry_policy_version_id
            ) VALUES (
              gen_random_uuid(), :qid, 'live-producer', 2,
              statement_timestamp(), :pid
            )
            RETURNING id
            """
        ),
        {"qid": queue_pk, "pid": policy_id},
    ).scalar_one()
    conn.execute(
        text(
            """
            INSERT INTO task_payloads_active (task_id, payload, payload_bytes)
            VALUES (:tid, CAST(:payload AS jsonb), 13)
            """
        ),
        {"tid": task_pk, "payload": '{"live": true}'},
    )
    return int(queue_pk), int(task_pk)


def test_tasks_terminal_uses_payload_retention_policy_directly(
    retention_schema: tuple[str, str, Engine],
) -> None:
    schema, _url, engine = retention_schema
    with engine.connect() as conn:
        _set_search_path(conn, schema)
        today = _utc_today(conn)
        queue_pk, task_pk = _seed_queue_with_live_payload(conn)

        expired_day = today - timedelta(days=91)
        child = _create_past_child(conn, "tasks_terminal", expired_day)
        terminal_at = datetime.combine(expired_day, datetime.min.time(), tzinfo=UTC) + timedelta(
            hours=1
        )
        conn.execute(
            text(
                """
                INSERT INTO tasks_terminal (
                  task_id, queue_id, producer_id, state_code, priority,
                  available_at, retry_policy_version, payload, payload_bytes,
                  created_at, terminal_at
                ) VALUES (
                  gen_random_uuid(), :qid, 'done-producer', 10, 0,
                  :ts, 1, CAST(:payload AS jsonb), 13,
                  :ts, :ts
                )
                """
            ),
            {"qid": queue_pk, "ts": terminal_at, "payload": '{"term": true}'},
        )
        now = _store_now(conn)
        conn.execute(
            text(
                """
                INSERT INTO claim_registry (
                  claim_id, task_id, claim_token, generation,
                  claimed_at, lease_expires_at, created_at
                ) VALUES (
                  gen_random_uuid(), gen_random_uuid(), gen_random_uuid(), 1,
                  :claimed, :lease, :claimed
                )
                """
            ),
            {"claimed": now, "lease": now + timedelta(hours=1)},
        )
        conn.commit()

        result = _run_retention(conn, payload_days=90)
        assert not _relation_exists(conn, child)
        assert any(
            o.parent_name == "tasks_terminal" and o.status == "dropped"
            for o in result.outcomes
        )

        live_payload = conn.execute(
            text("SELECT payload FROM task_payloads_active WHERE task_id = :tid"),
            {"tid": task_pk},
        ).scalar_one()
        assert live_payload == {"live": True}
        claims = conn.execute(text("SELECT count(*) FROM claim_registry")).scalar_one()
        assert int(claims) >= 1
        # No orphan: active payload rows still equal live tasks.
        orphans = conn.execute(
            text(
                """
                SELECT count(*) FROM task_payloads_active p
                WHERE NOT EXISTS (
                  SELECT 1 FROM tasks_active t WHERE t.id = p.task_id
                )
                """
            )
        ).scalar_one()
        assert int(orphans) == 0
        src = _RETENTION_SRC.read_text(encoding="utf-8")
        assert ".expires_at(" in src
        assert ".is_expired(" in src


def test_interruption_after_detach_is_resumable_and_idempotent(
    retention_schema: tuple[str, str, Engine],
) -> None:
    schema, _url, engine = retention_schema
    with engine.connect() as conn:
        _set_search_path(conn, schema)
        today = _utc_today(conn)
        expired_day = today - timedelta(days=40)
        child = _create_past_child(conn, "admin_audit_log", expired_day)
        conn.commit()

        # Simulate interruption: detach concurrently, leave table undropped.
        if conn.in_transaction():
            conn.commit()
        ac = conn.execution_options(isolation_level="AUTOCOMMIT")
        ac.execute(
            text(
                f'ALTER TABLE admin_audit_log DETACH PARTITION "{child}" CONCURRENTLY'
            )
        )
        assert _relation_exists(conn, child)
        assert not _child_attached(conn, "admin_audit_log", child)

        first = _run_retention(
            conn,
            payload_days=30,
            history_days={
                "admin_audit_log": 30,
                "task_attempts": 90,
                "delivery_events_terminal": 30,
                "tasks_terminal": 30,
            },
        )
        assert not _relation_exists(conn, child)
        assert any(
            o.child_name == child and o.status in {"resumed", "dropped"}
            for o in first.outcomes
        )

        second = _run_retention(
            conn,
            payload_days=30,
            history_days={
                "admin_audit_log": 30,
                "task_attempts": 90,
                "delivery_events_terminal": 30,
                "tasks_terminal": 30,
            },
        )
        # Idempotent: no failed outcomes for missing child; no relation recreated.
        assert not _relation_exists(conn, child)
        assert not any(o.child_name == child and o.status == "failed" for o in second.outcomes)


def test_retention_does_not_cascade_into_active_or_correctness(
    retention_schema: tuple[str, str, Engine],
) -> None:
    schema, _url, engine = retention_schema
    with engine.connect() as conn:
        _set_search_path(conn, schema)
        today = _utc_today(conn)
        # Seed registries independent of history partitions.
        before_dedup = conn.execute(text("SELECT count(*) FROM enqueue_dedup")).scalar_one()
        before_replay = conn.execute(text("SELECT count(*) FROM complete_replay")).scalar_one()
        before_active = conn.execute(text("SELECT count(*) FROM tasks_active")).scalar_one()
        before_payload = conn.execute(
            text("SELECT count(*) FROM task_payloads_active")
        ).scalar_one()

        expired_day = today - timedelta(days=100)
        for parent in DAILY_RANGE_PARENTS:
            _create_past_child(conn, parent, expired_day)
        conn.commit()

        _run_retention(conn, payload_days=90)

        assert conn.execute(text("SELECT count(*) FROM enqueue_dedup")).scalar_one() == before_dedup
        assert (
            conn.execute(text("SELECT count(*) FROM complete_replay")).scalar_one()
            == before_replay
        )
        assert conn.execute(text("SELECT count(*) FROM tasks_active")).scalar_one() == before_active
        assert (
            conn.execute(text("SELECT count(*) FROM task_payloads_active")).scalar_one()
            == before_payload
        )
        # Today’s migration children remain attached.
        today_child = partition_catalog.child_name_for("task_attempts", today)
        assert _child_attached(conn, "task_attempts", today_child)


def test_held_session_only_no_commit_ownership_api(
    retention_schema: tuple[str, str, Engine],
) -> None:
    schema, _url, engine = retention_schema
    with engine.connect() as conn:
        _set_search_path(conn, schema)
        conn.commit()
        marker = id(conn)
        _run_retention(conn, payload_days=30)
        assert id(conn) == marker
        assert not conn.closed
        # Session still usable.
        assert _utc_today(conn) is not None

    src = _RETENTION_SRC.read_text(encoding="utf-8")
    # Must not open engines; commit/rollback only for concurrent-DDL session hygiene is
    # forbidden as owning outer txn — raise if in_transaction instead.
    assert "create_engine" not in src
