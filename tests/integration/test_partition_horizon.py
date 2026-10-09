"""UTC partition horizon and no-DEFAULT proofs on live PostgreSQL 18.6."""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone

import pytest

DAILY_RANGE_PARENTS = {
    "admin_audit_log": "audit_at",
    "task_attempts": "claimed_at",
    "tasks_terminal": "terminal_at",
    "delivery_events_terminal": "terminal_at",
}

HORIZON_DAYS_AHEAD = 30


def test_four_range_parents_and_no_default_child(migrated_schema) -> None:
    conn, schema = migrated_schema
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT c.relname, pt.partstrat
            FROM pg_partitioned_table pt
            JOIN pg_class c ON c.oid = pt.partrelid
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = %s
            ORDER BY c.relname
            """,
            (schema,),
        )
        parents = {name: strat for name, strat in cur.fetchall()}
    assert set(parents) == set(DAILY_RANGE_PARENTS)
    assert all(strat == "r" for strat in parents.values())

    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT parent.relname AS parent_name,
                   child.relname AS child_name,
                   pg_get_expr(child.relpartbound, child.oid) AS bound
            FROM pg_inherits i
            JOIN pg_class child ON child.oid = i.inhrelid
            JOIN pg_class parent ON parent.oid = i.inhparent
            JOIN pg_namespace n ON n.oid = parent.relnamespace
            WHERE n.nspname = %s
              AND parent.relkind = 'p'
              AND child.relkind = 'r'
            ORDER BY parent.relname, child.relname
            """,
            (schema,),
        )
        children = cur.fetchall()

    assert children, "expected premade daily children"
    for parent_name, child_name, bound in children:
        assert parent_name in DAILY_RANGE_PARENTS
        assert bound is not None
        assert "DEFAULT" not in bound.upper()
        # Child suffix YYYYMMDD
        suffix = child_name.rsplit("_", 1)[-1]
        assert len(suffix) == 8 and suffix.isdigit()


def test_horizon_at_least_30_utc_days_ahead(migrated_schema) -> None:
    conn, schema = migrated_schema
    with conn.cursor() as cur:
        cur.execute(
            "SELECT (CURRENT_TIMESTAMP AT TIME ZONE 'UTC')::date"
        )
        store_today: date = cur.fetchone()[0]
        cur.execute(
            """
            SELECT parent.relname,
                   MAX(to_date(right(child.relname, 8), 'YYYYMMDD')) AS max_day
            FROM pg_inherits i
            JOIN pg_class child ON child.oid = i.inhrelid
            JOIN pg_class parent ON parent.oid = i.inhparent
            JOIN pg_namespace n ON n.oid = parent.relnamespace
            WHERE n.nspname = %s
              AND parent.relkind = 'p'
              AND child.relkind = 'r'
            GROUP BY parent.relname
            """,
            (schema,),
        )
        rows = cur.fetchall()

    assert len(rows) == 4
    required_last_day = store_today + timedelta(days=HORIZON_DAYS_AHEAD)
    for parent_name, max_day in rows:
        assert max_day is not None, parent_name
        # Inclusive horizon: migration UTC day through +30 days.
        assert max_day >= required_last_day, (
            f"{parent_name} last child day {max_day} < required {required_last_day}"
        )


def test_in_range_history_insert_and_beyond_horizon_rejected(migrated_schema) -> None:
    conn, schema = migrated_schema
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO admin_audit_log (
                audit_at, actor_id, operation_code, request_id, details
            ) VALUES (
                statement_timestamp(), 'admin-a', 1, gen_random_uuid(), '{}'::jsonb
            )
            """
        )

        cur.execute(
            "SELECT (CURRENT_TIMESTAMP AT TIME ZONE 'UTC')::date"
        )
        store_today: date = cur.fetchone()[0]
        beyond = datetime.combine(
            store_today + timedelta(days=HORIZON_DAYS_AHEAD + 2),
            datetime.min.time(),
            tzinfo=timezone.utc,
        )
        with pytest.raises(Exception):
            cur.execute(
                """
                INSERT INTO admin_audit_log (
                    audit_at, actor_id, operation_code, request_id, details
                ) VALUES (
                    %s, 'admin-a', 1, gen_random_uuid(), '{}'::jsonb
                )
                """,
                (beyond,),
            )
        conn.rollback()
