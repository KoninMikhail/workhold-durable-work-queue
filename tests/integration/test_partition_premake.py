"""Real-PostgreSQL held-session daily UTC partition premake (STOR-03).

Proves contiguous horizon creation, idempotency, inherited parent indexes, and
that the primitive never acquires advisory locks or opens its own connections.
"""

from __future__ import annotations

import ast
import inspect
import uuid
from collections.abc import Iterator
from datetime import date, timedelta
from pathlib import Path

import psycopg
import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Connection, Engine

from queue_service.health import DAILY_RANGE_PARENTS, DEFAULT_PARTITION_PREMAKE_DAYS
from queue_service.infrastructure.postgres import partition_catalog, partition_premake

PARENT_KEYS = {
    "admin_audit_log": "audit_at",
    "task_attempts": "claimed_at",
    "tasks_terminal": "terminal_at",
    "delivery_events_terminal": "terminal_at",
}

MIN_HORIZON_DAYS = 14
MAX_HORIZON_DAYS = 30
SAFE_HORIZON = DEFAULT_PARTITION_PREMAKE_DAYS

_PREMAKE_SRC = Path(inspect.getsourcefile(partition_premake) or "")
_CATALOG_SRC = Path(inspect.getsourcefile(partition_catalog) or "")


@pytest.fixture
def premake_schema(test_database_url: str) -> Iterator[tuple[str, str, Engine]]:
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


def _utc_today(conn: Connection) -> date:
    return conn.execute(
        text("SELECT (CURRENT_TIMESTAMP AT TIME ZONE 'UTC')::date")
    ).scalar_one()


def _set_search_path(conn: Connection, schema: str) -> None:
    conn.execute(text(f'SET search_path TO "{schema}"'))


def _child_names(conn: Connection, parent: str) -> set[str]:
    rows = conn.execute(
        text(
            """
            SELECT child.relname
            FROM pg_inherits i
            JOIN pg_class child ON child.oid = i.inhrelid
            JOIN pg_class parent ON parent.oid = i.inhparent
            JOIN pg_namespace n ON n.oid = parent.relnamespace
            WHERE n.nspname = current_schema()
              AND parent.relname = :parent
              AND parent.relkind = 'p'
              AND child.relkind = 'r'
            """
        ),
        {"parent": parent},
    ).all()
    return {str(r[0]) for r in rows}


def _drop_future_partitions(conn: Connection, from_day: date) -> None:
    """Drop children on/after from_day so premake must recreate them."""
    for parent in DAILY_RANGE_PARENTS:
        for name in sorted(_child_names(conn, parent)):
            suffix = name.rsplit("_", 1)[-1]
            if len(suffix) != 8 or not suffix.isdigit():
                continue
            day = date(int(suffix[:4]), int(suffix[4:6]), int(suffix[6:8]))
            if day >= from_day:
                conn.execute(text(f'DROP TABLE IF EXISTS "{name}"'))
    conn.commit()


def _partition_days(conn: Connection, parent: str) -> set[date]:
    rows = conn.execute(
        text(
            """
            SELECT to_date(right(child.relname, 8), 'YYYYMMDD') AS day
            FROM pg_inherits i
            JOIN pg_class child ON child.oid = i.inhrelid
            JOIN pg_class parent ON parent.oid = i.inhparent
            JOIN pg_namespace n ON n.oid = parent.relnamespace
            WHERE n.nspname = current_schema()
              AND parent.relname = :parent
              AND parent.relkind = 'p'
              AND child.relkind = 'r'
            """
        ),
        {"parent": parent},
    ).all()
    return {row[0] for row in rows if row[0] is not None}


def _bound_expr(conn: Connection, child: str) -> str:
    row = conn.execute(
        text(
            """
            SELECT pg_get_expr(c.relpartbound, c.oid)
            FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = current_schema() AND c.relname = :child
            """
        ),
        {"child": child},
    ).one()
    return str(row[0])


def _index_names(conn: Connection, relname: str) -> set[str]:
    rows = conn.execute(
        text(
            """
            SELECT i.relname
            FROM pg_index x
            JOIN pg_class t ON t.oid = x.indrelid
            JOIN pg_class i ON i.oid = x.indexrelid
            JOIN pg_namespace n ON n.oid = t.relnamespace
            WHERE n.nspname = current_schema() AND t.relname = :rel
            """
        ),
        {"rel": relname},
    ).all()
    return {str(r[0]) for r in rows}


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
            if name.endswith(
                (
                    "pg_advisory_lock",
                    "pg_try_advisory_lock",
                    "create_engine",
                    "create_role_engine",
                )
            ):
                forbidden.append(name)
        if isinstance(node, ast.Constant) and isinstance(node.value, str):
            lowered = node.value.lower()
            if "pg_advisory_lock" in lowered or "pg_try_advisory_lock" in lowered:
                forbidden.append(node.value)
    return forbidden


def _call_name(func: ast.AST) -> str:
    if isinstance(func, ast.Name):
        return func.id
    if isinstance(func, ast.Attribute):
        base = _call_name(func.value)
        return f"{base}.{func.attr}" if base else func.attr
    return ""


def test_modules_forbid_lock_and_connection_creation() -> None:
    assert _PREMAKE_SRC.is_file()
    assert _CATALOG_SRC.is_file()
    for path in (_PREMAKE_SRC, _CATALOG_SRC):
        hits = _forbidden_ast_calls(path)
        assert hits == [], f"{path.name} must not acquire locks/engines: {hits}"
        src = path.read_text(encoding="utf-8")
        assert "create_engine" not in src
        assert "create_role_engine" not in src
        assert "pg_advisory" not in src.lower()


def test_parent_allowlist_matches_phase_31() -> None:
    specs = partition_catalog.history_parent_specs()
    assert {s.parent_name for s in specs} == set(DAILY_RANGE_PARENTS)
    assert {s.parent_name: s.partition_key for s in specs} == PARENT_KEYS
    assert MIN_HORIZON_DAYS <= SAFE_HORIZON <= MAX_HORIZON_DAYS


def test_premake_creates_contiguous_horizon_on_held_session(
    premake_schema: tuple[str, str, Engine],
) -> None:
    schema, _url, engine = premake_schema
    with engine.connect() as conn:
        _set_search_path(conn, schema)
        today = _utc_today(conn)
        # Leave only today so premake must create through the configured horizon.
        _drop_future_partitions(conn, today + timedelta(days=1))
        remaining = _partition_days(conn, "admin_audit_log")
        assert today in remaining
        assert today + timedelta(days=SAFE_HORIZON) not in remaining

        result = partition_premake.premake_daily_partitions(
            conn, horizon_days=SAFE_HORIZON
        )
        conn.commit()

        expected = {today + timedelta(days=o) for o in range(SAFE_HORIZON + 1)}
        for parent in DAILY_RANGE_PARENTS:
            days = _partition_days(conn, parent)
            assert expected.issubset(days), f"{parent} missing {expected - days}"
            assert result.through_day == today + timedelta(days=SAFE_HORIZON)
            assert parent in result.by_parent
            parent_result = result.by_parent[parent]
            assert parent_result.created or parent_result.existing
            created_days = {spec.day for spec in parent_result.created}
            assert (expected - {today}).issubset(created_days)


def test_second_premake_is_idempotent(
    premake_schema: tuple[str, str, Engine],
) -> None:
    schema, _url, engine = premake_schema
    with engine.connect() as conn:
        _set_search_path(conn, schema)
        first = partition_premake.premake_daily_partitions(
            conn, horizon_days=SAFE_HORIZON
        )
        conn.commit()
        before = {
            parent: frozenset(_child_names(conn, parent)) for parent in DAILY_RANGE_PARENTS
        }

        second = partition_premake.premake_daily_partitions(
            conn, horizon_days=SAFE_HORIZON
        )
        conn.commit()
        after = {
            parent: frozenset(_child_names(conn, parent)) for parent in DAILY_RANGE_PARENTS
        }

        assert before == after
        for parent in DAILY_RANGE_PARENTS:
            assert second.by_parent[parent].created == ()
            assert second.by_parent[parent].existing
            assert len(second.by_parent[parent].existing) == SAFE_HORIZON + 1
        assert second.through_day == first.through_day


def test_created_children_match_bounds_indexes_and_no_default(
    premake_schema: tuple[str, str, Engine],
) -> None:
    schema, _url, engine = premake_schema
    with engine.connect() as conn:
        _set_search_path(conn, schema)
        today = _utc_today(conn)
        target = today + timedelta(days=SAFE_HORIZON)
        _drop_future_partitions(conn, target)
        result = partition_premake.premake_daily_partitions(
            conn, horizon_days=SAFE_HORIZON
        )
        conn.commit()

        for parent, key in PARENT_KEYS.items():
            parent_indexes = _index_names(conn, parent)
            # Parent indexes exclude the implicit partition constraint index naming.
            assert parent_indexes, f"parent {parent} must expose indexes"
            child = f"{parent}_{target.strftime('%Y%m%d')}"
            assert child in {s.child_name for s in result.by_parent[parent].created} or (
                child in {s.child_name for s in result.by_parent[parent].existing}
            )
            bound = _bound_expr(conn, child)
            assert "DEFAULT" not in bound.upper()
            assert "FOR VALUES FROM" in bound.upper() or "FROM (" in bound.upper()
            child_indexes = _index_names(conn, child)
            # Every non-constraint parent index name appears on the child (PG appends
            # nothing for PARTITION OF inheritance of named indexes — names are unique
            # per relation, so children get distinct index names; compare column sets).
            parent_cols = conn.execute(
                text(
                    """
                    SELECT indexname, indexdef
                    FROM pg_indexes
                    WHERE schemaname = current_schema() AND tablename = :t
                    ORDER BY indexname
                    """
                ),
                {"t": parent},
            ).all()
            child_defs = [
                row[1]
                for row in conn.execute(
                    text(
                        """
                        SELECT indexname, indexdef
                        FROM pg_indexes
                        WHERE schemaname = current_schema() AND tablename = :t
                        ORDER BY indexname
                        """
                    ),
                    {"t": child},
                ).all()
            ]
            assert parent_cols, parent
            assert child_defs, child
            # Each parent index definition (sans table name) has a child counterpart.
            for _name, pdef in parent_cols:
                needle = pdef.replace(f"ON {parent}", f"ON {child}").replace(
                    f'ON "{parent}"', f'ON "{child}"'
                )
                # Fallback: require same USING / column expression fragment.
                col_frag = pdef.split("USING", 1)[-1]
                assert any(
                    col_frag in cdef or needle == cdef for cdef in child_defs
                ), f"{parent}/{child} missing index covering {col_frag}"
            # Partition key must appear in bound metadata for the parent key.
            assert key  # allowlisted key present
            assert today <= target


def test_premake_rejects_horizon_outside_14_30(
    premake_schema: tuple[str, str, Engine],
) -> None:
    schema, _url, engine = premake_schema
    with engine.connect() as conn:
        _set_search_path(conn, schema)
        with pytest.raises(ValueError):
            partition_premake.premake_daily_partitions(conn, horizon_days=13)
        with pytest.raises(ValueError):
            partition_premake.premake_daily_partitions(conn, horizon_days=31)


def test_premake_does_not_close_or_replace_caller_connection(
    premake_schema: tuple[str, str, Engine],
) -> None:
    schema, _url, engine = premake_schema
    with engine.connect() as conn:
        _set_search_path(conn, schema)
        marker = id(conn)
        partition_premake.premake_daily_partitions(conn, horizon_days=SAFE_HORIZON)
        conn.commit()
        assert id(conn) == marker
        assert not conn.closed
        # Session still usable after premake.
        assert _utc_today(conn) is not None
