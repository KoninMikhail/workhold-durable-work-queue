"""PostgreSQL 18.6 integration fixtures for physical-contract storage tests."""

from __future__ import annotations

import os
import re
import uuid
from collections.abc import Iterator
from pathlib import Path

import psycopg
import pytest
from alembic import command
from alembic.config import Config

ROOT = Path(__file__).resolve().parents[2]
ALEMBIC_INI = ROOT / "alembic.ini"

_SCHEMA_NAME_RE = re.compile(r"^[a-z_][a-z0-9_]*$")

QUEUE_RELATION_KINDS = ("r", "p", "i", "S")


def require_test_database_url() -> str:
    """Return TEST_DATABASE_URL or fail hard (never skip/xfail)."""
    url = os.environ.get("TEST_DATABASE_URL", "").strip()
    if not url:
        pytest.fail(
            "TEST_DATABASE_URL is required for tests/integration "
            "(PostgreSQL 18.6). Refusing to skip or xfail."
        )
    return url


def to_psycopg_conninfo(url: str) -> str:
    """Normalize SQLAlchemy-style URLs for psycopg.connect."""
    if url.startswith("postgresql+psycopg://"):
        return "postgresql://" + url.removeprefix("postgresql+psycopg://")
    return url


def _validate_schema_name(schema: str) -> str:
    if not _SCHEMA_NAME_RE.fullmatch(schema):
        raise ValueError(f"refusing unsafe schema name: {schema!r}")
    return schema


def run_alembic(direction: str, target: str, *, schema: str, database_url: str) -> None:
    """Run Alembic upgrade/downgrade with version table + search_path in schema."""
    schema = _validate_schema_name(schema)
    previous_url = os.environ.get("DATABASE_URL")
    previous_schema = os.environ.get("ALEMBIC_VERSION_TABLE_SCHEMA")
    os.environ["DATABASE_URL"] = database_url
    os.environ["ALEMBIC_VERSION_TABLE_SCHEMA"] = schema
    try:
        cfg = Config(str(ALEMBIC_INI))
        if direction == "upgrade":
            command.upgrade(cfg, target)
        elif direction == "downgrade":
            command.downgrade(cfg, target)
        else:
            raise ValueError(f"unknown alembic direction: {direction}")
    finally:
        if previous_url is None:
            os.environ.pop("DATABASE_URL", None)
        else:
            os.environ["DATABASE_URL"] = previous_url
        if previous_schema is None:
            os.environ.pop("ALEMBIC_VERSION_TABLE_SCHEMA", None)
        else:
            os.environ["ALEMBIC_VERSION_TABLE_SCHEMA"] = previous_schema


def assert_schema_has_no_queue_relations(conn: psycopg.Connection, schema: str) -> None:
    """Assert isolated schema has no Queue relations (alembic_version may remain)."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT c.relname
            FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = %s
              AND c.relkind IN ('r', 'p', 'i', 'I', 'S')
              AND c.relname <> 'alembic_version'
              AND c.relname NOT LIKE 'alembic_version_%%'
            ORDER BY c.relname
            """,
            (schema,),
        )
        remaining = [row[0] for row in cur.fetchall()]
    assert remaining == [], f"Queue relations remain after downgrade: {remaining}"


def _open_schema_connection(database_url: str, schema: str) -> psycopg.Connection:
    conn = psycopg.connect(to_psycopg_conninfo(database_url))
    # SET must commit: a later rollback would otherwise undo session GUCs.
    conn.autocommit = True
    conn.execute(f'SET search_path TO "{schema}"')
    conn.autocommit = False
    return conn


@pytest.fixture(scope="session")
def test_database_url() -> str:
    return require_test_database_url()


@pytest.fixture(scope="session")
def migrated_schema(
    test_database_url: str,
) -> Iterator[tuple[psycopg.Connection, str]]:
    """Upgrade into a unique temporary schema; always downgrade/drop on cleanup."""
    schema = _validate_schema_name(f"qit_{uuid.uuid4().hex}")
    admin = psycopg.connect(to_psycopg_conninfo(test_database_url))
    admin.autocommit = True
    try:
        admin.execute(f'CREATE SCHEMA "{schema}"')
    finally:
        admin.close()

    run_alembic("upgrade", "head", schema=schema, database_url=test_database_url)
    conn = _open_schema_connection(test_database_url, schema)
    try:
        yield conn, schema
    finally:
        try:
            conn.close()
        except Exception:
            pass
        try:
            try:
                run_alembic(
                    "downgrade", "base", schema=schema, database_url=test_database_url
                )
            except RuntimeError as exc:
                if "downgrade blocked" not in str(exc).lower():
                    raise
        finally:
            drop = psycopg.connect(to_psycopg_conninfo(test_database_url))
            drop.autocommit = True
            try:
                drop.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
            finally:
                drop.close()
