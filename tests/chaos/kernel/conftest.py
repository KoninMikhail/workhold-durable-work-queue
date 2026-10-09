"""Chaos kernel fixtures: per-test disposable schema + Docker PostgreSQL."""

from __future__ import annotations

import uuid
from collections.abc import Iterator

import psycopg
import pytest
from sqlalchemy import create_engine, event
from sqlalchemy.orm import Session, sessionmaker

from tests.chaos.kernel.harness import KernelChaosHarness
from tests.integration.conftest import (
    require_test_database_url,
    run_alembic,
    to_psycopg_conninfo,
)


@pytest.fixture
def chaos_schema() -> Iterator[tuple[str, str]]:
    """Function-scoped schema so PITR restore cannot poison sibling tests."""
    database_url = require_test_database_url()
    schema = f"qch_{uuid.uuid4().hex}"
    admin = psycopg.connect(to_psycopg_conninfo(database_url))
    admin.autocommit = True
    try:
        admin.execute(f'CREATE SCHEMA "{schema}"')
    finally:
        admin.close()
    run_alembic("upgrade", "head", schema=schema, database_url=database_url)
    try:
        yield database_url, schema
    finally:
        try:
            run_alembic("downgrade", "base", schema=schema, database_url=database_url)
        finally:
            drop = psycopg.connect(to_psycopg_conninfo(database_url))
            drop.autocommit = True
            try:
                drop.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
            finally:
                drop.close()


@pytest.fixture
def chaos_harness(chaos_schema) -> Iterator[KernelChaosHarness]:
    """Provide a ready KernelChaosHarness bound to disposable schema + Docker PG."""
    database_url, schema = chaos_schema
    engine = create_engine(database_url, pool_pre_ping=True)

    @event.listens_for(engine, "connect")
    def _set_search_path(dbapi_connection, _connection_record) -> None:  # noqa: ANN001
        previous = dbapi_connection.autocommit
        dbapi_connection.autocommit = True
        try:
            cursor = dbapi_connection.cursor()
            cursor.execute(f'SET search_path TO "{schema}"')
            cursor.close()
        finally:
            dbapi_connection.autocommit = previous

    factory = sessionmaker(bind=engine, expire_on_commit=False)
    harness = KernelChaosHarness(
        session_factory=factory,
        database_url=database_url,
        schema=schema,
        engine=engine,
    )
    try:
        yield harness
    finally:
        harness.close()
        engine.dispose()
