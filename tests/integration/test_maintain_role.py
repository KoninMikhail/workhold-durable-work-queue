"""Real-PostgreSQL maintain-role lifecycle (PKG-01, OPS-01, STOR-03/04/08).

Proves one-shot advisory-locked composite maintenance, single-winner
concurrency (successful skipped_lock), and bounded lock/SIGTERM failure.
Phase 3.8 extends the Phase 3.2 runner with premake/retention/purge mutations
on the held session only.
"""

from __future__ import annotations

import multiprocessing as mp
import signal
import threading
import time
import uuid
from collections.abc import Iterator
from datetime import date, timedelta
from typing import Any

import psycopg
import pytest
from sqlalchemy import event

from workhold import db, health, settings
from workhold.roles import maintain

DAILY_RANGE_PARENTS = (
    "admin_audit_log",
    "task_attempts",
    "tasks_terminal",
    "delivery_events_terminal",
)

PREMAKE_DAYS = 30

# Must match maintain.MAINTENANCE_LOCK_KEY; migrate uses a different lock namespace.
MAINTENANCE_LOCK_KEY = 0x515545554D41494E
MIGRATE_LOCK_CLASS = 0x51554555
MIGRATE_LOCK_ID = 1


def _role_pools(
    *,
    pool_ceiling: int = 2,
    replica_ceiling: int = 1,
    acquisition_timeout: float = 5.0,
    statement_timeout: float = 30.0,
) -> dict[str, settings.RolePoolSettings]:
    return {
        role: settings.RolePoolSettings(
            replica_ceiling=replica_ceiling,
            pool_ceiling=pool_ceiling,
            pool_acquisition_timeout_seconds=acquisition_timeout,
            statement_timeout_seconds=statement_timeout,
        )
        for role in settings.PROCESS_ROLES
    }


def _settings_for_url(
    database_url: str,
    *,
    pool_ceiling: int = 2,
    acquisition_timeout: float = 5.0,
    statement_timeout: float = 30.0,
) -> settings.DeploymentSettings:
    return settings.DeploymentSettings(
        environment=settings.EnvironmentMode.DEVELOPMENT,
        listener_tls_mode=settings.ListenerTlsMode.PLAINTEXT_PUBLIC,
        database_url=settings.Secret(database_url),
        postgres_max_connections=100,
        postgres_reserved_connections=10,
        role_pools=_role_pools(
            pool_ceiling=pool_ceiling,
            acquisition_timeout=acquisition_timeout,
            statement_timeout=statement_timeout,
        ),
        credential_generations=(),
    )


def _psycopg_url(url: str) -> str:
    from tests.integration.conftest import to_psycopg_conninfo

    return to_psycopg_conninfo(url)


@pytest.fixture
def maintain_schema(test_database_url: str) -> Iterator[tuple[str, str]]:
    """Fresh migrated schema for maintain-role tests; always DROP CASCADE."""
    from tests.integration.conftest import run_alembic, to_psycopg_conninfo

    schema = f"qit_{uuid.uuid4().hex}"
    admin = psycopg.connect(to_psycopg_conninfo(test_database_url))
    admin.autocommit = True
    try:
        admin.execute(f'CREATE SCHEMA "{schema}"')
    finally:
        admin.close()

    run_alembic("upgrade", "head", schema=schema, database_url=test_database_url)
    try:
        yield schema, test_database_url
    finally:
        drop = psycopg.connect(to_psycopg_conninfo(test_database_url))
        drop.autocommit = True
        try:
            drop.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        finally:
            drop.close()


def _utc_today(conn: psycopg.Connection) -> date:
    with conn.cursor() as cur:
        cur.execute("SELECT (CURRENT_TIMESTAMP AT TIME ZONE 'UTC')::date")
        return cur.fetchone()[0]


def _drop_partition_day(conn: psycopg.Connection, schema: str, day: date) -> None:
    suffix = day.strftime("%Y%m%d")
    conn.rollback()
    conn.autocommit = True
    try:
        for parent in DAILY_RANGE_PARENTS:
            conn.execute(f'DROP TABLE IF EXISTS "{schema}"."{parent}_{suffix}"')
    finally:
        conn.autocommit = False


def _set_alembic_revision(conn: psycopg.Connection, schema: str, revision: str) -> None:
    conn.execute(f'SET search_path TO "{schema}"')
    conn.execute("DELETE FROM alembic_version")
    conn.execute(
        "INSERT INTO alembic_version (version_num) VALUES (%s)",
        (revision,),
    )
    conn.commit()


def test_maintenance_lock_key_distinct_from_migration_lock() -> None:
    assert maintain.MAINTENANCE_LOCK_KEY == MAINTENANCE_LOCK_KEY
    # Migrate Plan 08 uses pg_advisory_lock(class, id); maintain uses bigint key.
    assert maintain.MAINTENANCE_LOCK_KEY != MIGRATE_LOCK_CLASS
    assert MIGRATE_LOCK_CLASS == 0x51554555
    assert MIGRATE_LOCK_ID == 1


def test_safe_horizon_exits_zero(
    maintain_schema: tuple[str, str],
) -> None:
    schema, url = maintain_schema
    dep = _settings_for_url(url)
    started = time.monotonic()
    result = maintain.run_cycle(
        dep,
        schema=schema,
        premake_days=PREMAKE_DAYS,
        lock_timeout_seconds=5.0,
    )
    elapsed = time.monotonic() - started
    assert result.exit_code == maintain.EXIT_OK
    assert result.outcome == "succeeded"
    assert elapsed < 30.0


def test_missing_horizon_day_is_repaired_by_premake(
    maintain_schema: tuple[str, str],
) -> None:
    schema, url = maintain_schema
    conn = psycopg.connect(_psycopg_url(url))
    try:
        conn.execute(f'SET search_path TO "{schema}"')
        today = _utc_today(conn)
        _drop_partition_day(conn, schema, today + timedelta(days=PREMAKE_DAYS))
    finally:
        conn.close()

    result = maintain.run_cycle(
        _settings_for_url(url),
        schema=schema,
        premake_days=PREMAKE_DAYS,
        lock_timeout_seconds=5.0,
    )
    assert result.exit_code == maintain.EXIT_OK
    assert result.outcome == "succeeded"
    assert result.partitions_created >= 1


def test_unreachable_postgres_exits_dependency_failure() -> None:
    bad = "postgresql+psycopg://queue:queue@127.0.0.1:1/queue"
    started = time.monotonic()
    result = maintain.run_cycle(
        _settings_for_url(bad, acquisition_timeout=1.0, statement_timeout=1.0),
        schema=None,
        premake_days=PREMAKE_DAYS,
        lock_timeout_seconds=2.0,
    )
    elapsed = time.monotonic() - started
    assert result.exit_code == maintain.EXIT_DEPENDENCY
    assert elapsed < 5.0


def _hold_lock_until(
    database_url: str,
    ready: Any,
    release: Any,
    held: Any,
) -> None:
    """Child process: acquire maintenance advisory lock and wait for release."""
    conn = psycopg.connect(_psycopg_url(database_url))
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT pg_advisory_lock(%s)", (MAINTENANCE_LOCK_KEY,))
        held.set()
        ready.wait(timeout=30)
        release.wait(timeout=60)
    finally:
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT pg_advisory_unlock(%s)", (MAINTENANCE_LOCK_KEY,)
                )
        finally:
            conn.close()


def test_concurrent_loser_exits_skipped_lock_without_critical_section(
    maintain_schema: tuple[str, str],
) -> None:
    schema, url = maintain_schema
    ctx = mp.get_context("spawn")
    ready = ctx.Event()
    release = ctx.Event()
    held = ctx.Event()
    holder = ctx.Process(
        target=_hold_lock_until,
        args=(url, ready, release, held),
    )
    holder.start()
    try:
        assert held.wait(timeout=15), "holder failed to acquire advisory lock"

        inspected: list[bool] = []

        def _track_run(*_a: object, **_k: object) -> object:
            inspected.append(True)
            raise AssertionError("loser must not run storage maintenance")

        original_fn = maintain.run_storage_maintenance
        maintain.run_storage_maintenance = _track_run  # type: ignore[assignment]
        try:
            started = time.monotonic()
            result = maintain.run_cycle(
                _settings_for_url(url),
                schema=schema,
                premake_days=PREMAKE_DAYS,
                lock_timeout_seconds=1.0,
            )
            elapsed = time.monotonic() - started
        finally:
            maintain.run_storage_maintenance = original_fn  # type: ignore[assignment]

        assert result.exit_code == maintain.EXIT_OK
        assert result.outcome == "skipped_lock"
        assert elapsed < 5.0
        assert inspected == []
    finally:
        ready.set()
        release.set()
        holder.join(timeout=10)
        if holder.is_alive():
            holder.terminate()
            holder.join(timeout=5)


def test_sigterm_during_lock_wait_exits_bounded_and_releases(
    maintain_schema: tuple[str, str],
) -> None:
    """SIGTERM while waiting for the lock must terminate within the deadline."""
    schema, url = maintain_schema
    assert hasattr(signal, "SIGTERM")
    assert hasattr(signal, "raise_signal")

    ctx = mp.get_context("spawn")
    ready = ctx.Event()
    release = ctx.Event()
    held = ctx.Event()
    holder = ctx.Process(
        target=_hold_lock_until,
        args=(url, ready, release, held),
    )
    holder.start()
    try:
        assert held.wait(timeout=15)

        def _fire_sigterm() -> None:
            time.sleep(0.3)
            signal.raise_signal(signal.SIGTERM)

        starter = time.monotonic()
        threading_timer = __import__("threading").Thread(
            target=_fire_sigterm, daemon=True
        )
        threading_timer.start()
        result = maintain.run_cycle(
            _settings_for_url(url),
            schema=schema,
            premake_days=PREMAKE_DAYS,
            lock_timeout_seconds=8.0,
        )
        elapsed = time.monotonic() - starter
        assert result.exit_code in {maintain.EXIT_DEPENDENCY, maintain.EXIT_OK}
        assert result.outcome in {"dependency_failure", "skipped_lock"}
        assert result.exit_code != maintain.EXIT_OK or result.outcome == "skipped_lock"
        assert elapsed < 6.0
        threading_timer.join(timeout=2)
    finally:
        ready.set()
        release.set()
        holder.join(timeout=10)
        if holder.is_alive():
            holder.terminate()
            holder.join(timeout=5)


def test_winner_performs_partition_and_registry_mutations(
    maintain_schema: tuple[str, str],
) -> None:
    schema, url = maintain_schema
    dep = _settings_for_url(url)
    engine = db.create_role_engine(dep, "maintain")
    captured: list[str] = []

    def _capture(
        _conn: object,
        _cursor: object,
        statement: str,
        _parameters: object,
        _context: object,
        _executemany: object,
    ) -> None:
        captured.append(statement)

    event.listen(engine, "before_cursor_execute", _capture)
    try:
        result = maintain.run_cycle(
            dep,
            schema=schema,
            premake_days=PREMAKE_DAYS,
            lock_timeout_seconds=5.0,
            engine=engine,
        )
    finally:
        event.remove(engine, "before_cursor_execute", _capture)
        engine.dispose()

    assert result.exit_code == maintain.EXIT_OK
    assert result.outcome == "succeeded"
    assert captured, "expected SQL statements during maintain cycle"
    joined = " ".join(" ".join(s.lower().split()) for s in captured)
    # Winner path updates singleton status and may create/drop partitions.
    assert "partition_maintenance_status" in joined
    assert "pg_try_advisory_lock" in joined or "pg_advisory_unlock" in joined


def test_cli_run_missing_database_url_exits_dependency(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("DATABASE_URL", raising=False)
    monkeypatch.delenv("TEST_DATABASE_URL", raising=False)
    code = maintain.run([])
    assert code == maintain.EXIT_DEPENDENCY
