"""Real-PostgreSQL maintain-role lifecycle reporting (STOR-03, STOR-04, STOR-08).

Proves one-shot composite maintenance (premake → bound verify → retention →
registry purge), sole outer advisory-lock ownership, singleton status
transitions, skipped-lock and partial-failure reporting, and secret-free
telemetry. Plan path ``storage/postgres/maintenance`` remaps to
``infrastructure/postgres/maintenance``.
"""

from __future__ import annotations

import ast
import inspect
import multiprocessing as mp
import re
import uuid
from collections.abc import Iterator
from datetime import date, timedelta
from pathlib import Path
from typing import Any

import psycopg
import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from queue_service import db, health, settings
from queue_service.infrastructure.postgres import (
    history_retention,
    partition_premake,
    registry_retention,
)
from queue_service.roles import maintain
from queue_service.security.redaction import REDACTED, sanitize_for_diagnostics

DAILY_RANGE_PARENTS = health.DAILY_RANGE_PARENTS
PREMAKE_DAYS = health.DEFAULT_PARTITION_PREMAKE_DAYS
MAINTENANCE_LOCK_KEY = 0x515545554D41494E

@pytest.fixture
def maint_schema(test_database_url: str) -> Iterator[tuple[str, str, Engine]]:
    """Fresh migrated schema; yield (schema, url, engine). Always DROP CASCADE."""
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


def _role_pools() -> dict[str, settings.RolePoolSettings]:
    return {
        role: settings.RolePoolSettings(
            replica_ceiling=1,
            pool_ceiling=2,
            pool_acquisition_timeout_seconds=5.0,
            statement_timeout_seconds=30.0,
        )
        for role in settings.PROCESS_ROLES
    }


def _settings_for_url(database_url: str) -> settings.DeploymentSettings:
    return settings.DeploymentSettings(
        environment=settings.EnvironmentMode.DEVELOPMENT,
        listener_tls_mode=settings.ListenerTlsMode.PLAINTEXT_PUBLIC,
        database_url=settings.Secret(database_url),
        postgres_max_connections=100,
        postgres_reserved_connections=10,
        role_pools=_role_pools(),
        credential_generations=(),
    )


def _psycopg_url(url: str) -> str:
    from tests.integration.conftest import to_psycopg_conninfo

    return to_psycopg_conninfo(url)


def _read_status(engine: Engine, schema: str) -> dict[str, Any]:
    with engine.connect() as conn:
        conn.execute(text(f'SET search_path TO "{schema}"'))
        row = conn.execute(
            text(
                """
                SELECT singleton_id, last_started_at, last_succeeded_at,
                       premade_through, retained_from,
                       last_error_code, last_error_detail, updated_at
                FROM partition_maintenance_status
                WHERE singleton_id = 1
                """
            )
        ).mappings().one_or_none()
        return dict(row) if row is not None else {}


def _hold_lock_until(
    database_url: str,
    ready: Any,
    release: Any,
    held: Any,
) -> None:
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


def test_maintenance_module_exists_under_infrastructure_postgres() -> None:
    from queue_service.infrastructure.postgres import maintenance as maint_mod

    assert hasattr(maint_mod, "run_storage_maintenance")
    src = Path(inspect.getsourcefile(maint_mod) or "")
    assert src.name == "maintenance.py"
    assert "infrastructure" in str(src).replace("\\", "/")
    assert "storage/postgres" not in str(src).replace("\\", "/")


def test_only_maintain_role_acquires_advisory_lock() -> None:
    from queue_service.infrastructure.postgres import maintenance as maint_mod

    maintain_src = Path(inspect.getsourcefile(maintain) or "").read_text(
        encoding="utf-8"
    )
    maint_src = Path(inspect.getsourcefile(maint_mod) or "").read_text(encoding="utf-8")
    premake_src = Path(inspect.getsourcefile(partition_premake) or "").read_text(
        encoding="utf-8"
    )
    hist_src = Path(inspect.getsourcefile(history_retention) or "").read_text(
        encoding="utf-8"
    )
    reg_src = Path(inspect.getsourcefile(registry_retention) or "").read_text(
        encoding="utf-8"
    )

    assert "pg_try_advisory_lock" in maintain_src
    assert "MAINTENANCE_LOCK_KEY" in maintain_src
    for blob, label in (
        (maint_src, "maintenance"),
        (premake_src, "premake"),
        (hist_src, "history_retention"),
        (reg_src, "registry_retention"),
    ):
        assert "pg_try_advisory_lock" not in blob, label
        assert "pg_advisory_lock" not in blob, label
        assert "create_engine" not in blob, label
        assert "create_role_engine" not in blob, label


def test_complete_run_covers_premake_verify_retention_and_one_purge_batch(
    maint_schema: tuple[str, str, Engine],
) -> None:
    schema, url, engine = maint_schema
    dep = _settings_for_url(url)

    # Drop a far-horizon day so premake must recreate it.
    with engine.begin() as conn:
        conn.execute(text(f'SET search_path TO "{schema}"'))
        today = conn.execute(
            text("SELECT (CURRENT_TIMESTAMP AT TIME ZONE 'UTC')::date")
        ).scalar_one()
        far = today + timedelta(days=PREMAKE_DAYS)
        suffix = far.strftime("%Y%m%d")
        for parent in DAILY_RANGE_PARENTS:
            conn.execute(text(f'DROP TABLE IF EXISTS "{parent}_{suffix}"'))

    result = maintain.run_cycle(
        dep,
        schema=schema,
        premake_days=PREMAKE_DAYS,
        lock_timeout_seconds=5.0,
    )
    assert result.exit_code == maintain.EXIT_OK
    assert result.outcome == "succeeded"
    assert result.lock_outcome == "acquired"
    assert result.premade_through is not None
    assert result.retained_from is not None
    assert result.purge_examined_total >= 0
    assert result.purge_deleted_total >= 0
    assert result.last_succeeded_at is not None
    assert result.error_code is None

    # One batch per accepted registry in the typed report.
    assert result.purge_by_registry is not None
    assert set(result.purge_by_registry) == {
        "enqueue_dedup",
        "complete_replay",
        "admin_replay",
        "delivery_published",
    }

    status = _read_status(engine, schema)
    assert status["singleton_id"] == 1
    assert status["last_succeeded_at"] is not None
    assert status["premade_through"] == result.premade_through
    assert status["retained_from"] == result.retained_from
    assert status["last_error_code"] is None
    assert status["last_error_detail"] is None
    assert status["last_started_at"] is not None


def test_skipped_lock_is_successful_without_singleton_mutation(
    maint_schema: tuple[str, str, Engine],
) -> None:
    schema, url, engine = maint_schema
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
        before = _read_status(engine, schema)
        result = maintain.run_cycle(
            _settings_for_url(url),
            schema=schema,
            premake_days=PREMAKE_DAYS,
            lock_timeout_seconds=1.0,
        )
        after = _read_status(engine, schema)
        assert result.exit_code == maintain.EXIT_OK
        assert result.outcome == "skipped_lock"
        assert result.lock_outcome == "skipped"
        assert before == after
    finally:
        ready.set()
        release.set()
        holder.join(timeout=10)
        if holder.is_alive():
            holder.terminate()
            holder.join(timeout=5)


def test_partial_failure_preserves_last_succeeded_and_exits_nonzero(
    maint_schema: tuple[str, str, Engine],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    schema, url, engine = maint_schema
    dep = _settings_for_url(url)

    ok = maintain.run_cycle(
        dep,
        schema=schema,
        premake_days=PREMAKE_DAYS,
        lock_timeout_seconds=5.0,
    )
    assert ok.exit_code == maintain.EXIT_OK
    prior = _read_status(engine, schema)
    prior_succeeded = prior["last_succeeded_at"]
    prior_premade = prior["premade_through"]
    prior_retained = prior["retained_from"]
    assert prior_succeeded is not None

    def _boom(*_a: object, **_k: object) -> object:
        raise RuntimeError("injected retention failure with payload=secret-body")

    monkeypatch.setattr(
        "queue_service.infrastructure.postgres.maintenance.retain_expired_history",
        _boom,
    )

    failed = maintain.run_cycle(
        dep,
        schema=schema,
        premake_days=PREMAKE_DAYS,
        lock_timeout_seconds=5.0,
    )
    assert failed.exit_code != maintain.EXIT_OK
    assert failed.outcome == "failed"
    assert failed.error_code is not None
    assert 1 <= len(failed.error_code) <= 128
    assert failed.error_detail is not None
    assert len(failed.error_detail) <= 4096
    assert "payload" not in failed.error_detail.lower() or REDACTED in failed.error_detail
    assert "secret-body" not in (failed.error_detail or "")

    status = _read_status(engine, schema)
    assert status["last_succeeded_at"] == prior_succeeded
    assert status["premade_through"] == prior_premade
    assert status["retained_from"] == prior_retained
    assert status["last_error_code"] == failed.error_code
    assert status["last_started_at"] is not None
    assert status["updated_at"] is not None


def test_report_and_logs_are_bounded_and_secret_free(
    maint_schema: tuple[str, str, Engine],
    capsys: pytest.CaptureFixture[str],
) -> None:
    schema, url, _engine = maint_schema
    result = maintain.run_cycle(
        _settings_for_url(url),
        schema=schema,
        premake_days=PREMAKE_DAYS,
        lock_timeout_seconds=5.0,
    )
    assert result.exit_code == maintain.EXIT_OK

    # Typed report projected through diagnostic sanitizer must not leak secrets.
    projection = {
        "status": result.outcome,
        "reason": result.error_code or "none",
        "outcome": result.lock_outcome,
        "payload": "must-never-appear",
        "claim_token": "tok-secret",
        "idempotency_key": "idem-secret",
    }
    cleaned = sanitize_for_diagnostics(projection)
    assert cleaned.get("payload") == REDACTED
    assert cleaned.get("claim_token") == REDACTED
    assert "idempotency_key" not in cleaned or cleaned["idempotency_key"] == REDACTED

    captured = capsys.readouterr()
    combined = f"{captured.out}\n{captured.err}".lower()
    assert "postgresql+psycopg://" not in combined
    assert "create table" not in combined
    assert "delete from" not in combined
    assert "claim_token" not in combined
    assert "idempotency" not in combined
    # Status line is low-cardinality key=value.
    assert re.search(r"maintain status=\S+ reason=\S+", captured.out)


def test_singleton_contract_unchanged_no_duplicate_status_relation() -> None:
    from queue_service.storage import models

    names = {t.name for t in models.Base.metadata.tables.values()}
    assert "partition_maintenance_status" in names
    status_tables = [n for n in names if "maintenance_status" in n]
    assert status_tables == ["partition_maintenance_status"]

    table = models.Base.metadata.tables["partition_maintenance_status"]
    cols = set(table.c.keys())
    assert cols == {
        "singleton_id",
        "last_started_at",
        "last_succeeded_at",
        "premade_through",
        "retained_from",
        "last_error_code",
        "last_error_detail",
        "updated_at",
    }
    assert table.c.singleton_id.identity is None


def test_primitives_receive_held_session_ast_contract() -> None:
    from queue_service.infrastructure.postgres import maintenance as maint_mod

    tree = ast.parse(Path(inspect.getsourcefile(maint_mod) or "").read_text(encoding="utf-8"))
    calls: list[str] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Call):
            if isinstance(node.func, ast.Name):
                calls.append(node.func.id)
            elif isinstance(node.func, ast.Attribute):
                calls.append(node.func.attr)
    assert "premake_daily_partitions" in calls
    assert "retain_expired_history" in calls or "purge_expired_registries" in calls
    assert "create_engine" not in calls
    assert "create_role_engine" not in calls
