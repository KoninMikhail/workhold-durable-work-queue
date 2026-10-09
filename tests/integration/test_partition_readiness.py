"""Real-PostgreSQL partition premake headroom readiness (STOR-03).

Proves readiness fails at the configured safety boundary with a limiting parent,
reports remaining UTC headroom without scanning history rows, and recovers after
plan-01 premake while liveness stays process-only.
"""

from __future__ import annotations

import ast
import inspect
import time
import uuid
from collections.abc import Iterator
from datetime import date, timedelta
from pathlib import Path

import psycopg
import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Connection, Engine

from workhold import db, health, settings
from workhold.health import DAILY_RANGE_PARENTS, DEFAULT_PARTITION_PREMAKE_DAYS
from workhold.infrastructure.postgres import partition_catalog, partition_premake

SAFE_HORIZON = DEFAULT_PARTITION_PREMAKE_DAYS
LIMITING_PARENT = "tasks_terminal"

# Disposable schemas migrate to alembic head (040–0502). Production
# BINARY_COMPATIBLE_MAX remains 039; extend only for readiness probes that
# must reach partition checks against head (same pattern as chaos harness).
_HEAD_REVISION_ORDER: tuple[str, ...] = (
    "0001_physical_contract_foundations",
    "039_apply_qualified_storage_layout",
    "040_admin_dead_letter_replay_ops",
    "041_admin_bulk_ops",
    "042_admin_break_glass_ops",
    "0501_delivery_outbox",
    "0502_delivery_pending_generation",
)
_HEAD_COMPATIBLE_MAX = "0502_delivery_pending_generation"


def _check_readiness(
    engine: Engine,
    *,
    schema: str | None = None,
    premake_days: int = SAFE_HORIZON,
) -> health.HealthStatus:
    return health.check_readiness(
        engine,
        schema=schema,
        premake_days=premake_days,
        compatible_max=_HEAD_COMPATIBLE_MAX,
        revision_order=_HEAD_REVISION_ORDER,
    )


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
        credential_generations=(
            settings.CredentialGeneration(
                principal_id="partition-readiness-test",
                generation_id="gen-1",
                secret=settings.Secret("token"),
            ),
        ),
    )


def _api_engine(database_url: str, **pool_kwargs: object) -> Engine:
    return db.create_role_engine(_settings_for_url(database_url, **pool_kwargs), "api")


@pytest.fixture
def readiness_schema(test_database_url: str) -> Iterator[tuple[Engine, str, str]]:
    """Fresh migrated schema + API-role engine; always DROP CASCADE."""
    from tests.integration.conftest import run_alembic, to_psycopg_conninfo

    schema = f"qit_{uuid.uuid4().hex}"
    admin = psycopg.connect(to_psycopg_conninfo(test_database_url))
    admin.autocommit = True
    try:
        admin.execute(f'CREATE SCHEMA "{schema}"')
    finally:
        admin.close()

    run_alembic("upgrade", "head", schema=schema, database_url=test_database_url)
    engine = _api_engine(test_database_url)
    try:
        yield engine, schema, test_database_url
    finally:
        engine.dispose()
        drop = psycopg.connect(to_psycopg_conninfo(test_database_url))
        drop.autocommit = True
        try:
            drop.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        finally:
            drop.close()


def _psycopg_url(url: str) -> str:
    from tests.integration.conftest import to_psycopg_conninfo

    return to_psycopg_conninfo(url)


def _utc_today(conn: psycopg.Connection) -> date:
    with conn.cursor() as cur:
        cur.execute("SELECT (CURRENT_TIMESTAMP AT TIME ZONE 'UTC')::date")
        return cur.fetchone()[0]


def _drop_parent_day(
    conn: psycopg.Connection, schema: str, parent: str, day: date
) -> None:
    suffix = day.strftime("%Y%m%d")
    conn.rollback()
    conn.autocommit = True
    try:
        conn.execute(f'DROP TABLE IF EXISTS "{schema}"."{parent}_{suffix}"')
    finally:
        conn.autocommit = False


def _insert_history_noise(conn: Connection, day: date, rows: int) -> None:
    """Insert retained history rows that must not affect catalog readiness cost."""
    for i in range(rows):
        conn.execute(
            text(
                """
                INSERT INTO admin_audit_log (
                    audit_at, actor_id, operation_code, request_id, details
                ) VALUES (
                    CAST(:ts AS timestamptz),
                    :actor,
                    1,
                    gen_random_uuid(),
                    '{}'::jsonb
                )
                """
            ),
            {
                "ts": f"{day.isoformat()}T12:00:00+00",
                "actor": f"noise-{i}",
            },
        )
    conn.commit()


def test_readiness_fails_at_safety_boundary_with_limiting_parent(
    readiness_schema: tuple[Engine, str, str],
) -> None:
    engine, schema, url = readiness_schema
    conn = psycopg.connect(_psycopg_url(url))
    try:
        conn.execute(f'SET search_path TO "{schema}"')
        today = _utc_today(conn)
        # Exact Phase 3.1 safety boundary: drop the final required day on one parent.
        # Write coverage still exists through today+SAFE_HORIZON-1.
        boundary = today + timedelta(days=SAFE_HORIZON)
        _drop_parent_day(conn, schema, LIMITING_PARENT, boundary)
    finally:
        conn.close()

    live = health.check_liveness()
    assert live.ok is True
    assert live.reason_code is None

    started = time.monotonic()
    status = _check_readiness(engine, schema=schema, premake_days=SAFE_HORIZON)
    elapsed = time.monotonic() - started

    assert status.ok is False
    assert status.reason_code == health.ReasonCode.PARTITION_HORIZON_UNSAFE
    assert status.partition is not None
    assert status.partition.safe is False
    assert status.partition.limiting_parent == LIMITING_PARENT
    assert status.partition.required_horizon_days == SAFE_HORIZON
    assert status.partition.remaining_utc_days == SAFE_HORIZON - 1
    assert status.partition.through_day == today + timedelta(days=SAFE_HORIZON - 1)
    assert status.partition.reason_code == health.ReasonCode.PARTITION_HORIZON_UNSAFE
    assert elapsed < 5.0
    assert "select" not in repr(status).lower()


def test_malformed_gap_makes_aggregate_unready_with_stable_code(
    readiness_schema: tuple[Engine, str, str],
) -> None:
    engine, schema, url = readiness_schema
    conn = psycopg.connect(_psycopg_url(url))
    try:
        conn.execute(f'SET search_path TO "{schema}"')
        today = _utc_today(conn)
        interior = today + timedelta(days=10)
        _drop_parent_day(conn, schema, LIMITING_PARENT, interior)
    finally:
        conn.close()

    status = _check_readiness(engine, schema=schema, premake_days=SAFE_HORIZON)
    assert status.ok is False
    assert status.reason_code == health.ReasonCode.PARTITION_MISSING
    assert status.partition is not None
    assert status.partition.safe is False
    assert status.partition.limiting_parent == LIMITING_PARENT
    assert status.partition.reason_code == health.ReasonCode.PARTITION_MISSING
    assert status.partition.required_horizon_days == SAFE_HORIZON


def test_safe_unequal_surplus_reports_true_min_limiting_parent(
    readiness_schema: tuple[Engine, str, str],
) -> None:
    """All parents safe with unequal surplus → true min remaining + limiting parent.

    Regression for init-to-premake_days: the loop must still select min(remaining)
    when every parent remains at or above the configured horizon.
    """
    engine, schema, url = readiness_schema
    # Horizon below migration coverage so every parent starts with surplus.
    check_horizon = 20
    true_min_remaining = 21
    limiting = "task_attempts"
    # Other parents keep more headroom than the limiting parent (still safe).
    richer_remaining = 25

    conn = psycopg.connect(_psycopg_url(url))
    try:
        conn.execute(f'SET search_path TO "{schema}"')
        today = _utc_today(conn)
        # Trim limiting parent to true_min_remaining (still >= check_horizon).
        for offset in range(true_min_remaining + 1, SAFE_HORIZON + 1):
            _drop_parent_day(conn, schema, limiting, today + timedelta(days=offset))
        # Trim a lex-earlier parent to a higher remaining so it must not win.
        for offset in range(richer_remaining + 1, SAFE_HORIZON + 1):
            _drop_parent_day(
                conn, schema, "admin_audit_log", today + timedelta(days=offset)
            )
    finally:
        conn.close()

    status = _check_readiness(engine, schema=schema, premake_days=check_horizon)
    assert status.ok is True
    assert status.reason_code is None
    assert status.partition is not None
    assert status.partition.safe is True
    assert status.partition.limiting_parent == limiting
    assert status.partition.remaining_utc_days == true_min_remaining
    assert status.partition.through_day == today + timedelta(days=true_min_remaining)
    assert status.partition.required_horizon_days == check_horizon
    assert status.partition.reason_code is None


def test_premake_restores_readiness_without_changing_liveness(
    readiness_schema: tuple[Engine, str, str],
) -> None:
    engine, schema, url = readiness_schema
    conn = psycopg.connect(_psycopg_url(url))
    try:
        conn.execute(f'SET search_path TO "{schema}"')
        today = _utc_today(conn)
        boundary = today + timedelta(days=SAFE_HORIZON)
        for parent in DAILY_RANGE_PARENTS:
            _drop_parent_day(conn, schema, parent, boundary)
    finally:
        conn.close()

    before_live = health.check_liveness()
    assert before_live.ok is True

    unsafe = _check_readiness(engine, schema=schema, premake_days=SAFE_HORIZON)
    assert unsafe.ok is False
    assert unsafe.reason_code == health.ReasonCode.PARTITION_HORIZON_UNSAFE
    assert unsafe.partition is not None
    assert unsafe.partition.safe is False

    sa = create_engine(url, pool_pre_ping=True)
    try:
        with sa.connect() as held:
            held.execute(text(f'SET search_path TO "{schema}"'))
            result = partition_premake.premake_daily_partitions(
                held, horizon_days=SAFE_HORIZON
            )
            held.commit()
            assert result.through_day == today + timedelta(days=SAFE_HORIZON)
    finally:
        sa.dispose()

    after_live = health.check_liveness()
    assert after_live.ok is True
    assert after_live.reason_code is None

    restored = _check_readiness(engine, schema=schema, premake_days=SAFE_HORIZON)
    assert restored.ok is True
    assert restored.reason_code is None
    assert restored.partition is not None
    assert restored.partition.safe is True
    assert restored.partition.required_horizon_days == SAFE_HORIZON
    assert restored.partition.remaining_utc_days >= SAFE_HORIZON
    assert restored.partition.through_day >= today + timedelta(days=SAFE_HORIZON)
    assert restored.partition.limiting_parent in DAILY_RANGE_PARENTS
    assert restored.partition.reason_code is None


def test_readiness_is_catalog_bounded_not_history_row_count(
    readiness_schema: tuple[Engine, str, str],
) -> None:
    engine, schema, url = readiness_schema

    health_src_path = Path(inspect.getsourcefile(health) or "")
    assert health_src_path.is_file()
    health_text = health_src_path.read_text(encoding="utf-8")
    tree = ast.parse(health_text)
    sql_literals: list[str] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Constant) and isinstance(node.value, str):
            sql_literals.append(node.value.lower())
    joined = "\n".join(sql_literals)
    for parent in DAILY_RANGE_PARENTS:
        assert f"from {parent}" not in joined
        assert f"into {parent}" not in joined
        assert f"join {parent}" not in joined
    assert "partition_catalog" in health_text

    sa = create_engine(url, pool_pre_ping=True)
    try:
        with sa.connect() as held:
            held.execute(text(f'SET search_path TO "{schema}"'))
            today = partition_catalog.store_utc_today(held)
            _insert_history_noise(held, today, rows=200)
    finally:
        sa.dispose()

    started = time.monotonic()
    status = _check_readiness(engine, schema=schema, premake_days=SAFE_HORIZON)
    elapsed = time.monotonic() - started
    assert status.ok is True
    assert status.partition is not None
    assert status.partition.safe is True
    assert elapsed < 5.0


def test_retention_kwargs_still_rejected(
    readiness_schema: tuple[Engine, str, str],
) -> None:
    engine, schema, _url = readiness_schema
    with pytest.raises(TypeError):
        health.check_readiness(  # type: ignore[call-arg]
            engine,
            schema=schema,
            retention_lag_seconds=10_000,
        )
