"""Real-PostgreSQL liveness/readiness matrix (OPS-01, DEP-01)."""

from __future__ import annotations

import time
import uuid
from collections.abc import Iterator
from datetime import date, timedelta

import psycopg
import pytest
from sqlalchemy import text
from sqlalchemy.engine import Engine

from workhold import db, health, settings

# Helpers live in the integration conftest; import lazily where needed so
# collection does not depend on package layout quirks.


DAILY_RANGE_PARENTS = (
    "admin_audit_log",
    "task_attempts",
    "tasks_terminal",
    "delivery_events_terminal",
)

PREMAKE_DAYS = 30

# Extended order used only to classify below/above relative to the binary range.
TEST_REVISION_ORDER = (
    "0000_too_old",
    "0001_physical_contract_foundations",
    "0002_too_new",
)

PRIOR_COMPATIBLE_REVISION = "0502_delivery_pending_generation"
FUTURE_REVISION = "9999_future_head"


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
                principal_id="health-test",
                generation_id="gen-1",
                secret=settings.Secret("token"),
            ),
        ),
    )


def _api_engine(database_url: str, **pool_kwargs: object) -> Engine:
    return db.create_role_engine(_settings_for_url(database_url, **pool_kwargs), "api")


@pytest.fixture
def health_schema(test_database_url: str) -> Iterator[tuple[Engine, str, str]]:
    """Fresh migrated schema + API-role engine; always cleaned up."""
    from tests.integration.conftest import run_alembic, to_psycopg_conninfo

    schema = f"qit_{uuid.uuid4().hex}"
    admin = psycopg.connect(to_psycopg_conninfo(test_database_url))
    admin.autocommit = True
    try:
        admin.execute(f'CREATE SCHEMA "{schema}"')
    finally:
        admin.close()

    # Stay within the binary-compatible window so readiness can reach partition
    # probes; alembic head may be ahead of BINARY_COMPATIBLE_MAX (040+).
    run_alembic(
        "upgrade",
        health.BINARY_COMPATIBLE_MAX,
        schema=schema,
        database_url=test_database_url,
    )
    engine = _api_engine(test_database_url)
    try:
        yield engine, schema, test_database_url
    finally:
        engine.dispose()
        # Always DROP CASCADE: tests may leave a non-catalog alembic_version
        # that Alembic cannot resolve for downgrade.
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


def _psycopg_url(url: str) -> str:
    from tests.integration.conftest import to_psycopg_conninfo

    return to_psycopg_conninfo(url)


def test_liveness_is_db_independent(monkeypatch: pytest.MonkeyPatch) -> None:
    """Liveness must succeed without constructing a DB engine."""
    created: list[object] = []

    def _forbid_create_engine(*args: object, **kwargs: object) -> object:
        created.append((args, kwargs))
        raise AssertionError("liveness must not create a DB engine")

    monkeypatch.setattr(db, "create_engine", _forbid_create_engine)
    status = health.check_liveness()
    assert status.ok is True
    assert status.reason_code is None
    assert created == []


def test_liveness_ok_while_readiness_sees_unreachable_postgres(
    test_database_url: str,
) -> None:
    """Process liveness stays ok when readiness cannot reach PostgreSQL."""
    from tests.integration.conftest import require_test_database_url

    bad_url = "postgresql+psycopg://queue:queue@127.0.0.1:1/queue"
    engine = _api_engine(bad_url, acquisition_timeout=1.0, statement_timeout=1.0)
    try:
        live = health.check_liveness()
        assert live.ok is True
        assert live.reason_code is None

        started = time.monotonic()
        ready = health.check_readiness(engine)
        elapsed = time.monotonic() - started
        assert ready.ok is False
        assert ready.reason_code == health.ReasonCode.POSTGRES_UNAVAILABLE
        assert elapsed < 5.0
        assert "postgresql" not in (ready.reason_code or "")
        assert "127.0.0.1" not in repr(ready)
    finally:
        engine.dispose()
    # Shared fixture DB remains usable.
    require_test_database_url()
    assert test_database_url


def test_readiness_ready_on_compatible_schema_and_horizon(
    health_schema: tuple[Engine, str, str],
) -> None:
    engine, schema, _url = health_schema
    started = time.monotonic()
    status = health.check_readiness(engine, schema=schema, premake_days=PREMAKE_DAYS)
    elapsed = time.monotonic() - started
    assert status.ok is True
    assert status.reason_code is None
    assert elapsed < 5.0


def test_non_correctness_deps_do_not_gate_readiness(
    health_schema: tuple[Engine, str, str],
) -> None:
    """Statistics / relay / retention lag are not readiness inputs or kwargs."""
    engine, schema, _url = health_schema
    with pytest.raises(TypeError):
        health.check_readiness(  # type: ignore[call-arg]
            engine,
            schema=schema,
            statistics_stale=True,
            relay_configured=False,
            retention_lag_seconds=10_000,
        )
    status = health.check_readiness(engine, schema=schema, premake_days=PREMAKE_DAYS)
    assert status.ok is True
    assert status.reason_code is None


def test_schema_below_range_not_ready(
    health_schema: tuple[Engine, str, str],
    test_database_url: str,
) -> None:
    engine, schema, _url = health_schema
    conn = psycopg.connect(_psycopg_url(test_database_url))
    try:
        _set_alembic_revision(conn, schema, "0000_too_old")
    finally:
        conn.close()

    started = time.monotonic()
    status = health.check_readiness(
        engine,
        schema=schema,
        premake_days=PREMAKE_DAYS,
        compatible_min="0001_physical_contract_foundations",
        compatible_max="0001_physical_contract_foundations",
        revision_order=TEST_REVISION_ORDER,
    )
    elapsed = time.monotonic() - started
    assert status.ok is False
    assert status.reason_code == health.ReasonCode.SCHEMA_BELOW_RANGE
    assert elapsed < 5.0


def test_schema_above_range_not_ready(
    health_schema: tuple[Engine, str, str],
    test_database_url: str,
) -> None:
    engine, schema, _url = health_schema
    conn = psycopg.connect(_psycopg_url(test_database_url))
    try:
        _set_alembic_revision(conn, schema, "0002_too_new")
    finally:
        conn.close()

    started = time.monotonic()
    status = health.check_readiness(
        engine,
        schema=schema,
        premake_days=PREMAKE_DAYS,
        compatible_min="0001_physical_contract_foundations",
        compatible_max="0001_physical_contract_foundations",
        revision_order=TEST_REVISION_ORDER,
    )
    elapsed = time.monotonic() - started
    assert status.ok is False
    assert status.reason_code == health.ReasonCode.SCHEMA_ABOVE_RANGE
    assert elapsed < 5.0


def test_readiness_ready_at_prior_revision_within_binary_window(
    test_database_url: str,
) -> None:
    """0502 remains compatible after 1201 becomes BINARY_COMPATIBLE_MAX."""
    from tests.integration.conftest import run_alembic, to_psycopg_conninfo

    schema = f"qit_{uuid.uuid4().hex}"
    admin = psycopg.connect(to_psycopg_conninfo(test_database_url))
    admin.autocommit = True
    try:
        admin.execute(f'CREATE SCHEMA "{schema}"')
    finally:
        admin.close()

    run_alembic(
        "upgrade",
        PRIOR_COMPATIBLE_REVISION,
        schema=schema,
        database_url=test_database_url,
    )
    engine = _api_engine(test_database_url)
    try:
        status = health.check_readiness(engine, schema=schema, premake_days=PREMAKE_DAYS)
        assert status.ok is True
        assert status.reason_code is None
    finally:
        engine.dispose()
        drop = psycopg.connect(to_psycopg_conninfo(test_database_url))
        drop.autocommit = True
        try:
            drop.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        finally:
            drop.close()


def test_readiness_rejects_revision_above_binary_compatible_max(
    health_schema: tuple[Engine, str, str],
    test_database_url: str,
) -> None:
    engine, schema, _url = health_schema
    conn = psycopg.connect(_psycopg_url(test_database_url))
    try:
        _set_alembic_revision(conn, schema, FUTURE_REVISION)
    finally:
        conn.close()

    status = health.check_readiness(engine, schema=schema, premake_days=PREMAKE_DAYS)
    assert status.ok is False
    assert status.reason_code == health.ReasonCode.SCHEMA_ABOVE_RANGE


def test_missing_interior_partition_not_ready(
    health_schema: tuple[Engine, str, str],
    test_database_url: str,
) -> None:
    engine, schema, _url = health_schema
    conn = psycopg.connect(_psycopg_url(test_database_url))
    try:
        conn.execute(f'SET search_path TO "{schema}"')
        today = _utc_today(conn)
        interior = today + timedelta(days=10)
        _drop_partition_day(conn, schema, interior)
    finally:
        conn.close()

    started = time.monotonic()
    status = health.check_readiness(engine, schema=schema, premake_days=PREMAKE_DAYS)
    elapsed = time.monotonic() - started
    assert status.ok is False
    assert status.reason_code == health.ReasonCode.PARTITION_MISSING
    assert elapsed < 5.0


def test_missing_end_partition_horizon_unsafe(
    health_schema: tuple[Engine, str, str],
    test_database_url: str,
) -> None:
    engine, schema, _url = health_schema
    conn = psycopg.connect(_psycopg_url(test_database_url))
    try:
        conn.execute(f'SET search_path TO "{schema}"')
        today = _utc_today(conn)
        end = today + timedelta(days=PREMAKE_DAYS)
        _drop_partition_day(conn, schema, end)
    finally:
        conn.close()

    started = time.monotonic()
    status = health.check_readiness(engine, schema=schema, premake_days=PREMAKE_DAYS)
    elapsed = time.monotonic() - started
    assert status.ok is False
    assert status.reason_code == health.ReasonCode.PARTITION_HORIZON_UNSAFE
    assert elapsed < 5.0


def test_pool_acquisition_timeout_not_ready(
    health_schema: tuple[Engine, str, str],
) -> None:
    _engine, schema, url = health_schema
    tight = _api_engine(url, pool_ceiling=1, acquisition_timeout=0.5)
    held = tight.connect()
    try:
        started = time.monotonic()
        status = health.check_readiness(tight, schema=schema, premake_days=PREMAKE_DAYS)
        elapsed = time.monotonic() - started
        assert status.ok is False
        assert status.reason_code == health.ReasonCode.POOL_TIMEOUT
        assert elapsed < 3.0
    finally:
        held.close()
        tight.dispose()


def test_statement_timeout_not_ready(
    health_schema: tuple[Engine, str, str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _engine, schema, url = health_schema
    slow = _api_engine(url, statement_timeout=0.05)
    # Force the readiness probe to exceed statement_timeout.
    monkeypatch.setattr(health, "_READINESS_PING_SQL", text("SELECT pg_sleep(1)"))
    try:
        started = time.monotonic()
        status = health.check_readiness(slow, schema=schema, premake_days=PREMAKE_DAYS)
        elapsed = time.monotonic() - started
        assert status.ok is False
        assert status.reason_code == health.ReasonCode.STATEMENT_TIMEOUT
        assert elapsed < 3.0
        assert "pg_sleep" not in repr(status)
        assert "SELECT" not in repr(status)
    finally:
        slow.dispose()


@pytest.mark.parametrize("version_num", [160000, 180004, 190000])
def test_readiness_rejects_non_18_6_server_version(
    health_schema: tuple[Engine, str, str],
    monkeypatch: pytest.MonkeyPatch,
    version_num: int,
) -> None:
    """Fail closed on 16 / 18.4 / 19 without leaking SQL, DSN, or version_num."""
    engine, schema, _url = health_schema
    monkeypatch.setattr(
        health,
        "_SERVER_VERSION_NUM_SQL",
        text(f"SELECT {version_num}"),
    )
    status = health.check_readiness(engine, schema=schema, premake_days=PREMAKE_DAYS)
    assert status.ok is False
    assert status.reason_code == health.ReasonCode.POSTGRES_VERSION_UNSUPPORTED
    assert "postgresql" not in (status.reason_code or "")
    rendered = repr(status)
    assert "SELECT" not in rendered
    assert "SHOW" not in rendered
    assert str(version_num) not in rendered
    assert "@" not in rendered
    assert "127.0.0.1" not in rendered
    assert "postgresql" not in rendered


def test_reason_codes_are_bounded_and_non_secret() -> None:
    codes = {
        health.ReasonCode.POSTGRES_UNAVAILABLE,
        health.ReasonCode.POOL_TIMEOUT,
        health.ReasonCode.STATEMENT_TIMEOUT,
        health.ReasonCode.SCHEMA_BELOW_RANGE,
        health.ReasonCode.SCHEMA_ABOVE_RANGE,
        health.ReasonCode.PARTITION_MISSING,
        health.ReasonCode.PARTITION_HORIZON_UNSAFE,
        health.ReasonCode.POSTGRES_VERSION_UNSUPPORTED,
    }
    assert len(codes) == 8
    for code in codes:
        assert code.isidentifier() or "_" in code
        assert "password" not in code
        assert "postgresql" not in code
        assert len(code) <= 64
        assert "postgresql" not in code.lower()
