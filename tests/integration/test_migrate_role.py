"""Real-PostgreSQL migrate role: single-winner advisory lock + readiness (DEP-01)."""

from __future__ import annotations

import os
import socket
import subprocess
import sys
import time
import uuid
from collections.abc import Iterator
from pathlib import Path

import psycopg
import pytest
from sqlalchemy.engine import Engine

from queue_service import db, health, settings
from queue_service.roles import migrate as migrate_role

ROOT = Path(__file__).resolve().parents[2]

def _role_pools(
    *,
    pool_ceiling: int = 2,
    replica_ceiling: int = 1,
    acquisition_timeout: float = 5.0,
    statement_timeout: float = 60.0,
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
    statement_timeout: float = 60.0,
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
                principal_id="migrate-test",
                generation_id="gen-1",
                secret=settings.Secret("token"),
            ),
        ),
    )


def _api_engine(database_url: str) -> Engine:
    return db.create_role_engine(_settings_for_url(database_url), "api")


@pytest.fixture
def empty_schema(test_database_url: str) -> Iterator[tuple[str, str]]:
    """Isolated schema with no Queue relations (pre-migration)."""
    from tests.integration.conftest import to_psycopg_conninfo

    schema = f"qmig_{uuid.uuid4().hex}"
    admin = psycopg.connect(to_psycopg_conninfo(test_database_url))
    admin.autocommit = True
    try:
        admin.execute(f'CREATE SCHEMA "{schema}"')
    finally:
        admin.close()
    try:
        yield test_database_url, schema
    finally:
        drop = psycopg.connect(to_psycopg_conninfo(test_database_url))
        drop.autocommit = True
        try:
            drop.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        finally:
            drop.close()


def _hold_migrate_lock(database_url: str) -> psycopg.Connection:
    """Hold the migrate advisory lock on a dedicated connection."""
    from tests.integration.conftest import to_psycopg_conninfo

    conn = psycopg.connect(to_psycopg_conninfo(database_url))
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute(
            "SELECT pg_advisory_lock(%s)",
            (migrate_role.MIGRATE_ADVISORY_LOCK_KEY,),
        )
    return conn


def _subprocess_env(database_url: str, schema: str, *, lock_deadline: float) -> dict[str, str]:
    env = os.environ.copy()
    env["DATABASE_URL"] = database_url
    env["ALEMBIC_VERSION_TABLE_SCHEMA"] = schema
    env["QUEUE_MIGRATE_LOCK_DEADLINE_SECONDS"] = str(lock_deadline)
    # Ensure src layout is importable for `python -m queue_service`.
    src = str(ROOT / "src")
    existing = env.get("PYTHONPATH", "")
    env["PYTHONPATH"] = src if not existing else f"{src}{os.pathsep}{existing}"
    return env


def test_successful_migrate_exits_zero_at_head_and_gates_readiness(
    empty_schema: tuple[str, str],
) -> None:
    database_url, schema = empty_schema
    api = _api_engine(database_url)
    try:
        before = health.check_readiness(api, schema=schema)
        assert before.ok is False

        cfg = _settings_for_url(database_url)
        code = migrate_role.run_migrate(
            cfg,
            schema=schema,
            lock_deadline_seconds=5.0,
        )
        assert code == migrate_role.EXIT_OK

        after = health.check_readiness(api, schema=schema)
        assert after.ok is True
        assert after.reason_code is None
    finally:
        api.dispose()


def test_loser_exits_within_lock_deadline_without_entering_upgrade(
    empty_schema: tuple[str, str],
) -> None:
    database_url, schema = empty_schema
    holder = _hold_migrate_lock(database_url)
    try:
        started = time.monotonic()
        cfg = _settings_for_url(database_url)
        code = migrate_role.run_migrate(
            cfg,
            schema=schema,
            lock_deadline_seconds=1.0,
        )
        elapsed = time.monotonic() - started
        assert code == migrate_role.EXIT_LOCK_TIMEOUT
        assert elapsed < 2.5

        from tests.integration.conftest import (
            assert_schema_has_no_queue_relations,
            to_psycopg_conninfo,
        )

        probe = psycopg.connect(to_psycopg_conninfo(database_url))
        try:
            assert_schema_has_no_queue_relations(probe, schema)
        finally:
            probe.close()
    finally:
        holder.close()


def test_two_concurrent_migrate_subprocesses_single_winner(
    empty_schema: tuple[str, str],
) -> None:
    database_url, schema = empty_schema
    env = _subprocess_env(database_url, schema, lock_deadline=10.0)
    cmd = [sys.executable, "-m", "queue_service", "migrate"]

    # Hold the lock briefly so both children race on release.
    holder = _hold_migrate_lock(database_url)
    procs = [
        subprocess.Popen(
            cmd,
            cwd=str(ROOT),
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        for _ in range(2)
    ]
    time.sleep(0.3)
    holder.close()

    codes: list[int] = []
    for proc in procs:
        out, err = proc.communicate(timeout=60)
        codes.append(proc.returncode)
        assert "Traceback" not in (err or ""), err

    assert migrate_role.EXIT_OK in codes
    assert all(
        code in {migrate_role.EXIT_OK, migrate_role.EXIT_LOCK_TIMEOUT} for code in codes
    )

    # At most one critical-section winner while the other is blocked: after both
    # finish, schema is exactly once at head and readiness admits traffic.
    api = _api_engine(database_url)
    try:
        status = health.check_readiness(api, schema=schema)
        assert status.ok is True
    finally:
        api.dispose()


def test_migration_failure_exits_nonzero_and_readiness_stays_false(
    empty_schema: tuple[str, str],
) -> None:
    database_url, schema = empty_schema
    api = _api_engine(database_url)
    try:
        cfg = _settings_for_url(database_url)

        def _boom(**_kwargs: object) -> None:
            raise RuntimeError("forced migration failure")

        code = migrate_role.run_migrate(
            cfg,
            schema=schema,
            lock_deadline_seconds=5.0,
            upgrade_to_head=_boom,
        )
        assert code == migrate_role.EXIT_MIGRATION_FAILED

        status = health.check_readiness(api, schema=schema)
        assert status.ok is False
    finally:
        api.dispose()


def test_migrate_releases_lock_so_second_run_can_proceed(
    empty_schema: tuple[str, str],
) -> None:
    database_url, schema = empty_schema
    cfg = _settings_for_url(database_url)
    assert (
        migrate_role.run_migrate(cfg, schema=schema, lock_deadline_seconds=5.0)
        == migrate_role.EXIT_OK
    )
    # Second run is a no-op upgrade under the same lock path.
    assert (
        migrate_role.run_migrate(cfg, schema=schema, lock_deadline_seconds=5.0)
        == migrate_role.EXIT_OK
    )


def test_migrate_never_starts_http_listener(empty_schema: tuple[str, str]) -> None:
    database_url, schema = empty_schema
    cfg = _settings_for_url(database_url)
    original_bind = socket.socket.bind

    def _forbid_bind(self: socket.socket, address: object) -> None:  # noqa: ANN001
        raise AssertionError(f"migrate must not bind sockets; got {address!r}")

    socket.socket.bind = _forbid_bind  # type: ignore[method-assign]
    try:
        code = migrate_role.run_migrate(
            cfg,
            schema=schema,
            lock_deadline_seconds=5.0,
        )
        assert code == migrate_role.EXIT_OK
    finally:
        socket.socket.bind = original_bind  # type: ignore[method-assign]


def test_migrate_uses_only_migrate_role_pool(empty_schema: tuple[str, str], monkeypatch: pytest.MonkeyPatch) -> None:
    database_url, schema = empty_schema
    cfg = _settings_for_url(database_url, pool_ceiling=1)
    seen: list[str] = []

    real_create = db.create_role_engine

    def _spy(settings_obj: settings.DeploymentSettings, role: str) -> Engine:
        seen.append(role)
        return real_create(settings_obj, role)

    monkeypatch.setattr(migrate_role, "create_role_engine", _spy)
    code = migrate_role.run_migrate(cfg, schema=schema, lock_deadline_seconds=5.0)
    assert code == migrate_role.EXIT_OK
    assert seen == ["migrate"]


def test_cli_migrate_subprocess_success(empty_schema: tuple[str, str]) -> None:
    database_url, schema = empty_schema
    env = _subprocess_env(database_url, schema, lock_deadline=30.0)
    completed = subprocess.run(
        [sys.executable, "-m", "queue_service", "migrate"],
        cwd=str(ROOT),
        env=env,
        capture_output=True,
        text=True,
        timeout=60,
        check=False,
    )
    assert completed.returncode == migrate_role.EXIT_OK, completed.stderr
    api = _api_engine(database_url)
    try:
        assert health.check_readiness(api, schema=schema).ok is True
    finally:
        api.dispose()


def test_compatible_revision_keeps_readiness_true_after_noop_failure_path(
    empty_schema: tuple[str, str],
) -> None:
    """Failure leaves readiness false unless DB already remains compatible."""
    from tests.integration.conftest import run_alembic

    database_url, schema = empty_schema
    run_alembic("upgrade", "head", schema=schema, database_url=database_url)
    api = _api_engine(database_url)
    try:
        assert health.check_readiness(api, schema=schema).ok is True

        cfg = _settings_for_url(database_url)

        def _boom(**_kwargs: object) -> None:
            raise RuntimeError("upgrade blew up after lock")

        code = migrate_role.run_migrate(
            cfg,
            schema=schema,
            lock_deadline_seconds=5.0,
            upgrade_to_head=_boom,
        )
        assert code == migrate_role.EXIT_MIGRATION_FAILED
        # Schema was already at the binary-compatible revision before the failed
        # upgrade attempt; readiness must remain true.
        assert health.check_readiness(api, schema=schema).ok is True
    finally:
        api.dispose()
