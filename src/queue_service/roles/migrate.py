"""One-shot single-winner Alembic migrate role (DEP-01 / PKG-01).

Acquires a fixed Queue migrate advisory lock, upgrades to head via the
established Alembic configuration, releases resources, and exits. Never starts
HTTP listeners and never runs from API startup.
"""

from __future__ import annotations

import os
import sys
import time
from collections.abc import Callable, Sequence
from pathlib import Path
from typing import Final

from alembic import command
from alembic.config import Config
from sqlalchemy import text
from sqlalchemy.engine import Connection, Engine

from queue_service import __version__
from queue_service.db import create_role_engine
from queue_service.observability.error_reporting import maybe_init_error_reporting
from queue_service.settings import (
    DeploymentSettings,
    SettingsValidationError,
    from_environ,
    sentry_dsn_from_environ,
)

# ASCII "QUEUEMIGR" — must stay distinct from Plan 10 maintain lock ("QUEUEMAIN").
MIGRATE_ADVISORY_LOCK_KEY: Final[int] = 0x515545554D494752
# Documented sibling key for maintain (Plan 10); never acquire here.
_MAINTENANCE_LOCK_KEY_DOCUMENTED: Final[int] = 0x515545554D41494E

EXIT_OK: Final[int] = 0
EXIT_MIGRATION_FAILED: Final[int] = 1
EXIT_USAGE: Final[int] = 2
EXIT_LOCK_TIMEOUT: Final[int] = 4
EXIT_DEPENDENCY: Final[int] = 5

_DEFAULT_LOCK_DEADLINE_SECONDS: Final[float] = 30.0
_LOCK_POLL_INTERVAL_SECONDS: Final[float] = 0.05

_TRY_LOCK_SQL = text("SELECT pg_try_advisory_lock(:key)")
_UNLOCK_SQL = text("SELECT pg_advisory_unlock(:key)")

UpgradeFn = Callable[..., None]

assert MIGRATE_ADVISORY_LOCK_KEY != _MAINTENANCE_LOCK_KEY_DOCUMENTED


def _alembic_ini_path() -> Path:
    """Resolve alembic.ini from the process working directory or package parents."""
    cwd_candidate = Path.cwd() / "alembic.ini"
    if cwd_candidate.is_file():
        return cwd_candidate
    here = Path(__file__).resolve()
    for parent in here.parents:
        candidate = parent / "alembic.ini"
        if candidate.is_file():
            return candidate
    raise FileNotFoundError("alembic.ini not found from cwd or package path")


def settings_from_env() -> DeploymentSettings | None:
    """Build DeploymentSettings from the process environment."""
    return from_environ()


def _emit(message: str) -> None:
    """Emit one sanitized stderr line (no DSN/SQL/secrets)."""
    print(f"migrate: {message}", file=sys.stderr, flush=True)


def _acquire_lock(conn: Connection, *, deadline_seconds: float) -> bool:
    deadline = time.monotonic() + max(0.0, deadline_seconds)
    while True:
        acquired = bool(conn.execute(_TRY_LOCK_SQL, {"key": MIGRATE_ADVISORY_LOCK_KEY}).scalar())
        if acquired:
            return True
        if time.monotonic() >= deadline:
            return False
        time.sleep(_LOCK_POLL_INTERVAL_SECONDS)


def _release_lock(conn: Connection) -> None:
    try:
        conn.execute(_UNLOCK_SQL, {"key": MIGRATE_ADVISORY_LOCK_KEY})
    except Exception:
        # Session close / disconnect still drops session-level advisory locks.
        pass


def apply_upgrade_to_head(*, schema: str | None, database_url: str) -> None:
    """Apply Alembic upgrade to head using the repo alembic.ini + env.py."""
    previous_url = os.environ.get("DATABASE_URL")
    previous_schema = os.environ.get("ALEMBIC_VERSION_TABLE_SCHEMA")
    os.environ["DATABASE_URL"] = database_url
    if schema:
        os.environ["ALEMBIC_VERSION_TABLE_SCHEMA"] = schema
    try:
        cfg = Config(str(_alembic_ini_path()))
        command.upgrade(cfg, "head")
    finally:
        if previous_url is None:
            os.environ.pop("DATABASE_URL", None)
        else:
            os.environ["DATABASE_URL"] = previous_url
        if previous_schema is None:
            os.environ.pop("ALEMBIC_VERSION_TABLE_SCHEMA", None)
        else:
            os.environ["ALEMBIC_VERSION_TABLE_SCHEMA"] = previous_schema


def run_migrate(
    deployment: DeploymentSettings,
    *,
    schema: str | None = None,
    lock_deadline_seconds: float = _DEFAULT_LOCK_DEADLINE_SECONDS,
    upgrade_to_head: UpgradeFn | None = None,
    engine: Engine | None = None,
) -> int:
    """Acquire the migrate lock, upgrade to head, release, and return exit code.

    Uses only the migrator role pool. Never starts HTTP listeners.
    """
    upgrade = apply_upgrade_to_head if upgrade_to_head is None else upgrade_to_head
    owns_engine = engine is None
    role_engine = engine if engine is not None else create_role_engine(deployment, "migrate")
    lock_conn: Connection | None = None
    locked = False
    try:
        lock_conn = role_engine.connect()
        if not _acquire_lock(lock_conn, deadline_seconds=lock_deadline_seconds):
            _emit("lock_timeout")
            return EXIT_LOCK_TIMEOUT
        locked = True

        database_url = deployment.database_url.get_secret_value()
        try:
            upgrade(schema=schema, database_url=database_url)
        except Exception:
            _emit("migration_failed")
            return EXIT_MIGRATION_FAILED

        return EXIT_OK
    except SettingsValidationError:
        _emit("settings_invalid")
        return EXIT_DEPENDENCY
    except Exception:
        _emit("dependency_failed")
        return EXIT_DEPENDENCY
    finally:
        if lock_conn is not None:
            if locked:
                _release_lock(lock_conn)
            try:
                lock_conn.close()
            except Exception:
                pass
        if owns_engine:
            try:
                role_engine.dispose()
            except Exception:
                pass


def _parse_argv(argv: Sequence[str]) -> tuple[str | None, float] | int:
    schema: str | None = os.environ.get("ALEMBIC_VERSION_TABLE_SCHEMA") or os.environ.get(
        "QUEUE_SCHEMA"
    )
    if schema is not None:
        schema = schema.strip() or None

    lock_deadline = _DEFAULT_LOCK_DEADLINE_SECONDS
    env_deadline = os.environ.get("QUEUE_MIGRATE_LOCK_DEADLINE_SECONDS", "").strip()
    if env_deadline:
        try:
            lock_deadline = float(env_deadline)
        except ValueError:
            _emit("invalid QUEUE_MIGRATE_LOCK_DEADLINE_SECONDS")
            return EXIT_USAGE

    args = list(argv)
    i = 0
    while i < len(args):
        arg = args[i]
        if arg in {"-h", "--help"}:
            print(
                "usage: queue migrate [--schema NAME] [--lock-deadline-seconds SEC]",
                file=sys.stderr,
            )
            return EXIT_USAGE
        if arg == "--schema":
            i += 1
            if i >= len(args):
                _emit("--schema requires a value")
                return EXIT_USAGE
            schema = args[i]
        elif arg.startswith("--schema="):
            schema = arg.split("=", 1)[1]
        elif arg == "--lock-deadline-seconds":
            i += 1
            if i >= len(args):
                _emit("--lock-deadline-seconds requires a value")
                return EXIT_USAGE
            try:
                lock_deadline = float(args[i])
            except ValueError:
                _emit("invalid --lock-deadline-seconds")
                return EXIT_USAGE
        elif arg.startswith("--lock-deadline-seconds="):
            try:
                lock_deadline = float(arg.split("=", 1)[1])
            except ValueError:
                _emit("invalid --lock-deadline-seconds")
                return EXIT_USAGE
        else:
            _emit(f"unknown argument {arg!r}")
            return EXIT_USAGE
        i += 1

    if lock_deadline <= 0:
        _emit("lock deadline must be positive")
        return EXIT_USAGE
    return schema, lock_deadline


def run(argv: Sequence[str]) -> int:
    """CLI entry used by ``queue migrate`` / ``python -m queue_service migrate``."""
    parsed = _parse_argv(argv)
    if isinstance(parsed, int):
        return parsed
    schema, lock_deadline = parsed

    maybe_init_error_reporting(
        dsn=sentry_dsn_from_environ(),
        environment=os.environ.get("QUEUE_ENVIRONMENT", "development").strip() or "development",
        release=f"queue@{__version__}",
        process_role="migrate",
    )

    try:
        deployment = settings_from_env()
    except SettingsValidationError as exc:
        _emit(str(exc))
        return EXIT_DEPENDENCY
    if deployment is None:
        _emit("DATABASE_URL is required")
        return EXIT_DEPENDENCY

    return run_migrate(
        deployment,
        schema=schema,
        lock_deadline_seconds=lock_deadline,
    )
