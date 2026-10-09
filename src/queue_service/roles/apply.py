"""One-shot ensure-exists named-queue catalog apply role (CTRL-10 / D-11).

Validates ``QUEUE_CATALOG_PATH``, acquires a dedicated apply advisory lock, then
creates missing named queues through ``QueueControlRepository.create_named_queue``.
Existing queues are name-only skipped (no pause/drain/policy activate). Never
starts HTTP listeners and is not invoked from API startup.
"""

from __future__ import annotations

import os
import re
import sys
import time
import uuid
from collections.abc import Sequence
from pathlib import Path
from typing import Final

from sqlalchemy import event, text
from sqlalchemy.engine import Connection, Engine
from sqlalchemy.orm import Session, sessionmaker

from queue_service import __version__
from queue_service.db import create_role_engine
from queue_service.domain.catalog import CatalogEntry, parse_catalog_bytes
from queue_service.domain.queue_control import (
    RETRY_DELAY_SECONDS_ABSOLUTE_MAX,
    AdminRequestMetadata,
    CreateQueueMutation,
    DomainValidationError,
)
from queue_service.infrastructure.postgres.queue_control_repository import (
    QueueControlRepository,
)
from queue_service.observability.error_reporting import maybe_init_error_reporting
from queue_service.settings import (
    DeploymentSettings,
    SettingsValidationError,
    from_environ,
    sentry_dsn_from_environ,
)

# ASCII "QUEUAPLY" — must stay distinct from migrate ("QUEUEMIGR") and maintain
# ("QUEUEMAIN").
APPLY_ADVISORY_LOCK_KEY: Final[int] = 0x5155455541504C59
_MIGRATE_LOCK_KEY_DOCUMENTED: Final[int] = 0x515545554D494752
_MAINTAIN_LOCK_KEY_DOCUMENTED: Final[int] = 0x515545554D41494E

EXIT_OK: Final[int] = 0
EXIT_APPLY_FAILED: Final[int] = 1
EXIT_USAGE: Final[int] = 2
EXIT_LOCK_TIMEOUT: Final[int] = 4
EXIT_DEPENDENCY: Final[int] = 5

_DEFAULT_LOCK_DEADLINE_SECONDS: Final[float] = 30.0
_LOCK_POLL_INTERVAL_SECONDS: Final[float] = 0.05
_ACTOR_ID: Final[str] = "catalog_apply"
_IDEMPOTENCY_PREFIX: Final[str] = "catalog-apply:v1:"

_TRY_LOCK_SQL = text("SELECT pg_try_advisory_lock(:key)")
_UNLOCK_SQL = text("SELECT pg_advisory_unlock(:key)")
_SCHEMA_NAME_RE: Final[re.Pattern[str]] = re.compile(r"^[a-z_][a-z0-9_]*$")

assert APPLY_ADVISORY_LOCK_KEY != _MIGRATE_LOCK_KEY_DOCUMENTED
assert APPLY_ADVISORY_LOCK_KEY != _MAINTAIN_LOCK_KEY_DOCUMENTED
assert APPLY_ADVISORY_LOCK_KEY != 0


def settings_from_env() -> DeploymentSettings | None:
    """Build DeploymentSettings from the process environment."""
    return from_environ()


def _emit(message: str) -> None:
    """Emit one sanitized stderr line (no DSN/SQL/secrets/catalog body)."""
    print(f"apply: {message}", file=sys.stderr, flush=True)


def _acquire_lock(conn: Connection, *, deadline_seconds: float) -> bool:
    deadline = time.monotonic() + max(0.0, deadline_seconds)
    while True:
        acquired = bool(
            conn.execute(_TRY_LOCK_SQL, {"key": APPLY_ADVISORY_LOCK_KEY}).scalar()
        )
        if acquired:
            return True
        if time.monotonic() >= deadline:
            return False
        time.sleep(_LOCK_POLL_INTERVAL_SECONDS)


def _release_lock(conn: Connection) -> None:
    try:
        conn.execute(_UNLOCK_SQL, {"key": APPLY_ADVISORY_LOCK_KEY})
    except Exception:
        # Session close / disconnect still drops session-level advisory locks.
        pass


def _ensure_search_path(engine: Engine, schema: str) -> None:
    if not _SCHEMA_NAME_RE.fullmatch(schema):
        raise ValueError(f"refusing unsafe schema name: {schema!r}")
    key = f"_queue_apply_search_path_{schema}"
    if getattr(engine, key, False):
        return

    def _on_connect(dbapi_conn: object, _connection_record: object) -> None:
        # Autocommit so SET is not rolled back when SQLAlchemy begins a TX.
        previous = dbapi_conn.autocommit  # type: ignore[attr-defined]
        dbapi_conn.autocommit = True  # type: ignore[attr-defined]
        cursor = dbapi_conn.cursor()  # type: ignore[attr-defined]
        try:
            cursor.execute(f'SET search_path TO "{schema}"')
        finally:
            cursor.close()
            dbapi_conn.autocommit = previous  # type: ignore[attr-defined]

    event.listen(engine, "connect", _on_connect)
    setattr(engine, key, True)


def _parse_lock_deadline() -> float | int:
    """Parse ``QUEUE_APPLY_LOCK_DEADLINE_SECONDS``; return seconds or exit code."""
    lock_deadline = _DEFAULT_LOCK_DEADLINE_SECONDS
    env_deadline = os.environ.get("QUEUE_APPLY_LOCK_DEADLINE_SECONDS", "").strip()
    if env_deadline:
        try:
            lock_deadline = float(env_deadline)
        except ValueError:
            _emit("invalid QUEUE_APPLY_LOCK_DEADLINE_SECONDS")
            return EXIT_USAGE
    if lock_deadline <= 0:
        _emit("lock deadline must be positive")
        return EXIT_USAGE
    return lock_deadline


def _resolve_schema() -> str | None:
    raw = (
        os.environ.get("QUEUE_SCHEMA")
        or os.environ.get("ALEMBIC_VERSION_TABLE_SCHEMA")
        or ""
    ).strip()
    return raw or None


def run_apply(
    deployment: DeploymentSettings,
    entries: tuple[CatalogEntry, ...],
    *,
    lock_deadline_seconds: float = _DEFAULT_LOCK_DEADLINE_SECONDS,
    engine: Engine | None = None,
    schema: str | None = None,
) -> int:
    """Acquire the apply lock, ensure-exists each catalog entry, release, exit code."""
    owns_engine = engine is None
    role_engine = engine if engine is not None else create_role_engine(deployment, "apply")
    lock_conn: Connection | None = None
    locked = False
    resolved_schema = schema if schema is not None else _resolve_schema()
    try:
        if resolved_schema is not None:
            _ensure_search_path(role_engine, resolved_schema)

        lock_conn = role_engine.connect()
        if not _acquire_lock(lock_conn, deadline_seconds=lock_deadline_seconds):
            _emit("lock_timeout")
            return EXIT_LOCK_TIMEOUT
        locked = True

        if not entries:
            return EXIT_OK

        factory = sessionmaker(bind=role_engine, expire_on_commit=False)
        repository = QueueControlRepository()
        run_request_id = str(uuid.uuid4())

        for entry in entries:
            session: Session = factory()
            try:
                existing = repository.get_queue_configuration(session, name=entry.name)
                if existing is not None:
                    session.rollback()
                    continue

                mutation = CreateQueueMutation(
                    name=entry.name,
                    initial_policy=entry.initial_policy,
                    metadata=AdminRequestMetadata(
                        actor_id=_ACTOR_ID,
                        request_id=run_request_id,
                        idempotency_key=f"{_IDEMPOTENCY_PREFIX}{entry.name}",
                    ),
                )
                try:
                    repository.create_named_queue(session, mutation)
                    session.commit()
                except DomainValidationError as exc:
                    session.rollback()
                    if exc.code == "idempotency_conflict":
                        continue
                    _emit("apply_failed")
                    return EXIT_APPLY_FAILED
                except Exception:
                    session.rollback()
                    _emit("apply_failed")
                    return EXIT_APPLY_FAILED
            finally:
                session.close()

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


def run(argv: Sequence[str]) -> int:
    """CLI entry used by ``queue apply`` / ``python -m queue_service apply``."""
    args = list(argv)
    if args and args[0] in {"-h", "--help"}:
        print("usage: queue apply", file=sys.stderr)
        return EXIT_USAGE
    if args:
        _emit(f"unknown argument {args[0]!r}")
        return EXIT_USAGE

    maybe_init_error_reporting(
        dsn=sentry_dsn_from_environ(),
        environment=os.environ.get("QUEUE_ENVIRONMENT", "development").strip()
        or "development",
        release=f"queue@{__version__}",
        process_role="apply",
    )

    catalog_path_raw = os.environ.get("QUEUE_CATALOG_PATH", "").strip()
    if not catalog_path_raw:
        _emit("QUEUE_CATALOG_PATH is required")
        return EXIT_USAGE

    catalog_path = Path(catalog_path_raw)
    if not catalog_path.is_absolute():
        _emit("QUEUE_CATALOG_PATH must be an absolute path")
        return EXIT_USAGE
    try:
        if not catalog_path.is_file():
            _emit("QUEUE_CATALOG_PATH must be an existing readable file")
            return EXIT_USAGE
        raw = catalog_path.read_bytes()
    except OSError:
        _emit("QUEUE_CATALOG_PATH must be an existing readable file")
        return EXIT_USAGE

    try:
        entries = parse_catalog_bytes(raw, ceiling=RETRY_DELAY_SECONDS_ABSOLUTE_MAX)
    except DomainValidationError:
        _emit("catalog_invalid")
        return EXIT_USAGE

    parsed_deadline = _parse_lock_deadline()
    if isinstance(parsed_deadline, int):
        return parsed_deadline
    lock_deadline = parsed_deadline

    try:
        deployment = settings_from_env()
    except SettingsValidationError as exc:
        _emit(str(exc))
        return EXIT_DEPENDENCY
    if deployment is None:
        _emit("DATABASE_URL is required")
        return EXIT_DEPENDENCY

    return run_apply(
        deployment,
        entries,
        lock_deadline_seconds=lock_deadline,
    )
