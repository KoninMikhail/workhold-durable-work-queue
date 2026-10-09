"""Bounded one-shot maintain role (PKG-01 / OPS-01 / STOR-03/04/08).

Sole owner of the maintenance advisory lock and dedicated PostgreSQL session.
Passes the held session to composite storage maintenance (premake, bound
verification, history retention, incremental registry purge) and exits with a
typed process report. Lock losers return a successful ``skipped_lock`` result
without DDL, purge, or singleton mutation.
"""

from __future__ import annotations

import os
import re
import signal
import sys
import threading
import time
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime
from typing import Final, Literal

from sqlalchemy import text
from sqlalchemy.engine import Connection, Engine

from workhold import __version__, db, health, settings
from workhold.observability.error_reporting import maybe_init_error_reporting
from workhold.settings import sentry_dsn_from_environ
from workhold.infrastructure.postgres.maintenance import (
    StorageMaintenanceReport,
    run_storage_maintenance,
)

# ASCII "QUEUEMAIN" — single-key advisory lock (Phase 3.8 outer maintain contract).
# Distinct from migrate's two-int lock (class 0x51554555 / id 1) in Plan 08.
MAINTENANCE_LOCK_KEY: Final[int] = 0x515545554D41494E
_MIGRATE_LOCK_CLASS_DOCUMENTED: Final[int] = 0x51554555
_MIGRATE_LOCK_ID_DOCUMENTED: Final[int] = 1

EXIT_OK: Final[int] = 0
EXIT_USAGE: Final[int] = 2
EXIT_UNSAFE: Final[int] = 3
EXIT_LOCK_TIMEOUT: Final[int] = 4  # retained for CLI docs; skipped_lock uses EXIT_OK
EXIT_DEPENDENCY: Final[int] = 5
EXIT_MAINTENANCE_FAILED: Final[int] = 1

_DEFAULT_LOCK_TIMEOUT_SECONDS: Final[float] = 5.0
_LOCK_POLL_INTERVAL_SECONDS: Final[float] = 0.05

_TRY_LOCK_SQL = text("SELECT pg_try_advisory_lock(:key)")
_UNLOCK_SQL = text("SELECT pg_advisory_unlock(:key)")

_SCHEMA_NAME_RE = re.compile(r"^[a-z_][a-z0-9_]*$")

# Single-key bigint space vs migrate's (int,int) space — different PG lock namespaces.
assert MAINTENANCE_LOCK_KEY != 0
assert _MIGRATE_LOCK_CLASS_DOCUMENTED == 0x51554555
assert _MIGRATE_LOCK_ID_DOCUMENTED == 1

MaintainOutcome = Literal["succeeded", "skipped_lock", "failed", "dependency_failure"]
LockOutcome = Literal["acquired", "skipped", "none"]


@dataclass(frozen=True, slots=True)
class MaintainResult:
    """Typed one-shot maintain process result (CLI uses ``exit_code``)."""

    exit_code: int
    outcome: MaintainOutcome
    lock_outcome: LockOutcome
    premade_through: date | None = None
    retained_from: date | None = None
    last_succeeded_at: datetime | None = None
    last_started_at: datetime | None = None
    purge_examined_total: int = 0
    purge_deleted_total: int = 0
    purge_by_registry: Mapping[str, Mapping[str, object]] | None = None
    partitions_created: int = 0
    partitions_detached: int = 0
    partitions_dropped: int = 0
    error_code: str | None = None
    error_detail: str | None = None
    storage: StorageMaintenanceReport | None = None


class _StopFlag:
    """Thread-safe cooperative stop requested by SIGTERM/SIGINT."""

    __slots__ = ("_event",)

    def __init__(self) -> None:
        self._event = threading.Event()

    def set(self) -> None:
        self._event.set()

    def is_set(self) -> bool:
        return self._event.is_set()


def _emit_status(kind: str, reason: str | None) -> None:
    """Emit one bounded low-cardinality status line (no DSN/SQL/secrets)."""
    code = reason if reason else "none"
    print(f"maintain status={kind} reason={code}", flush=True)


def settings_from_env() -> settings.DeploymentSettings | None:
    """Build DeploymentSettings from the process environment."""
    return settings.from_environ()


def _parse_argv(argv: Sequence[str]) -> tuple[str | None, float, int] | int:
    """Parse optional ``--schema``, ``--lock-timeout-seconds``, ``--premake-days``.

    Returns a tuple on success or ``EXIT_USAGE`` on parse failure.
    """
    schema: str | None = os.environ.get("QUEUE_SCHEMA") or os.environ.get(
        "ALEMBIC_VERSION_TABLE_SCHEMA"
    )
    if schema is not None:
        schema = schema.strip() or None
    lock_timeout = _DEFAULT_LOCK_TIMEOUT_SECONDS
    premake_days = health.DEFAULT_PARTITION_PREMAKE_DAYS

    args = list(argv)
    i = 0
    while i < len(args):
        arg = args[i]
        if arg in {"-h", "--help"}:
            print(
                "usage: workhold maintain [--schema NAME] "
                "[--lock-timeout-seconds SEC] [--premake-days N]",
                file=sys.stderr,
            )
            return EXIT_USAGE
        if arg == "--schema":
            i += 1
            if i >= len(args):
                print("maintain: --schema requires a value", file=sys.stderr)
                return EXIT_USAGE
            schema = args[i]
        elif arg.startswith("--schema="):
            schema = arg.split("=", 1)[1]
        elif arg == "--lock-timeout-seconds":
            i += 1
            if i >= len(args):
                print(
                    "maintain: --lock-timeout-seconds requires a value",
                    file=sys.stderr,
                )
                return EXIT_USAGE
            try:
                lock_timeout = float(args[i])
            except ValueError:
                print("maintain: invalid --lock-timeout-seconds", file=sys.stderr)
                return EXIT_USAGE
        elif arg.startswith("--lock-timeout-seconds="):
            try:
                lock_timeout = float(arg.split("=", 1)[1])
            except ValueError:
                print("maintain: invalid --lock-timeout-seconds", file=sys.stderr)
                return EXIT_USAGE
        elif arg == "--premake-days":
            i += 1
            if i >= len(args):
                print("maintain: --premake-days requires a value", file=sys.stderr)
                return EXIT_USAGE
            try:
                premake_days = int(args[i])
            except ValueError:
                print("maintain: invalid --premake-days", file=sys.stderr)
                return EXIT_USAGE
        elif arg.startswith("--premake-days="):
            try:
                premake_days = int(arg.split("=", 1)[1])
            except ValueError:
                print("maintain: invalid --premake-days", file=sys.stderr)
                return EXIT_USAGE
        else:
            print(f"maintain: unknown argument {arg!r}", file=sys.stderr)
            return EXIT_USAGE
        i += 1

    if lock_timeout <= 0:
        print("maintain: --lock-timeout-seconds must be positive", file=sys.stderr)
        return EXIT_USAGE
    if premake_days < 0:
        print("maintain: --premake-days must be non-negative", file=sys.stderr)
        return EXIT_USAGE
    return schema, lock_timeout, premake_days


def _try_acquire_lock(conn: Connection, stop: _StopFlag, deadline: float) -> bool:
    """Poll ``pg_try_advisory_lock`` until acquired, deadline, or stop."""
    while True:
        if stop.is_set():
            return False
        if time.monotonic() >= deadline:
            return False
        row = conn.execute(_TRY_LOCK_SQL, {"key": MAINTENANCE_LOCK_KEY}).one()
        if bool(row[0]):
            return True
        remaining = deadline - time.monotonic()
        if remaining <= 0 or stop.is_set():
            return False
        time.sleep(min(_LOCK_POLL_INTERVAL_SECONDS, max(remaining, 0.0)))


def _set_search_path(conn: Connection, schema: str | None) -> None:
    if schema is None:
        return
    if not _SCHEMA_NAME_RE.fullmatch(schema):
        raise ValueError(f"refusing unsafe schema name: {schema!r}")
    conn.execute(text(f"SET search_path TO {schema}"))
    conn.commit()


def _from_storage(report: StorageMaintenanceReport) -> MaintainResult:
    if report.outcome == "succeeded":
        return MaintainResult(
            exit_code=EXIT_OK,
            outcome="succeeded",
            lock_outcome="acquired",
            premade_through=report.premade_through,
            retained_from=report.retained_from,
            last_succeeded_at=report.last_succeeded_at,
            last_started_at=report.last_started_at,
            purge_examined_total=report.purge_examined_total,
            purge_deleted_total=report.purge_deleted_total,
            purge_by_registry=report.purge_by_registry,
            partitions_created=report.partitions_created,
            partitions_detached=report.partitions_detached,
            partitions_dropped=report.partitions_dropped,
            error_code=None,
            error_detail=None,
            storage=report,
        )
    return MaintainResult(
        exit_code=EXIT_MAINTENANCE_FAILED,
        outcome="failed",
        lock_outcome="acquired",
        premade_through=report.premade_through,
        retained_from=report.retained_from,
        last_succeeded_at=report.last_succeeded_at,
        last_started_at=report.last_started_at,
        purge_examined_total=report.purge_examined_total,
        purge_deleted_total=report.purge_deleted_total,
        purge_by_registry=report.purge_by_registry,
        partitions_created=report.partitions_created,
        partitions_detached=report.partitions_detached,
        partitions_dropped=report.partitions_dropped,
        error_code=report.error_code,
        error_detail=report.error_detail,
        storage=report,
    )


def run_cycle(
    deployment: settings.DeploymentSettings,
    *,
    schema: str | None = None,
    premake_days: int = health.DEFAULT_PARTITION_PREMAKE_DAYS,
    lock_timeout_seconds: float = _DEFAULT_LOCK_TIMEOUT_SECONDS,
    compatible_min: str = health.BINARY_COMPATIBLE_MIN,
    compatible_max: str = health.BINARY_COMPATIBLE_MAX,
    revision_order: Sequence[str] = health.BINARY_REVISION_ORDER,
    engine: Engine | None = None,
    stop: _StopFlag | None = None,
) -> MaintainResult:
    """Run one advisory-locked maintenance cycle and return a typed result.

    ``compatible_*`` / ``revision_order`` are accepted for Phase 3.2 caller
    compatibility; schema readiness remains an API/readiness concern. Maintain
    verifies partition bounds after premake on the held session.
    """
    _ = (compatible_min, compatible_max, revision_order)

    if lock_timeout_seconds <= 0:
        _emit_status("dependency_failure", "invalid_lock_timeout")
        return MaintainResult(
            exit_code=EXIT_USAGE,
            outcome="dependency_failure",
            lock_outcome="none",
            error_code="invalid_lock_timeout",
        )
    if premake_days < 0:
        _emit_status("dependency_failure", "invalid_premake_days")
        return MaintainResult(
            exit_code=EXIT_USAGE,
            outcome="dependency_failure",
            lock_outcome="none",
            error_code="invalid_premake_days",
        )

    stop_flag = stop if stop is not None else _StopFlag()
    owns_engine = engine is None
    role_engine = engine if engine is not None else db.create_role_engine(
        deployment, "maintain"
    )

    previous_term = signal.getsignal(signal.SIGTERM)
    previous_int = signal.getsignal(signal.SIGINT)

    def _on_signal(signum: int, _frame: object) -> None:
        stop_flag.set()

    signal.signal(signal.SIGTERM, _on_signal)
    signal.signal(signal.SIGINT, _on_signal)

    lock_conn: Connection | None = None
    lock_held = False
    deadline = time.monotonic() + lock_timeout_seconds
    try:
        if stop_flag.is_set():
            _emit_status("dependency_failure", "signal")
            return MaintainResult(
                exit_code=EXIT_DEPENDENCY,
                outcome="dependency_failure",
                lock_outcome="none",
                error_code="signal",
            )

        try:
            lock_conn = role_engine.connect()
            _set_search_path(lock_conn, schema)
        except Exception:
            _emit_status("dependency_failure", health.ReasonCode.POSTGRES_UNAVAILABLE)
            return MaintainResult(
                exit_code=EXIT_DEPENDENCY,
                outcome="dependency_failure",
                lock_outcome="none",
                error_code=health.ReasonCode.POSTGRES_UNAVAILABLE,
            )

        if not _try_acquire_lock(lock_conn, stop_flag, deadline):
            if stop_flag.is_set():
                _emit_status("dependency_failure", "signal")
                return MaintainResult(
                    exit_code=EXIT_DEPENDENCY,
                    outcome="dependency_failure",
                    lock_outcome="none",
                    error_code="signal",
                )
            # Successful skip: another winner holds the lock.
            _emit_status("skipped_lock", "lock_held")
            return MaintainResult(
                exit_code=EXIT_OK,
                outcome="skipped_lock",
                lock_outcome="skipped",
            )

        lock_held = True

        if stop_flag.is_set():
            _emit_status("dependency_failure", "signal")
            return MaintainResult(
                exit_code=EXIT_DEPENDENCY,
                outcome="dependency_failure",
                lock_outcome="acquired",
                error_code="signal",
            )

        policy = deployment.payload_retention_policy()
        report = run_storage_maintenance(
            lock_conn,
            horizon_days=premake_days,
            payload_retention_policy=policy,
            registry_purge_batch_size=deployment.registry_purge_batch_size,
        )
        result = _from_storage(report)
        if result.outcome == "succeeded":
            _emit_status("ok", None)
        else:
            _emit_status("failed", result.error_code)
        return result
    finally:
        if lock_held and lock_conn is not None:
            try:
                lock_conn.execute(_UNLOCK_SQL, {"key": MAINTENANCE_LOCK_KEY})
                if lock_conn.in_transaction():
                    lock_conn.commit()
            except Exception:
                pass
        if lock_conn is not None:
            try:
                lock_conn.close()
            except Exception:
                pass
        if owns_engine:
            try:
                role_engine.dispose()
            except Exception:
                pass
        try:
            signal.signal(signal.SIGTERM, previous_term)
            signal.signal(signal.SIGINT, previous_int)
        except Exception:
            pass


def run(argv: Sequence[str] | None = None) -> int:
    """CLI entry point used by ``workhold.cli`` for the ``maintain`` role."""
    args = list(argv if argv is not None else ())
    parsed = _parse_argv(args)
    if isinstance(parsed, int):
        return parsed
    schema, lock_timeout, premake_days = parsed

    maybe_init_error_reporting(
        dsn=sentry_dsn_from_environ(),
        environment=os.environ.get("QUEUE_ENVIRONMENT", "development").strip() or "development",
        release=f"workhold@{__version__}",
        process_role="maintain",
    )

    try:
        deployment = settings_from_env()
    except settings.SettingsValidationError as exc:
        print(f"maintain: {exc}", file=sys.stderr)
        return EXIT_DEPENDENCY
    if deployment is None:
        _emit_status("dependency_failure", health.ReasonCode.POSTGRES_UNAVAILABLE)
        print("maintain: DATABASE_URL is required", file=sys.stderr)
        return EXIT_DEPENDENCY

    return run_cycle(
        deployment,
        schema=schema,
        premake_days=premake_days,
        lock_timeout_seconds=lock_timeout,
    ).exit_code
