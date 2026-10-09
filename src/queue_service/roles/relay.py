"""Delivery Outbox relay process role (PKG-01 / DLVR-03).

Composes the relay-scoped engine, DeliveryEventRepository, HttpDeliveryTransport,
and RelayService. Stops claiming on SIGTERM, awaits in-flight work within grace,
then closes HTTP and database resources. Does not start API/admin listeners.
"""

from __future__ import annotations

import asyncio
import os
import signal
import sys
import threading
import time
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Final

from sqlalchemy import event as sa_event
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker

from queue_service import __version__, db, settings
from queue_service.observability.error_reporting import maybe_init_error_reporting
from queue_service.settings import sentry_dsn_from_environ
from queue_service.delivery.relay import RelayConfig, RelayFaultHooks, RelayService
from queue_service.delivery.repository import (
    ClaimedDeliveryEvent,
    DeliveryEventRepository,
)
from queue_service.delivery.telemetry import (
    DeliveryTelemetry,
    reconcile_delivery_projection,
)
from queue_service.delivery.transports.http import (
    HttpDeliveryConfig,
    HttpDeliveryTransport,
    http_config_from_mapping,
)
from queue_service.observability.metrics import KernelMetrics
from queue_service.settings import DeploymentSettings, EnvironmentMode, SettingsValidationError

EXIT_OK: Final[int] = 0
EXIT_USAGE: Final[int] = 2
EXIT_DEPENDENCY: Final[int] = 5
EXIT_FAILED: Final[int] = 1

_DEFAULT_GRACE_SECONDS: Final[float] = 5.0
_DEFAULT_IDLE_SLEEP_SECONDS: Final[float] = 0.25

# Test-only fault injection (T-05-21). Never armed in production environments.
_TEST_FAULT_ENV: Final[str] = "QUEUE_TEST_RELAY_FAULT"
_TEST_FAULT_READY_ENV: Final[str] = "QUEUE_TEST_RELAY_FAULT_READY_PATH"
_TEST_FAULT_AFTER_PUBLISH: Final[str] = "crash_after_publish"
_TEST_FAULT_BEFORE_PUBLISH: Final[str] = "crash_before_publish"


def build_transport(config: HttpDeliveryConfig) -> HttpDeliveryTransport:
    return HttpDeliveryTransport(config)


def build_service(
    *,
    deployment: DeploymentSettings,
    transport: HttpDeliveryTransport,
    schema: str | None = None,
    relay_config: RelayConfig | None = None,
    relay_principal_id: str | None = None,
    engine: Engine | None = None,
    environ: Mapping[str, str] | None = None,
    telemetry: DeliveryTelemetry | None = None,
    fault_hooks: RelayFaultHooks | None = None,
) -> RelayService:
    env = os.environ if environ is None else environ
    role_engine = engine or db.create_role_engine(deployment, "relay")
    if schema:
        _ensure_search_path(role_engine, schema)

    factory = sessionmaker(bind=role_engine, class_=Session, expire_on_commit=False)
    cfg = relay_config or _relay_config_from_environ(env)
    principal = relay_principal_id or (
        (env.get("QUEUE_RELAY_PRINCIPAL_ID") or "relay").strip() or "relay"
    )
    hooks = fault_hooks
    if hooks is None:
        hooks = _test_fault_hooks_from_environ(env, deployment=deployment)
    return RelayService(
        session_factory=factory,
        transport=transport,
        config=cfg,
        relay_principal_id=principal,
        repository=DeliveryEventRepository(),
        telemetry=telemetry,
        fault_hooks=hooks,
    )


def _signal_ready_and_block(ready_path: str) -> None:
    """Write the orchestrator ready marker, then block until the process is killed."""
    path = Path(ready_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    # Atomic-ish replace so waiters never observe a zero-byte race on Windows.
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text("ready\n", encoding="utf-8")
    tmp.replace(path)
    while True:
        time.sleep(3600.0)


def _test_fault_hooks_from_environ(
    environ: Mapping[str, str],
    *,
    deployment: DeploymentSettings,
) -> RelayFaultHooks | None:
    """Arm test-only crash boundaries; refuse in production."""
    mode = (environ.get(_TEST_FAULT_ENV) or "").strip()
    if not mode:
        return None
    if deployment.environment is EnvironmentMode.PRODUCTION:
        raise SettingsValidationError(
            f"{_TEST_FAULT_ENV} is forbidden when QUEUE_ENVIRONMENT=production"
        )
    ready_path = (environ.get(_TEST_FAULT_READY_ENV) or "").strip()
    if not ready_path:
        raise SettingsValidationError(
            f"{_TEST_FAULT_READY_ENV} is required when {_TEST_FAULT_ENV} is set"
        )

    if mode == _TEST_FAULT_AFTER_PUBLISH:
        from queue_service.delivery.relay import DeliveryDisposition, DeliveryResult

        def _after(
            _claimed: ClaimedDeliveryEvent, result: DeliveryResult
        ) -> None:
            # Exact post-2xx / pre-ack boundary (T-05-21).
            if result.disposition is not DeliveryDisposition.ACKNOWLEDGED:
                return
            _signal_ready_and_block(ready_path)

        return RelayFaultHooks(after_publish_before_outcome=_after)

    if mode == _TEST_FAULT_BEFORE_PUBLISH:

        def _before(_claimed: ClaimedDeliveryEvent) -> None:
            _signal_ready_and_block(ready_path)

        return RelayFaultHooks(before_publish=_before)

    raise SettingsValidationError(
        f"unknown {_TEST_FAULT_ENV}={mode!r}; "
        f"expected {_TEST_FAULT_AFTER_PUBLISH!r} or {_TEST_FAULT_BEFORE_PUBLISH!r}"
    )


def _ensure_search_path(engine: Engine, schema: str) -> None:
    key = f"_queue_relay_search_path_{schema}"
    if getattr(engine, key, False):
        return

    def _on_connect(dbapi_conn: object, _connection_record: object) -> None:
        cursor = dbapi_conn.cursor()  # type: ignore[attr-defined]
        try:
            cursor.execute(f'SET search_path TO "{schema}"')
        finally:
            cursor.close()

    sa_event.listen(engine, "connect", _on_connect)
    setattr(engine, key, True)


def _relay_config_from_environ(environ: Mapping[str, str]) -> RelayConfig:
    def _float(name: str, default: float) -> float:
        raw = (environ.get(name) or "").strip()
        return float(raw) if raw else default

    def _int(name: str, default: int) -> int:
        raw = (environ.get(name) or "").strip()
        return int(raw) if raw else default

    return RelayConfig(
        lease_seconds=_int("QUEUE_RELAY_LEASE_SECONDS", 30),
        max_attempts=_int("QUEUE_RELAY_MAX_ATTEMPTS", 10),
        backoff_base_seconds=_float("QUEUE_RELAY_BACKOFF_BASE_SECONDS", 1.0),
        backoff_max_seconds=_float("QUEUE_RELAY_BACKOFF_MAX_SECONDS", 300.0),
        retry_after_cap_seconds=_float("QUEUE_RELAY_RETRY_AFTER_CAP_SECONDS", 60.0),
        jitter_ratio=_float("QUEUE_RELAY_JITTER_RATIO", 0.1),
        default_probe_seconds=_float("QUEUE_RELAY_DEFAULT_PROBE_SECONDS", 0.25),
    )


def settings_from_env(
    environ: Mapping[str, str] | None = None,
) -> DeploymentSettings | None:
    return settings.from_environ(environ)


def run_relay(
    argv: Sequence[str] = (),
    *,
    deployment: DeploymentSettings | None = None,
    http_config: HttpDeliveryConfig | None = None,
    environ: Mapping[str, str] | None = None,
    schema: str | None = None,
    max_cycles: int | None = None,
    idle_sleep_seconds: float | None = None,
    grace_seconds: float | None = None,
    install_signals: bool = True,
    stop_event: threading.Event | None = None,
) -> int:
    """Compose and run the bounded relay loop; return a process exit code."""
    _ = argv
    env_map: Mapping[str, str] = os.environ if environ is None else environ

    try:
        deployment_settings = (
            deployment if deployment is not None else settings_from_env(env_map)
        )
    except SettingsValidationError as exc:
        print(f"relay: {exc}", file=sys.stderr)
        return EXIT_DEPENDENCY
    if deployment_settings is None:
        print("relay: DATABASE_URL is required", file=sys.stderr)
        return EXIT_DEPENDENCY

    resolved_schema = schema
    if resolved_schema is None:
        resolved_schema = (
            env_map.get("QUEUE_SCHEMA")
            or env_map.get("ALEMBIC_VERSION_TABLE_SCHEMA")
            or ""
        ).strip() or None

    try:
        transport_config = (
            http_config
            if http_config is not None
            else http_config_from_mapping(
                env_map, environment=deployment_settings.environment
            )
        )
    except (ValueError, SettingsValidationError) as exc:
        print(f"relay: invalid delivery config: {exc}", file=sys.stderr)
        return EXIT_DEPENDENCY

    try:
        cycles_env = (env_map.get("QUEUE_RELAY_MAX_CYCLES") or "").strip()
        resolved_max_cycles = max_cycles
        if resolved_max_cycles is None and cycles_env:
            resolved_max_cycles = int(cycles_env)
        idle = (
            idle_sleep_seconds
            if idle_sleep_seconds is not None
            else float(
                (env_map.get("QUEUE_RELAY_IDLE_SLEEP_SECONDS") or "").strip()
                or _DEFAULT_IDLE_SLEEP_SECONDS
            )
        )
        grace = (
            grace_seconds
            if grace_seconds is not None
            else float(
                (env_map.get("QUEUE_RELAY_GRACE_SECONDS") or "").strip()
                or _DEFAULT_GRACE_SECONDS
            )
        )
    except ValueError:
        print("relay: invalid numeric runtime bound", file=sys.stderr)
        return EXIT_USAGE

    stop = stop_event if stop_event is not None else threading.Event()
    previous_term = signal.getsignal(signal.SIGTERM)
    previous_int = signal.getsignal(signal.SIGINT)
    signals_installed = False

    def _on_signal(_signum: int, _frame: object) -> None:
        stop.set()

    if install_signals:
        try:
            signal.signal(signal.SIGTERM, _on_signal)
            signal.signal(signal.SIGINT, _on_signal)
            signals_installed = True
        except ValueError:
            signals_installed = False

    engine = db.create_role_engine(deployment_settings, "relay")
    transport = build_transport(transport_config)
    metrics = KernelMetrics(process_role="relay")
    telemetry = DeliveryTelemetry(metrics=metrics)
    try:
        service = build_service(
            deployment=deployment_settings,
            transport=transport,
            schema=resolved_schema,
            engine=engine,
            environ=env_map,
            telemetry=telemetry,
        )
    except SettingsValidationError as exc:
        print(f"relay: {exc}", file=sys.stderr)
        try:
            transport.close()
        except Exception:
            pass
        try:
            engine.dispose()
        except Exception:
            pass
        return EXIT_DEPENDENCY

    print("relay status=running role=relay", flush=True)
    exit_code = EXIT_OK
    cycles = 0
    in_flight = 0
    try:
        while not stop.is_set():
            if resolved_max_cycles is not None and cycles >= resolved_max_cycles:
                break
            try:
                in_flight = 1
                result = asyncio.run(service.process_one())
                in_flight = 0
            except Exception as exc:
                in_flight = 0
                print(f"relay: cycle failed: {type(exc).__name__}", file=sys.stderr)
                exit_code = EXIT_FAILED
                break
            cycles += 1
            # Refresh depth/oldest-lag gauges after each cycle (bounded COUNT + LIMIT 1).
            try:
                with Session(bind=engine, expire_on_commit=False) as proj_session:
                    reconcile_delivery_projection(
                        proj_session, telemetry=telemetry
                    )
            except Exception as proj_exc:  # noqa: BLE001 — gauges are best-effort
                print(
                    f"relay: delivery projection reconcile failed: "
                    f"{type(proj_exc).__name__}",
                    file=sys.stderr,
                )
            if stop.is_set():
                break
            if result.kind in {"empty", "skipped_backpressure"}:
                if (
                    resolved_max_cycles is not None
                    and cycles >= resolved_max_cycles
                ):
                    break
                deadline = time.monotonic() + idle
                while not stop.is_set() and time.monotonic() < deadline:
                    time.sleep(min(0.05, idle))
            elif (
                resolved_max_cycles is not None and cycles >= resolved_max_cycles
            ):
                break
    finally:
        _ = grace  # grace reserved for future multi-inflight await
        telemetry.record_shutdown(in_flight=in_flight)
        try:
            transport.close()
        except Exception:
            pass
        try:
            engine.dispose()
        except Exception:
            pass
        if signals_installed:
            try:
                signal.signal(signal.SIGTERM, previous_term)
                signal.signal(signal.SIGINT, previous_int)
            except Exception:
                pass
        print("relay status=stopped", flush=True)

    return exit_code


def run(argv: Sequence[str] | None = None) -> int:
    """CLI entry point used by ``queue_service.cli`` for the ``relay`` role."""
    args = list(argv if argv is not None else ())
    if args and args[0] in {"-h", "--help"}:
        print(
            "usage: queue relay\n"
            "Delivery Outbox publication (HTTP webhook). "
            "Requires DATABASE_URL and QUEUE_DELIVERY_* settings.",
            file=sys.stderr,
        )
        return EXIT_USAGE

    maybe_init_error_reporting(
        dsn=sentry_dsn_from_environ(),
        environment=os.environ.get("QUEUE_ENVIRONMENT", "development").strip() or "development",
        release=f"queue@{__version__}",
        process_role="relay",
    )
    return run_relay(args)
