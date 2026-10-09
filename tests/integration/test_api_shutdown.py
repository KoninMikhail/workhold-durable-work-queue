"""Dual-listener API graceful shutdown (DEP-02 / PKG-01 / OPS-01).

Instrumented tests prove SIGTERM ordering: readiness fails, acceptance stops,
in-flight work is grace-bounded, pools close, and no queue/task SQL mutations
occur during process termination.
"""

from __future__ import annotations

import json
import socket
import threading
import time
import uuid
from collections.abc import Iterator
from http.client import HTTPConnection
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

import pytest
from sqlalchemy import event
from sqlalchemy.engine import Engine

from workhold import db, settings
from workhold.api.security import ListenerBind
from workhold.lifecycle import Lifecycle, LifecyclePhase
from workhold.roles import api as api_role

PREMAKE_DAYS = 0  # migrated schema already has day-0 partitions; avoid horizon wait


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


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
        credential_generations=(
            settings.CredentialGeneration(
                principal_id="api-shutdown-test",
                generation_id="gen-1",
                secret=settings.Secret("token"),
            ),
        ),
    )


@pytest.fixture
def api_schema(test_database_url: str) -> Iterator[tuple[str, str]]:
    """Fresh migrated schema for API pools; always dropped."""
    from tests.integration.conftest import run_alembic, to_psycopg_conninfo

    import psycopg

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


def _http_get(url: str, *, timeout: float = 2.0) -> tuple[int, bytes]:
    req = Request(url, method="GET")
    try:
        with urlopen(req, timeout=timeout) as resp:
            return int(resp.status), resp.read()
    except HTTPError as exc:
        return int(exc.code), exc.read()


def _wait_ready(url: str, *, timeout: float = 10.0) -> None:
    deadline = time.monotonic() + timeout
    last: Exception | None = None
    while time.monotonic() < deadline:
        try:
            status, _ = _http_get(url, timeout=1.0)
            if status == 200:
                return
        except (URLError, TimeoutError, OSError) as exc:
            last = exc
        time.sleep(0.05)
    raise AssertionError(f"readyz not ready at {url}: {last}")


def _make_sleep_app(delay_seconds: float) -> Any:
    async def app(scope: dict[str, Any], receive: Any, send: Any) -> None:
        if scope["type"] != "http":
            return
        path = str(scope.get("path", "/"))
        if path == "/work":
            await __import__("asyncio").sleep(delay_seconds)
            body = b'{"ok":true}'
            await send(
                {
                    "type": "http.response.start",
                    "status": 200,
                    "headers": [
                        (b"content-type", b"application/json"),
                        (b"content-length", str(len(body)).encode("ascii")),
                    ],
                }
            )
            await send({"type": "http.response.body", "body": body})
            return
        body = b'{"ok":true,"path":"%s"}' % path.encode("ascii", "replace")
        await send(
            {
                "type": "http.response.start",
                "status": 200,
                "headers": [
                    (b"content-type", b"application/json"),
                    (b"content-length", str(len(body)).encode("ascii")),
                ],
            }
        )
        await send({"type": "http.response.body", "body": body})

    return app


def test_lifecycle_is_monotonic_and_idempotent() -> None:
    life = Lifecycle()
    assert life.phase is LifecyclePhase.STARTUP
    assert life.is_ready() is False
    assert life.is_accepting() is False

    life.mark_running()
    assert life.phase is LifecyclePhase.RUNNING
    assert life.is_ready() is True
    assert life.is_accepting() is True

    first = life.request_stop(grace_seconds=1.5)
    assert first is True
    assert life.phase is LifecyclePhase.STOPPING
    assert life.is_ready() is False
    assert life.is_accepting() is False
    first_deadline = life.stop_deadline_mono
    assert first_deadline is not None

    time.sleep(0.05)
    second = life.request_stop(grace_seconds=30.0)
    assert second is False
    assert life.stop_deadline_mono == first_deadline
    assert life.grace_seconds == 1.5

    life.mark_stopped()
    assert life.phase is LifecyclePhase.STOPPED
    third = life.request_stop(grace_seconds=5.0)
    assert third is False
    assert life.phase is LifecyclePhase.STOPPED


def test_sigterm_order_ready_false_then_bounded_exit(
    api_schema: tuple[str, str],
) -> None:
    schema, url = api_schema
    dep = _settings_for_url(url)
    api_engine = db.create_role_engine(dep, "api")
    admin_engine = db.create_role_engine(dep, "admin")
    app_port = _free_port()
    admin_port = _free_port()
    life = Lifecycle()
    grace = 1.0
    started = threading.Event()
    result: dict[str, object] = {}

    def _run() -> None:
        code = api_role.serve(
            application_app=_make_sleep_app(0.05),
            admin_app=_make_sleep_app(0.05),
            application_bind=ListenerBind(host="127.0.0.1", port=app_port),
            admin_bind=ListenerBind(host="127.0.0.1", port=admin_port),
            lifecycle=life,
            grace_seconds=grace,
            api_engine=api_engine,
            admin_engine=admin_engine,
            schema=schema,
            premake_days=PREMAKE_DAYS,
            on_running=started.set,
            install_signals=False,
        )
        result["code"] = code

    thread = threading.Thread(target=_run, name="api-serve", daemon=True)
    thread.start()
    assert started.wait(timeout=10)
    _wait_ready(f"http://127.0.0.1:{app_port}/readyz")

    ready_before, _ = _http_get(f"http://127.0.0.1:{app_port}/readyz")
    assert ready_before == 200

    t0 = time.monotonic()
    assert life.request_stop(grace) is True
    # Readiness must fail immediately — before process exit.
    ready_after, body = _http_get(f"http://127.0.0.1:{app_port}/readyz")
    assert ready_after == 503
    payload = json.loads(body.decode("utf-8"))
    assert payload.get("ready") is False

    thread.join(timeout=grace + 2.0)
    elapsed = time.monotonic() - t0
    assert not thread.is_alive()
    assert result.get("code") == api_role.EXIT_OK
    assert elapsed <= grace + 1.5
    assert life.phase is LifecyclePhase.STOPPED
    assert life.shutdown_events[0] == "ready_false"
    assert "acceptance_stopped" in life.shutdown_events
    assert "pools_disposed" in life.shutdown_events
    assert life.shutdown_events[-1] == "stopped"


def test_short_inflight_finishes_within_grace(api_schema: tuple[str, str]) -> None:
    schema, url = api_schema
    dep = _settings_for_url(url)
    api_engine = db.create_role_engine(dep, "api")
    admin_engine = db.create_role_engine(dep, "admin")
    app_port = _free_port()
    admin_port = _free_port()
    life = Lifecycle()
    grace = 2.0
    started = threading.Event()
    work_done = threading.Event()
    result: dict[str, object] = {}

    def _run() -> None:
        result["code"] = api_role.serve(
            application_app=_make_sleep_app(0.25),
            admin_app=_make_sleep_app(0.05),
            application_bind=ListenerBind(host="127.0.0.1", port=app_port),
            admin_bind=ListenerBind(host="127.0.0.1", port=admin_port),
            lifecycle=life,
            grace_seconds=grace,
            api_engine=api_engine,
            admin_engine=admin_engine,
            schema=schema,
            premake_days=PREMAKE_DAYS,
            on_running=started.set,
            install_signals=False,
        )

    thread = threading.Thread(target=_run, name="api-serve-short", daemon=True)
    thread.start()
    assert started.wait(timeout=10)
    _wait_ready(f"http://127.0.0.1:{app_port}/readyz")

    def _client() -> None:
        conn = HTTPConnection("127.0.0.1", app_port, timeout=5)
        try:
            conn.request("GET", "/work")
            resp = conn.getresponse()
            assert resp.status == 200
            work_done.set()
        finally:
            conn.close()

    client = threading.Thread(target=_client, daemon=True)
    client.start()
    time.sleep(0.05)
    t0 = time.monotonic()
    life.request_stop(grace)
    thread.join(timeout=grace + 2.0)
    client.join(timeout=grace + 2.0)
    elapsed = time.monotonic() - t0
    assert work_done.is_set()
    assert result.get("code") == api_role.EXIT_OK
    assert elapsed <= grace + 1.5


def test_over_grace_request_cannot_hold_exit(api_schema: tuple[str, str]) -> None:
    schema, url = api_schema
    dep = _settings_for_url(url)
    api_engine = db.create_role_engine(dep, "api")
    admin_engine = db.create_role_engine(dep, "admin")
    app_port = _free_port()
    admin_port = _free_port()
    life = Lifecycle()
    grace = 0.4
    started = threading.Event()
    result: dict[str, object] = {}

    def _run() -> None:
        result["code"] = api_role.serve(
            application_app=_make_sleep_app(5.0),
            admin_app=_make_sleep_app(0.05),
            application_bind=ListenerBind(host="127.0.0.1", port=app_port),
            admin_bind=ListenerBind(host="127.0.0.1", port=admin_port),
            lifecycle=life,
            grace_seconds=grace,
            api_engine=api_engine,
            admin_engine=admin_engine,
            schema=schema,
            premake_days=PREMAKE_DAYS,
            on_running=started.set,
            install_signals=False,
        )

    thread = threading.Thread(target=_run, name="api-serve-long", daemon=True)
    thread.start()
    assert started.wait(timeout=10)
    _wait_ready(f"http://127.0.0.1:{app_port}/readyz")

    def _client() -> None:
        try:
            conn = HTTPConnection("127.0.0.1", app_port, timeout=8)
            conn.request("GET", "/work")
            conn.getresponse()
        except Exception:
            pass

    client = threading.Thread(target=_client, daemon=True)
    client.start()
    time.sleep(0.05)
    t0 = time.monotonic()
    life.request_stop(grace)
    thread.join(timeout=grace + 2.0)
    elapsed = time.monotonic() - t0
    assert not thread.is_alive()
    assert result.get("code") == api_role.EXIT_OK
    assert elapsed <= grace + 1.5
    assert elapsed < 3.0


def test_new_traffic_rejected_after_stopping(api_schema: tuple[str, str]) -> None:
    schema, url = api_schema
    dep = _settings_for_url(url)
    api_engine = db.create_role_engine(dep, "api")
    admin_engine = db.create_role_engine(dep, "admin")
    app_port = _free_port()
    admin_port = _free_port()
    life = Lifecycle()
    started = threading.Event()

    def _run() -> None:
        api_role.serve(
            application_app=_make_sleep_app(0.05),
            admin_app=_make_sleep_app(0.05),
            application_bind=ListenerBind(host="127.0.0.1", port=app_port),
            admin_bind=ListenerBind(host="127.0.0.1", port=admin_port),
            lifecycle=life,
            grace_seconds=2.0,
            api_engine=api_engine,
            admin_engine=admin_engine,
            schema=schema,
            premake_days=PREMAKE_DAYS,
            on_running=started.set,
            install_signals=False,
        )

    thread = threading.Thread(target=_run, name="api-serve-reject", daemon=True)
    thread.start()
    assert started.wait(timeout=10)
    _wait_ready(f"http://127.0.0.1:{app_port}/readyz")

    life.request_stop(2.0)
    status, body = _http_get(f"http://127.0.0.1:{app_port}/v1/anything")
    assert status == 503
    payload = json.loads(body.decode("utf-8"))
    assert payload.get("code") == "not_accepting"
    admin_status, _ = _http_get(f"http://127.0.0.1:{admin_port}/admin/v1/anything")
    assert admin_status == 503
    thread.join(timeout=4.0)
    assert not thread.is_alive()


def test_repeated_signals_do_not_extend_grace(api_schema: tuple[str, str]) -> None:
    schema, url = api_schema
    dep = _settings_for_url(url)
    api_engine = db.create_role_engine(dep, "api")
    admin_engine = db.create_role_engine(dep, "admin")
    app_port = _free_port()
    admin_port = _free_port()
    life = Lifecycle()
    grace = 0.6
    started = threading.Event()
    result: dict[str, object] = {}

    def _run() -> None:
        result["code"] = api_role.serve(
            application_app=_make_sleep_app(5.0),
            admin_app=_make_sleep_app(0.05),
            application_bind=ListenerBind(host="127.0.0.1", port=app_port),
            admin_bind=ListenerBind(host="127.0.0.1", port=admin_port),
            lifecycle=life,
            grace_seconds=grace,
            api_engine=api_engine,
            admin_engine=admin_engine,
            schema=schema,
            premake_days=PREMAKE_DAYS,
            on_running=started.set,
            install_signals=False,
        )

    thread = threading.Thread(target=_run, name="api-serve-repeat", daemon=True)
    thread.start()
    assert started.wait(timeout=10)
    _wait_ready(f"http://127.0.0.1:{app_port}/readyz")

    def _client() -> None:
        try:
            conn = HTTPConnection("127.0.0.1", app_port, timeout=8)
            conn.request("GET", "/work")
            conn.getresponse()
        except Exception:
            pass

    client = threading.Thread(target=_client, daemon=True)
    client.start()
    time.sleep(0.05)
    t0 = time.monotonic()
    assert life.request_stop(grace) is True
    deadline = life.stop_deadline_mono
    time.sleep(0.1)
    assert life.request_stop(30.0) is False
    assert life.stop_deadline_mono == deadline
    thread.join(timeout=grace + 2.0)
    elapsed = time.monotonic() - t0
    assert not thread.is_alive()
    assert elapsed <= grace + 1.5


def test_both_listeners_and_pools_close(api_schema: tuple[str, str]) -> None:
    schema, url = api_schema
    dep = _settings_for_url(url)
    api_engine = db.create_role_engine(dep, "api")
    admin_engine = db.create_role_engine(dep, "admin")
    app_port = _free_port()
    admin_port = _free_port()
    life = Lifecycle()
    started = threading.Event()

    def _run() -> None:
        api_role.serve(
            application_app=_make_sleep_app(0.05),
            admin_app=_make_sleep_app(0.05),
            application_bind=ListenerBind(host="127.0.0.1", port=app_port),
            admin_bind=ListenerBind(host="127.0.0.1", port=admin_port),
            lifecycle=life,
            grace_seconds=0.5,
            api_engine=api_engine,
            admin_engine=admin_engine,
            schema=schema,
            premake_days=PREMAKE_DAYS,
            on_running=started.set,
            install_signals=False,
        )

    thread = threading.Thread(target=_run, name="api-serve-close", daemon=True)
    thread.start()
    assert started.wait(timeout=10)
    _wait_ready(f"http://127.0.0.1:{app_port}/readyz")
    _wait_ready(f"http://127.0.0.1:{admin_port}/readyz")

    life.request_stop(0.5)
    thread.join(timeout=3.0)
    assert not thread.is_alive()
    assert "pools_disposed" in life.shutdown_events

    with pytest.raises((URLError, OSError, ConnectionError, TimeoutError)):
        _http_get(f"http://127.0.0.1:{app_port}/readyz", timeout=0.5)
    with pytest.raises((URLError, OSError, ConnectionError, TimeoutError)):
        _http_get(f"http://127.0.0.1:{admin_port}/readyz", timeout=0.5)

    assert "listeners_closed" in life.shutdown_events
    # Pool dispose completed; checked via lifecycle event (SQLAlchemy dispose
    # still allows fresh checkouts, so we do not assert connect failure).


def test_shutdown_emits_no_queue_task_update_sql(api_schema: tuple[str, str]) -> None:
    schema, url = api_schema
    dep = _settings_for_url(url)
    api_engine = db.create_role_engine(dep, "api")
    admin_engine = db.create_role_engine(dep, "admin")
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

    event.listen(api_engine, "before_cursor_execute", _capture)
    event.listen(admin_engine, "before_cursor_execute", _capture)

    app_port = _free_port()
    admin_port = _free_port()
    life = Lifecycle()
    started = threading.Event()

    def _run() -> None:
        api_role.serve(
            application_app=_make_sleep_app(0.05),
            admin_app=_make_sleep_app(0.05),
            application_bind=ListenerBind(host="127.0.0.1", port=app_port),
            admin_bind=ListenerBind(host="127.0.0.1", port=admin_port),
            lifecycle=life,
            grace_seconds=0.5,
            api_engine=api_engine,
            admin_engine=admin_engine,
            schema=schema,
            premake_days=PREMAKE_DAYS,
            on_running=started.set,
            install_signals=False,
        )

    thread = threading.Thread(target=_run, name="api-serve-sql", daemon=True)
    thread.start()
    assert started.wait(timeout=10)
    _wait_ready(f"http://127.0.0.1:{app_port}/readyz")

    # Baseline noise from readiness probes is fine; clear and capture shutdown window.
    captured.clear()
    life.request_stop(0.5)
    thread.join(timeout=3.0)
    assert not thread.is_alive()

    mutating = [
        stmt
        for stmt in captured
        if _is_queue_task_mutation(stmt)
    ]
    assert mutating == [], f"unexpected mutations during shutdown: {mutating}"


def test_shutdown_stops_wake_listener_before_pools_disposed() -> None:
    """Waiters/listener signal before grace drain and pool disposal (Plan 03).

    Uses ``api_engine=None`` so readiness is process-local; partition horizon is
    out of scope for this shutdown-ordering assertion.
    """
    from workhold.application.claim_long_poll import WaiterAdmission
    from workhold.infrastructure.postgres.claim_wakeup import (
        ClaimWakeListener,
        QueueGenerationCoordinator,
    )

    coordinator = QueueGenerationCoordinator()
    admission = WaiterAdmission(4)

    class _FakeWakeListener:
        def __init__(self) -> None:
            self.started = False
            self.stopped = False

        @property
        def health(self):  # noqa: ANN201
            from workhold.infrastructure.postgres.claim_wakeup import ListenerHealth

            return ListenerHealth.CONNECTED if self.started and not self.stopped else ListenerHealth.DEGRADED

        def start(self) -> None:
            self.started = True

        def stop(self, *, join_timeout_seconds: float = 5.0) -> None:
            _ = join_timeout_seconds
            self.stopped = True

    listener = _FakeWakeListener()
    app_port = _free_port()
    admin_port = _free_port()
    life = Lifecycle()
    started = threading.Event()

    def _run() -> None:
        api_role.serve(
            application_app=_make_sleep_app(0.05),
            admin_app=_make_sleep_app(0.05),
            application_bind=ListenerBind(host="127.0.0.1", port=app_port),
            admin_bind=ListenerBind(host="127.0.0.1", port=admin_port),
            lifecycle=life,
            grace_seconds=0.5,
            api_engine=None,
            admin_engine=None,
            on_running=started.set,
            install_signals=False,
            wake_listener=listener,  # type: ignore[arg-type]
            waiter_admission=admission,
            wake_coordinator=coordinator,
        )

    thread = threading.Thread(target=_run, name="api-serve-wake", daemon=True)
    thread.start()
    assert started.wait(timeout=10)
    _wait_ready(f"http://127.0.0.1:{app_port}/readyz")

    life.request_stop(0.5)
    thread.join(timeout=5.0)
    assert not thread.is_alive()
    events = life.shutdown_events
    assert "acceptance_stopped" in events
    assert "waiters_signalled" in events
    assert "wake_listener_stopped" in events
    assert "pools_disposed" in events
    assert events.index("wake_listener_stopped") < events.index("pools_disposed")
    assert events.index("waiters_signalled") < events.index("wake_listener_stopped")
    assert listener.started is True
    assert listener.stopped is True


def _is_queue_task_mutation(statement: str) -> bool:
    upper = " ".join(statement.upper().split())
    if not any(
        verb in upper
        for verb in ("UPDATE ", "INSERT ", "DELETE ", "ALTER ", "DROP ", "TRUNCATE ")
    ):
        return False
    # Queue/task domain tables — readiness may SELECT from catalogs only.
    targets = (
        " TASKS",
        " TASK_",
        " QUEUES",
        " QUEUE_",
        " CLAIMS",
        " DELIVERY_",
        " ADMIN_AUDIT",
        " TASK_ATTEMPTS",
        " TASK_PAYLOADS",
        " IDEMPOTENCY",
    )
    return any(token in upper for token in targets)
