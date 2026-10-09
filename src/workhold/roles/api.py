"""Dual-listener API role with bounded graceful shutdown (DEP-02 / PKG-01 / OPS-01).

Starts application and admin ASGI compositions on distinct bindings using the
Phase 3.1 stdlib ``ThreadingHTTPServer`` stack. SIGTERM/SIGINT make readiness
fail, stop acceptance, wait a monotonic grace for in-flight requests, dispose
API/admin pools, and exit — without mutating persisted queue state.
"""

from __future__ import annotations

import asyncio
import json
import os
import select
import signal
import socket
import sys
import threading
import time
from collections.abc import Callable, Sequence
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Final
from urllib.parse import unquote, urlsplit

from sqlalchemy.engine import Engine
from sqlalchemy.orm import sessionmaker

from workhold import __version__, db, health, settings
from workhold.observability.error_reporting import maybe_init_error_reporting
from workhold.observability.metrics import KernelMetrics
from workhold.settings import sentry_dsn_from_environ
from workhold.api.admin import create_admin_app
from workhold.api.application import create_application_app
from workhold.api.security import ListenerBind
from workhold.application.claim_long_poll import (
    ClaimLongPollService,
    WaiterAdmission,
)
from workhold.infrastructure.postgres.claim_wakeup import (
    ClaimWakeListener,
    ListenerHealth,
    QueueGenerationCoordinator,
)
from workhold.lifecycle import (
    DEFAULT_SHUTDOWN_GRACE_SECONDS,
    Lifecycle,
    LifecyclePhase,
)
from workhold.security.authorization import Authorizer
from workhold.security.credentials import (
    BearerCredentialAuthenticator,
    CredentialBinding,
)
from workhold.security.principals import ServiceRole
from workhold.settings import Secret

EXIT_OK: Final[int] = 0
EXIT_USAGE: Final[int] = 2
EXIT_DEPENDENCY: Final[int] = 5

_HEALTH_PATHS: Final[frozenset[str]] = frozenset({"/healthz", "/readyz"})


def _request_socket_disconnected(sock: socket.socket | None) -> bool:
    """Non-consuming liveness probe: True when the peer has closed the socket."""
    if sock is None:
        return False
    try:
        readable, _, _ = select.select([sock], [], [], 0)
        if not readable:
            return False
        peeked = sock.recv(1, socket.MSG_PEEK)
        return len(peeked) == 0
    except (OSError, ValueError):
        return True


class InFlightGate:
    """Tracks accepted in-flight requests and cooperative acceptance."""

    __slots__ = ("_cond", "_count", "_accepting")

    def __init__(self) -> None:
        self._cond = threading.Condition()
        self._count = 0
        self._accepting = True

    def stop_accepting(self) -> None:
        with self._cond:
            self._accepting = False
            self._cond.notify_all()

    def try_enter(self) -> bool:
        with self._cond:
            if not self._accepting:
                return False
            self._count += 1
            return True

    def leave(self) -> None:
        with self._cond:
            self._count -= 1
            self._cond.notify_all()

    def wait_empty(self, deadline_mono: float) -> bool:
        with self._cond:
            while self._count > 0:
                remaining = deadline_mono - time.monotonic()
                if remaining <= 0:
                    return False
                self._cond.wait(timeout=remaining)
            return True

    @property
    def count(self) -> int:
        with self._cond:
            return self._count


class QuietThreadingHTTPServer(ThreadingHTTPServer):
    daemon_threads = True
    allow_reuse_address = True
    request_queue_size = 32
    timeout = 0.5

    def __init__(
        self,
        server_address: tuple[str, int],
        RequestHandlerClass: type[BaseHTTPRequestHandler],
        *,
        app: Any,
        lifecycle: Lifecycle,
        gate: InFlightGate,
        api_engine: Engine | None,
        schema: str | None,
        premake_days: int,
    ) -> None:
        self.queue_app = app
        self.queue_lifecycle = lifecycle
        self.queue_gate = gate
        self.queue_api_engine = api_engine
        self.queue_schema = schema
        self.queue_premake_days = premake_days
        super().__init__(server_address, RequestHandlerClass)


def _json_bytes(payload: dict[str, Any]) -> bytes:
    return json.dumps(payload, separators=(",", ":"), ensure_ascii=False).encode("utf-8")


def _send_raw(
    handler: BaseHTTPRequestHandler,
    *,
    status: int,
    body: bytes,
    content_type: str = "application/json; charset=utf-8",
) -> None:
    handler.send_response(status)
    handler.send_header("Content-Type", content_type)
    handler.send_header("Content-Length", str(len(body)))
    handler.send_header("Connection", "close")
    handler.end_headers()
    handler.wfile.write(body)


def _client_disconnect_confirmed(scope: dict[str, Any]) -> bool:
    """True when the request-scoped cancel probe reports a confirmed peer close."""
    cancel_probe = scope.get("queue_request_cancelled")
    if not callable(cancel_probe):
        return False
    return bool(cancel_probe())


def _run_asgi(
    app: Any, scope: dict[str, Any], body: bytes
) -> tuple[int | None, list[tuple[bytes, bytes]], bytes]:
    """Run one ASGI HTTP call.

    Returns ``(None, [], b"")`` when the app completes without starting a response
    and the peer is already confirmed disconnected (claim long-poll cancel path).
    A missing response while the client is still connected fails closed as 500.
    """
    status_code: int | None = None
    response_headers: list[tuple[bytes, bytes]] = []
    chunks: list[bytes] = []
    body_sent = False

    async def receive() -> dict[str, Any]:
        nonlocal body_sent
        if not body_sent:
            body_sent = True
            return {"type": "http.request", "body": body, "more_body": False}
        return {"type": "http.disconnect"}

    async def send(message: dict[str, Any]) -> None:
        nonlocal status_code, response_headers
        if message["type"] == "http.response.start":
            status_code = int(message["status"])
            response_headers = list(message.get("headers") or [])
        elif message["type"] == "http.response.body":
            chunk = message.get("body", b"")
            if chunk:
                chunks.append(chunk)

    asyncio.run(app(scope, receive, send))
    if status_code is None:
        if _client_disconnect_confirmed(scope):
            # Peer is gone — do not fabricate a synthetic HTTP 500.
            return None, [], b""
        # Fail closed: application exited without a response while still connected.
        return 500, response_headers, b"".join(chunks)
    return status_code, response_headers, b"".join(chunks)


class AsgiRequestHandler(BaseHTTPRequestHandler):
    server: QuietThreadingHTTPServer  # type: ignore[assignment]

    protocol_version = "HTTP/1.1"

    def log_message(self, format: str, *args: object) -> None:  # noqa: A003
        return

    def handle_one_request(self) -> None:
        try:
            super().handle_one_request()
        except (ConnectionResetError, BrokenPipeError, ConnectionAbortedError):
            pass

    def do_GET(self) -> None:  # noqa: N802
        self._dispatch()

    def do_POST(self) -> None:  # noqa: N802
        self._dispatch()

    def do_PUT(self) -> None:  # noqa: N802
        self._dispatch()

    def do_PATCH(self) -> None:  # noqa: N802
        self._dispatch()

    def do_DELETE(self) -> None:  # noqa: N802
        self._dispatch()

    def do_HEAD(self) -> None:  # noqa: N802
        self._dispatch()

    def do_OPTIONS(self) -> None:  # noqa: N802
        self._dispatch()

    def _dispatch(self) -> None:
        server = self.server
        lifecycle = server.queue_lifecycle
        gate = server.queue_gate
        parsed = urlsplit(self.path)
        path = unquote(parsed.path) or "/"

        if path == "/healthz":
            status = health.check_liveness()
            body = _json_bytes({"ok": status.ok, "reason_code": status.reason_code})
            _send_raw(self, status=200 if status.ok else 503, body=body)
            return

        if path == "/readyz":
            if not lifecycle.is_ready():
                body = _json_bytes(
                    {
                        "ready": False,
                        "reason_code": "stopping"
                        if lifecycle.phase
                        in {LifecyclePhase.STOPPING, LifecyclePhase.STOPPED}
                        else "starting",
                    }
                )
                _send_raw(self, status=503, body=body)
                return
            engine = server.queue_api_engine
            if engine is None:
                body = _json_bytes({"ready": True, "reason_code": None})
                _send_raw(self, status=200, body=body)
                return
            status = health.check_readiness(
                engine,
                schema=server.queue_schema,
                premake_days=server.queue_premake_days,
            )
            body = _json_bytes({"ready": status.ok, "reason_code": status.reason_code})
            _send_raw(self, status=200 if status.ok else 503, body=body)
            return

        if not gate.try_enter():
            body = _json_bytes(
                {
                    "code": "not_accepting",
                    "message": "process is shutting down",
                    "retryable": True,
                    "ready": False,
                }
            )
            _send_raw(self, status=503, body=body)
            return

        try:
            length = int(self.headers.get("Content-Length", "0") or "0")
            if length < 0:
                length = 0
            body = self.rfile.read(length) if length else b""
            header_list = [
                (k.lower().encode("latin-1"), v.encode("latin-1"))
                for k, v in self.headers.items()
            ]
            request_sock = self.connection

            def _cancelled() -> bool:
                return _request_socket_disconnected(request_sock)

            def _stopping() -> bool:
                return lifecycle.stop_event.is_set()

            scope = {
                "type": "http",
                "asgi": {"version": "3.0", "spec_version": "2.3"},
                "http_version": "1.1",
                "method": self.command,
                "scheme": "http",
                "path": path,
                "raw_path": path.encode("utf-8"),
                "query_string": (parsed.query or "").encode("utf-8"),
                "headers": header_list,
                "client": self.client_address,
                "server": self.server.server_address[:2],
                # Real cancel seams — do not use synthetic ASGI disconnect.
                "queue_request_cancelled": _cancelled,
                "queue_lifecycle_stopping": _stopping,
            }
            status, headers, response_body = _run_asgi(server.queue_app, scope, body)
            if status is None:
                # Confirmed disconnect with no ASGI response — peer is gone.
                return
            self.send_response(status)
            sent_length = False
            for key, value in headers:
                name = key.decode("latin-1")
                if name.lower() == "content-length":
                    sent_length = True
                self.send_header(name, value.decode("latin-1"))
            if not sent_length:
                self.send_header("Content-Length", str(len(response_body)))
            self.send_header("Connection", "close")
            self.end_headers()
            if self.command != "HEAD":
                self.wfile.write(response_body)
        finally:
            gate.leave()


def settings_from_env() -> settings.DeploymentSettings | None:
    """Build DeploymentSettings from the process environment.

    Raises:
        settings.SettingsValidationError: unsafe production/TLS/budget config.
    """
    return settings.from_environ()


def _parse_bind(host_env: str, port_env: str, default_host: str, default_port: int) -> ListenerBind | int:
    host = os.environ.get(host_env, default_host).strip() or default_host
    raw_port = os.environ.get(port_env, str(default_port)).strip()
    try:
        port = int(raw_port)
    except ValueError:
        print(f"api: invalid {port_env}={raw_port!r}", file=sys.stderr)
        return EXIT_USAGE
    try:
        return ListenerBind(host=host, port=port)
    except ValueError as exc:
        print(f"api: invalid bind: {exc}", file=sys.stderr)
        return EXIT_USAGE


def _parse_grace(argv: Sequence[str]) -> float | int:
    grace = DEFAULT_SHUTDOWN_GRACE_SECONDS
    env_grace = os.environ.get("QUEUE_SHUTDOWN_GRACE_SECONDS", "").strip()
    if env_grace:
        try:
            grace = float(env_grace)
        except ValueError:
            print("api: invalid QUEUE_SHUTDOWN_GRACE_SECONDS", file=sys.stderr)
            return EXIT_USAGE

    args = list(argv)
    i = 0
    while i < len(args):
        arg = args[i]
        if arg in {"-h", "--help"}:
            print(
                "usage: workhold api [--grace-seconds SEC] "
                "(binds via QUEUE_APPLICATION_HOST/PORT and QUEUE_ADMIN_HOST/PORT)",
                file=sys.stderr,
            )
            return EXIT_USAGE
        if arg == "--grace-seconds":
            i += 1
            if i >= len(args):
                print("api: --grace-seconds requires a value", file=sys.stderr)
                return EXIT_USAGE
            try:
                grace = float(args[i])
            except ValueError:
                print("api: invalid --grace-seconds", file=sys.stderr)
                return EXIT_USAGE
        elif arg.startswith("--grace-seconds="):
            try:
                grace = float(arg.split("=", 1)[1])
            except ValueError:
                print("api: invalid --grace-seconds", file=sys.stderr)
                return EXIT_USAGE
        else:
            print(f"api: unknown argument {arg!r}", file=sys.stderr)
            return EXIT_USAGE
        i += 1

    if grace < 0:
        print("api: grace seconds must be non-negative", file=sys.stderr)
        return EXIT_USAGE
    return grace


def _build_plane_apps(
    deployment: settings.DeploymentSettings,
    *,
    api_engine: Engine,
    application_bind: ListenerBind,
    admin_bind: ListenerBind,
    claim_long_poll_service: ClaimLongPollService | None = None,
    metrics: KernelMetrics | None = None,
) -> tuple[Any, Any]:
    if deployment.api_credential_bindings is not None:
        bindings = deployment.api_credential_bindings
        queue_scopes = deployment.api_queue_scopes
    else:
        bindings = tuple(
            CredentialBinding(
                principal_id=gen.principal_id,
                role=ServiceRole.ADMIN,
                generation_id=gen.generation_id,
                secret=gen.secret,
            )
            for gen in deployment.credential_generations
        )
        if not bindings:
            bindings = (
                CredentialBinding(
                    principal_id="api-dev",
                    role=ServiceRole.ADMIN,
                    generation_id="env-default",
                    secret=Secret("dev-token"),
                ),
            )
        queue_scopes = {}
    authenticator = BearerCredentialAuthenticator.from_bindings(bindings)
    authorizer = Authorizer(queue_scopes=queue_scopes)
    session_factory = sessionmaker(bind=api_engine, expire_on_commit=False)
    application = create_application_app(
        authenticator=authenticator,
        authorizer=authorizer,
        bind=application_bind,
        session_factory=session_factory,
        schedule_horizon_seconds=deployment.schedule_horizon_seconds,
        claim_long_poll_service=claim_long_poll_service,
        max_wait_seconds=deployment.claim_max_wait_seconds,
    )
    admin = create_admin_app(
        authenticator=authenticator,
        authorizer=authorizer,
        bind=admin_bind,
    )
    _ = metrics  # reserved for future plane-level injection
    return application, admin


def serve(
    *,
    application_app: Any,
    admin_app: Any,
    application_bind: ListenerBind,
    admin_bind: ListenerBind,
    lifecycle: Lifecycle,
    grace_seconds: float,
    api_engine: Engine | None = None,
    admin_engine: Engine | None = None,
    schema: str | None = None,
    premake_days: int = health.DEFAULT_PARTITION_PREMAKE_DAYS,
    install_signals: bool = True,
    on_running: Callable[[], None] | None = None,
    wake_listener: ClaimWakeListener | None = None,
    waiter_admission: WaiterAdmission | None = None,
    wake_coordinator: QueueGenerationCoordinator | None = None,
    metrics: KernelMetrics | None = None,
) -> int:
    """Serve dual listeners until stop, then shut down in DEP-02 order."""
    if grace_seconds < 0:
        return EXIT_USAGE
    if application_bind.host == admin_bind.host and application_bind.port == admin_bind.port:
        print("api: application and admin binds must be distinct", file=sys.stderr)
        return EXIT_USAGE

    gate = InFlightGate()
    app_server = QuietThreadingHTTPServer(
        (application_bind.host, application_bind.port),
        AsgiRequestHandler,
        app=application_app,
        lifecycle=lifecycle,
        gate=gate,
        api_engine=api_engine,
        schema=schema,
        premake_days=premake_days,
    )
    admin_server = QuietThreadingHTTPServer(
        (admin_bind.host, admin_bind.port),
        AsgiRequestHandler,
        app=admin_app,
        lifecycle=lifecycle,
        gate=gate,
        api_engine=api_engine,
        schema=schema,
        premake_days=premake_days,
    )

    previous_term = signal.getsignal(signal.SIGTERM)
    previous_int = signal.getsignal(signal.SIGINT)
    previous_break = None
    sigbreak = getattr(signal, "SIGBREAK", None)

    def _on_signal(_signum: int, _frame: object) -> None:
        lifecycle.request_stop(grace_seconds)

    signals_installed = False
    if install_signals:
        try:
            signal.signal(signal.SIGTERM, _on_signal)
            signal.signal(signal.SIGINT, _on_signal)
            if sigbreak is not None:
                previous_break = signal.getsignal(sigbreak)
                signal.signal(sigbreak, _on_signal)
            signals_installed = True
        except ValueError:
            # signal.signal is main-thread-only; tests call serve() off-main.
            signals_installed = False

    if wake_listener is not None:
        wake_listener.start()
        if metrics is not None:
            metrics.set_claim_listener_connected(
                wake_listener.health is ListenerHealth.CONNECTED
            )
        lifecycle.record("wake_listener_started")

    app_thread = threading.Thread(
        target=app_server.serve_forever,
        kwargs={"poll_interval": 0.1},
        name="api-application-listener",
        daemon=True,
    )
    admin_thread = threading.Thread(
        target=admin_server.serve_forever,
        kwargs={"poll_interval": 0.1},
        name="api-admin-listener",
        daemon=True,
    )

    try:
        app_thread.start()
        admin_thread.start()
        lifecycle.mark_running()
        if on_running is not None:
            on_running()

        # Block until stop requested.
        while not lifecycle.stop_event.wait(timeout=0.1):
            if wake_listener is not None and metrics is not None:
                metrics.set_claim_listener_connected(
                    wake_listener.health is ListenerHealth.CONNECTED
                )

        # Shutdown order: ready already false via request_stop → stop acceptance →
        # signal waiters + wake listener → bounded in-flight wait → cancel HTTP
        # listeners → dispose pools → stopped.
        gate.stop_accepting()
        lifecycle.record("acceptance_stopped")

        if waiter_admission is not None:
            waiter_admission.stop_accepting()
        if wake_coordinator is not None:
            wake_coordinator.bump_global()
        lifecycle.record("waiters_signalled")

        if wake_listener is not None:
            wake_listener.stop()
            if metrics is not None:
                metrics.set_claim_listener_connected(False)
            lifecycle.record("wake_listener_stopped")

        deadline = lifecycle.stop_deadline_mono
        if deadline is None:
            deadline = time.monotonic() + grace_seconds
        # Drain in-flight, then hold listeners until the monotonic grace deadline so
        # readiness probes and 503 rejection remain observable during termination.
        gate.wait_empty(deadline)
        remaining = deadline - time.monotonic()
        if remaining > 0:
            time.sleep(remaining)
        lifecycle.record("inflight_wait_done")

        def _shutdown(server: QuietThreadingHTTPServer) -> None:
            try:
                server.shutdown()
            except Exception:
                pass
            try:
                server.server_close()
            except Exception:
                pass

        _shutdown(app_server)
        _shutdown(admin_server)
        app_thread.join(timeout=1.0)
        admin_thread.join(timeout=1.0)
        lifecycle.record("listeners_closed")

        for engine in (api_engine, admin_engine):
            if engine is None:
                continue
            try:
                engine.dispose()
            except Exception:
                pass
        lifecycle.record("pools_disposed")
        lifecycle.mark_stopped()
        return EXIT_OK
    finally:
        try:
            app_server.server_close()
        except Exception:
            pass
        try:
            admin_server.server_close()
        except Exception:
            pass
        if wake_listener is not None:
            try:
                wake_listener.stop(join_timeout_seconds=1.0)
            except Exception:
                pass
        if signals_installed:
            try:
                signal.signal(signal.SIGTERM, previous_term)
                signal.signal(signal.SIGINT, previous_int)
                if sigbreak is not None and previous_break is not None:
                    signal.signal(sigbreak, previous_break)
            except Exception:
                pass


def run(argv: Sequence[str] | None = None) -> int:
    """CLI entry point used by ``workhold.cli`` for the ``api`` role."""
    args = list(argv if argv is not None else ())
    parsed_grace = _parse_grace(args)
    # ``_parse_grace`` returns float on success; int exit codes on failure.
    if isinstance(parsed_grace, int):
        return parsed_grace
    grace_seconds = parsed_grace

    maybe_init_error_reporting(
        dsn=sentry_dsn_from_environ(),
        environment=os.environ.get("QUEUE_ENVIRONMENT", "development").strip() or "development",
        release=f"workhold@{__version__}",
        process_role="api",
    )

    application_bind = _parse_bind(
        "QUEUE_APPLICATION_HOST",
        "QUEUE_APPLICATION_PORT",
        "127.0.0.1",
        8080,
    )
    if isinstance(application_bind, int):
        return application_bind
    admin_bind = _parse_bind(
        "QUEUE_ADMIN_HOST",
        "QUEUE_ADMIN_PORT",
        "127.0.0.1",
        8081,
    )
    if isinstance(admin_bind, int):
        return admin_bind

    try:
        deployment = settings_from_env()
    except settings.SettingsValidationError as exc:
        print(f"api: {exc}", file=sys.stderr)
        return EXIT_DEPENDENCY
    if deployment is None:
        print("api: DATABASE_URL is required", file=sys.stderr)
        return EXIT_DEPENDENCY

    schema = os.environ.get("QUEUE_SCHEMA") or os.environ.get(
        "ALEMBIC_VERSION_TABLE_SCHEMA"
    )
    if schema is not None:
        schema = schema.strip() or None
    premake_raw = os.environ.get("QUEUE_PARTITION_PREMAKE_DAYS", "").strip()
    premake_days = health.DEFAULT_PARTITION_PREMAKE_DAYS
    if premake_raw:
        try:
            premake_days = int(premake_raw)
        except ValueError:
            print("api: invalid QUEUE_PARTITION_PREMAKE_DAYS", file=sys.stderr)
            return EXIT_USAGE
        if premake_days < 0:
            print("api: QUEUE_PARTITION_PREMAKE_DAYS must be non-negative", file=sys.stderr)
            return EXIT_USAGE

    api_engine = db.create_role_engine(deployment, "api")
    admin_engine = db.create_role_engine(deployment, "admin")
    metrics = KernelMetrics(process_role="api")
    coordinator = QueueGenerationCoordinator()
    admission = WaiterAdmission(deployment.claim_max_outstanding_waits)
    long_poll = ClaimLongPollService(
        coordinator=coordinator,
        admission=admission,
        metrics=metrics,
        fallback_seconds=deployment.claim_wait_fallback_seconds,
        probe_seconds=deployment.claim_cancellation_probe_seconds,
    )
    wake_dsn = db.to_psycopg_dsn(deployment.database_url.get_secret_value())
    wake_connect_timeout = db.psycopg_connect_timeout_seconds(
        deployment.pool_for("api").pool_acquisition_timeout_seconds
    )
    wake_listener = ClaimWakeListener(
        dsn=wake_dsn,
        coordinator=coordinator,
        connect_timeout_seconds=wake_connect_timeout,
    )
    application_app, admin_app = _build_plane_apps(
        deployment,
        api_engine=api_engine,
        application_bind=application_bind,
        admin_bind=admin_bind,
        claim_long_poll_service=long_poll,
        metrics=metrics,
    )
    lifecycle = Lifecycle()
    print(
        f"api listening application=http://{application_bind.host}:{application_bind.port} "
        f"admin=http://{admin_bind.host}:{admin_bind.port} grace={grace_seconds}",
        flush=True,
    )
    return serve(
        application_app=application_app,
        admin_app=admin_app,
        application_bind=application_bind,
        admin_bind=admin_bind,
        lifecycle=lifecycle,
        grace_seconds=grace_seconds,
        api_engine=api_engine,
        admin_engine=admin_engine,
        schema=schema,
        premake_days=premake_days,
        wake_listener=wake_listener,
        waiter_admission=admission,
        wake_coordinator=coordinator,
        metrics=metrics,
    )
