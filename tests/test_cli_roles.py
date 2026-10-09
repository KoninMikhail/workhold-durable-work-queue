"""Subprocess and injectable-runner lifecycle contract for process roles (PKG-01)."""

from __future__ import annotations

import asyncio
import json
import os
import subprocess
import sys
import time
import uuid
from collections.abc import Callable, Mapping, Sequence
from pathlib import Path
from typing import Any

import pytest

import importlib

from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from queue_service import settings
from queue_service.api.application import SKELETON_CODE, create_application_app
from queue_service.api.security import ListenerBind
from queue_service.application.completion import CompletionService
from queue_service.cli import CLI_ROLES, main
from queue_service.intake.service import EnqueueService
from queue_service.roles import api as api_role
from queue_service.security.authorization import (
    AuthorizationContext,
    AuthorizationDenied,
    Authorizer,
    Operation,
)
from queue_service.security.credentials import (
    BearerCredentialAuthenticator,
    CredentialBinding,
)
from queue_service.security.principals import Principal, ServiceRole
from queue_service.settings import Secret

REPO_ROOT = Path(__file__).resolve().parents[1]
SENTRY_DSN_SENTINEL = "SENTRY_DSN_SENTINEL_9z8y"
EXIT_USAGE = 2

_ROLE_MODULES: dict[str, str] = {
    "api": "queue_service.roles.api",
    "migrate": "queue_service.roles.migrate",
    "maintain": "queue_service.roles.maintain",
    "relay": "queue_service.roles.relay",
    "apply": "queue_service.roles.apply",
}


def _run_queue(
    args: Sequence[str],
    *,
    module: bool = False,
    env: Mapping[str, str] | None = None,
    timeout: float = 5.0,
) -> subprocess.CompletedProcess[str]:
    if module:
        cmd = [sys.executable, "-m", "queue_service", *args]
    else:
        cmd = ["uv", "run", "--no-sync", "queue", *args]
    merged = os.environ.copy()
    if env:
        merged.update(env)
    return subprocess.run(
        cmd,
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        timeout=timeout,
        env=merged,
        check=False,
    )


def test_help_with_sentry_dsn_does_not_leak_or_init() -> None:
    env = {"SENTRY_DSN": SENTRY_DSN_SENTINEL}
    for module in (False, True):
        result = _run_queue(["--help"], module=module, env=env)
        assert result.returncode == 0, result.stderr
        combined = f"{result.stdout}\n{result.stderr}"
        assert SENTRY_DSN_SENTINEL not in combined
        assert "https://" not in combined.lower()
        out = combined.lower()
        for role in CLI_ROLES:
            assert role in out, role
        assert " admin" not in f" {out}"
        assert "hello from queue!" not in out


def test_help_lists_only_five_roles_and_exits_zero() -> None:
    for module in (False, True):
        result = _run_queue(["--help"], module=module)
        assert result.returncode == 0, result.stderr
        out = f"{result.stdout}\n{result.stderr}".lower()
        for role in CLI_ROLES:
            assert role in out, role
        # Admin shares the api binary; it must not appear as a CLI role.
        assert " admin" not in f" {out}"
        assert "hello from queue!" not in out


def test_apply_is_fifth_cli_role_and_admin_absent() -> None:
    """CTRL-10: apply joins CLI_ROLES; admin stays out."""
    assert "apply" in CLI_ROLES
    assert _ROLE_MODULES.get("apply") == "queue_service.roles.apply"
    assert "admin" not in CLI_ROLES


def test_missing_role_exits_nonzero_with_usage() -> None:
    for module in (False, True):
        result = _run_queue([], module=module)
        assert result.returncode != 0
        combined = f"{result.stdout}\n{result.stderr}".lower()
        assert "usage" in combined
        for role in CLI_ROLES:
            assert role in combined
        assert "hello from queue!" not in combined


def test_unknown_role_exits_nonzero_without_runtime() -> None:
    result = _run_queue(["not-a-role"])
    assert result.returncode != 0
    combined = f"{result.stdout}\n{result.stderr}".lower()
    assert "usage" in combined
    assert "not-a-role" in combined or "unknown" in combined
    # Static usage-only errors: no settings/secret material.
    assert "database_url" not in combined
    assert "secret" not in combined
    assert "password" not in combined


@pytest.mark.parametrize("role", list(CLI_ROLES))
def test_each_role_dispatches_through_injectable_runner(role: str) -> None:
    calls: list[tuple[str, tuple[str, ...]]] = []

    def make_runner(name: str) -> Callable[[Sequence[str]], int]:
        def _runner(argv: Sequence[str]) -> int:
            calls.append((name, tuple(argv)))
            return 17 if name != "relay" else 3

        return _runner

    runners = {name: make_runner(name) for name in CLI_ROLES}
    code = main([role, "--flag", "x"], runners=runners)
    assert calls == [(role, ("--flag", "x"))]
    assert code == (3 if role == "relay" else 17)


def test_exit_code_propagation_is_bounded() -> None:
    def boom(_argv: Sequence[str]) -> int:
        return 255

    assert main(["api"], runners={"api": boom, **{
        r: (lambda _a: 0) for r in CLI_ROLES if r != "api"
    }}) == 255


def test_relay_without_database_exits_nonzero_without_side_effects() -> None:
    # No database URL — relay must fail closed fast (dependency), no publication.
    env = {
        "DATABASE_URL": "",
        "PGHOST": "",
        "PGDATABASE": "",
    }
    started = time.monotonic()
    result = _run_queue(["relay"], env=env, timeout=5.0)
    elapsed = time.monotonic() - started
    assert result.returncode != 0
    combined = f"{result.stdout}\n{result.stderr}".lower()
    assert "database_url" in combined or "required" in combined
    assert "cloudevents" not in combined
    assert elapsed < 3.0


def test_default_api_migrate_maintain_exit_deterministically() -> None:
    # Real roles fail closed without DATABASE_URL / catalog path (bounded exit).
    empty_db = {"DATABASE_URL": "", "TEST_DATABASE_URL": ""}
    for role in ("api", "migrate", "maintain", "apply"):
        env = dict(empty_db)
        if role == "apply":
            # Missing catalog path is enough for apply to fail closed (exit 2).
            env["QUEUE_CATALOG_PATH"] = ""
        started = time.monotonic()
        result = _run_queue([role], env=env, timeout=5.0)
        elapsed = time.monotonic() - started
        assert result.returncode != 0, (role, result.stdout, result.stderr)
        assert elapsed < 3.0, role
        assert "hello from queue!" not in result.stdout.lower()


@pytest.mark.parametrize("role", list(CLI_ROLES))
def test_role_help_does_not_init(
    monkeypatch: pytest.MonkeyPatch,
    role: str,
) -> None:
    module = importlib.import_module(_ROLE_MODULES[role])
    calls: list[dict[str, object]] = []

    def _fake_init(**kwargs: object) -> bool:
        calls.append(dict(kwargs))
        return False

    monkeypatch.setattr(module, "maybe_init_error_reporting", _fake_init)
    code = module.run(["--help"])
    assert code == EXIT_USAGE
    assert calls == []


@pytest.mark.parametrize("role", list(CLI_ROLES))
def test_each_role_run_calls_maybe_init_after_help(
    monkeypatch: pytest.MonkeyPatch,
    role: str,
) -> None:
    module = importlib.import_module(_ROLE_MODULES[role])
    calls: list[dict[str, object]] = []

    def _record_init(**kwargs: object) -> bool:
        calls.append(dict(kwargs))
        return False

    monkeypatch.setattr(module, "maybe_init_error_reporting", _record_init)
    monkeypatch.setattr(module, "settings_from_env", lambda *_a, **_k: None)
    monkeypatch.delenv("DATABASE_URL", raising=False)
    monkeypatch.delenv("SENTRY_DSN", raising=False)

    expected_dsn = settings.sentry_dsn_from_environ()

    code = module.run([])
    assert len(calls) == 1
    assert calls[0]["process_role"] == role
    assert calls[0]["dsn"] == expected_dsn
    assert code != 0


def test_module_and_script_entry_points_share_dispatcher() -> None:
    script = _run_queue(["--help"], module=False)
    module = _run_queue(["--help"], module=True)
    assert script.returncode == module.returncode == 0
    for role in CLI_ROLES:
        assert role in script.stdout.lower()
        assert role in module.stdout.lower()


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


def _deployment(*, schedule_horizon_seconds: int = 4321) -> settings.DeploymentSettings:
    return settings.DeploymentSettings(
        environment=settings.EnvironmentMode.DEVELOPMENT,
        listener_tls_mode=settings.ListenerTlsMode.PLAINTEXT_PUBLIC,
        database_url=Secret("postgresql+psycopg://queue:queue@localhost:5432/queue"),
        postgres_max_connections=100,
        postgres_reserved_connections=10,
        role_pools=_role_pools(),
        credential_generations=(
            settings.CredentialGeneration(
                principal_id="producer-a",
                generation_id="gen-1",
                secret=Secret("producer-token"),
            ),
        ),
        schedule_horizon_seconds=schedule_horizon_seconds,
    )


def _asgi_http_call(
    app: Any,
    *,
    method: str,
    path: str,
    headers: Mapping[str, str] | None = None,
    body: bytes = b"",
) -> tuple[int, dict[str, str], bytes]:
    header_list = [
        (k.lower().encode("latin-1"), v.encode("latin-1"))
        for k, v in (headers or {}).items()
    ]
    if body and not any(k == b"content-length" for k, _ in header_list):
        header_list.append((b"content-length", str(len(body)).encode("latin-1")))

    scope = {
        "type": "http",
        "asgi": {"version": "3.0", "spec_version": "2.3"},
        "http_version": "1.1",
        "method": method.upper(),
        "scheme": "http",
        "path": path,
        "raw_path": path.encode("utf-8"),
        "query_string": b"",
        "headers": header_list,
        "client": ("127.0.0.1", 12345),
        "server": ("test", 80),
    }

    request_body = body
    body_sent = False
    status_code = 500
    response_headers: dict[str, str] = {}
    response_chunks: list[bytes] = []

    async def receive() -> dict[str, Any]:
        nonlocal body_sent
        if not body_sent:
            body_sent = True
            return {
                "type": "http.request",
                "body": request_body,
                "more_body": False,
            }
        return {"type": "http.disconnect"}

    async def send(message: dict[str, Any]) -> None:
        nonlocal status_code, response_headers
        if message["type"] == "http.response.start":
            status_code = int(message["status"])
            response_headers = {
                k.decode("latin-1").lower(): v.decode("latin-1")
                for k, v in message.get("headers", [])
            }
        elif message["type"] == "http.response.body":
            chunk = message.get("body", b"")
            if chunk:
                response_chunks.append(chunk)

    asyncio.run(app(scope, receive, send))
    return status_code, response_headers, b"".join(response_chunks)


def test_api_role_build_plane_apps_binds_session_factory_to_api_engine() -> None:
    engine = create_engine("sqlite:///:memory:")
    deployment = _deployment(schedule_horizon_seconds=7200)
    captured: dict[str, object] = {}
    original = create_application_app

    def _capture(**kwargs: object) -> Any:
        captured.update(kwargs)
        return original(**kwargs)

    import queue_service.roles.api as api_module

    api_module.create_application_app = _capture  # type: ignore[method-assign]
    try:
        application_app, _admin = api_role._build_plane_apps(
            deployment,
            api_engine=engine,
            application_bind=ListenerBind(host="127.0.0.1", port=8080),
            admin_bind=ListenerBind(host="127.0.0.1", port=8081),
        )
    finally:
        api_module.create_application_app = original  # type: ignore[method-assign]

    session_factory = captured["session_factory"]
    assert isinstance(session_factory, sessionmaker)
    assert session_factory.kw["bind"] is engine
    assert session_factory.kw["expire_on_commit"] is False
    assert captured["schedule_horizon_seconds"] == 7200
    assert application_app is not None


def test_api_role_uses_manifest_roles_scopes_and_excludes_legacy_tokens(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    engine = create_engine("sqlite:///:memory:")
    deployment = settings.DeploymentSettings(
        environment=settings.EnvironmentMode.DEVELOPMENT,
        listener_tls_mode=settings.ListenerTlsMode.PLAINTEXT_PUBLIC,
        database_url=Secret("postgresql+psycopg://queue:queue@localhost/queue"),
        postgres_max_connections=100,
        postgres_reserved_connections=10,
        role_pools=_role_pools(),
        credential_generations=(),
        api_credential_bindings=(
            CredentialBinding(
                principal_id="producer-orders",
                role=ServiceRole.PRODUCER,
                generation_id="old",
                secret=Secret("producer-old"),
            ),
            CredentialBinding(
                principal_id="producer-orders",
                role=ServiceRole.PRODUCER,
                generation_id="current",
                secret=Secret("producer-current"),
            ),
            CredentialBinding(
                principal_id="worker-orders",
                role=ServiceRole.WORKER,
                generation_id="current-worker",
                secret=Secret("worker-current"),
            ),
        ),
        api_queue_scopes={
            "producer-orders": frozenset({"orders"}),
            "worker-orders": frozenset({"orders"}),
        },
    )
    captured: list[dict[str, object]] = []

    def _capture(**kwargs: object) -> dict[str, object]:
        captured.append(dict(kwargs))
        return dict(kwargs)

    monkeypatch.setattr(api_role, "create_application_app", _capture)
    monkeypatch.setattr(api_role, "create_admin_app", _capture)
    api_role._build_plane_apps(
        deployment,
        api_engine=engine,
        application_bind=ListenerBind(host="127.0.0.1", port=8080),
        admin_bind=ListenerBind(host="127.0.0.1", port=8081),
    )

    authenticator = captured[0]["authenticator"]
    authorizer = captured[0]["authorizer"]
    assert isinstance(authenticator, BearerCredentialAuthenticator)
    assert isinstance(authorizer, Authorizer)

    producer_old = authenticator.authenticate("Bearer producer-old")
    producer_current = authenticator.authenticate("Bearer producer-current")
    worker = authenticator.authenticate("Bearer worker-current")
    legacy = authenticator.authenticate("Bearer dev-token")
    assert isinstance(producer_old, Principal)
    assert producer_old == producer_current
    assert isinstance(worker, Principal)
    assert producer_old.role is ServiceRole.PRODUCER
    assert worker.role is ServiceRole.WORKER
    assert not isinstance(
        authorizer.authorize(
            producer_old, Operation.ENQUEUE_TASK, queue_name="orders"
        ),
        AuthorizationDenied,
    )
    assert isinstance(
        authorizer.authorize(
            producer_old, Operation.ENQUEUE_TASK, queue_name="other"
        ),
        AuthorizationDenied,
    )
    assert isinstance(
        authorizer.authorize(
            producer_old, Operation.CLAIM_TASKS, queue_name="orders"
        ),
        AuthorizationDenied,
    )
    assert isinstance(
        authorizer.authorize(worker, Operation.CLAIM_TASKS, queue_name="orders"),
        AuthorizationContext,
    )
    assert not isinstance(legacy, Principal)


def test_legacy_api_token_remains_admin_only() -> None:
    engine = create_engine("sqlite:///:memory:")
    application, _admin = api_role._build_plane_apps(
        _deployment(),
        api_engine=engine,
        application_bind=ListenerBind(host="127.0.0.1", port=8080),
        admin_bind=ListenerBind(host="127.0.0.1", port=8081),
    )
    status, _, body = _asgi_http_call(
        application,
        method="POST",
        path="/v1/queues/orders/tasks",
        headers={
            "Authorization": "Bearer producer-token",
            "Idempotency-Key": "legacy-admin-cannot-enqueue",
            "Content-Type": "application/json",
        },
        body=b'{"payload":{"x":1}}',
    )
    assert status == 403
    assert json.loads(body)["code"] == "permission_denied"


def test_invalid_manifest_stops_api_before_engine_and_does_not_leak(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    sentinel = "MANIFEST_SENTINEL_never_log"
    manifest = json.dumps(
        {
            "schema_version": 1,
            "principals": [
                {
                    "principal_id": "producer-orders",
                    "role": "PRODUCER",
                    "queue_scopes": [],
                    "credentials": [
                        {"generation_id": "current", "secret": sentinel}
                    ],
                }
            ],
        }
    )
    monkeypatch.setenv(
        "DATABASE_URL", "postgresql+psycopg://queue:queue@localhost/queue"
    )
    monkeypatch.setenv("QUEUE_API_PRINCIPALS_MANIFEST", manifest)
    monkeypatch.setattr(api_role, "maybe_init_error_reporting", lambda **_kw: False)

    def _no_engine(*_args: object, **_kwargs: object) -> object:
        raise AssertionError("invalid manifest must fail before engine creation")

    monkeypatch.setattr(api_role.db, "create_role_engine", _no_engine)
    assert api_role.run([]) == api_role.EXIT_DEPENDENCY
    captured = capsys.readouterr()
    combined = captured.out + captured.err
    assert sentinel not in combined


def test_api_role_composed_app_registers_durable_enqueue_claim_complete() -> None:
    engine = create_engine("sqlite:///:memory:")
    deployment = _deployment()
    application_app, _admin = api_role._build_plane_apps(
        deployment,
        api_engine=engine,
        application_bind=ListenerBind(host="127.0.0.1", port=8080),
        admin_bind=ListenerBind(host="127.0.0.1", port=8081),
    )

    auth = {"Authorization": "Bearer producer-token"}
    claim_id = str(uuid.uuid4())

    enqueue_status, _, enqueue_body = _asgi_http_call(
        application_app,
        method="POST",
        path="/v1/queues/orders/tasks",
        headers={
            **auth,
            "Idempotency-Key": "k1",
            "Content-Type": "application/json",
        },
        body=b'{"payload":{"x":1}}',
    )
    assert enqueue_status != 501
    assert SKELETON_CODE not in enqueue_body.decode("utf-8")

    claim_status, _, claim_body = _asgi_http_call(
        application_app,
        method="POST",
        path="/v1/claims",
        headers={
            **auth,
            "Content-Type": "application/json",
        },
        body=json.dumps({"queue_names": ["orders"], "worker_id": "worker-1"}).encode(
            "utf-8"
        ),
    )
    assert claim_status != 501
    assert SKELETON_CODE not in claim_body.decode("utf-8")

    complete_status, _, complete_body = _asgi_http_call(
        application_app,
        method="POST",
        path=f"/v1/claims/{claim_id}:complete",
        headers={
            **auth,
            "Content-Type": "application/json",
        },
        body=b'{"spawn":[]}',
    )
    assert complete_status != 501
    assert SKELETON_CODE not in complete_body.decode("utf-8")


def test_create_application_app_get_capabilities_returns_live_payload() -> None:
    engine = create_engine("sqlite:///:memory:")
    factory: sessionmaker[Session] = sessionmaker(bind=engine, expire_on_commit=False)
    authenticator = BearerCredentialAuthenticator.from_bindings(
        (
            CredentialBinding(
                principal_id="producer-a",
                role=ServiceRole.PRODUCER,
                generation_id="gen-1",
                secret=Secret("producer-token"),
            ),
        )
    )
    application_app = create_application_app(
        authenticator=authenticator,
        authorizer=Authorizer(queue_scopes={"producer-a": frozenset({"orders"})}),
        bind=ListenerBind(host="127.0.0.1", port=8080),
        session_factory=factory,
        schedule_horizon_seconds=3600,
    )
    status, headers, body = _asgi_http_call(
        application_app,
        method="GET",
        path="/v1/capabilities",
        headers={"Authorization": "Bearer producer-token"},
    )
    assert status == 200
    assert "x-request-id" in headers
    payload = json.loads(body.decode("utf-8"))
    assert payload["scheduling"] is True
    assert payload["priority"] is True
    assert payload["schema_revision"] == "0001"
    assert payload["protocol_major"] == 1
    assert "code" not in payload


def test_create_application_app_injects_schedule_horizon_into_default_services() -> None:
    engine = create_engine("sqlite:///:memory:")
    factory: sessionmaker[Session] = sessionmaker(bind=engine, expire_on_commit=False)
    authenticator = BearerCredentialAuthenticator.from_bindings(
        (
            CredentialBinding(
                principal_id="producer-a",
                role=ServiceRole.PRODUCER,
                generation_id="gen-1",
                secret=Secret("producer-token"),
            ),
        )
    )
    enqueue_kwargs: dict[str, object] = {}
    completion_kwargs: dict[str, object] = {}
    original_enqueue_init = EnqueueService.__init__
    original_completion_init = CompletionService.__init__

    def _capture_enqueue(self: EnqueueService, **kwargs: object) -> None:
        enqueue_kwargs.update(kwargs)
        original_enqueue_init(self, **kwargs)

    def _capture_completion(self: CompletionService, **kwargs: object) -> None:
        completion_kwargs.update(kwargs)
        original_completion_init(self, **kwargs)

    EnqueueService.__init__ = _capture_enqueue  # type: ignore[method-assign]
    CompletionService.__init__ = _capture_completion  # type: ignore[method-assign]
    try:
        create_application_app(
            authenticator=authenticator,
            authorizer=Authorizer(queue_scopes={}),
            bind=ListenerBind(host="127.0.0.1", port=8080),
            session_factory=factory,
            schedule_horizon_seconds=9900,
        )
    finally:
        EnqueueService.__init__ = original_enqueue_init  # type: ignore[method-assign]
        CompletionService.__init__ = original_completion_init  # type: ignore[method-assign]

    enqueue_policy = enqueue_kwargs["scheduling_policy"]
    completion_policy = completion_kwargs["scheduling_policy"]
    assert enqueue_policy.horizon_seconds == 9900
    assert completion_policy.horizon_seconds == 9900


def test_create_application_app_preserves_injected_service_scheduling_policy() -> None:
    engine = create_engine("sqlite:///:memory:")
    factory: sessionmaker[Session] = sessionmaker(bind=engine, expire_on_commit=False)
    authenticator = BearerCredentialAuthenticator.from_bindings(
        (
            CredentialBinding(
                principal_id="producer-a",
                role=ServiceRole.PRODUCER,
                generation_id="gen-1",
                secret=Secret("producer-token"),
            ),
        )
    )
    from queue_service.scheduling import SchedulingPolicy

    injected_enqueue = EnqueueService(
        session_factory=factory,
        scheduling_policy=SchedulingPolicy(horizon_seconds=111),
    )
    injected_completion = CompletionService(
        session_factory=factory,
        scheduling_policy=SchedulingPolicy(horizon_seconds=222),
    )
    enqueue_kwargs: dict[str, object] = {}
    original_enqueue_init = EnqueueService.__init__

    def _capture_enqueue(self: EnqueueService, **kwargs: object) -> None:
        enqueue_kwargs.update(kwargs)
        original_enqueue_init(self, **kwargs)

    EnqueueService.__init__ = _capture_enqueue  # type: ignore[method-assign]
    try:
        create_application_app(
            authenticator=authenticator,
            authorizer=Authorizer(queue_scopes={}),
            bind=ListenerBind(host="127.0.0.1", port=8080),
            session_factory=factory,
            enqueue_service=injected_enqueue,
            completion_service=injected_completion,
            schedule_horizon_seconds=9999,
        )
    finally:
        EnqueueService.__init__ = original_enqueue_init  # type: ignore[method-assign]

    assert enqueue_kwargs == {}
    assert injected_enqueue._scheduling_policy.horizon_seconds == 111
    assert injected_completion._scheduling_policy.horizon_seconds == 222
