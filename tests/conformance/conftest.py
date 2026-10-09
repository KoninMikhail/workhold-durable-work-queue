"""Shared live Queue + PostgreSQL fixtures for dual-client kernel conformance."""

from __future__ import annotations

import os
import socket
import threading
import uuid
from collections.abc import Iterator
from typing import Any

import pytest
from sqlalchemy import create_engine, event, select
from sqlalchemy.orm import Session, sessionmaker

from workhold.api.admin import create_admin_app
from workhold.api.application import create_application_app
from workhold.api.security import ListenerBind
from workhold.application.claim_service import ClaimService
from workhold.application.completion import CompletionService
from workhold.application.lease_service import LeaseService
from workhold.domain.queue_control import (
    AdminRequestMetadata,
    BackoffStrategy,
    ConfigVersion,
    CreateQueueMutation,
    QueueState,
    RetryPolicyDraft,
    SetQueueStateMutation,
)
from workhold.infrastructure.postgres.queue_control_repository import (
    QueueControlRepository,
)
from workhold.intake.depth import DepthCeilings
from workhold.intake.service import EnqueueService
from workhold.lifecycle import Lifecycle
from workhold.roles.api import AsgiRequestHandler, InFlightGate, QuietThreadingHTTPServer
from workhold.security.authorization import Authorizer
from workhold.security.credentials import (
    BearerCredentialAuthenticator,
    CredentialBinding,
)
from workhold.security.principals import ServiceRole
from workhold.settings import Secret
from workhold.storage.models import Queue
from tests.conformance.clients import CLIENT_KINDS, ClientKind, build_client

# Integration DB fixtures come from tests/conftest.py (top-level pytest_plugins).
# Nested pytest_plugins here breaks whole-suite collection on modern pytest.

PRODUCER_TOKEN = "tok-producer-kernel-dual"
WORKER_TOKEN = "tok-worker-kernel-dual"
ADMIN_TOKEN = "tok-admin-kernel-dual"
OBSERVER_TOKEN = "tok-observer-kernel-dual"
FOREIGN_TOKEN = "tok-foreign-kernel-dual"

PRODUCER_PRINCIPAL = "producer-kernel-dual"
WORKER_PRINCIPAL = "worker-kernel-dual"
ADMIN_PRINCIPAL = "admin-kernel-dual"
OBSERVER_PRINCIPAL = "observer-kernel-dual"
FOREIGN_PRINCIPAL = "foreign-kernel-dual"

LEASE_SECONDS = 30


def _bindings() -> tuple[CredentialBinding, ...]:
    return (
        CredentialBinding(
            principal_id=PRODUCER_PRINCIPAL,
            role=ServiceRole.PRODUCER,
            generation_id="g1",
            secret=Secret(PRODUCER_TOKEN),
        ),
        CredentialBinding(
            principal_id=WORKER_PRINCIPAL,
            role=ServiceRole.WORKER,
            generation_id="g1",
            secret=Secret(WORKER_TOKEN),
        ),
        CredentialBinding(
            principal_id=ADMIN_PRINCIPAL,
            role=ServiceRole.ADMIN,
            generation_id="g1",
            secret=Secret(ADMIN_TOKEN),
        ),
        CredentialBinding(
            principal_id=OBSERVER_PRINCIPAL,
            role=ServiceRole.OBSERVER,
            generation_id="g1",
            secret=Secret(OBSERVER_TOKEN),
        ),
        CredentialBinding(
            principal_id=FOREIGN_PRINCIPAL,
            role=ServiceRole.PRODUCER,
            generation_id="g1",
            secret=Secret(FOREIGN_TOKEN),
        ),
    )


def _admin_meta() -> AdminRequestMetadata:
    return AdminRequestMetadata(
        actor_id="kernel-dual-seed",
        request_id=str(uuid.uuid4()),
        idempotency_key=f"idem-{uuid.uuid4().hex}",
    )


@pytest.fixture
def queue_name() -> str:
    return f"kernel.dual.{uuid.uuid4().hex[:12]}"


@pytest.fixture
def authorizer(queue_name: str) -> Authorizer:
    scoped = frozenset({queue_name})
    # Admin GET/policy/state are queue-scoped; grant the seeded name plus a
    # control-plane sibling used by typed AdminClient create/read tests.
    admin_scoped = frozenset({queue_name, f"{queue_name}.ctl"})
    return Authorizer(
        queue_scopes={
            PRODUCER_PRINCIPAL: scoped,
            WORKER_PRINCIPAL: scoped,
            ADMIN_PRINCIPAL: admin_scoped,
            OBSERVER_PRINCIPAL: scoped,
            FOREIGN_PRINCIPAL: frozenset({f"other.{uuid.uuid4().hex[:8]}"}),
        }
    )


@pytest.fixture
def session_factory(migrated_schema) -> Iterator[sessionmaker[Session]]:
    _conn, schema = migrated_schema
    database_url = os.environ.get("TEST_DATABASE_URL", "").strip()
    if not database_url:
        pytest.fail("TEST_DATABASE_URL is required for dual-client kernel conformance")
    engine = create_engine(database_url, pool_pre_ping=True)

    @event.listens_for(engine, "connect")
    def _set_search_path(dbapi_connection, _connection_record) -> None:  # noqa: ANN001
        previous = dbapi_connection.autocommit
        dbapi_connection.autocommit = True
        try:
            cursor = dbapi_connection.cursor()
            cursor.execute(f'SET search_path TO "{schema}"')
            cursor.close()
        finally:
            dbapi_connection.autocommit = previous

    factory = sessionmaker(bind=engine, expire_on_commit=False)
    try:
        yield factory
    finally:
        engine.dispose()


def seed_queue(session: Session, *, name: str) -> Queue:
    QueueControlRepository().create_named_queue(
        session,
        CreateQueueMutation(
            name=name,
            initial_policy=RetryPolicyDraft(
                enabled=True,
                max_attempts=3,
                backoff_strategy=BackoffStrategy.FIXED,
                retry_delay_seconds=0,
            ),
            metadata=_admin_meta(),
        ),
    )
    session.commit()
    return session.execute(select(Queue).where(Queue.name == name)).scalar_one()


def set_queue_state(
    session: Session,
    *,
    queue_name: str,
    state: QueueState,
    expected_config_version: int,
) -> None:
    QueueControlRepository().set_queue_state(
        session,
        queue_name=queue_name,
        mutation=SetQueueStateMutation(
            expected_config_version=ConfigVersion(value=expected_config_version),
            state=state,
            metadata=_admin_meta(),
        ),
    )
    session.commit()


def _build_admin_app(
    session_factory: sessionmaker[Session],
    authorizer: Authorizer,
) -> Any:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
        probe.bind(("127.0.0.1", 0))
        port = int(probe.getsockname()[1])
    return create_admin_app(
        authenticator=BearerCredentialAuthenticator.from_bindings(_bindings()),
        authorizer=authorizer,
        bind=ListenerBind(host="127.0.0.1", port=port),
        session_factory=session_factory,
        repository=QueueControlRepository(),
    )


def _start_http_server(app: Any, *, thread_name: str) -> tuple[str, QuietThreadingHTTPServer, threading.Thread]:
    lifecycle = Lifecycle()
    gate = InFlightGate()
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
        probe.bind(("127.0.0.1", 0))
        port = int(probe.getsockname()[1])
    server = QuietThreadingHTTPServer(
        ("127.0.0.1", port),
        AsgiRequestHandler,
        app=app,
        lifecycle=lifecycle,
        gate=gate,
        api_engine=None,
        schema=None,
        premake_days=0,
    )
    lifecycle.mark_running()
    thread = threading.Thread(
        target=server.serve_forever,
        kwargs={"poll_interval": 0.05},
        name=thread_name,
        daemon=True,
    )
    thread.start()
    return f"http://127.0.0.1:{port}", server, thread


def _stop_http_server(server: QuietThreadingHTTPServer, thread: threading.Thread) -> None:
    try:
        server.shutdown()
    except Exception:  # noqa: BLE001
        pass
    try:
        server.server_close()
    except Exception:  # noqa: BLE001
        pass
    thread.join(timeout=2.0)


def _build_app(
    session_factory: sessionmaker[Session],
    authorizer: Authorizer,
) -> Any:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
        probe.bind(("127.0.0.1", 0))
        port = int(probe.getsockname()[1])
    return create_application_app(
        authenticator=BearerCredentialAuthenticator.from_bindings(_bindings()),
        authorizer=authorizer,
        bind=ListenerBind(host="127.0.0.1", port=port),
        session_factory=session_factory,
        enqueue_service=EnqueueService(
            session_factory=session_factory,
            depth_ceilings=DepthCeilings(
                queue_active_depth=100,
                instance_active_depth=500,
                retry_after_ms=250,
            ),
        ),
        claim_service=ClaimService(session_factory=session_factory),
        lease_service=LeaseService(session_factory=session_factory),
        completion_service=CompletionService(session_factory=session_factory),
    )


@pytest.fixture
def live_service_url(
    session_factory: sessionmaker[Session],
    authorizer: Authorizer,
    queue_name: str,
) -> Iterator[str]:
    """Boot one real application-plane HTTP server over the migrated PostgreSQL schema."""

    session = session_factory()
    try:
        seed_queue(session, name=queue_name)
    finally:
        session.close()

    app = _build_app(session_factory, authorizer)
    url, server, thread = _start_http_server(app, thread_name="kernel-dual-client")
    try:
        yield url
    finally:
        _stop_http_server(server, thread)


@pytest.fixture
def live_admin_service_url(
    session_factory: sessionmaker[Session],
    authorizer: Authorizer,
) -> Iterator[str]:
    """Boot the private admin-plane HTTP server over the same PostgreSQL schema."""

    app = _build_admin_app(session_factory, authorizer)
    url, server, thread = _start_http_server(app, thread_name="kernel-dual-admin")
    try:
        yield url
    finally:
        _stop_http_server(server, thread)


@pytest.fixture(params=list(CLIENT_KINDS), ids=list(CLIENT_KINDS))
def client_kind(request: pytest.FixtureRequest) -> ClientKind:
    return request.param  # type: ignore[no-any-return]


@pytest.fixture
def conformance_client(
    client_kind: ClientKind,
    live_service_url: str,
    live_admin_service_url: str,
):
    """Both kinds share the same live service URL and PostgreSQL schema."""

    return build_client(
        client_kind,
        live_service_url,
        admin_base_url=live_admin_service_url,
    )
