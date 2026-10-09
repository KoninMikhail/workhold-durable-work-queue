"""Real Queue + independent app-PostgreSQL fixtures for bridge conformance."""

from __future__ import annotations

import json
import os
import socket
import threading
import uuid
from collections.abc import Iterator
from dataclasses import dataclass
from typing import Any

import psycopg
import pytest
from sqlalchemy import create_engine, event, select
from sqlalchemy.orm import Session, sessionmaker

from queue_service.api.application import create_application_app
from queue_service.api.security import ListenerBind
from queue_service.application.claim_service import ClaimService
from queue_service.application.completion import CompletionService
from queue_service.application.lease_service import LeaseService
from queue_service.domain.queue_control import (
    AdminRequestMetadata,
    BackoffStrategy,
    CreateQueueMutation,
    RetryPolicyDraft,
)
from queue_service.infrastructure.postgres.queue_control_repository import (
    QueueControlRepository,
)
from queue_service.intake.depth import DepthCeilings
from queue_service.intake.service import EnqueueService
from queue_service.lifecycle import Lifecycle
from queue_service.roles.api import AsgiRequestHandler, InFlightGate, QuietThreadingHTTPServer
from queue_service.security.authorization import Authorizer
from queue_service.security.credentials import (
    BearerCredentialAuthenticator,
    CredentialBinding,
)
from queue_service.security.principals import ServiceRole
from queue_service.settings import Secret
from queue_service.storage.models import Queue
from queue_service_producer.client import ProducerClient
from _queue_service_client_core.transport import HttpJsonTransport
from tests.integration.conftest import require_test_database_url, to_psycopg_conninfo

PROCESS_KILL_REPEATS = 25
REPLICA_RECLAIM_CYCLES = 50

PRODUCER_TOKEN = "tok-producer-bridge-crash"
ADMIN_TOKEN = "tok-admin-bridge-crash"
PRODUCER_PRINCIPAL = "producer-bridge-crash"
ADMIN_PRINCIPAL = "admin-bridge-crash"

OUTBOX_TABLE = "enqueue_outbox"


def _admin_meta() -> AdminRequestMetadata:
    return AdminRequestMetadata(
        actor_id="bridge-crash-seed",
        request_id=str(uuid.uuid4()),
        idempotency_key=f"idem-{uuid.uuid4().hex}",
    )


def _bindings() -> tuple[CredentialBinding, ...]:
    return (
        CredentialBinding(
            principal_id=PRODUCER_PRINCIPAL,
            role=ServiceRole.PRODUCER,
            generation_id="g1",
            secret=Secret(PRODUCER_TOKEN),
        ),
        CredentialBinding(
            principal_id=ADMIN_PRINCIPAL,
            role=ServiceRole.ADMIN,
            generation_id="g1",
            secret=Secret(ADMIN_TOKEN),
        ),
    )


@dataclass
class BridgeWorld:
    """Independent app outbox schema + live Queue HTTP surface."""

    database_url: str
    queue_schema: str
    app_schema: str
    app_table: str
    queue_name: str
    base_url: str
    producer: ProducerClient
    producer_token: str
    session_factory: sessionmaker[Session]
    _server: QuietThreadingHTTPServer
    _thread: threading.Thread
    _lifecycle: Lifecycle
    _app: Any
    _authorizer: Authorizer

    @property
    def app_conninfo(self) -> str:
        return to_psycopg_conninfo(self.database_url)

    def restart_queue_http(self) -> None:
        """Stop and rebind a fresh application-plane HTTP server on the same DB."""
        self._shutdown_server()
        self._start_server()
        self.producer = ProducerClient(
            HttpJsonTransport(self.base_url, timeout_s=10.0),
            bearer_token=self.producer_token,
        )

    def reset_intent_pending_with_payload(
        self,
        *,
        namespace: str,
        row_id: str,
        payload: dict[str, Any],
    ) -> None:
        """Reuse the same source identity with a changed enqueue body (app-side)."""
        body = {"payload": payload, "priority": 0}
        conn = psycopg.connect(self.app_conninfo)
        try:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    UPDATE "{self.app_schema}"."{self.app_table}"
                    SET
                        enqueue_request = %s::jsonb,
                        state = 'pending',
                        ownership_token = NULL,
                        lease_expires_at = NULL,
                        available_at = now(),
                        updated_at = now(),
                        queue_task_id = NULL,
                        last_failure_code = NULL
                    WHERE source_namespace = %s AND source_row_id = %s
                    """,
                    (json.dumps(body), namespace, row_id),
                )
            conn.commit()
        finally:
            conn.close()

    def count_current_leases(self) -> int:
        conn = psycopg.connect(self.app_conninfo)
        try:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT count(*)
                    FROM "{self.app_schema}"."{self.app_table}"
                    WHERE state = 'leased'
                      AND lease_expires_at IS NOT NULL
                      AND lease_expires_at > now()
                    """
                )
                return int(cur.fetchone()[0])
        finally:
            conn.close()

    def _build_app(self) -> Any:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
            probe.bind(("127.0.0.1", 0))
            port = int(probe.getsockname()[1])
        return create_application_app(
            authenticator=BearerCredentialAuthenticator.from_bindings(_bindings()),
            authorizer=self._authorizer,
            bind=ListenerBind(host="127.0.0.1", port=port),
            session_factory=self.session_factory,
            enqueue_service=EnqueueService(
                session_factory=self.session_factory,
                depth_ceilings=DepthCeilings(
                    queue_active_depth=500,
                    instance_active_depth=2000,
                    retry_after_ms=250,
                ),
            ),
            claim_service=ClaimService(session_factory=self.session_factory),
            lease_service=LeaseService(session_factory=self.session_factory),
            completion_service=CompletionService(session_factory=self.session_factory),
        )

    def _start_server(self) -> None:
        self._app = self._build_app()
        self._lifecycle = Lifecycle()
        gate = InFlightGate()
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
            probe.bind(("127.0.0.1", 0))
            port = int(probe.getsockname()[1])
        self._server = QuietThreadingHTTPServer(
            ("127.0.0.1", port),
            AsgiRequestHandler,
            app=self._app,
            lifecycle=self._lifecycle,
            gate=gate,
            api_engine=None,
            schema=None,
            premake_days=0,
        )
        self._lifecycle.mark_running()
        self._thread = threading.Thread(
            target=self._server.serve_forever,
            kwargs={"poll_interval": 0.05},
            name="bridge-crash-queue",
            daemon=True,
        )
        self._thread.start()
        self.base_url = f"http://127.0.0.1:{port}"

    def _shutdown_server(self) -> None:
        try:
            self._server.shutdown()
        except Exception:  # noqa: BLE001
            pass
        try:
            self._server.server_close()
        except Exception:  # noqa: BLE001
            pass
        self._thread.join(timeout=2.0)


def seed_pending_intent(
    world: BridgeWorld,
    *,
    namespace: str,
    row_id: str,
    payload: dict[str, Any],
) -> None:
    """Seed one pending intent through an application-side transaction."""
    body = {"payload": payload, "priority": 0}
    conn = psycopg.connect(world.app_conninfo)
    try:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                INSERT INTO "{world.app_schema}"."{world.app_table}" (
                    source_namespace, source_row_id, schema_version, target_queue,
                    enqueue_request, created_at, state, ownership_token, generation,
                    lease_expires_at, available_at, updated_at
                ) VALUES (
                    %s, %s, 1, %s,
                    %s::jsonb,
                    now(),
                    'pending', NULL, 0,
                    NULL,
                    now(),
                    now()
                )
                """,
                (namespace, row_id, world.queue_name, json.dumps(body)),
            )
        conn.commit()
    finally:
        conn.close()


def read_intent_row(
    world: BridgeWorld, *, namespace: str, row_id: str
) -> dict[str, Any]:
    conn = psycopg.connect(world.app_conninfo)
    try:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT state, ownership_token, generation, queue_task_id,
                       last_failure_code, lease_expires_at
                FROM "{world.app_schema}"."{world.app_table}"
                WHERE source_namespace = %s AND source_row_id = %s
                """,
                (namespace, row_id),
            )
            row = cur.fetchone()
            if row is None:
                raise LookupError(f"intent missing: {namespace}/{row_id}")
            return {
                "state": row[0],
                "ownership_token": row[1],
                "generation": int(row[2]),
                "queue_task_id": row[3],
                "last_failure_code": row[4],
                "lease_expires_at": row[5],
            }
    finally:
        conn.close()


def force_expire_lease(
    world: BridgeWorld, *, namespace: str, row_id: str
) -> None:
    """Make an app-owned lease reclaimable under application-DB time."""
    conn = psycopg.connect(world.app_conninfo)
    try:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                UPDATE "{world.app_schema}"."{world.app_table}"
                SET lease_expires_at = now() - interval '1 second',
                    updated_at = now()
                WHERE source_namespace = %s AND source_row_id = %s
                  AND state = 'leased'
                """,
                (namespace, row_id),
            )
        conn.commit()
    finally:
        conn.close()


def _create_app_outbox_schema(conninfo: str) -> tuple[str, str]:
    schema = f"br_app_{uuid.uuid4().hex[:16]}"
    table = OUTBOX_TABLE
    admin = psycopg.connect(conninfo)
    admin.autocommit = True
    try:
        admin.execute(f'CREATE SCHEMA "{schema}"')
        admin.execute(
            f"""
            CREATE TABLE "{schema}"."{table}" (
                source_namespace text NOT NULL,
                source_row_id text NOT NULL,
                schema_version integer NOT NULL,
                target_queue text NOT NULL,
                enqueue_request jsonb NOT NULL,
                created_at timestamptz NOT NULL,
                traceparent text,
                tracestate text,
                extensions jsonb,
                state text NOT NULL,
                ownership_token text,
                generation integer NOT NULL DEFAULT 0,
                lease_expires_at timestamptz,
                available_at timestamptz NOT NULL,
                updated_at timestamptz NOT NULL,
                queue_task_id text,
                last_failure_code text,
                PRIMARY KEY (source_namespace, source_row_id)
            )
            """
        )
        admin.execute(
            f"""
            CREATE INDEX "{table}_claim_idx"
            ON "{schema}"."{table}" (available_at, source_namespace, source_row_id)
            WHERE state IN ('pending', 'retryable_failure')
               OR (state = 'leased' AND lease_expires_at IS NOT NULL)
            """
        )
    finally:
        admin.close()
    return schema, table


def _drop_schema(conninfo: str, schema: str) -> None:
    drop = psycopg.connect(conninfo)
    drop.autocommit = True
    try:
        drop.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
    finally:
        drop.close()


def _seed_queue(session: Session, *, name: str) -> Queue:
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


@pytest.fixture
def bridge_world(migrated_schema) -> Iterator[BridgeWorld]:
    """Live Queue (Alembic schema) + independent app outbox schema."""
    database_url = require_test_database_url()
    _conn, queue_schema = migrated_schema
    conninfo = to_psycopg_conninfo(database_url)
    app_schema, app_table = _create_app_outbox_schema(conninfo)

    engine = create_engine(database_url, pool_pre_ping=True)

    @event.listens_for(engine, "connect")
    def _set_search_path(dbapi_connection, _connection_record) -> None:  # noqa: ANN001
        previous = dbapi_connection.autocommit
        dbapi_connection.autocommit = True
        try:
            cursor = dbapi_connection.cursor()
            cursor.execute(f'SET search_path TO "{queue_schema}"')
            cursor.close()
        finally:
            dbapi_connection.autocommit = previous

    factory = sessionmaker(bind=engine, expire_on_commit=False)
    queue_name = f"bridge.crash.{uuid.uuid4().hex[:12]}"
    authorizer = Authorizer(
        queue_scopes={
            PRODUCER_PRINCIPAL: frozenset({queue_name}),
            ADMIN_PRINCIPAL: frozenset({queue_name}),
        }
    )

    session = factory()
    try:
        _seed_queue(session, name=queue_name)
    finally:
        session.close()

    world = BridgeWorld(
        database_url=database_url,
        queue_schema=queue_schema,
        app_schema=app_schema,
        app_table=app_table,
        queue_name=queue_name,
        base_url="",
        producer=ProducerClient(
            HttpJsonTransport("http://127.0.0.1:9", timeout_s=10.0),
            bearer_token=PRODUCER_TOKEN,
        ),
        producer_token=PRODUCER_TOKEN,
        session_factory=factory,
        _server=None,  # type: ignore[arg-type]
        _thread=None,  # type: ignore[arg-type]
        _lifecycle=None,  # type: ignore[arg-type]
        _app=None,
        _authorizer=authorizer,
    )
    try:
        world._start_server()
        world.producer = ProducerClient(
            HttpJsonTransport(world.base_url, timeout_s=10.0),
            bearer_token=PRODUCER_TOKEN,
        )
        yield world
    finally:
        try:
            world._shutdown_server()
        except Exception:  # noqa: BLE001
            pass
        engine.dispose()
        _drop_schema(conninfo, app_schema)
