"""PostgreSQL coverage for audited drain and maintenance tools (CTRL-06)."""

from __future__ import annotations

import asyncio
import json
import os
import uuid
from collections.abc import Iterator, Mapping
from typing import Any

import pytest
from sqlalchemy import create_engine, event, text
from sqlalchemy.orm import Session, sessionmaker

from queue_service.api.admin import create_admin_app
from queue_service.api.security import ListenerBind
from queue_service.domain.queue_control import (
    AdminRequestMetadata,
    BackoffStrategy,
    CreateQueueMutation,
    QueueState,
    RetryPolicyDraft,
)
from queue_service.infrastructure.postgres.queue_control_repository import (
    QueueControlRepository,
)
from queue_service.roles.maintain import MAINTENANCE_LOCK_KEY
from queue_service.security.authorization import Authorizer
from queue_service.security.credentials import (
    BearerCredentialAuthenticator,
    CredentialBinding,
)
from queue_service.security.payload_policy import PayloadRetentionPolicy
from queue_service.security.principals import ServiceRole
from queue_service.settings import Secret

pytest_plugins = ["tests.integration.conftest"]

OBSERVER_TOKEN = "tok-observer-routine"
ADMIN_TOKEN = "tok-admin-routine"
PRODUCER_TOKEN = "tok-producer-routine"
WORKER_TOKEN = "tok-worker-routine"

OBSERVER_PRINCIPAL = "observer-routine"
ADMIN_PRINCIPAL = "admin-routine"
PRODUCER_PRINCIPAL = "producer-routine"
WORKER_PRINCIPAL = "worker-routine"


def _unique(prefix: str) -> str:
    return f"{prefix}.{uuid.uuid4().hex[:8]}"


def _bindings(queue_name: str) -> tuple[CredentialBinding, ...]:
    return (
        CredentialBinding(
            principal_id=OBSERVER_PRINCIPAL,
            role=ServiceRole.OBSERVER,
            generation_id="g1",
            secret=Secret(OBSERVER_TOKEN),
        ),
        CredentialBinding(
            principal_id=ADMIN_PRINCIPAL,
            role=ServiceRole.ADMIN,
            generation_id="g1",
            secret=Secret(ADMIN_TOKEN),
        ),
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
    )


@pytest.fixture
def sa_engine(migrated_schema):
    _conn, schema = migrated_schema
    database_url = os.environ.get("TEST_DATABASE_URL", "").strip()
    if not database_url:
        pytest.fail("TEST_DATABASE_URL is required for tests/integration/operations")
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

    try:
        yield engine
    finally:
        engine.dispose()


@pytest.fixture
def sa_session(sa_engine) -> Iterator[Session]:
    factory = sessionmaker(bind=sa_engine, expire_on_commit=False)
    session = factory()
    try:
        yield session
    finally:
        session.close()


@pytest.fixture
def session_factory(sa_engine) -> sessionmaker[Session]:
    return sessionmaker(bind=sa_engine, expire_on_commit=False)


@pytest.fixture
def admin_app(sa_engine, session_factory: sessionmaker[Session]) -> Any:
    queue_name = "orders.routine"
    return create_admin_app(
        authenticator=BearerCredentialAuthenticator.from_bindings(
            _bindings(queue_name)
        ),
        authorizer=Authorizer(
            queue_scopes={
                OBSERVER_PRINCIPAL: frozenset({queue_name}),
                ADMIN_PRINCIPAL: frozenset({queue_name}),
                PRODUCER_PRINCIPAL: frozenset({queue_name}),
                WORKER_PRINCIPAL: frozenset({queue_name}),
            }
        ),
        bind=ListenerBind(host="127.0.0.1", port=18096),
        session_factory=session_factory,
        repository=QueueControlRepository(),
        engine=sa_engine,
        payload_retention_policy=PayloadRetentionPolicy(retention_days=30),
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
        "client": ("127.0.0.1", 9),
        "server": ("127.0.0.1", 18096),
    }
    status_box: dict[str, int] = {}
    header_box: dict[str, str] = {}
    body_chunks: list[bytes] = []

    async def receive() -> dict[str, Any]:
        return {"type": "http.request", "body": body, "more_body": False}

    async def send(message: dict[str, Any]) -> None:
        if message["type"] == "http.response.start":
            status_box["status"] = int(message["status"])
            for key, value in message.get("headers", []):
                header_box[key.decode("latin-1").lower()] = value.decode("latin-1")
        elif message["type"] == "http.response.body":
            body_chunks.append(message.get("body", b"") or b"")

    asyncio.run(app(scope, receive, send))
    return status_box["status"], header_box, b"".join(body_chunks)


def _auth(token: str) -> dict[str, str]:
    return {"authorization": f"Bearer {token}"}


def _seed_queue(session: Session, *, name: str) -> None:
    QueueControlRepository().create_named_queue(
        session,
        CreateQueueMutation(
            name=name,
            initial_policy=RetryPolicyDraft(
                enabled=True,
                max_attempts=3,
                backoff_strategy=BackoffStrategy.FIXED,
                retry_delay_seconds=5,
            ),
            metadata=AdminRequestMetadata(
                actor_id=ADMIN_PRINCIPAL,
                request_id=str(uuid.uuid4()),
                idempotency_key=f"seed-{uuid.uuid4().hex}",
            ),
        ),
    )
    session.commit()


def test_admin_drain_requires_config_version_and_reports_zero_depth(
    admin_app: Any,
    sa_session: Session,
    session_factory: sessionmaker[Session],
) -> None:
    queue_name = _unique("orders.drain")
    # Rebuild app with this queue in observer scope.
    app = create_admin_app(
        authenticator=BearerCredentialAuthenticator.from_bindings(_bindings(queue_name)),
        authorizer=Authorizer(
            queue_scopes={
                OBSERVER_PRINCIPAL: frozenset({queue_name}),
                ADMIN_PRINCIPAL: frozenset({queue_name}),
                PRODUCER_PRINCIPAL: frozenset({queue_name}),
                WORKER_PRINCIPAL: frozenset({queue_name}),
            }
        ),
        bind=ListenerBind(host="127.0.0.1", port=18097),
        session_factory=session_factory,
        repository=QueueControlRepository(),
        engine=session_factory.kw["bind"],
        payload_retention_policy=PayloadRetentionPolicy(retention_days=30),
    )
    _seed_queue(sa_session, name=queue_name)

    # Bump counters to prove completion is only at zero depth.
    sa_session.execute(
        text(
            """
            UPDATE queue_counters
            SET ready_count = 2, delayed_count = 1, leased_count = 0
            WHERE queue_id = (SELECT id FROM queues WHERE name = :name)
            """
        ),
        {"name": queue_name},
    )
    sa_session.commit()

    bad = _asgi_http_call(
        app,
        method="POST",
        path=f"/admin/v1/queues/{queue_name}:set-state",
        headers={
            **_auth(ADMIN_TOKEN),
            "idempotency-key": f"drain-bad-{uuid.uuid4().hex}",
            "content-type": "application/json",
        },
        body=json.dumps(
            {"expected_config_version": 99, "state": "draining"}
        ).encode("utf-8"),
    )
    assert bad[0] == 412

    status, _, body = _asgi_http_call(
        app,
        method="POST",
        path=f"/admin/v1/queues/{queue_name}:set-state",
        headers={
            **_auth(ADMIN_TOKEN),
            "idempotency-key": f"drain-ok-{uuid.uuid4().hex}",
            "content-type": "application/json",
        },
        body=json.dumps(
            {"expected_config_version": 1, "state": "draining"}
        ).encode("utf-8"),
    )
    assert status == 200
    payload = json.loads(body.decode("utf-8"))
    assert payload["queue"]["state"] == QueueState.DRAINING.value
    assert payload["queue"]["active_depth"] == 3
    assert payload["queue"]["drain_complete"] is False

    audit = sa_session.execute(
        text(
            """
            SELECT actor_id, operation_code, details
            FROM admin_audit_log
            WHERE queue_id = (SELECT id FROM queues WHERE name = :name)
            ORDER BY audit_at DESC, id DESC
            LIMIT 1
            """
        ),
        {"name": queue_name},
    ).mappings().one()
    assert audit["actor_id"] == ADMIN_PRINCIPAL
    assert int(audit["operation_code"]) == 4
    assert audit["details"]["new_state"] == "draining"

    # Clear depth → drain_complete
    sa_session.execute(
        text(
            """
            UPDATE queue_counters
            SET ready_count = 0, delayed_count = 0, leased_count = 0
            WHERE queue_id = (SELECT id FROM queues WHERE name = :name)
            """
        ),
        {"name": queue_name},
    )
    sa_session.commit()

    get_status, _, get_body = _asgi_http_call(
        app,
        method="GET",
        path=f"/admin/v1/queues/{queue_name}",
        headers=_auth(OBSERVER_TOKEN),
    )
    assert get_status == 200
    observed = json.loads(get_body.decode("utf-8"))
    assert observed["state"] == "draining"
    assert observed["active_depth"] == 0
    assert observed["drain_complete"] is True


def test_observer_cannot_trigger_drain_or_maintenance_producer_worker_denied(
    admin_app: Any,
    sa_session: Session,
) -> None:
    queue_name = "orders.routine"
    _seed_queue(sa_session, name=queue_name)

    for token in (OBSERVER_TOKEN, PRODUCER_TOKEN, WORKER_TOKEN):
        status, _, body = _asgi_http_call(
            admin_app,
            method="POST",
            path=f"/admin/v1/queues/{queue_name}:set-state",
            headers={
                **_auth(token),
                "idempotency-key": f"deny-{uuid.uuid4().hex}",
                "content-type": "application/json",
            },
            body=json.dumps(
                {"expected_config_version": 1, "state": "draining"}
            ).encode("utf-8"),
        )
        assert status == 403
        assert json.loads(body.decode("utf-8"))["code"] == "permission_denied"

        status, _, body = _asgi_http_call(
            admin_app,
            method="POST",
            path="/admin/v1/maintenance:run",
            headers={
                **_auth(token),
                "idempotency-key": f"maint-deny-{uuid.uuid4().hex}",
            },
        )
        assert status == 403

    # Observer may read maintenance status
    status, _, _ = _asgi_http_call(
        admin_app,
        method="GET",
        path="/admin/v1/maintenance",
        headers=_auth(OBSERVER_TOKEN),
    )
    assert status == 200


def test_maintenance_trigger_idempotent_and_single_winner(
    admin_app: Any,
    sa_engine,
    sa_session: Session,
) -> None:
    idem = f"maint-{uuid.uuid4().hex}"

    status, _, body = _asgi_http_call(
        admin_app,
        method="POST",
        path="/admin/v1/maintenance:run",
        headers={**_auth(ADMIN_TOKEN), "idempotency-key": idem},
    )
    assert status == 200
    first = json.loads(body.decode("utf-8"))
    assert first["replayed"] is False
    assert "status" in first
    assert "last_error_detail" not in first["status"]
    assert "sql" not in json.dumps(first).lower()

    status2, _, body2 = _asgi_http_call(
        admin_app,
        method="POST",
        path="/admin/v1/maintenance:run",
        headers={**_auth(ADMIN_TOKEN), "idempotency-key": idem},
    )
    assert status2 == 200
    second = json.loads(body2.decode("utf-8"))
    assert second["replayed"] is True

    audit_count = sa_session.execute(
        text("SELECT COUNT(*) FROM admin_audit_log WHERE operation_code = 5")
    ).scalar_one()
    assert int(audit_count) >= 1

    # Hold the advisory lock on a sibling connection → skipped_lock, no second race.
    raw = sa_engine.raw_connection()
    try:
        cur = raw.cursor()
        cur.execute("SELECT pg_advisory_lock(%s)", (MAINTENANCE_LOCK_KEY,))
        raw.commit()
        try:
            status3, _, body3 = _asgi_http_call(
                admin_app,
                method="POST",
                path="/admin/v1/maintenance:run",
                headers={
                    **_auth(ADMIN_TOKEN),
                    "idempotency-key": f"maint-lock-{uuid.uuid4().hex}",
                },
            )
            assert status3 == 200
            skipped = json.loads(body3.decode("utf-8"))
            assert skipped["status"].get("outcome") == "skipped_lock"
            assert skipped["replayed"] is False
        finally:
            cur.execute("SELECT pg_advisory_unlock(%s)", (MAINTENANCE_LOCK_KEY,))
            raw.commit()
            cur.close()
    finally:
        raw.close()


def test_routine_admin_handlers_emit_allowlisted_correlation_logs(
    admin_app: Any,
    sa_session: Session,
    session_factory: sessionmaker[Session],
    caplog: pytest.LogCaptureFixture,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """OPS-08: drain/maintenance handlers emit projected correlation at runtime."""
    import logging

    from queue_service.api import admin_operations as ops_mod
    from queue_service.api import admin_queues as queues_mod
    from queue_service.operations import routine as routine_mod

    queue_name = _unique("orders.corr")
    app = create_admin_app(
        authenticator=BearerCredentialAuthenticator.from_bindings(_bindings(queue_name)),
        authorizer=Authorizer(
            queue_scopes={
                OBSERVER_PRINCIPAL: frozenset({queue_name}),
                ADMIN_PRINCIPAL: frozenset({queue_name}),
                PRODUCER_PRINCIPAL: frozenset({queue_name}),
                WORKER_PRINCIPAL: frozenset({queue_name}),
            }
        ),
        bind=ListenerBind(host="127.0.0.1", port=18098),
        session_factory=session_factory,
        repository=QueueControlRepository(),
        engine=session_factory.kw["bind"],
        payload_retention_policy=PayloadRetentionPolicy(retention_days=30),
    )
    _seed_queue(sa_session, name=queue_name)

    emitted: list[dict[str, Any]] = []
    original = routine_mod.emit_routine_admin_correlation

    def _capture(logger: logging.Logger, **kwargs: Any) -> dict[str, Any]:
        projected = original(logger, **kwargs)
        emitted.append(dict(projected))
        return projected

    monkeypatch.setattr(ops_mod, "emit_routine_admin_correlation", _capture)
    monkeypatch.setattr(queues_mod, "emit_routine_admin_correlation", _capture)

    with (
        caplog.at_level(logging.INFO, logger="queue_service.api.admin_operations"),
        caplog.at_level(logging.INFO, logger="queue_service.api.admin_queues"),
    ):
        status_m, _, _ = _asgi_http_call(
            app,
            method="GET",
            path="/admin/v1/maintenance",
            headers=_auth(ADMIN_TOKEN),
        )
        assert status_m == 200

        status_d, _, _ = _asgi_http_call(
            app,
            method="POST",
            path=f"/admin/v1/queues/{queue_name}:set-state",
            headers={
                **_auth(ADMIN_TOKEN),
                "idempotency-key": f"drain-corr-{uuid.uuid4().hex}",
                "content-type": "application/json",
            },
            body=json.dumps(
                {"expected_config_version": 1, "state": "draining"}
            ).encode("utf-8"),
        )
        assert status_d == 200

        denied, _, _ = _asgi_http_call(
            app,
            method="POST",
            path="/admin/v1/maintenance:run",
            headers=_auth(ADMIN_TOKEN),
        )
        assert denied == 400

    assert len(emitted) >= 3
    results = {row.get("result") for row in emitted}
    assert "success" in results
    assert "denied" in results
    for row in emitted:
        assert "actor_id" in row
        assert row["actor_id"] == ADMIN_PRINCIPAL
        assert "operation" in row
        for denied_key in ("payload", "claim_token", "dsn", "sql"):
            assert denied_key not in row

    joined = " ".join(r.getMessage() for r in caplog.records)
    if joined:
        assert "routine_admin" in joined
        for denied_key in ("payload", "claim_token", "dsn", "sql"):
            assert denied_key not in joined
