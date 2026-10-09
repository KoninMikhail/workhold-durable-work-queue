"""Black-box producer HTTP resolveSubmission conformance (Phase 03.4-06).

Covers WORK-02 / API-02 / API-03 addressability: owner resolve by queue +
idempotency key, non-disclosing cross-producer and unauthorized-queue outcomes,
expired/unknown not-found, indexed enqueue_dedup lookup without payload search.
"""

from __future__ import annotations

import asyncio
import json
import os
import uuid
from collections.abc import Iterator, Mapping
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest
from sqlalchemy import create_engine, event, select, text, update
from sqlalchemy.orm import Session, sessionmaker

from queue_service.api.application import create_application_app
from queue_service.api.security import ListenerBind
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
from queue_service.intake.repository import EnqueueRepository
from queue_service.intake.service import EnqueueService
from queue_service.security.authorization import Authorizer
from queue_service.security.credentials import (
    BearerCredentialAuthenticator,
    CredentialBinding,
)
from queue_service.security.principals import ServiceRole
from queue_service.settings import Secret
from queue_service.storage.models import EnqueueDedup, Queue
from tests.conformance.harness import ConformanceHarness, ObservedResponse

pytest_plugins = ["tests.integration.conftest"]

REPO_ROOT = Path(__file__).resolve().parents[2]
OPENAPI_PATH = REPO_ROOT / "openapi" / "queue.openapi.json"

PRODUCER_TOKEN = "tok-producer-resolve-http"
PRODUCER_OTHER_TOKEN = "tok-producer-resolve-other"
PRODUCER_UNSCOPED_TOKEN = "tok-producer-resolve-unscoped"
WORKER_TOKEN = "tok-worker-resolve-http"
ADMIN_TOKEN = "tok-admin-resolve-http"

PRODUCER_PRINCIPAL = "producer-resolve-http"
PRODUCER_OTHER_PRINCIPAL = "producer-resolve-other"
PRODUCER_UNSCOPED_PRINCIPAL = "producer-resolve-unscoped"
WORKER_PRINCIPAL = "worker-resolve-http"
ADMIN_PRINCIPAL = "admin-resolve-http"

BASE_QUEUE_NAME = "orders.resolve"
OTHER_QUEUE = "billing.resolve"
PAYLOAD_SENTINEL = "SECRET_PAYLOAD_SHOULD_NEVER_LEAK"
CREDENTIAL_SENTINEL = PRODUCER_TOKEN


def _unique_queue_name(prefix: str = BASE_QUEUE_NAME) -> str:
    return f"{prefix}.{uuid.uuid4().hex[:12]}"


def _bindings() -> tuple[CredentialBinding, ...]:
    return (
        CredentialBinding(
            principal_id=PRODUCER_PRINCIPAL,
            role=ServiceRole.PRODUCER,
            generation_id="g1",
            secret=Secret(PRODUCER_TOKEN),
        ),
        CredentialBinding(
            principal_id=PRODUCER_OTHER_PRINCIPAL,
            role=ServiceRole.PRODUCER,
            generation_id="g1",
            secret=Secret(PRODUCER_OTHER_TOKEN),
        ),
        CredentialBinding(
            principal_id=PRODUCER_UNSCOPED_PRINCIPAL,
            role=ServiceRole.PRODUCER,
            generation_id="g1",
            secret=Secret(PRODUCER_UNSCOPED_TOKEN),
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
    )


@pytest.fixture
def queue_name() -> str:
    return _unique_queue_name()


@pytest.fixture
def authorizer(queue_name: str) -> Authorizer:
    # Owner + other producer share named-queue scope so cross-producer resolve
    # is not conflated with 403 queue-scope denial.
    return Authorizer(
        queue_scopes={
            PRODUCER_PRINCIPAL: frozenset({queue_name, BASE_QUEUE_NAME}),
            PRODUCER_OTHER_PRINCIPAL: frozenset({queue_name, OTHER_QUEUE}),
            PRODUCER_UNSCOPED_PRINCIPAL: frozenset({OTHER_QUEUE}),
            WORKER_PRINCIPAL: frozenset({queue_name, BASE_QUEUE_NAME}),
            ADMIN_PRINCIPAL: frozenset({queue_name, BASE_QUEUE_NAME}),
        }
    )


@pytest.fixture
def session_factory(migrated_schema) -> Iterator[sessionmaker[Session]]:
    _conn, schema = migrated_schema
    database_url = os.environ.get("TEST_DATABASE_URL", "").strip()
    if not database_url:
        pytest.fail("TEST_DATABASE_URL is required for resolveSubmission HTTP conformance")
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


@pytest.fixture
def app(
    session_factory: sessionmaker[Session],
    authorizer: Authorizer,
) -> Any:
    service = EnqueueService(
        session_factory=session_factory,
        depth_ceilings=DepthCeilings(
            queue_active_depth=100,
            instance_active_depth=500,
            retry_after_ms=250,
        ),
    )
    return create_application_app(
        authenticator=BearerCredentialAuthenticator.from_bindings(_bindings()),
        authorizer=authorizer,
        bind=ListenerBind(host="127.0.0.1", port=18092),
        session_factory=session_factory,
        enqueue_service=service,
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


def _admin_meta() -> AdminRequestMetadata:
    return AdminRequestMetadata(
        actor_id=ADMIN_PRINCIPAL,
        request_id=str(uuid.uuid4()),
        idempotency_key=f"admin-idem-{uuid.uuid4().hex}",
    )


def _seed_queue(session: Session, *, name: str) -> Queue:
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
            metadata=_admin_meta(),
        ),
    )
    session.commit()
    return session.execute(select(Queue).where(Queue.name == name)).scalar_one()


def _producer_headers(*, token: str = PRODUCER_TOKEN) -> dict[str, str]:
    return {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }


def _enqueue_headers(
    *,
    idempotency_key: str,
    token: str = PRODUCER_TOKEN,
) -> dict[str, str]:
    headers = _producer_headers(token=token)
    headers["Idempotency-Key"] = idempotency_key
    return headers


def _resolve_path(queue_name: str) -> str:
    return f"/v1/queues/{queue_name}/submissions:resolve"


def _resolve_body(*, idempotency_key: str) -> bytes:
    return json.dumps({"idempotency_key": idempotency_key}).encode("utf-8")


def _assert_error(
    body: bytes,
    *,
    code: str,
    retryable: bool,
) -> dict[str, Any]:
    payload = json.loads(body.decode("utf-8"))
    assert isinstance(payload, dict)
    assert payload["code"] == code
    assert payload["retryable"] is retryable
    assert "request_id" in payload
    assert isinstance(payload.get("details"), dict)
    assert "message" in payload
    return payload


def _assert_no_secrets_or_payload(payload: Any, *, raw_response: bytes = b"") -> None:
    text = json.dumps(payload, default=str)
    combined = text + raw_response.decode("utf-8", errors="replace")
    for forbidden in (
        CREDENTIAL_SENTINEL,
        PRODUCER_OTHER_TOKEN,
        PRODUCER_UNSCOPED_TOKEN,
        WORKER_TOKEN,
        ADMIN_TOKEN,
        PAYLOAD_SENTINEL,
        "claim_token",
        "password",
        "Bearer ",
    ):
        assert forbidden not in combined


def _validate_resolve_response(
    harness: ConformanceHarness,
    *,
    status: int,
    headers: Mapping[str, str],
    body: bytes,
) -> dict[str, Any]:
    payload = json.loads(body.decode("utf-8"))
    findings = harness._validate_response(  # noqa: SLF001 - schema gate
        "resolveSubmission",
        ObservedResponse(
            status=status,
            headers=dict(headers),
            body_text=body.decode("utf-8"),
            body_json=payload,
            content_type=headers.get("content-type", "application/json"),
        ),
    )
    assert findings == [], findings
    return payload


def _validate_error_response(
    harness: ConformanceHarness,
    *,
    status: int,
    headers: Mapping[str, str],
    body: bytes,
) -> dict[str, Any]:
    payload = json.loads(body.decode("utf-8"))
    findings = harness._validate_response(  # noqa: SLF001
        "resolveSubmission",
        ObservedResponse(
            status=status,
            headers=dict(headers),
            body_text=body.decode("utf-8"),
            body_json=payload,
            content_type=headers.get("content-type", "application/json"),
        ),
    )
    assert findings == [], findings
    return payload


def _enqueue_task(
    app: Any,
    *,
    queue_name: str,
    idempotency_key: str,
    payload: Any | None = None,
    token: str = PRODUCER_TOKEN,
) -> str:
    body = json.dumps(
        {
            "payload": payload if payload is not None else {"n": 1},
            "priority": 0,
        }
    ).encode("utf-8")
    status, _headers, raw = _asgi_http_call(
        app,
        method="POST",
        path=f"/v1/queues/{queue_name}/tasks",
        headers=_enqueue_headers(idempotency_key=idempotency_key, token=token),
        body=body,
    )
    assert status in {200, 201}, raw.decode("utf-8")
    return str(json.loads(raw.decode("utf-8"))["task"]["task_id"])


def test_resolve_request_fixture_is_closed() -> None:
    harness = ConformanceHarness(
        openapi_path=OPENAPI_PATH,
        base_url="http://127.0.0.1:9",
    )
    harness.validate_request_fixture(
        "resolveSubmission",
        {"idempotency_key": "idem-resolve-1"},
    )
    with pytest.raises(Exception):
        harness.validate_request_fixture(
            "resolveSubmission",
            {"idempotency_key": "idem-resolve-1", "payload": {"x": 1}},
        )


def test_owner_resolve_by_queue_and_key_matches_enqueued_task(
    app: Any,
    session_factory: sessionmaker[Session],
    queue_name: str,
) -> None:
    harness = ConformanceHarness(
        openapi_path=OPENAPI_PATH,
        base_url="http://127.0.0.1:9",
    )
    session = session_factory()
    try:
        _seed_queue(session, name=queue_name)
    finally:
        session.close()

    idem = f"idem-owner-{uuid.uuid4().hex}"
    task_id = _enqueue_task(
        app,
        queue_name=queue_name,
        idempotency_key=idem,
        payload={"secret": PAYLOAD_SENTINEL},
    )

    status, headers, raw = _asgi_http_call(
        app,
        method="POST",
        path=_resolve_path(queue_name),
        headers=_producer_headers(),
        body=_resolve_body(idempotency_key=idem),
    )
    assert status == 200, raw.decode("utf-8")
    payload = _validate_resolve_response(
        harness, status=status, headers=headers, body=raw
    )
    assert payload["task"]["task_id"] == task_id
    assert payload["task"]["queue_name"] == queue_name
    assert payload["task"]["producer_id"] == PRODUCER_PRINCIPAL
    assert "payload" not in payload["task"] or payload["task"].get("payload") is None
    assert "current_claim" not in payload["task"] or payload["task"].get("current_claim") is None
    assert "dedup_expires_at" in payload
    _assert_no_secrets_or_payload(payload, raw_response=raw)

    session = session_factory()
    try:
        dedup = session.execute(
            select(EnqueueDedup).where(EnqueueDedup.task_id == uuid.UUID(task_id))
        ).scalar_one()
        assert dedup.expires_at == datetime.fromisoformat(
            payload["dedup_expires_at"].replace("Z", "+00:00")
        )
        # Default ADR017 window: ~90 days from creation.
        delta = dedup.expires_at - dedup.created_at
        assert timedelta(days=89) <= delta <= timedelta(days=91)
    finally:
        session.close()


def test_cross_producer_and_unauthorized_queue_do_not_disclose(
    app: Any,
    session_factory: sessionmaker[Session],
    queue_name: str,
) -> None:
    harness = ConformanceHarness(
        openapi_path=OPENAPI_PATH,
        base_url="http://127.0.0.1:9",
    )
    session = session_factory()
    try:
        _seed_queue(session, name=queue_name)
    finally:
        session.close()

    idem = f"idem-cross-{uuid.uuid4().hex}"
    task_id = _enqueue_task(
        app,
        queue_name=queue_name,
        idempotency_key=idem,
        payload={"secret": PAYLOAD_SENTINEL},
    )

    # Same queue scope, different producer → non-disclosing task_not_found.
    status, headers, raw = _asgi_http_call(
        app,
        method="POST",
        path=_resolve_path(queue_name),
        headers=_producer_headers(token=PRODUCER_OTHER_TOKEN),
        body=_resolve_body(idempotency_key=idem),
    )
    assert status == 404, raw.decode("utf-8")
    err = _validate_error_response(harness, status=status, headers=headers, body=raw)
    _assert_error(raw, code="task_not_found", retryable=False)
    assert task_id not in raw.decode("utf-8")
    _assert_no_secrets_or_payload(err, raw_response=raw)

    # Authenticated producer without queue scope → permission_denied before lookup.
    status2, headers2, raw2 = _asgi_http_call(
        app,
        method="POST",
        path=_resolve_path(queue_name),
        headers=_producer_headers(token=PRODUCER_UNSCOPED_TOKEN),
        body=_resolve_body(idempotency_key=idem),
    )
    assert status2 == 403, raw2.decode("utf-8")
    err2 = _validate_error_response(harness, status=status2, headers=headers2, body=raw2)
    _assert_error(raw2, code="permission_denied", retryable=False)
    assert task_id not in raw2.decode("utf-8")
    _assert_no_secrets_or_payload(err2, raw_response=raw2)


def test_unknown_and_expired_registry_return_task_not_found(
    app: Any,
    session_factory: sessionmaker[Session],
    queue_name: str,
) -> None:
    harness = ConformanceHarness(
        openapi_path=OPENAPI_PATH,
        base_url="http://127.0.0.1:9",
    )
    session = session_factory()
    try:
        _seed_queue(session, name=queue_name)
    finally:
        session.close()

    status, headers, raw = _asgi_http_call(
        app,
        method="POST",
        path=_resolve_path(queue_name),
        headers=_producer_headers(),
        body=_resolve_body(idempotency_key=f"idem-missing-{uuid.uuid4().hex}"),
    )
    assert status == 404, raw.decode("utf-8")
    err = _validate_error_response(harness, status=status, headers=headers, body=raw)
    _assert_error(raw, code="task_not_found", retryable=False)
    _assert_no_secrets_or_payload(err, raw_response=raw)

    idem = f"idem-expired-{uuid.uuid4().hex}"
    task_id = _enqueue_task(
        app,
        queue_name=queue_name,
        idempotency_key=idem,
        payload={"secret": PAYLOAD_SENTINEL},
    )

    session = session_factory()
    try:
        now = datetime.now(tz=UTC)
        created = now - timedelta(days=100)
        expires = created + timedelta(days=90)  # 10 days in the past
        session.execute(
            update(EnqueueDedup)
            .where(EnqueueDedup.task_id == uuid.UUID(task_id))
            .values(created_at=created, expires_at=expires)
        )
        session.commit()
    finally:
        session.close()

    status2, headers2, raw2 = _asgi_http_call(
        app,
        method="POST",
        path=_resolve_path(queue_name),
        headers=_producer_headers(),
        body=_resolve_body(idempotency_key=idem),
    )
    assert status2 == 404, raw2.decode("utf-8")
    err2 = _validate_error_response(harness, status=status2, headers=headers2, body=raw2)
    _assert_error(raw2, code="task_not_found", retryable=False)
    assert task_id not in raw2.decode("utf-8")
    _assert_no_secrets_or_payload(err2, raw_response=raw2)


def test_resolve_uses_indexed_dedup_lookup_without_payload_predicate(
    app: Any,
    session_factory: sessionmaker[Session],
    queue_name: str,
) -> None:
    session = session_factory()
    try:
        _seed_queue(session, name=queue_name)
        bind = session.get_bind()
    finally:
        session.close()

    idem = f"idem-sql-{uuid.uuid4().hex}"
    _enqueue_task(
        app,
        queue_name=queue_name,
        idempotency_key=idem,
        payload={"secret": PAYLOAD_SENTINEL, "biz_key": "should-not-be-queried"},
    )

    captured_sql: list[str] = []
    captured_params: list[Any] = []

    def _capture(_conn, _cursor, statement, parameters, _context, _executemany) -> None:  # noqa: ANN001
        sql = statement if isinstance(statement, str) else str(statement)
        captured_sql.append(sql)
        if parameters is not None:
            captured_params.append(parameters)

    event.listen(bind, "before_cursor_execute", _capture)
    try:
        status, _headers, raw = _asgi_http_call(
            app,
            method="POST",
            path=_resolve_path(queue_name),
            headers=_producer_headers(),
            body=_resolve_body(idempotency_key=idem),
        )
    finally:
        event.remove(bind, "before_cursor_execute", _capture)

    assert status == 200, raw.decode("utf-8")
    joined = "\n".join(captured_sql).lower()
    assert "enqueue_dedup" in joined
    assert "producer_id" in joined
    assert "queue_id" in joined
    assert "key_hash" in joined
    # No payload-field predicate / JSON search on opaque body.
    assert "task_payloads" not in joined
    assert "->>" not in joined
    assert "@>" not in joined
    assert "jsonb" not in joined
    # Dedup registry is consulted before projecting the active task row.
    dedup_idx = next(
        i for i, sql in enumerate(captured_sql) if "enqueue_dedup" in sql.lower()
    )
    task_idx = next(
        (i for i, sql in enumerate(captured_sql) if "tasks_active" in sql.lower()),
        None,
    )
    assert task_idx is None or dedup_idx < task_idx
    # Bound key_hash equals enqueue correctness-registry digest (not payload).
    key_hash = EnqueueRepository.key_hash_for(idem)

    def _as_bytes(value: Any) -> bytes | None:
        if isinstance(value, (bytes, bytearray, memoryview)):
            return bytes(value)
        adapted = getattr(value, "adapted", None)
        if isinstance(adapted, (bytes, bytearray, memoryview)):
            return bytes(adapted)
        for attr in ("value", "obj", "payload", "data"):
            candidate = getattr(value, attr, None)
            if isinstance(candidate, (bytes, bytearray, memoryview)):
                return bytes(candidate)
        try:
            return bytes(value)  # type: ignore[arg-type]
        except (TypeError, ValueError):
            return None

    def _params_contain_hash(params: Any) -> bool:
        if isinstance(params, dict):
            values = params.values()
        elif isinstance(params, (list, tuple)):
            values = params
        else:
            return False
        for value in values:
            raw = _as_bytes(value)
            if raw == key_hash:
                return True
        return False

    assert any(_params_contain_hash(p) for p in captured_params), captured_params

    # Sanity: raw SQL against registry still finds the scoped row.
    session = session_factory()
    try:
        queue = session.execute(select(Queue).where(Queue.name == queue_name)).scalar_one()
        row = session.execute(
            text(
                "SELECT task_id FROM enqueue_dedup "
                "WHERE producer_id = :producer_id "
                "AND queue_id = :queue_id "
                "AND key_hash = :key_hash"
            ),
            {
                "producer_id": PRODUCER_PRINCIPAL,
                "queue_id": int(queue.id),
                "key_hash": key_hash,
            },
        ).one()
        assert row is not None
    finally:
        session.close()
