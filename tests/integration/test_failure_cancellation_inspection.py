"""PostgreSQL-backed task/attempt inspection for Phase 03.6-06.

Covers WORK-06/11 visibility: retry-scheduled, dead-letter, immediate cancel,
cancel-requested leases, and expiry cancellation — without claim tokens or
business-result fields. Diagnostics stay exact and bounded.
"""

from __future__ import annotations

import asyncio
import json
import os
import re
import uuid
from collections.abc import Iterator, Mapping
from dataclasses import dataclass
from datetime import datetime
from typing import Any
from uuid import UUID

import pytest
from sqlalchemy import create_engine, event, select, text, update
from sqlalchemy.orm import Session, sessionmaker

from queue_service.api.application import create_application_app
from queue_service.api.security import ListenerBind
from queue_service.application.claim_service import ClaimService
from queue_service.application.lease_expiry import LeaseExpiryService
from queue_service.application.lease_service import LeaseService
from queue_service.application.worker_terminal import WorkerTerminalService
from queue_service.domain.queue_control import (
    AdminRequestMetadata,
    BackoffStrategy,
    CreateQueueMutation,
    RetryPolicyDraft,
)
from queue_service.domain.retry import LEASE_EXPIRY_FAILURE_CODE
from queue_service.infrastructure.postgres.queue_control_repository import (
    QueueControlRepository,
)
from queue_service.intake.depth import DepthCeilings
from queue_service.intake.service import EnqueueService
from queue_service.security.authorization import Authorizer
from queue_service.security.credentials import (
    BearerCredentialAuthenticator,
    CredentialBinding,
)
from queue_service.security.payload_policy import PayloadRetentionPolicy
from queue_service.security.principals import ServiceRole
from queue_service.settings import Secret
from queue_service.storage.models import ClaimRegistry, TaskActive, TaskAttempt

pytest_plugins = ["tests.integration.conftest"]

PRODUCER_TOKEN = "tok-producer-inspect-http"
PRODUCER_OTHER_TOKEN = "tok-producer-inspect-other"
WORKER_TOKEN = "tok-worker-inspect-http"
OBSERVER_TOKEN = "tok-observer-inspect-http"
ADMIN_TOKEN = "tok-admin-inspect-http"

PRODUCER_PRINCIPAL = "producer-inspect-http"
PRODUCER_OTHER_PRINCIPAL = "producer-inspect-other"
WORKER_PRINCIPAL = "worker-inspect-http"
OBSERVER_PRINCIPAL = "observer-inspect-http"
ADMIN_PRINCIPAL = "admin-inspect-http"

BASE_QUEUE_NAME = "orders.inspect"
OTHER_QUEUE = "billing.inspect"
PAYLOAD_SENTINEL = "INSPECT_SECRET_PAYLOAD_NEVER_AS_RESULT"
CLAIM_PATH = "/v1/claims"
REPLICA_ID = "pool-inspect/replica-1"
CLAIM_TOKEN_HEADER = "X-Queue-Claim-Token"
FAILURE_CODE_RE = re.compile(r"^[a-z][a-z0-9._-]{0,127}$")
UNICODE_DETAIL = "café 🚀\n\t exact whitespace"
EXPECTED_WORKER_ID = f"{WORKER_PRINCIPAL}/{REPLICA_ID}"


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
            principal_id=WORKER_PRINCIPAL,
            role=ServiceRole.WORKER,
            generation_id="g1",
            secret=Secret(WORKER_TOKEN),
        ),
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
    )


@pytest.fixture
def queue_name() -> str:
    return _unique_queue_name()


@pytest.fixture
def authorizer(queue_name: str) -> Authorizer:
    return Authorizer(
        queue_scopes={
            PRODUCER_PRINCIPAL: frozenset({queue_name, BASE_QUEUE_NAME, OTHER_QUEUE}),
            PRODUCER_OTHER_PRINCIPAL: frozenset({queue_name, BASE_QUEUE_NAME}),
            WORKER_PRINCIPAL: frozenset({queue_name, BASE_QUEUE_NAME}),
            OBSERVER_PRINCIPAL: frozenset({queue_name, BASE_QUEUE_NAME}),
            ADMIN_PRINCIPAL: frozenset({queue_name, BASE_QUEUE_NAME}),
        }
    )


@pytest.fixture
def session_factory(migrated_schema) -> Iterator[sessionmaker[Session]]:
    _conn, schema = migrated_schema
    database_url = os.environ.get("TEST_DATABASE_URL", "").strip()
    if not database_url:
        pytest.fail("TEST_DATABASE_URL is required for inspection integration tests")
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
    enqueue_service = EnqueueService(
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
        bind=ListenerBind(host="127.0.0.1", port=18106),
        session_factory=session_factory,
        enqueue_service=enqueue_service,
        claim_service=ClaimService(session_factory=session_factory),
        lease_service=LeaseService(session_factory=session_factory),
        worker_terminal_service=WorkerTerminalService(session_factory=session_factory),
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
    if "?" in path:
        path_only, query = path.split("?", 1)
    else:
        path_only, query = path, ""

    async def _run() -> tuple[int, dict[str, str], bytes]:
        status_box: dict[str, int] = {}
        header_box: dict[str, str] = {}
        body_parts: list[bytes] = []

        async def receive() -> dict[str, Any]:
            return {"type": "http.request", "body": body, "more_body": False}

        async def send(message: dict[str, Any]) -> None:
            if message["type"] == "http.response.start":
                status_box["status"] = int(message["status"])
                for raw_k, raw_v in message.get("headers", []):
                    header_box[raw_k.decode("latin-1").lower()] = raw_v.decode("latin-1")
            elif message["type"] == "http.response.body":
                body_parts.append(message.get("body", b""))

        scope = {
            "type": "http",
            "asgi": {"version": "3.0"},
            "http_version": "1.1",
            "method": method,
            "path": path_only,
            "raw_path": path_only.encode("utf-8"),
            "query_string": query.encode("latin-1"),
            "headers": header_list,
            "client": ("127.0.0.1", 9),
            "server": ("127.0.0.1", 18106),
            "scheme": "http",
        }
        await app(scope, receive, send)
        return status_box["status"], header_box, b"".join(body_parts)

    return asyncio.run(_run())


def _seed_queue(
    session_factory: sessionmaker[Session],
    *,
    queue_name: str,
    enabled: bool = True,
    max_attempts: int = 3,
    retry_delay_seconds: int = 30,
) -> None:
    session = session_factory()
    try:
        QueueControlRepository().create_named_queue(
            session,
            CreateQueueMutation(
                name=queue_name,
                initial_policy=RetryPolicyDraft(
                    enabled=enabled,
                    max_attempts=max_attempts,
                    backoff_strategy=BackoffStrategy.FIXED,
                    retry_delay_seconds=retry_delay_seconds,
                ),
                metadata=AdminRequestMetadata(
                    actor_id=ADMIN_PRINCIPAL,
                    request_id=str(uuid.uuid4()),
                    idempotency_key=f"seed-{queue_name}-{uuid.uuid4().hex[:8]}",
                ),
            ),
        )
        session.commit()
    finally:
        session.close()


def _enqueue_ready(
    app: Any,
    *,
    queue_name: str,
    payload: dict[str, Any] | None = None,
) -> str:
    path = f"/v1/queues/{queue_name}/tasks"
    body = json.dumps(
        {"payload": payload or {"marker": PAYLOAD_SENTINEL}, "priority": 0},
        separators=(",", ":"),
    ).encode("utf-8")
    headers = {
        "Authorization": f"Bearer {PRODUCER_TOKEN}",
        "Content-Type": "application/json",
        "Idempotency-Key": f"ik-{uuid.uuid4().hex}",
    }
    status, _hdrs, resp = _asgi_http_call(
        app, method="POST", path=path, headers=headers, body=body
    )
    assert status == 201, resp.decode("utf-8", errors="replace")
    return json.loads(resp.decode("utf-8"))["task"]["task_id"]


def _claim_one(
    app: Any,
    *,
    queue_name: str,
    lease_seconds: int = 60,
) -> tuple[str, str, int, str]:
    body = json.dumps(
        {
            "queues": [queue_name],
            "max_tasks": 1,
            "lease_seconds": lease_seconds,
            "wait_seconds": 0,
            "worker_id": REPLICA_ID,
        },
        separators=(",", ":"),
    ).encode("utf-8")
    headers = {
        "Authorization": f"Bearer {WORKER_TOKEN}",
        "Content-Type": "application/json",
    }
    status, _hdrs, resp = _asgi_http_call(
        app, method="POST", path=CLAIM_PATH, headers=headers, body=body
    )
    assert status == 200, resp.decode("utf-8", errors="replace")
    payload = json.loads(resp.decode("utf-8"))
    assert payload["tasks"], payload
    claimed = payload["tasks"][0]
    claim = claimed["claim"]
    return (
        claim["claim_id"],
        claim["claim_token"],
        int(claim["generation"]),
        claimed["task"]["task_id"],
    )


def _fail(
    app: Any,
    *,
    claim_id: str,
    claim_token: str,
    generation: int,
    retryable: bool,
    failure_code: str,
    failure_detail: str | None,
) -> dict[str, Any]:
    body_obj: dict[str, Any] = {
        "generation": generation,
        "retryable": retryable,
        "failure_code": failure_code,
    }
    if failure_detail is not None:
        body_obj["failure_detail"] = failure_detail
    body = json.dumps(body_obj, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    headers = {
        "Authorization": f"Bearer {WORKER_TOKEN}",
        "Content-Type": "application/json",
        CLAIM_TOKEN_HEADER: claim_token,
    }
    status, _hdrs, resp = _asgi_http_call(
        app,
        method="POST",
        path=f"/v1/claims/{claim_id}:fail",
        headers=headers,
        body=body,
    )
    assert status == 200, resp.decode("utf-8", errors="replace")
    return json.loads(resp.decode("utf-8"))


def _cancel(app: Any, *, task_id: str, token: str = PRODUCER_TOKEN) -> dict[str, Any]:
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }
    status, _hdrs, resp = _asgi_http_call(
        app,
        method="POST",
        path=f"/v1/tasks/{task_id}:cancel",
        headers=headers,
        body=b"{}",
    )
    assert status == 200, resp.decode("utf-8", errors="replace")
    return json.loads(resp.decode("utf-8"))


def _get_task(
    app: Any,
    *,
    task_id: str,
    token: str,
) -> tuple[int, dict[str, Any]]:
    headers = {"Authorization": f"Bearer {token}"}
    status, _hdrs, resp = _asgi_http_call(
        app,
        method="GET",
        path=f"/v1/tasks/{task_id}",
        headers=headers,
    )
    payload = json.loads(resp.decode("utf-8"))
    return status, payload


def _list_attempts(
    app: Any,
    *,
    task_id: str,
    token: str,
    cursor: str | None = None,
    limit: int | None = None,
) -> tuple[int, dict[str, Any]]:
    query: list[str] = []
    if cursor is not None:
        query.append(f"cursor={cursor}")
    if limit is not None:
        query.append(f"limit={limit}")
    suffix = f"?{'&'.join(query)}" if query else ""
    headers = {"Authorization": f"Bearer {token}"}
    status, _hdrs, resp = _asgi_http_call(
        app,
        method="GET",
        path=f"/v1/tasks/{task_id}/attempts{suffix}",
        headers=headers,
    )
    payload = json.loads(resp.decode("utf-8"))
    return status, payload


def _assert_no_secret_leak(payload: Any, *, claim_token: str | None = None) -> None:
    dumped = json.dumps(payload, ensure_ascii=False)
    assert "claim_token" not in dumped
    assert '"result"' not in dumped
    assert "parse_result" not in dumped
    if claim_token:
        assert claim_token not in dumped
    assert PAYLOAD_SENTINEL not in dumped


def _assert_failure_diagnostics(
    *,
    failure_code: str | None,
    failure_detail: str | None,
    expected_code: str,
    expected_detail: str,
) -> None:
    assert failure_code == expected_code
    assert FAILURE_CODE_RE.fullmatch(failure_code or "") is not None
    assert 1 <= len(failure_code or "") <= 128
    assert failure_detail == expected_detail
    assert len(failure_detail) <= 4096


def test_retry_scheduled_task_inspection_exposes_state_and_diagnostics(
    app: Any,
    session_factory: sessionmaker[Session],
    queue_name: str,
) -> None:
    _seed_queue(
        session_factory,
        queue_name=queue_name,
        enabled=True,
        max_attempts=3,
        retry_delay_seconds=45,
    )
    task_id = _enqueue_ready(app, queue_name=queue_name)
    claim_id, claim_token, generation, _ = _claim_one(app, queue_name=queue_name)
    _fail(
        app,
        claim_id=claim_id,
        claim_token=claim_token,
        generation=generation,
        retryable=True,
        failure_code="worker.timeout",
        failure_detail=UNICODE_DETAIL,
    )

    status, task = _get_task(app, task_id=task_id, token=OBSERVER_TOKEN)
    assert status == 200
    assert task["task_id"] == task_id
    assert task["state"] == "retry_scheduled"
    assert task["retry_policy_version"] == 1
    assert "available_at" in task
    assert task.get("terminal_at") in (None, )
    # last failure diagnostics on active retry-scheduled view
    _assert_failure_diagnostics(
        failure_code=task.get("failure_code"),
        failure_detail=task.get("failure_detail"),
        expected_code="worker.timeout",
        expected_detail=UNICODE_DETAIL,
    )
    _assert_no_secret_leak(task, claim_token=claim_token)

    status, page = _list_attempts(app, task_id=task_id, token=OBSERVER_TOKEN)
    assert status == 200
    assert "items" in page
    assert len(page["items"]) == 1
    attempt = page["items"][0]
    assert attempt["generation"] == generation
    assert attempt["worker_id"] == EXPECTED_WORKER_ID
    assert attempt["outcome"] == "retry_scheduled"
    assert attempt["ended_at"] is not None
    assert "claimed_at" in attempt and "lease_expires_at" in attempt
    _assert_failure_diagnostics(
        failure_code=attempt.get("failure_code"),
        failure_detail=attempt.get("failure_detail"),
        expected_code="worker.timeout",
        expected_detail=UNICODE_DETAIL,
    )
    _assert_no_secret_leak(page, claim_token=claim_token)


def test_dead_lettered_task_inspection_exposes_terminal_diagnostics(
    app: Any,
    session_factory: sessionmaker[Session],
    queue_name: str,
) -> None:
    _seed_queue(
        session_factory,
        queue_name=queue_name,
        enabled=True,
        max_attempts=1,
        retry_delay_seconds=0,
    )
    task_id = _enqueue_ready(app, queue_name=queue_name)
    claim_id, claim_token, generation, _ = _claim_one(app, queue_name=queue_name)
    _fail(
        app,
        claim_id=claim_id,
        claim_token=claim_token,
        generation=generation,
        retryable=False,
        failure_code="worker.permanent",
        failure_detail=UNICODE_DETAIL,
    )

    status, task = _get_task(app, task_id=task_id, token=PRODUCER_TOKEN)
    assert status == 200
    assert task["state"] == "dead_lettered"
    assert task["terminal_at"] is not None
    _assert_failure_diagnostics(
        failure_code=task.get("failure_code"),
        failure_detail=task.get("failure_detail"),
        expected_code="worker.permanent",
        expected_detail=UNICODE_DETAIL,
    )
    _assert_no_secret_leak(task, claim_token=claim_token)

    status, page = _list_attempts(app, task_id=task_id, token=OBSERVER_TOKEN)
    assert status == 200
    attempt = page["items"][0]
    assert attempt["outcome"] == "dead_lettered"
    _assert_failure_diagnostics(
        failure_code=attempt.get("failure_code"),
        failure_detail=attempt.get("failure_detail"),
        expected_code="worker.permanent",
        expected_detail=UNICODE_DETAIL,
    )


def test_immediate_cancel_and_cancel_requested_lease_inspection(
    app: Any,
    session_factory: sessionmaker[Session],
    queue_name: str,
) -> None:
    _seed_queue(session_factory, queue_name=queue_name, retry_delay_seconds=0)

    ready_id = _enqueue_ready(app, queue_name=queue_name)
    _cancel(app, task_id=ready_id)
    status, cancelled = _get_task(app, task_id=ready_id, token=OBSERVER_TOKEN)
    assert status == 200
    assert cancelled["state"] == "cancelled"
    assert cancelled["terminal_at"] is not None
    assert cancelled.get("current_claim") in (None,)
    _assert_no_secret_leak(cancelled)

    leased_id = _enqueue_ready(app, queue_name=queue_name)
    claim_id, claim_token, _generation, _ = _claim_one(app, queue_name=queue_name)
    _cancel(app, task_id=leased_id)
    status, leased = _get_task(app, task_id=leased_id, token=OBSERVER_TOKEN)
    assert status == 200
    assert leased["state"] == "leased"
    claim = leased["current_claim"]
    assert isinstance(claim, dict)
    assert claim["claim_id"] == claim_id
    assert claim["cancel_requested"] is True
    assert claim["worker_id"] == EXPECTED_WORKER_ID
    _assert_no_secret_leak(leased, claim_token=claim_token)


def test_expiry_cancellation_inspection_and_attempt_history(
    app: Any,
    session_factory: sessionmaker[Session],
    queue_name: str,
) -> None:
    _seed_queue(session_factory, queue_name=queue_name, retry_delay_seconds=0)
    task_id = _enqueue_ready(app, queue_name=queue_name)
    claim_id, claim_token, _generation, _ = _claim_one(app, queue_name=queue_name, lease_seconds=30)
    _cancel(app, task_id=task_id)

    session = session_factory()
    try:
        session.execute(
            update(TaskActive)
            .where(TaskActive.task_id == UUID(task_id))
            .values(lease_expires_at=text("transaction_timestamp() - interval '1 second'"))
        )
        session.commit()
    finally:
        session.close()

    LeaseExpiryService(session_factory=session_factory).finalize_expired(
        task_id=UUID(task_id)
    )

    status, task = _get_task(app, task_id=task_id, token=OBSERVER_TOKEN)
    assert status == 200
    assert task["state"] == "cancelled"
    assert task["terminal_at"] is not None
    _assert_no_secret_leak(task, claim_token=claim_token)

    status, page = _list_attempts(app, task_id=task_id, token=OBSERVER_TOKEN)
    assert status == 200
    assert len(page["items"]) == 1
    attempt = page["items"][0]
    assert attempt["outcome"] == "expired"
    assert attempt["claim_id"] == claim_id
    assert attempt["ended_at"] is not None
    # Cancel-via-expiry attempt keeps expiry outcome; no worker failure code required.
    assert attempt.get("failure_code") in (None,)
    _assert_no_secret_leak(page, claim_token=claim_token)


def test_authorization_and_retention_bounds(
    app: Any,
    session_factory: sessionmaker[Session],
    queue_name: str,
    authorizer: Authorizer,
) -> None:
    _seed_queue(session_factory, queue_name=queue_name, max_attempts=1, retry_delay_seconds=0)
    task_id = _enqueue_ready(app, queue_name=queue_name)
    claim_id, claim_token, generation, _ = _claim_one(app, queue_name=queue_name)
    _fail(
        app,
        claim_id=claim_id,
        claim_token=claim_token,
        generation=generation,
        retryable=False,
        failure_code="worker.permanent",
        failure_detail="gone",
    )

    # Wrong producer: non-disclosing not-found.
    status, err = _get_task(app, task_id=task_id, token=PRODUCER_OTHER_TOKEN)
    assert status == 404
    assert err["code"] == "task_not_found"

    # Worker role cannot inspect.
    status, err = _get_task(app, task_id=task_id, token=WORKER_TOKEN)
    assert status == 403
    assert err["code"] == "permission_denied"

    # Retention: force-expired policy maps retained terminals to task_not_found.
    @dataclass(frozen=True, slots=True)
    class ForceExpiredRetention(PayloadRetentionPolicy):
        def is_expired(
            self,
            queue_store_now: datetime,
            expires_at: datetime,
        ) -> bool:
            return True

    short_retention_app = create_application_app(
        authenticator=BearerCredentialAuthenticator.from_bindings(_bindings()),
        authorizer=authorizer,
        bind=ListenerBind(host="127.0.0.1", port=18107),
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
        worker_terminal_service=WorkerTerminalService(session_factory=session_factory),
        retention_policy=ForceExpiredRetention(retention_days=30),
    )
    status, err = _get_task(short_retention_app, task_id=task_id, token=OBSERVER_TOKEN)
    assert status == 404
    assert err["code"] == "task_not_found"
    status, err = _list_attempts(
        short_retention_app, task_id=task_id, token=OBSERVER_TOKEN
    )
    assert status == 404
    assert err["code"] == "task_not_found"


def test_append_only_attempts_and_exact_diagnostic_contract(
    app: Any,
    session_factory: sessionmaker[Session],
    queue_name: str,
) -> None:
    _seed_queue(
        session_factory,
        queue_name=queue_name,
        enabled=True,
        max_attempts=5,
        retry_delay_seconds=0,
    )
    task_id = _enqueue_ready(app, queue_name=queue_name)

    claim_id_1, token_1, gen_1, _ = _claim_one(app, queue_name=queue_name)
    _fail(
        app,
        claim_id=claim_id_1,
        claim_token=token_1,
        generation=gen_1,
        retryable=True,
        failure_code="worker.timeout",
        failure_detail=UNICODE_DETAIL,
    )
    claim_id_2, token_2, gen_2, _ = _claim_one(app, queue_name=queue_name)
    _fail(
        app,
        claim_id=claim_id_2,
        claim_token=token_2,
        generation=gen_2,
        retryable=True,
        failure_code="worker.timeout",
        failure_detail=UNICODE_DETAIL,
    )

    status, page = _list_attempts(app, task_id=task_id, token=OBSERVER_TOKEN, limit=10)
    assert status == 200
    items = page["items"]
    assert len(items) == 2
    # Newest-first (claimed_at desc).
    assert items[0]["generation"] == gen_2
    assert items[1]["generation"] == gen_1
    for attempt in items:
        assert attempt["outcome"] == "retry_scheduled"
        assert attempt["ended_at"] is not None
        _assert_failure_diagnostics(
            failure_code=attempt["failure_code"],
            failure_detail=attempt["failure_detail"],
            expected_code="worker.timeout",
            expected_detail=UNICODE_DETAIL,
        )

    # Attempt rows remain append-only in storage (two ended rows).
    session = session_factory()
    try:
        rows = session.execute(
            select(TaskAttempt).where(TaskAttempt.task_id == UUID(task_id))
        ).scalars().all()
        assert len(rows) == 2
        assert all(int(r.outcome_code) != 1 for r in rows)
        # Registry claim tokens never projected.
        tokens = {
            str(r.claim_token)
            for r in session.execute(select(ClaimRegistry)).scalars().all()
        }
    finally:
        session.close()

    status, task = _get_task(app, task_id=task_id, token=OBSERVER_TOKEN)
    assert status == 200
    for tok in tokens:
        _assert_no_secret_leak(task, claim_token=tok)
        _assert_no_secret_leak(page, claim_token=tok)
