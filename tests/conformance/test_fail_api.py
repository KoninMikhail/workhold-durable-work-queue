"""Black-box worker HTTP failClaim conformance (Phase 03.6-02).

Covers WORK-05/06/11, COMP-03, API-03: atomic fenced fail over real PostgreSQL,
bounded diagnostics, enqueue-policy retry/dead-letter, and terminal replay.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import uuid
from collections.abc import Iterator, Mapping
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import pytest
from sqlalchemy import create_engine, event, func, select, text, update
from sqlalchemy.orm import Session, sessionmaker

from workhold.api.application import create_application_app
from workhold.api.security import ListenerBind
from workhold.application.claim_service import ClaimService
from workhold.application.lease_service import LeaseService
from workhold.domain.queue_control import (
    AdminRequestMetadata,
    BackoffStrategy,
    CreateQueueMutation,
    RetryPolicyDraft,
)
from workhold.infrastructure.postgres.lease_repository import (
    FenceDecision,
    LeaseRepository,
)
from workhold.infrastructure.postgres.queue_control_repository import (
    QueueControlRepository,
)
from workhold.intake.depth import DepthCeilings
from workhold.intake.service import EnqueueService
from workhold.security.authorization import Authorizer
from workhold.security.credentials import (
    BearerCredentialAuthenticator,
    CredentialBinding,
)
from workhold.security.principals import ServiceRole
from workhold.settings import Secret
from workhold.storage.models import (
    ClaimRegistry,
    CompleteReplay,
    Queue,
    QueueCounter,
    TaskActive,
    TaskAttempt,
    TaskTerminal,
)
from tests.conformance.harness import ConformanceHarness, ObservedResponse

pytest_plugins = ["tests.integration.conftest"]

REPO_ROOT = Path(__file__).resolve().parents[2]
OPENAPI_PATH = REPO_ROOT / "openapi" / "queue.openapi.json"

PRODUCER_TOKEN = "tok-producer-fail-http"
WORKER_TOKEN = "tok-worker-fail-http"
WORKER_OTHER_TOKEN = "tok-worker-fail-other"
ADMIN_TOKEN = "tok-admin-fail-http"

PRODUCER_PRINCIPAL = "producer-fail-http"
WORKER_PRINCIPAL = "worker-fail-http"
WORKER_OTHER_PRINCIPAL = "worker-fail-other"
ADMIN_PRINCIPAL = "admin-fail-http"

BASE_QUEUE_NAME = "orders.fail"
OTHER_QUEUE = "billing.fail"
PAYLOAD_SENTINEL = "FAIL_SECRET_PAYLOAD_SHOULD_NEVER_LEAK"
CLAIM_PATH = "/v1/claims"
REPLICA_ID = "pool-fail/replica-1"
CLAIM_TOKEN_HEADER = "X-Queue-Claim-Token"

_OP_COMPLETE = 1
_OP_FAIL = 2
_OUTCOME_RETRY = 3
_OUTCOME_DEAD = 4
_STATE_DELAYED = 1
_STATE_READY = 2
_STATE_LEASED = 3
_STATE_DEAD = 11
_RESULT_RETRY = 3
_RESULT_DEAD = 11


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
            principal_id=WORKER_PRINCIPAL,
            role=ServiceRole.WORKER,
            generation_id="g1",
            secret=Secret(WORKER_TOKEN),
        ),
        CredentialBinding(
            principal_id=WORKER_OTHER_PRINCIPAL,
            role=ServiceRole.WORKER,
            generation_id="g1",
            secret=Secret(WORKER_OTHER_TOKEN),
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
            WORKER_PRINCIPAL: frozenset({queue_name, BASE_QUEUE_NAME}),
            WORKER_OTHER_PRINCIPAL: frozenset({OTHER_QUEUE}),
            ADMIN_PRINCIPAL: frozenset({queue_name, BASE_QUEUE_NAME}),
        }
    )


@pytest.fixture
def session_factory(migrated_schema) -> Iterator[sessionmaker[Session]]:
    _conn, schema = migrated_schema
    database_url = os.environ.get("TEST_DATABASE_URL", "").strip()
    if not database_url:
        pytest.fail("TEST_DATABASE_URL is required for fail HTTP conformance")
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
        bind=ListenerBind(host="127.0.0.1", port=18096),
        session_factory=session_factory,
        enqueue_service=enqueue_service,
        claim_service=ClaimService(session_factory=session_factory),
        lease_service=LeaseService(session_factory=session_factory),
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
        "client": ("127.0.0.1", 50000),
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
            header_box.clear()
            for raw_k, raw_v in message.get("headers", []):
                header_box[raw_k.decode("latin-1").lower()] = raw_v.decode("latin-1")
        elif message["type"] == "http.response.body":
            body_chunks.append(message.get("body", b"") or b"")

    asyncio.run(app(scope, receive, send))
    return status_box["status"], header_box, b"".join(body_chunks)


def _admin_meta() -> AdminRequestMetadata:
    return AdminRequestMetadata(
        actor_id=ADMIN_PRINCIPAL,
        request_id=str(uuid.uuid4()),
        idempotency_key=f"admin-idem-{uuid.uuid4().hex}",
    )


def _seed_queue(
    session: Session,
    *,
    name: str,
    enabled: bool = True,
    max_attempts: int = 3,
    retry_delay_seconds: int = 0,
) -> Queue:
    QueueControlRepository().create_named_queue(
        session,
        CreateQueueMutation(
            name=name,
            initial_policy=RetryPolicyDraft(
                enabled=enabled,
                max_attempts=max_attempts,
                backoff_strategy=BackoffStrategy.FIXED,
                retry_delay_seconds=retry_delay_seconds,
            ),
            metadata=_admin_meta(),
        ),
    )
    session.commit()
    return session.execute(select(Queue).where(Queue.name == name)).scalar_one()


def _enqueue_ready(
    app: Any,
    *,
    queue_name: str,
    payload: Any,
    idempotency_key: str,
) -> str:
    path = f"/v1/queues/{queue_name}/tasks"
    body = json.dumps(
        {"payload": payload, "priority": 0},
        separators=(",", ":"),
    ).encode("utf-8")
    headers = {
        "Authorization": f"Bearer {PRODUCER_TOKEN}",
        "Content-Type": "application/json",
        "Idempotency-Key": idempotency_key,
    }
    status, _hdrs, resp = _asgi_http_call(app, method="POST", path=path, headers=headers, body=body)
    assert status == 201, resp.decode("utf-8", errors="replace")
    return json.loads(resp.decode("utf-8"))["task"]["task_id"]


def _claim_body(
    *,
    queues: list[str],
    lease_seconds: int = 60,
    worker_id: str = REPLICA_ID,
) -> dict[str, Any]:
    return {
        "queues": queues,
        "max_tasks": 1,
        "lease_seconds": lease_seconds,
        "wait_seconds": 0,
        "worker_id": worker_id,
    }


def _worker_headers(
    *,
    token: str = WORKER_TOKEN,
    claim_token: str | None = None,
) -> dict[str, str]:
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }
    if claim_token is not None:
        headers[CLAIM_TOKEN_HEADER] = claim_token
    return headers


def _fail_path(claim_id: str) -> str:
    return f"/v1/claims/{claim_id}:fail"


def _fail_body(
    *,
    generation: int,
    retryable: bool = True,
    failure_code: str = "worker.timeout",
    failure_detail: str | None = "bounded detail",
    include_detail: bool = True,
) -> dict[str, Any]:
    body: dict[str, Any] = {
        "generation": generation,
        "retryable": retryable,
        "failure_code": failure_code,
    }
    if include_detail:
        body["failure_detail"] = failure_detail
    return body


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
    return payload


def _validate_fail_response(
    harness: ConformanceHarness,
    *,
    status: int,
    headers: Mapping[str, str],
    body: bytes,
) -> dict[str, Any]:
    payload = json.loads(body.decode("utf-8"))
    findings = harness._validate_response(  # noqa: SLF001 - schema gate
        "failClaim",
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


def _claim_one(
    app: Any,
    *,
    queue_name: str,
    lease_seconds: int = 120,
) -> dict[str, Any]:
    body = json.dumps(
        _claim_body(queues=[queue_name], lease_seconds=lease_seconds),
        separators=(",", ":"),
    ).encode("utf-8")
    status, _hdrs, resp = _asgi_http_call(
        app,
        method="POST",
        path=CLAIM_PATH,
        headers=_worker_headers(),
        body=body,
    )
    assert status == 200, resp.decode("utf-8", errors="replace")
    payload = json.loads(resp.decode("utf-8"))
    assert len(payload["tasks"]) == 1
    return payload["tasks"][0]


def _snapshot_lease_rows(
    session: Session,
    *,
    task_id: uuid.UUID,
) -> dict[str, Any]:
    task = session.execute(
        select(TaskActive).where(TaskActive.task_id == task_id)
    ).scalar_one_or_none()
    registry = session.execute(
        select(ClaimRegistry).where(ClaimRegistry.task_id == task_id)
    ).scalar_one_or_none()
    attempts = list(
        session.scalars(
            select(TaskAttempt)
            .where(TaskAttempt.task_id == task_id)
            .order_by(TaskAttempt.generation)
        )
    )
    return {
        "task": task,
        "registry": registry,
        "attempt_count": len(attempts),
        "attempt_outcomes": [int(a.outcome_code) for a in attempts],
        "attempt_failure_codes": [a.failure_code for a in attempts],
        "attempt_failure_details": [a.failure_detail for a in attempts],
    }


def test_fail_request_fixture_is_closed() -> None:
    harness = ConformanceHarness(
        openapi_path=OPENAPI_PATH,
        base_url="http://127.0.0.1:9",
    )
    harness.validate_request_fixture(
        "failClaim",
        _fail_body(generation=1),
    )
    with pytest.raises(Exception):
        harness.validate_request_fixture(
            "failClaim",
            {"generation": 1, "retryable": True},
        )


def test_retryable_fail_schedules_retry_and_closes_attempt(
    app: Any,
    session_factory: sessionmaker[Session],
    queue_name: str,
    caplog: pytest.LogCaptureFixture,
) -> None:
    harness = ConformanceHarness(openapi_path=OPENAPI_PATH, base_url="http://127.0.0.1:9")
    session = session_factory()
    try:
        _seed_queue(session, name=queue_name, retry_delay_seconds=5)
    finally:
        session.close()

    _enqueue_ready(
        app,
        queue_name=queue_name,
        payload={"secret": PAYLOAD_SENTINEL, "n": 1},
        idempotency_key="idem-fail-retry-1",
    )
    claimed = _claim_one(app, queue_name=queue_name, lease_seconds=120)
    claim = claimed["claim"]
    task_id = uuid.UUID(claimed["task"]["task_id"])
    claim_id = claim["claim_id"]
    token = claim["claim_token"]
    generation = int(claim["generation"])
    detail = "  keep whitespace  "

    path = _fail_path(claim_id)
    body = json.dumps(
        _fail_body(
            generation=generation,
            retryable=True,
            failure_code="worker.timeout",
            failure_detail=detail,
        ),
        separators=(",", ":"),
        ensure_ascii=False,
    ).encode("utf-8")

    with caplog.at_level(logging.INFO):
        status, resp_headers, resp_body = _asgi_http_call(
            app,
            method="POST",
            path=path,
            headers=_worker_headers(claim_token=token),
            body=body,
        )

    assert status == 200, resp_body.decode("utf-8", errors="replace")
    payload = _validate_fail_response(
        harness, status=status, headers=resp_headers, body=resp_body
    )
    assert payload["task_id"] == str(task_id)
    assert payload["state"] == "retry_scheduled"
    assert payload["replayed"] is False
    available_at = datetime.fromisoformat(payload["available_at"].replace("Z", "+00:00"))

    assert PAYLOAD_SENTINEL not in caplog.text
    assert WORKER_TOKEN not in caplog.text
    assert token not in caplog.text
    assert token not in path
    assert CLAIM_TOKEN_HEADER.lower() not in resp_body.decode("utf-8").lower()

    after = session_factory()
    try:
        snap = _snapshot_lease_rows(after, task_id=task_id)
        assert snap["registry"] is None
        task = snap["task"]
        assert task is not None
        assert int(task.state_code) == _STATE_DELAYED
        assert task.current_claim_id is None
        assert task.worker_id is None
        assert abs((task.available_at - available_at).total_seconds()) < 1.0
        assert snap["attempt_count"] == 1
        assert snap["attempt_outcomes"] == [_OUTCOME_RETRY]
        assert snap["attempt_failure_codes"] == ["worker.timeout"]
        assert snap["attempt_failure_details"] == [detail]
        replay = after.execute(
            select(CompleteReplay).where(
                CompleteReplay.claim_id == uuid.UUID(claim_id),
                CompleteReplay.operation_code == _OP_FAIL,
            )
        ).scalar_one()
        assert int(replay.result_state_code) == _RESULT_RETRY
        assert replay.available_at is not None
        assert replay.terminal_at is None
        counter = after.get(QueueCounter, int(task.queue_id))
        assert counter is not None
        assert int(counter.leased_count) == 0
        assert int(counter.delayed_count) == 1
    finally:
        after.close()


def test_nonretryable_fail_dead_letters_without_policy(
    app: Any,
    session_factory: sessionmaker[Session],
    queue_name: str,
) -> None:
    harness = ConformanceHarness(openapi_path=OPENAPI_PATH, base_url="http://127.0.0.1:9")
    session = session_factory()
    try:
        # Policy would schedule retries; retryable=false must ignore it.
        _seed_queue(
            session,
            name=queue_name,
            enabled=True,
            max_attempts=5,
            retry_delay_seconds=30,
        )
    finally:
        session.close()

    _enqueue_ready(
        app,
        queue_name=queue_name,
        payload={"secret": PAYLOAD_SENTINEL},
        idempotency_key="idem-fail-final-1",
    )
    claimed = _claim_one(app, queue_name=queue_name)
    claim = claimed["claim"]
    task_id = uuid.UUID(claimed["task"]["task_id"])

    status, headers, resp = _asgi_http_call(
        app,
        method="POST",
        path=_fail_path(claim["claim_id"]),
        headers=_worker_headers(claim_token=claim["claim_token"]),
        body=json.dumps(
            _fail_body(
                generation=int(claim["generation"]),
                retryable=False,
                failure_code="worker.fatal",
                failure_detail="final",
            ),
            separators=(",", ":"),
        ).encode("utf-8"),
    )
    assert status == 200, resp.decode("utf-8", errors="replace")
    payload = _validate_fail_response(harness, status=status, headers=headers, body=resp)
    assert payload["state"] == "dead_lettered"
    assert payload["replayed"] is False
    assert payload["task_id"] == str(task_id)
    terminal_at = datetime.fromisoformat(payload["terminal_at"].replace("Z", "+00:00"))

    after = session_factory()
    try:
        assert (
            after.execute(select(TaskActive).where(TaskActive.task_id == task_id)).scalar_one_or_none()
            is None
        )
        terminal = after.execute(
            select(TaskTerminal).where(TaskTerminal.task_id == task_id)
        ).scalar_one()
        assert int(terminal.state_code) == _STATE_DEAD
        assert terminal.failure_code == "worker.fatal"
        assert terminal.failure_detail == "final"
        assert abs((terminal.terminal_at - terminal_at).total_seconds()) < 1.0
        attempts = list(
            after.scalars(select(TaskAttempt).where(TaskAttempt.task_id == task_id))
        )
        assert len(attempts) == 1
        assert int(attempts[0].outcome_code) == _OUTCOME_DEAD
        replay = after.execute(
            select(CompleteReplay).where(
                CompleteReplay.claim_id == uuid.UUID(claim["claim_id"]),
                CompleteReplay.operation_code == _OP_FAIL,
            )
        ).scalar_one()
        assert int(replay.result_state_code) == _RESULT_DEAD
        assert replay.terminal_at is not None
        assert replay.available_at is None
    finally:
        after.close()


def test_retryable_exhaustion_dead_letters_from_policy_snapshot(
    app: Any,
    session_factory: sessionmaker[Session],
    queue_name: str,
) -> None:
    harness = ConformanceHarness(openapi_path=OPENAPI_PATH, base_url="http://127.0.0.1:9")
    session = session_factory()
    try:
        _seed_queue(
            session,
            name=queue_name,
            enabled=True,
            max_attempts=1,
            retry_delay_seconds=0,
        )
    finally:
        session.close()

    _enqueue_ready(
        app,
        queue_name=queue_name,
        payload={"n": 1},
        idempotency_key="idem-fail-exhaust",
    )
    claimed = _claim_one(app, queue_name=queue_name)
    claim = claimed["claim"]

    status, headers, resp = _asgi_http_call(
        app,
        method="POST",
        path=_fail_path(claim["claim_id"]),
        headers=_worker_headers(claim_token=claim["claim_token"]),
        body=json.dumps(
            _fail_body(generation=int(claim["generation"]), retryable=True),
            separators=(",", ":"),
        ).encode("utf-8"),
    )
    assert status == 200
    payload = _validate_fail_response(harness, status=status, headers=headers, body=resp)
    assert payload["state"] == "dead_lettered"
    assert payload["replayed"] is False


def test_same_body_replay_and_changed_body_conflict(
    app: Any,
    session_factory: sessionmaker[Session],
    queue_name: str,
) -> None:
    harness = ConformanceHarness(openapi_path=OPENAPI_PATH, base_url="http://127.0.0.1:9")
    session = session_factory()
    try:
        _seed_queue(session, name=queue_name, retry_delay_seconds=0)
    finally:
        session.close()

    _enqueue_ready(
        app,
        queue_name=queue_name,
        payload={"n": 2},
        idempotency_key="idem-fail-replay",
    )
    claimed = _claim_one(app, queue_name=queue_name)
    claim = claimed["claim"]
    original_body = _fail_body(
        generation=int(claim["generation"]),
        retryable=False,
        failure_code="worker.fatal",
        failure_detail="same",
    )
    encoded = json.dumps(original_body, separators=(",", ":")).encode("utf-8")

    status1, headers1, resp1 = _asgi_http_call(
        app,
        method="POST",
        path=_fail_path(claim["claim_id"]),
        headers=_worker_headers(claim_token=claim["claim_token"]),
        body=encoded,
    )
    assert status1 == 200
    first = _validate_fail_response(harness, status=status1, headers=headers1, body=resp1)
    assert first["replayed"] is False

    status2, headers2, resp2 = _asgi_http_call(
        app,
        method="POST",
        path=_fail_path(claim["claim_id"]),
        headers=_worker_headers(claim_token=claim["claim_token"]),
        body=encoded,
    )
    assert status2 == 200
    second = _validate_fail_response(harness, status=status2, headers=headers2, body=resp2)
    assert second["replayed"] is True
    assert {k: v for k, v in second.items() if k != "replayed"} == {
        k: v for k, v in first.items() if k != "replayed"
    }

    after = session_factory()
    try:
        replays = list(
            after.scalars(
                select(CompleteReplay).where(
                    CompleteReplay.claim_id == uuid.UUID(claim["claim_id"]),
                    CompleteReplay.operation_code == _OP_FAIL,
                )
            )
        )
        assert len(replays) == 1
        terminals = list(
            after.scalars(
                select(TaskTerminal).where(
                    TaskTerminal.task_id == uuid.UUID(claimed["task"]["task_id"])
                )
            )
        )
        assert len(terminals) == 1
    finally:
        after.close()

    changed = dict(original_body)
    changed["failure_detail"] = "different"
    status3, _h3, resp3 = _asgi_http_call(
        app,
        method="POST",
        path=_fail_path(claim["claim_id"]),
        headers=_worker_headers(claim_token=claim["claim_token"]),
        body=json.dumps(changed, separators=(",", ":")).encode("utf-8"),
    )
    assert status3 == 409
    _assert_error(resp3, code="idempotency_conflict", retryable=False)


@pytest.mark.parametrize(
    "mutate",
    ["wrong_token", "wrong_generation", "expired", "superseded"],
)
def test_stale_credentials_return_lease_lost_without_mutation(
    app: Any,
    session_factory: sessionmaker[Session],
    queue_name: str,
    mutate: str,
) -> None:
    session = session_factory()
    try:
        _seed_queue(session, name=queue_name)
    finally:
        session.close()

    _enqueue_ready(
        app,
        queue_name=queue_name,
        payload={"secret": PAYLOAD_SENTINEL},
        idempotency_key=f"idem-fail-stale-{mutate}",
    )
    claimed = _claim_one(app, queue_name=queue_name, lease_seconds=90)
    claim = claimed["claim"]
    task_id = uuid.UUID(claimed["task"]["task_id"])
    claim_id = claim["claim_id"]
    token = claim["claim_token"]
    generation = int(claim["generation"])

    if mutate == "wrong_token":
        token = str(uuid.uuid4())
    elif mutate == "wrong_generation":
        generation = generation + 1
    elif mutate == "expired":
        expire = session_factory()
        try:
            expire.execute(
                update(TaskActive)
                .where(TaskActive.task_id == task_id)
                .values(
                    lease_expires_at=func.transaction_timestamp()
                    - text("interval '1 second'")
                )
            )
            expire.execute(
                update(ClaimRegistry)
                .where(ClaimRegistry.claim_id == uuid.UUID(claim_id))
                .values(
                    claimed_at=func.transaction_timestamp() - text("interval '2 hours'"),
                    lease_expires_at=func.transaction_timestamp()
                    - text("interval '1 second'"),
                )
            )
            expire.commit()
        finally:
            expire.close()
    elif mutate == "superseded":
        expire = session_factory()
        try:
            expire.execute(
                update(TaskActive)
                .where(TaskActive.task_id == task_id)
                .values(
                    lease_expires_at=func.transaction_timestamp()
                    - text("interval '1 second'")
                )
            )
            expire.commit()
        finally:
            expire.close()
        reclaim = ClaimService(session_factory=session_factory).claim(
            queue_name=queue_name,
            worker_id=f"{WORKER_PRINCIPAL}/reclaimer",
            lease_seconds=90,
        )
        assert reclaim.empty is False
        assert reclaim.generation == 2

    before = session_factory()
    try:
        snap_before = _snapshot_lease_rows(before, task_id=task_id)
        attempt_outcomes_before = list(snap_before["attempt_outcomes"])
    finally:
        before.close()

    status, _hdrs, resp = _asgi_http_call(
        app,
        method="POST",
        path=_fail_path(claim_id),
        headers=_worker_headers(claim_token=token),
        body=json.dumps(
            _fail_body(generation=generation, retryable=False),
            separators=(",", ":"),
        ).encode("utf-8"),
    )
    assert status == 409
    _assert_error(resp, code="lease_lost", retryable=False)

    after = session_factory()
    try:
        snap_after = _snapshot_lease_rows(after, task_id=task_id)
        assert snap_after["attempt_outcomes"] == attempt_outcomes_before
        assert (
            after.execute(
                select(CompleteReplay).where(
                    CompleteReplay.claim_id == uuid.UUID(claim_id),
                    CompleteReplay.operation_code == _OP_FAIL,
                )
            ).scalar_one_or_none()
            is None
        )
    finally:
        after.close()


def test_shared_fence_probe_matches_fail_stale_decision(
    app: Any,
    session_factory: sessionmaker[Session],
    queue_name: str,
) -> None:
    session = session_factory()
    try:
        _seed_queue(session, name=queue_name)
    finally:
        session.close()
    _enqueue_ready(
        app,
        queue_name=queue_name,
        payload={"n": 1},
        idempotency_key="idem-fail-probe",
    )
    claimed = _claim_one(app, queue_name=queue_name, lease_seconds=60)
    claim = claimed["claim"]
    task_id = uuid.UUID(claimed["task"]["task_id"])
    claim_id = uuid.UUID(claim["claim_id"])
    token = uuid.UUID(claim["claim_token"])
    generation = int(claim["generation"])

    repo = LeaseRepository()
    probe_session = session_factory()
    try:
        current = repo.validate_current_lease(
            probe_session,
            claim_id=claim_id,
            claim_token=token,
            generation=generation,
        )
        assert current.decision is FenceDecision.CURRENT
    finally:
        probe_session.rollback()
        probe_session.close()

    expire = session_factory()
    try:
        expire.execute(
            update(TaskActive)
            .where(TaskActive.task_id == task_id)
            .values(
                lease_expires_at=func.transaction_timestamp()
                - text("interval '1 second'")
            )
        )
        expire.commit()
    finally:
        expire.close()

    reclaim = ClaimService(session_factory=session_factory).claim(
        queue_name=queue_name,
        worker_id=f"{WORKER_PRINCIPAL}/probe-reclaim",
        lease_seconds=60,
    )
    assert reclaim.empty is False
    assert reclaim.generation == 2

    stale_session = session_factory()
    try:
        stale = repo.validate_current_lease(
            stale_session,
            claim_id=claim_id,
            claim_token=token,
            generation=generation,
        )
        assert stale.decision is FenceDecision.STALE
    finally:
        stale_session.rollback()
        stale_session.close()

    status, _hdrs, resp = _asgi_http_call(
        app,
        method="POST",
        path=_fail_path(str(claim_id)),
        headers=_worker_headers(claim_token=str(token)),
        body=json.dumps(
            _fail_body(generation=generation, retryable=False),
            separators=(",", ":"),
        ).encode("utf-8"),
    )
    assert status == 409
    _assert_error(resp, code="lease_lost", retryable=False)


def test_cancel_requested_returns_cancel_race_lost(
    app: Any,
    session_factory: sessionmaker[Session],
    queue_name: str,
) -> None:
    session = session_factory()
    try:
        _seed_queue(session, name=queue_name)
    finally:
        session.close()
    _enqueue_ready(
        app,
        queue_name=queue_name,
        payload={"n": 1},
        idempotency_key="idem-fail-cancel-race",
    )
    claimed = _claim_one(app, queue_name=queue_name)
    claim = claimed["claim"]
    task_id = uuid.UUID(claimed["task"]["task_id"])

    mark = session_factory()
    try:
        mark.execute(
            update(TaskActive)
            .where(TaskActive.task_id == task_id)
            .values(cancel_requested_at=func.transaction_timestamp())
        )
        mark.commit()
    finally:
        mark.close()

    status, _hdrs, resp = _asgi_http_call(
        app,
        method="POST",
        path=_fail_path(claim["claim_id"]),
        headers=_worker_headers(claim_token=claim["claim_token"]),
        body=json.dumps(
            _fail_body(generation=int(claim["generation"]), retryable=False),
            separators=(",", ":"),
        ).encode("utf-8"),
    )
    assert status == 409
    _assert_error(resp, code="cancel_race_lost", retryable=False)

    after = session_factory()
    try:
        assert after.execute(
            select(CompleteReplay).where(
                CompleteReplay.claim_id == uuid.UUID(claim["claim_id"])
            )
        ).scalar_one_or_none() is None
        task = after.execute(
            select(TaskActive).where(TaskActive.task_id == task_id)
        ).scalar_one()
        assert task.cancel_requested_at is not None
        assert int(task.state_code) == 3
    finally:
        after.close()


def test_other_terminal_replay_returns_task_already_terminal(
    app: Any,
    session_factory: sessionmaker[Session],
    queue_name: str,
) -> None:
    session = session_factory()
    try:
        _seed_queue(session, name=queue_name)
    finally:
        session.close()
    _enqueue_ready(
        app,
        queue_name=queue_name,
        payload={"n": 1},
        idempotency_key="idem-fail-other-terminal",
    )
    claimed = _claim_one(app, queue_name=queue_name)
    claim = claimed["claim"]
    claim_id = uuid.UUID(claim["claim_id"])
    task_id = uuid.UUID(claimed["task"]["task_id"])

    seed = session_factory()
    try:
        now = seed.scalar(select(func.transaction_timestamp()))
        assert now is not None
        seed.add(
            CompleteReplay(
                claim_id=claim_id,
                operation_code=_OP_COMPLETE,
                request_fingerprint=b"\x01" * 32,
                task_id=task_id,
                result_state_code=10,
                available_at=None,
                terminal_at=now,
                spawned_task_ids=[],
                event_ids=[],
                created_at=now,
                expires_at=now + timedelta(days=7),
            )
        )
        seed.commit()
    finally:
        seed.close()

    status, _hdrs, resp = _asgi_http_call(
        app,
        method="POST",
        path=_fail_path(str(claim_id)),
        headers=_worker_headers(claim_token=claim["claim_token"]),
        body=json.dumps(
            _fail_body(generation=int(claim["generation"]), retryable=False),
            separators=(",", ":"),
        ).encode("utf-8"),
    )
    assert status == 409
    _assert_error(resp, code="task_already_terminal", retryable=False)


def test_unknown_claim_is_not_found(
    app: Any,
    session_factory: sessionmaker[Session],
    queue_name: str,
) -> None:
    session = session_factory()
    try:
        _seed_queue(session, name=queue_name)
    finally:
        session.close()

    status, _hdrs, resp = _asgi_http_call(
        app,
        method="POST",
        path=_fail_path(str(uuid.uuid4())),
        headers=_worker_headers(claim_token=str(uuid.uuid4())),
        body=json.dumps(
            _fail_body(generation=1, retryable=False),
            separators=(",", ":"),
        ).encode("utf-8"),
    )
    assert status == 404
    _assert_error(resp, code="claim_not_found", retryable=False)


def test_failure_code_and_detail_bounds_rejected(
    app: Any,
    session_factory: sessionmaker[Session],
    queue_name: str,
) -> None:
    session = session_factory()
    try:
        _seed_queue(session, name=queue_name)
    finally:
        session.close()
    _enqueue_ready(
        app,
        queue_name=queue_name,
        payload={"n": 1},
        idempotency_key="idem-fail-bounds",
    )
    claimed = _claim_one(app, queue_name=queue_name)
    claim = claimed["claim"]
    path = _fail_path(claim["claim_id"])
    headers = _worker_headers(claim_token=claim["claim_token"])
    generation = int(claim["generation"])

    cases = [
        _fail_body(generation=generation, failure_code="Worker.Timeout"),
        _fail_body(generation=generation, failure_code=""),
        _fail_body(generation=generation, failure_code="1bad"),
        _fail_body(generation=generation, failure_code="bad code"),
        _fail_body(
            generation=generation,
            failure_detail="x" * 4097,
        ),
    ]
    for body in cases:
        status, _h, resp = _asgi_http_call(
            app,
            method="POST",
            path=path,
            headers=headers,
            body=json.dumps(body, separators=(",", ":")).encode("utf-8"),
        )
        assert status == 400, body
        _assert_error(resp, code="validation_failed", retryable=False)

    after = session_factory()
    try:
        snap = _snapshot_lease_rows(
            after, task_id=uuid.UUID(claimed["task"]["task_id"])
        )
        assert snap["task"] is not None
        assert int(snap["task"].state_code) == 3
        assert snap["attempt_outcomes"] == [1]
    finally:
        after.close()


def test_missing_claim_token_and_out_of_scope_worker(
    app: Any,
    session_factory: sessionmaker[Session],
    queue_name: str,
) -> None:
    session = session_factory()
    try:
        _seed_queue(session, name=queue_name)
    finally:
        session.close()
    _enqueue_ready(
        app,
        queue_name=queue_name,
        payload={"n": 1},
        idempotency_key="idem-fail-auth",
    )
    claimed = _claim_one(app, queue_name=queue_name)
    claim = claimed["claim"]
    path = _fail_path(claim["claim_id"])
    body = json.dumps(
        _fail_body(generation=int(claim["generation"]), retryable=False),
        separators=(",", ":"),
    ).encode("utf-8")

    status_missing, _h1, resp_missing = _asgi_http_call(
        app,
        method="POST",
        path=path,
        headers=_worker_headers(),
        body=body,
    )
    assert status_missing == 400
    _assert_error(resp_missing, code="validation_failed", retryable=False)

    status_scope, _h2, resp_scope = _asgi_http_call(
        app,
        method="POST",
        path=path,
        headers=_worker_headers(
            token=WORKER_OTHER_TOKEN,
            claim_token=claim["claim_token"],
        ),
        body=body,
    )
    assert status_scope == 403
    _assert_error(resp_scope, code="permission_denied", retryable=False)


def test_retryable_fail_positive_delay_empty_before_due_claimable_after(
    app: Any,
    session_factory: sessionmaker[Session],
    queue_name: str,
) -> None:
    """Retry-scheduled delayed row: no claim before due; one lease after SQL advance."""
    session = session_factory()
    try:
        _seed_queue(session, name=queue_name, retry_delay_seconds=120)
    finally:
        session.close()

    _enqueue_ready(
        app,
        queue_name=queue_name,
        payload={"n": 1},
        idempotency_key="idem-fail-delay-scaffold",
    )
    claimed = _claim_one(app, queue_name=queue_name, lease_seconds=120)
    claim = claimed["claim"]
    task_id = uuid.UUID(claimed["task"]["task_id"])
    claim_id = claim["claim_id"]
    token = claim["claim_token"]
    generation = int(claim["generation"])

    path = _fail_path(claim_id)
    body = json.dumps(
        _fail_body(generation=generation, retryable=True, failure_code="worker.timeout"),
        separators=(",", ":"),
    ).encode("utf-8")
    status, _headers, resp_body = _asgi_http_call(
        app,
        method="POST",
        path=path,
        headers=_worker_headers(claim_token=token),
        body=body,
    )
    assert status == 200, resp_body.decode("utf-8", errors="replace")
    payload = json.loads(resp_body.decode("utf-8"))
    assert payload["state"] == "retry_scheduled"

    empty_body = json.dumps(
        _claim_body(queues=[queue_name], lease_seconds=120),
        separators=(",", ":"),
    ).encode("utf-8")
    status_empty, _empty_headers, empty_resp = _asgi_http_call(
        app,
        method="POST",
        path=CLAIM_PATH,
        headers=_worker_headers(),
        body=empty_body,
    )
    assert status_empty == 200, empty_resp.decode("utf-8", errors="replace")
    before_due = json.loads(empty_resp.decode("utf-8"))
    assert before_due["tasks"] == []

    advance = session_factory()
    try:
        advance.execute(
            update(TaskActive)
            .where(TaskActive.task_id == task_id)
            .values(
                available_at=func.transaction_timestamp()
                - text("interval '1 second'")
            )
        )
        advance.commit()
    finally:
        advance.close()

    leased = _claim_one(app, queue_name=queue_name, lease_seconds=120)
    assert leased["task"]["task_id"] == str(task_id)
    assert int(leased["claim"]["generation"]) == 2

    verify = session_factory()
    try:
        snap = _snapshot_lease_rows(verify, task_id=task_id)
        task = snap["task"]
        assert task is not None
        assert int(task.state_code) == _STATE_LEASED
        counter = verify.get(QueueCounter, int(task.queue_id))
        assert counter is not None
        assert int(counter.leased_count) == 1
        assert int(counter.delayed_count) == 0
    finally:
        verify.close()
