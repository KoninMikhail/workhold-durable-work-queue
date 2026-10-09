"""Black-box worker HTTP heartbeatClaim conformance (Phase 03.5-03).

Covers WORK-03 / WORK-04 / OPS-04 / API-08: fenced heartbeat over real PostgreSQL,
protected claim-token header, lease reset from Queue-store now, and reusable
current-lease fence probes for later terminal commands.
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

from queue_service.api.application import create_application_app
from queue_service.api.security import ListenerBind
from queue_service.application.claim_service import ClaimService
from queue_service.application.lease_service import LeaseService
from queue_service.domain.queue_control import (
    AdminRequestMetadata,
    BackoffStrategy,
    CreateQueueMutation,
    RetryPolicyDraft,
)
from queue_service.infrastructure.postgres.lease_repository import (
    FenceDecision,
    LeaseRepository,
)
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
from queue_service.security.principals import ServiceRole
from queue_service.settings import Secret
from queue_service.storage.models import ClaimRegistry, Queue, TaskActive, TaskAttempt
from tests.conformance.harness import ConformanceHarness, ObservedResponse

pytest_plugins = ["tests.integration.conftest"]

REPO_ROOT = Path(__file__).resolve().parents[2]
OPENAPI_PATH = REPO_ROOT / "openapi" / "queue.openapi.json"

PRODUCER_TOKEN = "tok-producer-heartbeat-http"
WORKER_TOKEN = "tok-worker-heartbeat-http"
WORKER_OTHER_TOKEN = "tok-worker-heartbeat-other"
ADMIN_TOKEN = "tok-admin-heartbeat-http"

PRODUCER_PRINCIPAL = "producer-heartbeat-http"
WORKER_PRINCIPAL = "worker-heartbeat-http"
WORKER_OTHER_PRINCIPAL = "worker-heartbeat-other"
ADMIN_PRINCIPAL = "admin-heartbeat-http"

BASE_QUEUE_NAME = "orders.heartbeat"
OTHER_QUEUE = "billing.heartbeat"
PAYLOAD_SENTINEL = "HB_SECRET_PAYLOAD_SHOULD_NEVER_LEAK"
CLAIM_PATH = "/v1/claims"
REPLICA_ID = "pool-hb/replica-1"
CLAIM_TOKEN_HEADER = "X-Queue-Claim-Token"


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
        pytest.fail("TEST_DATABASE_URL is required for heartbeat HTTP conformance")
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
        bind=ListenerBind(host="127.0.0.1", port=18093),
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
                # Zero delay so expire→retry is immediately claimable (matches
                # concurrency reclaim suites). Non-zero delay would leave DELAYED
                # work and reclaim would correctly return empty until available_at.
                retry_delay_seconds=0,
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


def _heartbeat_path(claim_id: str) -> str:
    return f"/v1/claims/{claim_id}:heartbeat"


def _heartbeat_body(*, generation: int, lease_seconds: int) -> dict[str, Any]:
    return {"generation": generation, "lease_seconds": lease_seconds}


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


def _validate_heartbeat_response(
    harness: ConformanceHarness,
    *,
    status: int,
    headers: Mapping[str, str],
    body: bytes,
) -> dict[str, Any]:
    payload = json.loads(body.decode("utf-8"))
    findings = harness._validate_response(  # noqa: SLF001 - schema gate
        "heartbeatClaim",
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
    ).scalar_one()
    registry = session.execute(
        select(ClaimRegistry).where(ClaimRegistry.task_id == task_id)
    ).scalar_one()
    attempts = list(
        session.scalars(
            select(TaskAttempt)
            .where(TaskAttempt.task_id == task_id)
            .order_by(TaskAttempt.generation)
        )
    )
    return {
        "task_generation": int(task.generation),
        "task_claim_id": task.current_claim_id,
        "task_claimed_at": task.claimed_at,
        "task_lease_expires_at": task.lease_expires_at,
        "task_worker_id": task.worker_id,
        "task_state_code": int(task.state_code),
        "registry_claim_id": registry.claim_id,
        "registry_token": registry.claim_token,
        "registry_generation": int(registry.generation),
        "registry_claimed_at": registry.claimed_at,
        "registry_lease_expires_at": registry.lease_expires_at,
        "attempt_count": len(attempts),
        "attempt_claim_ids": [a.claim_id for a in attempts],
        "attempt_outcomes": [int(a.outcome_code) for a in attempts],
    }


def test_heartbeat_request_fixture_is_closed() -> None:
    harness = ConformanceHarness(
        openapi_path=OPENAPI_PATH,
        base_url="http://127.0.0.1:9",
    )
    harness.validate_request_fixture(
        "heartbeatClaim",
        _heartbeat_body(generation=1, lease_seconds=30),
    )
    with pytest.raises(Exception):
        harness.validate_request_fixture(
            "heartbeatClaim",
            {"generation": 1, "lease_seconds": 3601},
        )
    with pytest.raises(Exception):
        harness.validate_request_fixture(
            "heartbeatClaim",
            {"generation": 1},
        )


def test_valid_heartbeat_resets_expiry_from_queue_store_now(
    app: Any,
    session_factory: sessionmaker[Session],
    queue_name: str,
    caplog: pytest.LogCaptureFixture,
) -> None:
    harness = ConformanceHarness(openapi_path=OPENAPI_PATH, base_url="http://127.0.0.1:9")
    session = session_factory()
    try:
        _seed_queue(session, name=queue_name)
    finally:
        session.close()

    _enqueue_ready(
        app,
        queue_name=queue_name,
        payload={"secret": PAYLOAD_SENTINEL, "n": 1},
        idempotency_key="idem-hb-ready-1",
    )
    claimed = _claim_one(app, queue_name=queue_name, lease_seconds=120)
    claim = claimed["claim"]
    task_id = uuid.UUID(claimed["task"]["task_id"])
    old_expiry = datetime.fromisoformat(claim["lease_expires_at"].replace("Z", "+00:00"))
    old_claimed_at = claim["claimed_at"]
    old_generation = claim["generation"]
    old_claim_id = claim["claim_id"]
    old_token = claim["claim_token"]
    old_worker = claim["worker_id"]

    before = session_factory()
    try:
        snap_before = _snapshot_lease_rows(before, task_id=task_id)
    finally:
        before.close()

    path = _heartbeat_path(old_claim_id)
    body = json.dumps(
        _heartbeat_body(generation=old_generation, lease_seconds=30),
        separators=(",", ":"),
    ).encode("utf-8")

    with caplog.at_level(logging.INFO):
        status, resp_headers, resp_body = _asgi_http_call(
            app,
            method="POST",
            path=path,
            headers=_worker_headers(claim_token=old_token),
            body=body,
        )

    assert status == 200, resp_body.decode("utf-8", errors="replace")
    payload = _validate_heartbeat_response(
        harness, status=status, headers=resp_headers, body=resp_body
    )
    summary = payload["claim"]
    assert summary["claim_id"] == old_claim_id
    assert summary["generation"] == old_generation
    assert summary["claimed_at"] == old_claimed_at
    assert summary["worker_id"] == old_worker
    assert summary["cancel_requested"] is False
    assert "claim_token" not in summary
    assert payload["recommended_heartbeat_seconds"] == 10

    new_expiry = datetime.fromisoformat(
        summary["lease_expires_at"].replace("Z", "+00:00")
    )
    server_time = datetime.fromisoformat(
        payload["server_time"].replace("Z", "+00:00")
    )
    # Reset from Queue-store now + duration — not old_deadline + duration.
    assert new_expiry < old_expiry
    delta = abs((new_expiry - (server_time + timedelta(seconds=30))).total_seconds())
    assert delta < 2.0

    assert old_token not in caplog.text
    assert PAYLOAD_SENTINEL not in caplog.text
    assert WORKER_TOKEN not in caplog.text
    assert old_token not in path
    assert "?" not in path
    assert CLAIM_TOKEN_HEADER.lower() not in resp_body.decode("utf-8").lower()

    after = session_factory()
    try:
        snap_after = _snapshot_lease_rows(after, task_id=task_id)
        assert snap_after["task_generation"] == snap_before["task_generation"]
        assert snap_after["task_claim_id"] == snap_before["task_claim_id"]
        assert snap_after["task_claimed_at"] == snap_before["task_claimed_at"]
        assert snap_after["task_worker_id"] == snap_before["task_worker_id"]
        assert snap_after["task_state_code"] == 3
        assert snap_after["registry_token"] == snap_before["registry_token"]
        assert snap_after["registry_generation"] == snap_before["registry_generation"]
        assert snap_after["registry_claimed_at"] == snap_before["registry_claimed_at"]
        assert snap_after["attempt_count"] == snap_before["attempt_count"] == 1
        assert snap_after["task_lease_expires_at"] == snap_after["registry_lease_expires_at"]
        assert snap_after["task_lease_expires_at"] != snap_before["task_lease_expires_at"]
    finally:
        after.close()


@pytest.mark.parametrize(
    "mutate",
    [
        "wrong_token",
        "wrong_generation",
        "expired",
        "superseded",
    ],
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
        idempotency_key=f"idem-hb-stale-{mutate}",
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
                    lease_expires_at=ClaimRegistry.claimed_at + text("interval '1 second'")
                )
            )
            # Force both past now while satisfying registry check lease_expires_at > claimed_at:
            # set claimed_at far in the past then expiry still before now.
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
    finally:
        before.close()

    status, _hdrs, resp = _asgi_http_call(
        app,
        method="POST",
        path=_heartbeat_path(claim_id),
        headers=_worker_headers(claim_token=token),
        body=json.dumps(
            _heartbeat_body(generation=generation, lease_seconds=30),
            separators=(",", ":"),
        ).encode("utf-8"),
    )
    assert status == 409
    _assert_error(resp, code="lease_lost", retryable=False)

    after = session_factory()
    try:
        snap_after = _snapshot_lease_rows(after, task_id=task_id)
        assert snap_after == snap_before
    finally:
        after.close()


def test_over_ceiling_lease_rejected_before_write(
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
        idempotency_key="idem-hb-ceiling",
    )
    claimed = _claim_one(app, queue_name=queue_name, lease_seconds=60)
    claim = claimed["claim"]
    task_id = uuid.UUID(claimed["task"]["task_id"])

    before = session_factory()
    try:
        snap_before = _snapshot_lease_rows(before, task_id=task_id)
    finally:
        before.close()

    for bad_seconds in (0, 3601, 1.5, True):
        status, _hdrs, resp = _asgi_http_call(
            app,
            method="POST",
            path=_heartbeat_path(claim["claim_id"]),
            headers=_worker_headers(claim_token=claim["claim_token"]),
            body=json.dumps(
                {"generation": claim["generation"], "lease_seconds": bad_seconds},
                separators=(",", ":"),
            ).encode("utf-8"),
        )
        assert status == 400, bad_seconds
        _assert_error(resp, code="validation_failed", retryable=False)

    after = session_factory()
    try:
        assert _snapshot_lease_rows(after, task_id=task_id) == snap_before
    finally:
        after.close()


def test_missing_claim_token_header_is_validation_failure(
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
        idempotency_key="idem-hb-missing-token",
    )
    claimed = _claim_one(app, queue_name=queue_name)
    claim = claimed["claim"]

    status, _hdrs, resp = _asgi_http_call(
        app,
        method="POST",
        path=_heartbeat_path(claim["claim_id"]),
        headers=_worker_headers(),  # no claim token
        body=json.dumps(
            _heartbeat_body(generation=claim["generation"], lease_seconds=30),
            separators=(",", ":"),
        ).encode("utf-8"),
    )
    assert status == 400
    _assert_error(resp, code="validation_failed", retryable=False)


def test_shared_fence_probe_matches_heartbeat_stale_decision(
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
        idempotency_key="idem-hb-probe",
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
        assert current.queue_name == queue_name
        assert current.task_id == task_id
    finally:
        probe_session.rollback()
        probe_session.close()

    # Expire and reclaim so old credentials become stale.
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
        path=_heartbeat_path(str(claim_id)),
        headers=_worker_headers(claim_token=str(token)),
        body=json.dumps(
            _heartbeat_body(generation=generation, lease_seconds=30),
            separators=(",", ":"),
        ).encode("utf-8"),
    )
    assert status == 409
    _assert_error(resp, code="lease_lost", retryable=False)

    # Probe must not implement terminal transitions.
    verify = session_factory()
    try:
        task = verify.execute(
            select(TaskActive).where(TaskActive.task_id == task_id)
        ).scalar_one()
        assert int(task.state_code) == 3
        assert int(task.generation) == 2
        attempts = list(
            verify.scalars(select(TaskAttempt).where(TaskAttempt.task_id == task_id))
        )
        assert len(attempts) == 2
    finally:
        verify.close()


def test_out_of_scope_worker_denied_without_mutation(
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
        idempotency_key="idem-hb-scope",
    )
    claimed = _claim_one(app, queue_name=queue_name)
    claim = claimed["claim"]
    task_id = uuid.UUID(claimed["task"]["task_id"])

    before = session_factory()
    try:
        snap_before = _snapshot_lease_rows(before, task_id=task_id)
    finally:
        before.close()

    status, _hdrs, resp = _asgi_http_call(
        app,
        method="POST",
        path=_heartbeat_path(claim["claim_id"]),
        headers=_worker_headers(
            token=WORKER_OTHER_TOKEN,
            claim_token=claim["claim_token"],
        ),
        body=json.dumps(
            _heartbeat_body(generation=claim["generation"], lease_seconds=30),
            separators=(",", ":"),
        ).encode("utf-8"),
    )
    assert status == 403
    _assert_error(resp, code="permission_denied", retryable=False)

    after = session_factory()
    try:
        assert _snapshot_lease_rows(after, task_id=task_id) == snap_before
    finally:
        after.close()
