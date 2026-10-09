"""Deterministic real-PostgreSQL cancellation race qualification (Phase 03.6-08).

Covers WORK-08 / WORK-14 / COMP-03 / API-03 / QUAL-02:
cancel vs fail lock orders, cancel-request vs lease expiry, ack_cancel vs expiry,
duplicate/changed-body ack_cancel replay, and stale-worker fencing after
cancellation finalizes.

Thread Events coordinate hold points at documented ``FOR UPDATE`` boundaries.
Wall-clock sleeps are never the race oracle.
"""

from __future__ import annotations

import asyncio
import json
import os
import re
import threading
import uuid
from collections.abc import Iterator, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from unittest.mock import patch
from uuid import UUID

import psycopg
import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import create_engine, event, func, select, text, update
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker

from workhold.api.application import create_application_app
from workhold.api.security import ListenerBind
from workhold.application.claim_service import ClaimService
from workhold.application.lease_expiry import LeaseExpiryService
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
from workhold.infrastructure.postgres.task_transitions import (
    TaskTransitionRepository,
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
    DeliveryEventActive,
    Queue,
    TaskActive,
    TaskAttempt,
    TaskTerminal,
)

_JOIN_TIMEOUT_S = 45.0
_RACE_ITERS = 10
_LEASE_SECONDS = 60
_ROOT = Path(__file__).resolve().parents[2]
_ALEMBIC_INI = _ROOT / "alembic.ini"
_SCHEMA_NAME_RE = re.compile(r"^[a-z_][a-z0-9_]*$")

CLAIM_PATH = "/v1/claims"
CLAIM_TOKEN_HEADER = "X-Queue-Claim-Token"
PAYLOAD_SENTINEL = "CANCEL_RACE_PAYLOAD_SHOULD_NEVER_LEAK"

PRODUCER_TOKEN = "tok-producer-cancel-race"
WORKER_TOKEN = "tok-worker-cancel-race"
ADMIN_TOKEN = "tok-admin-cancel-race"

PRODUCER_PRINCIPAL = "producer-cancel-race"
WORKER_PRINCIPAL = "worker-cancel-race"
ADMIN_PRINCIPAL = "admin-cancel-race"

FAILURE_CODE = "race.cancel.boundary"
FAILURE_DETAIL_MAX = "é" + ("x" * 4095)  # exactly 4096 Unicode code points
assert len(FAILURE_DETAIL_MAX) == 4096

_OP_FAIL = 2
_OP_ACK_CANCEL = 3
_OUTCOME_ACTIVE = 1
_OUTCOME_DEAD = 4
_OUTCOME_EXPIRED = 5
_OUTCOME_CANCELLED = 6
_RESULT_DEAD = 11
_RESULT_CANCELLED = 12
_TASK_LEASED = 3


def _require_test_database_url() -> str:
    url = os.environ.get("TEST_DATABASE_URL", "").strip()
    if not url:
        pytest.fail(
            "TEST_DATABASE_URL is required for tests/concurrency "
            "(PostgreSQL 16). Refusing to skip or xfail."
        )
    return url


def _to_psycopg_conninfo(url: str) -> str:
    if url.startswith("postgresql+psycopg://"):
        return "postgresql://" + url.removeprefix("postgresql+psycopg://")
    return url


def _run_alembic(direction: str, target: str, *, schema: str, database_url: str) -> None:
    if not _SCHEMA_NAME_RE.fullmatch(schema):
        raise ValueError(f"refusing unsafe schema name: {schema!r}")
    previous_url = os.environ.get("DATABASE_URL")
    previous_schema = os.environ.get("ALEMBIC_VERSION_TABLE_SCHEMA")
    os.environ["DATABASE_URL"] = database_url
    os.environ["ALEMBIC_VERSION_TABLE_SCHEMA"] = schema
    try:
        cfg = Config(str(_ALEMBIC_INI))
        if direction == "upgrade":
            command.upgrade(cfg, target)
        elif direction == "downgrade":
            command.downgrade(cfg, target)
        else:
            raise ValueError(f"unknown alembic direction: {direction}")
    finally:
        if previous_url is None:
            os.environ.pop("DATABASE_URL", None)
        else:
            os.environ["DATABASE_URL"] = previous_url
        if previous_schema is None:
            os.environ.pop("ALEMBIC_VERSION_TABLE_SCHEMA", None)
        else:
            os.environ["ALEMBIC_VERSION_TABLE_SCHEMA"] = previous_schema


@pytest.fixture(scope="module")
def race_migrated_schema() -> Iterator[str]:
    database_url = _require_test_database_url()
    schema = f"cancelrace_{uuid.uuid4().hex}"
    if not _SCHEMA_NAME_RE.fullmatch(schema):
        raise ValueError(f"refusing unsafe schema name: {schema!r}")
    admin = psycopg.connect(_to_psycopg_conninfo(database_url))
    admin.autocommit = True
    try:
        admin.execute(f'CREATE SCHEMA "{schema}"')
    finally:
        admin.close()

    _run_alembic("upgrade", "head", schema=schema, database_url=database_url)
    try:
        yield schema
    finally:
        try:
            _run_alembic(
                "downgrade", "base", schema=schema, database_url=database_url
            )
        finally:
            drop = psycopg.connect(_to_psycopg_conninfo(database_url))
            drop.autocommit = True
            try:
                drop.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
            finally:
                drop.close()


@pytest.fixture
def sa_engine(race_migrated_schema: str) -> Iterator[Engine]:
    database_url = _require_test_database_url()
    schema = race_migrated_schema
    engine = create_engine(
        database_url,
        pool_pre_ping=True,
        pool_size=40,
        max_overflow=0,
    )

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
def session_factory(sa_engine: Engine) -> sessionmaker[Session]:
    return sessionmaker(bind=sa_engine, expire_on_commit=False)


@dataclass
class _ThreadResult:
    ok: bool = False
    value: Any = None
    error: BaseException | None = None
    status: int | None = None
    body: bytes = b""


def _unique(prefix: str) -> str:
    return f"{prefix}.{uuid.uuid4().hex[:12]}"


def _meta(*, actor_id: str) -> AdminRequestMetadata:
    return AdminRequestMetadata(
        actor_id=actor_id,
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


def _make_app(
    session_factory: sessionmaker[Session],
    *,
    queue_name: str,
) -> Any:
    authorizer = Authorizer(
        queue_scopes={
            PRODUCER_PRINCIPAL: frozenset({queue_name}),
            WORKER_PRINCIPAL: frozenset({queue_name}),
            ADMIN_PRINCIPAL: frozenset({queue_name}),
        }
    )
    return create_application_app(
        authenticator=BearerCredentialAuthenticator.from_bindings(_bindings()),
        authorizer=authorizer,
        bind=ListenerBind(host="127.0.0.1", port=18198),
        session_factory=session_factory,
        enqueue_service=EnqueueService(
            session_factory=session_factory,
            depth_ceilings=DepthCeilings(
                queue_active_depth=10000,
                instance_active_depth=50000,
                retry_after_ms=250,
            ),
        ),
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


def _join(thread: threading.Thread, *, label: str) -> None:
    thread.join(timeout=_JOIN_TIMEOUT_S)
    assert not thread.is_alive(), f"{label} did not finish within {_JOIN_TIMEOUT_S}s"


def _assert_clean_text(text: str, *, forbidden_tokens: tuple[str, ...] = ()) -> None:
    assert PAYLOAD_SENTINEL not in text
    for token in forbidden_tokens:
        if token:
            assert token not in text


def _safe_status_detail(status: int, resp: bytes) -> str:
    return f"unexpected status={status} body_len={len(resp)}"


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
            metadata=_meta(actor_id="seed"),
        ),
    )
    session.commit()
    return session.execute(select(Queue).where(Queue.name == name)).scalar_one()


def _enqueue_ready(
    app: Any,
    *,
    queue_name: str,
    idempotency_key: str | None = None,
) -> str:
    path = f"/v1/queues/{queue_name}/tasks"
    body = json.dumps(
        {"payload": {"secret": PAYLOAD_SENTINEL, "n": 1}, "priority": 0},
        separators=(",", ":"),
    ).encode("utf-8")
    headers = {
        "Authorization": f"Bearer {PRODUCER_TOKEN}",
        "Content-Type": "application/json",
        "Idempotency-Key": idempotency_key or f"idem-{uuid.uuid4().hex}",
    }
    status, _hdrs, resp = _asgi_http_call(
        app, method="POST", path=path, headers=headers, body=body
    )
    assert status == 201, _safe_status_detail(status, resp)
    return str(json.loads(resp.decode("utf-8"))["task"]["task_id"])


def _worker_headers(*, claim_token: str | None = None) -> dict[str, str]:
    headers = {
        "Authorization": f"Bearer {WORKER_TOKEN}",
        "Content-Type": "application/json",
    }
    if claim_token is not None:
        headers[CLAIM_TOKEN_HEADER] = claim_token
    return headers


def _claim_body(*, queues: list[str], worker_id: str) -> dict[str, Any]:
    return {
        "queues": queues,
        "max_tasks": 1,
        "lease_seconds": _LEASE_SECONDS,
        "wait_seconds": 0,
        "worker_id": worker_id,
    }


def _http_claim(
    app: Any,
    *,
    queue_name: str,
    worker_id: str,
) -> tuple[int, dict[str, Any], bytes]:
    body = json.dumps(
        _claim_body(queues=[queue_name], worker_id=worker_id),
        separators=(",", ":"),
    ).encode("utf-8")
    status, _hdrs, resp = _asgi_http_call(
        app,
        method="POST",
        path=CLAIM_PATH,
        headers=_worker_headers(),
        body=body,
    )
    payload = json.loads(resp.decode("utf-8")) if resp else {}
    return status, payload, resp


def _claim_one_http(app: Any, *, queue_name: str, worker_id: str) -> dict[str, Any]:
    status, payload, resp = _http_claim(app, queue_name=queue_name, worker_id=worker_id)
    assert status == 200, _safe_status_detail(status, resp)
    assert len(payload["tasks"]) == 1
    return payload["tasks"][0]


def _fail_path(claim_id: str) -> str:
    return f"/v1/claims/{claim_id}:fail"


def _ack_path(claim_id: str) -> str:
    return f"/v1/claims/{claim_id}:ack-cancel"


def _cancel_path(task_id: str) -> str:
    return f"/v1/tasks/{task_id}:cancel"


def _fail_body(
    *,
    generation: int,
    retryable: bool = True,
    failure_code: str = FAILURE_CODE,
    failure_detail: str | None = FAILURE_DETAIL_MAX,
) -> dict[str, Any]:
    body: dict[str, Any] = {
        "generation": generation,
        "retryable": retryable,
        "failure_code": failure_code,
    }
    if failure_detail is not None:
        body["failure_detail"] = failure_detail
    return body


def _ack_body(*, generation: int) -> dict[str, Any]:
    return {"generation": generation}


def _http_fail(
    app: Any,
    *,
    claim_id: str,
    claim_token: str,
    generation: int,
    retryable: bool = True,
    failure_code: str = FAILURE_CODE,
    failure_detail: str | None = FAILURE_DETAIL_MAX,
) -> tuple[int, dict[str, Any], bytes]:
    body = json.dumps(
        _fail_body(
            generation=generation,
            retryable=retryable,
            failure_code=failure_code,
            failure_detail=failure_detail,
        ),
        separators=(",", ":"),
        ensure_ascii=False,
    ).encode("utf-8")
    status, _hdrs, resp = _asgi_http_call(
        app,
        method="POST",
        path=_fail_path(claim_id),
        headers=_worker_headers(claim_token=claim_token),
        body=body,
    )
    payload = json.loads(resp.decode("utf-8")) if resp else {}
    return status, payload, resp


def _http_ack_cancel(
    app: Any,
    *,
    claim_id: str,
    claim_token: str,
    generation: int,
) -> tuple[int, dict[str, Any], bytes]:
    body = json.dumps(
        _ack_body(generation=generation),
        separators=(",", ":"),
    ).encode("utf-8")
    status, _hdrs, resp = _asgi_http_call(
        app,
        method="POST",
        path=_ack_path(claim_id),
        headers=_worker_headers(claim_token=claim_token),
        body=body,
    )
    payload = json.loads(resp.decode("utf-8")) if resp else {}
    return status, payload, resp


def _http_cancel(app: Any, *, task_id: str) -> tuple[int, dict[str, Any], bytes]:
    status, _hdrs, resp = _asgi_http_call(
        app,
        method="POST",
        path=_cancel_path(task_id),
        headers={
            "Authorization": f"Bearer {PRODUCER_TOKEN}",
            "Content-Type": "application/json",
        },
        body=b"{}",
    )
    payload = json.loads(resp.decode("utf-8")) if resp else {}
    return status, payload, resp


def _expire_lease(session: Session, *, task_id: UUID, claim_id: UUID) -> None:
    """Cross expiry using Queue-store timestamps only (not worker wall clock)."""
    session.execute(
        update(TaskActive)
        .where(TaskActive.task_id == task_id)
        .values(
            lease_expires_at=func.transaction_timestamp() - text("interval '1 second'")
        )
    )
    session.execute(
        update(ClaimRegistry)
        .where(ClaimRegistry.claim_id == claim_id)
        .values(
            claimed_at=func.transaction_timestamp() - text("interval '2 hours'"),
            lease_expires_at=func.transaction_timestamp()
            - text("interval '1 second'"),
        )
    )
    session.commit()


def _assert_error(
    status: int,
    payload: dict[str, Any],
    resp: bytes,
    *,
    code: str,
) -> None:
    assert status in {409, 404}, _safe_status_detail(status, resp)
    assert payload.get("code") == code
    assert payload.get("retryable") is False
    _assert_clean_text(resp.decode("utf-8", errors="replace"))


def _assert_one_closed_attempt(
    session: Session,
    *,
    task_id: UUID,
    allowed_outcomes: set[int],
) -> TaskAttempt:
    attempts = list(
        session.scalars(
            select(TaskAttempt)
            .where(TaskAttempt.task_id == task_id)
            .order_by(TaskAttempt.generation)
        )
    )
    closed = [a for a in attempts if int(a.outcome_code) != _OUTCOME_ACTIVE]
    assert len(closed) == 1, f"expected one closed attempt, got {len(closed)}"
    assert int(closed[0].outcome_code) in allowed_outcomes
    return closed[0]


def _assert_cancelled_terminal_clean(
    session: Session,
    *,
    task_id: UUID,
    claim_id: UUID,
    allowed_outcomes: set[int],
) -> None:
    closed = _assert_one_closed_attempt(
        session, task_id=task_id, allowed_outcomes=allowed_outcomes
    )
    assert closed.ended_at is not None
    terminals = list(
        session.scalars(select(TaskTerminal).where(TaskTerminal.task_id == task_id))
    )
    assert len(terminals) == 1
    assert int(terminals[0].state_code) == _RESULT_CANCELLED
    assert (
        session.execute(
            select(TaskActive).where(TaskActive.task_id == task_id)
        ).scalar_one_or_none()
        is None
    )
    assert (
        session.execute(
            select(ClaimRegistry).where(ClaimRegistry.claim_id == claim_id)
        ).scalar_one_or_none()
        is None
    )
    spawned = session.scalar(
        select(func.count())
        .select_from(TaskActive)
        .where(TaskActive.source_task_id == task_id)
    )
    events = session.scalar(
        select(func.count())
        .select_from(DeliveryEventActive)
        .where(DeliveryEventActive.source_task_id == task_id)
    )
    assert int(spawned or 0) == 0
    assert int(events or 0) == 0


def _assert_request_only_leased(
    session: Session,
    *,
    task_id: UUID,
    claim_id: UUID,
) -> None:
    task = session.execute(
        select(TaskActive).where(TaskActive.task_id == task_id)
    ).scalar_one()
    assert int(task.state_code) == _TASK_LEASED
    assert task.cancel_requested_at is not None
    assert task.current_claim_id == claim_id
    attempts = list(
        session.scalars(select(TaskAttempt).where(TaskAttempt.task_id == task_id))
    )
    assert len(attempts) == 1
    assert int(attempts[0].outcome_code) == _OUTCOME_ACTIVE
    assert (
        session.execute(
            select(TaskTerminal).where(TaskTerminal.task_id == task_id)
        ).scalar_one_or_none()
        is None
    )
    assert (
        session.execute(
            select(ClaimRegistry).where(ClaimRegistry.claim_id == claim_id)
        ).scalar_one_or_none()
        is not None
    )


def _setup_leased(
    session_factory: sessionmaker[Session],
    *,
    name: str,
    enabled: bool = True,
    max_attempts: int = 3,
) -> tuple[Any, UUID, dict[str, Any]]:
    setup = session_factory()
    try:
        _seed_queue(
            setup,
            name=name,
            enabled=enabled,
            max_attempts=max_attempts,
            retry_delay_seconds=0,
        )
    finally:
        setup.close()
    app = _make_app(session_factory, queue_name=name)
    task_id = UUID(_enqueue_ready(app, queue_name=name))
    claimed = _claim_one_http(app, queue_name=name, worker_id="cancel-owner")
    return app, task_id, claimed


# ---------------------------------------------------------------------------
# Cancel-before-fail sequential paths (request-only → cancel_race_lost → finish)
# ---------------------------------------------------------------------------


def test_cancel_before_fail_then_ack_cancel_one_cancelled(
    session_factory: sessionmaker[Session],
) -> None:
    name = _unique("seq.cancel-fail-ack")
    app, task_id, claimed = _setup_leased(session_factory, name=name)
    claim = claimed["claim"]
    claim_id = str(claim["claim_id"])
    claim_token = str(claim["claim_token"])
    generation = int(claim["generation"])

    status_c, payload_c, resp_c = _http_cancel(app, task_id=str(task_id))
    assert status_c == 200, _safe_status_detail(status_c, resp_c)
    assert payload_c["task"]["state"] == "leased"
    assert payload_c["task"]["current_claim"]["cancel_requested"] is True

    verify = session_factory()
    try:
        _assert_request_only_leased(
            verify, task_id=task_id, claim_id=UUID(claim_id)
        )
    finally:
        verify.close()

    status_f, payload_f, resp_f = _http_fail(
        app,
        claim_id=claim_id,
        claim_token=claim_token,
        generation=generation,
        retryable=True,
    )
    _assert_error(status_f, payload_f, resp_f, code="cancel_race_lost")

    after_fail = session_factory()
    try:
        _assert_request_only_leased(
            after_fail, task_id=task_id, claim_id=UUID(claim_id)
        )
        assert (
            after_fail.execute(
                select(CompleteReplay).where(CompleteReplay.claim_id == UUID(claim_id))
            ).scalar_one_or_none()
            is None
        )
    finally:
        after_fail.close()

    status_a, payload_a, resp_a = _http_ack_cancel(
        app,
        claim_id=claim_id,
        claim_token=claim_token,
        generation=generation,
    )
    assert status_a == 200, _safe_status_detail(status_a, resp_a)
    assert payload_a["state"] == "cancelled"
    assert payload_a["replayed"] is False
    _assert_clean_text(
        resp_a.decode("utf-8", errors="replace"),
        forbidden_tokens=(claim_token,),
    )

    final = session_factory()
    try:
        _assert_cancelled_terminal_clean(
            final,
            task_id=task_id,
            claim_id=UUID(claim_id),
            allowed_outcomes={_OUTCOME_CANCELLED},
        )
        _assert_one_closed_attempt(
            final, task_id=task_id, allowed_outcomes={_OUTCOME_CANCELLED}
        )
        replays = list(
            final.scalars(
                select(CompleteReplay).where(
                    CompleteReplay.claim_id == UUID(claim_id),
                    CompleteReplay.operation_code == _OP_ACK_CANCEL,
                )
            )
        )
        assert len(replays) == 1
    finally:
        final.close()


def test_cancel_before_fail_then_expiry_one_cancelled(
    session_factory: sessionmaker[Session],
) -> None:
    name = _unique("seq.cancel-fail-exp")
    app, task_id, claimed = _setup_leased(session_factory, name=name)
    claim = claimed["claim"]
    claim_id = str(claim["claim_id"])
    claim_token = str(claim["claim_token"])
    generation = int(claim["generation"])

    status_c, _payload_c, resp_c = _http_cancel(app, task_id=str(task_id))
    assert status_c == 200, _safe_status_detail(status_c, resp_c)

    verify = session_factory()
    try:
        _assert_request_only_leased(
            verify, task_id=task_id, claim_id=UUID(claim_id)
        )
    finally:
        verify.close()

    status_f, payload_f, resp_f = _http_fail(
        app,
        claim_id=claim_id,
        claim_token=claim_token,
        generation=generation,
        retryable=False,
    )
    _assert_error(status_f, payload_f, resp_f, code="cancel_race_lost")

    expire = session_factory()
    try:
        _expire_lease(expire, task_id=task_id, claim_id=UUID(claim_id))
    finally:
        expire.close()

    outcome = LeaseExpiryService(session_factory=session_factory).finalize_expired(
        task_id=task_id
    )
    assert outcome.state == "cancelled"

    final = session_factory()
    try:
        _assert_cancelled_terminal_clean(
            final,
            task_id=task_id,
            claim_id=UUID(claim_id),
            allowed_outcomes={_OUTCOME_EXPIRED},
        )
        assert (
            final.execute(
                select(CompleteReplay).where(CompleteReplay.claim_id == UUID(claim_id))
            ).scalar_one_or_none()
            is None
        )
    finally:
        final.close()

    reclaim = _http_claim(app, queue_name=name, worker_id="reclaimer")
    assert reclaim[0] == 200
    assert reclaim[1].get("tasks") == []


# ---------------------------------------------------------------------------
# Barrier-controlled cancel vs fail lock winners
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("iteration", range(_RACE_ITERS))
def test_cancel_first_fail_observes_cancel_race_lost(
    session_factory: sessionmaker[Session],
    iteration: int,
) -> None:
    """Cancel holds after TaskActive FOR UPDATE; fail then sees cancel_race_lost."""
    name = _unique(f"race.cancel-first.{iteration}")
    app, task_id, claimed = _setup_leased(session_factory, name=name)
    claim = claimed["claim"]
    claim_id = str(claim["claim_id"])
    claim_token = str(claim["claim_token"])
    generation = int(claim["generation"])

    cancel_locked = threading.Event()
    fail_started = threading.Event()
    results = {"cancel": _ThreadResult(), "fail": _ThreadResult()}
    original_cancel = TaskTransitionRepository.cancel_task

    def cancel_then_hold(
        self: TaskTransitionRepository,
        session: Session,
        *,
        task_id: UUID,
        producer_id: str,
        authorize_queue: Any,
    ) -> Any:
        result = original_cancel(
            self,
            session,
            task_id=task_id,
            producer_id=producer_id,
            authorize_queue=authorize_queue,
        )
        cancel_locked.set()
        assert fail_started.wait(timeout=_JOIN_TIMEOUT_S), "fail never started"
        return result

    def run_cancel() -> None:
        try:
            with patch.object(
                TaskTransitionRepository, "cancel_task", cancel_then_hold
            ):
                status, payload, resp = _http_cancel(app, task_id=str(task_id))
            results["cancel"] = _ThreadResult(
                ok=True, value=payload, status=status, body=resp
            )
        except BaseException as exc:  # noqa: BLE001
            results["cancel"] = _ThreadResult(ok=False, error=exc)

    def run_fail() -> None:
        assert cancel_locked.wait(timeout=_JOIN_TIMEOUT_S), "cancel never locked"
        fail_started.set()
        try:
            status, payload, resp = _http_fail(
                app,
                claim_id=claim_id,
                claim_token=claim_token,
                generation=generation,
                retryable=True,
            )
            results["fail"] = _ThreadResult(
                ok=True, value=payload, status=status, body=resp
            )
        except BaseException as exc:  # noqa: BLE001
            results["fail"] = _ThreadResult(ok=False, error=exc)

    t_cancel = threading.Thread(target=run_cancel, daemon=True)
    t_fail = threading.Thread(target=run_fail, daemon=True)
    t_cancel.start()
    t_fail.start()
    _join(t_cancel, label="cancel-first")
    _join(t_fail, label="fail-during-cancel")

    assert results["cancel"].ok, results["cancel"].error
    assert results["fail"].ok, results["fail"].error
    assert results["cancel"].status == 200
    _assert_error(
        results["fail"].status,
        results["fail"].value,
        results["fail"].body,
        code="cancel_race_lost",
    )

    verify = session_factory()
    try:
        _assert_request_only_leased(
            verify, task_id=task_id, claim_id=UUID(claim_id)
        )
    finally:
        verify.close()


@pytest.mark.parametrize("iteration", range(_RACE_ITERS))
def test_fail_first_cancel_preserves_dead_letter(
    session_factory: sessionmaker[Session],
    iteration: int,
) -> None:
    """Fail holds after CURRENT fence; cancel then sees task_already_terminal."""
    name = _unique(f"race.fail-first.{iteration}")
    app, task_id, claimed = _setup_leased(
        session_factory, name=name, enabled=False, max_attempts=1
    )
    claim = claimed["claim"]
    claim_id = str(claim["claim_id"])
    claim_token = str(claim["claim_token"])
    generation = int(claim["generation"])

    fail_locked = threading.Event()
    cancel_started = threading.Event()
    results = {"fail": _ThreadResult(), "cancel": _ThreadResult()}
    original_validate = LeaseRepository.validate_current_lease

    def validate_then_hold(
        self: LeaseRepository,
        session: Session,
        *,
        claim_id: UUID,
        claim_token: UUID,
        generation: int,
        for_update: bool = True,
    ) -> Any:
        result = original_validate(
            self,
            session,
            claim_id=claim_id,
            claim_token=claim_token,
            generation=generation,
            for_update=for_update,
        )
        if for_update and result.decision is FenceDecision.CURRENT:
            fail_locked.set()
            assert cancel_started.wait(timeout=_JOIN_TIMEOUT_S), "cancel never started"
        return result

    def run_fail() -> None:
        try:
            with patch.object(
                LeaseRepository, "validate_current_lease", validate_then_hold
            ):
                status, payload, resp = _http_fail(
                    app,
                    claim_id=claim_id,
                    claim_token=claim_token,
                    generation=generation,
                    retryable=False,
                )
            results["fail"] = _ThreadResult(
                ok=True, value=payload, status=status, body=resp
            )
        except BaseException as exc:  # noqa: BLE001
            results["fail"] = _ThreadResult(ok=False, error=exc)

    def run_cancel() -> None:
        assert fail_locked.wait(timeout=_JOIN_TIMEOUT_S), "fail never locked"
        cancel_started.set()
        try:
            status, payload, resp = _http_cancel(app, task_id=str(task_id))
            results["cancel"] = _ThreadResult(
                ok=True, value=payload, status=status, body=resp
            )
        except BaseException as exc:  # noqa: BLE001
            results["cancel"] = _ThreadResult(ok=False, error=exc)

    t_fail = threading.Thread(target=run_fail, daemon=True)
    t_cancel = threading.Thread(target=run_cancel, daemon=True)
    t_fail.start()
    t_cancel.start()
    _join(t_fail, label="fail-first")
    _join(t_cancel, label="cancel-during-fail")

    assert results["fail"].ok, results["fail"].error
    assert results["cancel"].ok, results["cancel"].error
    assert results["fail"].status == 200
    assert results["fail"].value["state"] == "dead_lettered"
    assert results["fail"].value["replayed"] is False
    _assert_error(
        results["cancel"].status,
        results["cancel"].value,
        results["cancel"].body,
        code="task_already_terminal",
    )

    verify = session_factory()
    try:
        closed = _assert_one_closed_attempt(
            verify, task_id=task_id, allowed_outcomes={_OUTCOME_DEAD}
        )
        assert closed.failure_code == FAILURE_CODE
        assert closed.failure_detail == FAILURE_DETAIL_MAX
        terminals = list(
            verify.scalars(select(TaskTerminal).where(TaskTerminal.task_id == task_id))
        )
        assert len(terminals) == 1
        assert int(terminals[0].state_code) == _RESULT_DEAD
        assert (
            verify.execute(
                select(CompleteReplay).where(
                    CompleteReplay.claim_id == UUID(claim_id),
                    CompleteReplay.operation_code == _OP_FAIL,
                )
            ).scalar_one()
            is not None
        )
        assert (
            verify.execute(
                select(TaskActive).where(TaskActive.task_id == task_id)
            ).scalar_one_or_none()
            is None
        )
    finally:
        verify.close()


# ---------------------------------------------------------------------------
# Cancel request vs lease expiry
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("iteration", range(_RACE_ITERS))
def test_cancel_request_before_expiry_cancels_no_reclaim(
    session_factory: sessionmaker[Session],
    iteration: int,
) -> None:
    """Cancel holds request marker; concurrent expiry yields one cancelled terminal."""
    name = _unique(f"race.req-before-exp.{iteration}")
    app, task_id, claimed = _setup_leased(session_factory, name=name, max_attempts=5)
    claim = claimed["claim"]
    claim_id = str(claim["claim_id"])

    # Force-expire before the race so finalize can proceed once TaskActive unlocks.
    expire = session_factory()
    try:
        _expire_lease(expire, task_id=task_id, claim_id=UUID(claim_id))
    finally:
        expire.close()

    cancel_locked = threading.Event()
    expiry_started = threading.Event()
    results = {"cancel": _ThreadResult(), "expiry": _ThreadResult()}
    original_cancel = TaskTransitionRepository.cancel_task

    def cancel_then_hold(
        self: TaskTransitionRepository,
        session: Session,
        *,
        task_id: UUID,
        producer_id: str,
        authorize_queue: Any,
    ) -> Any:
        result = original_cancel(
            self,
            session,
            task_id=task_id,
            producer_id=producer_id,
            authorize_queue=authorize_queue,
        )
        cancel_locked.set()
        assert expiry_started.wait(timeout=_JOIN_TIMEOUT_S), "expiry never started"
        return result

    def run_cancel() -> None:
        try:
            with patch.object(
                TaskTransitionRepository, "cancel_task", cancel_then_hold
            ):
                status, payload, resp = _http_cancel(app, task_id=str(task_id))
            results["cancel"] = _ThreadResult(
                ok=True, value=payload, status=status, body=resp
            )
        except BaseException as exc:  # noqa: BLE001
            results["cancel"] = _ThreadResult(ok=False, error=exc)

    def run_expiry() -> None:
        assert cancel_locked.wait(timeout=_JOIN_TIMEOUT_S), "cancel never locked"
        expiry_started.set()
        try:
            outcome = LeaseExpiryService(
                session_factory=session_factory
            ).finalize_expired(task_id=task_id)
            results["expiry"] = _ThreadResult(ok=True, value=outcome)
        except BaseException as exc:  # noqa: BLE001
            results["expiry"] = _ThreadResult(ok=False, error=exc)

    t_cancel = threading.Thread(target=run_cancel, daemon=True)
    t_exp = threading.Thread(target=run_expiry, daemon=True)
    t_cancel.start()
    t_exp.start()
    _join(t_cancel, label="cancel-before-expiry")
    _join(t_exp, label="expiry-during-cancel")

    assert results["cancel"].ok, results["cancel"].error
    assert results["expiry"].ok, results["expiry"].error
    assert results["cancel"].status == 200
    assert results["expiry"].value.state == "cancelled"

    verify = session_factory()
    try:
        _assert_cancelled_terminal_clean(
            verify,
            task_id=task_id,
            claim_id=UUID(claim_id),
            allowed_outcomes={_OUTCOME_EXPIRED},
        )
    finally:
        verify.close()

    reclaim_status, reclaim_payload, reclaim_resp = _http_claim(
        app, queue_name=name, worker_id="post-cancel-reclaim"
    )
    assert reclaim_status == 200, _safe_status_detail(reclaim_status, reclaim_resp)
    assert reclaim_payload.get("tasks") == []


@pytest.mark.parametrize("iteration", range(_RACE_ITERS))
def test_expiry_first_with_prior_cancel_request(
    session_factory: sessionmaker[Session],
    iteration: int,
) -> None:
    """Request committed, then expiry holds; concurrent stale fail cannot mutate."""
    name = _unique(f"race.exp-after-req.{iteration}")
    app, task_id, claimed = _setup_leased(session_factory, name=name, max_attempts=5)
    claim = claimed["claim"]
    claim_id = str(claim["claim_id"])
    claim_token = str(claim["claim_token"])
    generation = int(claim["generation"])

    status_c, _p, resp_c = _http_cancel(app, task_id=str(task_id))
    assert status_c == 200, _safe_status_detail(status_c, resp_c)

    expire = session_factory()
    try:
        _expire_lease(expire, task_id=task_id, claim_id=UUID(claim_id))
    finally:
        expire.close()

    expiry_locked = threading.Event()
    fail_started = threading.Event()
    results = {"expiry": _ThreadResult(), "fail": _ThreadResult()}
    original_expire = TaskTransitionRepository.expire_lease

    def expire_then_hold(
        self: TaskTransitionRepository,
        session: Session,
        *,
        task_id: UUID,
    ) -> Any:
        task = session.execute(
            select(TaskActive)
            .where(TaskActive.task_id == task_id)
            .with_for_update()
        ).scalar_one_or_none()
        if task is not None:
            expiry_locked.set()
            assert fail_started.wait(timeout=_JOIN_TIMEOUT_S), "fail never started"
            return self.expire_locked_lease(session, task=task)
        return original_expire(self, session, task_id=task_id)

    def run_expiry() -> None:
        try:
            with patch.object(
                TaskTransitionRepository, "expire_lease", expire_then_hold
            ):
                outcome = LeaseExpiryService(
                    session_factory=session_factory
                ).finalize_expired(task_id=task_id)
            results["expiry"] = _ThreadResult(ok=True, value=outcome)
        except BaseException as exc:  # noqa: BLE001
            results["expiry"] = _ThreadResult(ok=False, error=exc)

    def run_fail() -> None:
        assert expiry_locked.wait(timeout=_JOIN_TIMEOUT_S), "expiry never locked"
        fail_started.set()
        try:
            status, payload, resp = _http_fail(
                app,
                claim_id=claim_id,
                claim_token=claim_token,
                generation=generation,
                retryable=True,
            )
            results["fail"] = _ThreadResult(
                ok=True, value=payload, status=status, body=resp
            )
        except BaseException as exc:  # noqa: BLE001
            results["fail"] = _ThreadResult(ok=False, error=exc)

    t_exp = threading.Thread(target=run_expiry, daemon=True)
    t_fail = threading.Thread(target=run_fail, daemon=True)
    t_exp.start()
    t_fail.start()
    _join(t_exp, label="expiry-first-cancel-req")
    _join(t_fail, label="stale-fail-during-cancel-expiry")

    assert results["expiry"].ok, results["expiry"].error
    assert results["fail"].ok, results["fail"].error
    assert results["expiry"].value.state == "cancelled"
    # Stale after expiry: lease_lost (fence) — not a second terminal.
    _assert_error(
        results["fail"].status,
        results["fail"].value,
        results["fail"].body,
        code="lease_lost",
    )

    verify = session_factory()
    try:
        _assert_cancelled_terminal_clean(
            verify,
            task_id=task_id,
            claim_id=UUID(claim_id),
            allowed_outcomes={_OUTCOME_EXPIRED},
        )
    finally:
        verify.close()


# ---------------------------------------------------------------------------
# ack_cancel vs expiry
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("iteration", range(_RACE_ITERS))
def test_ack_cancel_first_expiry_already_finalized(
    session_factory: sessionmaker[Session],
    iteration: int,
) -> None:
    name = _unique(f"race.ack-first.{iteration}")
    app, task_id, claimed = _setup_leased(session_factory, name=name)
    claim = claimed["claim"]
    claim_id = str(claim["claim_id"])
    claim_token = str(claim["claim_token"])
    generation = int(claim["generation"])

    status_c, _p, resp_c = _http_cancel(app, task_id=str(task_id))
    assert status_c == 200, _safe_status_detail(status_c, resp_c)

    ack_locked = threading.Event()
    expiry_started = threading.Event()
    results = {"ack": _ThreadResult(), "expiry": _ThreadResult()}
    original_validate = LeaseRepository.validate_current_lease

    def validate_then_hold(
        self: LeaseRepository,
        session: Session,
        *,
        claim_id: UUID,
        claim_token: UUID,
        generation: int,
        for_update: bool = True,
    ) -> Any:
        result = original_validate(
            self,
            session,
            claim_id=claim_id,
            claim_token=claim_token,
            generation=generation,
            for_update=for_update,
        )
        if for_update and result.decision is FenceDecision.CURRENT:
            ack_locked.set()
            assert expiry_started.wait(timeout=_JOIN_TIMEOUT_S), "expiry never started"
        return result

    def run_ack() -> None:
        try:
            with patch.object(
                LeaseRepository, "validate_current_lease", validate_then_hold
            ):
                status, payload, resp = _http_ack_cancel(
                    app,
                    claim_id=claim_id,
                    claim_token=claim_token,
                    generation=generation,
                )
            results["ack"] = _ThreadResult(
                ok=True, value=payload, status=status, body=resp
            )
        except BaseException as exc:  # noqa: BLE001
            results["ack"] = _ThreadResult(ok=False, error=exc)

    def run_expiry() -> None:
        assert ack_locked.wait(timeout=_JOIN_TIMEOUT_S), "ack never locked"
        expiry_started.set()
        try:
            # Lease may still be unexpired; finalize blocks on TaskActive until
            # ack_cancel commits and removes the row → already_finalized.
            outcome = LeaseExpiryService(
                session_factory=session_factory
            ).finalize_expired(task_id=task_id)
            results["expiry"] = _ThreadResult(ok=True, value=outcome)
        except BaseException as exc:  # noqa: BLE001
            results["expiry"] = _ThreadResult(ok=False, error=exc)

    t_ack = threading.Thread(target=run_ack, daemon=True)
    t_exp = threading.Thread(target=run_expiry, daemon=True)
    t_ack.start()
    t_exp.start()
    _join(t_ack, label="ack-first")
    _join(t_exp, label="expiry-during-ack")

    assert results["ack"].ok, results["ack"].error
    assert results["expiry"].ok, results["expiry"].error
    assert results["ack"].status == 200
    assert results["ack"].value["state"] == "cancelled"
    assert results["ack"].value["replayed"] is False
    assert results["expiry"].value.state == "already_finalized"

    verify = session_factory()
    try:
        _assert_cancelled_terminal_clean(
            verify,
            task_id=task_id,
            claim_id=UUID(claim_id),
            allowed_outcomes={_OUTCOME_CANCELLED},
        )
        replays = list(
            verify.scalars(
                select(CompleteReplay).where(
                    CompleteReplay.claim_id == UUID(claim_id),
                    CompleteReplay.operation_code == _OP_ACK_CANCEL,
                )
            )
        )
        assert len(replays) == 1
    finally:
        verify.close()


@pytest.mark.parametrize("iteration", range(_RACE_ITERS))
def test_expiry_first_stale_ack_cancel_lease_lost(
    session_factory: sessionmaker[Session],
    iteration: int,
) -> None:
    name = _unique(f"race.exp-first-ack.{iteration}")
    app, task_id, claimed = _setup_leased(session_factory, name=name)
    claim = claimed["claim"]
    claim_id = str(claim["claim_id"])
    claim_token = str(claim["claim_token"])
    generation = int(claim["generation"])

    status_c, _p, resp_c = _http_cancel(app, task_id=str(task_id))
    assert status_c == 200, _safe_status_detail(status_c, resp_c)

    expire = session_factory()
    try:
        _expire_lease(expire, task_id=task_id, claim_id=UUID(claim_id))
    finally:
        expire.close()

    expiry_locked = threading.Event()
    ack_started = threading.Event()
    results = {"expiry": _ThreadResult(), "ack": _ThreadResult()}
    original_expire = TaskTransitionRepository.expire_lease

    def expire_then_hold(
        self: TaskTransitionRepository,
        session: Session,
        *,
        task_id: UUID,
    ) -> Any:
        task = session.execute(
            select(TaskActive)
            .where(TaskActive.task_id == task_id)
            .with_for_update()
        ).scalar_one_or_none()
        if task is not None:
            expiry_locked.set()
            assert ack_started.wait(timeout=_JOIN_TIMEOUT_S), "ack never started"
            return self.expire_locked_lease(session, task=task)
        return original_expire(self, session, task_id=task_id)

    def run_expiry() -> None:
        try:
            with patch.object(
                TaskTransitionRepository, "expire_lease", expire_then_hold
            ):
                outcome = LeaseExpiryService(
                    session_factory=session_factory
                ).finalize_expired(task_id=task_id)
            results["expiry"] = _ThreadResult(ok=True, value=outcome)
        except BaseException as exc:  # noqa: BLE001
            results["expiry"] = _ThreadResult(ok=False, error=exc)

    def run_ack() -> None:
        assert expiry_locked.wait(timeout=_JOIN_TIMEOUT_S), "expiry never locked"
        ack_started.set()
        try:
            status, payload, resp = _http_ack_cancel(
                app,
                claim_id=claim_id,
                claim_token=claim_token,
                generation=generation,
            )
            results["ack"] = _ThreadResult(
                ok=True, value=payload, status=status, body=resp
            )
        except BaseException as exc:  # noqa: BLE001
            results["ack"] = _ThreadResult(ok=False, error=exc)

    t_exp = threading.Thread(target=run_expiry, daemon=True)
    t_ack = threading.Thread(target=run_ack, daemon=True)
    t_exp.start()
    t_ack.start()
    _join(t_exp, label="expiry-first-ack")
    _join(t_ack, label="stale-ack-during-expiry")

    assert results["expiry"].ok, results["expiry"].error
    assert results["ack"].ok, results["ack"].error
    assert results["expiry"].value.state == "cancelled"
    _assert_error(
        results["ack"].status,
        results["ack"].value,
        results["ack"].body,
        code="lease_lost",
    )

    verify = session_factory()
    try:
        _assert_cancelled_terminal_clean(
            verify,
            task_id=task_id,
            claim_id=UUID(claim_id),
            allowed_outcomes={_OUTCOME_EXPIRED},
        )
        assert (
            verify.execute(
                select(CompleteReplay).where(CompleteReplay.claim_id == UUID(claim_id))
            ).scalar_one_or_none()
            is None
        )
    finally:
        verify.close()


# ---------------------------------------------------------------------------
# Duplicate / changed-body ack_cancel + stale after finalize
# ---------------------------------------------------------------------------


def test_duplicate_ack_cancel_replay_identical_fields(
    session_factory: sessionmaker[Session],
) -> None:
    name = _unique("seq.ack-replay")
    app, task_id, claimed = _setup_leased(session_factory, name=name)
    claim = claimed["claim"]
    claim_id = str(claim["claim_id"])
    claim_token = str(claim["claim_token"])
    generation = int(claim["generation"])

    status_c, _p, resp_c = _http_cancel(app, task_id=str(task_id))
    assert status_c == 200, _safe_status_detail(status_c, resp_c)

    status1, payload1, resp1 = _http_ack_cancel(
        app,
        claim_id=claim_id,
        claim_token=claim_token,
        generation=generation,
    )
    assert status1 == 200, _safe_status_detail(status1, resp1)
    assert payload1["replayed"] is False
    assert payload1["state"] == "cancelled"

    status2, payload2, resp2 = _http_ack_cancel(
        app,
        claim_id=claim_id,
        claim_token=claim_token,
        generation=generation,
    )
    assert status2 == 200, _safe_status_detail(status2, resp2)
    assert payload2["replayed"] is True
    assert {k: v for k, v in payload2.items() if k != "replayed"} == {
        k: v for k, v in payload1.items() if k != "replayed"
    }
    _assert_clean_text(
        resp2.decode("utf-8", errors="replace"),
        forbidden_tokens=(claim_token,),
    )

    verify = session_factory()
    try:
        _assert_cancelled_terminal_clean(
            verify,
            task_id=task_id,
            claim_id=UUID(claim_id),
            allowed_outcomes={_OUTCOME_CANCELLED},
        )
        replays = list(
            verify.scalars(
                select(CompleteReplay).where(
                    CompleteReplay.claim_id == UUID(claim_id),
                    CompleteReplay.operation_code == _OP_ACK_CANCEL,
                )
            )
        )
        assert len(replays) == 1
    finally:
        verify.close()


def test_changed_body_ack_cancel_conflicts(
    session_factory: sessionmaker[Session],
) -> None:
    name = _unique("seq.ack-conflict")
    app, task_id, claimed = _setup_leased(session_factory, name=name)
    claim = claimed["claim"]
    claim_id = str(claim["claim_id"])
    claim_token = str(claim["claim_token"])
    generation = int(claim["generation"])

    status_c, _p, resp_c = _http_cancel(app, task_id=str(task_id))
    assert status_c == 200, _safe_status_detail(status_c, resp_c)

    status1, payload1, resp1 = _http_ack_cancel(
        app,
        claim_id=claim_id,
        claim_token=claim_token,
        generation=generation,
    )
    assert status1 == 200, _safe_status_detail(status1, resp1)
    assert payload1["replayed"] is False

    status3, payload3, resp3 = _http_ack_cancel(
        app,
        claim_id=claim_id,
        claim_token=claim_token,
        generation=generation + 1,
    )
    _assert_error(status3, payload3, resp3, code="idempotency_conflict")

    verify = session_factory()
    try:
        _assert_cancelled_terminal_clean(
            verify,
            task_id=task_id,
            claim_id=UUID(claim_id),
            allowed_outcomes={_OUTCOME_CANCELLED},
        )
        assert (
            len(
                list(
                    verify.scalars(
                        select(CompleteReplay).where(
                            CompleteReplay.claim_id == UUID(claim_id)
                        )
                    )
                )
            )
            == 1
        )
    finally:
        verify.close()


def test_stale_ack_cancel_after_expiry_cancellation(
    session_factory: sessionmaker[Session],
) -> None:
    name = _unique("seq.stale-ack-after-exp")
    app, task_id, claimed = _setup_leased(session_factory, name=name, max_attempts=5)
    claim = claimed["claim"]
    claim_id = str(claim["claim_id"])
    claim_token = str(claim["claim_token"])
    generation = int(claim["generation"])

    status_c, _p, resp_c = _http_cancel(app, task_id=str(task_id))
    assert status_c == 200, _safe_status_detail(status_c, resp_c)

    expire = session_factory()
    try:
        _expire_lease(expire, task_id=task_id, claim_id=UUID(claim_id))
    finally:
        expire.close()

    outcome = LeaseExpiryService(session_factory=session_factory).finalize_expired(
        task_id=task_id
    )
    assert outcome.state == "cancelled"

    status_a, payload_a, resp_a = _http_ack_cancel(
        app,
        claim_id=claim_id,
        claim_token=claim_token,
        generation=generation,
    )
    _assert_error(status_a, payload_a, resp_a, code="lease_lost")

    status_f, payload_f, resp_f = _http_fail(
        app,
        claim_id=claim_id,
        claim_token=claim_token,
        generation=generation,
        retryable=True,
    )
    _assert_error(status_f, payload_f, resp_f, code="lease_lost")

    verify = session_factory()
    try:
        _assert_cancelled_terminal_clean(
            verify,
            task_id=task_id,
            claim_id=UUID(claim_id),
            allowed_outcomes={_OUTCOME_EXPIRED},
        )
    finally:
        verify.close()


def test_fail_after_cancellation_request_cancel_race_lost(
    session_factory: sessionmaker[Session],
) -> None:
    name = _unique("seq.fail-after-req")
    app, task_id, claimed = _setup_leased(session_factory, name=name)
    claim = claimed["claim"]
    claim_id = str(claim["claim_id"])
    claim_token = str(claim["claim_token"])
    generation = int(claim["generation"])

    status_c, _p, resp_c = _http_cancel(app, task_id=str(task_id))
    assert status_c == 200, _safe_status_detail(status_c, resp_c)

    status_f, payload_f, resp_f = _http_fail(
        app,
        claim_id=claim_id,
        claim_token=claim_token,
        generation=generation,
        retryable=True,
        failure_code=FAILURE_CODE,
        failure_detail=FAILURE_DETAIL_MAX,
    )
    _assert_error(status_f, payload_f, resp_f, code="cancel_race_lost")

    verify = session_factory()
    try:
        _assert_request_only_leased(
            verify, task_id=task_id, claim_id=UUID(claim_id)
        )
        assert (
            verify.execute(
                select(CompleteReplay).where(CompleteReplay.claim_id == UUID(claim_id))
            ).scalar_one_or_none()
            is None
        )
    finally:
        verify.close()
