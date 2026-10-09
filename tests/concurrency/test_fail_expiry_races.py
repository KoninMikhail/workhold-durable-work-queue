"""Deterministic real-PostgreSQL fail / lease-expiry race qualification (Phase 03.6-07).

Covers WORK-05 / WORK-06 / COMP-03 / API-03 / QUAL-02:
fail-before-expiry and expiry-before-fail lock orders, reclaim-then-stale-fail,
concurrent identical and changed-body fails, and uncertain same-body replay.

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
from workhold.infrastructure.postgres.claim_repository import ClaimRepository
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
PAYLOAD_SENTINEL = "FAIL_EXPIRY_RACE_PAYLOAD_SHOULD_NEVER_LEAK"

PRODUCER_TOKEN = "tok-producer-fail-expiry-race"
WORKER_TOKEN = "tok-worker-fail-expiry-race"
ADMIN_TOKEN = "tok-admin-fail-expiry-race"

PRODUCER_PRINCIPAL = "producer-fail-expiry-race"
WORKER_PRINCIPAL = "worker-fail-expiry-race"
ADMIN_PRINCIPAL = "admin-fail-expiry-race"

FAILURE_CODE = "race.fail.boundary"
FAILURE_DETAIL_MAX = "é" + ("x" * 4095)  # exactly 4096 Unicode code points
assert len(FAILURE_DETAIL_MAX) == 4096

_OP_FAIL = 2
_OUTCOME_RETRY = 3
_OUTCOME_DEAD = 4
_OUTCOME_EXPIRED = 5
_RESULT_RETRY = 3
_RESULT_DEAD = 11
_TASK_DELAYED = 1
_TASK_READY = 2
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
    schema = f"failexprace_{uuid.uuid4().hex}"
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
        bind=ListenerBind(host="127.0.0.1", port=18197),
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


def _assert_one_closed_attempt_outcome(
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
    closed = [a for a in attempts if int(a.outcome_code) != 1]
    assert len(closed) == 1, f"expected one closed attempt, got {len(closed)}"
    assert int(closed[0].outcome_code) in allowed_outcomes
    return closed[0]


def _assert_single_fail_replay(session: Session, *, claim_id: UUID) -> CompleteReplay:
    replays = list(
        session.scalars(
            select(CompleteReplay).where(
                CompleteReplay.claim_id == claim_id,
                CompleteReplay.operation_code == _OP_FAIL,
            )
        )
    )
    assert len(replays) == 1
    return replays[0]


def _assert_no_duplicate_terminals(session: Session, *, task_id: UUID) -> None:
    terminals = list(
        session.scalars(select(TaskTerminal).where(TaskTerminal.task_id == task_id))
    )
    assert len(terminals) <= 1


@pytest.mark.parametrize("iteration", range(_RACE_ITERS))
def test_fail_first_expiry_observes_already_finalized(
    session_factory: sessionmaker[Session],
    iteration: int,
) -> None:
    """Fail holds after CURRENT fence; concurrent expiry sees already_finalized."""
    name = _unique(f"race.fail-first.{iteration}")
    setup = session_factory()
    try:
        _seed_queue(setup, name=name, enabled=True, max_attempts=3, retry_delay_seconds=0)
    finally:
        setup.close()

    app = _make_app(session_factory, queue_name=name)
    task_id = UUID(_enqueue_ready(app, queue_name=name))
    claimed = _claim_one_http(app, queue_name=name, worker_id="fail-owner")
    claim = claimed["claim"]
    claim_id = str(claim["claim_id"])
    claim_token = str(claim["claim_token"])
    generation = int(claim["generation"])

    fail_locked = threading.Event()
    expiry_started = threading.Event()
    results = {"fail": _ThreadResult(), "expiry": _ThreadResult()}
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
            assert expiry_started.wait(timeout=_JOIN_TIMEOUT_S), "expiry never started"
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
                    retryable=True,
                )
            results["fail"] = _ThreadResult(
                ok=True, value=payload, status=status, body=resp
            )
        except BaseException as exc:  # noqa: BLE001
            results["fail"] = _ThreadResult(ok=False, error=exc)

    def run_expiry() -> None:
        assert fail_locked.wait(timeout=_JOIN_TIMEOUT_S), "fail never locked"
        expiry_started.set()
        try:
            outcome = LeaseExpiryService(session_factory=session_factory).finalize_expired(
                task_id=task_id
            )
            results["expiry"] = _ThreadResult(ok=True, value=outcome)
        except BaseException as exc:  # noqa: BLE001
            results["expiry"] = _ThreadResult(ok=False, error=exc)

    t_fail = threading.Thread(target=run_fail, daemon=True)
    t_exp = threading.Thread(target=run_expiry, daemon=True)
    t_fail.start()
    t_exp.start()
    _join(t_fail, label="fail-first")
    _join(t_exp, label="expiry-during-fail")

    assert results["fail"].ok, results["fail"].error
    assert results["expiry"].ok, results["expiry"].error
    assert results["fail"].status == 200
    assert results["fail"].value["replayed"] is False
    assert results["fail"].value["state"] == "retry_scheduled"
    assert results["expiry"].value.state == "already_finalized"
    _assert_clean_text(
        results["fail"].body.decode("utf-8", errors="replace"),
        forbidden_tokens=(claim_token,),
    )

    verify = session_factory()
    try:
        closed = _assert_one_closed_attempt_outcome(
            verify, task_id=task_id, allowed_outcomes={_OUTCOME_RETRY}
        )
        assert closed.failure_code == FAILURE_CODE
        assert closed.failure_detail == FAILURE_DETAIL_MAX
        _assert_single_fail_replay(verify, claim_id=UUID(claim_id))
        _assert_no_duplicate_terminals(verify, task_id=task_id)
        task = verify.execute(
            select(TaskActive).where(TaskActive.task_id == task_id)
        ).scalar_one()
        assert int(task.state_code) in {_TASK_READY, _TASK_DELAYED}
        assert task.current_claim_id is None
        assert (
            verify.execute(
                select(ClaimRegistry).where(ClaimRegistry.claim_id == UUID(claim_id))
            ).scalar_one_or_none()
            is None
        )
    finally:
        verify.close()


@pytest.mark.parametrize("iteration", range(_RACE_ITERS))
def test_expiry_first_stale_fail_lease_lost(
    session_factory: sessionmaker[Session],
    iteration: int,
) -> None:
    """Expiry holds after task lock; concurrent stale fail returns lease_lost."""
    name = _unique(f"race.expiry-first.{iteration}")
    setup = session_factory()
    try:
        _seed_queue(
            setup, name=name, enabled=False, max_attempts=1, retry_delay_seconds=0
        )
    finally:
        setup.close()

    app = _make_app(session_factory, queue_name=name)
    task_id = UUID(_enqueue_ready(app, queue_name=name))
    claimed = _claim_one_http(app, queue_name=name, worker_id="stale-fail")
    claim = claimed["claim"]
    claim_id = str(claim["claim_id"])
    claim_token = str(claim["claim_token"])
    generation = int(claim["generation"])

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
                retryable=False,
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
    _join(t_exp, label="expiry-first")
    _join(t_fail, label="stale-fail-during-expiry")

    assert results["expiry"].ok, results["expiry"].error
    assert results["fail"].ok, results["fail"].error
    assert results["expiry"].value.state == "dead_lettered"
    _assert_error(
        results["fail"].status,
        results["fail"].value,
        results["fail"].body,
        code="lease_lost",
    )
    _assert_clean_text(
        results["fail"].body.decode("utf-8", errors="replace"),
        forbidden_tokens=(claim_token,),
    )

    verify = session_factory()
    try:
        closed = _assert_one_closed_attempt_outcome(
            verify, task_id=task_id, allowed_outcomes={_OUTCOME_EXPIRED}
        )
        assert closed.failure_code == "lease.expired"
        terminals = list(
            verify.scalars(select(TaskTerminal).where(TaskTerminal.task_id == task_id))
        )
        assert len(terminals) == 1
        assert int(terminals[0].state_code) == _RESULT_DEAD
        assert (
            verify.execute(
                select(CompleteReplay).where(CompleteReplay.claim_id == UUID(claim_id))
            ).scalar_one_or_none()
            is None
        )
        assert (
            verify.execute(
                select(TaskActive).where(TaskActive.task_id == task_id)
            ).scalar_one_or_none()
            is None
        )
    finally:
        verify.close()


@pytest.mark.parametrize("iteration", range(_RACE_ITERS))
def test_reclaim_after_expiry_stale_fail_unchanged(
    session_factory: sessionmaker[Session],
    iteration: int,
) -> None:
    """Force-expire, reclaim holds task row, stale fail cannot mutate Queue rows."""
    name = _unique(f"race.reclaim-stale-fail.{iteration}")
    setup = session_factory()
    try:
        _seed_queue(setup, name=name, enabled=True, max_attempts=3, retry_delay_seconds=0)
    finally:
        setup.close()

    app = _make_app(session_factory, queue_name=name)
    task_id = UUID(_enqueue_ready(app, queue_name=name))
    claimed = _claim_one_http(app, queue_name=name, worker_id="stale-owner")
    claim = claimed["claim"]
    old_claim_id = str(claim["claim_id"])
    old_token = str(claim["claim_token"])
    old_generation = int(claim["generation"])

    expire = session_factory()
    try:
        _expire_lease(expire, task_id=task_id, claim_id=UUID(old_claim_id))
    finally:
        expire.close()

    reclaim_locked = threading.Event()
    fail_started = threading.Event()
    results = {"reclaim": _ThreadResult(), "fail": _ThreadResult()}
    original_select = ClaimRepository._select_claimable_task

    def select_then_hold(
        self: ClaimRepository,
        session: Session,
        *,
        queue_pk: int,
    ) -> TaskActive | None:
        task = original_select(self, session, queue_pk=queue_pk)
        if task is not None:
            reclaim_locked.set()
            assert fail_started.wait(timeout=_JOIN_TIMEOUT_S), "fail never started"
        return task

    def run_reclaim() -> None:
        try:
            with patch.object(
                ClaimRepository, "_select_claimable_task", select_then_hold
            ):
                status, payload, resp = _http_claim(
                    app, queue_name=name, worker_id="reclaimer"
                )
            results["reclaim"] = _ThreadResult(
                ok=True, value=payload, status=status, body=resp
            )
        except BaseException as exc:  # noqa: BLE001
            results["reclaim"] = _ThreadResult(ok=False, error=exc)

    def run_fail() -> None:
        assert reclaim_locked.wait(timeout=_JOIN_TIMEOUT_S), "reclaim never locked"
        fail_started.set()
        try:
            status, payload, resp = _http_fail(
                app,
                claim_id=old_claim_id,
                claim_token=old_token,
                generation=old_generation,
                retryable=True,
            )
            results["fail"] = _ThreadResult(
                ok=True, value=payload, status=status, body=resp
            )
        except BaseException as exc:  # noqa: BLE001
            results["fail"] = _ThreadResult(ok=False, error=exc)

    t_reclaim = threading.Thread(target=run_reclaim, daemon=True)
    t_fail = threading.Thread(target=run_fail, daemon=True)
    t_reclaim.start()
    t_fail.start()
    _join(t_reclaim, label="reclaim-first")
    _join(t_fail, label="stale-fail-during-reclaim")

    assert results["reclaim"].ok, results["reclaim"].error
    assert results["fail"].ok, results["fail"].error
    assert results["reclaim"].status == 200
    assert len(results["reclaim"].value["tasks"]) == 1
    new_claim = results["reclaim"].value["tasks"][0]["claim"]
    assert int(new_claim["generation"]) == 2
    _assert_error(
        results["fail"].status,
        results["fail"].value,
        results["fail"].body,
        code="lease_lost",
    )
    _assert_clean_text(
        results["fail"].body.decode("utf-8", errors="replace"),
        forbidden_tokens=(old_token, str(new_claim["claim_token"])),
    )

    before = session_factory()
    try:
        attempts_before = list(
            before.scalars(
                select(TaskAttempt)
                .where(TaskAttempt.task_id == task_id)
                .order_by(TaskAttempt.generation)
            )
        )
        task_before = before.execute(
            select(TaskActive).where(TaskActive.task_id == task_id)
        ).scalar_one()
        snap = {
            "generation": int(task_before.generation),
            "claim_id": task_before.current_claim_id,
            "state": int(task_before.state_code),
            "attempt_outcomes": [int(a.outcome_code) for a in attempts_before],
            "attempt_failure_codes": [a.failure_code for a in attempts_before],
            "registry_count": before.scalar(
                select(func.count())
                .select_from(ClaimRegistry)
                .where(ClaimRegistry.task_id == task_id)
            ),
        }
        assert snap["generation"] == 2
        assert snap["state"] == _TASK_LEASED
        assert snap["attempt_outcomes"][0] == _OUTCOME_EXPIRED
        assert snap["attempt_outcomes"][1] == 1
    finally:
        before.close()

    for _ in range(2):
        status, payload, resp = _http_fail(
            app,
            claim_id=old_claim_id,
            claim_token=old_token,
            generation=old_generation,
            retryable=True,
        )
        _assert_error(status, payload, resp, code="lease_lost")

    after = session_factory()
    try:
        attempts_after = list(
            after.scalars(
                select(TaskAttempt)
                .where(TaskAttempt.task_id == task_id)
                .order_by(TaskAttempt.generation)
            )
        )
        task_after = after.execute(
            select(TaskActive).where(TaskActive.task_id == task_id)
        ).scalar_one()
        assert int(task_after.generation) == snap["generation"]
        assert task_after.current_claim_id == snap["claim_id"]
        assert int(task_after.state_code) == snap["state"]
        assert [int(a.outcome_code) for a in attempts_after] == snap["attempt_outcomes"]
        assert [a.failure_code for a in attempts_after] == snap["attempt_failure_codes"]
        assert (
            after.scalar(
                select(func.count())
                .select_from(ClaimRegistry)
                .where(ClaimRegistry.task_id == task_id)
            )
            == snap["registry_count"]
        )
        assert (
            after.execute(
                select(CompleteReplay).where(
                    CompleteReplay.claim_id == UUID(old_claim_id)
                )
            ).scalar_one_or_none()
            is None
        )
    finally:
        after.close()


@pytest.mark.parametrize("iteration", range(_RACE_ITERS))
def test_concurrent_identical_fails_one_transition(
    session_factory: sessionmaker[Session],
    iteration: int,
) -> None:
    """Two identical concurrent fails commit exactly one attempt/replay outcome."""
    name = _unique(f"race.dup-fail.{iteration}")
    setup = session_factory()
    try:
        _seed_queue(setup, name=name, enabled=True, max_attempts=3, retry_delay_seconds=45)
    finally:
        setup.close()

    app = _make_app(session_factory, queue_name=name)
    task_id = UUID(_enqueue_ready(app, queue_name=name))
    claimed = _claim_one_http(app, queue_name=name, worker_id="dup")
    claim = claimed["claim"]
    claim_id = str(claim["claim_id"])
    claim_token = str(claim["claim_token"])
    generation = int(claim["generation"])

    barrier = threading.Barrier(2, timeout=_JOIN_TIMEOUT_S)
    results: dict[str, _ThreadResult] = {
        "a": _ThreadResult(),
        "b": _ThreadResult(),
    }

    def run(label: str) -> None:
        try:
            barrier.wait()
            status, payload, resp = _http_fail(
                app,
                claim_id=claim_id,
                claim_token=claim_token,
                generation=generation,
                retryable=True,
            )
            results[label] = _ThreadResult(
                ok=True, value=payload, status=status, body=resp
            )
        except BaseException as exc:  # noqa: BLE001
            results[label] = _ThreadResult(ok=False, error=exc)

    t_a = threading.Thread(target=run, args=("a",), daemon=True)
    t_b = threading.Thread(target=run, args=("b",), daemon=True)
    t_a.start()
    t_b.start()
    _join(t_a, label="fail-a")
    _join(t_b, label="fail-b")

    assert results["a"].ok, results["a"].error
    assert results["b"].ok, results["b"].error

    successes = [
        r
        for r in (results["a"], results["b"])
        if r.status == 200 and r.value.get("state") == "retry_scheduled"
    ]
    assert len(successes) >= 1
    originals = [s for s in successes if s.value.get("replayed") is False]
    replays = [s for s in successes if s.value.get("replayed") is True]
    losers = [
        r
        for r in (results["a"], results["b"])
        if r.status != 200 or r.value.get("code") == "lease_lost"
    ]
    assert len(originals) == 1
    assert len(replays) + len(losers) == 1
    if losers:
        _assert_error(
            losers[0].status, losers[0].value, losers[0].body, code="lease_lost"
        )
    if replays:
        assert {k: v for k, v in replays[0].value.items() if k != "replayed"} == {
            k: v for k, v in originals[0].value.items() if k != "replayed"
        }

    for r in (results["a"], results["b"]):
        _assert_clean_text(
            r.body.decode("utf-8", errors="replace"),
            forbidden_tokens=(claim_token,),
        )

    verify = session_factory()
    try:
        closed = _assert_one_closed_attempt_outcome(
            verify, task_id=task_id, allowed_outcomes={_OUTCOME_RETRY}
        )
        assert closed.failure_code == FAILURE_CODE
        assert closed.failure_detail == FAILURE_DETAIL_MAX
        replay = _assert_single_fail_replay(verify, claim_id=UUID(claim_id))
        assert int(replay.result_state_code) == _RESULT_RETRY
        assert replay.available_at is not None
        task = verify.execute(
            select(TaskActive).where(TaskActive.task_id == task_id)
        ).scalar_one()
        assert task.available_at == replay.available_at
        _assert_no_duplicate_terminals(verify, task_id=task_id)
    finally:
        verify.close()


@pytest.mark.parametrize("iteration", range(_RACE_ITERS))
def test_concurrent_changed_body_fails_one_transition(
    session_factory: sessionmaker[Session],
    iteration: int,
) -> None:
    """Changed-body concurrent fails keep one transition; loser conflicts or loses lease."""
    name = _unique(f"race.changed-fail.{iteration}")
    setup = session_factory()
    try:
        _seed_queue(
            setup, name=name, enabled=False, max_attempts=1, retry_delay_seconds=0
        )
    finally:
        setup.close()

    app = _make_app(session_factory, queue_name=name)
    task_id = UUID(_enqueue_ready(app, queue_name=name))
    claimed = _claim_one_http(app, queue_name=name, worker_id="changed")
    claim = claimed["claim"]
    claim_id = str(claim["claim_id"])
    claim_token = str(claim["claim_token"])
    generation = int(claim["generation"])

    barrier = threading.Barrier(2, timeout=_JOIN_TIMEOUT_S)
    results: dict[str, _ThreadResult] = {
        "a": _ThreadResult(),
        "b": _ThreadResult(),
    }
    bodies = {
        "a": ("race.fail.alpha", FAILURE_DETAIL_MAX),
        "b": ("race.fail.beta", "different-detail"),
    }

    def run(label: str) -> None:
        code, detail = bodies[label]
        try:
            barrier.wait()
            status, payload, resp = _http_fail(
                app,
                claim_id=claim_id,
                claim_token=claim_token,
                generation=generation,
                retryable=False,
                failure_code=code,
                failure_detail=detail,
            )
            results[label] = _ThreadResult(
                ok=True, value=payload, status=status, body=resp
            )
        except BaseException as exc:  # noqa: BLE001
            results[label] = _ThreadResult(ok=False, error=exc)

    t_a = threading.Thread(target=run, args=("a",), daemon=True)
    t_b = threading.Thread(target=run, args=("b",), daemon=True)
    t_a.start()
    t_b.start()
    _join(t_a, label="changed-a")
    _join(t_b, label="changed-b")

    assert results["a"].ok, results["a"].error
    assert results["b"].ok, results["b"].error

    winners = [
        r
        for r in (results["a"], results["b"])
        if r.status == 200 and r.value.get("replayed") is False
    ]
    assert len(winners) == 1
    losers = [r for r in (results["a"], results["b"]) if r is not winners[0]]
    assert len(losers) == 1
    loser_code = losers[0].value.get("code")
    assert loser_code in {"lease_lost", "idempotency_conflict"}
    _assert_error(
        losers[0].status,
        losers[0].value,
        losers[0].body,
        code=loser_code,
    )

    verify = session_factory()
    try:
        closed = _assert_one_closed_attempt_outcome(
            verify, task_id=task_id, allowed_outcomes={_OUTCOME_DEAD}
        )
        assert closed.failure_code in {"race.fail.alpha", "race.fail.beta"}
        _assert_single_fail_replay(verify, claim_id=UUID(claim_id))
        terminals = list(
            verify.scalars(select(TaskTerminal).where(TaskTerminal.task_id == task_id))
        )
        assert len(terminals) == 1
    finally:
        verify.close()


def test_uncertain_fail_replay_returns_stored_result(
    session_factory: sessionmaker[Session],
) -> None:
    """Response-loss same-body replay is identical except replayed=true; changed body conflicts."""
    name = _unique("race.uncertain-replay")
    setup = session_factory()
    try:
        _seed_queue(setup, name=name, enabled=True, max_attempts=3, retry_delay_seconds=90)
    finally:
        setup.close()

    app = _make_app(session_factory, queue_name=name)
    task_id = UUID(_enqueue_ready(app, queue_name=name))
    claimed = _claim_one_http(app, queue_name=name, worker_id="replay")
    claim = claimed["claim"]
    claim_id = str(claim["claim_id"])
    claim_token = str(claim["claim_token"])
    generation = int(claim["generation"])

    status1, first, resp1 = _http_fail(
        app,
        claim_id=claim_id,
        claim_token=claim_token,
        generation=generation,
        retryable=True,
    )
    assert status1 == 200, _safe_status_detail(status1, resp1)
    assert first["replayed"] is False
    assert first["state"] == "retry_scheduled"
    assert "available_at" in first

    status2, second, resp2 = _http_fail(
        app,
        claim_id=claim_id,
        claim_token=claim_token,
        generation=generation,
        retryable=True,
    )
    assert status2 == 200, _safe_status_detail(status2, resp2)
    assert second["replayed"] is True
    assert {k: v for k, v in second.items() if k != "replayed"} == {
        k: v for k, v in first.items() if k != "replayed"
    }

    status3, third, resp3 = _http_fail(
        app,
        claim_id=claim_id,
        claim_token=claim_token,
        generation=generation,
        retryable=True,
        failure_code="race.fail.other",
        failure_detail="changed",
    )
    _assert_error(status3, third, resp3, code="idempotency_conflict")

    for resp, token in ((resp1, claim_token), (resp2, claim_token), (resp3, claim_token)):
        _assert_clean_text(
            resp.decode("utf-8", errors="replace"),
            forbidden_tokens=(token,),
        )

    verify = session_factory()
    try:
        closed = _assert_one_closed_attempt_outcome(
            verify, task_id=task_id, allowed_outcomes={_OUTCOME_RETRY}
        )
        assert closed.failure_code == FAILURE_CODE
        assert closed.failure_detail == FAILURE_DETAIL_MAX
        replay = _assert_single_fail_replay(verify, claim_id=UUID(claim_id))
        assert int(replay.result_state_code) == _RESULT_RETRY
        assert replay.available_at is not None
        task = verify.execute(
            select(TaskActive).where(TaskActive.task_id == task_id)
        ).scalar_one()
        assert task.available_at == replay.available_at
        # ISO response available_at matches Queue-store available_at.
        assert first["available_at"].startswith(
            task.available_at.astimezone(task.available_at.tzinfo).isoformat()[:19]
            if task.available_at.tzinfo is not None
            else str(task.available_at)[:19]
        )
        _assert_no_duplicate_terminals(verify, task_id=task_id)
    finally:
        verify.close()
