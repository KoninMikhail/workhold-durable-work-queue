"""Deterministic real-PostgreSQL claim/heartbeat race qualification (Phase 03.5-04).

Covers WORK-03 / WORK-04 / WORK-07 / WORK-09 / OPS-04 / API-01 / API-08 / QUAL-02:
32-way claim exclusivity, pause/drain versus claim in both lock orders, heartbeat
versus reclaim in both lock orders, and Queue-store time as lease authority.

Thread Events coordinate hold points at documented ``FOR UPDATE`` boundaries.
Wall-clock sleeps are never the race oracle.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import threading
import uuid
from collections.abc import Iterator, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
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

from queue_service.api.application import create_application_app
from queue_service.api.security import ListenerBind
from queue_service.application.claim_service import ClaimService
from queue_service.application.lease_service import LeaseService
from queue_service.domain.queue_control import (
    AdminRequestMetadata,
    BackoffStrategy,
    ConfigVersion,
    CreateQueueMutation,
    QueueState,
    RetryPolicyDraft,
    SetQueueStateMutation,
)
from queue_service.infrastructure.postgres.claim_repository import ClaimRepository
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
from queue_service.storage.models import (
    AdminAuditLog,
    ClaimRegistry,
    Queue,
    QueueCounter,
    TaskActive,
    TaskAttempt,
)

_JOIN_TIMEOUT_S = 45.0
_AUDIT_OP_SET_STATE = 4
_STATE_PAUSED = 2
_STATE_DRAINING = 3
_TASK_READY = 2
_TASK_LEASED = 3
_OUTCOME_ACTIVE = 1
_OUTCOME_EXPIRED = 5
_RACE_ITERS = 20
_CLAIMER_COUNT = 32
_LEASE_SECONDS = 60
_ROOT = Path(__file__).resolve().parents[2]
_ALEMBIC_INI = _ROOT / "alembic.ini"
_SCHEMA_NAME_RE = re.compile(r"^[a-z_][a-z0-9_]*$")

CLAIM_PATH = "/v1/claims"
CLAIM_TOKEN_HEADER = "X-Queue-Claim-Token"
PAYLOAD_SENTINEL = "CLAIM_HB_RACE_PAYLOAD_SHOULD_NEVER_LEAK"

PRODUCER_TOKEN = "tok-producer-claim-hb-race"
WORKER_TOKEN = "tok-worker-claim-hb-race"
ADMIN_TOKEN = "tok-admin-claim-hb-race"

PRODUCER_PRINCIPAL = "producer-claim-hb-race"
WORKER_PRINCIPAL = "worker-claim-hb-race"
ADMIN_PRINCIPAL = "admin-claim-hb-race"


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
    schema = f"claimhbrace_{uuid.uuid4().hex}"
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
        bind=ListenerBind(host="127.0.0.1", port=18194),
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


def _claim_body(
    *,
    queues: list[str],
    worker_id: str,
    lease_seconds: int = _LEASE_SECONDS,
) -> dict[str, Any]:
    return {
        "queues": queues,
        "max_tasks": 1,
        "lease_seconds": lease_seconds,
        "wait_seconds": 0,
        "worker_id": worker_id,
    }


def _worker_headers(*, claim_token: str | None = None) -> dict[str, str]:
    headers = {
        "Authorization": f"Bearer {WORKER_TOKEN}",
        "Content-Type": "application/json",
    }
    if claim_token is not None:
        headers[CLAIM_TOKEN_HEADER] = claim_token
    return headers


def _heartbeat_path(claim_id: str) -> str:
    return f"/v1/claims/{claim_id}:heartbeat"


def _http_claim(
    app: Any,
    *,
    queue_name: str,
    worker_id: str,
    lease_seconds: int = _LEASE_SECONDS,
) -> tuple[int, dict[str, Any], bytes]:
    body = json.dumps(
        _claim_body(
            queues=[queue_name], worker_id=worker_id, lease_seconds=lease_seconds
        ),
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


def _http_heartbeat(
    app: Any,
    *,
    claim_id: str,
    claim_token: str,
    generation: int,
    lease_seconds: int = 30,
) -> tuple[int, dict[str, Any], bytes]:
    body = json.dumps(
        {"generation": generation, "lease_seconds": lease_seconds},
        separators=(",", ":"),
    ).encode("utf-8")
    status, _hdrs, resp = _asgi_http_call(
        app,
        method="POST",
        path=_heartbeat_path(claim_id),
        headers=_worker_headers(claim_token=claim_token),
        body=body,
    )
    payload = json.loads(resp.decode("utf-8")) if resp else {}
    return status, payload, resp


def _assert_lease_lost(status: int, payload: dict[str, Any], resp: bytes) -> None:
    assert status == 409, _safe_status_detail(status, resp)
    assert payload.get("code") == "lease_lost"
    assert payload.get("retryable") is False
    _assert_clean_text(resp.decode("utf-8", errors="replace"))


def _set_queue_state(
    session: Session,
    *,
    queue_name: str,
    state: QueueState,
    expected_config_version: int,
    actor_id: str,
) -> None:
    QueueControlRepository().set_queue_state(
        session,
        queue_name=queue_name,
        mutation=SetQueueStateMutation(
            expected_config_version=ConfigVersion(value=expected_config_version),
            state=state,
            metadata=_meta(actor_id=actor_id),
        ),
    )
    session.commit()


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


def _snapshot_lease_rows(session: Session, *, task_id: UUID) -> dict[str, Any]:
    task = session.execute(
        select(TaskActive).where(TaskActive.task_id == task_id)
    ).scalar_one()
    registries = list(
        session.scalars(select(ClaimRegistry).where(ClaimRegistry.task_id == task_id))
    )
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
        "registry_count": len(registries),
        "registry_claim_ids": [r.claim_id for r in registries],
        "registry_generations": [int(r.generation) for r in registries],
        "registry_claimed_at": [r.claimed_at for r in registries],
        "registry_lease_expires_at": [r.lease_expires_at for r in registries],
        "attempt_count": len(attempts),
        "attempt_claim_ids": [a.claim_id for a in attempts],
        "attempt_outcomes": [int(a.outcome_code) for a in attempts],
        "attempt_ended_at": [a.ended_at for a in attempts],
    }


def _assert_single_lease(
    session: Session,
    *,
    task_id: UUID,
    claim_id: UUID,
    generation: int,
    worker_id: str,
) -> None:
    task = session.execute(
        select(TaskActive).where(TaskActive.task_id == task_id)
    ).scalar_one()
    assert int(task.state_code) == _TASK_LEASED
    assert task.current_claim_id == claim_id
    assert int(task.generation) == generation
    assert task.worker_id == worker_id

    registries = list(
        session.scalars(select(ClaimRegistry).where(ClaimRegistry.task_id == task_id))
    )
    assert len(registries) == 1
    assert registries[0].claim_id == claim_id
    assert int(registries[0].generation) == generation

    active_attempts = list(
        session.scalars(
            select(TaskAttempt).where(
                TaskAttempt.task_id == task_id,
                TaskAttempt.outcome_code == _OUTCOME_ACTIVE,
            )
        )
    )
    assert len(active_attempts) == 1
    assert active_attempts[0].claim_id == claim_id
    assert int(active_attempts[0].generation) == generation
    assert active_attempts[0].worker_id == worker_id
    assert active_attempts[0].ended_at is None


def _assert_counters_ready_to_leased(
    session: Session,
    *,
    queue_pk: int,
    expected_ready: int,
    expected_leased: int,
) -> None:
    counter = session.get(QueueCounter, queue_pk)
    assert counter is not None
    assert int(counter.ready_count) == expected_ready
    assert int(counter.leased_count) == expected_leased


def _assert_state_transition_audit(
    session: Session,
    *,
    queue_pk: int,
    state_code: int,
    previous_config_version: int = 1,
    new_config_version: int = 2,
) -> None:
    queue = session.get(Queue, queue_pk)
    assert queue is not None
    assert int(queue.state_code) == state_code
    assert int(queue.config_version) == new_config_version

    audits = list(
        session.scalars(
            select(AdminAuditLog).where(
                AdminAuditLog.queue_id == queue_pk,
                AdminAuditLog.operation_code == _AUDIT_OP_SET_STATE,
            )
        )
    )
    assert len(audits) == 1
    assert audits[0].previous_config_version == previous_config_version
    assert audits[0].new_config_version == new_config_version


def _claim_one_http(
    app: Any,
    *,
    queue_name: str,
    worker_id: str,
) -> dict[str, Any]:
    status, payload, resp = _http_claim(
        app, queue_name=queue_name, worker_id=worker_id
    )
    assert status == 200, _safe_status_detail(status, resp)
    assert len(payload["tasks"]) == 1
    return payload["tasks"][0]


def test_thirty_two_claimers_yield_one_lease(
    session_factory: sessionmaker[Session],
) -> None:
    name = _unique("race.32claimers")
    setup = session_factory()
    try:
        queue = _seed_queue(setup, name=name)
        queue_pk = int(queue.id)
    finally:
        setup.close()

    app = _make_app(session_factory, queue_name=name)
    task_id = UUID(_enqueue_ready(app, queue_name=name))

    barrier = threading.Barrier(_CLAIMER_COUNT, timeout=_JOIN_TIMEOUT_S)
    results: list[_ThreadResult] = [_ThreadResult() for _ in range(_CLAIMER_COUNT)]

    def run_claimer(index: int) -> None:
        try:
            barrier.wait()
            status, payload, resp = _http_claim(
                app,
                queue_name=name,
                worker_id=f"w{index:02d}",
            )
            results[index] = _ThreadResult(
                ok=True, value=payload, status=status, body=resp
            )
        except BaseException as exc:  # noqa: BLE001
            results[index] = _ThreadResult(ok=False, error=exc)

    threads = [
        threading.Thread(target=run_claimer, args=(i,), daemon=True)
        for i in range(_CLAIMER_COUNT)
    ]
    for t in threads:
        t.start()
    for i, t in enumerate(threads):
        _join(t, label=f"claimer-{i}")

    for i, r in enumerate(results):
        assert r.ok, f"claimer {i} failed: {type(r.error).__name__}: {r.error}"
        assert r.status == 200

    winners = [r for r in results if r.value and len(r.value.get("tasks", [])) == 1]
    empties = [r for r in results if r.value and r.value.get("tasks") == []]
    assert len(winners) == 1, f"expected 1 winner got {len(winners)}"
    assert len(empties) == _CLAIMER_COUNT - 1

    claimed = winners[0].value["tasks"][0]
    claim = claimed["claim"]
    claim_id = UUID(claim["claim_id"])
    claim_token = str(claim["claim_token"])
    generation = int(claim["generation"])
    worker_id = str(claim["worker_id"])
    assert UUID(claimed["task"]["task_id"]) == task_id
    assert generation == 1
    assert worker_id.startswith(f"{WORKER_PRINCIPAL}/")

    for r in empties:
        _assert_clean_text(
            r.body.decode("utf-8", errors="replace"),
            forbidden_tokens=(claim_token,),
        )

    verify = session_factory()
    try:
        _assert_single_lease(
            verify,
            task_id=task_id,
            claim_id=claim_id,
            generation=1,
            worker_id=worker_id,
        )
        _assert_counters_ready_to_leased(
            verify, queue_pk=queue_pk, expected_ready=0, expected_leased=1
        )
        assert (
            verify.scalar(
                select(func.count())
                .select_from(ClaimRegistry)
                .where(ClaimRegistry.task_id == task_id)
            )
            == 1
        )
        assert (
            verify.scalar(
                select(func.count())
                .select_from(TaskAttempt)
                .where(TaskAttempt.task_id == task_id)
            )
            == 1
        )
    finally:
        verify.close()


@pytest.mark.parametrize("iteration", range(_RACE_ITERS))
def test_pause_first_claim_empty(
    session_factory: sessionmaker[Session],
    iteration: int,
) -> None:
    name = _unique(f"race.pause-first.{iteration}")
    setup = session_factory()
    try:
        queue = _seed_queue(setup, name=name)
        queue_pk = int(queue.id)
    finally:
        setup.close()

    app = _make_app(session_factory, queue_name=name)
    task_id = UUID(_enqueue_ready(app, queue_name=name))

    pause_locked = threading.Event()
    claim_started = threading.Event()
    results = {"pause": _ThreadResult(), "claim": _ThreadResult()}
    original_lock = QueueControlRepository._lock_queue_by_name

    def control_lock_then_hold(
        self: QueueControlRepository,
        session: Session,
        queue_name: str,
    ) -> Queue:
        locked = original_lock(self, session, queue_name)
        pause_locked.set()
        assert claim_started.wait(timeout=_JOIN_TIMEOUT_S), "claim never started"
        return locked

    def run_pause() -> None:
        session = session_factory()
        try:
            with patch.object(
                QueueControlRepository,
                "_lock_queue_by_name",
                control_lock_then_hold,
            ):
                _set_queue_state(
                    session,
                    queue_name=name,
                    state=QueueState.PAUSED,
                    expected_config_version=1,
                    actor_id="pause-first",
                )
            results["pause"] = _ThreadResult(ok=True, value=True)
        except BaseException as exc:  # noqa: BLE001
            session.rollback()
            results["pause"] = _ThreadResult(ok=False, error=exc)
        finally:
            session.close()

    def run_claim() -> None:
        assert pause_locked.wait(timeout=_JOIN_TIMEOUT_S), "pause never locked"
        claim_started.set()
        try:
            status, payload, resp = _http_claim(
                app, queue_name=name, worker_id="after-pause"
            )
            results["claim"] = _ThreadResult(
                ok=True, value=payload, status=status, body=resp
            )
        except BaseException as exc:  # noqa: BLE001
            results["claim"] = _ThreadResult(ok=False, error=exc)

    t_pause = threading.Thread(target=run_pause, daemon=True)
    t_claim = threading.Thread(target=run_claim, daemon=True)
    t_pause.start()
    t_claim.start()
    _join(t_pause, label="pause-first")
    _join(t_claim, label="claim-after-pause")

    assert results["pause"].ok, results["pause"].error
    assert results["claim"].ok, results["claim"].error
    assert results["claim"].status == 200
    assert results["claim"].value["tasks"] == []
    assert results["claim"].value["queue_states"][name] == "paused"
    _assert_clean_text(results["claim"].body.decode("utf-8", errors="replace"))

    verify = session_factory()
    try:
        _assert_state_transition_audit(
            verify, queue_pk=queue_pk, state_code=_STATE_PAUSED
        )
        task = verify.execute(
            select(TaskActive).where(TaskActive.task_id == task_id)
        ).scalar_one()
        assert int(task.state_code) == _TASK_READY
        assert task.current_claim_id is None
        assert (
            verify.scalar(
                select(func.count())
                .select_from(ClaimRegistry)
                .where(ClaimRegistry.task_id == task_id)
            )
            == 0
        )
        _assert_counters_ready_to_leased(
            verify, queue_pk=queue_pk, expected_ready=1, expected_leased=0
        )
    finally:
        verify.close()


@pytest.mark.parametrize("iteration", range(_RACE_ITERS))
def test_claim_first_pause_keeps_lease(
    session_factory: sessionmaker[Session],
    iteration: int,
) -> None:
    name = _unique(f"race.claim-first-pause.{iteration}")
    setup = session_factory()
    try:
        queue = _seed_queue(setup, name=name)
        queue_pk = int(queue.id)
    finally:
        setup.close()

    app = _make_app(session_factory, queue_name=name)
    task_id = UUID(_enqueue_ready(app, queue_name=name))

    claim_locked = threading.Event()
    pause_started = threading.Event()
    results = {"claim": _ThreadResult(), "pause": _ThreadResult()}
    original_lock = ClaimRepository.lock_named_queue
    replica_id = "claim-first"
    worker_id = f"{WORKER_PRINCIPAL}/{replica_id}"

    def lock_then_hold(
        self: ClaimRepository,
        session: Session,
        queue_name: str,
    ) -> Queue:
        locked = original_lock(self, session, queue_name)
        claim_locked.set()
        assert pause_started.wait(timeout=_JOIN_TIMEOUT_S), "pause never started"
        return locked

    def run_claim() -> None:
        try:
            with patch.object(ClaimRepository, "lock_named_queue", lock_then_hold):
                status, payload, resp = _http_claim(
                    app, queue_name=name, worker_id=replica_id
                )
            results["claim"] = _ThreadResult(
                ok=True, value=payload, status=status, body=resp
            )
        except BaseException as exc:  # noqa: BLE001
            results["claim"] = _ThreadResult(ok=False, error=exc)

    def run_pause() -> None:
        assert claim_locked.wait(timeout=_JOIN_TIMEOUT_S), "claim never locked"
        session = session_factory()
        pause_started.set()
        try:
            _set_queue_state(
                session,
                queue_name=name,
                state=QueueState.PAUSED,
                expected_config_version=1,
                actor_id="pause-after-claim",
            )
            results["pause"] = _ThreadResult(ok=True, value=True)
        except BaseException as exc:  # noqa: BLE001
            session.rollback()
            results["pause"] = _ThreadResult(ok=False, error=exc)
        finally:
            session.close()

    t_claim = threading.Thread(target=run_claim, daemon=True)
    t_pause = threading.Thread(target=run_pause, daemon=True)
    t_claim.start()
    t_pause.start()
    _join(t_claim, label="claim-first")
    _join(t_pause, label="pause-second")

    assert results["claim"].ok, results["claim"].error
    assert results["pause"].ok, results["pause"].error
    assert results["claim"].status == 200
    assert len(results["claim"].value["tasks"]) == 1
    claimed = results["claim"].value["tasks"][0]
    claim = claimed["claim"]
    claim_id = UUID(claim["claim_id"])
    claim_token = str(claim["claim_token"])
    assert UUID(claimed["task"]["task_id"]) == task_id
    assert int(claim["generation"]) == 1
    assert claim["worker_id"] == worker_id

    verify = session_factory()
    try:
        _assert_state_transition_audit(
            verify, queue_pk=queue_pk, state_code=_STATE_PAUSED
        )
        _assert_single_lease(
            verify,
            task_id=task_id,
            claim_id=claim_id,
            generation=1,
            worker_id=worker_id,
        )
        _assert_counters_ready_to_leased(
            verify, queue_pk=queue_pk, expected_ready=0, expected_leased=1
        )
        registry = verify.execute(
            select(ClaimRegistry).where(ClaimRegistry.claim_id == claim_id)
        ).scalar_one()
        assert str(registry.claim_token) == claim_token
    finally:
        verify.close()


@pytest.mark.parametrize("iteration", range(_RACE_ITERS))
def test_drain_first_claim_succeeds(
    session_factory: sessionmaker[Session],
    iteration: int,
) -> None:
    name = _unique(f"race.drain-first.{iteration}")
    setup = session_factory()
    try:
        queue = _seed_queue(setup, name=name)
        queue_pk = int(queue.id)
    finally:
        setup.close()

    app = _make_app(session_factory, queue_name=name)
    task_id = UUID(_enqueue_ready(app, queue_name=name))

    drain_locked = threading.Event()
    claim_started = threading.Event()
    results = {"drain": _ThreadResult(), "claim": _ThreadResult()}
    original_lock = QueueControlRepository._lock_queue_by_name
    replica_id = "drain-first-claim"
    worker_id = f"{WORKER_PRINCIPAL}/{replica_id}"

    def control_lock_then_hold(
        self: QueueControlRepository,
        session: Session,
        queue_name: str,
    ) -> Queue:
        locked = original_lock(self, session, queue_name)
        drain_locked.set()
        assert claim_started.wait(timeout=_JOIN_TIMEOUT_S), "claim never started"
        return locked

    def run_drain() -> None:
        session = session_factory()
        try:
            with patch.object(
                QueueControlRepository,
                "_lock_queue_by_name",
                control_lock_then_hold,
            ):
                _set_queue_state(
                    session,
                    queue_name=name,
                    state=QueueState.DRAINING,
                    expected_config_version=1,
                    actor_id="drain-first",
                )
            results["drain"] = _ThreadResult(ok=True, value=True)
        except BaseException as exc:  # noqa: BLE001
            session.rollback()
            results["drain"] = _ThreadResult(ok=False, error=exc)
        finally:
            session.close()

    def run_claim() -> None:
        assert drain_locked.wait(timeout=_JOIN_TIMEOUT_S), "drain never locked"
        claim_started.set()
        try:
            status, payload, resp = _http_claim(
                app, queue_name=name, worker_id=replica_id
            )
            results["claim"] = _ThreadResult(
                ok=True, value=payload, status=status, body=resp
            )
        except BaseException as exc:  # noqa: BLE001
            results["claim"] = _ThreadResult(ok=False, error=exc)

    t_drain = threading.Thread(target=run_drain, daemon=True)
    t_claim = threading.Thread(target=run_claim, daemon=True)
    t_drain.start()
    t_claim.start()
    _join(t_drain, label="drain-first")
    _join(t_claim, label="claim-after-drain")

    assert results["drain"].ok, results["drain"].error
    assert results["claim"].ok, results["claim"].error
    assert results["claim"].status == 200
    assert len(results["claim"].value["tasks"]) == 1
    claimed = results["claim"].value["tasks"][0]
    claim = claimed["claim"]
    claim_id = UUID(claim["claim_id"])
    assert UUID(claimed["task"]["task_id"]) == task_id
    assert results["claim"].value["queue_states"][name] == "draining"

    verify = session_factory()
    try:
        _assert_state_transition_audit(
            verify, queue_pk=queue_pk, state_code=_STATE_DRAINING
        )
        _assert_single_lease(
            verify,
            task_id=task_id,
            claim_id=claim_id,
            generation=1,
            worker_id=worker_id,
        )
        _assert_counters_ready_to_leased(
            verify, queue_pk=queue_pk, expected_ready=0, expected_leased=1
        )
    finally:
        verify.close()


@pytest.mark.parametrize("iteration", range(_RACE_ITERS))
def test_claim_first_drain_waits_keeps_lease(
    session_factory: sessionmaker[Session],
    iteration: int,
) -> None:
    name = _unique(f"race.claim-first-drain.{iteration}")
    setup = session_factory()
    try:
        queue = _seed_queue(setup, name=name)
        queue_pk = int(queue.id)
    finally:
        setup.close()

    app = _make_app(session_factory, queue_name=name)
    task_id = UUID(_enqueue_ready(app, queue_name=name))

    claim_locked = threading.Event()
    drain_started = threading.Event()
    results = {"claim": _ThreadResult(), "drain": _ThreadResult()}
    original_lock = ClaimRepository.lock_named_queue
    replica_id = "claim-first-drain"
    worker_id = f"{WORKER_PRINCIPAL}/{replica_id}"

    def lock_then_hold(
        self: ClaimRepository,
        session: Session,
        queue_name: str,
    ) -> Queue:
        locked = original_lock(self, session, queue_name)
        claim_locked.set()
        assert drain_started.wait(timeout=_JOIN_TIMEOUT_S), "drain never started"
        return locked

    def run_claim() -> None:
        try:
            with patch.object(ClaimRepository, "lock_named_queue", lock_then_hold):
                status, payload, resp = _http_claim(
                    app, queue_name=name, worker_id=replica_id
                )
            results["claim"] = _ThreadResult(
                ok=True, value=payload, status=status, body=resp
            )
        except BaseException as exc:  # noqa: BLE001
            results["claim"] = _ThreadResult(ok=False, error=exc)

    def run_drain() -> None:
        assert claim_locked.wait(timeout=_JOIN_TIMEOUT_S), "claim never locked"
        session = session_factory()
        drain_started.set()
        try:
            _set_queue_state(
                session,
                queue_name=name,
                state=QueueState.DRAINING,
                expected_config_version=1,
                actor_id="drain-after-claim",
            )
            results["drain"] = _ThreadResult(ok=True, value=True)
        except BaseException as exc:  # noqa: BLE001
            session.rollback()
            results["drain"] = _ThreadResult(ok=False, error=exc)
        finally:
            session.close()

    t_claim = threading.Thread(target=run_claim, daemon=True)
    t_drain = threading.Thread(target=run_drain, daemon=True)
    t_claim.start()
    t_drain.start()
    _join(t_claim, label="claim-first-drain")
    _join(t_drain, label="drain-second")

    assert results["claim"].ok, results["claim"].error
    assert results["drain"].ok, results["drain"].error
    assert results["claim"].status == 200
    assert len(results["claim"].value["tasks"]) == 1
    claimed = results["claim"].value["tasks"][0]
    claim = claimed["claim"]
    claim_id = UUID(claim["claim_id"])
    claim_token = str(claim["claim_token"])
    generation = int(claim["generation"])

    verify = session_factory()
    try:
        _assert_state_transition_audit(
            verify, queue_pk=queue_pk, state_code=_STATE_DRAINING
        )
        _assert_single_lease(
            verify,
            task_id=task_id,
            claim_id=claim_id,
            generation=generation,
            worker_id=worker_id,
        )
        _assert_counters_ready_to_leased(
            verify, queue_pk=queue_pk, expected_ready=0, expected_leased=1
        )
        registry = verify.execute(
            select(ClaimRegistry).where(ClaimRegistry.claim_id == claim_id)
        ).scalar_one()
        assert str(registry.claim_token) == claim_token
        task = verify.execute(
            select(TaskActive).where(TaskActive.task_id == task_id)
        ).scalar_one()
        assert task.current_claim_id == claim_id
        assert task.lease_expires_at == registry.lease_expires_at
    finally:
        verify.close()


@pytest.mark.parametrize("iteration", range(_RACE_ITERS))
def test_heartbeat_first_reclaim_claim_empty(
    session_factory: sessionmaker[Session],
    iteration: int,
) -> None:
    """Valid lease: HB holds after fence; concurrent claim observes empty."""
    name = _unique(f"race.hb-first.{iteration}")
    setup = session_factory()
    try:
        _seed_queue(setup, name=name)
    finally:
        setup.close()

    app = _make_app(session_factory, queue_name=name)
    task_id = UUID(_enqueue_ready(app, queue_name=name))
    claimed = _claim_one_http(app, queue_name=name, worker_id="owner")
    claim = claimed["claim"]
    claim_id = str(claim["claim_id"])
    claim_token = str(claim["claim_token"])
    generation = int(claim["generation"])
    owner_worker_id = f"{WORKER_PRINCIPAL}/owner"

    hb_locked = threading.Event()
    claim_started = threading.Event()
    results = {"heartbeat": _ThreadResult(), "claim": _ThreadResult()}
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
            hb_locked.set()
            assert claim_started.wait(timeout=_JOIN_TIMEOUT_S), "claim never started"
        return result

    def run_heartbeat() -> None:
        try:
            with patch.object(
                LeaseRepository, "validate_current_lease", validate_then_hold
            ):
                status, payload, resp = _http_heartbeat(
                    app,
                    claim_id=claim_id,
                    claim_token=claim_token,
                    generation=generation,
                    lease_seconds=45,
                )
            results["heartbeat"] = _ThreadResult(
                ok=True, value=payload, status=status, body=resp
            )
        except BaseException as exc:  # noqa: BLE001
            results["heartbeat"] = _ThreadResult(ok=False, error=exc)

    def run_claim() -> None:
        assert hb_locked.wait(timeout=_JOIN_TIMEOUT_S), "heartbeat never locked"
        claim_started.set()
        try:
            status, payload, resp = _http_claim(
                app, queue_name=name, worker_id="concurrent"
            )
            results["claim"] = _ThreadResult(
                ok=True, value=payload, status=status, body=resp
            )
        except BaseException as exc:  # noqa: BLE001
            results["claim"] = _ThreadResult(ok=False, error=exc)

    t_hb = threading.Thread(target=run_heartbeat, daemon=True)
    t_claim = threading.Thread(target=run_claim, daemon=True)
    t_hb.start()
    t_claim.start()
    _join(t_hb, label="heartbeat-first")
    _join(t_claim, label="claim-during-hb")

    assert results["heartbeat"].ok, results["heartbeat"].error
    assert results["claim"].ok, results["claim"].error
    assert results["heartbeat"].status == 200
    assert results["claim"].status == 200
    assert results["claim"].value["tasks"] == []
    _assert_clean_text(
        results["claim"].body.decode("utf-8", errors="replace"),
        forbidden_tokens=(claim_token,),
    )

    verify = session_factory()
    try:
        _assert_single_lease(
            verify,
            task_id=task_id,
            claim_id=UUID(claim_id),
            generation=generation,
            worker_id=owner_worker_id,
        )
        snap = _snapshot_lease_rows(verify, task_id=task_id)
        assert snap["attempt_count"] == 1
        assert snap["registry_count"] == 1
    finally:
        verify.close()


@pytest.mark.parametrize("iteration", range(_RACE_ITERS))
def test_reclaim_first_stale_heartbeat_and_probe(
    session_factory: sessionmaker[Session],
    iteration: int,
) -> None:
    """Expire via Queue-store time; reclaim holds task row; stale HB + probe."""
    name = _unique(f"race.reclaim-first.{iteration}")
    setup = session_factory()
    try:
        queue = _seed_queue(setup, name=name)
        queue_pk = int(queue.id)
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
    hb_started = threading.Event()
    results = {"reclaim": _ThreadResult(), "heartbeat": _ThreadResult()}
    original_select = ClaimRepository._select_claimable_task
    reclaimer_replica = "reclaimer"
    reclaimer_id = f"{WORKER_PRINCIPAL}/{reclaimer_replica}"

    def select_then_hold(
        self: ClaimRepository,
        session: Session,
        *,
        queue_pk: int,
    ) -> TaskActive | None:
        task = original_select(self, session, queue_pk=queue_pk)
        if task is not None:
            reclaim_locked.set()
            assert hb_started.wait(timeout=_JOIN_TIMEOUT_S), "heartbeat never started"
        return task

    def run_reclaim() -> None:
        try:
            with patch.object(
                ClaimRepository, "_select_claimable_task", select_then_hold
            ):
                status, payload, resp = _http_claim(
                    app, queue_name=name, worker_id=reclaimer_replica
                )
            results["reclaim"] = _ThreadResult(
                ok=True, value=payload, status=status, body=resp
            )
        except BaseException as exc:  # noqa: BLE001
            results["reclaim"] = _ThreadResult(ok=False, error=exc)

    def run_heartbeat() -> None:
        assert reclaim_locked.wait(timeout=_JOIN_TIMEOUT_S), "reclaim never locked"
        hb_started.set()
        try:
            status, payload, resp = _http_heartbeat(
                app,
                claim_id=old_claim_id,
                claim_token=old_token,
                generation=old_generation,
            )
            results["heartbeat"] = _ThreadResult(
                ok=True, value=payload, status=status, body=resp
            )
        except BaseException as exc:  # noqa: BLE001
            results["heartbeat"] = _ThreadResult(ok=False, error=exc)

    t_reclaim = threading.Thread(target=run_reclaim, daemon=True)
    t_hb = threading.Thread(target=run_heartbeat, daemon=True)
    t_reclaim.start()
    t_hb.start()
    _join(t_reclaim, label="reclaim-first")
    _join(t_hb, label="stale-heartbeat")

    assert results["reclaim"].ok, results["reclaim"].error
    assert results["heartbeat"].ok, results["heartbeat"].error
    assert results["reclaim"].status == 200
    assert len(results["reclaim"].value["tasks"]) == 1
    new_claim = results["reclaim"].value["tasks"][0]["claim"]
    assert int(new_claim["generation"]) == 2
    new_claim_id = UUID(new_claim["claim_id"])
    _assert_lease_lost(
        results["heartbeat"].status,
        results["heartbeat"].value,
        results["heartbeat"].body,
    )
    _assert_clean_text(
        results["heartbeat"].body.decode("utf-8", errors="replace"),
        forbidden_tokens=(old_token, str(new_claim["claim_token"])),
    )

    before = session_factory()
    try:
        snap_before = _snapshot_lease_rows(before, task_id=task_id)
    finally:
        before.close()

    for _ in range(2):
        status, payload, resp = _http_heartbeat(
            app,
            claim_id=old_claim_id,
            claim_token=old_token,
            generation=old_generation,
        )
        _assert_lease_lost(status, payload, resp)
        _assert_clean_text(
            resp.decode("utf-8", errors="replace"),
            forbidden_tokens=(old_token,),
        )

    after = session_factory()
    try:
        snap_after = _snapshot_lease_rows(after, task_id=task_id)
        assert snap_after == snap_before
        _assert_single_lease(
            after,
            task_id=task_id,
            claim_id=new_claim_id,
            generation=2,
            worker_id=reclaimer_id,
        )
        attempts = list(
            after.scalars(
                select(TaskAttempt)
                .where(TaskAttempt.task_id == task_id)
                .order_by(TaskAttempt.generation)
            )
        )
        assert len(attempts) == 2
        assert int(attempts[0].outcome_code) == _OUTCOME_EXPIRED
        assert attempts[0].ended_at is not None
        assert int(attempts[1].outcome_code) == _OUTCOME_ACTIVE
        _assert_counters_ready_to_leased(
            after, queue_pk=queue_pk, expected_ready=0, expected_leased=1
        )
    finally:
        after.close()

    probe = LeaseService(session_factory=session_factory).probe_current_lease(
        claim_id=UUID(old_claim_id),
        claim_token=UUID(old_token),
        generation=old_generation,
    )
    assert probe.decision is FenceDecision.STALE

    final = session_factory()
    try:
        assert _snapshot_lease_rows(final, task_id=task_id) == snap_before
    finally:
        final.close()


def test_queue_store_time_governs_expiry(
    session_factory: sessionmaker[Session],
    caplog: pytest.LogCaptureFixture,
) -> None:
    name = _unique("race.store-time")
    setup = session_factory()
    try:
        _seed_queue(setup, name=name)
    finally:
        setup.close()

    app = _make_app(session_factory, queue_name=name)
    task_id = UUID(_enqueue_ready(app, queue_name=name))
    claimed = _claim_one_http(app, queue_name=name, worker_id="clock")
    claim = claimed["claim"]
    claim_id = UUID(claim["claim_id"])
    claim_token = str(claim["claim_token"])
    generation = int(claim["generation"])

    worker_future = datetime(2099, 1, 1, tzinfo=UTC)
    assert worker_future.year == 2099

    expire = session_factory()
    try:
        db_now = expire.scalar(select(func.transaction_timestamp()))
        assert db_now is not None
        assert db_now.year < 2099
        _expire_lease(expire, task_id=task_id, claim_id=claim_id)
        expired_row = expire.execute(
            select(TaskActive).where(TaskActive.task_id == task_id)
        ).scalar_one()
        store_now = expire.scalar(select(func.transaction_timestamp()))
        assert store_now is not None
        assert expired_row.lease_expires_at < store_now
    finally:
        expire.close()

    before = session_factory()
    try:
        snap_before = _snapshot_lease_rows(before, task_id=task_id)
    finally:
        before.close()

    with caplog.at_level(logging.DEBUG):
        status, payload, resp = _http_heartbeat(
            app,
            claim_id=str(claim_id),
            claim_token=claim_token,
            generation=generation,
            lease_seconds=120,
        )
    _assert_lease_lost(status, payload, resp)
    _assert_clean_text(caplog.text, forbidden_tokens=(claim_token,))
    _assert_clean_text(
        resp.decode("utf-8", errors="replace"),
        forbidden_tokens=(claim_token,),
    )
    assert PAYLOAD_SENTINEL not in caplog.text

    after = session_factory()
    try:
        assert _snapshot_lease_rows(after, task_id=task_id) == snap_before
        task = after.execute(
            select(TaskActive).where(TaskActive.task_id == task_id)
        ).scalar_one()
        assert task.current_claim_id == claim_id
        assert int(task.generation) == generation
    finally:
        after.close()
