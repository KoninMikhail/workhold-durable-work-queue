"""Deterministic Complete-versus-cancel race qualification (Phase 03.7-05).

Covers COMP-01 / COMP-03 / QUAL-02:
leased cancel-request vs Complete lock orders, Complete winner replay,
loser retry stability, and the immediate-cancel terminal boundary.

Thread Events coordinate hold points at documented ``FOR UPDATE`` boundaries
(``TaskTransitionRepository.cancel_task`` and ``LeaseRepository.validate_current_lease``).
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
from sqlalchemy import create_engine, event, func, select
from sqlalchemy.engine import Engine
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
    CompletionEffect,
    DeliveryEventActive,
    Queue,
    QueueCounter,
    TaskActive,
    TaskAttempt,
    TaskTerminal,
)

_JOIN_TIMEOUT_S = 45.0
_RACE_ITERS = 25
_LEASE_SECONDS = 60
_ROOT = Path(__file__).resolve().parents[2]
_ALEMBIC_INI = _ROOT / "alembic.ini"
_SCHEMA_NAME_RE = re.compile(r"^[a-z_][a-z0-9_]*$")

CLAIM_PATH = "/v1/claims"
CLAIM_TOKEN_HEADER = "X-Queue-Claim-Token"
PAYLOAD_SENTINEL = "COMPLETE_CANCEL_RACE_PAYLOAD_SHOULD_NEVER_LEAK"

PRODUCER_TOKEN = "tok-producer-cc-race"
WORKER_TOKEN = "tok-worker-cc-race"
ADMIN_TOKEN = "tok-admin-cc-race"

PRODUCER_PRINCIPAL = "producer-cc-race"
WORKER_PRINCIPAL = "worker-cc-race"
ADMIN_PRINCIPAL = "admin-cc-race"

_OP_COMPLETE = 1
_OUTCOME_ACTIVE = 1
_OUTCOME_SUCCEEDED = 2
_RESULT_SUCCEEDED = 10
_RESULT_CANCELLED = 12
_TASK_LEASED = 3
_TASK_READY = 2
_EFFECT_KIND_SPAWN = 1


def _require_test_database_url() -> str:
    url = os.environ.get("TEST_DATABASE_URL", "").strip()
    if not url:
        pytest.fail(
            "TEST_DATABASE_URL is required for tests/concurrency "
            "(PostgreSQL). Refusing to skip or xfail."
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
    schema = f"ccrace_{uuid.uuid4().hex}"
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
    queue_names: tuple[str, ...],
) -> Any:
    scopes = frozenset(queue_names)
    authorizer = Authorizer(
        queue_scopes={
            PRODUCER_PRINCIPAL: scopes,
            WORKER_PRINCIPAL: scopes,
            ADMIN_PRINCIPAL: scopes,
        }
    )
    return create_application_app(
        authenticator=BearerCredentialAuthenticator.from_bindings(_bindings()),
        authorizer=authorizer,
        bind=ListenerBind(host="127.0.0.1", port=18207),
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


def _worker_headers(*, claim_token: str | None = None) -> dict[str, str]:
    headers = {
        "Authorization": f"Bearer {WORKER_TOKEN}",
        "Content-Type": "application/json",
    }
    if claim_token is not None:
        headers[CLAIM_TOKEN_HEADER] = claim_token
    return headers


def _http_claim(
    app: Any,
    *,
    queue_name: str,
    worker_id: str,
) -> tuple[int, dict[str, Any], bytes]:
    body = json.dumps(
        {
            "queues": [queue_name],
            "max_tasks": 1,
            "lease_seconds": _LEASE_SECONDS,
            "wait_seconds": 0,
            "worker_id": worker_id,
        },
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


def _complete_path(claim_id: str) -> str:
    return f"/v1/claims/{claim_id}:complete"


def _cancel_path(task_id: str) -> str:
    return f"/v1/tasks/{task_id}:cancel"


def _spawn_item(*, queue_name: str, payload: Any) -> dict[str, Any]:
    return {
        "queue_name": queue_name,
        "idempotency_key": f"spawn-{uuid.uuid4().hex}",
        "payload": payload,
        "priority": 0,
    }


def _complete_body(
    *,
    generation: int,
    spawn: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    return {"generation": generation, "spawn": [] if spawn is None else spawn}


def _http_complete(
    app: Any,
    *,
    claim_id: str,
    claim_token: str,
    generation: int,
    spawn: list[dict[str, Any]] | None = None,
) -> tuple[int, dict[str, Any], bytes]:
    body = json.dumps(
        _complete_body(generation=generation, spawn=spawn),
        separators=(",", ":"),
    ).encode("utf-8")
    status, _hdrs, resp = _asgi_http_call(
        app,
        method="POST",
        path=_complete_path(claim_id),
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


def _setup_leased_pair(
    session_factory: sessionmaker[Session],
    *,
    source_name: str,
    target_name: str,
) -> tuple[Any, UUID, dict[str, Any]]:
    setup = session_factory()
    try:
        _seed_queue(setup, name=source_name)
        _seed_queue(setup, name=target_name)
    finally:
        setup.close()
    app = _make_app(session_factory, queue_names=(source_name, target_name))
    task_id = UUID(_enqueue_ready(app, queue_name=source_name))
    claimed = _claim_one_http(app, queue_name=source_name, worker_id="cc-owner")
    return app, task_id, claimed


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
    assert task.current_claim_id == claim_id
    assert task.cancel_requested_at is not None
    assert (
        session.execute(
            select(ClaimRegistry).where(ClaimRegistry.claim_id == claim_id)
        ).scalar_one_or_none()
        is not None
    )
    attempts = list(
        session.scalars(
            select(TaskAttempt).where(TaskAttempt.task_id == task_id)
        )
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
            select(CompleteReplay).where(CompleteReplay.claim_id == claim_id)
        ).scalar_one_or_none()
        is None
    )
    assert (
        session.execute(
            select(func.count())
            .select_from(CompletionEffect)
            .where(CompletionEffect.source_claim_id == claim_id)
        ).scalar_one()
        == 0
    )
    assert (
        session.execute(
            select(func.count())
            .select_from(TaskActive)
            .where(TaskActive.source_task_id == task_id)
        ).scalar_one()
        == 0
    )
    assert (
        session.execute(
            select(func.count())
            .select_from(DeliveryEventActive)
        ).scalar_one()
        == 0
    )


def _assert_succeeded_with_spawns(
    session: Session,
    *,
    task_id: UUID,
    claim_id: UUID,
    spawned_task_ids: list[str],
) -> None:
    assert (
        session.execute(
            select(TaskActive).where(TaskActive.task_id == task_id)
        ).scalar_one_or_none()
        is None
    )
    terminal = session.execute(
        select(TaskTerminal).where(TaskTerminal.task_id == task_id)
    ).scalar_one()
    assert int(terminal.state_code) == _RESULT_SUCCEEDED
    attempts = list(
        session.scalars(
            select(TaskAttempt).where(TaskAttempt.task_id == task_id)
        )
    )
    closed = [a for a in attempts if int(a.outcome_code) != _OUTCOME_ACTIVE]
    assert len(closed) == 1
    assert int(closed[0].outcome_code) == _OUTCOME_SUCCEEDED
    replay = session.execute(
        select(CompleteReplay).where(
            CompleteReplay.claim_id == claim_id,
            CompleteReplay.operation_code == _OP_COMPLETE,
        )
    ).scalar_one()
    assert list(replay.spawned_task_ids) == [UUID(x) for x in spawned_task_ids]
    assert list(replay.event_ids or []) == []
    effects = list(
        session.scalars(
            select(CompletionEffect)
            .where(
                CompletionEffect.source_claim_id == claim_id,
                CompletionEffect.effect_kind_code == _EFFECT_KIND_SPAWN,
            )
            .order_by(CompletionEffect.ordinal)
        )
    )
    assert len(effects) == len(spawned_task_ids)
    for ordinal, (effect, spawn_id) in enumerate(zip(effects, spawned_task_ids, strict=True)):
        assert int(effect.ordinal) == ordinal
        assert effect.resource_id == UUID(spawn_id)
        child = session.execute(
            select(TaskActive).where(TaskActive.task_id == UUID(spawn_id))
        ).scalar_one()
        assert child.source_task_id == task_id
        assert int(child.spawn_ordinal) == ordinal
        assert int(child.state_code) == _TASK_READY
    assert (
        session.execute(
            select(ClaimRegistry).where(ClaimRegistry.claim_id == claim_id)
        ).scalar_one_or_none()
        is None
    )
    assert (
        session.execute(
            select(func.count())
            .select_from(DeliveryEventActive)
        ).scalar_one()
        == 0
    )


# ---------------------------------------------------------------------------
# Barrier-controlled cancel-request vs Complete
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("iteration", range(_RACE_ITERS))
def test_cancel_first_complete_observes_cancel_race_lost(
    session_factory: sessionmaker[Session],
    iteration: int,
) -> None:
    """Cancel holds after TaskActive FOR UPDATE; Complete then sees cancel_race_lost."""
    source = _unique(f"race.cancel-first.src.{iteration}")
    target = _unique(f"race.cancel-first.tgt.{iteration}")
    app, task_id, claimed = _setup_leased_pair(
        session_factory, source_name=source, target_name=target
    )
    claim = claimed["claim"]
    claim_id = str(claim["claim_id"])
    claim_token = str(claim["claim_token"])
    generation = int(claim["generation"])
    spawn = [
        _spawn_item(
            queue_name=target,
            payload={"secret": PAYLOAD_SENTINEL, "spawn": 1},
        )
    ]

    cancel_locked = threading.Event()
    complete_started = threading.Event()
    results = {"cancel": _ThreadResult(), "complete": _ThreadResult()}
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
        assert complete_started.wait(timeout=_JOIN_TIMEOUT_S), "complete never started"
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

    def run_complete() -> None:
        assert cancel_locked.wait(timeout=_JOIN_TIMEOUT_S), "cancel never locked"
        complete_started.set()
        try:
            status, payload, resp = _http_complete(
                app,
                claim_id=claim_id,
                claim_token=claim_token,
                generation=generation,
                spawn=spawn,
            )
            results["complete"] = _ThreadResult(
                ok=True, value=payload, status=status, body=resp
            )
        except BaseException as exc:  # noqa: BLE001
            results["complete"] = _ThreadResult(ok=False, error=exc)

    t_cancel = threading.Thread(target=run_cancel, daemon=True)
    t_complete = threading.Thread(target=run_complete, daemon=True)
    t_cancel.start()
    t_complete.start()
    _join(t_cancel, label="cancel-first")
    _join(t_complete, label="complete-during-cancel")

    assert results["cancel"].ok, results["cancel"].error
    assert results["complete"].ok, results["complete"].error
    assert results["cancel"].status == 200
    assert results["cancel"].value["task"]["state"] == "leased"
    assert results["cancel"].value["task"]["current_claim"]["cancel_requested"] is True
    _assert_error(
        results["complete"].status,
        results["complete"].value,
        results["complete"].body,
        code="cancel_race_lost",
    )

    verify = session_factory()
    try:
        _assert_request_only_leased(
            verify, task_id=task_id, claim_id=UUID(claim_id)
        )
        queue = verify.execute(select(Queue).where(Queue.name == source)).scalar_one()
        counter = verify.get(QueueCounter, int(queue.id))
        assert counter is not None
        assert int(counter.leased_count) == 1
    finally:
        verify.close()

    # Loser retry cannot change the winner (still request-only leased).
    status_r, payload_r, resp_r = _http_complete(
        app,
        claim_id=claim_id,
        claim_token=claim_token,
        generation=generation,
        spawn=spawn,
    )
    _assert_error(status_r, payload_r, resp_r, code="cancel_race_lost")
    after = session_factory()
    try:
        _assert_request_only_leased(after, task_id=task_id, claim_id=UUID(claim_id))
    finally:
        after.close()


@pytest.mark.parametrize("iteration", range(_RACE_ITERS))
def test_complete_first_cancel_preserves_succeeded_and_spawns(
    session_factory: sessionmaker[Session],
    iteration: int,
) -> None:
    """Complete holds after CURRENT fence; cancel then sees task_already_terminal."""
    source = _unique(f"race.complete-first.src.{iteration}")
    target = _unique(f"race.complete-first.tgt.{iteration}")
    app, task_id, claimed = _setup_leased_pair(
        session_factory, source_name=source, target_name=target
    )
    claim = claimed["claim"]
    claim_id = str(claim["claim_id"])
    claim_token = str(claim["claim_token"])
    generation = int(claim["generation"])
    spawn = [
        _spawn_item(
            queue_name=target,
            payload={"secret": PAYLOAD_SENTINEL, "spawn": 1},
        ),
        _spawn_item(
            queue_name=target,
            payload={"secret": PAYLOAD_SENTINEL, "spawn": 2},
        ),
    ]

    complete_locked = threading.Event()
    cancel_started = threading.Event()
    results = {"complete": _ThreadResult(), "cancel": _ThreadResult()}
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
            complete_locked.set()
            assert cancel_started.wait(timeout=_JOIN_TIMEOUT_S), "cancel never started"
        return result

    def run_complete() -> None:
        try:
            with patch.object(
                LeaseRepository, "validate_current_lease", validate_then_hold
            ):
                status, payload, resp = _http_complete(
                    app,
                    claim_id=claim_id,
                    claim_token=claim_token,
                    generation=generation,
                    spawn=spawn,
                )
            results["complete"] = _ThreadResult(
                ok=True, value=payload, status=status, body=resp
            )
        except BaseException as exc:  # noqa: BLE001
            results["complete"] = _ThreadResult(ok=False, error=exc)

    def run_cancel() -> None:
        assert complete_locked.wait(timeout=_JOIN_TIMEOUT_S), "complete never locked"
        cancel_started.set()
        try:
            status, payload, resp = _http_cancel(app, task_id=str(task_id))
            results["cancel"] = _ThreadResult(
                ok=True, value=payload, status=status, body=resp
            )
        except BaseException as exc:  # noqa: BLE001
            results["cancel"] = _ThreadResult(ok=False, error=exc)

    t_complete = threading.Thread(target=run_complete, daemon=True)
    t_cancel = threading.Thread(target=run_cancel, daemon=True)
    t_complete.start()
    t_cancel.start()
    _join(t_complete, label="complete-first")
    _join(t_cancel, label="cancel-during-complete")

    assert results["complete"].ok, results["complete"].error
    assert results["cancel"].ok, results["cancel"].error
    assert results["complete"].status == 200
    assert results["complete"].value["state"] == "succeeded"
    assert results["complete"].value["replayed"] is False
    spawned = list(results["complete"].value["spawned_task_ids"])
    assert len(spawned) == 2
    _assert_error(
        results["cancel"].status,
        results["cancel"].value,
        results["cancel"].body,
        code="task_already_terminal",
    )

    verify = session_factory()
    try:
        _assert_succeeded_with_spawns(
            verify,
            task_id=task_id,
            claim_id=UUID(claim_id),
            spawned_task_ids=spawned,
        )
        source_q = verify.execute(select(Queue).where(Queue.name == source)).scalar_one()
        counter = verify.get(QueueCounter, int(source_q.id))
        assert counter is not None
        assert int(counter.leased_count) == 0
    finally:
        verify.close()

    # Winner Complete replay returns stored success; cancel loser stays stable.
    status_replay, payload_replay, resp_replay = _http_complete(
        app,
        claim_id=claim_id,
        claim_token=claim_token,
        generation=generation,
        spawn=spawn,
    )
    assert status_replay == 200, _safe_status_detail(status_replay, resp_replay)
    assert payload_replay["state"] == "succeeded"
    assert payload_replay["replayed"] is True
    assert payload_replay["spawned_task_ids"] == spawned
    assert payload_replay["task_id"] == results["complete"].value["task_id"]

    status_c2, payload_c2, resp_c2 = _http_cancel(app, task_id=str(task_id))
    _assert_error(status_c2, payload_c2, resp_c2, code="task_already_terminal")

    after = session_factory()
    try:
        _assert_succeeded_with_spawns(
            after,
            task_id=task_id,
            claim_id=UUID(claim_id),
            spawned_task_ids=spawned,
        )
        terminals = list(
            after.scalars(select(TaskTerminal).where(TaskTerminal.task_id == task_id))
        )
        assert len(terminals) == 1
    finally:
        after.close()


# ---------------------------------------------------------------------------
# Immediate-cancel boundary (ready) — Phase 3.6 state machine preserved
# ---------------------------------------------------------------------------


def test_immediate_ready_cancel_blocks_later_complete(
    session_factory: sessionmaker[Session],
) -> None:
    """Ready cancel terminals immediately; a later Complete cannot invent success."""
    source = _unique("seq.immediate-ready.src")
    target = _unique("seq.immediate-ready.tgt")
    setup = session_factory()
    try:
        _seed_queue(setup, name=source)
        _seed_queue(setup, name=target)
    finally:
        setup.close()
    app = _make_app(session_factory, queue_names=(source, target))
    task_id = UUID(_enqueue_ready(app, queue_name=source))

    status_c, payload_c, resp_c = _http_cancel(app, task_id=str(task_id))
    assert status_c == 200, _safe_status_detail(status_c, resp_c)
    assert payload_c["task"]["state"] == "cancelled"
    assert "current_claim" not in payload_c["task"]

    verify = session_factory()
    try:
        assert (
            verify.execute(
                select(TaskActive).where(TaskActive.task_id == task_id)
            ).scalar_one_or_none()
            is None
        )
        terminal = verify.execute(
            select(TaskTerminal).where(TaskTerminal.task_id == task_id)
        ).scalar_one()
        assert int(terminal.state_code) == _RESULT_CANCELLED
        assert (
            verify.execute(
                select(func.count())
                .select_from(TaskActive)
                .where(TaskActive.source_task_id == task_id)
            ).scalar_one()
            == 0
        )
        assert (
            verify.execute(
                select(func.count())
                .select_from(CompleteReplay)
                .where(CompleteReplay.task_id == task_id)
            ).scalar_one()
            == 0
        )
    finally:
        verify.close()

    # No live claim exists after immediate cancel; Complete must not succeed.
    fake_claim = str(uuid.uuid4())
    fake_token = str(uuid.uuid4())
    status_f, payload_f, resp_f = _http_complete(
        app,
        claim_id=fake_claim,
        claim_token=fake_token,
        generation=1,
        spawn=[
            _spawn_item(
                queue_name=target,
                payload={"secret": PAYLOAD_SENTINEL},
            )
        ],
    )
    _assert_error(status_f, payload_f, resp_f, code="claim_not_found")

    after = session_factory()
    try:
        terminals = list(
            after.scalars(select(TaskTerminal).where(TaskTerminal.task_id == task_id))
        )
        assert len(terminals) == 1
        assert int(terminals[0].state_code) == _RESULT_CANCELLED
        assert (
            after.execute(
                select(func.count())
                .select_from(TaskActive)
                .where(TaskActive.source_task_id == task_id)
            ).scalar_one()
            == 0
        )
        assert (
            after.execute(
                select(func.count())
                .select_from(CompleteReplay)
                .where(CompleteReplay.task_id == task_id)
            ).scalar_one()
            == 0
        )
    finally:
        after.close()
