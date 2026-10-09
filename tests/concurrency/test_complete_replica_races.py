"""Deterministic Complete replica-race qualification (Phase 03.7-06).

Covers COMP-01 / COMP-03 / API-03 / API-04 / QUAL-02:
same-body two-completer, different-Complete-body fingerprint conflict,
fail/cancel terminal winners before Complete, pre-commit abort recovery,
rollback injection at write boundaries, and final lineage inspection.

Thread Events / Barriers coordinate hold points at documented ``FOR UPDATE``
boundaries. Wall-clock sleeps are never the race oracle.
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

from queue_service.api.application import create_application_app
from queue_service.api.schemas.terminal import parse_complete_command
from queue_service.api.security import ListenerBind
from queue_service.application.claim_service import ClaimService
from queue_service.application.completion import CompletionFaultHooks, CompletionService
from queue_service.application.lease_service import LeaseService
from queue_service.application.worker_terminal import WorkerTerminalService
from queue_service.domain.queue_control import (
    AdminRequestMetadata,
    BackoffStrategy,
    CreateQueueMutation,
    RetryPolicyDraft,
)
from queue_service.infrastructure.postgres.lease_repository import LeaseRepository
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
from queue_service.storage.leases import FenceDecision
from queue_service.storage.models import (
    ClaimRegistry,
    CompleteReplay,
    CompletionEffect,
    DeliveryEventActive,
    Queue,
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
PAYLOAD_SENTINEL = "COMPLETE_REPLICA_RACE_PAYLOAD_SHOULD_NEVER_LEAK"

PRODUCER_TOKEN = "tok-producer-cr-race"
WORKER_TOKEN = "tok-worker-cr-race"
ADMIN_TOKEN = "tok-admin-cr-race"

PRODUCER_PRINCIPAL = "producer-cr-race"
WORKER_PRINCIPAL = "worker-cr-race"
ADMIN_PRINCIPAL = "admin-cr-race"

_OP_COMPLETE = 1
_OUTCOME_ACTIVE = 1
_OUTCOME_SUCCEEDED = 2
_RESULT_SUCCEEDED = 10
_RESULT_DEAD = 11
_TASK_READY = 2
_TASK_LEASED = 3
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
    schema = f"crrace_{uuid.uuid4().hex}"
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
    completion_service: CompletionService | None = None,
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
        bind=ListenerBind(host="127.0.0.1", port=18217),
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
        completion_service=completion_service
        or CompletionService(session_factory=session_factory),
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


def _fail_path(claim_id: str) -> str:
    return f"/v1/claims/{claim_id}:fail"


def _cancel_path(task_id: str) -> str:
    return f"/v1/tasks/{task_id}:cancel"


def _spawn_item(*, queue_name: str, payload: Any, key: str | None = None) -> dict[str, Any]:
    return {
        "queue_name": queue_name,
        "idempotency_key": key or f"spawn-{uuid.uuid4().hex}",
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


def _http_fail(
    app: Any,
    *,
    claim_id: str,
    claim_token: str,
    generation: int,
) -> tuple[int, dict[str, Any], bytes]:
    body = json.dumps(
        {
            "generation": generation,
            "retryable": False,
            "failure_code": "replica.fail.winner",
            "failure_detail": "terminal-before-complete",
        },
        separators=(",", ":"),
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


def _http_get_task(app: Any, *, task_id: str) -> tuple[int, dict[str, Any], bytes]:
    status, _hdrs, resp = _asgi_http_call(
        app,
        method="GET",
        path=f"/v1/tasks/{task_id}",
        headers={"Authorization": f"Bearer {PRODUCER_TOKEN}"},
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
    completion_service: CompletionService | None = None,
) -> tuple[Any, UUID, dict[str, Any]]:
    setup = session_factory()
    try:
        _seed_queue(setup, name=source_name)
        _seed_queue(setup, name=target_name)
    finally:
        setup.close()
    app = _make_app(
        session_factory,
        queue_names=(source_name, target_name),
        completion_service=completion_service,
    )
    task_id = UUID(_enqueue_ready(app, queue_name=source_name))
    claimed = _claim_one_http(app, queue_name=source_name, worker_id="cr-owner")
    return app, task_id, claimed


def _assert_succeeded_lineage(
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
        session.scalars(select(TaskAttempt).where(TaskAttempt.task_id == task_id))
    )
    closed = [a for a in attempts if int(a.outcome_code) != _OUTCOME_ACTIVE]
    assert len(closed) == 1
    assert int(closed[0].outcome_code) == _OUTCOME_SUCCEEDED
    assert closed[0].claim_id == claim_id
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
    for ordinal, (effect, spawn_id) in enumerate(
        zip(effects, spawned_task_ids, strict=True)
    ):
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
            select(func.count())
            .select_from(CompletionEffect)
            .where(CompletionEffect.source_claim_id == claim_id)
        ).scalar_one()
        == len(spawned_task_ids)
    )
    assert (
        session.execute(
            select(ClaimRegistry).where(ClaimRegistry.claim_id == claim_id)
        ).scalar_one_or_none()
        is None
    )
    assert (
        session.execute(
            select(func.count()).select_from(DeliveryEventActive)
        ).scalar_one()
        == 0
    )


# ---------------------------------------------------------------------------
# Same-body / different-body two-completer races
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("iteration", range(_RACE_ITERS))
def test_same_body_two_completers_one_commit_one_replay(
    session_factory: sessionmaker[Session],
    iteration: int,
) -> None:
    """Two replicas with identical Complete bodies: one write + one replay."""
    source = _unique(f"race.same-body.src.{iteration}")
    target = _unique(f"race.same-body.tgt.{iteration}")
    app, task_id, claimed = _setup_leased_pair(
        session_factory, source_name=source, target_name=target
    )
    claim = claimed["claim"]
    claim_id = str(claim["claim_id"])
    claim_token = str(claim["claim_token"])
    generation = int(claim["generation"])
    spawn_key = f"spawn-same-{iteration}"
    spawn = [
        _spawn_item(
            queue_name=target,
            payload={"secret": PAYLOAD_SENTINEL, "spawn": 1},
            key=spawn_key,
        ),
        _spawn_item(
            queue_name=target,
            payload={"secret": PAYLOAD_SENTINEL, "spawn": 2},
            key=f"{spawn_key}-b",
        ),
    ]

    barrier = threading.Barrier(2, timeout=_JOIN_TIMEOUT_S)
    results: dict[str, _ThreadResult] = {"a": _ThreadResult(), "b": _ThreadResult()}

    def run(label: str) -> None:
        try:
            barrier.wait()
            status, payload, resp = _http_complete(
                app,
                claim_id=claim_id,
                claim_token=claim_token,
                generation=generation,
                spawn=spawn,
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
    _join(t_a, label="complete-a")
    _join(t_b, label="complete-b")

    assert results["a"].ok, results["a"].error
    assert results["b"].ok, results["b"].error

    successes = [
        r
        for r in (results["a"], results["b"])
        if r.status == 200 and r.value.get("state") == "succeeded"
    ]
    assert len(successes) == 2
    originals = [s for s in successes if s.value.get("replayed") is False]
    replays = [s for s in successes if s.value.get("replayed") is True]
    assert len(originals) == 1
    assert len(replays) == 1
    assert {k: v for k, v in replays[0].value.items() if k != "replayed"} == {
        k: v for k, v in originals[0].value.items() if k != "replayed"
    }
    spawned = list(originals[0].value["spawned_task_ids"])
    assert len(spawned) == 2

    for r in (results["a"], results["b"]):
        _assert_clean_text(
            r.body.decode("utf-8", errors="replace"),
            forbidden_tokens=(claim_token,),
        )

    verify = session_factory()
    try:
        _assert_succeeded_lineage(
            verify,
            task_id=task_id,
            claim_id=UUID(claim_id),
            spawned_task_ids=spawned,
        )
    finally:
        verify.close()

    status_i, inspected, resp_i = _http_get_task(app, task_id=str(task_id))
    assert status_i == 200, _safe_status_detail(status_i, resp_i)
    assert inspected["state"] == "succeeded"
    assert inspected["spawned_task_ids"] == spawned
    _assert_clean_text(resp_i.decode("utf-8", errors="replace"), forbidden_tokens=(claim_token,))


@pytest.mark.parametrize("iteration", range(_RACE_ITERS))
def test_different_body_two_completers_exact_idempotency_conflict(
    session_factory: sessionmaker[Session],
    iteration: int,
) -> None:
    """Different Complete fingerprints: winner commits; loser gets idempotency_conflict."""
    source = _unique(f"race.diff-body.src.{iteration}")
    target = _unique(f"race.diff-body.tgt.{iteration}")
    app, task_id, claimed = _setup_leased_pair(
        session_factory, source_name=source, target_name=target
    )
    claim = claimed["claim"]
    claim_id = str(claim["claim_id"])
    claim_token = str(claim["claim_token"])
    generation = int(claim["generation"])
    bodies = {
        "a": [
            _spawn_item(
                queue_name=target,
                payload={"secret": PAYLOAD_SENTINEL, "variant": "a"},
                key=f"spawn-a-{iteration}",
            )
        ],
        "b": [
            _spawn_item(
                queue_name=target,
                payload={"secret": PAYLOAD_SENTINEL, "variant": "b"},
                key=f"spawn-b-{iteration}",
            )
        ],
    }

    # Hold the first CURRENT fence until the second Completer is waiting on FOR UPDATE.
    first_locked = threading.Event()
    second_started = threading.Event()
    results: dict[str, _ThreadResult] = {"a": _ThreadResult(), "b": _ThreadResult()}
    original_validate = LeaseRepository.validate_current_lease
    hold_once = {"done": False}
    hold_lock = threading.Lock()

    def validate_then_maybe_hold(
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
        with hold_lock:
            should_hold = (
                not hold_once["done"]
                and result.decision is FenceDecision.CURRENT
            )
            if should_hold:
                hold_once["done"] = True
        if should_hold:
            first_locked.set()
            assert second_started.wait(timeout=_JOIN_TIMEOUT_S), "second never started"
        return result

    def run_a() -> None:
        try:
            with patch.object(
                LeaseRepository, "validate_current_lease", validate_then_maybe_hold
            ):
                status, payload, resp = _http_complete(
                    app,
                    claim_id=claim_id,
                    claim_token=claim_token,
                    generation=generation,
                    spawn=bodies["a"],
                )
            results["a"] = _ThreadResult(
                ok=True, value=payload, status=status, body=resp
            )
        except BaseException as exc:  # noqa: BLE001
            results["a"] = _ThreadResult(ok=False, error=exc)

    def run_b() -> None:
        assert first_locked.wait(timeout=_JOIN_TIMEOUT_S), "first never locked"
        second_started.set()
        try:
            status, payload, resp = _http_complete(
                app,
                claim_id=claim_id,
                claim_token=claim_token,
                generation=generation,
                spawn=bodies["b"],
            )
            results["b"] = _ThreadResult(
                ok=True, value=payload, status=status, body=resp
            )
        except BaseException as exc:  # noqa: BLE001
            results["b"] = _ThreadResult(ok=False, error=exc)

    t_a = threading.Thread(target=run_a, daemon=True)
    t_b = threading.Thread(target=run_b, daemon=True)
    t_a.start()
    t_b.start()
    _join(t_a, label="diff-a")
    _join(t_b, label="diff-b")

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
    # Exact contract: Complete-vs-Complete fingerprint mismatch is never a
    # terminal-race code (task_already_terminal / cancel_race_lost / lease_lost).
    _assert_error(
        losers[0].status,
        losers[0].value,
        losers[0].body,
        code="idempotency_conflict",
    )
    assert losers[0].value.get("code") not in {
        "task_already_terminal",
        "cancel_race_lost",
        "lease_lost",
    }

    spawned = list(winners[0].value["spawned_task_ids"])
    assert len(spawned) == 1

    verify = session_factory()
    try:
        _assert_succeeded_lineage(
            verify,
            task_id=task_id,
            claim_id=UUID(claim_id),
            spawned_task_ids=spawned,
        )
        # Only the winning fingerprint's descendant exists.
        assert (
            verify.execute(
                select(func.count())
                .select_from(TaskActive)
                .where(TaskActive.source_task_id == task_id)
            ).scalar_one()
            == 1
        )
    finally:
        verify.close()


# ---------------------------------------------------------------------------
# Distinct terminal command won before Complete
# ---------------------------------------------------------------------------


def test_fail_first_complete_observes_task_already_terminal(
    session_factory: sessionmaker[Session],
) -> None:
    source = _unique("race.fail-first.src")
    target = _unique("race.fail-first.tgt")
    app, task_id, claimed = _setup_leased_pair(
        session_factory, source_name=source, target_name=target
    )
    claim = claimed["claim"]
    claim_id = str(claim["claim_id"])
    claim_token = str(claim["claim_token"])
    generation = int(claim["generation"])

    status_f, fail_body, resp_f = _http_fail(
        app, claim_id=claim_id, claim_token=claim_token, generation=generation
    )
    assert status_f == 200, _safe_status_detail(status_f, resp_f)
    assert fail_body.get("state") == "dead_lettered"

    status_c, complete_body, resp_c = _http_complete(
        app,
        claim_id=claim_id,
        claim_token=claim_token,
        generation=generation,
        spawn=[
            _spawn_item(
                queue_name=target,
                payload={"secret": PAYLOAD_SENTINEL, "late": True},
            )
        ],
    )
    _assert_error(
        status_c, complete_body, resp_c, code="task_already_terminal"
    )

    verify = session_factory()
    try:
        terminal = verify.execute(
            select(TaskTerminal).where(TaskTerminal.task_id == task_id)
        ).scalar_one()
        assert int(terminal.state_code) == _RESULT_DEAD
        assert (
            verify.execute(
                select(func.count())
                .select_from(CompletionEffect)
                .where(CompletionEffect.source_claim_id == UUID(claim_id))
            ).scalar_one()
            == 0
        )
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
                select(CompleteReplay).where(
                    CompleteReplay.claim_id == UUID(claim_id),
                    CompleteReplay.operation_code == _OP_COMPLETE,
                )
            ).scalar_one_or_none()
            is None
        )
    finally:
        verify.close()


def test_cancel_first_complete_observes_cancel_race_lost(
    session_factory: sessionmaker[Session],
) -> None:
    source = _unique("race.cancel-first.src")
    target = _unique("race.cancel-first.tgt")
    app, task_id, claimed = _setup_leased_pair(
        session_factory, source_name=source, target_name=target
    )
    claim = claimed["claim"]
    claim_id = str(claim["claim_id"])
    claim_token = str(claim["claim_token"])
    generation = int(claim["generation"])

    status_x, cancel_body, resp_x = _http_cancel(app, task_id=str(task_id))
    assert status_x == 200, _safe_status_detail(status_x, resp_x)
    task_view = cancel_body.get("task") or {}
    assert (
        cancel_body.get("state") in {"cancel_requested", "cancelled"}
        or task_view.get("state") in {"leased", "cancelled"}
        or (task_view.get("current_claim") or {}).get("cancel_requested") is True
    )

    status_c, complete_body, resp_c = _http_complete(
        app,
        claim_id=claim_id,
        claim_token=claim_token,
        generation=generation,
        spawn=[
            _spawn_item(
                queue_name=target,
                payload={"secret": PAYLOAD_SENTINEL, "late": True},
            )
        ],
    )
    _assert_error(status_c, complete_body, resp_c, code="cancel_race_lost")

    verify = session_factory()
    try:
        assert (
            verify.execute(
                select(func.count())
                .select_from(CompletionEffect)
                .where(CompletionEffect.source_claim_id == UUID(claim_id))
            ).scalar_one()
            == 0
        )
        assert (
            verify.execute(
                select(CompleteReplay).where(
                    CompleteReplay.claim_id == UUID(claim_id),
                    CompleteReplay.operation_code == _OP_COMPLETE,
                )
            ).scalar_one_or_none()
            is None
        )
    finally:
        verify.close()


# ---------------------------------------------------------------------------
# Pre-commit abort + boundary rollback injection
# ---------------------------------------------------------------------------


def test_pre_commit_abort_leaves_no_effects_then_complete_succeeds(
    session_factory: sessionmaker[Session],
) -> None:
    source = _unique("race.pre-commit.src")
    target = _unique("race.pre-commit.tgt")
    boom = CompletionService(
        session_factory=session_factory,
        fault_hooks=CompletionFaultHooks(
            after_replay_flush=lambda: (_ for _ in ()).throw(
                RuntimeError("injected pre-commit abort")
            )
        ),
    )
    app, task_id, claimed = _setup_leased_pair(
        session_factory,
        source_name=source,
        target_name=target,
        completion_service=boom,
    )
    claim = claimed["claim"]
    claim_id = str(claim["claim_id"])
    claim_token = str(claim["claim_token"])
    generation = int(claim["generation"])
    spawn = [
        _spawn_item(
            queue_name=target,
            payload={"secret": PAYLOAD_SENTINEL, "spawn": 1},
            key="spawn-pre-commit",
        )
    ]

    status1, body1, resp1 = _http_complete(
        app,
        claim_id=claim_id,
        claim_token=claim_token,
        generation=generation,
        spawn=spawn,
    )
    assert status1 >= 500, _safe_status_detail(status1, resp1)
    assert body1.get("state") != "succeeded"

    verify = session_factory()
    try:
        assert (
            verify.execute(
                select(TaskActive).where(TaskActive.task_id == task_id)
            ).scalar_one_or_none()
            is not None
        )
        assert int(
            verify.execute(
                select(TaskActive).where(TaskActive.task_id == task_id)
            ).scalar_one().state_code
        ) == _TASK_LEASED
        assert (
            verify.execute(
                select(func.count())
                .select_from(CompleteReplay)
                .where(CompleteReplay.claim_id == UUID(claim_id))
            ).scalar_one()
            == 0
        )
        assert (
            verify.execute(
                select(func.count())
                .select_from(CompletionEffect)
                .where(CompletionEffect.source_claim_id == UUID(claim_id))
            ).scalar_one()
            == 0
        )
        assert (
            verify.execute(
                select(func.count())
                .select_from(TaskActive)
                .where(TaskActive.source_task_id == task_id)
            ).scalar_one()
            == 0
        )
    finally:
        verify.close()

    healthy = _make_app(session_factory, queue_names=(source, target))
    status2, body2, resp2 = _http_complete(
        healthy,
        claim_id=claim_id,
        claim_token=claim_token,
        generation=generation,
        spawn=spawn,
    )
    assert status2 == 200, _safe_status_detail(status2, resp2)
    assert body2["replayed"] is False
    assert body2["state"] == "succeeded"
    spawned = list(body2["spawned_task_ids"])
    assert len(spawned) == 1

    verify2 = session_factory()
    try:
        _assert_succeeded_lineage(
            verify2,
            task_id=task_id,
            claim_id=UUID(claim_id),
            spawned_task_ids=spawned,
        )
    finally:
        verify2.close()


def test_rollback_injection_all_or_none_at_boundaries(
    session_factory: sessionmaker[Session],
) -> None:
    source = _unique("race.rollback.src")
    target = _unique("race.rollback.tgt")
    setup = session_factory()
    try:
        _seed_queue(setup, name=source)
        _seed_queue(setup, name=target)
    finally:
        setup.close()
    bootstrap = _make_app(session_factory, queue_names=(source, target))
    task_id = UUID(_enqueue_ready(bootstrap, queue_name=source))
    claimed = _claim_one_http(bootstrap, queue_name=source, worker_id="rollback")
    claim = claimed["claim"]
    claim_id = UUID(claim["claim_id"])
    claim_token = UUID(claim["claim_token"])
    generation = int(claim["generation"])
    command = parse_complete_command(
        _complete_body(
            generation=generation,
            spawn=[
                _spawn_item(
                    queue_name=target,
                    payload={"secret": PAYLOAD_SENTINEL, "spawn": 1},
                    key="spawn-rollback",
                )
            ],
        )
    )

    stages = (
        "after_attempt_close",
        "after_claim_delete",
        "after_terminal_insert",
        "after_active_delete",
        "after_counter_adjust",
        "after_spawn_effect",
        "after_spawns",
        "after_replay_flush",
    )

    def _snapshot(session: Session) -> dict[str, int]:
        return {
            "active": int(
                session.execute(
                    select(func.count())
                    .select_from(TaskActive)
                    .where(TaskActive.task_id == task_id)
                ).scalar_one()
            ),
            "terminal": int(
                session.execute(
                    select(func.count())
                    .select_from(TaskTerminal)
                    .where(TaskTerminal.task_id == task_id)
                ).scalar_one()
            ),
            "replay": int(
                session.execute(
                    select(func.count())
                    .select_from(CompleteReplay)
                    .where(CompleteReplay.claim_id == claim_id)
                ).scalar_one()
            ),
            "effects": int(
                session.execute(
                    select(func.count())
                    .select_from(CompletionEffect)
                    .where(CompletionEffect.source_claim_id == claim_id)
                ).scalar_one()
            ),
            "descendants": int(
                session.execute(
                    select(func.count())
                    .select_from(TaskActive)
                    .where(TaskActive.source_task_id == task_id)
                ).scalar_one()
            ),
            "delivery": int(
                session.execute(
                    select(func.count()).select_from(DeliveryEventActive)
                ).scalar_one()
            ),
        }

    before = session_factory()
    try:
        snap = _snapshot(before)
        assert snap == {
            "active": 1,
            "terminal": 0,
            "replay": 0,
            "effects": 0,
            "descendants": 0,
            "delivery": 0,
        }
    finally:
        before.close()

    for stage in stages:
        hooks = CompletionFaultHooks(
            **{
                stage: lambda s=stage: (_ for _ in ()).throw(
                    RuntimeError(f"injected failure at {s}")
                )
            }
        )
        service = CompletionService(
            session_factory=session_factory,
            fault_hooks=hooks,
        )
        with pytest.raises(RuntimeError, match="injected failure"):
            service.complete(
                claim_id=claim_id,
                claim_token=claim_token,
                command=command,
                authorize_queue=lambda name, src=source, tgt=target: name in {src, tgt},
            )
        after = session_factory()
        try:
            assert _snapshot(after) == snap, stage
        finally:
            after.close()

    # Final healthy Complete commits once.
    healthy = CompletionService(session_factory=session_factory)
    result = healthy.complete(
        claim_id=claim_id,
        claim_token=claim_token,
        command=command,
        authorize_queue=lambda name, src=source, tgt=target: name in {src, tgt},
    )
    assert result.replayed is False
    assert len(result.spawned_task_ids) == 1
    final = session_factory()
    try:
        _assert_succeeded_lineage(
            final,
            task_id=task_id,
            claim_id=claim_id,
            spawned_task_ids=[str(x) for x in result.spawned_task_ids],
        )
    finally:
        final.close()
