"""Complete uncertain-response conformance (Phase 03.7-06 / QUAL-02).

Proves the externally observable Complete commit-window boundary:
- failure before Complete commit → no terminal/spawn/replay; later Complete succeeds;
- failure after commit but before HTTP response → same-body replay returns original
  source/spawn identifiers without duplicate descendants;
- API context restart preserves durable replay;
- changed Complete body after commit returns exact ``idempotency_conflict``.

Uses test-only ``DropCommittedResponseProxy`` and ``CompletionFaultHooks``.
Real application-plane HTTP + real PostgreSQL; no repository mocks of commit.
"""

from __future__ import annotations

import json
import os
import socket
import threading
import time
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any
from urllib.request import Request, urlopen
from uuid import UUID

import pytest
from sqlalchemy import create_engine, event, func, select
from sqlalchemy.orm import Session, sessionmaker

from queue_service.api.application import create_application_app
from queue_service.api.security import ListenerBind
from queue_service.application.claim_service import ClaimService
from queue_service.application.completion import CompletionFaultHooks, CompletionService
from queue_service.application.lease_service import LeaseService
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
from queue_service.intake.service import EnqueueService
from queue_service.lifecycle import Lifecycle
from queue_service.roles.api import AsgiRequestHandler, InFlightGate, QuietThreadingHTTPServer
from queue_service.security.authorization import Authorizer
from queue_service.security.credentials import (
    BearerCredentialAuthenticator,
    CredentialBinding,
)
from queue_service.security.principals import ServiceRole
from queue_service.settings import Secret
from queue_service.storage.models import (
    CompleteReplay,
    CompletionEffect,
    DeliveryEventActive,
    Queue,
    TaskActive,
    TaskAttempt,
    TaskTerminal,
)
from tests.conformance.faults import DropCommittedResponseProxy

pytest_plugins = ["tests.integration.conftest"]

REPO_ROOT = Path(__file__).resolve().parents[2]

PRODUCER_TOKEN = "tok-producer-complete-uncertain"
WORKER_TOKEN = "tok-worker-complete-uncertain"
ADMIN_TOKEN = "tok-admin-complete-uncertain"
PRODUCER_PRINCIPAL = "producer-complete-uncertain"
WORKER_PRINCIPAL = "worker-complete-uncertain"
ADMIN_PRINCIPAL = "admin-complete-uncertain"
BASE_SOURCE = "orders.complete.uncertain"
BASE_TARGET = "billing.complete.uncertain"
PAYLOAD_SENTINEL = "COMPLETE_UNCERTAIN_PAYLOAD_NEVER"
CLAIM_TOKEN_HEADER = "X-Queue-Claim-Token"
_LEASE_SECONDS = 60
_OP_COMPLETE = 1
_EFFECT_KIND_SPAWN = 1
_OUTCOME_ACTIVE = 1
_OUTCOME_SUCCEEDED = 2
_RESULT_SUCCEEDED = 10
_TASK_READY = 2


def _unique(prefix: str) -> str:
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
            principal_id=ADMIN_PRINCIPAL,
            role=ServiceRole.ADMIN,
            generation_id="g1",
            secret=Secret(ADMIN_TOKEN),
        ),
    )


@pytest.fixture
def source_queue() -> str:
    return _unique(BASE_SOURCE)


@pytest.fixture
def target_queue() -> str:
    return _unique(BASE_TARGET)


@pytest.fixture
def authorizer(source_queue: str, target_queue: str) -> Authorizer:
    scopes = frozenset({source_queue, target_queue})
    return Authorizer(
        queue_scopes={
            PRODUCER_PRINCIPAL: scopes,
            WORKER_PRINCIPAL: scopes,
            ADMIN_PRINCIPAL: scopes,
        }
    )


@pytest.fixture
def session_factory(migrated_schema) -> Iterator[sessionmaker[Session]]:
    _conn, schema = migrated_schema
    database_url = os.environ.get("TEST_DATABASE_URL", "").strip()
    if not database_url:
        pytest.fail("TEST_DATABASE_URL is required for Complete uncertain conformance")
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


def _meta(*, actor_id: str) -> AdminRequestMetadata:
    return AdminRequestMetadata(
        actor_id=actor_id,
        request_id=str(uuid.uuid4()),
        idempotency_key=f"idem-{uuid.uuid4().hex}",
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
                retry_delay_seconds=0,
            ),
            metadata=_meta(actor_id="seed"),
        ),
    )
    session.commit()
    return session.execute(select(Queue).where(Queue.name == name)).scalar_one()


def _build_app(
    session_factory: sessionmaker[Session],
    authorizer: Authorizer,
    *,
    completion_service: CompletionService | None = None,
) -> Any:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
        probe.bind(("127.0.0.1", 0))
        port = int(probe.getsockname()[1])
    return create_application_app(
        authenticator=BearerCredentialAuthenticator.from_bindings(_bindings()),
        authorizer=authorizer,
        bind=ListenerBind(host="127.0.0.1", port=port),
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
        completion_service=completion_service
        or CompletionService(session_factory=session_factory),
    )


@contextmanager
def _serve_app(app: Any) -> Iterator[str]:
    lifecycle = Lifecycle()
    gate = InFlightGate()
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
        probe.bind(("127.0.0.1", 0))
        port = int(probe.getsockname()[1])
    server = QuietThreadingHTTPServer(
        ("127.0.0.1", port),
        AsgiRequestHandler,
        app=app,
        lifecycle=lifecycle,
        gate=gate,
        api_engine=None,
        schema=None,
        premake_days=0,
    )
    lifecycle.mark_running()
    thread = threading.Thread(
        target=server.serve_forever,
        kwargs={"poll_interval": 0.05},
        name="complete-uncertain-app",
        daemon=True,
    )
    thread.start()
    try:
        yield f"http://127.0.0.1:{port}"
    finally:
        try:
            server.shutdown()
        except Exception:  # noqa: BLE001
            pass
        try:
            server.server_close()
        except Exception:  # noqa: BLE001
            pass
        thread.join(timeout=2.0)


def _raw_http_exchange(
    base_url: str,
    *,
    method: str,
    path: str,
    headers: dict[str, str],
    body: bytes,
    timeout_s: float,
) -> tuple[int, dict[str, str], bytes]:
    url = f"{base_url.rstrip('/')}{path}"
    req = Request(url, data=body if body else None, headers=headers, method=method)
    with urlopen(req, timeout=timeout_s) as resp:
        raw = resp.read()
        header_map = {k.lower(): v for k, v in resp.headers.items()}
        return int(resp.status), header_map, raw


def _producer_headers(idem: str) -> dict[str, str]:
    return {
        "Authorization": f"Bearer {PRODUCER_TOKEN}",
        "Content-Type": "application/json",
        "Idempotency-Key": idem,
    }


def _worker_headers(*, claim_token: str) -> dict[str, str]:
    return {
        "Authorization": f"Bearer {WORKER_TOKEN}",
        "Content-Type": "application/json",
        CLAIM_TOKEN_HEADER: claim_token,
    }


def _spawn_item(*, queue_name: str, payload: Any, key: str) -> dict[str, Any]:
    return {
        "queue_name": queue_name,
        "idempotency_key": key,
        "payload": payload,
        "priority": 0,
    }


def _enqueue_claim(
    base_url: str,
    *,
    source_queue: str,
) -> tuple[str, dict[str, Any]]:
    enqueue_body = json.dumps(
        {"payload": {"secret": PAYLOAD_SENTINEL, "n": 1}, "priority": 0},
        separators=(",", ":"),
    ).encode("utf-8")
    status, _h, resp = _raw_http_exchange(
        base_url,
        method="POST",
        path=f"/v1/queues/{source_queue}/tasks",
        headers=_producer_headers(f"idem-{uuid.uuid4().hex}"),
        body=enqueue_body,
        timeout_s=10.0,
    )
    assert status == 201, f"enqueue status={status}"
    task_id = str(json.loads(resp.decode("utf-8"))["task"]["task_id"])

    claim_body = json.dumps(
        {
            "queues": [source_queue],
            "max_tasks": 1,
            "lease_seconds": _LEASE_SECONDS,
            "wait_seconds": 0,
            "worker_id": "uncertain-worker",
        },
        separators=(",", ":"),
    ).encode("utf-8")
    status_c, _hc, resp_c = _raw_http_exchange(
        base_url,
        method="POST",
        path="/v1/claims",
        headers={
            "Authorization": f"Bearer {WORKER_TOKEN}",
            "Content-Type": "application/json",
        },
        body=claim_body,
        timeout_s=10.0,
    )
    assert status_c == 200, f"claim status={status_c}"
    claimed = json.loads(resp_c.decode("utf-8"))["tasks"][0]
    return task_id, claimed["claim"]


def _assert_lineage(
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
    replay = session.execute(
        select(CompleteReplay).where(
            CompleteReplay.claim_id == claim_id,
            CompleteReplay.operation_code == _OP_COMPLETE,
        )
    ).scalar_one()
    assert list(replay.spawned_task_ids) == [UUID(x) for x in spawned_task_ids]
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
            select(func.count()).select_from(DeliveryEventActive)
        ).scalar_one()
        == 0
    )


def test_failure_after_commit_before_response_same_body_replay_stable_ids(
    session_factory: sessionmaker[Session],
    authorizer: Authorizer,
    source_queue: str,
    target_queue: str,
) -> None:
    session = session_factory()
    try:
        _seed_queue(session, name=source_queue)
        _seed_queue(session, name=target_queue)
    finally:
        session.close()

    app = _build_app(session_factory, authorizer)
    with _serve_app(app) as upstream_url:
        task_id, claim = _enqueue_claim(upstream_url, source_queue=source_queue)
        claim_id = str(claim["claim_id"])
        claim_token = str(claim["claim_token"])
        generation = int(claim["generation"])
        spawn = [
            _spawn_item(
                queue_name=target_queue,
                payload={"secret": PAYLOAD_SENTINEL, "spawn": 1},
                key="spawn-uncertain-a",
            ),
            _spawn_item(
                queue_name=target_queue,
                payload={"secret": PAYLOAD_SENTINEL, "spawn": 2},
                key="spawn-uncertain-b",
            ),
        ]
        complete_body = json.dumps(
            {"generation": generation, "spawn": spawn},
            separators=(",", ":"),
        ).encode("utf-8")
        complete_path = f"/v1/claims/{claim_id}:complete"
        headers = _worker_headers(claim_token=claim_token)

        deadline = time.monotonic() + 20.0
        proxy = DropCommittedResponseProxy()
        listen_url = proxy.serve_once(upstream_url, deadline)

        loss_error: BaseException | None = None
        try:
            _raw_http_exchange(
                listen_url,
                method="POST",
                path=complete_path,
                headers=headers,
                body=complete_body,
                timeout_s=15.0,
            )
        except Exception as exc:  # noqa: BLE001 — expected transport loss
            loss_error = exc
        assert loss_error is not None

        buffered = proxy.wait(deadline)
        assert buffered.status_code == 200
        upstream_payload = buffered.json()
        assert upstream_payload["state"] == "succeeded"
        assert upstream_payload["replayed"] is False
        original_task_id = upstream_payload["task_id"]
        original_spawns = list(upstream_payload["spawned_task_ids"])
        assert original_task_id == task_id
        assert len(original_spawns) == 2
        proxy.close()

        session = session_factory()
        try:
            _assert_lineage(
                session,
                task_id=UUID(task_id),
                claim_id=UUID(claim_id),
                spawned_task_ids=original_spawns,
            )
            before_counts = {
                "replays": int(
                    session.execute(
                        select(func.count())
                        .select_from(CompleteReplay)
                        .where(CompleteReplay.claim_id == UUID(claim_id))
                    ).scalar_one()
                ),
                "effects": int(
                    session.execute(
                        select(func.count())
                        .select_from(CompletionEffect)
                        .where(CompletionEffect.source_claim_id == UUID(claim_id))
                    ).scalar_one()
                ),
                "descendants": int(
                    session.execute(
                        select(func.count())
                        .select_from(TaskActive)
                        .where(TaskActive.source_task_id == UUID(task_id))
                    ).scalar_one()
                ),
            }
            assert before_counts == {"replays": 1, "effects": 2, "descendants": 2}
        finally:
            session.close()

        # Process restart: new ASGI composition over the same PostgreSQL schema.
        del app
        app2 = _build_app(session_factory, authorizer)
        with _serve_app(app2) as restart_url:
            status2, _h2, resp2 = _raw_http_exchange(
                restart_url,
                method="POST",
                path=complete_path,
                headers=headers,
                body=complete_body,
                timeout_s=10.0,
            )
            assert status2 == 200
            replay_payload = json.loads(resp2.decode("utf-8"))
            assert replay_payload["replayed"] is True
            assert replay_payload["task_id"] == original_task_id
            assert replay_payload["spawned_task_ids"] == original_spawns
            assert "events" not in replay_payload
            assert PAYLOAD_SENTINEL not in resp2.decode("utf-8", errors="replace")
            assert claim_token not in resp2.decode("utf-8", errors="replace")

            # Changed body after durable commit → exact idempotency_conflict.
            changed = json.dumps(
                {
                    "generation": generation,
                    "spawn": [
                        _spawn_item(
                            queue_name=target_queue,
                            payload={"secret": PAYLOAD_SENTINEL, "changed": True},
                            key="spawn-changed",
                        )
                    ],
                },
                separators=(",", ":"),
            ).encode("utf-8")
            try:
                status3, _h3, resp3 = _raw_http_exchange(
                    restart_url,
                    method="POST",
                    path=complete_path,
                    headers=headers,
                    body=changed,
                    timeout_s=10.0,
                )
            except Exception as exc:  # noqa: BLE001 — urllib raises on HTTPError
                from urllib.error import HTTPError

                assert isinstance(exc, HTTPError)
                status3 = int(exc.code)
                resp3 = exc.read()
            assert status3 == 409
            err = json.loads(resp3.decode("utf-8"))
            assert err["code"] == "idempotency_conflict"
            assert err["retryable"] is False

        session = session_factory()
        try:
            _assert_lineage(
                session,
                task_id=UUID(task_id),
                claim_id=UUID(claim_id),
                spawned_task_ids=original_spawns,
            )
            assert (
                session.execute(
                    select(func.count())
                    .select_from(TaskActive)
                    .where(TaskActive.source_task_id == UUID(task_id))
                ).scalar_one()
                == 2
            )
        finally:
            session.close()


def test_failure_before_commit_leaves_no_effects_then_complete_succeeds(
    session_factory: sessionmaker[Session],
    authorizer: Authorizer,
    source_queue: str,
    target_queue: str,
) -> None:
    session = session_factory()
    try:
        _seed_queue(session, name=source_queue)
        _seed_queue(session, name=target_queue)
    finally:
        session.close()

    boom = CompletionService(
        session_factory=session_factory,
        fault_hooks=CompletionFaultHooks(
            after_spawns=lambda: (_ for _ in ()).throw(
                RuntimeError("injected pre-commit abort at after_spawns")
            )
        ),
    )
    app = _build_app(session_factory, authorizer, completion_service=boom)
    with _serve_app(app) as base_url:
        task_id, claim = _enqueue_claim(base_url, source_queue=source_queue)
        claim_id = str(claim["claim_id"])
        claim_token = str(claim["claim_token"])
        generation = int(claim["generation"])
        spawn = [
            _spawn_item(
                queue_name=target_queue,
                payload={"secret": PAYLOAD_SENTINEL, "spawn": 1},
                key="spawn-pre-commit",
            )
        ]
        complete_body = json.dumps(
            {"generation": generation, "spawn": spawn},
            separators=(",", ":"),
        ).encode("utf-8")
        complete_path = f"/v1/claims/{claim_id}:complete"
        headers = _worker_headers(claim_token=claim_token)

        try:
            status1, _h1, resp1 = _raw_http_exchange(
                base_url,
                method="POST",
                path=complete_path,
                headers=headers,
                body=complete_body,
                timeout_s=10.0,
            )
        except Exception as exc:  # noqa: BLE001
            from urllib.error import HTTPError

            assert isinstance(exc, HTTPError)
            status1 = int(exc.code)
            resp1 = exc.read()
        assert status1 >= 500
        assert b"succeeded" not in resp1 or b'"state":"succeeded"' not in resp1

        session = session_factory()
        try:
            active = session.execute(
                select(TaskActive).where(TaskActive.task_id == UUID(task_id))
            ).scalar_one()
            assert active.current_claim_id == UUID(claim_id)
            assert (
                session.execute(
                    select(func.count())
                    .select_from(CompleteReplay)
                    .where(CompleteReplay.claim_id == UUID(claim_id))
                ).scalar_one()
                == 0
            )
            assert (
                session.execute(
                    select(func.count())
                    .select_from(CompletionEffect)
                    .where(CompletionEffect.source_claim_id == UUID(claim_id))
                ).scalar_one()
                == 0
            )
            assert (
                session.execute(
                    select(func.count())
                    .select_from(TaskActive)
                    .where(TaskActive.source_task_id == UUID(task_id))
                ).scalar_one()
                == 0
            )
            assert (
                session.execute(
                    select(TaskTerminal).where(TaskTerminal.task_id == UUID(task_id))
                ).scalar_one_or_none()
                is None
            )
        finally:
            session.close()

    # Fresh healthy API context recovers the leased claim.
    healthy = _build_app(session_factory, authorizer)
    with _serve_app(healthy) as healthy_url:
        status2, _h2, resp2 = _raw_http_exchange(
            healthy_url,
            method="POST",
            path=complete_path,
            headers=headers,
            body=complete_body,
            timeout_s=10.0,
        )
        assert status2 == 200
        payload = json.loads(resp2.decode("utf-8"))
        assert payload["replayed"] is False
        assert payload["state"] == "succeeded"
        spawned = list(payload["spawned_task_ids"])
        assert len(spawned) == 1

    session = session_factory()
    try:
        _assert_lineage(
            session,
            task_id=UUID(task_id),
            claim_id=UUID(claim_id),
            spawned_task_ids=spawned,
        )
    finally:
        session.close()
