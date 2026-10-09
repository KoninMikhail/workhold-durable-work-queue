"""Black-box OpenAPI conformance for API-04 completion / spawn inspection.

Enqueue → claim → complete with ordered spawns, then inspect source, attempts,
and descendants under producer, observer, worker, and unauthorized principals.
Cross-checks copied lineage against completion_effects and complete_replay.
"""

from __future__ import annotations

import asyncio
import json
import os
import uuid
from collections.abc import Iterator, Mapping
from dataclasses import dataclass
from datetime import datetime
from typing import Any
from uuid import UUID

import pytest
from sqlalchemy import create_engine, event, select
from sqlalchemy.orm import Session, sessionmaker

from queue_service.api.application import create_application_app
from queue_service.api.security import ListenerBind
from queue_service.application.claim_service import ClaimService
from queue_service.application.completion import CompletionService
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
from queue_service.security.authorization import Authorizer
from queue_service.security.credentials import (
    BearerCredentialAuthenticator,
    CredentialBinding,
)
from queue_service.security.payload_policy import PayloadRetentionPolicy
from queue_service.security.principals import ServiceRole
from queue_service.settings import Secret
from queue_service.storage.models import (
    CompleteReplay,
    CompletionEffect,
    Queue,
)

pytest_plugins = ["tests.integration.conftest"]

PRODUCER_TOKEN = "tok-producer-inspect-complete"
PRODUCER_OTHER_TOKEN = "tok-producer-inspect-complete-other"
WORKER_TOKEN = "tok-worker-inspect-complete"
OBSERVER_TOKEN = "tok-observer-inspect-complete"
ADMIN_TOKEN = "tok-admin-inspect-complete"

PRODUCER_PRINCIPAL = "producer-inspect-complete"
PRODUCER_OTHER_PRINCIPAL = "producer-inspect-complete-other"
WORKER_PRINCIPAL = "worker-inspect-complete"
OBSERVER_PRINCIPAL = "observer-inspect-complete"
ADMIN_PRINCIPAL = "admin-inspect-complete"

BASE_QUEUE_NAME = "orders.inspect.complete"
TARGET_QUEUE = "billing.inspect.complete"
PAYLOAD_SENTINEL = "INSPECT_COMPLETE_BUSINESS_RESULT_NEVER"
CLAIM_PATH = "/v1/claims"
REPLICA_ID = "pool-inspect-complete/replica-1"
CLAIM_TOKEN_HEADER = "X-Queue-Claim-Token"

_EFFECT_KIND_SPAWN = 1
_OP_COMPLETE = 1


def _unique_queue_name(prefix: str) -> str:
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
    return _unique_queue_name(BASE_QUEUE_NAME)


@pytest.fixture
def target_queue_name() -> str:
    return _unique_queue_name(TARGET_QUEUE)


@pytest.fixture
def authorizer(queue_name: str, target_queue_name: str) -> Authorizer:
    scopes = frozenset({queue_name, target_queue_name, BASE_QUEUE_NAME, TARGET_QUEUE})
    return Authorizer(
        queue_scopes={
            PRODUCER_PRINCIPAL: scopes,
            PRODUCER_OTHER_PRINCIPAL: frozenset({queue_name, BASE_QUEUE_NAME}),
            WORKER_PRINCIPAL: scopes,
            OBSERVER_PRINCIPAL: scopes,
            ADMIN_PRINCIPAL: scopes,
        }
    )


@pytest.fixture
def session_factory(migrated_schema) -> Iterator[sessionmaker[Session]]:
    _conn, schema = migrated_schema
    database_url = os.environ.get("TEST_DATABASE_URL", "").strip()
    if not database_url:
        pytest.fail("TEST_DATABASE_URL is required for inspection conformance tests")
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
    depth = DepthCeilings(
        queue_active_depth=100,
        instance_active_depth=500,
        retry_after_ms=250,
    )
    return create_application_app(
        authenticator=BearerCredentialAuthenticator.from_bindings(_bindings()),
        authorizer=authorizer,
        bind=ListenerBind(host="127.0.0.1", port=18117),
        session_factory=session_factory,
        enqueue_service=EnqueueService(
            session_factory=session_factory,
            depth_ceilings=depth,
        ),
        claim_service=ClaimService(session_factory=session_factory),
        lease_service=LeaseService(session_factory=session_factory),
        completion_service=CompletionService(
            session_factory=session_factory,
            depth_ceilings=depth,
        ),
    )


def _asgi_http_call(
    app: Any,
    *,
    method: str,
    path: str,
    headers: Mapping[str, str] | None = None,
    body: bytes = b"",
    query_string: bytes = b"",
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
        "query_string": query_string,
        "headers": header_list,
        "client": ("127.0.0.1", 50000),
        "server": ("127.0.0.1", 18117),
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


def _seed_queue(session_factory: sessionmaker[Session], *, name: str) -> Queue:
    session = session_factory()
    try:
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
                metadata=_admin_meta(),
            ),
        )
        session.commit()
        return session.execute(select(Queue).where(Queue.name == name)).scalar_one()
    finally:
        session.close()


def _enqueue_ready(
    app: Any,
    *,
    queue_name: str,
    payload: Any | None = None,
    priority: int = 0,
) -> str:
    body = json.dumps(
        {
            "payload": payload if payload is not None else {"marker": PAYLOAD_SENTINEL},
            "priority": priority,
        },
        separators=(",", ":"),
    ).encode("utf-8")
    status, _hdrs, resp = _asgi_http_call(
        app,
        method="POST",
        path=f"/v1/queues/{queue_name}/tasks",
        headers={
            "Authorization": f"Bearer {PRODUCER_TOKEN}",
            "Content-Type": "application/json",
            "Idempotency-Key": f"enq-{uuid.uuid4().hex}",
        },
        body=body,
    )
    assert status == 201, resp.decode("utf-8", errors="replace")
    return json.loads(resp.decode("utf-8"))["task"]["task_id"]


def _claim_one(app: Any, *, queue_name: str) -> dict[str, Any]:
    body = json.dumps(
        {
            "queues": [queue_name],
            "max_tasks": 1,
            "lease_seconds": 30,
            "wait_seconds": 0,
            "worker_id": REPLICA_ID,
        },
        separators=(",", ":"),
    ).encode("utf-8")
    status, _hdrs, resp = _asgi_http_call(
        app,
        method="POST",
        path=CLAIM_PATH,
        headers={
            "Authorization": f"Bearer {WORKER_TOKEN}",
            "Content-Type": "application/json",
        },
        body=body,
    )
    assert status == 200, resp.decode("utf-8", errors="replace")
    claimed = json.loads(resp.decode("utf-8"))["tasks"][0]
    claim = claimed["claim"]
    return {
        "task_id": claimed["task"]["task_id"],
        "claim_id": claim["claim_id"],
        "claim_token": claim["claim_token"],
        "generation": int(claim["generation"]),
    }


def _complete_with_spawns(
    app: Any,
    *,
    claim: dict[str, Any],
    target_queue_name: str,
    spawn_count: int,
) -> dict[str, Any]:
    spawn = [
        {
            "queue_name": target_queue_name,
            "idempotency_key": f"spawn-{i}-{uuid.uuid4().hex}",
            "payload": {"spawn_index": i, "marker": PAYLOAD_SENTINEL},
            "priority": 0,
        }
        for i in range(spawn_count)
    ]
    body = json.dumps(
        {"generation": claim["generation"], "spawn": spawn},
        separators=(",", ":"),
    ).encode("utf-8")
    status, _hdrs, resp = _asgi_http_call(
        app,
        method="POST",
        path=f"/v1/claims/{claim['claim_id']}:complete",
        headers={
            "Authorization": f"Bearer {WORKER_TOKEN}",
            "Content-Type": "application/json",
            CLAIM_TOKEN_HEADER: claim["claim_token"],
        },
        body=body,
    )
    assert status == 200, resp.decode("utf-8", errors="replace")
    return json.loads(resp.decode("utf-8"))


def _get_task(app: Any, *, task_id: str, token: str) -> tuple[int, dict[str, Any]]:
    status, _hdrs, resp = _asgi_http_call(
        app,
        method="GET",
        path=f"/v1/tasks/{task_id}",
        headers={"Authorization": f"Bearer {token}"},
    )
    return status, json.loads(resp.decode("utf-8"))


def _list_attempts(
    app: Any,
    *,
    task_id: str,
    token: str,
) -> tuple[int, dict[str, Any]]:
    status, _hdrs, resp = _asgi_http_call(
        app,
        method="GET",
        path=f"/v1/tasks/{task_id}/attempts",
        headers={"Authorization": f"Bearer {token}"},
    )
    return status, json.loads(resp.decode("utf-8"))


def _assert_no_secret_leak(payload: Any, *, claim_token: str) -> None:
    dumped = json.dumps(payload, ensure_ascii=False)
    assert "claim_token" not in dumped
    assert claim_token not in dumped
    assert '"result"' not in dumped
    assert "parse_result" not in dumped
    assert "request_fingerprint" not in dumped
    assert "fingerprint" not in dumped
    assert "effect_kind_code" not in dumped
    assert "source_claim_id" not in dumped
    assert PAYLOAD_SENTINEL not in dumped


def _effects_for_claim(session: Session, *, claim_id: UUID) -> list[CompletionEffect]:
    return list(
        session.execute(
            select(CompletionEffect)
            .where(CompletionEffect.source_claim_id == claim_id)
            .order_by(CompletionEffect.ordinal)
        )
        .scalars()
        .all()
    )


def test_completed_source_and_spawn_lineage_inspection(
    app: Any,
    session_factory: sessionmaker[Session],
    queue_name: str,
    target_queue_name: str,
) -> None:
    _seed_queue(session_factory, name=queue_name)
    _seed_queue(session_factory, name=target_queue_name)

    source_id = _enqueue_ready(app, queue_name=queue_name)
    claim = _claim_one(app, queue_name=queue_name)
    assert claim["task_id"] == source_id
    complete = _complete_with_spawns(
        app,
        claim=claim,
        target_queue_name=target_queue_name,
        spawn_count=3,
    )
    assert complete["state"] == "succeeded"
    assert len(complete["spawned_task_ids"]) == 3
    claim_token = claim["claim_token"]

    status, source = _get_task(app, task_id=source_id, token=PRODUCER_TOKEN)
    assert status == 200, source
    assert source["task_id"] == source_id
    assert source["state"] == "succeeded"
    assert source["terminal_at"] is not None
    assert source["spawned_task_ids"] == complete["spawned_task_ids"]
    assert source["delivery_event_ids"] == []
    assert "source_task_id" not in source or source.get("source_task_id") is None
    _assert_no_secret_leak(source, claim_token=claim_token)

    status, attempts = _list_attempts(app, task_id=source_id, token=OBSERVER_TOKEN)
    assert status == 200, attempts
    assert len(attempts["items"]) >= 1
    succeeded = [a for a in attempts["items"] if a["outcome"] == "succeeded"]
    assert len(succeeded) == 1
    assert succeeded[0]["claim_id"] == claim["claim_id"]
    _assert_no_secret_leak(attempts, claim_token=claim_token)

    session = session_factory()
    try:
        effects = _effects_for_claim(session, claim_id=UUID(claim["claim_id"]))
        replay = session.execute(
            select(CompleteReplay).where(
                CompleteReplay.claim_id == UUID(claim["claim_id"]),
                CompleteReplay.operation_code == _OP_COMPLETE,
            )
        ).scalar_one()
        assert list(replay.spawned_task_ids) == [
            UUID(x) for x in complete["spawned_task_ids"]
        ]
        assert list(replay.event_ids or []) == []
        assert len(effects) == 3
        for ordinal, spawn_id in enumerate(complete["spawned_task_ids"]):
            effect = effects[ordinal]
            assert int(effect.effect_kind_code) == _EFFECT_KIND_SPAWN
            assert int(effect.ordinal) == ordinal
            assert effect.resource_id == UUID(spawn_id)
            assert effect.source_claim_id == UUID(claim["claim_id"])
            assert source["spawned_task_ids"][ordinal] == spawn_id

            status, child = _get_task(app, task_id=spawn_id, token=OBSERVER_TOKEN)
            assert status == 200, child
            assert child["task_id"] == spawn_id
            assert child["queue_name"] == target_queue_name
            assert child["source_task_id"] == source_id
            assert child["spawn_ordinal"] == ordinal
            assert child["spawned_task_ids"] == []
            assert child["delivery_event_ids"] == []
            assert child["retry_policy_version"] >= 1
            _assert_no_secret_leak(child, claim_token=claim_token)
    finally:
        session.close()

    # Observer operational visibility; admin has no getTask grant on application plane.
    status, obs = _get_task(app, task_id=source_id, token=OBSERVER_TOKEN)
    assert status == 200
    assert obs["spawned_task_ids"] == complete["spawned_task_ids"]

    status, admin_err = _get_task(app, task_id=source_id, token=ADMIN_TOKEN)
    assert status == 403
    assert admin_err["code"] == "permission_denied"

    status, worker_err = _get_task(app, task_id=source_id, token=WORKER_TOKEN)
    assert status == 403
    assert worker_err["code"] == "permission_denied"

    status, other_err = _get_task(app, task_id=source_id, token=PRODUCER_OTHER_TOKEN)
    assert status == 404
    assert other_err["code"] == "task_not_found"
    _assert_no_secret_leak(other_err, claim_token=claim_token)


def test_retention_and_unauthorized_freeze_to_task_not_found(
    app: Any,
    session_factory: sessionmaker[Session],
    authorizer: Authorizer,
    queue_name: str,
    target_queue_name: str,
) -> None:
    _seed_queue(session_factory, name=queue_name)
    _seed_queue(session_factory, name=target_queue_name)
    source_id = _enqueue_ready(app, queue_name=queue_name)
    claim = _claim_one(app, queue_name=queue_name)
    _complete_with_spawns(
        app,
        claim=claim,
        target_queue_name=target_queue_name,
        spawn_count=1,
    )

    @dataclass(frozen=True, slots=True)
    class ForceExpiredRetention(PayloadRetentionPolicy):
        def is_expired(
            self,
            queue_store_now: datetime,
            expires_at: datetime,
        ) -> bool:
            return True

    depth = DepthCeilings(
        queue_active_depth=100,
        instance_active_depth=500,
        retry_after_ms=250,
    )
    short_app = create_application_app(
        authenticator=BearerCredentialAuthenticator.from_bindings(_bindings()),
        authorizer=authorizer,
        bind=ListenerBind(host="127.0.0.1", port=18118),
        session_factory=session_factory,
        enqueue_service=EnqueueService(
            session_factory=session_factory,
            depth_ceilings=depth,
        ),
        claim_service=ClaimService(session_factory=session_factory),
        lease_service=LeaseService(session_factory=session_factory),
        completion_service=CompletionService(
            session_factory=session_factory,
            depth_ceilings=depth,
        ),
        retention_policy=ForceExpiredRetention(retention_days=30),
    )

    status, err = _get_task(short_app, task_id=source_id, token=OBSERVER_TOKEN)
    assert status == 404
    assert err["code"] == "task_not_found"
    assert "exists" not in json.dumps(err).lower()

    status, err = _list_attempts(short_app, task_id=source_id, token=OBSERVER_TOKEN)
    assert status == 404
    assert err["code"] == "task_not_found"


_SOURCE_PRIORITY = 500
_SPAWN_PRIORITY_LOW = -100
_SPAWN_PRIORITY_HIGH = 2000


def test_spawn_inspection_returns_exact_non_zero_priority(
    app: Any,
    session_factory: sessionmaker[Session],
    queue_name: str,
    target_queue_name: str,
) -> None:
    _seed_queue(session_factory, name=queue_name)
    _seed_queue(session_factory, name=target_queue_name)

    source_id = _enqueue_ready(
        app,
        queue_name=queue_name,
        priority=_SOURCE_PRIORITY,
    )
    claim = _claim_one(app, queue_name=queue_name)
    spawn = [
        {
            "queue_name": target_queue_name,
            "idempotency_key": f"spawn-low-{uuid.uuid4().hex}",
            "payload": {"tier": "low", "marker": PAYLOAD_SENTINEL},
            "priority": _SPAWN_PRIORITY_LOW,
        },
        {
            "queue_name": target_queue_name,
            "idempotency_key": f"spawn-high-{uuid.uuid4().hex}",
            "payload": {"tier": "high", "marker": PAYLOAD_SENTINEL},
            "priority": _SPAWN_PRIORITY_HIGH,
        },
    ]
    body = json.dumps(
        {"generation": claim["generation"], "spawn": spawn},
        separators=(",", ":"),
    ).encode("utf-8")
    status, _hdrs, resp = _asgi_http_call(
        app,
        method="POST",
        path=f"/v1/claims/{claim['claim_id']}:complete",
        headers={
            "Authorization": f"Bearer {WORKER_TOKEN}",
            "Content-Type": "application/json",
            CLAIM_TOKEN_HEADER: claim["claim_token"],
        },
        body=body,
    )
    assert status == 200, resp.decode("utf-8", errors="replace")
    complete = json.loads(resp.decode("utf-8"))
    assert complete["state"] == "succeeded"
    assert len(complete["spawned_task_ids"]) == 2

    status, source = _get_task(app, task_id=source_id, token=PRODUCER_TOKEN)
    assert status == 200, source
    assert int(source["priority"]) == _SOURCE_PRIORITY

    child_priorities: dict[str, int] = {}
    for spawn_id in complete["spawned_task_ids"]:
        status, child = _get_task(app, task_id=spawn_id, token=OBSERVER_TOKEN)
        assert status == 200, child
        child_priorities[spawn_id] = int(child["priority"])

    assert sorted(child_priorities.values()) == [
        _SPAWN_PRIORITY_LOW,
        _SPAWN_PRIORITY_HIGH,
    ]
