"""Uncertain enqueue commit-window conformance (Phase 03.4-08).

Proves the externally observable guarantee boundary:
- failure before enqueue commit → no task; same-key retry creates one;
- failure after commit but before response → same-key retry returns that task.

Uses test-only ``PostgresPreCommitGate`` and ``DropCommittedResponseProxy``.
Real application-plane HTTP + real PostgreSQL; no repository/SDK mocks.
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

import pytest
from sqlalchemy import create_engine, event, func, select
from sqlalchemy.orm import Session, sessionmaker

from workhold.api.application import create_application_app
from workhold.api.security import ListenerBind
from workhold.domain.queue_control import (
    AdminRequestMetadata,
    BackoffStrategy,
    CreateQueueMutation,
    RetryPolicyDraft,
)
from workhold.infrastructure.postgres.queue_control_repository import (
    QueueControlRepository,
)
from workhold.intake.depth import DepthCeilings
from workhold.intake.service import EnqueueService
from workhold.lifecycle import Lifecycle
from workhold.roles.api import AsgiRequestHandler, InFlightGate, QuietThreadingHTTPServer
from workhold.security.authorization import Authorizer
from workhold.security.credentials import (
    BearerCredentialAuthenticator,
    CredentialBinding,
)
from workhold.security.principals import ServiceRole
from workhold.settings import Secret
from workhold.storage.models import (
    EnqueueDedup,
    Queue,
    QueueCounter,
    TaskActive,
    TaskPayloadActive,
)
from tests.conformance.faults import DropCommittedResponseProxy, PostgresPreCommitGate
from tests.conformance.harness import (
    CaseOutcome,
    ConformanceHarness,
    ObservedResponse,
    RequestCase,
)

pytest_plugins = ["tests.integration.conftest"]

REPO_ROOT = Path(__file__).resolve().parents[2]
OPENAPI_PATH = REPO_ROOT / "openapi" / "queue.openapi.json"

PRODUCER_TOKEN = "tok-producer-uncertain-commit"
ADMIN_TOKEN = "tok-admin-uncertain-commit"
PRODUCER_PRINCIPAL = "producer-uncertain-commit"
ADMIN_PRINCIPAL = "admin-uncertain-commit"
BASE_QUEUE_NAME = "orders.uncertain"


def _unique_queue_name() -> str:
    return f"{BASE_QUEUE_NAME}.{uuid.uuid4().hex[:12]}"


def _bindings() -> tuple[CredentialBinding, ...]:
    return (
        CredentialBinding(
            principal_id=PRODUCER_PRINCIPAL,
            role=ServiceRole.PRODUCER,
            generation_id="g1",
            secret=Secret(PRODUCER_TOKEN),
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
            PRODUCER_PRINCIPAL: frozenset({queue_name}),
            ADMIN_PRINCIPAL: frozenset({queue_name}),
        }
    )


@pytest.fixture
def session_factory(migrated_schema) -> Iterator[sessionmaker[Session]]:
    _conn, schema = migrated_schema
    database_url = os.environ.get("TEST_DATABASE_URL", "").strip()
    if not database_url:
        pytest.fail("TEST_DATABASE_URL is required for uncertain-commit conformance")
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
def schema_name(migrated_schema) -> str:
    return migrated_schema[1]


@pytest.fixture
def database_url() -> str:
    url = os.environ.get("TEST_DATABASE_URL", "").strip()
    if not url:
        pytest.fail("TEST_DATABASE_URL is required for uncertain-commit conformance")
    return url


@pytest.fixture
def app(
    session_factory: sessionmaker[Session],
    authorizer: Authorizer,
) -> Any:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
        probe.bind(("127.0.0.1", 0))
        port = int(probe.getsockname()[1])
    service = EnqueueService(
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
        bind=ListenerBind(host="127.0.0.1", port=port),
        session_factory=session_factory,
        enqueue_service=service,
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
        name="uncertain-commit-app",
        daemon=True,
    )
    thread.start()
    base_url = f"http://127.0.0.1:{port}"
    try:
        yield base_url
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
                retry_delay_seconds=5,
            ),
            metadata=_admin_meta(),
        ),
    )
    session.commit()
    return session.execute(select(Queue).where(Queue.name == name)).scalar_one()


def _intake_counts(
    session: Session, queue_name: str
) -> tuple[int, int, int, int]:
    """Return (tasks, payloads, dedups, depth)."""
    queue = session.execute(select(Queue).where(Queue.name == queue_name)).scalar_one_or_none()
    if queue is None:
        return (0, 0, 0, 0)
    tasks = int(
        session.scalar(
            select(func.count()).select_from(TaskActive).where(TaskActive.queue_id == queue.id)
        )
        or 0
    )
    payloads = int(
        session.scalar(
            select(func.count())
            .select_from(TaskPayloadActive)
            .join(TaskActive, TaskActive.id == TaskPayloadActive.task_id)
            .where(TaskActive.queue_id == queue.id)
        )
        or 0
    )
    dedup = int(
        session.scalar(
            select(func.count())
            .select_from(EnqueueDedup)
            .where(EnqueueDedup.queue_id == queue.id)
        )
        or 0
    )
    counters = session.execute(
        select(QueueCounter).where(QueueCounter.queue_id == queue.id)
    ).scalar_one_or_none()
    depth = 0 if counters is None else int(counters.ready_count) + int(counters.delayed_count)
    return tasks, payloads, dedup, depth


def _enqueue_body() -> dict[str, Any]:
    return {"payload": {"n": 1}, "priority": 0}


def _producer_headers(idempotency_key: str) -> dict[str, str]:
    return {
        "Authorization": f"Bearer {PRODUCER_TOKEN}",
        "Content-Type": "application/json",
        "Idempotency-Key": idempotency_key,
    }


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
    req = Request(url, data=body, headers=headers, method=method)
    with urlopen(req, timeout=timeout_s) as resp:
        raw = resp.read()
        header_map = {k.lower(): v for k, v in resp.headers.items()}
        return int(resp.status), header_map, raw


def test_failure_before_commit_leaves_no_task_then_retry_creates_one(
    app: Any,
    session_factory: sessionmaker[Session],
    queue_name: str,
    schema_name: str,
    database_url: str,
) -> None:
    session = session_factory()
    try:
        queue = _seed_queue(session, name=queue_name)
        queue_id = int(queue.id)
    finally:
        session.close()

    path = f"/v1/queues/{queue_name}/tasks"
    idem = f"idem-pre-commit-{uuid.uuid4().hex}"
    body = json.dumps(_enqueue_body(), separators=(",", ":")).encode("utf-8")
    headers = _producer_headers(idem)

    gate = PostgresPreCommitGate(database_url=database_url, schema=schema_name)
    deadline = time.monotonic() + 15.0
    result_box: dict[str, Any] = {}

    with _serve_app(app) as base_url:
        gate.enter(queue_id)

        def _blocked_enqueue() -> None:
            try:
                status, resp_headers, resp_body = _raw_http_exchange(
                    base_url,
                    method="POST",
                    path=path,
                    headers=headers,
                    body=body,
                    timeout_s=20.0,
                )
                result_box["ok"] = (status, resp_headers, resp_body)
            except Exception as exc:  # noqa: BLE001 — expected failure modes
                result_box["error"] = exc

        worker = threading.Thread(target=_blocked_enqueue, name="pre-commit-enqueue", daemon=True)
        worker.start()
        blocked_pid = gate.wait_until_blocked(deadline)
        assert blocked_pid != gate.gate_pid
        assert gate.blocked_query is not None
        assert "queues" in gate.blocked_query.lower()
        assert "for update" in gate.blocked_query.lower()

        gate.terminate_blocked_backend()
        worker.join(timeout=20.0)
        assert not worker.is_alive()
        assert "ok" in result_box or "error" in result_box
        if "ok" in result_box:
            status, _hdrs, resp_body = result_box["ok"]
            assert status >= 400
            # Must not look like durable enqueue success.
            if resp_body:
                payload = json.loads(resp_body.decode("utf-8"))
                assert "task" not in payload or payload.get("code")

        session = session_factory()
        try:
            assert _intake_counts(session, queue_name) == (0, 0, 0, 0)
        finally:
            session.close()

        gate.release()

        status, resp_headers, resp_body = _raw_http_exchange(
            base_url,
            method="POST",
            path=path,
            headers=headers,
            body=body,
            timeout_s=10.0,
        )
        assert status == 201
        payload = json.loads(resp_body.decode("utf-8"))
        assert payload["replayed"] is False
        task_id = payload["task"]["task_id"]
        assert isinstance(task_id, str) and task_id

        session = session_factory()
        try:
            tasks, payloads, dedup, depth = _intake_counts(session, queue_name)
            assert (tasks, payloads, dedup) == (1, 1, 1)
            assert depth == 1
        finally:
            session.close()


def test_failure_after_commit_before_response_retry_returns_same_task(
    app: Any,
    session_factory: sessionmaker[Session],
    queue_name: str,
) -> None:
    session = session_factory()
    try:
        _seed_queue(session, name=queue_name)
    finally:
        session.close()

    path = f"/v1/queues/{queue_name}/tasks"
    idem = f"idem-post-commit-{uuid.uuid4().hex}"
    body_obj = _enqueue_body()
    body = json.dumps(body_obj, separators=(",", ":")).encode("utf-8")
    headers = _producer_headers(idem)
    harness = ConformanceHarness(openapi_path=OPENAPI_PATH, base_url="http://127.0.0.1:9")

    with _serve_app(app) as upstream_url:
        deadline = time.monotonic() + 15.0
        proxy = DropCommittedResponseProxy()
        listen_url = proxy.serve_once(upstream_url, deadline)

        loss_case = RequestCase(
            operation_id="enqueueTask",
            method="POST",
            path=path,
            headers=headers,
            body=body_obj,
            expect_transport_loss=True,
        )
        # Point harness at the proxy so the producer observes connection loss.
        loss_harness = ConformanceHarness(
            openapi_path=OPENAPI_PATH,
            base_url=listen_url,
            timeout_s=10.0,
        )
        loss_report = loss_harness.run_cases([loss_case])
        assert loss_report.cases[0].outcome == CaseOutcome.EXPECTED_TRANSPORT_LOSS
        assert loss_report.suite_outcome == CaseOutcome.HARNESS_ERROR
        assert any(
            f.code == "expected_transport_loss_without_durable_proof"
            for f in loss_report.findings
        )

        buffered = proxy.wait(deadline)
        assert buffered.status_code == 201
        upstream_payload = buffered.json()
        original_task_id = upstream_payload["task"]["task_id"]
        assert original_task_id
        proxy.close()

        session = session_factory()
        try:
            assert _intake_counts(session, queue_name) == (1, 1, 1, 1)
        finally:
            session.close()

        retry_case = RequestCase(
            operation_id="enqueueTask",
            method="POST",
            path=path,
            headers=headers,
            body=body_obj,
        )
        retry_harness = ConformanceHarness(
            openapi_path=OPENAPI_PATH,
            base_url=upstream_url,
            timeout_s=10.0,
        )
        # Combined suite: expected loss evidence already observed; retry must PASS.
        combined = retry_harness.run_cases([retry_case])
        assert combined.cases[0].outcome == CaseOutcome.PASS
        # Re-run loss+retry as one harness suite to prove EXPECTED_TRANSPORT_LOSS
        # is not treated as success without durable PASS.
        # (loss already consumed the one-shot proxy; prove durable via retry alone
        # then assert identity.)
        retry_status, retry_headers, retry_body = _raw_http_exchange(
            upstream_url,
            method="POST",
            path=path,
            headers=headers,
            body=body,
            timeout_s=10.0,
        )
        assert retry_status == 200
        retry_payload = json.loads(retry_body.decode("utf-8"))
        assert retry_payload["replayed"] is True
        assert retry_payload["task"]["task_id"] == original_task_id
        findings = harness._validate_response(  # noqa: SLF001
            "enqueueTask",
            ObservedResponse(
                status=retry_status,
                headers=dict(retry_headers),
                body_text=retry_body.decode("utf-8"),
                body_json=retry_payload,
                content_type=retry_headers.get("content-type", "application/json"),
            ),
        )
        assert findings == [], findings

        session = session_factory()
        try:
            tasks, payloads, dedup, depth = _intake_counts(session, queue_name)
            assert (tasks, payloads, dedup) == (1, 1, 1)
            assert depth == 1
        finally:
            session.close()


def test_ordinary_transport_failure_remains_harness_error() -> None:
    harness = ConformanceHarness(
        openapi_path=OPENAPI_PATH,
        base_url="http://127.0.0.1:1",
        timeout_s=0.3,
    )
    report = harness.run_cases(
        [
            RequestCase(
                operation_id="enqueueTask",
                method="POST",
                path="/v1/queues/demo/tasks",
                headers=_producer_headers("idem-ordinary-loss"),
                body=_enqueue_body(),
            )
        ]
    )
    assert report.cases[0].outcome == CaseOutcome.HARNESS_ERROR
    assert report.exit_code != 0


def test_expected_transport_loss_alone_is_not_suite_success() -> None:
    """EXPECTED_TRANSPORT_LOSS without a durable PASS remains HARNESS_ERROR."""
    harness = ConformanceHarness(
        openapi_path=OPENAPI_PATH,
        base_url="http://127.0.0.1:1",
        timeout_s=0.2,
    )
    loss = RequestCase(
        operation_id="enqueueTask",
        method="POST",
        path="/v1/queues/demo/tasks",
        headers=_producer_headers("idem-loss-alone"),
        body=_enqueue_body(),
        expect_transport_loss=True,
    )
    report = harness.run_cases([loss])
    assert report.cases[0].outcome == CaseOutcome.EXPECTED_TRANSPORT_LOSS
    assert report.suite_outcome == CaseOutcome.HARNESS_ERROR
    assert report.exit_code != 0
    assert any(
        f.code == "expected_transport_loss_without_durable_proof"
        for f in report.findings
    )
