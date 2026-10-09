"""Live long-poll conformance composition (Plan 20.1-06).

Boots one or two API replicas over a shared migrated schema with
``ClaimLongPollService`` + ``ClaimWakeListener``. Adapters cover raw HTTP,
sync ``ConsumerClient`` and async ``AsyncConsumerClient``.
"""

from __future__ import annotations

import asyncio
import os
import socket
import threading
import time
import uuid
from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any, Literal
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

import pytest
from sqlalchemy import create_engine, event, select
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker

from _workhold_client_core.config import ClientConfig
from _workhold_client_core.errors import (
    ProtocolError,
    RequestCancelledError,
    TimeoutError as ClientTimeoutError,
)
from _workhold_client_core.transport import HttpJsonTransport
from workhold.api.application import create_application_app
from workhold.api.security import ListenerBind
from workhold.application.claim_long_poll import ClaimLongPollService, WaiterAdmission
from workhold.application.claim_service import ClaimService
from workhold.application.completion import CompletionService
from workhold.application.lease_service import LeaseService
from workhold.domain.queue_control import QueueState
from workhold.infrastructure.postgres.claim_wakeup import (
    ClaimWakeListener,
    ListenerHealth,
    QueueGenerationCoordinator,
)
from workhold.intake.depth import DepthCeilings
from workhold.intake.service import EnqueueService
from workhold.lifecycle import Lifecycle
from workhold.roles.api import AsgiRequestHandler, InFlightGate, QuietThreadingHTTPServer
from workhold.security.authorization import Authorizer
from workhold.security.credentials import BearerCredentialAuthenticator
from workhold.settings import (
    CLAIM_MAX_OUTSTANDING_WAITS_DEFAULT,
    CLAIM_MAX_WAIT_SECONDS_DEFAULT,
)
from workhold.storage.models import Queue
from workhold_consumer import ConsumerClient, DEFAULT_WAIT_SECONDS
from workhold_consumer.async_client import AsyncConsumerClient
from tests.conformance.clients import OperationResult, RawHttpClientAdapter
from tests.conformance.conftest import (
    PRODUCER_PRINCIPAL,
    PRODUCER_TOKEN,
    WORKER_PRINCIPAL,
    WORKER_TOKEN,
    _bindings,
    seed_queue,
    set_queue_state,
)
from tests.fixtures.claim_long_poll import (
    ListenerConnectionProbe,
    PoolCheckoutProbe,
    assert_no_forbidden_diagnostics,
)
from tests.integration.conftest import to_psycopg_conninfo

LongPollAdapterKind = Literal[
    "raw_http",
    "sync_consumer_client",
    "async_consumer_client",
]

LONG_POLL_ADAPTERS: tuple[LongPollAdapterKind, ...] = (
    "raw_http",
    "sync_consumer_client",
    "async_consumer_client",
)

PROXY_UPSTREAM_TIMEOUT_FLOOR_SECONDS = 30
SUPERVISOR_DEFAULT_WAIT_SECONDS = DEFAULT_WAIT_SECONDS


@dataclass
class LongPollReplica:
    """One API process composition sharing a PostgreSQL schema."""

    name: str
    base_url: str
    server: QuietThreadingHTTPServer
    thread: threading.Thread
    lifecycle: Lifecycle
    coordinator: QueueGenerationCoordinator
    admission: WaiterAdmission
    listener: ClaimWakeListener
    engine: Any
    session_factory: sessionmaker[Session]
    pool_probe: PoolCheckoutProbe
    listener_probe: ListenerConnectionProbe


@dataclass
class LongPollWorld:
    """Shared schema + one or two live replicas for long-poll qualification."""

    schema: str
    queue_name: str
    session_factory: sessionmaker[Session]
    replicas: list[LongPollReplica] = field(default_factory=list)

    @property
    def primary(self) -> LongPollReplica:
        return self.replicas[0]


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
        probe.bind(("127.0.0.1", 0))
        return int(probe.getsockname()[1])


def _wake_dsn(database_url: str, schema: str) -> str:
    base = to_psycopg_conninfo(database_url)
    separator = "&" if "?" in base else "?"
    return f"{base}{separator}options=-csearch_path%3D{schema}"


def _build_authorizer(queue_name: str) -> Authorizer:
    scoped = frozenset({queue_name})
    return Authorizer(
        queue_scopes={
            PRODUCER_PRINCIPAL: scoped,
            WORKER_PRINCIPAL: scoped,
            "admin-kernel-dual": scoped,
            "observer-kernel-dual": scoped,
            "foreign-kernel-dual": frozenset({f"other.{uuid.uuid4().hex[:8]}"}),
        }
    )


def _attach_pool_probe(engine: Any, probe: PoolCheckoutProbe) -> None:
    @event.listens_for(engine, "checkout")
    def _on_checkout(_dbapi_conn, _conn_rec, _proxy) -> None:  # noqa: ANN001
        probe.checkout()

    @event.listens_for(engine, "checkin")
    def _on_checkin(_dbapi_conn, _conn_rec) -> None:  # noqa: ANN001
        probe.checkin()


def _start_replica(
    *,
    name: str,
    session_factory: sessionmaker[Session],
    engine: Engine,
    authorizer: Authorizer,
    database_url: str,
    schema: str,
    max_outstanding_waits: int = CLAIM_MAX_OUTSTANDING_WAITS_DEFAULT,
    max_wait_seconds: int = CLAIM_MAX_WAIT_SECONDS_DEFAULT,
) -> LongPollReplica:
    coordinator = QueueGenerationCoordinator()
    admission = WaiterAdmission(max_outstanding_waits)
    long_poll = ClaimLongPollService(
        coordinator=coordinator,
        admission=admission,
        fallback_seconds=1.0,
        probe_seconds=0.25,
    )
    pool_probe = PoolCheckoutProbe()
    _attach_pool_probe(engine, pool_probe)
    listener_probe = ListenerConnectionProbe()

    port = _free_port()
    app = create_application_app(
        authenticator=BearerCredentialAuthenticator.from_bindings(_bindings()),
        authorizer=authorizer,
        bind=ListenerBind(host="127.0.0.1", port=port),
        session_factory=session_factory,
        enqueue_service=EnqueueService(
            session_factory=session_factory,
            depth_ceilings=DepthCeilings(
                queue_active_depth=1000,
                instance_active_depth=5000,
                retry_after_ms=250,
            ),
        ),
        claim_service=ClaimService(session_factory=session_factory),
        claim_long_poll_service=long_poll,
        max_wait_seconds=max_wait_seconds,
        lease_service=LeaseService(session_factory=session_factory),
        completion_service=CompletionService(session_factory=session_factory),
    )
    lifecycle = Lifecycle()
    gate = InFlightGate()
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
    # Admit concurrent long-poll qualification (default waiter cap is 64).
    server.request_queue_size = 128
    thread = threading.Thread(
        target=server.serve_forever,
        kwargs={"poll_interval": 0.05},
        name=f"long-poll-{name}",
        daemon=True,
    )
    thread.start()

    dsn = _wake_dsn(database_url, schema)
    listener = ClaimWakeListener(
        dsn,
        coordinator,
        notify_poll_seconds=0.05,
    )
    listener_probe.open_listener()
    listener.start()

    return LongPollReplica(
        name=name,
        base_url=f"http://127.0.0.1:{port}",
        server=server,
        thread=thread,
        lifecycle=lifecycle,
        coordinator=coordinator,
        admission=admission,
        listener=listener,
        engine=engine,
        session_factory=session_factory,
        pool_probe=pool_probe,
        listener_probe=listener_probe,
    )


def _stop_replica(replica: LongPollReplica) -> None:
    try:
        replica.lifecycle.request_stop(grace_seconds=1.0)
    except Exception:  # noqa: BLE001
        pass
    try:
        replica.listener.stop()
    except Exception:  # noqa: BLE001
        pass
    replica.listener_probe.close_listener()
    try:
        replica.server.shutdown()
    except Exception:  # noqa: BLE001
        pass
    try:
        replica.server.server_close()
    except Exception:  # noqa: BLE001
        pass
    replica.thread.join(timeout=2.0)


@contextmanager
def long_poll_world(
    migrated_schema: tuple[Any, str],
    *,
    replica_count: int = 1,
    max_outstanding_waits: int = CLAIM_MAX_OUTSTANDING_WAITS_DEFAULT,
) -> Iterator[LongPollWorld]:
    """Yield a live long-poll world over the migrated schema."""

    database_url = os.environ.get("TEST_DATABASE_URL", "").strip()
    if not database_url:
        pytest.fail("TEST_DATABASE_URL is required for live long-poll conformance")
    _conn, schema = migrated_schema
    queue_name = f"lp.live.{uuid.uuid4().hex[:12]}"
    engine = create_engine(database_url, pool_pre_ping=True, pool_size=8, max_overflow=16)

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
    session = factory()
    try:
        seed_queue(session, name=queue_name)
    finally:
        session.close()

    authorizer = _build_authorizer(queue_name)
    world = LongPollWorld(
        schema=schema,
        queue_name=queue_name,
        session_factory=factory,
    )
    try:
        for index in range(replica_count):
            world.replicas.append(
                _start_replica(
                    name=f"r{index}",
                    session_factory=factory,
                    engine=engine,
                    authorizer=authorizer,
                    database_url=database_url,
                    schema=schema,
                    max_outstanding_waits=max_outstanding_waits,
                )
            )
            # Allow listener to connect before issuing waits.
            deadline = time.monotonic() + 5.0
            while time.monotonic() < deadline:
                if world.replicas[-1].listener.health is ListenerHealth.CONNECTED:
                    break
                time.sleep(0.05)
        yield world
    finally:
        for replica in reversed(world.replicas):
            _stop_replica(replica)
        engine.dispose()


def enqueue_task(
    *,
    base_url: str,
    queue_name: str,
    payload: dict[str, Any],
    available_at: datetime | None = None,
    priority: int = 0,
) -> str:
    body: dict[str, Any] = {"payload": payload, "priority": priority}
    if available_at is not None:
        body["available_at"] = available_at.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
    raw = json_post(
        base_url,
        f"/v1/queues/{queue_name}/tasks",
        bearer=PRODUCER_TOKEN,
        body=body,
        extra_headers={"Idempotency-Key": f"idem-{uuid.uuid4().hex}"},
    )
    assert raw["ok"] is True, raw
    return str(raw["data"]["task"]["task_id"])


def json_post(
    base_url: str,
    path: str,
    *,
    bearer: str,
    body: dict[str, Any],
    timeout_s: float = 30.0,
    extra_headers: dict[str, str] | None = None,
) -> dict[str, Any]:
    import json as _json

    headers = {
        "Authorization": f"Bearer {bearer}",
        "Content-Type": "application/json",
    }
    if extra_headers:
        headers.update(extra_headers)
    raw_body = _json.dumps(body, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    request = Request(
        f"{base_url.rstrip('/')}{path}",
        data=raw_body,
        headers=headers,
        method="POST",
    )
    try:
        with urlopen(request, timeout=timeout_s) as response:
            payload = _json.loads(response.read().decode("utf-8") or "{}")
            return {"ok": True, "status": int(response.status), "data": payload}
    except HTTPError as exc:
        raw = exc.read() if exc.fp is not None else b""
        try:
            payload = _json.loads(raw.decode("utf-8") or "{}")
        except Exception:  # noqa: BLE001
            payload = {}
        return {
            "ok": False,
            "status": int(exc.code),
            "data": payload,
            "error_code": payload.get("code") if isinstance(payload, dict) else None,
        }
    except (URLError, TimeoutError, OSError) as exc:
        return {"ok": False, "status": 0, "data": {"reason": type(exc).__name__}}


def _claim_wire_from_sdk(claims: Sequence[Any]) -> dict[str, Any]:
    tasks: list[dict[str, Any]] = []
    for claim in claims:
        task = claim.task
        token = getattr(claim, "_claim_token", None)
        tasks.append(
            {
                "task": {
                    "task_id": task.task_id,
                    "queue_name": task.queue_name,
                    "state": getattr(task.state, "value", str(task.state)),
                    "priority": task.priority,
                    "payload": task.payload,
                },
                "claim": {
                    "claim_id": claim.claim_id,
                    "claim_token": token,
                    "generation": claim.generation,
                },
            }
        )
    return {"tasks": tasks}


class SyncConsumerLongPollAdapter:
    """Sync ConsumerClient adapter for the long-poll scenario matrix."""

    kind: LongPollAdapterKind = "sync_consumer_client"

    def __init__(self, base_url: str, *, bearer_token: str = WORKER_TOKEN) -> None:
        # read/total must admit wait=20 → wait+5 / wait+10.
        config = ClientConfig.for_public(
            base_url,
            read_timeout_s=25.0,
            total_timeout_s=30.0,
        )
        self._client = ConsumerClient(
            HttpJsonTransport.from_config(config),
            bearer_token=bearer_token,
        )
        self.base_url = base_url.rstrip("/")

    def claim(
        self,
        *,
        queues: Sequence[str],
        worker_id: str,
        lease_seconds: int = 30,
        wait_seconds: int = 0,
    ) -> OperationResult:
        try:
            caps = self._client.get_capabilities() if wait_seconds > 0 else None
            claims = self._client.claim(
                queues=queues,
                worker_id=worker_id,
                lease_seconds=lease_seconds,
                wait_seconds=wait_seconds,
                capabilities=caps,
            )
        except ClientTimeoutError:
            return OperationResult(ok=False, error_code="timeout", data={})
        except RequestCancelledError:
            return OperationResult(ok=False, error_code="cancelled", data={})
        except (ProtocolError, ValueError) as exc:
            code = getattr(exc, "code", None) or "protocol_error"
            return OperationResult(ok=False, error_code=str(code), data={"reason": str(exc)})
        return OperationResult(ok=True, data=_claim_wire_from_sdk(claims), typed=claims)


class AsyncConsumerLongPollAdapter:
    """Async ConsumerClient adapter for the long-poll scenario matrix."""

    kind: LongPollAdapterKind = "async_consumer_client"

    def __init__(self, base_url: str, *, bearer_token: str = WORKER_TOKEN) -> None:
        self.base_url = base_url.rstrip("/")
        self._bearer_token = bearer_token

    def claim(
        self,
        *,
        queues: Sequence[str],
        worker_id: str,
        lease_seconds: int = 30,
        wait_seconds: int = 0,
    ) -> OperationResult:
        async def _run() -> OperationResult:
            async with AsyncConsumerClient.from_url(
                self.base_url,
                bearer_token=self._bearer_token,
                timeout_s=30.0,
            ) as client:
                try:
                    caps = await client.get_capabilities() if wait_seconds > 0 else None
                    claims = await client.claim(
                        queues=queues,
                        worker_id=worker_id,
                        lease_seconds=lease_seconds,
                        wait_seconds=wait_seconds,
                        capabilities=caps,
                    )
                except ClientTimeoutError:
                    return OperationResult(ok=False, error_code="timeout", data={})
                except asyncio.CancelledError:
                    return OperationResult(ok=False, error_code="cancelled", data={})
                except (ProtocolError, ValueError) as exc:
                    code = getattr(exc, "code", None) or "protocol_error"
                    return OperationResult(
                        ok=False, error_code=str(code), data={"reason": str(exc)}
                    )
                return OperationResult(
                    ok=True, data=_claim_wire_from_sdk(claims), typed=claims
                )

        return asyncio.run(_run())


def build_long_poll_adapter(
    kind: LongPollAdapterKind,
    base_url: str,
) -> RawHttpClientAdapter | SyncConsumerLongPollAdapter | AsyncConsumerLongPollAdapter:
    if kind == "raw_http":
        return RawHttpClientAdapter(base_url, timeout_s=30.0)
    if kind == "sync_consumer_client":
        return SyncConsumerLongPollAdapter(base_url)
    if kind == "async_consumer_client":
        return AsyncConsumerLongPollAdapter(base_url)
    raise ValueError(f"unknown long-poll adapter: {kind!r}")


def claim_via_adapter(
    adapter: Any,
    *,
    queues: Sequence[str],
    worker_id: str,
    lease_seconds: int = 30,
    wait_seconds: int = 0,
    bearer_token: str = WORKER_TOKEN,
) -> OperationResult:
    if isinstance(adapter, RawHttpClientAdapter):
        return adapter.claim(
            queues=queues,
            worker_id=worker_id,
            lease_seconds=lease_seconds,
            bearer_token=bearer_token,
            wait_seconds=wait_seconds,
        )
    return adapter.claim(
        queues=queues,
        worker_id=worker_id,
        lease_seconds=lease_seconds,
        wait_seconds=wait_seconds,
    )


def pause_queue(session_factory: sessionmaker[Session], queue_name: str) -> None:
    session = session_factory()
    try:
        queue = session.execute(select(Queue).where(Queue.name == queue_name)).scalar_one()
        set_queue_state(
            session,
            queue_name=queue_name,
            state=QueueState.PAUSED,
            expected_config_version=int(queue.config_version),
        )
    finally:
        session.close()


def resume_queue(session_factory: sessionmaker[Session], queue_name: str) -> None:
    session = session_factory()
    try:
        queue = session.execute(select(Queue).where(Queue.name == queue_name)).scalar_one()
        set_queue_state(
            session,
            queue_name=queue_name,
            state=QueueState.ACTIVE,
            expected_config_version=int(queue.config_version),
        )
    finally:
        session.close()


def drain_queue(session_factory: sessionmaker[Session], queue_name: str) -> None:
    session = session_factory()
    try:
        queue = session.execute(select(Queue).where(Queue.name == queue_name)).scalar_one()
        set_queue_state(
            session,
            queue_name=queue_name,
            state=QueueState.DRAINING,
            expected_config_version=int(queue.config_version),
        )
    finally:
        session.close()


def assert_capability_contract(base_url: str) -> None:
    import json as _json

    request = Request(
        f"{base_url.rstrip('/')}/v1/capabilities",
        headers={"Authorization": f"Bearer {WORKER_TOKEN}"},
        method="GET",
    )
    with urlopen(request, timeout=5.0) as response:
        payload = _json.loads(response.read().decode("utf-8"))
    assert payload["long_polling"] is True
    assert payload["max_wait_seconds"] == CLAIM_MAX_WAIT_SECONDS_DEFAULT
    assert payload["batch_claim"] is False
    assert payload["max_claim_tasks"] == 1
    assert SUPERVISOR_DEFAULT_WAIT_SECONDS == 15
    assert PROXY_UPSTREAM_TIMEOUT_FLOOR_SECONDS >= CLAIM_MAX_WAIT_SECONDS_DEFAULT + 10
    rendered = _json.dumps(payload)
    assert_no_forbidden_diagnostics(rendered)


__all__ = [
    "LONG_POLL_ADAPTERS",
    "LongPollAdapterKind",
    "LongPollWorld",
    "PROXY_UPSTREAM_TIMEOUT_FLOOR_SECONDS",
    "SUPERVISOR_DEFAULT_WAIT_SECONDS",
    "assert_capability_contract",
    "build_long_poll_adapter",
    "claim_via_adapter",
    "drain_queue",
    "enqueue_task",
    "long_poll_world",
    "pause_queue",
    "resume_queue",
]
