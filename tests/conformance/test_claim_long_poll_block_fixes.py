"""Live BLOCK remediations for plan 20.1-06."""

from __future__ import annotations

import asyncio
import http.client
import socket
import json
import threading
import time
import uuid

import pytest
from sqlalchemy import select

from _queue_service_client_core.config import ClientConfig
from _queue_service_client_core.errors import RequestCancelledError
from _queue_service_client_core.transport import HttpJsonTransport
from queue_service.domain.queue_control import (
    ActivatePolicyMutation,
    AdminRequestMetadata,
    BackoffStrategy,
    ConfigVersion,
    CreatePolicyMutation,
    PolicyVersion,
    RetryPolicyDraft,
)
from queue_service.infrastructure.postgres.queue_control_repository import (
    QueueControlRepository,
)
from queue_service.settings import CLAIM_MAX_WAIT_SECONDS_DEFAULT
from queue_service.storage.models import Queue, QueuePolicyVersion
from queue_service_consumer import ConsumerClient, ConsumerSupervisor
from queue_service_consumer.async_client import AsyncConsumerClient
from queue_service_consumer.async_supervisor import AsyncConsumerSupervisor
from tests.conformance.clients import RawHttpClientAdapter
from tests.conformance.conftest import WORKER_TOKEN
from tests.conformance.long_poll_harness import (
    LONG_POLL_ADAPTERS,
    build_long_poll_adapter,
    claim_via_adapter,
    enqueue_task,
    long_poll_world,
)

pytest_plugins = ["tests.integration.conftest"]


@pytest.fixture
def long_poll_single(migrated_schema):
    with long_poll_world(migrated_schema, replica_count=1) as world:
        yield world


def _activate_short_retry_delay(*, session_factory, queue_name: str, delay_seconds: int = 2) -> None:
    session = session_factory()
    try:
        queue = session.execute(select(Queue).where(Queue.name == queue_name)).scalar_one()
        repo = QueueControlRepository()
        request_meta = AdminRequestMetadata(
            actor_id="lp-retry-due",
            request_id=str(uuid.uuid4()),
            idempotency_key=f"idem-{uuid.uuid4().hex}",
        )
        repo.create_policy_version(
            session,
            queue_name=queue_name,
            mutation=CreatePolicyMutation(
                policy=RetryPolicyDraft(
                    enabled=True,
                    max_attempts=5,
                    backoff_strategy=BackoffStrategy.FIXED,
                    retry_delay_seconds=delay_seconds,
                ),
                metadata=request_meta,
            ),
        )
        session.commit()
        session.refresh(queue)
        next_version = session.execute(
            select(QueuePolicyVersion.version)
            .where(QueuePolicyVersion.queue_id == queue.id)
            .order_by(QueuePolicyVersion.version.desc())
            .limit(1)
        ).scalar_one()
        repo.activate_policy_version(
            session,
            queue_name=queue_name,
            mutation=ActivatePolicyMutation(
                expected_config_version=ConfigVersion(value=int(queue.config_version)),
                policy_version=PolicyVersion(value=int(next_version)),
                metadata=AdminRequestMetadata(
                    actor_id="lp-retry-due",
                    request_id=str(uuid.uuid4()),
                    idempotency_key=f"idem-{uuid.uuid4().hex}",
                ),
            ),
        )
        session.commit()
    finally:
        session.close()


@pytest.mark.parametrize("adapter_kind", LONG_POLL_ADAPTERS)
def test_disconnect_during_wait(long_poll_single, adapter_kind: str) -> None:
    world = long_poll_single
    holder: dict[str, object] = {}

    if adapter_kind == "raw_http":
        host = world.primary.base_url.removeprefix("http://").split("/")[0]
        hostname, port_s = host.split(":")
        port = int(port_s)
        body = json.dumps(
            {
                "queues": [world.queue_name],
                "max_tasks": 1,
                "lease_seconds": 30,
                "wait_seconds": 8,
                "worker_id": "w-disconnect-raw",
            },
            separators=(",", ":"),
        ).encode("utf-8")

        def _run() -> None:
            conn = http.client.HTTPConnection(hostname, port, timeout=15.0)
            holder["conn"] = conn
            try:
                conn.request(
                    "POST",
                    "/v1/claims",
                    body=body,
                    headers={
                        "Authorization": f"Bearer {WORKER_TOKEN}",
                        "Content-Type": "application/json",
                        "Content-Length": str(len(body)),
                    },
                )
                holder["started"] = True
                try:
                    response = conn.getresponse()
                    holder["status"] = int(response.status)
                    response.read()
                except Exception as exc:  # noqa: BLE001
                    holder["error"] = type(exc).__name__
            finally:
                try:
                    conn.close()
                except Exception:  # noqa: BLE001
                    pass

        thread = threading.Thread(target=_run, daemon=True)
        thread.start()
        deadline = time.monotonic() + 2.0
        while time.monotonic() < deadline and not holder.get("started"):
            time.sleep(0.05)
        time.sleep(0.3)
        conn = holder.get("conn")
        assert conn is not None
        sock = getattr(conn, "sock", None)
        if sock is not None:
            try:
                sock.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
        try:
            conn.close()
        except OSError:
            pass
        thread.join(timeout=8.0)
        assert not thread.is_alive()
        assert holder.get("status") != 200
    elif adapter_kind == "sync_consumer_client":
        config = ClientConfig.for_public(
            world.primary.base_url,
            read_timeout_s=25.0,
            total_timeout_s=30.0,
        )
        transport = HttpJsonTransport.from_config(config)
        client = ConsumerClient(transport, bearer_token=WORKER_TOKEN)
        cancel = threading.Event()

        def _run() -> None:
            try:
                caps = getattr(client, "get_capabilities")()
                holder["result"] = client.claim(
                    queues=[world.queue_name],
                    worker_id="w-disconnect-sync",
                    lease_seconds=30,
                    wait_seconds=8,
                    capabilities=caps,
                    cancellation=cancel,
                )
            except RequestCancelledError:
                holder["cancelled"] = True
            except Exception as exc:  # noqa: BLE001
                holder["error"] = type(exc).__name__

        thread = threading.Thread(target=_run, daemon=True)
        thread.start()
        time.sleep(0.5)
        cancel.set()
        getattr(transport, "cancel_active")()
        thread.join(timeout=8.0)
        assert not thread.is_alive()
        assert holder.get("cancelled") is True or "error" in holder
        assert "result" not in holder
    else:

        async def _run() -> None:
            async with AsyncConsumerClient.from_url(
                world.primary.base_url,
                bearer_token=WORKER_TOKEN,
                timeout_s=30.0,
            ) as client:
                task = asyncio.create_task(
                    client.claim(
                        queues=[world.queue_name],
                        worker_id="w-disconnect-async",
                        lease_seconds=30,
                        wait_seconds=8,
                        capabilities=await getattr(client, "get_capabilities")(),
                    )
                )
                await asyncio.sleep(0.4)
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    holder["cancelled"] = True
                except Exception as exc:  # noqa: BLE001
                    holder["error"] = type(exc).__name__

        asyncio.run(_run())
        assert holder.get("cancelled") is True or "error" in holder


@pytest.mark.parametrize("adapter_kind", LONG_POLL_ADAPTERS)
def test_retry_due_wake(long_poll_single, adapter_kind: str) -> None:
    world = long_poll_single
    _activate_short_retry_delay(
        session_factory=world.session_factory,
        queue_name=world.queue_name,
        delay_seconds=2,
    )
    task_id = enqueue_task(
        base_url=world.primary.base_url,
        queue_name=world.queue_name,
        payload={"scenario": "retry-due", "adapter": adapter_kind},
    )
    raw = RawHttpClientAdapter(world.primary.base_url, timeout_s=10.0)
    first = raw.claim(
        queues=[world.queue_name],
        worker_id="w-retry-holder",
        lease_seconds=30,
        bearer_token=WORKER_TOKEN,
        wait_seconds=0,
    )
    assert first.ok is True, first
    claim = first.data["tasks"][0]["claim"]
    failed = raw.fail(
        claim_id=claim["claim_id"],
        claim_token=claim["claim_token"],
        generation=claim["generation"],
        bearer_token=WORKER_TOKEN,
        retryable=True,
        failure_code="worker.retry_due",
    )
    assert failed.ok is True, failed

    adapter = build_long_poll_adapter(adapter_kind, world.primary.base_url)
    started = time.monotonic()
    result = claim_via_adapter(
        adapter,
        queues=[world.queue_name],
        worker_id=f"w-{adapter_kind}-retry-due",
        wait_seconds=8,
    )
    assert result.ok is True, result
    tasks = result.data["tasks"]
    assert len(tasks) == 1
    assert tasks[0]["task"]["task_id"] == task_id
    assert time.monotonic() - started >= 1.0


@pytest.mark.parametrize("adapter_kind", LONG_POLL_ADAPTERS)
def test_wait_seconds_20_success_boundary(long_poll_single, adapter_kind: str) -> None:
    world = long_poll_single
    adapter = build_long_poll_adapter(adapter_kind, world.primary.base_url)
    holder: dict[str, object] = {}

    def _claim() -> None:
        holder["result"] = claim_via_adapter(
            adapter,
            queues=[world.queue_name],
            worker_id=f"w-{adapter_kind}-max-success",
            wait_seconds=CLAIM_MAX_WAIT_SECONDS_DEFAULT,
        )

    thread = threading.Thread(target=_claim, daemon=True)
    thread.start()
    time.sleep(0.3)
    task_id = enqueue_task(
        base_url=world.primary.base_url,
        queue_name=world.queue_name,
        payload={"scenario": "wait-20-success", "adapter": adapter_kind},
    )
    thread.join(timeout=15.0)
    assert not thread.is_alive()
    result = holder["result"]
    assert result.ok is True, result  # type: ignore[union-attr]
    tasks = result.data["tasks"]  # type: ignore[index]
    assert len(tasks) == 1
    assert tasks[0]["task"]["task_id"] == task_id


def test_wait_seconds_20_empty_boundary(long_poll_single) -> None:
    world = long_poll_single
    adapter = build_long_poll_adapter("raw_http", world.primary.base_url)
    started = time.monotonic()
    result = claim_via_adapter(
        adapter,
        queues=[world.queue_name],
        worker_id="w-max-empty",
        wait_seconds=CLAIM_MAX_WAIT_SECONDS_DEFAULT,
    )
    elapsed = time.monotonic() - started
    assert result.ok is True, result
    assert result.data["tasks"] == []
    assert result.error_code is None
    assert elapsed >= CLAIM_MAX_WAIT_SECONDS_DEFAULT - 0.5


@pytest.mark.parametrize("adapter_kind", ("sync_consumer_client", "async_consumer_client"))
def test_client_cancel_distinct_from_empty(long_poll_single, adapter_kind: str) -> None:
    world = long_poll_single

    if adapter_kind == "sync_consumer_client":
        config = ClientConfig.for_public(
            world.primary.base_url,
            read_timeout_s=25.0,
            total_timeout_s=30.0,
        )
        transport = HttpJsonTransport.from_config(config)
        client = ConsumerClient(transport, bearer_token=WORKER_TOKEN)
        holder: dict[str, object] = {}
        cancel = threading.Event()

        def _run() -> None:
            try:
                caps = getattr(client, "get_capabilities")()
                holder["result"] = client.claim(
                    queues=[world.queue_name],
                    worker_id="w-cancel-sync",
                    lease_seconds=30,
                    wait_seconds=8,
                    capabilities=caps,
                    cancellation=cancel,
                )
            except RequestCancelledError:
                holder["cancelled"] = True
            except Exception as exc:  # noqa: BLE001
                holder["error"] = type(exc).__name__

        thread = threading.Thread(target=_run, daemon=True)
        thread.start()
        time.sleep(0.5)
        cancel.set()
        getattr(transport, "cancel_active")()
        thread.join(timeout=8.0)
        assert not thread.is_alive()
        assert holder.get("cancelled") is True or holder.get("error") in {
            "ProtocolError",
            "TransportError",
            "OSError",
            "URLError",
        }
        assert "result" not in holder
    else:

        async def _run() -> None:
            async with AsyncConsumerClient.from_url(
                world.primary.base_url,
                bearer_token=WORKER_TOKEN,
                timeout_s=30.0,
            ) as client:
                task = asyncio.create_task(
                    client.claim(
                        queues=[world.queue_name],
                        worker_id="w-cancel-async",
                        lease_seconds=30,
                        wait_seconds=8,
                        capabilities=await getattr(client, "get_capabilities")(),
                    )
                )
                await asyncio.sleep(0.4)
                task.cancel()
                with pytest.raises(asyncio.CancelledError):
                    await task

        asyncio.run(_run())


def test_sync_supervisor_live_arrival(long_poll_single) -> None:
    world = long_poll_single
    config = ClientConfig.for_public(
        world.primary.base_url,
        read_timeout_s=25.0,
        total_timeout_s=30.0,
    )
    client = ConsumerClient(
        HttpJsonTransport.from_config(config),
        bearer_token=WORKER_TOKEN,
    )
    got: dict[str, object] = {}
    supervisor: ConsumerSupervisor

    def handler(claim, token) -> None:  # noqa: ANN001
        got["task_id"] = claim.task.task_id
        getattr(supervisor, "request_shutdown")()

    supervisor = ConsumerSupervisor(
        client,
        queues=[world.queue_name],
        worker_id="w-sync-supervisor",
        lease_seconds=30,
        handler=handler,
        wait_seconds=5,
    )
    thread = threading.Thread(target=supervisor.run, daemon=True)
    thread.start()
    time.sleep(0.3)
    task_id = enqueue_task(
        base_url=world.primary.base_url,
        queue_name=world.queue_name,
        payload={"scenario": "sync-supervisor"},
    )
    thread.join(timeout=12.0)
    assert not thread.is_alive()
    assert got.get("task_id") == task_id


def test_async_supervisor_live_arrival(long_poll_single) -> None:
    world = long_poll_single
    got: dict[str, object] = {}

    async def _run() -> None:
        async with AsyncConsumerClient.from_url(
            world.primary.base_url,
            bearer_token=WORKER_TOKEN,
            timeout_s=30.0,
        ) as client:
            supervisor: AsyncConsumerSupervisor

            async def handler(claim, token) -> None:  # noqa: ANN001
                got["task_id"] = claim.task.task_id
                getattr(supervisor, "request_shutdown")()

            supervisor = AsyncConsumerSupervisor(
                client,
                queues=[world.queue_name],
                worker_id="w-async-supervisor",
                lease_seconds=30,
                handler=handler,
                wait_seconds=5,
            )

            async def _enqueue_later() -> None:
                await asyncio.sleep(0.3)
                enqueue_task(
                    base_url=world.primary.base_url,
                    queue_name=world.queue_name,
                    payload={"scenario": "async-supervisor"},
                )

            enq = asyncio.create_task(_enqueue_later())
            await supervisor.run()
            await enq

    asyncio.run(_run())
    assert got.get("task_id")
