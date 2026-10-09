"""Phase 20.1-06: multi-replica wake, competition and listener qualification."""

from __future__ import annotations

import threading
import time

import pytest

from queue_service.infrastructure.postgres.claim_wakeup import ListenerHealth
from tests.conformance.long_poll_harness import (
    build_long_poll_adapter,
    claim_via_adapter,
    enqueue_task,
    long_poll_world,
)

pytest_plugins = ["tests.integration.conftest"]


@pytest.fixture
def long_poll_pair(migrated_schema):
    with long_poll_world(migrated_schema, replica_count=2) as world:
        yield world


def test_replica_a_enqueue_wakes_replica_b_waiter(long_poll_pair) -> None:
    world = long_poll_pair
    replica_a, replica_b = world.replicas
    adapter = build_long_poll_adapter("raw_http", replica_b.base_url)
    holder: dict[str, object] = {}

    def _wait() -> None:
        holder["result"] = claim_via_adapter(
            adapter,
            queues=[world.queue_name],
            worker_id="w-replica-b",
            wait_seconds=8,
        )

    thread = threading.Thread(target=_wait, daemon=True)
    thread.start()
    time.sleep(0.4)
    task_id = enqueue_task(
        base_url=replica_a.base_url,
        queue_name=world.queue_name,
        payload={"scenario": "cross-replica-wake"},
    )
    thread.join(timeout=12.0)
    assert not thread.is_alive()
    result = holder["result"]
    assert result.ok is True, result  # type: ignore[union-attr]
    tasks = result.data["tasks"]  # type: ignore[index]
    assert len(tasks) == 1
    assert tasks[0]["task"]["task_id"] == task_id
    # Distinct claim generation/token fencing preserved for the single grant.
    assert tasks[0]["claim"]["claim_id"]
    assert tasks[0]["claim"]["generation"] >= 1


def test_two_waiters_one_task_exactly_one_grant(long_poll_pair) -> None:
    world = long_poll_pair
    replica_a, replica_b = world.replicas
    results: list[object] = []
    lock = threading.Lock()

    def _wait(base_url: str, worker_id: str) -> None:
        adapter = build_long_poll_adapter("raw_http", base_url)
        outcome = claim_via_adapter(
            adapter,
            queues=[world.queue_name],
            worker_id=worker_id,
            wait_seconds=8,
        )
        with lock:
            results.append(outcome)

    threads = [
        threading.Thread(
            target=_wait, args=(replica_a.base_url, "w-compete-a"), daemon=True
        ),
        threading.Thread(
            target=_wait, args=(replica_b.base_url, "w-compete-b"), daemon=True
        ),
    ]
    for thread in threads:
        thread.start()
    time.sleep(0.4)
    task_id = enqueue_task(
        base_url=replica_a.base_url,
        queue_name=world.queue_name,
        payload={"scenario": "compete"},
    )
    for thread in threads:
        thread.join(timeout=12.0)
    assert len(results) == 2
    grants = [
        r
        for r in results
        if getattr(r, "ok", False) and len(getattr(r, "data", {}).get("tasks", [])) == 1
    ]
    empties = [
        r
        for r in results
        if getattr(r, "ok", False) and getattr(r, "data", {}).get("tasks") == []
    ]
    assert len(grants) == 1, results
    assert len(empties) == 1, results
    assert grants[0].data["tasks"][0]["task"]["task_id"] == task_id  # type: ignore[index]
    claim_ids = {
        grants[0].data["tasks"][0]["claim"]["claim_id"]  # type: ignore[index]
    }
    assert len(claim_ids) == 1


def test_cross_replica_missed_notify_fallback(long_poll_pair) -> None:
    """Fallback reconciliation still claims without relying on a live NOTIFY."""

    world = long_poll_pair
    replica_a, replica_b = world.replicas
    # Stop B's listener so wake hints are missed; fallback tick must still converge.
    replica_b.listener.stop()
    assert replica_b.listener.health is ListenerHealth.DEGRADED
    adapter = build_long_poll_adapter("raw_http", replica_b.base_url)
    holder: dict[str, object] = {}

    def _wait() -> None:
        holder["result"] = claim_via_adapter(
            adapter,
            queues=[world.queue_name],
            worker_id="w-fallback-b",
            wait_seconds=4,
        )

    thread = threading.Thread(target=_wait, daemon=True)
    thread.start()
    time.sleep(0.3)
    task_id = enqueue_task(
        base_url=replica_a.base_url,
        queue_name=world.queue_name,
        payload={"scenario": "fallback"},
    )
    thread.join(timeout=10.0)
    assert not thread.is_alive()
    result = holder["result"]
    assert result.ok is True, result  # type: ignore[union-attr]
    tasks = result.data["tasks"]  # type: ignore[index]
    assert len(tasks) == 1
    assert tasks[0]["task"]["task_id"] == task_id


def test_listener_outage_and_one_per_replica(long_poll_pair) -> None:
    world = long_poll_pair
    for replica in world.replicas:
        replica.listener_probe.assert_one_per_replica(1)
    # Kill and reconnect listener on replica A; wake path remains correct via fallback.
    replica_a = world.replicas[0]
    replica_a.listener.stop()
    time.sleep(0.2)
    from queue_service.infrastructure.postgres.claim_wakeup import ClaimWakeListener
    from tests.conformance.long_poll_harness import _wake_dsn
    import os

    database_url = os.environ["TEST_DATABASE_URL"]
    dsn = _wake_dsn(database_url, world.schema)
    replacement = ClaimWakeListener(
        dsn,
        replica_a.coordinator,
        notify_poll_seconds=0.05,
    )
    replacement.start()
    try:
        deadline = time.monotonic() + 5.0
        while time.monotonic() < deadline:
            if replacement.health is ListenerHealth.CONNECTED:
                break
            time.sleep(0.05)
        assert replacement.health is ListenerHealth.CONNECTED
        adapter = build_long_poll_adapter("raw_http", world.replicas[1].base_url)
        holder: dict[str, object] = {}

        def _wait() -> None:
            holder["result"] = claim_via_adapter(
                adapter,
                queues=[world.queue_name],
                worker_id="w-listener-rejoin",
                wait_seconds=5,
            )

        thread = threading.Thread(target=_wait, daemon=True)
        thread.start()
        time.sleep(0.3)
        task_id = enqueue_task(
            base_url=replica_a.base_url,
            queue_name=world.queue_name,
            payload={"scenario": "listener-rejoin"},
        )
        thread.join(timeout=10.0)
        result = holder["result"]
        assert result.ok is True, result  # type: ignore[union-attr]
        assert result.data["tasks"][0]["task"]["task_id"] == task_id  # type: ignore[index]
    finally:
        replacement.stop()


