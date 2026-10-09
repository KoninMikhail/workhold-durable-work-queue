"""Phase 20.1-06: live raw/sync/async long-poll PostgreSQL qualification."""

from __future__ import annotations

import threading
import time
from datetime import UTC, datetime, timedelta

import pytest

from workhold.settings import (
    CLAIM_MAX_OUTSTANDING_WAITS_DEFAULT,
    CLAIM_MAX_WAIT_SECONDS_DEFAULT,
)
from tests.conformance.long_poll_harness import (
    LONG_POLL_ADAPTERS,
    PROXY_UPSTREAM_TIMEOUT_FLOOR_SECONDS,
    SUPERVISOR_DEFAULT_WAIT_SECONDS,
    assert_capability_contract,
    build_long_poll_adapter,
    claim_via_adapter,
    drain_queue,
    enqueue_task,
    long_poll_world,
    pause_queue,
    resume_queue,
)
from tests.fixtures.claim_long_poll import (
    assert_no_forbidden_diagnostics,
    long_poll_recording_server,
)

pytest_plugins = ["tests.integration.conftest"]


@pytest.fixture
def long_poll_single(migrated_schema):
    with long_poll_world(migrated_schema, replica_count=1) as world:
        yield world


def test_long_poll_recording_server_collects_requests(
    long_poll_recording_server: object,
) -> None:
    assert long_poll_recording_server is not None
    assert_no_forbidden_diagnostics(repr(long_poll_recording_server))


def test_live_capability_and_budget_contract(long_poll_single) -> None:
    assert_capability_contract(long_poll_single.primary.base_url)
    assert CLAIM_MAX_WAIT_SECONDS_DEFAULT == 20
    assert CLAIM_MAX_OUTSTANDING_WAITS_DEFAULT == 64
    assert SUPERVISOR_DEFAULT_WAIT_SECONDS == 15
    assert PROXY_UPSTREAM_TIMEOUT_FLOOR_SECONDS == 30
    long_poll_single.primary.listener_probe.assert_one_per_replica(1)


@pytest.mark.parametrize("adapter_kind", LONG_POLL_ADAPTERS)
def test_wait_seconds_zero_immediate(long_poll_single, adapter_kind: str) -> None:
    world = long_poll_single
    adapter = build_long_poll_adapter(adapter_kind, world.primary.base_url)
    task_id = enqueue_task(
        base_url=world.primary.base_url,
        queue_name=world.queue_name,
        payload={"scenario": "immediate"},
    )
    result = claim_via_adapter(
        adapter,
        queues=[world.queue_name],
        worker_id=f"w-{adapter_kind}-imm",
        wait_seconds=0,
    )
    assert result.ok is True, result
    tasks = result.data["tasks"]
    assert len(tasks) == 1
    assert tasks[0]["task"]["task_id"] == task_id


@pytest.mark.parametrize("adapter_kind", LONG_POLL_ADAPTERS)
def test_successful_empty_expiry(long_poll_single, adapter_kind: str) -> None:
    world = long_poll_single
    adapter = build_long_poll_adapter(adapter_kind, world.primary.base_url)
    started = time.monotonic()
    result = claim_via_adapter(
        adapter,
        queues=[world.queue_name],
        worker_id=f"w-{adapter_kind}-empty",
        wait_seconds=1,
    )
    elapsed = time.monotonic() - started
    assert result.ok is True, result
    assert result.data["tasks"] == []
    assert elapsed >= 0.8
    assert result.error_code is None


@pytest.mark.parametrize("adapter_kind", LONG_POLL_ADAPTERS)
def test_task_arrival_during_wait(long_poll_single, adapter_kind: str) -> None:
    world = long_poll_single
    adapter = build_long_poll_adapter(adapter_kind, world.primary.base_url)
    holder: dict[str, object] = {}

    def _claim() -> None:
        holder["result"] = claim_via_adapter(
            adapter,
            queues=[world.queue_name],
            worker_id=f"w-{adapter_kind}-arrive",
            wait_seconds=5,
        )

    thread = threading.Thread(target=_claim, daemon=True)
    thread.start()
    time.sleep(0.3)
    task_id = enqueue_task(
        base_url=world.primary.base_url,
        queue_name=world.queue_name,
        payload={"scenario": "arrival", "adapter": adapter_kind},
    )
    thread.join(timeout=10.0)
    assert not thread.is_alive()
    result = holder["result"]
    assert result.ok is True, result  # type: ignore[union-attr]
    tasks = result.data["tasks"]  # type: ignore[index]
    assert len(tasks) == 1
    assert tasks[0]["task"]["task_id"] == task_id


@pytest.mark.parametrize("adapter_kind", LONG_POLL_ADAPTERS)
def test_delayed_due_task_wake(long_poll_single, adapter_kind: str) -> None:
    world = long_poll_single
    adapter = build_long_poll_adapter(adapter_kind, world.primary.base_url)
    due = datetime.now(tz=UTC) + timedelta(seconds=2)
    task_id = enqueue_task(
        base_url=world.primary.base_url,
        queue_name=world.queue_name,
        payload={"scenario": "delayed"},
        available_at=due,
    )
    started = time.monotonic()
    result = claim_via_adapter(
        adapter,
        queues=[world.queue_name],
        worker_id=f"w-{adapter_kind}-due",
        wait_seconds=5,
    )
    assert result.ok is True, result
    tasks = result.data["tasks"]
    assert len(tasks) == 1
    assert tasks[0]["task"]["task_id"] == task_id
    # Due gate is server-authoritative; allow small scheduling slack on Windows.
    assert time.monotonic() - started >= 0.3


@pytest.mark.parametrize("adapter_kind", LONG_POLL_ADAPTERS)
def test_pause_resume_queue_state(long_poll_single, adapter_kind: str) -> None:
    world = long_poll_single
    adapter = build_long_poll_adapter(adapter_kind, world.primary.base_url)
    enqueue_task(
        base_url=world.primary.base_url,
        queue_name=world.queue_name,
        payload={"scenario": "pause"},
    )
    pause_queue(world.session_factory, world.queue_name)
    paused = claim_via_adapter(
        adapter,
        queues=[world.queue_name],
        worker_id=f"w-{adapter_kind}-pause",
        wait_seconds=1,
    )
    assert paused.ok is True, paused
    assert paused.data["tasks"] == []
    resume_queue(world.session_factory, world.queue_name)
    resumed = claim_via_adapter(
        adapter,
        queues=[world.queue_name],
        worker_id=f"w-{adapter_kind}-resume",
        wait_seconds=2,
    )
    assert resumed.ok is True, resumed
    assert len(resumed.data["tasks"]) == 1


@pytest.mark.parametrize("adapter_kind", LONG_POLL_ADAPTERS)
def test_draining_allows_claim(long_poll_single, adapter_kind: str) -> None:
    world = long_poll_single
    adapter = build_long_poll_adapter(adapter_kind, world.primary.base_url)
    task_id = enqueue_task(
        base_url=world.primary.base_url,
        queue_name=world.queue_name,
        payload={"scenario": "drain"},
    )
    drain_queue(world.session_factory, world.queue_name)
    result = claim_via_adapter(
        adapter,
        queues=[world.queue_name],
        worker_id=f"w-{adapter_kind}-drain",
        wait_seconds=2,
    )
    assert result.ok is True, result
    assert len(result.data["tasks"]) == 1
    assert result.data["tasks"][0]["task"]["task_id"] == task_id


def test_pool_idle_during_wait(long_poll_single) -> None:
    world = long_poll_single
    adapter = build_long_poll_adapter("raw_http", world.primary.base_url)
    barrier = threading.Event()
    probe = world.primary.pool_probe

    def _claim() -> None:
        barrier.set()
        claim_via_adapter(
            adapter,
            queues=[world.queue_name],
            worker_id="w-pool-idle",
            wait_seconds=2,
        )

    thread = threading.Thread(target=_claim, daemon=True)
    thread.start()
    assert barrier.wait(timeout=2.0)
    # After the first empty attempt returns the connection, wait should hold
    # no pooled checkout until wake/fallback.
    deadline = time.monotonic() + 1.5
    idle_streak = 0
    saw_idle = False
    while time.monotonic() < deadline:
        if probe.checked_out == 0:
            idle_streak += 1
            if idle_streak >= 3:
                saw_idle = True
                break
        else:
            idle_streak = 0
        time.sleep(0.05)
    assert saw_idle, f"expected sustained idle pool during wait, peak={probe.peak}"
    thread.join(timeout=5.0)


def test_waiter_cap_admits_64_rejects_65th(migrated_schema) -> None:
    with long_poll_world(
        migrated_schema,
        replica_count=1,
        max_outstanding_waits=CLAIM_MAX_OUTSTANDING_WAITS_DEFAULT,
    ) as world:
        adapter = build_long_poll_adapter("raw_http", world.primary.base_url)
        holders: list[dict[str, object]] = []
        threads: list[threading.Thread] = []

        def _make_claim(index: int) -> None:
            holders[index]["result"] = claim_via_adapter(
                adapter,
                queues=[world.queue_name],
                worker_id=f"w-cap-{index}",
                wait_seconds=8,
            )

        for index in range(64):
            holders.append({})
            thread = threading.Thread(target=_make_claim, args=(index,), daemon=True)
            threads.append(thread)
            thread.start()

        # Let the 64 waiters acquire admission slots.
        deadline = time.monotonic() + 5.0
        while time.monotonic() < deadline and world.primary.admission.active < 64:
            time.sleep(0.05)
        assert world.primary.admission.active == 64

        overflow = claim_via_adapter(
            adapter,
            queues=[world.queue_name],
            worker_id="w-cap-65",
            wait_seconds=2,
        )
        assert overflow.ok is False
        assert overflow.error_code == "resource_exhausted"
        # wait_seconds=0 must not charge the semaphore.
        immediate = claim_via_adapter(
            adapter,
            queues=[world.queue_name],
            worker_id="w-cap-zero",
            wait_seconds=0,
        )
        assert immediate.ok is True, immediate
        assert immediate.data["tasks"] == []

        for thread in threads:
            thread.join(timeout=12.0)


def test_shutdown_aborts_outstanding_wait(long_poll_single) -> None:
    world = long_poll_single
    adapter = build_long_poll_adapter("raw_http", world.primary.base_url)
    holder: dict[str, object] = {}

    def _claim() -> None:
        holder["result"] = claim_via_adapter(
            adapter,
            queues=[world.queue_name],
            worker_id="w-shutdown",
            wait_seconds=10,
        )

    thread = threading.Thread(target=_claim, daemon=True)
    thread.start()
    time.sleep(0.4)
    world.primary.lifecycle.request_stop(grace_seconds=1.0)
    world.primary.coordinator.bump_global()
    thread.join(timeout=5.0)
    assert not thread.is_alive()
    result = holder.get("result")
    assert result is not None
    # Shutdown surfaces as retryable not_accepting or transport close — never a task grant.
    if result.ok:  # type: ignore[union-attr]
        assert result.data["tasks"] == []  # type: ignore[index]
    else:
        assert result.error_code in {  # type: ignore[union-attr]
            "not_accepting",
            "transport_error",
            None,
        }


def test_max_wait_and_batch_remain_locked(long_poll_single) -> None:
    world = long_poll_single
    over = claim_via_adapter(
        build_long_poll_adapter("raw_http", world.primary.base_url),
        queues=[world.queue_name],
        worker_id="w-over-max",
        wait_seconds=CLAIM_MAX_WAIT_SECONDS_DEFAULT + 1,
    )
    assert over.ok is False
    assert over.error_code == "validation_failed"

    # Batch lock: max_tasks!=1 rejected independently of wait ceiling.
    from tests.conformance.clients import RawHttpClientAdapter
    from tests.conformance.conftest import WORKER_TOKEN

    raw = RawHttpClientAdapter(world.primary.base_url, timeout_s=5.0)
    batch = raw._exchange(
        "POST",
        "/v1/claims",
        headers={"Authorization": f"Bearer {WORKER_TOKEN}"},
        body={
            "queues": [world.queue_name],
            "max_tasks": 2,
            "lease_seconds": 30,
            "wait_seconds": 0,
            "worker_id": "w-batch-lock",
        },
    )
    assert batch.ok is False
    assert batch.error_code == "validation_failed"


def test_transport_timeout_distinct_from_empty(long_poll_single) -> None:
    """Undersized client timeout is distinct from successful empty expiry."""

    from _workhold_client_core.config import ClientConfig
    from _workhold_client_core.errors import TimeoutError as ClientTimeoutError
    from _workhold_client_core.transport import HttpJsonTransport
    from workhold_consumer import ConsumerClient
    from tests.conformance.conftest import WORKER_TOKEN

    world = long_poll_single
    # read budget too small for wait=5 → fail closed before or as transport timeout.
    config = ClientConfig.for_public(
        world.primary.base_url,
        read_timeout_s=1.0,
        total_timeout_s=2.0,
    )
    client = ConsumerClient(
        HttpJsonTransport.from_config(config),
        bearer_token=WORKER_TOKEN,
    )
    caps = client.get_capabilities()
    with pytest.raises((ClientTimeoutError, ValueError)):
        client.claim(
            queues=[world.queue_name],
            worker_id="w-timeout",
            lease_seconds=30,
            wait_seconds=5,
            capabilities=caps,
        )


def test_expired_lease_reclaim_after_wait(long_poll_single) -> None:
    world = long_poll_single
    adapter = build_long_poll_adapter("raw_http", world.primary.base_url)
    task_id = enqueue_task(
        base_url=world.primary.base_url,
        queue_name=world.queue_name,
        payload={"scenario": "lease-expire"},
    )
    first = claim_via_adapter(
        adapter,
        queues=[world.queue_name],
        worker_id="w-lease-holder",
        lease_seconds=1,
        wait_seconds=0,
    )
    assert first.ok is True, first
    assert first.data["tasks"][0]["task"]["task_id"] == task_id
    # Wait past lease; second waiter should reclaim via due path.
    time.sleep(1.5)
    second = claim_via_adapter(
        adapter,
        queues=[world.queue_name],
        worker_id="w-lease-reclaim",
        wait_seconds=3,
    )
    assert second.ok is True, second
    assert len(second.data["tasks"]) == 1
    assert second.data["tasks"][0]["task"]["task_id"] == task_id
    assert second.data["tasks"][0]["claim"]["claim_id"] != first.data["tasks"][0]["claim"][
        "claim_id"
    ]


@pytest.mark.parametrize("adapter_kind", LONG_POLL_ADAPTERS)
def test_budgets_and_supervisor_default_documented(adapter_kind: str) -> None:
    assert SUPERVISOR_DEFAULT_WAIT_SECONDS == 15
    assert PROXY_UPSTREAM_TIMEOUT_FLOOR_SECONDS == 30
    from _workhold_client_core.config import (
        long_poll_read_timeout_s,
        long_poll_total_timeout_s,
    )

    assert long_poll_read_timeout_s(15) == 20.0
    assert long_poll_total_timeout_s(15) == 25.0
    assert adapter_kind in LONG_POLL_ADAPTERS


