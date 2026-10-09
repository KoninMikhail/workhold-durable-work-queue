"""Dual-client bounded priority lifecycle conformance (Phase 12 Plan 03 / WORK-16).

Plan 09 removes every temporary skip and proves behavior while capability stays
false. Plan 11 owns the sole priority=true activation node.
"""

from __future__ import annotations

import json
import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import UUID

import pytest
from sqlalchemy import func, select, text, update
from sqlalchemy.orm import Session, sessionmaker

from workhold.storage.models import (
    CompletionEffect,
    EnqueueDedup,
    Queue,
    QueueCounter,
    TaskActive,
)
from tests.conformance.clients import CLIENT_KINDS, ConformanceClient
from tests.conformance.conftest import ADMIN_TOKEN, LEASE_SECONDS, PRODUCER_TOKEN, WORKER_TOKEN

pytest_plugins = ["tests.integration.conftest"]

PRIORITY_MIN = -32768
PRIORITY_MAX = 32767

REPO_ROOT = Path(__file__).resolve().parents[2]
OPENAPI_PATH = REPO_ROOT / "openapi" / "queue.openapi.json"


def _intake_snapshot(session: Session, queue_name: str) -> tuple[int, int, int]:
    queue = session.execute(select(Queue).where(Queue.name == queue_name)).scalar_one_or_none()
    if queue is None:
        return (0, 0, 0)
    qid = int(queue.id)
    tasks = int(
        session.scalar(
            select(func.count()).select_from(TaskActive).where(TaskActive.queue_id == qid)
        )
        or 0
    )
    dedup = int(
        session.scalar(
            select(func.count()).select_from(EnqueueDedup).where(EnqueueDedup.queue_id == qid)
        )
        or 0
    )
    counter = session.get(QueueCounter, qid)
    depth = 0 if counter is None else int(counter.ready_count) + int(counter.delayed_count)
    return tasks, dedup, depth


def _set_available_at(
    session_factory: sessionmaker[Session],
    *,
    task_id: UUID,
    available_at: datetime,
) -> None:
    with session_factory() as session:
        session.execute(
            update(TaskActive)
            .where(TaskActive.task_id == task_id)
            .values(available_at=available_at)
        )
        session.commit()


def _advance_past_due(
    session_factory: sessionmaker[Session],
    *,
    task_id: UUID,
) -> None:
    with session_factory() as session:
        session.execute(
            update(TaskActive)
            .where(TaskActive.task_id == task_id)
            .values(
                available_at=func.transaction_timestamp()
                - text("interval '1 second'")
            )
        )
        session.commit()


def _dead_letter_via_fail(
    conformance_client: ConformanceClient,
    session_factory: sessionmaker[Session],
    *,
    queue_name: str,
    priority: int,
) -> UUID:
    enqueue = conformance_client.enqueue(
        queue_name=queue_name,
        idempotency_key=f"idem-dlq-{uuid.uuid4().hex}",
        payload={"priority": priority},
        bearer_token=PRODUCER_TOKEN,
        priority=priority,
    )
    assert enqueue.ok is True
    source_id = UUID(enqueue.data["task"]["task_id"])
    claim = conformance_client.claim(
        queues=[queue_name],
        worker_id=f"worker-dlq-{conformance_client.kind}",
        lease_seconds=LEASE_SECONDS,
        bearer_token=WORKER_TOKEN,
    )
    assert claim.ok is True
    tasks = claim.data.get("tasks") or []
    assert len(tasks) == 1
    handle = tasks[0]["claim"]
    failed = conformance_client.fail(
        claim_id=handle["claim_id"],
        claim_token=handle["claim_token"],
        generation=int(handle["generation"]),
        bearer_token=WORKER_TOKEN,
        retryable=False,
        failure_code="worker.fatal",
        failure_detail="dead-letter priority proof",
    )
    assert failed.ok is True
    with session_factory() as session:
        assert (
            session.execute(
                select(TaskActive).where(TaskActive.task_id == source_id)
            ).scalar_one_or_none()
            is None
        )
    return source_id


def test_openapi_capabilities_priority_is_true_after_plan_11_activation() -> None:
    doc = json.loads(OPENAPI_PATH.read_text(encoding="utf-8"))
    caps = doc["components"]["schemas"]["Capabilities"]["properties"]
    assert caps["priority"]["const"] is True


@pytest.mark.parametrize("client_kind", CLIENT_KINDS)
@pytest.mark.parametrize("priority", [PRIORITY_MIN, PRIORITY_MAX, 500, -100])
def test_enqueue_accepts_endpoints_and_retains_exact_priority(
    conformance_client: ConformanceClient,
    session_factory: sessionmaker[Session],
    queue_name: str,
    client_kind: str,
    priority: int,
) -> None:
    del client_kind
    result = conformance_client.enqueue(
        queue_name=queue_name,
        idempotency_key=f"idem-priority-{uuid.uuid4().hex}",
        payload={"phase": 12, "client": conformance_client.kind},
        bearer_token=PRODUCER_TOKEN,
        priority=priority,
    )
    assert result.ok is True
    task_id = UUID(result.data["task"]["task_id"])
    assert int(result.data["task"]["priority"]) == priority
    inspected = conformance_client.inspect(
        task_id=str(task_id),
        bearer_token=PRODUCER_TOKEN,
    )
    assert inspected.ok is True
    assert int(inspected.data["priority"]) == priority
    with session_factory() as session:
        row = session.execute(
            select(TaskActive).where(TaskActive.task_id == task_id)
        ).scalar_one()
        assert int(row.priority) == priority


@pytest.mark.parametrize("client_kind", CLIENT_KINDS)
@pytest.mark.parametrize(
    "priority",
    [PRIORITY_MIN - 1, PRIORITY_MAX + 1, True, "100", 1.5, None],
)
def test_invalid_priority_rejected_with_zero_durable_writes(
    conformance_client: ConformanceClient,
    session_factory: sessionmaker[Session],
    queue_name: str,
    client_kind: str,
    priority: object,
) -> None:
    del client_kind
    before = _intake_snapshot(session_factory(), queue_name)
    result = conformance_client.enqueue(
        queue_name=queue_name,
        idempotency_key=f"idem-invalid-{uuid.uuid4().hex}",
        payload={"phase": 12, "client": conformance_client.kind},
        bearer_token=PRODUCER_TOKEN,
        priority=priority,  # type: ignore[arg-type]
    )
    assert result.ok is False
    assert result.error_code == "validation_failed"
    after = _intake_snapshot(session_factory(), queue_name)
    assert after == before


@pytest.mark.parametrize("client_kind", CLIENT_KINDS)
def test_same_key_same_priority_replays_changed_priority_conflicts(
    conformance_client: ConformanceClient,
    queue_name: str,
    client_kind: str,
) -> None:
    del client_kind
    key = f"idem-replay-{uuid.uuid4().hex}"
    first = conformance_client.enqueue(
        queue_name=queue_name,
        idempotency_key=key,
        payload={"v": 1},
        bearer_token=PRODUCER_TOKEN,
        priority=100,
    )
    assert first.ok is True
    replay = conformance_client.enqueue(
        queue_name=queue_name,
        idempotency_key=key,
        payload={"v": 1},
        bearer_token=PRODUCER_TOKEN,
        priority=100,
    )
    assert replay.ok is True
    assert replay.data.get("replayed") is True
    conflict = conformance_client.enqueue(
        queue_name=queue_name,
        idempotency_key=key,
        payload={"v": 1},
        bearer_token=PRODUCER_TOKEN,
        priority=200,
    )
    assert conflict.ok is False
    assert conflict.error_code == "idempotency_conflict"


@pytest.mark.parametrize("client_kind", CLIENT_KINDS)
def test_two_due_tasks_claim_higher_priority_first(
    conformance_client: ConformanceClient,
    session_factory: sessionmaker[Session],
    queue_name: str,
    client_kind: str,
) -> None:
    del client_kind
    low = conformance_client.enqueue(
        queue_name=queue_name,
        idempotency_key=f"idem-low-{uuid.uuid4().hex}",
        payload={"label": "low"},
        bearer_token=PRODUCER_TOKEN,
        priority=PRIORITY_MIN,
    )
    high = conformance_client.enqueue(
        queue_name=queue_name,
        idempotency_key=f"idem-high-{uuid.uuid4().hex}",
        payload={"label": "high"},
        bearer_token=PRODUCER_TOKEN,
        priority=PRIORITY_MAX,
    )
    assert low.ok and high.ok
    low_id = UUID(low.data["task"]["task_id"])
    _set_available_at(
        session_factory,
        task_id=low_id,
        available_at=datetime.now(tz=UTC) - timedelta(minutes=5),
    )
    claim = conformance_client.claim(
        queues=[queue_name],
        worker_id=f"worker-priority-{conformance_client.kind}",
        lease_seconds=LEASE_SECONDS,
        bearer_token=WORKER_TOKEN,
    )
    assert claim.ok is True
    tasks = claim.data.get("tasks") or []
    assert len(tasks) == 1
    assert tasks[0]["task"]["task_id"] == high.data["task"]["task_id"]


@pytest.mark.parametrize("client_kind", CLIENT_KINDS)
def test_same_priority_fifo_by_available_at_then_id(
    conformance_client: ConformanceClient,
    session_factory: sessionmaker[Session],
    queue_name: str,
    client_kind: str,
) -> None:
    del client_kind
    first = conformance_client.enqueue(
        queue_name=queue_name,
        idempotency_key=f"idem-fifo-1-{uuid.uuid4().hex}",
        payload={"n": 1},
        bearer_token=PRODUCER_TOKEN,
        priority=10,
    )
    second = conformance_client.enqueue(
        queue_name=queue_name,
        idempotency_key=f"idem-fifo-2-{uuid.uuid4().hex}",
        payload={"n": 2},
        bearer_token=PRODUCER_TOKEN,
        priority=10,
    )
    assert first.ok and second.ok
    first_id = UUID(first.data["task"]["task_id"])
    second_id = UUID(second.data["task"]["task_id"])
    anchor = datetime.now(tz=UTC) - timedelta(minutes=2)
    _set_available_at(session_factory, task_id=first_id, available_at=anchor)
    _set_available_at(
        session_factory,
        task_id=second_id,
        available_at=anchor + timedelta(seconds=30),
    )
    claim = conformance_client.claim(
        queues=[queue_name],
        worker_id=f"worker-fifo-{conformance_client.kind}",
        lease_seconds=LEASE_SECONDS,
        bearer_token=WORKER_TOKEN,
    )
    assert claim.ok is True
    tasks = claim.data.get("tasks") or []
    assert len(tasks) == 1
    assert tasks[0]["task"]["task_id"] == first.data["task"]["task_id"]


@pytest.mark.parametrize("client_kind", CLIENT_KINDS)
def test_same_priority_tie_breaks_by_task_id(
    conformance_client: ConformanceClient,
    session_factory: sessionmaker[Session],
    queue_name: str,
    client_kind: str,
) -> None:
    del client_kind
    first = conformance_client.enqueue(
        queue_name=queue_name,
        idempotency_key=f"idem-id-1-{uuid.uuid4().hex}",
        payload={"n": 1},
        bearer_token=PRODUCER_TOKEN,
        priority=10,
    )
    second = conformance_client.enqueue(
        queue_name=queue_name,
        idempotency_key=f"idem-id-2-{uuid.uuid4().hex}",
        payload={"n": 2},
        bearer_token=PRODUCER_TOKEN,
        priority=10,
    )
    assert first.ok and second.ok
    first_id = UUID(first.data["task"]["task_id"])
    second_id = UUID(second.data["task"]["task_id"])
    anchor = datetime.now(tz=UTC) - timedelta(minutes=1)
    _set_available_at(session_factory, task_id=first_id, available_at=anchor)
    _set_available_at(session_factory, task_id=second_id, available_at=anchor)
    claim = conformance_client.claim(
        queues=[queue_name],
        worker_id=f"worker-id-tie-{conformance_client.kind}",
        lease_seconds=LEASE_SECONDS,
        bearer_token=WORKER_TOKEN,
    )
    assert claim.ok is True
    tasks = claim.data.get("tasks") or []
    assert len(tasks) == 1
    assert tasks[0]["task"]["task_id"] == first.data["task"]["task_id"]


@pytest.mark.parametrize("client_kind", CLIENT_KINDS)
def test_future_high_priority_never_preempts_due_min(
    conformance_client: ConformanceClient,
    queue_name: str,
    client_kind: str,
) -> None:
    del client_kind
    future_at = datetime.now(tz=UTC) + timedelta(seconds=60)
    future = conformance_client.enqueue(
        queue_name=queue_name,
        idempotency_key=f"idem-future-max-{uuid.uuid4().hex}",
        payload={"when": "future"},
        bearer_token=PRODUCER_TOKEN,
        priority=PRIORITY_MAX,
        available_at=future_at,
    )
    due = conformance_client.enqueue(
        queue_name=queue_name,
        idempotency_key=f"idem-due-min-{uuid.uuid4().hex}",
        payload={"when": "due"},
        bearer_token=PRODUCER_TOKEN,
        priority=PRIORITY_MIN,
    )
    assert future.ok and due.ok
    claim = conformance_client.claim(
        queues=[queue_name],
        worker_id=f"worker-future-{conformance_client.kind}",
        lease_seconds=LEASE_SECONDS,
        bearer_token=WORKER_TOKEN,
    )
    assert claim.ok is True
    tasks = claim.data.get("tasks") or []
    assert len(tasks) == 1
    assert tasks[0]["task"]["task_id"] == due.data["task"]["task_id"]


@pytest.mark.parametrize("client_kind", CLIENT_KINDS)
def test_complete_spawn_preserves_child_priority(
    conformance_client: ConformanceClient,
    session_factory: sessionmaker[Session],
    queue_name: str,
    client_kind: str,
) -> None:
    del client_kind
    parent = conformance_client.enqueue(
        queue_name=queue_name,
        idempotency_key=f"idem-spawn-parent-{uuid.uuid4().hex}",
        payload={"flow": "spawn"},
        bearer_token=PRODUCER_TOKEN,
        priority=50,
    )
    assert parent.ok is True
    parent_id = UUID(parent.data["task"]["task_id"])
    claim = conformance_client.claim(
        queues=[queue_name],
        worker_id=f"worker-spawn-{conformance_client.kind}",
        lease_seconds=LEASE_SECONDS,
        bearer_token=WORKER_TOKEN,
    )
    assert claim.ok is True
    tasks = claim.data.get("tasks") or []
    assert len(tasks) == 1
    complete = conformance_client.complete(
        claim_id=tasks[0]["claim"]["claim_id"],
        claim_token=tasks[0]["claim"]["claim_token"],
        generation=int(tasks[0]["claim"]["generation"]),
        bearer_token=WORKER_TOKEN,
        spawn=[
            {
                "queue_name": queue_name,
                "idempotency_key": f"spawn-{uuid.uuid4().hex}",
                "payload": {"child": True},
                "priority": 250,
            }
        ],
    )
    assert complete.ok is True
    child_id = UUID(complete.data["spawned_task_ids"][0])
    with session_factory() as session:
        child = session.execute(
            select(TaskActive).where(TaskActive.task_id == child_id)
        ).scalar_one()
        assert int(child.priority) == 250
        assert child.source_task_id == parent_id


@pytest.mark.parametrize("client_kind", CLIENT_KINDS)
def test_delayed_spawn_preserves_non_zero_child_priority(
    conformance_client: ConformanceClient,
    session_factory: sessionmaker[Session],
    queue_name: str,
    client_kind: str,
) -> None:
    del client_kind
    future_at = datetime.now(tz=UTC) + timedelta(seconds=30)
    parent = conformance_client.enqueue(
        queue_name=queue_name,
        idempotency_key=f"idem-delay-spawn-{uuid.uuid4().hex}",
        payload={"flow": "delayed-spawn"},
        bearer_token=PRODUCER_TOKEN,
        priority=40,
    )
    assert parent.ok is True
    claim = conformance_client.claim(
        queues=[queue_name],
        worker_id=f"worker-delay-spawn-{conformance_client.kind}",
        lease_seconds=LEASE_SECONDS,
        bearer_token=WORKER_TOKEN,
    )
    assert claim.ok is True
    tasks = claim.data.get("tasks") or []
    assert len(tasks) == 1
    complete = conformance_client.complete(
        claim_id=tasks[0]["claim"]["claim_id"],
        claim_token=tasks[0]["claim"]["claim_token"],
        generation=int(tasks[0]["claim"]["generation"]),
        bearer_token=WORKER_TOKEN,
        spawn=[
            {
                "queue_name": queue_name,
                "idempotency_key": f"spawn-delay-{uuid.uuid4().hex}",
                "payload": {"child": "delayed"},
                "priority": 900,
                "available_at": future_at.isoformat().replace("+00:00", "Z"),
            }
        ],
    )
    assert complete.ok is True
    child_id = UUID(complete.data["spawned_task_ids"][0])
    inspected = conformance_client.inspect(
        task_id=str(child_id),
        bearer_token=PRODUCER_TOKEN,
    )
    assert inspected.ok is True
    assert int(inspected.data["priority"]) == 900
    assert inspected.data["state"] == "delayed"

    before_due = conformance_client.claim(
        queues=[queue_name],
        worker_id=f"worker-delay-child-{conformance_client.kind}",
        lease_seconds=LEASE_SECONDS,
        bearer_token=WORKER_TOKEN,
    )
    assert before_due.ok is True
    assert before_due.data.get("tasks") == []

    _advance_past_due(session_factory, task_id=child_id)
    after_due = conformance_client.claim(
        queues=[queue_name],
        worker_id=f"worker-delay-child-{conformance_client.kind}",
        lease_seconds=LEASE_SECONDS,
        bearer_token=WORKER_TOKEN,
    )
    assert after_due.ok is True
    claimed = after_due.data.get("tasks") or []
    assert len(claimed) == 1
    assert claimed[0]["task"]["task_id"] == str(child_id)


@pytest.mark.parametrize("client_kind", CLIENT_KINDS)
def test_mixed_spawn_invalid_priority_rolls_back_atomically(
    conformance_client: ConformanceClient,
    session_factory: sessionmaker[Session],
    queue_name: str,
    client_kind: str,
) -> None:
    del client_kind
    source = conformance_client.enqueue(
        queue_name=queue_name,
        idempotency_key=f"idem-spawn-rollback-{uuid.uuid4().hex}",
        payload={"flow": "rollback"},
        bearer_token=PRODUCER_TOKEN,
        priority=75,
    )
    assert source.ok is True
    source_id = UUID(source.data["task"]["task_id"])
    claim = conformance_client.claim(
        queues=[queue_name],
        worker_id=f"worker-rollback-{conformance_client.kind}",
        lease_seconds=LEASE_SECONDS,
        bearer_token=WORKER_TOKEN,
    )
    assert claim.ok is True
    tasks = claim.data.get("tasks") or []
    assert len(tasks) == 1
    handle = tasks[0]["claim"]
    before = _intake_snapshot(session_factory(), queue_name)
    complete = conformance_client.complete(
        claim_id=handle["claim_id"],
        claim_token=handle["claim_token"],
        generation=int(handle["generation"]),
        bearer_token=WORKER_TOKEN,
        spawn=[
            {
                "queue_name": queue_name,
                "idempotency_key": f"spawn-ok-{uuid.uuid4().hex}",
                "payload": {"ok": True},
                "priority": 100,
            },
            {
                "queue_name": queue_name,
                "idempotency_key": f"spawn-bad-{uuid.uuid4().hex}",
                "payload": {"bad": True},
                "priority": PRIORITY_MAX + 1,
            },
        ],
    )
    assert complete.ok is False
    assert complete.error_code == "validation_failed"
    after = _intake_snapshot(session_factory(), queue_name)
    assert after == before
    with session_factory() as session:
        still = session.execute(
            select(TaskActive).where(TaskActive.task_id == source_id)
        ).scalar_one()
        assert still.current_claim_id == UUID(handle["claim_id"])
        assert (
            session.execute(
                select(TaskActive).where(TaskActive.source_task_id == source_id)
            ).scalars().all()
            == []
        )
        assert (
            session.execute(
                select(CompletionEffect).where(
                    CompletionEffect.source_claim_id == UUID(handle["claim_id"])
                )
            ).scalars().all()
            == []
        )


@pytest.mark.parametrize("client_kind", CLIENT_KINDS)
def test_in_flight_low_priority_lease_not_preempted_by_later_high_enqueue(
    conformance_client: ConformanceClient,
    queue_name: str,
    client_kind: str,
) -> None:
    del client_kind
    low = conformance_client.enqueue(
        queue_name=queue_name,
        idempotency_key=f"idem-lease-low-{uuid.uuid4().hex}",
        payload={"p": "low"},
        bearer_token=PRODUCER_TOKEN,
        priority=PRIORITY_MIN,
    )
    assert low.ok is True
    claim = conformance_client.claim(
        queues=[queue_name],
        worker_id=f"worker-lease-{conformance_client.kind}",
        lease_seconds=LEASE_SECONDS,
        bearer_token=WORKER_TOKEN,
    )
    assert claim.ok is True
    tasks = claim.data.get("tasks") or []
    assert len(tasks) == 1
    original = tasks[0]["claim"]
    high = conformance_client.enqueue(
        queue_name=queue_name,
        idempotency_key=f"idem-lease-high-{uuid.uuid4().hex}",
        payload={"p": "high"},
        bearer_token=PRODUCER_TOKEN,
        priority=PRIORITY_MAX,
    )
    assert high.ok is True
    heartbeat = conformance_client.heartbeat(
        claim_id=original["claim_id"],
        claim_token=original["claim_token"],
        generation=int(original["generation"]),
        lease_seconds=LEASE_SECONDS,
        bearer_token=WORKER_TOKEN,
    )
    assert heartbeat.ok is True
    assert int(heartbeat.data["claim"]["generation"]) == int(original["generation"])
    complete = conformance_client.complete(
        claim_id=original["claim_id"],
        claim_token=original["claim_token"],
        generation=int(original["generation"]),
        bearer_token=WORKER_TOKEN,
    )
    assert complete.ok is True
    assert complete.data.get("state") == "succeeded"


@pytest.mark.parametrize("client_kind", CLIENT_KINDS)
def test_active_and_terminal_inspection_expose_non_zero_priority(
    conformance_client: ConformanceClient,
    queue_name: str,
    client_kind: str,
) -> None:
    del client_kind
    enqueue = conformance_client.enqueue(
        queue_name=queue_name,
        idempotency_key=f"idem-inspect-{uuid.uuid4().hex}",
        payload={"inspect": True},
        bearer_token=PRODUCER_TOKEN,
        priority=777,
    )
    assert enqueue.ok is True
    task_id = enqueue.data["task"]["task_id"]
    active = conformance_client.inspect(task_id=task_id, bearer_token=PRODUCER_TOKEN)
    assert active.ok is True
    assert int(active.data["priority"]) == 777
    claim = conformance_client.claim(
        queues=[queue_name],
        worker_id=f"worker-inspect-{conformance_client.kind}",
        lease_seconds=LEASE_SECONDS,
        bearer_token=WORKER_TOKEN,
    )
    assert claim.ok is True
    tasks = claim.data.get("tasks") or []
    assert len(tasks) == 1
    handle = tasks[0]["claim"]
    complete = conformance_client.complete(
        claim_id=handle["claim_id"],
        claim_token=handle["claim_token"],
        generation=int(handle["generation"]),
        bearer_token=WORKER_TOKEN,
    )
    assert complete.ok is True
    terminal = conformance_client.inspect(task_id=task_id, bearer_token=PRODUCER_TOKEN)
    assert terminal.ok is True
    assert int(terminal.data["priority"]) == 777
    assert terminal.data["state"] == "succeeded"
    assert terminal.data["terminal_at"] is not None


@pytest.mark.parametrize("client_kind", CLIENT_KINDS)
def test_dead_letter_replay_preserves_source_priority(
    conformance_client: ConformanceClient,
    session_factory: sessionmaker[Session],
    queue_name: str,
    client_kind: str,
) -> None:
    del client_kind
    source_priority = 1337
    source_id = _dead_letter_via_fail(
        conformance_client,
        session_factory,
        queue_name=queue_name,
        priority=source_priority,
    )
    replay = conformance_client.replay_dead_letter(
        queue_name=queue_name,
        task_id=str(source_id),
        bearer_token=ADMIN_TOKEN,
        idempotency_key=f"replay-{uuid.uuid4().hex}",
        reason="preserve priority on replay",
    )
    assert replay.ok is True
    assert replay.data.get("replayed") is False
    assert "priority" not in replay.data
    replayed_id = UUID(replay.data["task_id"])
    with session_factory() as session:
        row = session.execute(
            select(TaskActive).where(TaskActive.task_id == replayed_id)
        ).scalar_one()
        assert int(row.priority) == source_priority
        assert row.source_task_id == source_id
    inspected = conformance_client.inspect(
        task_id=str(replayed_id),
        bearer_token=PRODUCER_TOKEN,
    )
    assert inspected.ok is True
    assert int(inspected.data["priority"]) == source_priority


@pytest.mark.parametrize("client_kind", CLIENT_KINDS)
def test_bulk_dead_letter_replay_preserves_each_source_priority(
    conformance_client: ConformanceClient,
    session_factory: sessionmaker[Session],
    queue_name: str,
    client_kind: str,
) -> None:
    del client_kind
    source_low = _dead_letter_via_fail(
        conformance_client,
        session_factory,
        queue_name=queue_name,
        priority=PRIORITY_MIN,
    )
    source_high = _dead_letter_via_fail(
        conformance_client,
        session_factory,
        queue_name=queue_name,
        priority=PRIORITY_MAX,
    )

    now = datetime.now(tz=UTC)
    filters = {
        "from": (now - timedelta(hours=1)).isoformat().replace("+00:00", "Z"),
        "to": (now + timedelta(hours=1)).isoformat().replace("+00:00", "Z"),
        "failure_code": "worker.fatal",
    }
    preview = conformance_client.bulk_preview_replay(
        queue_name=queue_name,
        bearer_token=ADMIN_TOKEN,
        filters=filters,
    )
    assert preview.ok is True
    token = preview.data["confirmation_token"]
    assert isinstance(token, str) and token

    execute = conformance_client.bulk_execute_replay(
        queue_name=queue_name,
        bearer_token=ADMIN_TOKEN,
        idempotency_key=f"bulk-{uuid.uuid4().hex}",
        confirmation_token=token,
        filters=filters,
        reason="bulk preserve source priority",
        start_index=0,
        batch_limit=10,
    )
    assert execute.ok is True
    assert execute.data.get("succeeded", 0) >= 2
    assert "priority" not in execute.data

    with session_factory() as session:
        replayed = {
            row.source_task_id: int(row.priority)
            for row in session.execute(
                select(TaskActive).where(
                    TaskActive.source_task_id.in_((source_low, source_high))
                )
            ).scalars()
        }
    assert replayed[source_low] == PRIORITY_MIN
    assert replayed[source_high] == PRIORITY_MAX
