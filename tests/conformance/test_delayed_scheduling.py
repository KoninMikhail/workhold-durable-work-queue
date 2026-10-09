"""Dual-client delayed scheduling lifecycle conformance (Phase 11 Plan 06 / WORK-15)."""

from __future__ import annotations

import uuid
from datetime import UTC, datetime, timedelta
from uuid import UUID

import pytest
from sqlalchemy import func, select, text, update
from sqlalchemy.orm import Session, sessionmaker

from queue_service.storage.models import QueueCounter, TaskActive
from tests.conformance.clients import ConformanceClient
from tests.conformance.conftest import LEASE_SECONDS, PRODUCER_TOKEN, WORKER_TOKEN

pytest_plugins = ["tests.integration.conftest"]

_STATE_DELAYED = 1


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


def test_delayed_enqueue_empty_before_due_one_claim_after_store_due(
    conformance_client: ConformanceClient,
    session_factory: sessionmaker[Session],
    queue_name: str,
) -> None:
    """Enqueue future → inspect delayed → claim empty → advance due → one claim."""
    future_at = datetime.now(tz=UTC) + timedelta(seconds=30)
    idempotency_key = f"idem-delayed-{uuid.uuid4().hex}"

    enqueue = conformance_client.enqueue(
        queue_name=queue_name,
        idempotency_key=idempotency_key,
        payload={"phase": 11, "client": conformance_client.kind},
        bearer_token=PRODUCER_TOKEN,
        available_at=future_at,
    )
    assert enqueue.ok is True
    task_id = UUID(enqueue.data["task"]["task_id"])
    assert enqueue.data["task"]["state"] == "delayed"

    with session_factory() as session:
        task = session.execute(
            select(TaskActive).where(TaskActive.task_id == task_id)
        ).scalar_one()
        assert int(task.state_code) == _STATE_DELAYED
        assert abs((task.available_at - future_at).total_seconds()) < 1.0
        counter = session.get(QueueCounter, int(task.queue_id))
        assert counter is not None
        assert int(counter.delayed_count) == 1
        assert int(counter.ready_count) == 0
        assert int(counter.leased_count) == 0

    before_due = conformance_client.claim(
        queues=[queue_name],
        worker_id=f"worker-{conformance_client.kind}",
        lease_seconds=LEASE_SECONDS,
        bearer_token=WORKER_TOKEN,
    )
    assert before_due.ok is True
    assert before_due.data.get("tasks") == []

    _advance_past_due(session_factory, task_id=task_id)

    after_due = conformance_client.claim(
        queues=[queue_name],
        worker_id=f"worker-{conformance_client.kind}",
        lease_seconds=LEASE_SECONDS,
        bearer_token=WORKER_TOKEN,
    )
    assert after_due.ok is True
    tasks = after_due.data.get("tasks") or []
    assert len(tasks) == 1
    assert tasks[0]["task"]["task_id"] == str(task_id)
    assert tasks[0]["claim"]["generation"] == 1

    second = conformance_client.claim(
        queues=[queue_name],
        worker_id=f"worker-{conformance_client.kind}-2",
        lease_seconds=LEASE_SECONDS,
        bearer_token=WORKER_TOKEN,
    )
    assert second.ok is True
    assert second.data.get("tasks") == []

    with session_factory() as session:
        counter = session.get(
            QueueCounter,
            int(
                session.execute(
                    select(TaskActive.queue_id).where(TaskActive.task_id == task_id)
                ).scalar_one()
            ),
        )
        assert counter is not None
        assert int(counter.leased_count) == 1
        assert int(counter.delayed_count) == 0


def test_delayed_spawn_empty_before_due_claimable_after_store_due(
    conformance_client: ConformanceClient,
    session_factory: sessionmaker[Session],
    queue_name: str,
) -> None:
    """Complete spawn with future available_at → delayed child → due claim."""
    future_at = datetime.now(tz=UTC) + timedelta(seconds=30)
    spawn_available_at = future_at.isoformat().replace("+00:00", "Z")

    parent_enqueue = conformance_client.enqueue(
        queue_name=queue_name,
        idempotency_key=f"idem-parent-{uuid.uuid4().hex}",
        payload={"phase": 11, "flow": "spawn", "client": conformance_client.kind},
        bearer_token=PRODUCER_TOKEN,
    )
    assert parent_enqueue.ok is True
    parent_id = UUID(parent_enqueue.data["task"]["task_id"])

    parent_claim = conformance_client.claim(
        queues=[queue_name],
        worker_id=f"worker-spawn-{conformance_client.kind}",
        lease_seconds=LEASE_SECONDS,
        bearer_token=WORKER_TOKEN,
    )
    assert parent_claim.ok is True
    parent_tasks = parent_claim.data.get("tasks") or []
    assert len(parent_tasks) == 1
    claim = parent_tasks[0]["claim"]

    complete = conformance_client.complete(
        claim_id=claim["claim_id"],
        claim_token=claim["claim_token"],
        generation=int(claim["generation"]),
        bearer_token=WORKER_TOKEN,
        spawn=[
            {
                "queue_name": queue_name,
                "idempotency_key": f"spawn-{uuid.uuid4().hex}",
                "payload": {"spawn": "delayed", "client": conformance_client.kind},
                "priority": 0,
                "available_at": spawn_available_at,
            }
        ],
    )
    assert complete.ok is True
    spawned_ids = complete.data.get("spawned_task_ids") or []
    assert len(spawned_ids) == 1
    child_id = UUID(spawned_ids[0])

    with session_factory() as session:
        child = session.execute(
            select(TaskActive).where(TaskActive.task_id == child_id)
        ).scalar_one()
        assert int(child.state_code) == _STATE_DELAYED
        assert child.source_task_id == parent_id
        assert int(child.spawn_ordinal) == 0
        assert abs((child.available_at - future_at).total_seconds()) < 1.0
        counter = session.get(QueueCounter, int(child.queue_id))
        assert counter is not None
        assert int(counter.delayed_count) == 1
        assert int(counter.ready_count) == 0
        assert int(counter.leased_count) == 0

    before_due = conformance_client.claim(
        queues=[queue_name],
        worker_id=f"worker-spawn-child-{conformance_client.kind}",
        lease_seconds=LEASE_SECONDS,
        bearer_token=WORKER_TOKEN,
    )
    assert before_due.ok is True
    assert before_due.data.get("tasks") == []

    _advance_past_due(session_factory, task_id=child_id)

    after_due = conformance_client.claim(
        queues=[queue_name],
        worker_id=f"worker-spawn-child-{conformance_client.kind}",
        lease_seconds=LEASE_SECONDS,
        bearer_token=WORKER_TOKEN,
    )
    assert after_due.ok is True
    tasks = after_due.data.get("tasks") or []
    assert len(tasks) == 1
    assert tasks[0]["task"]["task_id"] == str(child_id)
    assert tasks[0]["claim"]["generation"] == 1

    with session_factory() as session:
        counter = session.get(
            QueueCounter,
            int(
                session.execute(
                    select(TaskActive.queue_id).where(TaskActive.task_id == child_id)
                ).scalar_one()
            ),
        )
        assert counter is not None
        assert int(counter.leased_count) == 1
        assert int(counter.delayed_count) == 0
