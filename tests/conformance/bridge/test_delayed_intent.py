"""Bridge delayed-intent wire and Queue-boundary conformance (Phase 11 Plan 09 / WORK-15).

Malformed or naive bridge timestamp strings reach Queue unchanged and are rejected
with ``validation_failed`` without durable Queue writes.
"""

from __future__ import annotations

import json
import uuid
from datetime import UTC, datetime, timedelta
from typing import Any

import psycopg
import pytest
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from workhold.storage.models import EnqueueDedup, Queue, QueueCounter, TaskActive
from workhold_producer.bridge.postgres_store import PostgresOutboxMapping, PostgresOutboxStore
from workhold_producer.bridge.runner import BridgeRunner
from tests.conformance.bridge.fixtures import BridgeWorld, read_intent_row

pytest_plugins = ["tests.integration.conftest"]


def _seed_intent_with_request(
    world: BridgeWorld,
    *,
    namespace: str,
    row_id: str,
    enqueue_request: dict[str, Any],
) -> None:
    conn = psycopg.connect(world.app_conninfo)
    try:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                INSERT INTO "{world.app_schema}"."{world.app_table}" (
                    source_namespace, source_row_id, schema_version, target_queue,
                    enqueue_request, created_at, state, ownership_token, generation,
                    lease_expires_at, available_at, updated_at
                ) VALUES (
                    %s, %s, 1, %s,
                    %s::jsonb,
                    now(),
                    'pending', NULL, 0,
                    NULL,
                    now(),
                    now()
                )
                """,
                (namespace, row_id, world.queue_name, json.dumps(enqueue_request)),
            )
        conn.commit()
    finally:
        conn.close()


def _intake_counts(session: Session, queue_name: str) -> tuple[int, int, int]:
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


def _make_bridge_runner(world: BridgeWorld) -> BridgeRunner:
    def connection_factory() -> Any:
        return psycopg.connect(world.app_conninfo)

    store = PostgresOutboxStore(
        connection_factory=connection_factory,
        mapping=PostgresOutboxMapping(schema=world.app_schema, table=world.app_table),
    )
    return BridgeRunner(
        store=store,
        producer=world.producer,
        batch_size=1,
        lease_seconds=30,
        max_in_flight=1,
        idle_poll_seconds=0.0,
        initial_backoff_seconds=0.0,
        max_backoff_seconds=0.0,
        backoff_jitter_ratio=0.0,
    )


def test_bridge_future_and_explicit_null_strings_reach_queue(
    bridge_world: BridgeWorld,
) -> None:
    world = bridge_world
    namespace = "bridge.delayed"
    future_at = (datetime.now(tz=UTC) + timedelta(hours=1)).strftime("%Y-%m-%dT%H:%M:%SZ")
    runner = _make_bridge_runner(world)

    row_future = f"future-{uuid.uuid4().hex[:12]}"
    _seed_intent_with_request(
        world,
        namespace=namespace,
        row_id=row_future,
        enqueue_request={
            "payload": {"case": "future"},
            "priority": 0,
            "available_at": future_at,
        },
    )
    assert runner.poll_once() == 1
    delivered = read_intent_row(world, namespace=namespace, row_id=row_future)
    assert delivered["state"] == "delivered"
    task_id = delivered["queue_task_id"]
    assert task_id is not None

    session = world.session_factory()
    try:
        task = session.execute(
            select(TaskActive).where(TaskActive.task_id == task_id)
        ).scalar_one()
        expected = datetime.fromisoformat(future_at.replace("Z", "+00:00"))
        assert abs((task.available_at - expected).total_seconds()) < 2.0
    finally:
        session.close()

    row_null = f"null-{uuid.uuid4().hex[:12]}"
    _seed_intent_with_request(
        world,
        namespace=namespace,
        row_id=row_null,
        enqueue_request={
            "payload": {"case": "explicit_null"},
            "priority": 0,
            "available_at": None,
        },
    )
    assert runner.poll_once() == 1
    null_row = read_intent_row(world, namespace=namespace, row_id=row_null)
    assert null_row["state"] == "delivered"
    assert null_row["queue_task_id"] is not None


@pytest.mark.parametrize(
    "available_at_value",
    [
        "not-an-rfc3339-timestamp",
        "2026-09-20T00:00:00",
    ],
)
def test_bridge_invalid_timestamp_strings_validation_failed_zero_write(
    bridge_world: BridgeWorld,
    available_at_value: str,
) -> None:
    world = bridge_world
    namespace = "bridge.delayed"
    row_id = f"bad-{uuid.uuid4().hex[:12]}"

    session = world.session_factory()
    try:
        before = _intake_counts(session, world.queue_name)
    finally:
        session.close()

    _seed_intent_with_request(
        world,
        namespace=namespace,
        row_id=row_id,
        enqueue_request={
            "payload": {"case": "invalid"},
            "priority": 0,
            "available_at": available_at_value,
        },
    )
    runner = _make_bridge_runner(world)
    assert runner.poll_once() == 1
    row = read_intent_row(world, namespace=namespace, row_id=row_id)
    assert row["state"] == "terminal_operator_action"
    assert row["last_failure_code"] == "validation_failed"

    session = world.session_factory()
    try:
        after = _intake_counts(session, world.queue_name)
    finally:
        session.close()
    assert after == before
