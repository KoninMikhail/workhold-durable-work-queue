"""Bridge crash-window and replay conformance (06-05 / BRDG-02).

Black-box evidence at the app DB → bridge → Queue boundary using real
PostgreSQL (independent app schema) and a real Queue application-plane HTTP
server. No SDK or persistence mocks; no Queue-table mutation for success.

Honest boundary: bridge traffic is at-least-once toward Queue; idempotent
enqueue prevents a second task for a matching immutable intent. This suite
does not claim single-delivery execution or distributed transactions.
"""

from __future__ import annotations

import uuid
from pathlib import Path

import pytest

from workhold_producer.bridge.idempotency import bridge_idempotency_key
from _workhold_client_core.errors import ProtocolError
from tests.conformance.bridge.fixtures import (
    PROCESS_KILL_REPEATS,
    REPLICA_RECLAIM_CYCLES,
    BridgeWorld,
    force_expire_lease,
    read_intent_row,
    seed_pending_intent,
)
from tests.conformance.bridge.harness import (
    CrashWindow,
    BridgeProcessController,
)

# Bound diagnostic dumps — never include payloads/keys/tokens.
_MAX_DIAG = 256


def _diag(msg: str) -> str:
    text = msg.replace("\n", " ")
    if len(text) > _MAX_DIAG:
        return text[: _MAX_DIAG - 3] + "..."
    return text


def _await_delivered_one_task(
    world: BridgeWorld,
    ctrl: BridgeProcessController,
    *,
    namespace: str,
    row_id: str,
    deadline_s: float = 20.0,
) -> str:
    """Restart-clean bridge until app row is delivered; return public task_id."""
    key = bridge_idempotency_key(namespace, row_id)
    ctrl.start(failpoint=None, max_cycles=40)
    try:
        task_id = ctrl.await_app_delivered(
            world,
            namespace=namespace,
            row_id=row_id,
            deadline_s=deadline_s,
        )
        resolved = world.producer.resolve_submission(
            world.queue_name, idempotency_key=key
        )
        assert resolved.task.task_id == task_id, _diag(
            f"resolve/task mismatch resolve={resolved.task.task_id!r} "
            f"delivered={task_id!r}"
        )
        inspected = world.producer.inspect_task(task_id)
        assert inspected.task_id == task_id
        return task_id
    finally:
        ctrl.stop_all()


def test_crash_before_enqueue_leaves_pending_then_one_task(
    bridge_world: BridgeWorld,
) -> None:
    """Crash before enqueue leaves intent pending; restart creates one task."""
    world = bridge_world
    namespace = "bridge.crash"
    row_id = f"before-enq-{uuid.uuid4().hex[:12]}"
    seed_pending_intent(
        world,
        namespace=namespace,
        row_id=row_id,
        payload={"marker": "before_enqueue"},
    )
    ctrl = BridgeProcessController(world)
    ready = ctrl.temp_ready_path("before_enqueue")
    proc = ctrl.start(
        failpoint=CrashWindow.BEFORE_ENQUEUE,
        ready_path=ready,
        max_cycles=5,
    )
    try:
        ctrl.await_ready(ready, deadline_s=15.0)
        row = read_intent_row(world, namespace=namespace, row_id=row_id)
        assert row["state"] == "leased", _diag(f"expected leased got {row!r}")
        # Queue must not yet expose a submission for this identity.
        key = bridge_idempotency_key(namespace, row_id)
        with pytest.raises(ProtocolError) as exc_info:
            world.producer.resolve_submission(world.queue_name, idempotency_key=key)
        assert "not_found" in exc_info.value.code.value
        ctrl.kill(proc)
    finally:
        ctrl.stop_all()

    force_expire_lease(world, namespace=namespace, row_id=row_id)
    task_id = _await_delivered_one_task(
        world, BridgeProcessController(world), namespace=namespace, row_id=row_id
    )
    row = read_intent_row(world, namespace=namespace, row_id=row_id)
    assert row["state"] == "delivered"
    assert row["queue_task_id"] == task_id


@pytest.mark.parametrize("iteration", range(PROCESS_KILL_REPEATS))
def test_crash_after_enqueue_commit_before_response_25_process_kills(
    bridge_world: BridgeWorld,
    iteration: int,
) -> None:
    """After Queue commit / before response: kill + replay → one task identity."""
    _ = iteration
    world = bridge_world
    namespace = "bridge.crash"
    row_id = f"after-commit-{uuid.uuid4().hex[:12]}"
    seed_pending_intent(
        world,
        namespace=namespace,
        row_id=row_id,
        payload={"marker": "after_commit", "i": iteration},
    )
    key = bridge_idempotency_key(namespace, row_id)
    ctrl = BridgeProcessController(world)
    buffered_task_id = ctrl.run_crash_after_commit_before_response(
        namespace=namespace,
        row_id=row_id,
    )
    force_expire_lease(world, namespace=namespace, row_id=row_id)
    task_id = _await_delivered_one_task(
        world, BridgeProcessController(world), namespace=namespace, row_id=row_id
    )
    assert task_id == buffered_task_id, _diag(
        f"replay task_id diverged buffered={buffered_task_id!r} final={task_id!r}"
    )
    resolved = world.producer.resolve_submission(world.queue_name, idempotency_key=key)
    assert resolved.task.task_id == task_id
    # Matching replay must not create a second public identity.
    again = world.producer.enqueue(
        world.queue_name,
        idempotency_key=key,
        payload={"marker": "after_commit", "i": iteration},
        priority=0,
    )
    assert again.replayed is True
    assert again.task.task_id == task_id


@pytest.mark.parametrize("iteration", range(PROCESS_KILL_REPEATS))
def test_crash_after_response_before_app_ack_25_process_kills(
    bridge_world: BridgeWorld,
    iteration: int,
) -> None:
    """After response / before app ack: kill + replay resolves same task_id."""
    _ = iteration
    world = bridge_world
    namespace = "bridge.crash"
    row_id = f"after-resp-{uuid.uuid4().hex[:12]}"
    seed_pending_intent(
        world,
        namespace=namespace,
        row_id=row_id,
        payload={"marker": "after_response", "i": iteration},
    )
    key = bridge_idempotency_key(namespace, row_id)
    ctrl = BridgeProcessController(world)
    ready = ctrl.temp_ready_path("after_response")
    proc = ctrl.start(
        failpoint=CrashWindow.AFTER_RESPONSE_BEFORE_ACK,
        ready_path=ready,
        max_cycles=5,
    )
    try:
        ctrl.await_ready(ready, deadline_s=20.0)
        mid = world.producer.resolve_submission(world.queue_name, idempotency_key=key)
        mid_task_id = mid.task.task_id
        row = read_intent_row(world, namespace=namespace, row_id=row_id)
        assert row["state"] == "leased", _diag(f"expected leased mid-crash {row!r}")
        assert row["queue_task_id"] is None
        ctrl.kill(proc)
    finally:
        ctrl.stop_all()

    force_expire_lease(world, namespace=namespace, row_id=row_id)
    task_id = _await_delivered_one_task(
        world, BridgeProcessController(world), namespace=namespace, row_id=row_id
    )
    assert task_id == mid_task_id


def test_crash_after_app_ack_leaves_delivered_stable(
    bridge_world: BridgeWorld,
) -> None:
    """Crash after mark_delivered leaves delivered row stable; no new task."""
    world = bridge_world
    namespace = "bridge.crash"
    row_id = f"after-ack-{uuid.uuid4().hex[:12]}"
    seed_pending_intent(
        world,
        namespace=namespace,
        row_id=row_id,
        payload={"marker": "after_ack"},
    )
    key = bridge_idempotency_key(namespace, row_id)
    ctrl = BridgeProcessController(world)
    ready = ctrl.temp_ready_path("after_ack")
    proc = ctrl.start(
        failpoint=CrashWindow.AFTER_APP_ACK,
        ready_path=ready,
        max_cycles=5,
    )
    try:
        ctrl.await_ready(ready, deadline_s=20.0)
        row = read_intent_row(world, namespace=namespace, row_id=row_id)
        assert row["state"] == "delivered"
        task_id = row["queue_task_id"]
        assert isinstance(task_id, str) and task_id
        ctrl.kill(proc)
    finally:
        ctrl.stop_all()

    # Restart must be a no-op for this identity.
    ctrl2 = BridgeProcessController(world)
    ctrl2.start(failpoint=None, max_cycles=10)
    try:
        ctrl2.await_idle_or_exit(deadline_s=10.0)
    finally:
        ctrl2.stop_all()
    row2 = read_intent_row(world, namespace=namespace, row_id=row_id)
    assert row2["state"] == "delivered"
    assert row2["queue_task_id"] == task_id
    resolved = world.producer.resolve_submission(world.queue_name, idempotency_key=key)
    assert resolved.task.task_id == task_id


def test_two_replicas_50_reclaims_one_task(bridge_world: BridgeWorld) -> None:
    """Competing replicas + lease reclaim converge on one Queue task."""
    world = bridge_world
    namespace = "bridge.crash"
    row_id = f"reclaim-{uuid.uuid4().hex[:12]}"
    seed_pending_intent(
        world,
        namespace=namespace,
        row_id=row_id,
        payload={"marker": "reclaim50"},
    )
    key = bridge_idempotency_key(namespace, row_id)
    for cycle in range(REPLICA_RECLAIM_CYCLES):
        ctrl = BridgeProcessController(world)
        ready = ctrl.temp_ready_path(f"reclaim-{cycle}")
        proc = ctrl.start(
            failpoint=CrashWindow.BEFORE_ENQUEUE,
            ready_path=ready,
            max_cycles=3,
            lease_seconds=1,
        )
        try:
            ctrl.await_ready(ready, deadline_s=15.0)
            ctrl.kill(proc)
        finally:
            ctrl.stop_all()
        force_expire_lease(world, namespace=namespace, row_id=row_id)

    task_id = _await_delivered_one_task(
        world,
        BridgeProcessController(world),
        namespace=namespace,
        row_id=row_id,
        deadline_s=30.0,
    )
    resolved = world.producer.resolve_submission(world.queue_name, idempotency_key=key)
    assert resolved.task.task_id == task_id
    row = read_intent_row(world, namespace=namespace, row_id=row_id)
    assert row["state"] == "delivered"
    assert int(row["generation"]) >= REPLICA_RECLAIM_CYCLES


def test_queue_restart_and_process_restart_converge(
    bridge_world: BridgeWorld,
) -> None:
    """Queue HTTP restart + bridge process restart still converge to one task."""
    world = bridge_world
    namespace = "bridge.crash"
    row_id = f"qrestart-{uuid.uuid4().hex[:12]}"
    seed_pending_intent(
        world,
        namespace=namespace,
        row_id=row_id,
        payload={"marker": "queue_restart"},
    )
    ctrl = BridgeProcessController(world)
    ready = ctrl.temp_ready_path("qrestart")
    proc = ctrl.start(
        failpoint=CrashWindow.AFTER_RESPONSE_BEFORE_ACK,
        ready_path=ready,
        max_cycles=5,
    )
    try:
        ctrl.await_ready(ready, deadline_s=20.0)
        key = bridge_idempotency_key(namespace, row_id)
        mid = world.producer.resolve_submission(world.queue_name, idempotency_key=key)
        mid_id = mid.task.task_id
        ctrl.kill(proc)
    finally:
        ctrl.stop_all()

    world.restart_queue_http()
    force_expire_lease(world, namespace=namespace, row_id=row_id)
    task_id = _await_delivered_one_task(
        world, BridgeProcessController(world), namespace=namespace, row_id=row_id
    )
    assert task_id == mid_id


def test_changed_fingerprint_visible_conflict_no_second_task(
    bridge_world: BridgeWorld,
) -> None:
    """Same source identity with changed body → visible conflict, no second task."""
    world = bridge_world
    namespace = "bridge.crash"
    row_id = f"conflict-{uuid.uuid4().hex[:12]}"
    seed_pending_intent(
        world,
        namespace=namespace,
        row_id=row_id,
        payload={"marker": "original"},
    )
    key = bridge_idempotency_key(namespace, row_id)
    original_id = _await_delivered_one_task(
        world, BridgeProcessController(world), namespace=namespace, row_id=row_id
    )

    # Simulate accidental reuse of the same source identity with a new body.
    world.reset_intent_pending_with_payload(
        namespace=namespace,
        row_id=row_id,
        payload={"marker": "changed"},
    )
    ctrl = BridgeProcessController(world)
    ctrl.start(failpoint=None, max_cycles=20)
    try:
        ctrl.await_terminal_or_delivered(
            world,
            namespace=namespace,
            row_id=row_id,
            deadline_s=20.0,
        )
    finally:
        ctrl.stop_all()

    row = read_intent_row(world, namespace=namespace, row_id=row_id)
    assert row["state"] == "terminal_operator_action", _diag(f"row={row!r}")
    assert row["last_failure_code"] == "idempotency_conflict"
    # Original Queue identity unchanged; matching key still resolves it.
    resolved = world.producer.resolve_submission(world.queue_name, idempotency_key=key)
    assert resolved.task.task_id == original_id
    with pytest.raises(ProtocolError) as exc_info:
        world.producer.enqueue(
            world.queue_name,
            idempotency_key=key,
            payload={"marker": "changed"},
            priority=0,
        )
    assert exc_info.value.code.value == "idempotency_conflict"


def test_cleanup_leaves_zero_processes_and_zero_current_leases(
    bridge_world: BridgeWorld,
) -> None:
    """Harness cleanup kills bridge children and leaves no current app leases."""
    world = bridge_world
    namespace = "bridge.crash"
    row_id = f"cleanup-{uuid.uuid4().hex[:12]}"
    seed_pending_intent(
        world,
        namespace=namespace,
        row_id=row_id,
        payload={"marker": "cleanup"},
    )
    ctrl = BridgeProcessController(world)
    ready = ctrl.temp_ready_path("cleanup")
    proc = ctrl.start(
        failpoint=CrashWindow.BEFORE_ENQUEUE,
        ready_path=ready,
        max_cycles=3,
    )
    ctrl.await_ready(ready, deadline_s=15.0)
    assert proc.poll() is None
    ctrl.stop_all()
    assert ctrl.live_process_count() == 0
    force_expire_lease(world, namespace=namespace, row_id=row_id)
    # Deliver so the suite fixture teardown sees a quiet outbox.
    _await_delivered_one_task(
        world, BridgeProcessController(world), namespace=namespace, row_id=row_id
    )
    assert world.count_current_leases() == 0
    assert BridgeProcessController.global_live_process_count() == 0


def test_suite_does_not_claim_single_delivery_guarantee() -> None:
    """Refuse suite wording that promises a single-delivery external effect."""
    source = Path(__file__).read_text(encoding="utf-8")
    lowered = source.lower()
    banned = ("exactly" + "-once", "exactly" + " once")
    for phrase in banned:
        assert phrase not in lowered, f"forbidden guarantee wording: {phrase!r}"
    # Honest at-least-once boundary must remain visible in this module.
    assert "at-least-once" in lowered
