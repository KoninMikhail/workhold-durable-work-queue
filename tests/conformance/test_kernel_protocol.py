"""Black-box kernel protocol scenarios parametrized across raw_http and sdk.

Both client kinds share one live Queue HTTP server and one PostgreSQL schema.
Raw HTTP retains wire status/headers/JSON so SDK translation cannot mask drift.
"""

from __future__ import annotations

import time
import uuid
from typing import Any

import pytest

from queue_service.domain.queue_control import QueueState
from tests.conformance.clients import OperationResult, build_client
from tests.conformance.conftest import (
    FOREIGN_TOKEN,
    LEASE_SECONDS,
    PRODUCER_TOKEN,
    WORKER_TOKEN,
    set_queue_state,
)
from tests.conformance.faults import DropCommittedResponseProxy


def _task_id(result: OperationResult) -> str:
    task = result.data.get("task")
    if isinstance(task, dict) and isinstance(task.get("task_id"), str):
        return task["task_id"]
    raise AssertionError(f"enqueue result missing task_id: {result.data!r}")


def _first_claim(result: OperationResult) -> tuple[dict[str, Any], dict[str, Any]]:
    tasks = result.data.get("tasks")
    assert isinstance(tasks, list) and tasks, f"expected claimed tasks, got {result.data!r}"
    item = tasks[0]
    assert isinstance(item, dict)
    task = item["task"]
    claim = item["claim"]
    assert isinstance(task, dict) and isinstance(claim, dict)
    return task, claim


def _assert_raw_wire(
    result: OperationResult, *, min_status: int = 200, max_status: int = 299
) -> None:
    if result.wire is None:
        return
    assert min_status <= result.wire.status_code <= max_status
    assert any(k.lower() == "content-type" for k in result.wire.headers)
    assert result.wire.body is not None


def test_enqueue_idempotency(conformance_client, queue_name: str) -> None:
    key = f"idem-{uuid.uuid4().hex}"
    payload = {"marker": "enqueue-idempotency", "nonce": uuid.uuid4().hex}

    first = conformance_client.enqueue(
        queue_name=queue_name,
        idempotency_key=key,
        payload=payload,
        bearer_token=PRODUCER_TOKEN,
    )
    assert first.ok, first
    _assert_raw_wire(first, min_status=200, max_status=201)
    first_id = _task_id(first)

    second = conformance_client.enqueue(
        queue_name=queue_name,
        idempotency_key=key,
        payload=payload,
        bearer_token=PRODUCER_TOKEN,
    )
    assert second.ok, second
    _assert_raw_wire(second, min_status=200, max_status=201)
    assert _task_id(second) == first_id
    if second.wire is not None:
        assert second.wire.status_code in {200, 201}
        body = second.wire.body
        assert isinstance(body, dict)
        assert body["task"]["task_id"] == first_id


def test_claim_fencing(conformance_client, queue_name: str) -> None:
    enq = conformance_client.enqueue(
        queue_name=queue_name,
        idempotency_key=f"idem-{uuid.uuid4().hex}",
        payload={"marker": "claim-fencing"},
        bearer_token=PRODUCER_TOKEN,
    )
    assert enq.ok, enq
    task_id = _task_id(enq)

    claimed = conformance_client.claim(
        queues=[queue_name],
        worker_id="worker-fence-1",
        lease_seconds=LEASE_SECONDS,
        bearer_token=WORKER_TOKEN,
    )
    assert claimed.ok, claimed
    _assert_raw_wire(claimed)
    task, claim = _first_claim(claimed)
    assert task["task_id"] == task_id
    claim_id = str(claim["claim_id"])
    token = str(claim["claim_token"])
    generation = int(claim["generation"])

    stale = conformance_client.complete(
        claim_id=claim_id,
        claim_token=token,
        generation=generation + 1,
        bearer_token=WORKER_TOKEN,
    )
    assert not stale.ok
    assert stale.error_code in {
        "lease_lost",
        "fencing_token_mismatch",
        "claim_generation_mismatch",
    }
    if stale.wire is not None:
        assert stale.wire.status_code in {409, 412, 422}
        assert isinstance(stale.wire.body, dict)
        assert stale.wire.body.get("code") == stale.error_code


def test_stale_heartbeat_complete(conformance_client, queue_name: str) -> None:
    assert conformance_client.enqueue(
        queue_name=queue_name,
        idempotency_key=f"idem-{uuid.uuid4().hex}",
        payload={"marker": "stale-lease"},
        bearer_token=PRODUCER_TOKEN,
    ).ok

    claimed = conformance_client.claim(
        queues=[queue_name],
        worker_id="worker-stale-1",
        lease_seconds=LEASE_SECONDS,
        bearer_token=WORKER_TOKEN,
    )
    assert claimed.ok, claimed
    _, claim = _first_claim(claimed)
    claim_id = str(claim["claim_id"])
    token = str(claim["claim_token"])
    generation = int(claim["generation"])

    hb = conformance_client.heartbeat(
        claim_id=claim_id,
        claim_token=token,
        generation=generation + 99,
        lease_seconds=LEASE_SECONDS,
        bearer_token=WORKER_TOKEN,
    )
    assert not hb.ok
    assert hb.error_code in {
        "lease_lost",
        "fencing_token_mismatch",
        "claim_generation_mismatch",
        "validation_failed",
    }

    done = conformance_client.complete(
        claim_id=claim_id,
        claim_token=str(uuid.uuid4()),
        generation=generation,
        bearer_token=WORKER_TOKEN,
    )
    assert not done.ok
    assert done.error_code in {
        "lease_lost",
        "authentication_failed",
        "unauthorized",
        "permission_denied",
        "claim_not_found",
        "validation_failed",
    }


def test_uncertain_complete_replay(
    conformance_client, live_service_url: str, queue_name: str
) -> None:
    assert conformance_client.enqueue(
        queue_name=queue_name,
        idempotency_key=f"idem-{uuid.uuid4().hex}",
        payload={"marker": "uncertain-complete"},
        bearer_token=PRODUCER_TOKEN,
    ).ok
    claimed = conformance_client.claim(
        queues=[queue_name],
        worker_id="worker-uncertain-1",
        lease_seconds=LEASE_SECONDS,
        bearer_token=WORKER_TOKEN,
    )
    assert claimed.ok, claimed
    _, claim = _first_claim(claimed)
    claim_id = str(claim["claim_id"])
    token = str(claim["claim_token"])
    generation = int(claim["generation"])

    # Force the uncertain window at the wire with raw HTTP so SDK handle/transport
    # binding cannot bypass the drop proxy. Replay still runs through both kinds.
    proxy = DropCommittedResponseProxy()
    deadline = time.monotonic() + 20.0
    listen_url = proxy.serve_once(live_service_url, deadline)
    dropped = build_client("raw_http", listen_url).complete(
        claim_id=claim_id,
        claim_token=token,
        generation=generation,
        bearer_token=WORKER_TOKEN,
    )
    assert not dropped.ok
    assert dropped.error_code == "transport_error"
    buffered = proxy.wait(deadline)
    assert buffered.status_code in {200, 201}
    proxy.close()

    replay = conformance_client.complete(
        claim_id=claim_id,
        claim_token=token,
        generation=generation,
        bearer_token=WORKER_TOKEN,
    )
    assert replay.ok, replay
    _assert_raw_wire(replay)
    if replay.wire is not None:
        assert isinstance(replay.wire.body, dict)
        assert replay.wire.body.get("task_id") or replay.wire.body.get("task")
    assert replay.data.get("task_id") or (
        isinstance(replay.data.get("task"), dict) and replay.data["task"].get("task_id")
    )


def test_cancellation(conformance_client, queue_name: str) -> None:
    enq = conformance_client.enqueue(
        queue_name=queue_name,
        idempotency_key=f"idem-{uuid.uuid4().hex}",
        payload={"marker": "cancel"},
        bearer_token=PRODUCER_TOKEN,
    )
    assert enq.ok, enq
    task_id = _task_id(enq)

    claimed = conformance_client.claim(
        queues=[queue_name],
        worker_id="worker-cancel-1",
        lease_seconds=LEASE_SECONDS,
        bearer_token=WORKER_TOKEN,
    )
    assert claimed.ok, claimed
    _, claim = _first_claim(claimed)

    cancelled = conformance_client.cancel(
        task_id=task_id,
        bearer_token=PRODUCER_TOKEN,
        reason="kernel-dual-cancel",
    )
    assert cancelled.ok, cancelled
    _assert_raw_wire(cancelled)

    acked = conformance_client.ack_cancel(
        claim_id=str(claim["claim_id"]),
        claim_token=str(claim["claim_token"]),
        generation=int(claim["generation"]),
        bearer_token=WORKER_TOKEN,
    )
    assert acked.ok, acked


def test_queue_active_paused_draining_gates(
    conformance_client,
    session_factory,
    queue_name: str,
) -> None:
    session = session_factory()
    try:
        set_queue_state(
            session,
            queue_name=queue_name,
            state=QueueState.PAUSED,
            expected_config_version=1,
        )
    finally:
        session.close()

    assert conformance_client.enqueue(
        queue_name=queue_name,
        idempotency_key=f"idem-{uuid.uuid4().hex}",
        payload={"marker": "paused-enqueue-allowed"},
        bearer_token=PRODUCER_TOKEN,
    ).ok

    paused_claim = conformance_client.claim(
        queues=[queue_name],
        worker_id="worker-paused",
        lease_seconds=LEASE_SECONDS,
        bearer_token=WORKER_TOKEN,
    )
    assert paused_claim.ok, paused_claim
    assert paused_claim.data.get("tasks") == []
    _assert_raw_wire(paused_claim)

    session = session_factory()
    try:
        set_queue_state(
            session,
            queue_name=queue_name,
            state=QueueState.DRAINING,
            expected_config_version=2,
        )
    finally:
        session.close()

    drained = conformance_client.enqueue(
        queue_name=queue_name,
        idempotency_key=f"idem-{uuid.uuid4().hex}",
        payload={"marker": "draining-reject"},
        bearer_token=PRODUCER_TOKEN,
    )
    assert not drained.ok
    assert drained.error_code in {
        "queue_draining",
        "queue_not_accepting",
        "conflict",
        "failed_precondition",
    }

    session = session_factory()
    try:
        set_queue_state(
            session,
            queue_name=queue_name,
            state=QueueState.ACTIVE,
            expected_config_version=3,
        )
    finally:
        session.close()

    active = conformance_client.enqueue(
        queue_name=queue_name,
        idempotency_key=f"idem-{uuid.uuid4().hex}",
        payload={"marker": "active-ok"},
        bearer_token=PRODUCER_TOKEN,
    )
    assert active.ok, active


def test_structured_failures(conformance_client, queue_name: str) -> None:
    assert conformance_client.enqueue(
        queue_name=queue_name,
        idempotency_key=f"idem-{uuid.uuid4().hex}",
        payload={"marker": "structured-fail"},
        bearer_token=PRODUCER_TOKEN,
    ).ok
    claimed = conformance_client.claim(
        queues=[queue_name],
        worker_id="worker-fail-1",
        lease_seconds=LEASE_SECONDS,
        bearer_token=WORKER_TOKEN,
    )
    assert claimed.ok, claimed
    _, claim = _first_claim(claimed)

    failed = conformance_client.fail(
        claim_id=str(claim["claim_id"]),
        claim_token=str(claim["claim_token"]),
        generation=int(claim["generation"]),
        bearer_token=WORKER_TOKEN,
        retryable=False,
        failure_code="worker.fatal",
        failure_detail="kernel dual structured failure",
    )
    assert failed.ok, failed
    _assert_raw_wire(failed)
    if failed.wire is not None:
        assert isinstance(failed.wire.body, dict)


def test_authorization(conformance_client, queue_name: str) -> None:
    denied = conformance_client.enqueue(
        queue_name=queue_name,
        idempotency_key=f"idem-{uuid.uuid4().hex}",
        payload={"marker": "authz"},
        bearer_token=FOREIGN_TOKEN,
    )
    assert not denied.ok
    assert denied.error_code in {
        "authentication_failed",
        "unauthorized",
        "permission_denied",
        "forbidden",
    }
    if denied.wire is not None:
        assert denied.wire.status_code in {401, 403}
        assert isinstance(denied.wire.body, dict)
        assert denied.wire.body.get("code")

    unauthenticated = conformance_client.claim(
        queues=[queue_name],
        worker_id="worker-no-auth",
        lease_seconds=LEASE_SECONDS,
        bearer_token="definitely-not-a-token",
    )
    assert not unauthenticated.ok
    if unauthenticated.wire is not None:
        assert unauthenticated.wire.status_code in {401, 403}
