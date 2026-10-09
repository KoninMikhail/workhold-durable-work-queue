"""Live consumer SDK conformance: manifest ops, isolation, redaction (17-03).

Uses real PostgreSQL + live application/admin planes. Consumer lease operations
go through ``ConsumerClientAdapter``; negatives prove worker credentials are
required for claim/lease ops and diagnostics never leak claim tokens or secrets.
"""

from __future__ import annotations

import uuid

import pytest

from tests.conformance.clients import build_client
from tests.conformance.conftest import (
    ADMIN_TOKEN,
    PRODUCER_TOKEN,
    WORKER_TOKEN,
)

pytest_plugins = ["tests.integration.conftest"]


@pytest.fixture
def consumer_client(live_service_url: str, live_admin_service_url: str):
    return build_client(
        "consumer",
        live_service_url,
        admin_base_url=live_admin_service_url,
    )


@pytest.fixture
def raw_client(live_service_url: str, live_admin_service_url: str):
    return build_client(
        "raw_http",
        live_service_url,
        admin_base_url=live_admin_service_url,
    )


def test_consumer_manifest_ops_with_worker_credentials(
    consumer_client,
    queue_name: str,
) -> None:
    """ConsumerClient ownership-manifest ops with WORKER token."""

    caps = consumer_client.get_capabilities(bearer_token=WORKER_TOKEN)
    assert caps.ok is True
    assert caps.data

    enqueued = consumer_client.enqueue(
        queue_name=queue_name,
        idempotency_key=f"idem-consumer-live-{uuid.uuid4().hex}",
        payload={"phase": 17, "marker": "consumer-live"},
        bearer_token=PRODUCER_TOKEN,
    )
    assert enqueued.ok is True, enqueued
    task_id = enqueued.data["task"]["task_id"]

    claimed = consumer_client.claim(
        queues=[queue_name],
        worker_id="worker-consumer-live",
        lease_seconds=30,
        bearer_token=WORKER_TOKEN,
    )
    assert claimed.ok is True, claimed
    tasks = claimed.data["tasks"]
    assert len(tasks) == 1
    claim = tasks[0]["claim"]
    assert tasks[0]["task"]["task_id"] == task_id
    claim_id = str(claim["claim_id"])
    claim_token = str(claim["claim_token"])
    generation = int(claim["generation"])

    hb = consumer_client.heartbeat(
        claim_id=claim_id,
        claim_token=claim_token,
        generation=generation,
        lease_seconds=30,
        bearer_token=WORKER_TOKEN,
    )
    assert hb.ok is True, hb
    assert hb.data["claim"]["claim_id"] == claim_id
    assert "claim_token" not in hb.data.get("claim", {})

    done = consumer_client.complete(
        claim_id=claim_id,
        claim_token=claim_token,
        generation=generation,
        bearer_token=WORKER_TOKEN,
    )
    assert done.ok is True, done
    assert done.data.get("task_id") == task_id or (
        isinstance(done.data.get("task"), dict)
        and done.data["task"].get("task_id") == task_id
    )


def test_consumer_fail_and_ack_cancel_paths(
    consumer_client,
    queue_name: str,
) -> None:
    """failClaim and acknowledgeClaimCancellation through the consumer adapter."""

    enqueued = consumer_client.enqueue(
        queue_name=queue_name,
        idempotency_key=f"idem-consumer-fail-{uuid.uuid4().hex}",
        payload={"phase": 17, "marker": "consumer-fail"},
        bearer_token=PRODUCER_TOKEN,
    )
    assert enqueued.ok is True, enqueued

    claimed = consumer_client.claim(
        queues=[queue_name],
        worker_id="worker-consumer-fail",
        lease_seconds=30,
        bearer_token=WORKER_TOKEN,
    )
    assert claimed.ok is True, claimed
    claim = claimed.data["tasks"][0]["claim"]

    failed = consumer_client.fail(
        claim_id=str(claim["claim_id"]),
        claim_token=str(claim["claim_token"]),
        generation=int(claim["generation"]),
        bearer_token=WORKER_TOKEN,
        retryable=True,
        failure_code="handler_timeout",
        failure_detail="live-conformance",
    )
    assert failed.ok is True, failed

    enqueued2 = consumer_client.enqueue(
        queue_name=queue_name,
        idempotency_key=f"idem-consumer-ack-{uuid.uuid4().hex}",
        payload={"phase": 17, "marker": "consumer-ack"},
        bearer_token=PRODUCER_TOKEN,
    )
    assert enqueued2.ok is True, enqueued2
    task_id = enqueued2.data["task"]["task_id"]

    claimed2 = consumer_client.claim(
        queues=[queue_name],
        worker_id="worker-consumer-ack",
        lease_seconds=30,
        bearer_token=WORKER_TOKEN,
    )
    assert claimed2.ok is True, claimed2
    claimed_task = claimed2.data["tasks"][0]
    claim2 = claimed_task["claim"]
    # Cancel the task actually leased (retryable fail above may requeue the
    # first task ahead of the second enqueue).
    leased_task_id = str(claimed_task["task"]["task_id"])

    cancelled = consumer_client.cancel(
        task_id=leased_task_id,
        bearer_token=PRODUCER_TOKEN,
        reason="consumer-live-cancel",
    )
    assert cancelled.ok is True, cancelled

    acked = consumer_client.ack_cancel(
        claim_id=str(claim2["claim_id"]),
        claim_token=str(claim2["claim_token"]),
        generation=int(claim2["generation"]),
        bearer_token=WORKER_TOKEN,
    )
    assert acked.ok is True, acked


def test_producer_and_admin_credentials_cannot_claim(
    consumer_client,
    queue_name: str,
) -> None:
    """Producer/admin tokens are rejected on consumer claim."""

    enqueued = consumer_client.enqueue(
        queue_name=queue_name,
        idempotency_key=f"idem-consumer-neg-{uuid.uuid4().hex}",
        payload={"phase": 17, "marker": "neg"},
        bearer_token=PRODUCER_TOKEN,
    )
    assert enqueued.ok is True, enqueued

    for token in (PRODUCER_TOKEN, ADMIN_TOKEN):
        denied = consumer_client.claim(
            queues=[queue_name],
            worker_id="worker-consumer-neg",
            lease_seconds=30,
            bearer_token=token,
        )
        assert denied.ok is False
        assert denied.error_code in {
            "authentication_failed",
            "unauthenticated",
            "unauthorized",
            "permission_denied",
            "forbidden",
        }


def test_consumer_adapter_diagnostics_never_leak_secrets(
    consumer_client,
    queue_name: str,
) -> None:
    secret = WORKER_TOKEN
    blob = f"{consumer_client!r}{consumer_client!s}"
    assert secret not in blob

    denied = consumer_client.claim(
        queues=[queue_name],
        worker_id="worker-secret-scan",
        lease_seconds=30,
        bearer_token="definitely-not-a-token",
    )
    assert denied.ok is False
    err_blob = f"{denied!r}{denied.error_code}{denied.data}"
    assert secret not in err_blob
    assert "definitely-not-a-token" not in err_blob

    enqueued = consumer_client.enqueue(
        queue_name=queue_name,
        idempotency_key=f"idem-consumer-secret-{uuid.uuid4().hex}",
        payload={"phase": 17, "marker": "secret-scan"},
        bearer_token=PRODUCER_TOKEN,
    )
    assert enqueued.ok is True, enqueued
    claimed = consumer_client.claim(
        queues=[queue_name],
        worker_id="worker-secret-scan-2",
        lease_seconds=30,
        bearer_token=WORKER_TOKEN,
    )
    assert claimed.ok is True, claimed
    claim_token = claimed.data["tasks"][0]["claim"]["claim_token"]
    # Wire-shaped OperationResult.data retains claim_token for dual-client
    # parity; adapter/client diagnostics must not.
    assert claim_token not in f"{consumer_client!r}{consumer_client!s}"
    hb = consumer_client.heartbeat(
        claim_id=str(claimed.data["tasks"][0]["claim"]["claim_id"]),
        claim_token=claim_token,
        generation=int(claimed.data["tasks"][0]["claim"]["generation"]),
        lease_seconds=30,
        bearer_token=WORKER_TOKEN,
    )
    assert hb.ok is True, hb
    assert claim_token not in f"{hb!r}{hb.data}"
    consumer_client.complete(
        claim_id=str(claimed.data["tasks"][0]["claim"]["claim_id"]),
        claim_token=claim_token,
        generation=int(claimed.data["tasks"][0]["claim"]["generation"]),
        bearer_token=WORKER_TOKEN,
    )
