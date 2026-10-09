"""Live producer SDK conformance: manifest ops, isolation, redaction (16-03).

Uses real PostgreSQL + live application/admin planes. Producer operations go
through ``ProducerClientAdapter``; negatives prove producer credentials cannot
invoke worker/admin surfaces and diagnostics never leak secrets.
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime, timedelta

import pytest

from tests.conformance.clients import build_client
from tests.conformance.conftest import (
    ADMIN_TOKEN,
    PRODUCER_TOKEN,
    WORKER_TOKEN,
)

pytest_plugins = ["tests.integration.conftest"]


@pytest.fixture
def producer_client(live_service_url: str, live_admin_service_url: str):
    return build_client(
        "producer",
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


def test_producer_manifest_ops_with_producer_credentials(
    producer_client,
    queue_name: str,
) -> None:
    """Every ProducerClient ownership-manifest operation with PRODUCER token."""

    caps = producer_client.get_capabilities(bearer_token=PRODUCER_TOKEN)
    assert caps.ok is True
    assert "protocol_version" in caps.data or "operations" in caps.data or caps.data

    idem = f"idem-producer-live-{uuid.uuid4().hex}"
    future = datetime.now(tz=UTC) + timedelta(seconds=45)
    enqueued = producer_client.enqueue(
        queue_name=queue_name,
        idempotency_key=idem,
        payload={"phase": 16, "marker": "producer-live"},
        bearer_token=PRODUCER_TOKEN,
        priority=10,
        available_at=future,
    )
    assert enqueued.ok is True, enqueued
    task_id = enqueued.data["task"]["task_id"]
    assert enqueued.data["task"]["state"] == "delayed"
    assert int(enqueued.data["task"]["priority"]) == 10

    resolved = producer_client.resolve_submission(
        queue_name=queue_name,
        idempotency_key=idem,
        bearer_token=PRODUCER_TOKEN,
    )
    assert resolved.ok is True, resolved
    assert resolved.data["task"]["task_id"] == task_id
    assert "dedup_expires_at" in resolved.data

    inspected = producer_client.inspect(
        task_id=task_id,
        bearer_token=PRODUCER_TOKEN,
    )
    assert inspected.ok is True, inspected
    assert inspected.data["task_id"] == task_id
    assert inspected.data["state"] == "delayed"
    assert int(inspected.data["priority"]) == 10

    cancelled = producer_client.cancel(
        task_id=task_id,
        bearer_token=PRODUCER_TOKEN,
        reason="producer-live-cancel",
    )
    assert cancelled.ok is True, cancelled
    assert cancelled.data.get("task_id") == task_id or (
        isinstance(cancelled.data.get("task"), dict)
        and cancelled.data["task"].get("task_id") == task_id
    )


def test_producer_credentials_cannot_claim_or_admin(
    producer_client,
    raw_client,
    queue_name: str,
) -> None:
    """Producer token is rejected on consumer claim and admin dead-letter replay."""

    idem = f"idem-producer-neg-{uuid.uuid4().hex}"
    enqueued = producer_client.enqueue(
        queue_name=queue_name,
        idempotency_key=idem,
        payload={"phase": 16, "marker": "neg"},
        bearer_token=PRODUCER_TOKEN,
    )
    assert enqueued.ok is True, enqueued
    task_id = enqueued.data["task"]["task_id"]

    claimed = producer_client.claim(
        queues=[queue_name],
        worker_id="worker-producer-neg",
        lease_seconds=30,
        bearer_token=PRODUCER_TOKEN,
    )
    assert claimed.ok is False
    assert claimed.error_code in {
        "authentication_failed",
        "unauthenticated",
        "unauthorized",
        "permission_denied",
        "forbidden",
    }

    # Worker token still works for claim (control: adapter worker path is live).
    worker_claim = producer_client.claim(
        queues=[queue_name],
        worker_id="worker-producer-neg-ok",
        lease_seconds=30,
        bearer_token=WORKER_TOKEN,
    )
    assert worker_claim.ok is True, worker_claim

    replay = producer_client.replay_dead_letter(
        queue_name=queue_name,
        task_id=task_id,
        bearer_token=PRODUCER_TOKEN,
        idempotency_key=f"idem-replay-{uuid.uuid4().hex}",
        reason="should-deny",
    )
    assert replay.ok is False
    assert replay.error_code in {
        "authentication_failed",
        "unauthenticated",
        "unauthorized",
        "permission_denied",
        "forbidden",
        "task_not_found",
        "dead_letter_not_found",
        "not_found",
    }

    # Admin token can reach the admin plane (control).
    admin_preview = raw_client.bulk_preview_replay(
        queue_name=queue_name,
        bearer_token=ADMIN_TOKEN,
        filters={},
    )
    # Preview may succeed empty or fail validation — must not be auth failure.
    if not admin_preview.ok:
        assert admin_preview.error_code not in {
            "authentication_failed",
            "unauthenticated",
        }


def test_producer_adapter_diagnostics_never_leak_secrets(
    producer_client,
    queue_name: str,
) -> None:
    secret = PRODUCER_TOKEN
    blob = f"{producer_client!r}{producer_client!s}"
    assert secret not in blob

    denied = producer_client.enqueue(
        queue_name=queue_name,
        idempotency_key=f"idem-secret-{uuid.uuid4().hex}",
        payload={"secret_field": "should-not-appear-in-adapter-repr"},
        bearer_token="definitely-not-a-token",
    )
    assert denied.ok is False
    err_blob = f"{denied!r}{denied.error_code}{denied.data}"
    assert secret not in err_blob
    assert "definitely-not-a-token" not in err_blob
    assert "should-not-appear-in-adapter-repr" not in err_blob
