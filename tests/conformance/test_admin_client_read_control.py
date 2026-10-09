"""Live ObserverClient/AdminClient conformance for Phase 18 ownership ops."""

from __future__ import annotations

import json
import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest

from workhold_admin import ObserverClient
from tests.conformance.clients import build_client
from tests.conformance.conftest import (
    ADMIN_TOKEN,
    OBSERVER_TOKEN,
    PRODUCER_TOKEN,
    WORKER_TOKEN,
)

pytest_plugins = ["tests.integration.conftest"]

REPO_ROOT = Path(__file__).resolve().parents[2]
OWNERSHIP_PATH = REPO_ROOT / "packages" / "client-operation-ownership.json"
PHASE18_CLIENTS = frozenset({"ObserverClient", "AdminClient"})


def _phase18_operation_ids() -> frozenset[str]:
    payload = json.loads(OWNERSHIP_PATH.read_text(encoding="utf-8"))
    ids: set[str] = set()
    for entry in payload["operations"]:
        clients = set(entry.get("clients") or [])
        if clients & PHASE18_CLIENTS:
            ids.add(str(entry["operationId"]))
    return frozenset(ids)


PHASE18_OPERATION_IDS = _phase18_operation_ids()


@pytest.fixture
def observer_client(live_service_url: str, live_admin_service_url: str):
    return build_client(
        "observer",
        live_service_url,
        admin_base_url=live_admin_service_url,
    )


@pytest.fixture
def admin_client(live_service_url: str, live_admin_service_url: str):
    return build_client(
        "admin",
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


def _time_window() -> tuple[datetime, datetime]:
    now = datetime.now(tz=UTC)
    return now - timedelta(hours=1), now + timedelta(hours=1)


def _bulk_time_filters(time_from: datetime, time_to: datetime) -> dict[str, str]:
    """Indexed bulk filters require ISO-8601 ``from`` / ``to`` bounds."""

    def _wire(value: datetime) -> str:
        return value.astimezone(UTC).isoformat().replace("+00:00", "Z")

    return {"from": _wire(time_from), "to": _wire(time_to)}


def _state_value(raw: Any) -> str:
    if isinstance(raw, dict):
        return str(raw.get("value", raw))
    return str(getattr(raw, "value", raw))


def test_ownership_manifest_phase18_operation_set() -> None:
    assert PHASE18_OPERATION_IDS
    for required in (
        "createQueue",
        "listQueues",
        "getQueue",
        "createQueuePolicy",
        "activateQueuePolicy",
        "setQueueState",
        "getCapabilities",
        "getTask",
        "listTaskAttempts",
        "getStats",
        "getMaintenanceStatus",
        "listInspectionTasks",
        "listInspectionAttempts",
        "listDeadLetters",
        "listAdminAudit",
        "runMaintenance",
        "previewBulkReplay",
        "executeBulkReplay",
        "previewBulkCancel",
        "executeBulkCancel",
        "replayDeadLetter",
    ):
        assert required in PHASE18_OPERATION_IDS


def test_admin_queue_create_read_policy_state_via_typed_client(
    admin_client,
    queue_name: str,
) -> None:
    target = f"{queue_name}.ctl"
    created = admin_client.create_queue(
        name=target,
        bearer_token=ADMIN_TOKEN,
        idempotency_key=f"idem-create-{uuid.uuid4().hex}",
    )
    assert created.ok is True, created
    assert created.data["queue"]["name"] == target

    listed = admin_client.list_queues(bearer_token=ADMIN_TOKEN)
    assert listed.ok is True, listed
    names = [item["name"] for item in listed.data.get("items", ())]
    assert target in names

    got = admin_client.get_queue(queue_name=target, bearer_token=ADMIN_TOKEN)
    assert got.ok is True, got
    assert got.data["name"] == target
    config_v = int(got.data["config_version"])

    policy = admin_client.create_queue_policy(
        queue_name=target,
        bearer_token=ADMIN_TOKEN,
        idempotency_key=f"idem-policy-{uuid.uuid4().hex}",
    )
    assert policy.ok is True, policy
    after_policy = admin_client.get_queue(queue_name=target, bearer_token=ADMIN_TOKEN)
    assert after_policy.ok is True, after_policy
    config_v = int(after_policy.data["config_version"])

    activate = admin_client.activate_queue_policy(
        queue_name=target,
        policy_version=2,
        expected_config_version=config_v,
        bearer_token=ADMIN_TOKEN,
        idempotency_key=f"idem-activate-{uuid.uuid4().hex}",
    )
    assert activate.ok is True, activate
    config_v = int(activate.data["queue"]["config_version"])

    paused = admin_client.set_queue_state(
        queue_name=target,
        state="paused",
        expected_config_version=config_v,
        bearer_token=ADMIN_TOKEN,
        idempotency_key=f"idem-pause-{uuid.uuid4().hex}",
    )
    assert paused.ok is True, paused
    assert _state_value(paused.data["queue"]["state"]) == "paused"
    config_v = int(paused.data["queue"]["config_version"])

    stale = admin_client.set_queue_state(
        queue_name=target,
        state="active",
        expected_config_version=max(config_v - 1, 0),
        bearer_token=ADMIN_TOKEN,
        idempotency_key=f"idem-stale-{uuid.uuid4().hex}",
    )
    assert stale.ok is False
    assert stale.error_code is not None

    resumed = admin_client.set_queue_state(
        queue_name=target,
        state="active",
        expected_config_version=config_v,
        bearer_token=ADMIN_TOKEN,
        idempotency_key=f"idem-resume-{uuid.uuid4().hex}",
    )
    assert resumed.ok is True, resumed


def test_observer_and_admin_read_audit_maintenance_matrix(
    observer_client,
    admin_client,
    raw_client,
    queue_name: str,
) -> None:
    got = admin_client.get_queue(queue_name=queue_name, bearer_token=ADMIN_TOKEN)
    assert got.ok is True, got

    enqueued = raw_client.enqueue(
        queue_name=queue_name,
        idempotency_key=f"idem-task-{uuid.uuid4().hex}",
        payload={"phase": 18, "marker": "observer-matrix"},
        bearer_token=PRODUCER_TOKEN,
    )
    assert enqueued.ok is True, enqueued
    task_id = enqueued.data["task"]["task_id"]
    time_from, time_to = _time_window()

    assert observer_client.get_capabilities(bearer_token=OBSERVER_TOKEN).ok
    assert admin_client.get_capabilities(bearer_token=ADMIN_TOKEN).ok

    task = observer_client.get_task(task_id=task_id, bearer_token=OBSERVER_TOKEN)
    assert task.ok is True, task
    assert task.data["task_id"] == task_id

    assert observer_client.list_task_attempts(
        task_id=task_id, bearer_token=OBSERVER_TOKEN
    ).ok

    assert observer_client.get_queue(
        queue_name=queue_name, bearer_token=OBSERVER_TOKEN
    ).ok
    assert admin_client.get_queue(queue_name=queue_name, bearer_token=ADMIN_TOKEN).ok

    # OpenAPI getStats is parameterless (GET /admin/v1/stats).
    assert observer_client.get_stats(bearer_token=OBSERVER_TOKEN).ok
    assert admin_client.get_stats(bearer_token=ADMIN_TOKEN).ok

    assert observer_client.get_maintenance_status(bearer_token=OBSERVER_TOKEN).ok
    assert admin_client.get_maintenance_status(bearer_token=ADMIN_TOKEN).ok

    assert observer_client.list_inspection_tasks(
        queue_name=queue_name, bearer_token=OBSERVER_TOKEN
    ).ok
    assert admin_client.list_inspection_tasks(
        queue_name=queue_name, bearer_token=ADMIN_TOKEN
    ).ok

    assert observer_client.list_inspection_attempts(
        task_id=task_id,
        bearer_token=OBSERVER_TOKEN,
        time_from=time_from,
        time_to=time_to,
    ).ok
    assert admin_client.list_inspection_attempts(
        task_id=task_id,
        bearer_token=ADMIN_TOKEN,
        time_from=time_from,
        time_to=time_to,
    ).ok

    assert observer_client.list_dead_letters(
        queue_name=queue_name,
        bearer_token=OBSERVER_TOKEN,
        time_from=time_from,
        time_to=time_to,
    ).ok
    assert admin_client.list_dead_letters(
        queue_name=queue_name,
        bearer_token=ADMIN_TOKEN,
        time_from=time_from,
        time_to=time_to,
    ).ok

    assert admin_client.list_admin_audit(
        bearer_token=ADMIN_TOKEN,
        time_from=time_from,
        time_to=time_to,
        queue_name=queue_name,
    ).ok

    assert admin_client.run_maintenance(
        bearer_token=ADMIN_TOKEN,
        idempotency_key=f"idem-maint-{uuid.uuid4().hex}",
    ).ok

    bulk_filters = _bulk_time_filters(time_from, time_to)

    assert admin_client.preview_bulk_replay(
        queue_name=queue_name,
        bearer_token=ADMIN_TOKEN,
        filters=bulk_filters,
    ).ok
    assert admin_client.preview_bulk_cancel(
        queue_name=queue_name,
        bearer_token=ADMIN_TOKEN,
        filters=bulk_filters,
    ).ok

    claimed = raw_client.claim(
        queues=[queue_name],
        worker_id=f"worker-18-04-{uuid.uuid4().hex[:8]}",
        lease_seconds=30,
        bearer_token=WORKER_TOKEN,
    )
    assert claimed.ok is True, claimed
    tasks = claimed.data.get("tasks") or []
    assert tasks, claimed
    handle = tasks[0].get("claim") or tasks[0]
    failed = raw_client.fail(
        claim_id=handle["claim_id"],
        claim_token=handle["claim_token"],
        generation=int(handle["generation"]),
        bearer_token=WORKER_TOKEN,
        retryable=False,
        failure_code="worker.fatal",
        failure_detail="phase-18-04-dlq",
    )
    assert failed.ok is True, failed

    assert admin_client.replay_dead_letter(
        queue_name=queue_name,
        task_id=task_id,
        bearer_token=ADMIN_TOKEN,
        idempotency_key=f"idem-replay-{uuid.uuid4().hex}",
        reason="phase-18-04-replay",
    ).ok

    sdk = admin_client._client(ADMIN_TOKEN)  # noqa: SLF001
    replay_preview_obj = sdk.preview_bulk_replay(queue_name, filters=bulk_filters)
    assert admin_client.execute_bulk_replay(
        queue_name=queue_name,
        bearer_token=ADMIN_TOKEN,
        preview=replay_preview_obj,
        idempotency_key=f"idem-bulk-replay-{uuid.uuid4().hex}",
        reason="phase-18-04-bulk-replay",
        filters=bulk_filters,
    ).ok

    cancel_preview_obj = sdk.preview_bulk_cancel(queue_name, filters=bulk_filters)
    assert admin_client.execute_bulk_cancel(
        queue_name=queue_name,
        bearer_token=ADMIN_TOKEN,
        preview=cancel_preview_obj,
        reason="phase-18-04-bulk-cancel",
        filters=bulk_filters,
    ).ok

    assert (
        observer_client.get_queue(
            queue_name=queue_name, bearer_token=PRODUCER_TOKEN
        ).ok
        is False
    )
    assert (
        admin_client.create_queue(
            name=f"{queue_name}.denied",
            bearer_token=OBSERVER_TOKEN,
            idempotency_key=f"idem-denied-{uuid.uuid4().hex}",
        ).ok
        is False
    )
    assert (
        admin_client.list_admin_audit(
            bearer_token=OBSERVER_TOKEN,
            time_from=time_from,
            time_to=time_to,
        ).ok
        is False
    )
    assert (
        admin_client.run_maintenance(
            bearer_token=PRODUCER_TOKEN,
            idempotency_key=f"idem-maint-denied-{uuid.uuid4().hex}",
        ).ok
        is False
    )
    assert (
        admin_client.replay_dead_letter(
            queue_name=queue_name,
            task_id=task_id,
            bearer_token=WORKER_TOKEN,
            idempotency_key=f"idem-replay-denied-{uuid.uuid4().hex}",
            reason="denied",
        ).ok
        is False
    )


def test_observer_mutation_absent_on_api_and_server_authorization(
    observer_client,
    live_service_url: str,
    live_admin_service_url: str,
    queue_name: str,
) -> None:
    assert not hasattr(ObserverClient, "create_queue")
    assert not hasattr(ObserverClient, "set_queue_state")
    assert not hasattr(ObserverClient, "run_maintenance")
    assert not hasattr(ObserverClient, "replay_dead_letter")
    assert not hasattr(observer_client, "create_queue")
    assert not hasattr(observer_client, "set_queue_state")

    admin = build_client(
        "admin",
        live_service_url,
        admin_base_url=live_admin_service_url,
    )
    denied = admin.create_queue(
        name=f"{queue_name}.observer-denied",
        bearer_token=OBSERVER_TOKEN,
        idempotency_key=f"idem-obs-deny-{uuid.uuid4().hex}",
    )
    assert denied.ok is False
