"""Golden parity: AdminClient/BreakGlassClient vs async peers."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

import pytest

from _queue_service_client_core.async_transport import HttpxAsyncTransport
from _queue_service_client_core.transport import HttpJsonTransport
from queue_service_admin import AdminClient, BreakGlassClient
from queue_service_admin.async_client import (
    AsyncAdminClient,
    AsyncBreakGlassClient,
    AsyncObserverClient,
)
from queue_service_admin.models import (
    BackoffStrategy,
    ConfigVersion,
    PolicyVersion,
    QueueState,
    RetryPolicyDraft,
)

from .conftest import normalize_recorded

ADMIN_TOKEN = "admin-secret-token"
BREAK_GLASS_TOKEN = "break-glass-secret-token"
TASK_ID = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
EVENT_ID = "eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee"


def _queue_body(**overrides: Any) -> dict[str, Any]:
    body: dict[str, Any] = {
        "queue_id": "cccccccc-cccc-4ccc-8ccc-cccccccccccc",
        "name": "orders",
        "state": "active",
        "config_version": 1,
        "active_policy": {
            "version": 1,
            "enabled": True,
            "max_attempts": 3,
            "backoff_strategy": "fixed",
            "retry_delay_seconds": 5,
            "created_at": "2026-09-19T00:00:00Z",
        },
        "created_at": "2026-09-19T00:00:00Z",
        "updated_at": "2026-09-19T00:00:00Z",
    }
    body.update(overrides)
    return body


def _mutation_result(**queue_overrides: Any) -> dict[str, Any]:
    return {
        "queue": _queue_body(**queue_overrides),
        "replayed": False,
        "admin_replay_expires_at": "2026-10-19T00:00:00Z",
    }


def _policy_draft() -> RetryPolicyDraft:
    return RetryPolicyDraft(
        enabled=True,
        max_attempts=3,
        backoff_strategy=BackoffStrategy("fixed"),
        retry_delay_seconds=5,
    )


def _bg_mutation(**overrides: Any) -> dict[str, Any]:
    body: dict[str, Any] = {
        "operation": "forceLeaseExpiry",
        "queue": "orders",
        "target_id": TASK_ID,
        "outcome": "applied",
        "generation": 3,
    }
    body.update(overrides)
    return body


def _replay_result(**overrides: Any) -> dict[str, Any]:
    body: dict[str, Any] = {
        "task_id": "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb",
        "source_task_id": TASK_ID,
        "queue": "orders",
        "policy_version": 1,
        "replayed": False,
        "warning": "Replay is at-least-once and may repeat external side effects.",
        "admin_replay_expires_at": "2026-10-19T00:00:00Z",
    }
    body.update(overrides)
    return body


def _preview_result(operation: str, **overrides: Any) -> dict[str, Any]:
    body: dict[str, Any] = {
        "operation": operation,
        "queue": "orders",
        "candidate_count": 2,
        "truncated": False,
        "sample_task_ids": [TASK_ID],
        "confirmation_token": "preview-token-abc",
        "confirmation_expires_at": "2026-09-19T01:00:00Z",
        "max_batch": 25,
    }
    body.update(overrides)
    return body


def _execute_result(operation: str, **overrides: Any) -> dict[str, Any]:
    body: dict[str, Any] = {
        "operation": operation,
        "queue": "orders",
        "candidate_count": 2,
        "start_index": 0,
        "processed": 1,
        "succeeded": 1,
        "skipped": 0,
        "failed": 0,
        "partial": False,
        "outcomes": [
            {
                "task_id": TASK_ID,
                "outcome": "replayed" if operation == "bulk_replay" else "cancelled",
            }
        ],
    }
    body.update(overrides)
    return body


def _wire_admin_routes(server: Any) -> None:
    server.routes[("GET", "/admin/v1/queues")] = (
        lambda body, headers: (
            200,
            {"items": [_queue_body()], "next_cursor": None},
        )
    )
    server.routes[("POST", "/admin/v1/queues")] = (
        lambda body, headers: (200, _mutation_result())
    )
    server.routes[("GET", "/admin/v1/queues/orders")] = (
        lambda body, headers: (200, _queue_body())
    )
    server.routes[("POST", "/admin/v1/queues/orders/policies")] = (
        lambda body, headers: (200, _mutation_result())
    )
    server.routes[("POST", "/admin/v1/queues/orders/policies/2:activate")] = (
        lambda body, headers: (200, _mutation_result(config_version=2))
    )
    server.routes[("POST", "/admin/v1/queues/orders:set-state")] = (
        lambda body, headers: (200, _mutation_result(state="paused", config_version=2))
    )
    server.routes[("GET", "/admin/v1/maintenance")] = (
        lambda body, headers: (
            200,
            {
                "updated_at": "2026-09-19T00:00:00Z",
                "outcome": "succeeded",
                "maintenance_run_id": "dddddddd-dddd-4ddd-8ddd-dddddddddddd",
            },
        )
    )
    server.routes[("GET", "/admin/v1/audit")] = (
        lambda body, headers: (200, {"items": [], "next_cursor": None})
    )
    server.routes[("POST", "/admin/v1/maintenance:run")] = (
        lambda body, headers: (
            200,
            {
                "status": {
                    "updated_at": "2026-09-19T00:00:00Z",
                    "outcome": "succeeded",
                    "maintenance_run_id": "dddddddd-dddd-4ddd-8ddd-dddddddddddd",
                },
                "replayed": False,
                "admin_replay_expires_at": "2026-10-19T00:00:00Z",
            },
        )
    )
    server.routes[
        ("POST", f"/admin/v1/queues/orders/dead-letters/{TASK_ID}:replay")
    ] = (lambda body, headers: (200, _replay_result()))
    server.routes[("POST", "/admin/v1/queues/orders/bulk:preview-replay")] = (
        lambda body, headers: (200, _preview_result("bulk_replay"))
    )
    server.routes[("POST", "/admin/v1/queues/orders/bulk:execute-replay")] = (
        lambda body, headers: (200, _execute_result("bulk_replay"))
    )
    server.routes[("POST", "/admin/v1/queues/orders/bulk:preview-cancel")] = (
        lambda body, headers: (200, _preview_result("bulk_cancel"))
    )
    server.routes[("POST", "/admin/v1/queues/orders/bulk:execute-cancel")] = (
        lambda body, headers: (200, _execute_result("bulk_cancel"))
    )


def _wire_break_glass_routes(server: Any) -> None:
    server.routes[
        ("POST", f"/admin/v1/queues/orders/tasks/{TASK_ID}:force-lease-expiry")
    ] = (
        lambda body, headers: (
            200,
            _bg_mutation(operation="forceLeaseExpiry"),
        )
    )
    server.routes[
        ("POST", f"/admin/v1/queues/orders/delivery-events/{EVENT_ID}:force-reclaim")
    ] = (
        lambda body, headers: (
            200,
            _bg_mutation(
                operation="forceDeliveryReclaim",
                target_id=EVENT_ID,
                generation=None,
            ),
        )
    )
    server.routes[
        (
            "POST",
            f"/admin/v1/queues/orders/delivery-events/{EVENT_ID}:force-dead-letter",
        )
    ] = (
        lambda body, headers: (
            200,
            _bg_mutation(
                operation="forceDeliveryDeadLetter",
                target_id=EVENT_ID,
                generation=None,
            ),
        )
    )
    server.routes[("POST", "/admin/v1/queues/orders:reconcile-counters")] = (
        lambda body, headers: (
            200,
            {
                "operation": "reconcileCounters",
                "queue": "orders",
                "target_id": "orders",
                "outcome": "applied",
                "delayed_count": 1,
                "ready_count": 2,
                "leased_count": 0,
            },
        )
    )
    server.routes[("POST", "/admin/v1/queues/orders:raise-replay-limit")] = (
        lambda body, headers: (
            200,
            {
                "operation": "raiseReplayLimit",
                "queue": "orders",
                "target_id": "orders",
                "outcome": "applied",
                "effective_rps": 20.0,
            },
        )
    )
    server.routes[("POST", "/admin/v1/partitions/task_attempt_20260919:force-drop")] = (
        lambda body, headers: (
            200,
            _bg_mutation(
                operation="dropExpiredPartition",
                target_id="task_attempt_20260919",
                queue=None,
                generation=None,
            ),
        )
    )
    server.routes[("POST", "/admin/v1/queues/orders/registry:repair")] = (
        lambda body, headers: (
            200,
            _bg_mutation(operation="repairRegistryEntry", generation=None),
        )
    )


async def test_admin_queue_control_parity(recording_server: Any) -> None:
    server, base_url = recording_server
    _wire_admin_routes(server)
    policy = _policy_draft()
    t_from = datetime(2026, 9, 19, 0, 0, tzinfo=UTC)
    t_to = t_from + timedelta(hours=1)

    sync_client = AdminClient(
        HttpJsonTransport(base_url, timeout_s=2.0), bearer_token=ADMIN_TOKEN
    )
    sync_client.list_queues(limit=10)
    sync_client.create_queue(
        "orders", initial_policy=policy, idempotency_key="idem-create"
    )
    sync_client.get_queue("orders")
    sync_client.create_queue_policy(
        "orders", policy, idempotency_key="idem-policy"
    )
    sync_client.activate_queue_policy(
        "orders",
        PolicyVersion(2),
        expected_config_version=ConfigVersion(1),
        idempotency_key="idem-activate",
    )
    sync_client.set_queue_state(
        "orders",
        QueueState("paused"),
        expected_config_version=ConfigVersion(1),
        idempotency_key="idem-state",
    )
    sync_client.get_maintenance_status()
    sync_client.list_admin_audit(time_from=t_from, time_to=t_to)
    sync_client.run_maintenance(idempotency_key="idem-maint")
    sync_client.replay_dead_letter(
        "orders", TASK_ID, idempotency_key="idem-replay", reason="ops replay"
    )
    preview = sync_client.preview_bulk_replay(
        "orders", filters={"failure_code": "timeout"}
    )
    sync_client.execute_bulk_replay(
        "orders",
        preview=preview,
        idempotency_key="idem-bulk-replay",
        reason="ops bulk replay",
        filters={"failure_code": "timeout"},
    )
    cancel_preview = sync_client.preview_bulk_cancel(
        "orders", filters={"state": "ready"}
    )
    sync_client.execute_bulk_cancel(
        "orders",
        preview=cancel_preview,
        reason="ops bulk cancel",
        filters={"state": "ready"},
    )
    sync_records = normalize_recorded(server.recorded)
    server.recorded.clear()

    async_transport = HttpxAsyncTransport(base_url, timeout_s=2.0)
    async_client = AsyncAdminClient(async_transport, bearer_token=ADMIN_TOKEN)
    await async_client.list_queues(limit=10)
    await async_client.create_queue(
        "orders", initial_policy=policy, idempotency_key="idem-create"
    )
    await async_client.get_queue("orders")
    await async_client.create_queue_policy(
        "orders", policy, idempotency_key="idem-policy"
    )
    await async_client.activate_queue_policy(
        "orders",
        PolicyVersion(2),
        expected_config_version=ConfigVersion(1),
        idempotency_key="idem-activate",
    )
    await async_client.set_queue_state(
        "orders",
        QueueState("paused"),
        expected_config_version=ConfigVersion(1),
        idempotency_key="idem-state",
    )
    await async_client.get_maintenance_status()
    await async_client.list_admin_audit(time_from=t_from, time_to=t_to)
    await async_client.run_maintenance(idempotency_key="idem-maint")
    await async_client.replay_dead_letter(
        "orders", TASK_ID, idempotency_key="idem-replay", reason="ops replay"
    )
    async_preview = await async_client.preview_bulk_replay(
        "orders", filters={"failure_code": "timeout"}
    )
    await async_client.execute_bulk_replay(
        "orders",
        preview=async_preview,
        idempotency_key="idem-bulk-replay",
        reason="ops bulk replay",
        filters={"failure_code": "timeout"},
    )
    async_cancel_preview = await async_client.preview_bulk_cancel(
        "orders", filters={"state": "ready"}
    )
    await async_client.execute_bulk_cancel(
        "orders",
        preview=async_cancel_preview,
        reason="ops bulk cancel",
        filters={"state": "ready"},
    )
    await async_transport.aclose()
    async_records = normalize_recorded(server.recorded)

    assert sync_records == async_records


async def test_break_glass_parity(recording_server: Any) -> None:
    server, base_url = recording_server
    _wire_break_glass_routes(server)
    kwargs = {
        "reason": "incident mitigation",
        "incident_reference": "INC-1",
        "risk_acknowledged": True,
    }

    sync_client = BreakGlassClient(
        HttpJsonTransport(base_url, timeout_s=2.0), bearer_token=BREAK_GLASS_TOKEN
    )
    sync_client.force_lease_expiry("orders", TASK_ID, **kwargs)
    sync_client.force_delivery_reclaim("orders", EVENT_ID, **kwargs)
    sync_client.force_delivery_dead_letter("orders", EVENT_ID, **kwargs)
    sync_client.reconcile_counters("orders", **kwargs)
    sync_client.raise_replay_limit("orders", **kwargs)
    sync_client.drop_expired_partition("task_attempt_20260919", **kwargs)
    sync_client.repair_registry_entry(
        "orders",
        entry_id=7,
        acknowledge_duplicate_window=True,
        **kwargs,
    )
    sync_records = normalize_recorded(server.recorded)
    server.recorded.clear()

    async_transport = HttpxAsyncTransport(base_url, timeout_s=2.0)
    async_client = AsyncBreakGlassClient(
        async_transport, bearer_token=BREAK_GLASS_TOKEN
    )
    await async_client.force_lease_expiry("orders", TASK_ID, **kwargs)
    await async_client.force_delivery_reclaim("orders", EVENT_ID, **kwargs)
    await async_client.force_delivery_dead_letter("orders", EVENT_ID, **kwargs)
    await async_client.reconcile_counters("orders", **kwargs)
    await async_client.raise_replay_limit("orders", **kwargs)
    await async_client.drop_expired_partition("task_attempt_20260919", **kwargs)
    await async_client.repair_registry_entry(
        "orders",
        entry_id=7,
        acknowledge_duplicate_window=True,
        **kwargs,
    )
    await async_transport.aclose()
    async_records = normalize_recorded(server.recorded)

    assert sync_records == async_records


async def test_admin_owned_transport_context_manager(recording_server: Any) -> None:
    _, base_url = recording_server
    async with AsyncAdminClient.from_url(
        base_url, bearer_token=ADMIN_TOKEN, timeout_s=2.0
    ) as client:
        assert isinstance(client, AsyncAdminClient)


def _capabilities_body() -> dict[str, Any]:
    return {
        "protocol_major": 1,
        "protocol_version": "1.0",
        "schema_revision": "0001",
        "scheduling": True,
        "priority": True,
        "delivery_events": False,
        "batch_claim": False,
        "long_polling": False,
        "max_claim_tasks": 1,
        "max_wait_seconds": 0,
        "payload_runtime_max_bytes": 262144,
        "payload_hard_max_bytes": 1048576,
        "enqueue_dedup_ttl_seconds": 7776000,
        "enqueue_dedup_ttl_min_seconds": 2592000,
        "enqueue_dedup_ttl_max_seconds": 31536000,
        "terminal_replay_ttl_seconds": 604800,
        "terminal_replay_ttl_min_seconds": 86400,
        "terminal_replay_ttl_max_seconds": 2592000,
        "admin_replay_ttl_seconds": 2592000,
        "admin_replay_ttl_min_seconds": 604800,
        "admin_replay_ttl_max_seconds": 7776000,
    }


async def test_admin_from_urls_routes_public_and_admin_planes(
    recording_server: Any,
) -> None:
    import threading
    from http.server import HTTPServer

    from .conftest import RecordingHandler

    public_server, public_url = recording_server
    public_server.routes[("GET", "/v1/capabilities")] = (
        lambda body, headers: (200, _capabilities_body())
    )

    admin_server = HTTPServer(("127.0.0.1", 0), RecordingHandler)
    admin_server.recorded = []  # type: ignore[attr-defined]
    admin_server.routes = {}  # type: ignore[attr-defined]
    _wire_admin_routes(admin_server)
    admin_thread = threading.Thread(target=admin_server.serve_forever, daemon=True)
    admin_thread.start()
    host, port = admin_server.server_address[:2]
    admin_url = f"http://{host}:{port}"
    try:
        async with AsyncAdminClient.from_urls(
            public_url,
            admin_url,
            bearer_token=ADMIN_TOKEN,
            timeout_s=2.0,
        ) as client:
            await client.get_capabilities()
            await client.get_queue("orders")
    finally:
        admin_server.shutdown()
        admin_server.server_close()
        admin_thread.join(timeout=2)

    assert [r["path"] for r in public_server.recorded] == ["/v1/capabilities"]
    assert [r["path"] for r in admin_server.recorded] == [  # type: ignore[attr-defined]
        "/admin/v1/queues/orders"
    ]
    assert public_server.recorded[0]["method"] == "GET"
    assert admin_server.recorded[0]["method"] == "GET"  # type: ignore[attr-defined]


async def test_admin_from_urls_aclose_closes_each_owned_transport_once() -> None:
    class _CountingTransport:
        def __init__(self) -> None:
            self.close_count = 0

        async def aclose(self) -> None:
            self.close_count += 1

        async def request(self, *args: Any, **kwargs: Any) -> Any:
            raise AssertionError("request should not be called")

    public = _CountingTransport()
    admin = _CountingTransport()
    client = AsyncAdminClient(
        public,  # type: ignore[arg-type]
        bearer_token=ADMIN_TOKEN,
        admin_transport=admin,  # type: ignore[arg-type]
        owns_transport=True,
        owns_admin_transport=True,
    )
    await client.aclose()
    assert public.close_count == 1
    assert admin.close_count == 1


async def test_admin_aclose_is_idempotent_with_context_exit() -> None:
    class _CountingTransport:
        def __init__(self) -> None:
            self.close_count = 0

        async def aclose(self) -> None:
            self.close_count += 1

        async def request(self, *args: Any, **kwargs: Any) -> Any:
            raise AssertionError("request should not be called")

    public = _CountingTransport()
    admin = _CountingTransport()
    client = AsyncAdminClient(
        public,  # type: ignore[arg-type]
        bearer_token=ADMIN_TOKEN,
        admin_transport=admin,  # type: ignore[arg-type]
        owns_transport=True,
        owns_admin_transport=True,
    )
    await client.aclose()
    await client.aclose()
    async with client:
        pass
    assert public.close_count == 1
    assert admin.close_count == 1


async def test_admin_aclose_same_owned_transport_object_once() -> None:
    class _CountingTransport:
        def __init__(self) -> None:
            self.close_count = 0

        async def aclose(self) -> None:
            self.close_count += 1

        async def request(self, *args: Any, **kwargs: Any) -> Any:
            raise AssertionError("request should not be called")

    shared = _CountingTransport()
    client = AsyncAdminClient(
        shared,  # type: ignore[arg-type]
        bearer_token=ADMIN_TOKEN,
        admin_transport=shared,  # type: ignore[arg-type]
        owns_transport=True,
        owns_admin_transport=True,
    )
    await client.aclose()
    await client.aclose()
    assert shared.close_count == 1


async def test_admin_from_urls_equal_urls_still_owns_two_transports() -> None:
    """Equal bases still create two owned transports; aclose closes each once."""

    base = "http://127.0.0.1:9"
    client = AsyncAdminClient.from_urls(
        base, base, bearer_token=ADMIN_TOKEN, timeout_s=2.0
    )
    assert client._transport is not client._admin_transport
    assert client._owns_transport is True
    assert client._owns_admin_transport is True
    await client.aclose()


async def test_admin_caller_owned_transports_not_closed_on_aclose() -> None:
    class _CountingTransport:
        def __init__(self) -> None:
            self.close_count = 0

        async def aclose(self) -> None:
            self.close_count += 1

        async def request(self, *args: Any, **kwargs: Any) -> Any:
            raise AssertionError("request should not be called")

    public = _CountingTransport()
    admin = _CountingTransport()
    client = AsyncAdminClient(
        public,  # type: ignore[arg-type]
        bearer_token=ADMIN_TOKEN,
        admin_transport=admin,  # type: ignore[arg-type]
        owns_transport=False,
        owns_admin_transport=False,
    )
    await client.aclose()
    assert public.close_count == 0
    assert admin.close_count == 0


@pytest.mark.parametrize("token", [None, "", "   ", "\t", "\n"])
async def test_admin_from_urls_rejects_empty_bearer_token(token: str | None) -> None:
    with pytest.raises(ValueError, match="bearer_token"):
        AsyncAdminClient.from_urls(
            "http://127.0.0.1:1",
            "http://127.0.0.1:2",
            bearer_token=token,  # type: ignore[arg-type]
            timeout_s=2.0,
        )


@pytest.mark.parametrize("token", [None, "", "   ", "\t", "\n"])
async def test_async_admin_observer_break_glass_reject_whitespace_bearer(
    token: str | None,
) -> None:
    transport = HttpxAsyncTransport("http://127.0.0.1:9", timeout_s=0.1)
    with pytest.raises(ValueError, match="bearer_token"):
        AsyncAdminClient(transport, bearer_token=token)  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="bearer_token"):
        AsyncObserverClient(
            transport,
            bearer_token=token,  # type: ignore[arg-type]
            admin_transport=transport,
        )
    with pytest.raises(ValueError, match="bearer_token"):
        AsyncBreakGlassClient(transport, bearer_token=token)  # type: ignore[arg-type]
    exact = " tok en "
    admin = AsyncAdminClient(transport, bearer_token=exact)
    assert admin._auth_headers()["Authorization"] == f"Bearer {exact}"
    observer = AsyncObserverClient(
        transport, bearer_token=exact, admin_transport=transport
    )
    assert observer._auth_headers()["Authorization"] == f"Bearer {exact}"
    break_glass = AsyncBreakGlassClient(transport, bearer_token=exact)
    assert break_glass._auth_headers()["Authorization"] == f"Bearer {exact}"


async def test_async_execute_rejects_empty_confirmation_token_before_transport(
    recording_server: Any,
) -> None:
    import pytest
    from queue_service_admin.models import BulkPreviewResult

    server, base_url = recording_server
    _wire_admin_routes(server)
    preview = BulkPreviewResult.parse(_preview_result("bulk_replay"))
    empty_preview = BulkPreviewResult(
        operation=preview.operation,
        queue=preview.queue,
        candidate_count=preview.candidate_count,
        truncated=preview.truncated,
        sample_task_ids=preview.sample_task_ids,
        confirmation_token="",
        confirmation_expires_at=preview.confirmation_expires_at,
        max_batch=preview.max_batch,
    )
    transport = HttpxAsyncTransport(base_url, timeout_s=2.0)
    client = AsyncAdminClient(transport, bearer_token=ADMIN_TOKEN)
    with pytest.raises(ValueError, match="confirmation_token"):
        await client.execute_bulk_replay(
            "orders",
            preview=empty_preview,
            idempotency_key="key",
            reason="ok",
            filters={},
        )
    await transport.aclose()
    assert server.recorded == []


async def test_async_execute_rejects_oversized_confirmation_token_before_transport(
    recording_server: Any,
) -> None:
    import pytest
    from queue_service_admin.models import (
        CONFIRMATION_TOKEN_MAX_LENGTH,
        BulkPreviewResult,
    )

    server, base_url = recording_server
    _wire_admin_routes(server)
    preview = BulkPreviewResult.parse(_preview_result("bulk_replay"))
    oversized = BulkPreviewResult(
        operation=preview.operation,
        queue=preview.queue,
        candidate_count=preview.candidate_count,
        truncated=preview.truncated,
        sample_task_ids=preview.sample_task_ids,
        confirmation_token="x" * (CONFIRMATION_TOKEN_MAX_LENGTH + 1),
        confirmation_expires_at=preview.confirmation_expires_at,
        max_batch=preview.max_batch,
    )
    transport = HttpxAsyncTransport(base_url, timeout_s=2.0)
    client = AsyncAdminClient(transport, bearer_token=ADMIN_TOKEN)
    with pytest.raises(ValueError, match="24576"):
        await client.execute_bulk_replay(
            "orders",
            preview=oversized,
            idempotency_key="key",
            reason="ok",
            filters={},
        )
    await transport.aclose()
    assert server.recorded == []


async def test_admin_lacks_observer_only_task_reads() -> None:
    """OpenAPI/ownership: getTask and listTaskAttempts are Observer (not Admin)."""
    from queue_service_admin import ObserverClient
    from queue_service_admin.async_client import AsyncObserverClient

    for name in ("get_task", "list_task_attempts"):
        assert not hasattr(AdminClient, name), name
        assert not hasattr(AsyncAdminClient, name), name
        assert hasattr(ObserverClient, name) and callable(
            getattr(ObserverClient, name)
        ), name
        assert hasattr(AsyncObserverClient, name) and callable(
            getattr(AsyncObserverClient, name)
        ), name


def test_break_glass_async_cannot_be_subclassed() -> None:
    try:

        class _Child(AsyncBreakGlassClient):  # type: ignore[misc]
            pass

    except TypeError:
        return
    raise AssertionError("AsyncBreakGlassClient must forbid subclassing")
